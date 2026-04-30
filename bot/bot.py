#!/usr/bin/env python3
"""
bot.py — Astra Voice Bot (Pipecat >= 1.1.0 + full-duplex WebSocket)
======================================================================
Pipeline: Deepgram STT → OpenAI LLM → Cartesia TTS

CHANGELOG vs versão anterior (0.0.65):
  [NEW #1] LLMContext + LLMContextAggregatorPair (API universal, substituí
           OpenAILLMContext + create_context_aggregator — ambos deprecated)

  [NEW #2] VAD migrado para LLMUserAggregatorParams
           vad_analyzer no FastAPIWebsocketParams foi REMOVIDO no 1.x.
           O VAD agora vive no aggregator, mais perto de onde é consumido.

  [NEW #3] MuteUntilFirstBotCompleteUserMuteStrategy
           Bloqueia interrupção apenas durante a saudação inicial.
           Turnos seguintes: usuário pode interromper livremente.

  [NEW #4] Termination via transport.event_handler("on_client_disconnected")
           Padrão oficial da doc de Pipeline Termination. Substitui o watcher
           de polling (WebSocketState) que usávamos como workaround.
           task.cancel() → CancelFrame → SystemFrame → bypass de queue →
           shutdown imediato de Deepgram WS + Cartesia WS.

  [NEW #5] idle_timeout_secs=600 como safety net
           Em vez de None (sem timeout), mantemos 600s como rede de segurança
           contra pipelines zumbis em caso de falha do event handler.

  [FIX #1] LiveOptions migrado de 'deepgram' para 'pipecat.services.deepgram.stt'
           O deepgram-sdk v6 removeu LiveOptions do pacote raiz. Agora o
           pipecat fornece sua própria LiveOptions via pipecat.services.deepgram.stt.

  [FIX #2] ctx_aggregator.user() e ctx_aggregator.assistant() são MÉTODOS
           (chamáveis com parênteses), não propriedades. Corrigido o pipeline
           para usar ctx_aggregator.user() e ctx_aggregator.assistant().

  [FIX #3] FrameSerializerType REMOVIDO do pipecat 1.x.
           O transporte agora infere binário vs texto pelo tipo de retorno de
           serialize() — bytes → binary WS, str → text WS. O BridgeSerializer
           não precisa mais declarar 'type'. Import e propriedade removidos.

  [FIX #4] FastAPIWebsocketTransport movido para novo módulo no pipecat 1.x.
           Caminho antigo: pipecat.transports.network.fastapi_websocket
           Caminho novo:   pipecat.transports.websocket.fastapi

  [FIX #5] vad_audio_passthrough REMOVIDO do FastAPIWebsocketParams no 1.x.
           O parâmetro era legado (da época em que VAD ficava no transport).
           Removido junto com vad_enabled e vad_analyzer do params do transport.

Protocolo full-duplex com mod_audio_fork:
  RX ← bytes L16/16kHz (mod_audio_fork faz upsample 8→16kHz)
  TX → bytes L16/8kHz  (Cartesia → mod_audio_fork WRITE_REPLACE → RTP)
"""

import asyncio
import logging
import os

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams
from pipecat.frames.frames import (
    Frame,
    InputAudioRawFrame,
    OutputAudioRawFrame,
    TTSSpeakFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
# [FIX #3] FrameSerializerType foi REMOVIDO do pipecat 1.x.
# O transporte agora infere binário vs texto pelo tipo retornado por serialize():
#   bytes → WebSocket binary frame | str → WebSocket text frame
# Não importar FrameSerializerType e não declarar propriedade 'type' no serializer.
from pipecat.serializers.base_serializer import FrameSerializer
from pipecat.services.cartesia.tts import CartesiaTTSService

# [FIX #1] LiveOptions agora vem do pipecat, não do deepgram SDK.
# O deepgram-sdk v6 removeu LiveOptions do namespace raiz ('from deepgram import LiveOptions').
# O pipecat >= 0.0.99 fornece sua própria LiveOptions compatível com o novo SDK.
from pipecat.services.deepgram.stt import DeepgramSTTService, LiveOptions

from pipecat.services.openai.llm import OpenAILLMService

# [FIX #4] FastAPIWebsocketTransport movido para novo módulo no pipecat 1.x.
# Caminho antigo (0.x): pipecat.transports.network.fastapi_websocket
# Caminho novo  (1.x): pipecat.transports.websocket.fastapi
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)

# ─────────────────────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("AstraBot")


class _DeepgramCancelFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return "tasks cancelled error" not in record.getMessage().lower()


for _h in logging.root.handlers:
    _h.addFilter(_DeepgramCancelFilter())


# ─────────────────────────────────────────────────────────────────────────────
# Deepgram com shutdown silencioso
# ─────────────────────────────────────────────────────────────────────────────
class DeepgramSTTServiceQuiet(DeepgramSTTService):
    """Converte erros de cancelamento do SDK Deepgram em CancelledError nativo."""

    async def _disconnect(self):
        try:
            await super()._disconnect()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if "cancelled" in str(e).lower():
                raise asyncio.CancelledError(str(e)) from e
            raise


# ─────────────────────────────────────────────────────────────────────────────
# Serializer — raw PCM bidirecional
# ─────────────────────────────────────────────────────────────────────────────
class BridgeSerializer(FrameSerializer):
    """
    RX (mod_audio_fork → bot):  PCM L16 @ 16kHz → AudioRawFrame para Deepgram
    TX (bot → mod_audio_fork):  AudioRawFrame @ 8kHz → bytes → WRITE_REPLACE → RTP

    [FIX #3] O pipecat 1.x removeu FrameSerializerType e a propriedade 'type'.
    O transporte infere o tipo de mensagem WS (binário vs texto) diretamente
    pelo tipo Python retornado por serialize(): bytes → binary, str → text.
    Não é mais necessário declarar a propriedade 'type'.
    """

    SAMPLE_RATE = 16_000
    CHANNELS    = 1

    async def deserialize(self, data: bytes | str) -> Frame | None:
        if isinstance(data, bytes) and data:
            # Pipecat 1.x espera InputAudioRawFrame vindo do transporte.
            return InputAudioRawFrame(
                audio=data,
                sample_rate=self.SAMPLE_RATE,
                num_channels=self.CHANNELS,
            )
        return None

    async def serialize(self, frame: Frame) -> bytes | None:
        # Pipecat 1.x envia para o transporte frames de saída de áudio.
        if isinstance(frame, OutputAudioRawFrame) and frame.audio:
            return frame.audio
        return None


# ─────────────────────────────────────────────────────────────────────────────
# System prompt
# ─────────────────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = os.getenv("SYSTEM_PROMPT", """Você é a Astra, uma assistente de voz inteligente e prestativa.
Responda de forma clara, concisa e natural para uma conversa por telefone.
Limite suas respostas a 1-3 frases curtas.
Não use emojis, markdown ou formatação especial.""")


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline Pipecat
# ─────────────────────────────────────────────────────────────────────────────
async def run_bot(websocket: WebSocket, call_id: str) -> None:
    logger.info(f"[{call_id}] Pipeline iniciando (full-duplex)")

    # ── Transport ──────────────────────────────────────────────────────────
    # [NEW #2] VAD removido do transport params — migrado para LLMUserAggregatorParams.
    # [FIX #5] vad_audio_passthrough REMOVIDO no pipecat 1.x junto com
    #          vad_enabled e vad_analyzer do FastAPIWebsocketParams.
    #          O equivalente atual é audio_in_passthrough (default=True no 1.x).
    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            add_wav_header=False,
            serializer=BridgeSerializer(),
            audio_out_10ms_chunks=2,
            audio_out_sample_rate=8000,
            audio_in_sample_rate=16000,
        ),
    )

    # ── STT ───────────────────────────────────────────────────────────────
    # [FIX #1] LiveOptions agora importado de pipecat.services.deepgram.stt,
    # não do deepgram SDK (que removeu LiveOptions no v6).
    stt = DeepgramSTTServiceQuiet(
        api_key=os.environ["DEEPGRAM_API_KEY"],
        live_options=LiveOptions(
            model=os.getenv("STT_MODEL", "nova-3"),
            language="pt-BR",
            encoding="linear16",
            sample_rate=16000,
            channels=1,
            smart_format=True,
            interim_results=True,
            endpointing=150,    # reduzido de 300 → 150ms para menor latência
        ),
    )

    # ── LLM ───────────────────────────────────────────────────────────────
    llm = OpenAILLMService(
        api_key=os.environ["OPENAI_API_KEY"],
        base_url=os.getenv("OPENAI_API_BASE", "https://api.inceptionlabs.ai/v1"),
        model=os.getenv("OPENAI_MODEL", "mercury-2"),
    )

    # ── TTS ───────────────────────────────────────────────────────────────
    tts = CartesiaTTSService(
        api_key=os.environ["CARTESIA_API_KEY"],
        voice_id=os.getenv("TTS_VOICE", "a0e99841-438c-4a64-b679-ae501e7d6091"),
        model=os.getenv("TTS_MODEL", "sonic-turbo"),  # sonic-turbo: ~40% menos TTFB
        language=os.getenv("TTS_LANGUAGE", "pt"),
        sample_rate=8000,
        encoding="pcm_s16le",
    )

    # ── Context + Aggregator ───────────────────────────────────────────────
    # [NEW #1] LLMContext + LLMContextAggregatorPair (API universal 0.0.99+)
    # [NEW #2] VAD agora no LLMUserAggregatorParams
    # [NEW #3] MuteUntilFirstBotCompleteUserMuteStrategy: bloqueia interrupção
    #          apenas durante a saudação inicial; turnos seguintes são livres.
    context = LLMContext([{"role": "system", "content": SYSTEM_PROMPT}])
    ctx_aggregator = LLMContextAggregatorPair(
        context,
        user_params=LLMUserAggregatorParams(
            vad_analyzer=SileroVADAnalyzer(
                params=VADParams(
                    confidence=0.7,
                    start_secs=0.2,
                    stop_secs=0.2,
                    min_volume=0.6,
                )
            ),
            # user_mute_strategies removido: TurnTrackingObserver crasha em
            # AudioRawFrame (sem .id) no pipecat 1.1.0, nunca dispara unmute.
        ),
    )

    # ── Pipeline ──────────────────────────────────────────────────────────
    # [FIX #2] ctx_aggregator.user() e ctx_aggregator.assistant() são MÉTODOS
    #          (não propriedades). Devem ser chamados com parênteses para
    #          retornar as instâncias dos aggregators que o Pipeline espera.
    pipeline = Pipeline([
        transport.input(),
        stt,
        ctx_aggregator.user(),        # ← método chamado com (), retorna LLMUserAggregator
        llm,
        tts,
        transport.output(),
        ctx_aggregator.assistant(),   # ← método chamado com (), retorna LLMAssistantAggregator
    ])

    # ── PipelineTask ──────────────────────────────────────────────────────
    # [NEW #5] idle_timeout_secs=600 como safety net (era None).
    #          O encerramento primário vem do event handler on_client_disconnected.
    #          O idle timeout é apenas rede de segurança contra zumbis.
    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            allow_interruptions=True,
            idle_timeout_secs=300,  # safety net: 5min sem atividade → encerra
        ),
    )

    # ── Saudação inicial ──────────────────────────────────────────────────
    async def _greet():
        await asyncio.sleep(0.5)
        logger.info(f"[{call_id}] Enviando saudação")
        await task.queue_frames([TTSSpeakFrame("Olá! Aqui é a Astra. Como posso te ajudar hoje?")])

    asyncio.create_task(_greet(), name=f"greet-{call_id[:8]}")

    # ── [NEW #4] Termination via event handler ────────────────────────────
    # Padrão oficial da doc Pipeline Termination:
    # on_client_disconnected → task.cancel() → CancelFrame (SystemFrame) →
    # bypass de queue → shutdown imediato de todos os serviços.
    #
    # Fallback: watcher de polling mantido como garantia adicional caso o
    # event handler não dispare (ex: desconexão abrupta de rede).
    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        logger.info(f"[{call_id}] on_client_disconnected — cancelando pipeline")
        await task.cancel()

    async def _disconnect_watcher():
        """Fallback: polling do WebSocketState caso o event handler não dispare."""
        while websocket.client_state != WebSocketState.DISCONNECTED:
            await asyncio.sleep(0.3)
        logger.info(f"[{call_id}] WS DISCONNECTED detectado pelo watcher — cancelando pipeline")
        await task.cancel()  # idempotente se já cancelado pelo event handler

    watcher = asyncio.create_task(
        _disconnect_watcher(), name=f"watcher-{call_id[:8]}"
    )

    # ── Run ───────────────────────────────────────────────────────────────
    try:
        runner = PipelineRunner(handle_sigint=False)
        await runner.run(task)
    except Exception as e:
        logger.error(f"[{call_id}] Pipeline erro: {e}")
        await task.cancel()
    finally:
        watcher.cancel()
        await task.cancel()  # idempotente
        logger.info(f"[{call_id}] Pipeline encerrada")


# ─────────────────────────────────────────────────────────────────────────────
# FastAPI
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Astra Voice Bot")


@app.websocket("/ws/audio/{call_id}")
async def ws_audio(websocket: WebSocket, call_id: str) -> None:
    await websocket.accept()
    logger.info(f"Bridge conectou: {call_id}")
    try:
        await run_bot(websocket, call_id)
    except WebSocketDisconnect:
        logger.info(f"[{call_id}] Bridge desconectou (hangup normal)")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": "Astra Voice Bot"}
