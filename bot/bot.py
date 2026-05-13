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
import json
import logging
import os
import re

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams
from pipecat.frames.frames import (
    ErrorFrame,
    Frame,
    InputAudioRawFrame,
    OutputAudioRawFrame,
    TextFrame,
    TTSSpeakFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
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

from tools.datetime_tool import _get_current_datetime_with_timeout, TOOLS as DATETIME_TOOLS
from tools.web_search_tool import _search_web_with_timeout, TOOLS as WEB_SEARCH_TOOLS

# [FIX #4] FastAPIWebsocketTransport movido para novo módulo no pipecat 1.x.
# Caminho antigo (0.x): pipecat.transports.network.fastapi_websocket
# Caminho novo  (1.x): pipecat.transports.websocket.fastapi
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)

# ─────────────────────────────────────────────────────────────────────────────
load_dotenv()

# ── Debug: gravador de áudio por sessão ──────────────────────────────────────
# Ativado via variável de ambiente: AUDIO_DEBUG_RECORD=1
# Gera /tmp/rx_<call_id>.wav  — áudio que chega do mod_audio_fork (voz do usuário)
# Gera /tmp/tx_<call_id>.wav  — áudio enviado para o RTP (TTS do bot)
# NUNCA ativar em produção — escrita em disco por frame degrada latência.

import struct
import wave
import io

class AudioRecorder:
    """Grava frames PCM em arquivo WAV durante a chamada."""

    def __init__(self, path: str, sample_rate: int, channels: int = 1):
        self._path        = path
        self._sample_rate = sample_rate
        self._channels    = channels
        self._buf         = io.BytesIO()
        self._enabled     = os.getenv("AUDIO_DEBUG_RECORD", "0") == "1"
        if self._enabled:
            logger.debug(f"AudioRecorder ativo: {path}")

    def write(self, pcm_bytes: bytes) -> None:
        if self._enabled and pcm_bytes:
            self._buf.write(pcm_bytes)

    def close(self) -> None:
        if not self._enabled:
            return
        raw = self._buf.getvalue()
        if not raw:
            return
        try:
            with wave.open(self._path, "wb") as wf:
                wf.setnchannels(self._channels)
                wf.setsampwidth(2)                    # int16 = 2 bytes
                wf.setframerate(self._sample_rate)
                wf.writeframes(raw)
            logger.info(f"AudioRecorder salvo: {self._path} ({len(raw)//2} samples)")
        except Exception as e:
            logger.warning(f"AudioRecorder erro ao salvar {self._path}: {e}")


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
    SAMPLE_RATE = 16_000
    CHANNELS    = 1

    def __init__(self, rx_recorder: "AudioRecorder | None" = None,
                       tx_recorder: "AudioRecorder | None" = None):
        self._rx_rec = rx_recorder   # grava áudio chegando do FS (voz do usuário)
        self._tx_rec = tx_recorder   # grava áudio saindo para o FS (TTS do bot)

    async def deserialize(self, data: bytes | str) -> Frame | None:
        if isinstance(data, bytes) and data:
            # Grava ANTES de entregar ao Deepgram — áudio exato que o STT recebe
            if self._rx_rec:
                self._rx_rec.write(data)
            return InputAudioRawFrame(
                audio=data,
                sample_rate=self.SAMPLE_RATE,
                num_channels=self.CHANNELS,
            )
        return None

    async def serialize(self, frame: Frame) -> bytes | None:
        if isinstance(frame, OutputAudioRawFrame) and frame.audio:
            # Grava o TTS que vai para o RTP do usuário
            if self._tx_rec:
                self._tx_rec.write(frame.audio)
            return frame.audio
        return None


class ToolCallTextFallbackProcessor(FrameProcessor):
    """Converte pseudo-tool-call em JSON textual para resposta natural."""

    def __init__(self, get_search_query_hint=None):
        super().__init__()
        self._get_search_query_hint = get_search_query_hint

    @staticmethod
    def _looks_like_technical_fragment(text: str) -> bool:
        lowered = (text or "").lower()
        return bool(
            ("{" in text or "}" in text or "[" in text or "]" in text)
            or '"tool"' in lowered
            or '"params"' in lowered
            or "recency_days" in lowered
            or '"query"' in lowered
            or lowered.startswith("tool:")
        )

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if not isinstance(frame, TextFrame):
            await self.push_frame(frame, direction)
            return

        text = (frame.text or "").strip()
        if not text:
            await self.push_frame(frame, direction)
            return

        normalized = text.strip("` \n\t")
        lowered = normalized.lower()

        # Nunca deixa fragmento técnico de tool vazar para TTS.
        if self._looks_like_technical_fragment(normalized):
            try:
                payload = json.loads(normalized)
                if isinstance(payload, dict) and payload.get("tool") == "search_web":
                    params = payload.get("params") or {}
                    query = params.get("query", "")
                    if not query and self._get_search_query_hint:
                        query = self._get_search_query_hint() or ""
                    result = await _search_web_with_timeout(
                        query=query,
                        num_results=params.get("num_results", 3),
                        hl=params.get("hl", "pt-BR"),
                        gl=params.get("gl", "br"),
                        recency_days=params.get("recency_days"),
                    )
                    if result.get("success"):
                        speech = _human_web_search_from_tool_result(result.get("result"))
                    else:
                        speech = "Não consegui pesquisar isso na internet agora, mas posso tentar novamente em instantes."
                    await self.push_frame(TextFrame(speech), direction)
                elif isinstance(payload, dict) and payload.get("tool") == "get_current_datetime":
                    params = payload.get("params") or {}
                    timezone = params.get("timezone", "America/Sao_Paulo")
                    result = await _get_current_datetime_with_timeout(timezone=timezone)
                    if result.get("success"):
                        when = _human_datetime_from_tool_result(result.get("result"))
                        await self.push_frame(TextFrame(f"Claro. Acabei de verificar: agora são {when}."), direction)
                    else:
                        await self.push_frame(
                            TextFrame("Não consegui consultar essa informação agora, mas posso tentar novamente em instantes."),
                            direction,
                        )
                # se não deu parse útil, apenas suprime o frame técnico
            except Exception:
                pass
            return

        # Blindagem contra vazamento de texto técnico de tool para o TTS
        # (inclusive quando vem quebrado em chunks).
        if (
            "we need to call" in lowered
            or "we need to search" in lowered
            or "need to search" in lowered
            or "search the web" in lowered
            or "get_current_datetime" in lowered
            or "search_web" in lowered
            or '{"tool' in lowered
            or '"tool"' in lowered
            or lowered.startswith("tool:")
        ):
            if "search_web" in lowered or "pesquis" in lowered or "internet" in lowered:
                query = _extract_search_query_hint(normalized)
                if self._get_search_query_hint:
                    hinted = self._get_search_query_hint()
                    if hinted:
                        query = hinted
                result = await _search_web_with_timeout(query=query)
                if result.get("success"):
                    speech = _human_web_search_from_tool_result(result.get("result"))
                    await self.push_frame(TextFrame(speech), direction)
                else:
                    await self.push_frame(
                        TextFrame(
                            "Não consegui pesquisar isso na internet agora, mas posso tentar novamente em instantes."
                        ),
                        direction,
                    )
            else:
                result = await _get_current_datetime_with_timeout(timezone="America/Sao_Paulo")
                if result.get("success"):
                    when = _human_datetime_from_tool_result(result.get("result"))
                    await self.push_frame(
                        TextFrame(f"Claro. Acabei de verificar: agora são {when}."),
                        direction,
                    )
                else:
                    await self.push_frame(
                        TextFrame(
                            "Não consegui consultar essa informação agora, mas posso tentar novamente em instantes."
                        ),
                        direction,
                    )
            return

        # Alguns provedores retornam pseudo-tool-call em texto livre.
        if "get_current_datetime" in lowered and "tool" not in lowered:
            result = await _get_current_datetime_with_timeout(timezone="America/Sao_Paulo")
            if result.get("success"):
                when = _human_datetime_from_tool_result(result.get("result"))
                await self.push_frame(
                    TextFrame(f"Claro. Acabei de verificar: agora são {when}."),
                    direction,
                )
            else:
                await self.push_frame(
                    TextFrame(
                        "Não consegui consultar essa informação agora, mas posso tentar novamente em instantes."
                    ),
                    direction,
                )
            return

        try:
            payload = json.loads(normalized)
        except Exception:
            await self.push_frame(frame, direction)
            return

        if not isinstance(payload, dict) or payload.get("tool") != "get_current_datetime":
            if isinstance(payload, dict) and payload.get("tool") == "search_web":
                params = payload.get("params") or {}
                query = params.get("query", "")
                num_results = params.get("num_results", 3)
                hl = params.get("hl", "pt-BR")
                gl = params.get("gl", "br")
                result = await _search_web_with_timeout(
                    query=query,
                    num_results=num_results,
                    hl=hl,
                    gl=gl,
                    recency_days=params.get("recency_days"),
                )
                if result.get("success"):
                    speech = _human_web_search_from_tool_result(result.get("result"))
                else:
                    speech = "Não consegui pesquisar isso na internet agora, mas posso tentar novamente em instantes."
                await self.push_frame(TextFrame(speech), direction)
                return
            await self.push_frame(frame, direction)
            return

        params = payload.get("params") or {}
        timezone = params.get("timezone", "America/Sao_Paulo")
        result = await _get_current_datetime_with_timeout(timezone=timezone)

        if result.get("success"):
            when = _human_datetime_from_tool_result(result.get("result"))
            human_text = f"Claro. Acabei de verificar: agora são {when}."
        else:
            human_text = (
                "Não consegui consultar essa informação agora, mas posso tentar novamente em instantes."
            )

        await self.push_frame(TextFrame(human_text), direction)


class LLMErrorFallbackProcessor(FrameProcessor):
    """Transforma erro não-fatal do LLM em resposta falada amigável."""

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if not isinstance(frame, ErrorFrame):
            await self.push_frame(frame, direction)
            return

        if getattr(frame, "fatal", False):
            await self.push_frame(frame, direction)
            return

        human_text = (
            "Tive uma instabilidade rápida para consultar isso agora. "
            "Se você quiser, eu tento novamente neste momento."
        )
        await self.push_frame(TextFrame(human_text), direction)


UNITS_PT = {
    0: "zero",
    1: "um",
    2: "dois",
    3: "tres",
    4: "quatro",
    5: "cinco",
    6: "seis",
    7: "sete",
    8: "oito",
    9: "nove",
    10: "dez",
    11: "onze",
    12: "doze",
    13: "treze",
    14: "quatorze",
    15: "quinze",
    16: "dezesseis",
    17: "dezessete",
    18: "dezoito",
    19: "dezenove",
}
TENS_PT = {
    20: "vinte",
    30: "trinta",
    40: "quarenta",
    50: "cinquenta",
    60: "sessenta",
    70: "setenta",
    80: "oitenta",
    90: "noventa",
}
HUNDREDS_PT = {
    100: "cem",
    200: "duzentos",
    300: "trezentos",
    400: "quatrocentos",
    500: "quinhentos",
    600: "seiscentos",
    700: "setecentos",
    800: "oitocentos",
    900: "novecentos",
}


def _num_to_pt(n: int) -> str:
    if n < 20:
        return UNITS_PT[n]
    if n < 100:
        d = (n // 10) * 10
        r = n % 10
        return TENS_PT[d] if r == 0 else f"{TENS_PT[d]} e {UNITS_PT[r]}"
    if n < 1000:
        if n == 100:
            return "cem"
        h = (n // 100) * 100
        r = n % 100
        htxt = "cento" if h == 100 else HUNDREDS_PT[h]
        return htxt if r == 0 else f"{htxt} e {_num_to_pt(r)}"
    if n < 10000:
        th = n // 1000
        r = n % 1000
        prefix = "mil" if th == 1 else f"{_num_to_pt(th)} mil"
        return prefix if r == 0 else f"{prefix} e {_num_to_pt(r)}"
    return str(n)


def _numbers_to_words_pt(text: str) -> str:
    def repl(match: re.Match) -> str:
        value = int(match.group(0))
        if value > 9999:
            return match.group(0)
        return _num_to_pt(value)

    return re.sub(r"\b\d+\b", repl, text)


class NumberNormalizationProcessor(FrameProcessor):
    """Converte numeros em digitos para forma por extenso antes do TTS."""

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if not isinstance(frame, TextFrame):
            await self.push_frame(frame, direction)
            return

        text = frame.text or ""
        lowered = text.lower()
        if (
            "recency_days" in lowered
            or '"tool"' in lowered
            or '"params"' in lowered
            or '{"tool' in lowered
            or "[" in text
            or "]" in text
            or "{" in text
            or "}" in text
        ):
            # Conteúdo técnico: não transformar números, deixa filtros anteriores lidarem.
            return
        normalized = _numbers_to_words_pt(text)
        await self.push_frame(TextFrame(normalized), direction)


def _extract_latest_user_text(context: LLMContext) -> str:
    """Extrai a última mensagem de usuário do contexto, de forma resiliente."""
    try:
        messages = getattr(context, "messages", None)
        if messages is None and hasattr(context, "get_messages"):
            messages = context.get_messages()
        if not messages:
            return ""
        for msg in reversed(messages):
            if isinstance(msg, dict) and msg.get("role") == "user":
                content = msg.get("content")
                if isinstance(content, str):
                    return content.strip().lower()
    except Exception:
        pass
    return ""


def _human_datetime_from_tool_result(result: object) -> str:
    """Converte saída da tool de data/hora para texto natural de voz."""
    if isinstance(result, dict):
        spoken = result.get("spoken_pt_br")
        if isinstance(spoken, str) and spoken.strip():
            return spoken.strip()
        human = result.get("human")
        if isinstance(human, str) and human.strip():
            return human.strip()
        iso = result.get("iso")
        if isinstance(iso, str) and iso.strip():
            return iso.strip()
    if isinstance(result, str) and result.strip():
        return result.strip()
    return "um horário válido"


def _extract_search_query_hint(text: str) -> str:
    lowered = (text or "").lower()
    m = re.search(r"query[\"']?\s*[:=]\s*[\"']([^\"']+)[\"']", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    if "pesquis" in lowered:
        return "resumo do assunto solicitado pelo usuário"
    if "internet" in lowered:
        return "informação atual solicitada pelo usuário"
    if "search" in lowered:
        return "informação atual solicitada pelo usuário"
    return "informação solicitada pelo usuário"


def _human_web_search_from_tool_result(result: object) -> str:
    if not isinstance(result, dict):
        return "Encontrei informações, mas não consegui organizar um resumo agora."
    items = result.get("items")
    if not isinstance(items, list) or not items:
        return "Não encontrei resultados relevantes neste momento."
    first = items[0] if isinstance(items[0], dict) else {}
    title = (first.get("title") or "").strip()
    snippet = (first.get("snippet") or "").strip()
    source = (first.get("source") or "").strip()
    if title and snippet and source:
        return f"Pesquisei para você. Segundo {source}, {title}. Resumo: {snippet}"
    if title and snippet:
        return f"Pesquisei para você. {title}. Resumo: {snippet}"
    if snippet:
        return f"Pesquisei para você. {snippet}"
    return "Pesquisei para você, mas os resultados vieram sem resumo legível."


def _is_retry_like_text(text: str) -> bool:
    t = (text or "").strip().lower()
    return bool(
        re.search(
            r"\b(novamente|de novo|repetir|verificar novamente|poderia verificar)\b",
            t,
        )
    )


def _best_search_query_from_context(context: LLMContext, last_user_text: str) -> str:
    """
    Em pedidos de repetição, tenta recuperar a última pergunta de conteúdo
    (anterior ao 'verifique novamente') para usar como query de busca.
    """
    candidate = (last_user_text or "").strip()
    try:
        messages = getattr(context, "messages", None)
        if messages is None and hasattr(context, "get_messages"):
            messages = context.get_messages()
        if not messages:
            return candidate or "informação atual solicitada pelo usuário"

        user_msgs = [
            m.get("content", "").strip()
            for m in messages
            if isinstance(m, dict) and m.get("role") == "user" and isinstance(m.get("content"), str)
        ]
        if not user_msgs:
            return candidate or "informação atual solicitada pelo usuário"

        last = user_msgs[-1]
        if not _is_retry_like_text(last):
            return _sanitize_search_query(last)

        for prev in reversed(user_msgs[:-1]):
            if prev and not _is_retry_like_text(prev):
                return _sanitize_search_query(prev)
    except Exception:
        pass
    return _sanitize_search_query(candidate) or "informação atual solicitada pelo usuário"


def _sanitize_search_query(query: str) -> str:
    q = (query or "").strip()
    # Remove final de frase truncada comum do STT ("... e")
    q = re.sub(r"\s+e\s*$", "", q, flags=re.IGNORECASE)
    # Colapsa espaços
    q = re.sub(r"\s+", " ", q).strip()
    return q


# ─────────────────────────────────────────────────────────────────────────────
# System prompt
# ─────────────────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = os.getenv("SYSTEM_PROMPT", """Você é a Astra, uma assistente de voz inteligente e prestativa.
Responda de forma clara, concisa e natural para uma conversa por telefone.
Responda sempre em português brasileiro.
Limite suas respostas a 1-3 frases curtas.
Não use emojis, markdown ou formatação especial.
Sempre escreva numeros por extenso em portugues (ex.: 1930 -> mil novecentos e trinta).
Você tem acesso a ferramentas para obter informações.
Quando o usuário perguntar sobre a hora ou data atual, use a ferramenta `get_current_datetime`.
Quando o usuário pedir para pesquisar na internet, ou quando a informação for temporal/atual e você não tiver certeza, use a ferramenta `search_web`.
Nunca leia JSON, nomes de ferramentas, ou estruturas técnicas em voz alta.
Ao usar uma ferramenta, avise de forma natural e curta que vai verificar.
Se a ferramenta falhar (success=false/erro/timeout), responda de forma humana, empática e objetiva.
Exemplo de tom em falha: \"Não consegui confirmar isso agora, mas posso tentar novamente em seguida.\"
""")


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline Pipecat
# ─────────────────────────────────────────────────────────────────────────────
async def run_bot(websocket: WebSocket, call_id: str) -> None:
    logger.info(f"[{call_id}] Pipeline iniciando (full-duplex)")
    last_user_text = ""

    # ── [MOD_AUDIO_FORK] Captura frame de metadata inicial ────────────────
    # O mod_audio_fork envia um frame WebSocket TEXT (JSON) imediatamente
    # após a conexão, antes de qualquer frame de áudio. Esse frame contém
    # informações da chamada montadas pelo mod_audio_fork.c:
    #
    # Campos fixos (sempre presentes):
    #   uuid, direction, caller, destination,
    #   sip_sample_rate (8000 — taxa nativa da chamada SIP),
    #   ws_sample_rate  (16000 — taxa do áudio enviado via WS após upsample)
    #
    # Campos dinâmicos (opcionais, vindos de variáveis af_meta_* no dialplan):
    #   plan, language, e qualquer outro af_meta_* configurado
    #
    # Exemplo de payload recebido:
    #   {"uuid":"abc-123","direction":"inbound","caller":"1000",
    #    "destination":"510","sip_sample_rate":8000,"ws_sample_rate":16000,
    #    "plan":"premium","language":"pt-BR"}
    #
    # Precisamos consumir este frame ANTES de passar o websocket para o
    # FastAPIWebsocketTransport, pois o transport não sabe que o primeiro
    # frame é JSON de controle — ele tentaria deserializá-lo como PCM e
    # o BridgeSerializer retornaria None (descarte silencioso, sem crash).
    #
    # Estratégia: receive() com timeout de 3s. Se não chegar ou não for
    # JSON válido, seguimos com metadata vazia (chamada funciona normalmente).
    call_metadata: dict = {}
    try:
        first_msg = await asyncio.wait_for(websocket.receive(), timeout=3.0)
        raw = first_msg.get("text") if isinstance(first_msg, dict) else None
        if raw:
            incoming = json.loads(raw)
            if isinstance(incoming, dict):
                # Normaliza campos fixos com fallbacks seguros.
                # sip_sample_rate e ws_sample_rate substituem o antigo "sample_rate".
                call_metadata = {
                    "uuid":            str(incoming.get("uuid")            or call_id),
                    "direction":       str(incoming.get("direction")       or "inbound"),
                    "caller":          str(incoming.get("caller")          or ""),
                    "destination":     str(incoming.get("destination")     or ""),
                    "sip_sample_rate": int(incoming.get("sip_sample_rate") or 8000),
                    "ws_sample_rate":  int(incoming.get("ws_sample_rate")  or 16000),
                }
                # Preserva campos dinâmicos af_meta_* (plan, language, etc.)
                # que o C injeta iterando variáveis de canal prefixadas com "af_meta_".
                for key, val in incoming.items():
                    if key not in call_metadata:
                        call_metadata[key] = val

                logger.info(
                    f"[{call_id}] Metadata recebida: "
                    f"uuid={call_metadata.get('uuid', '?')} "
                    f"caller={call_metadata.get('caller', '?')} "
                    f"destination={call_metadata.get('destination', '?')} "
                    f"direction={call_metadata.get('direction', '?')} "
                    f"sip_sample_rate={call_metadata.get('sip_sample_rate', '?')} "
                    f"ws_sample_rate={call_metadata.get('ws_sample_rate', '?')} "
                    f"plan={call_metadata.get('plan', '?')} "
                    f"language={call_metadata.get('language', '?')}"
                )
            else:
                logger.warning(f"[{call_id}] Metadata JSON não é um objeto — ignorando")
        else:
            logger.warning(f"[{call_id}] Primeiro frame não veio como TEXT (metadata ausente)")
    except asyncio.TimeoutError:
        logger.warning(f"[{call_id}] Timeout aguardando metadata — continuando sem ela")
    except (json.JSONDecodeError, Exception) as e:
        logger.warning(f"[{call_id}] Metadata inválida ou ausente: {e}")


    # ── Debug recorders (apenas se AUDIO_DEBUG_RECORD=1) ────────────────────
    rx_recorder = AudioRecorder(
        path=f"/tmp/rx_{call_id[:8]}.wav",
        sample_rate=16000,   # RX chega em 16kHz (mod_audio_fork faz upsample)
    )
    tx_recorder = AudioRecorder(
        path=f"/tmp/tx_{call_id[:8]}.wav",
        sample_rate=8000,    # TX sai em 8kHz (Cartesia → RTP)
    )

    # ── Transport ──────────────────────────────────────────────────────────
    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            add_wav_header=False,
            serializer=BridgeSerializer(           # ← passa os recorders
                rx_recorder=rx_recorder,
                tx_recorder=tx_recorder,
            ),
            audio_out_10ms_chunks=2,    # era 2 (20ms); 4=40ms dá mais headroom no ring buffer TX do mod_audio_fork
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
            numerals=True,          # converte dígitos para texto nativamente no STT
            interim_results=True,
            endpointing=200,        # 150ms cortava frases em PSTN (micro-pausas do carrier); 200ms mais robusto
            utterance_end_ms="1000", # aguarda 1s de silêncio para confirmar fim de utterance
        ),
    )

    # ── LLM ───────────────────────────────────────────────────────────────
    llm = OpenAILLMService(
        api_key=os.environ["OPENAI_API_KEY"],
        base_url=os.getenv("OPENAI_API_BASE", "https://api.inceptionlabs.ai/v1"),
        model=os.getenv("OPENAI_MODEL", "mercury-2"),
        tools=[*DATETIME_TOOLS, *WEB_SEARCH_TOOLS],
        run_tools=True,
        tool_function_map={
            "get_current_datetime": _get_current_datetime_with_timeout,
            "search_web": _search_web_with_timeout,
        },
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
                    confidence=0.6,     # era 0.7 — PSTN tem SNR 37dB, threshold menor é mais seguro
                    start_secs=0.2,
                    stop_secs=0.3,      # era 0.2 — 200ms cortava fim de palavras em PSTN
                    min_volume=0.4,     # era 0.6 — carrier móvel comprime amplitude
                )
            ),
            # user_mute_strategies removido: TurnTrackingObserver crasha em
            # AudioRawFrame (sem .id) no pipecat 1.1.0, nunca dispara unmute.
        ),
    )
    user_aggregator = ctx_aggregator.user()

    @user_aggregator.event_handler("on_user_turn_stopped")
    async def on_user_turn_stopped(aggregator, *args):
        nonlocal last_user_text
        try:
            transcript = args[-1] if args else None
            if isinstance(transcript, str):
                last_user_text = transcript.strip().lower()
            elif transcript is not None:
                text = getattr(transcript, "text", None)
                if isinstance(text, str):
                    last_user_text = text.strip().lower()
                else:
                    last_user_text = str(transcript).strip().lower()
        except Exception:
            pass

    # ── Pipeline ──────────────────────────────────────────────────────────
    # [FIX #2] ctx_aggregator.user() e ctx_aggregator.assistant() são MÉTODOS
    #          (não propriedades). Devem ser chamados com parênteses para
    #          retornar as instâncias dos aggregators que o Pipeline espera.
    pipeline = Pipeline([
        transport.input(),
        stt,
        user_aggregator,              # agregado de usuário com handlers registrados
        llm,
        LLMErrorFallbackProcessor(),
        ToolCallTextFallbackProcessor(
            get_search_query_hint=lambda: _best_search_query_from_context(context, last_user_text)
        ),
        NumberNormalizationProcessor(),
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

    @llm.event_handler("on_function_calls_started")
    async def on_function_calls_started(service, function_calls):
        await task.queue_frame(
            TTSSpeakFrame("Só um instante, vou confirmar isso para você.")
        )

    @llm.event_handler("on_error")
    async def on_llm_error(processor, error):
        msg = (getattr(error, "error", "") or "").lower()
        is_server_error = "server had an error" in msg or "error during completion" in msg
        context_user_text = _extract_latest_user_text(context)
        intent_text = f"{last_user_text} {context_user_text}".strip()
        asks_time = bool(
            re.search(r"\b(hora|horário|horas|data|dia de hoje|que dia)\b", intent_text)
        )
        asks_web = bool(
            re.search(
                r"\b(presidente|quem é|quem foi|qual é|pesquis|internet|atual|hoje|agora|notícia|preço|cotação)\b",
                intent_text,
            )
        )

        if is_server_error and asks_time:
            result = await _get_current_datetime_with_timeout()
            if result.get("success"):
                when = _human_datetime_from_tool_result(result.get("result"))
                await task.queue_frame(
                    TTSSpeakFrame(f"Claro. Neste momento, são {when}.")
                )
            else:
                await task.queue_frame(
                    TTSSpeakFrame(
                        "No momento não consegui acessar essa informação, mas posso tentar novamente em seguida."
                    )
                )
            return

        if is_server_error and asks_web:
            query = _best_search_query_from_context(context, last_user_text)
            result = await _search_web_with_timeout(query=query, num_results=3, hl="pt-BR", gl="br")
            if result.get("success"):
                speech = _human_web_search_from_tool_result(result.get("result"))
                await task.queue_frame(TTSSpeakFrame(speech))
            else:
                await task.queue_frame(
                    TTSSpeakFrame(
                        "No momento não consegui pesquisar isso na internet, mas posso tentar novamente em instantes."
                    )
                )
            return

        if is_server_error:
            await task.queue_frame(
                TTSSpeakFrame(
                    "Tive uma instabilidade rápida agora e não consegui responder. Se quiser, pode repetir que eu tento novamente."
                )
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
        rx_recorder.close()   # ← salva rx_<call_id>.wav
        tx_recorder.close()   # ← salva tx_<call_id>.wav
        await task.cancel()

    async def _disconnect_watcher():
        """Fallback: polling do WebSocketState caso o event handler não dispare."""
        while websocket.client_state != WebSocketState.DISCONNECTED:
            await asyncio.sleep(0.3)
        logger.info(f"[{call_id}] WS DISCONNECTED detectado pelo watcher — cancelando pipeline")
        rx_recorder.close()   # ← salva rx_<call_id>.wav (fallback)
        tx_recorder.close()   # ← salva tx_<call_id>.wav (fallback)
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
        rx_recorder.close()  # garante gravação em qualquer caminho de saída
        tx_recorder.close()  # garante gravação em qualquer caminho de saída
        logger.info(f"[{call_id}] Pipeline encerrada")


# ─────────────────────────────────────────────────────────────────────────────
# FastAPI
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Astra Voice Bot")


@app.websocket("/ws/{call_id}")
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