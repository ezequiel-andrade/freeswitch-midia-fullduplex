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
