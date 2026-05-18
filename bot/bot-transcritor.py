#!/usr/bin/env python3
"""
bot-transcritor.py — Transcritor de áudio simples (sem LLM/TTS)
================================================================
Objetivo: testar se a transcrição e o áudio chegam corretamente.

Funcionalidades:
  - Recebe áudio do mod_audio_fork via WebSocket (PCM L16 16kHz)
  - Grava o áudio em /tmp/rx_<call_id>.wav
  - Transcreve com Deepgram e imprime no terminal em tempo real
  - Imprime [interim] em amarelo e [FINAL] em verde
  - Não faz nada com o áudio de saída (sem TTS/LLM)

Uso:
  python3 bot-transcritor.py

  No .env precisa de: DEEPGRAM_API_KEY
  Opcional:           STT_MODEL (default: nova-3)
                      LOG_LEVEL (default: INFO)
                      AUDIO_DEBUG_RECORD (default: 1 neste script)
"""

import asyncio
import io
import json
import logging
import os
import wave

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from pipecat.frames.frames import (
    Frame,
    InputAudioRawFrame,
    InterimTranscriptionFrame,
    OutputAudioRawFrame,
    TranscriptionFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.serializers.base_serializer import FrameSerializer
from pipecat.services.deepgram.stt import DeepgramSTTService, LiveOptions
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("Transcritor")

# Cores ANSI
YELLOW = "\033[33m"
GREEN  = "\033[32m"
CYAN   = "\033[36m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


# ─────────────────────────────────────────────────────────────────────────────
# Gravador de áudio WAV
# ─────────────────────────────────────────────────────────────────────────────
class AudioRecorder:
    def __init__(self, path: str, sample_rate: int):
        self._path        = path
        self._sample_rate = sample_rate
        self._buf         = io.BytesIO()
        # Neste script AUDIO_DEBUG_RECORD é sempre 1 por padrão
        self._enabled = os.getenv("AUDIO_DEBUG_RECORD", "1") == "1"
        if self._enabled:
            logger.info(f"AudioRecorder ativo: {path}")

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
                wf.setnchannels(1)
                wf.setsampwidth(2)            # int16
                wf.setframerate(self._sample_rate)
                wf.writeframes(raw)
            duration = len(raw) / 2 / self._sample_rate
            logger.info(f"AudioRecorder salvo: {self._path} ({duration:.2f}s)")
        except Exception as e:
            logger.warning(f"AudioRecorder erro: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Serializer — recebe PCM 16kHz do mod_audio_fork, ignora TX
# ─────────────────────────────────────────────────────────────────────────────
class RxOnlySerializer(FrameSerializer):
    """Deserializa PCM binário vindo do mod_audio_fork. Ignora saída."""

    SAMPLE_RATE = 16_000
    CHANNELS    = 1

    def __init__(self, recorder: AudioRecorder | None = None):
        self._rec = recorder

    async def deserialize(self, data: bytes | str) -> Frame | None:
        if isinstance(data, bytes) and data:
            if self._rec:
                self._rec.write(data)
            return InputAudioRawFrame(
                audio=data,
                sample_rate=self.SAMPLE_RATE,
                num_channels=self.CHANNELS,
            )
        return None

    async def serialize(self, frame: Frame) -> bytes | None:
        # Sem saída de áudio — retorna None para não enviar nada ao RTP
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Processador de transcrição — imprime no terminal
# ─────────────────────────────────────────────────────────────────────────────
class TranscriptPrinter(FrameProcessor):
    """Intercepta TranscriptionFrame e InterimTranscriptionFrame e imprime."""

    def __init__(self, call_id: str):
        super().__init__()
        self._call_id = call_id

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if isinstance(frame, InterimTranscriptionFrame) and frame.text.strip():
            print(
                f"{YELLOW}[interim] {frame.text}{RESET}",
                flush=True,
            )

        elif isinstance(frame, TranscriptionFrame) and frame.text.strip():
            print(
                f"{GREEN}{BOLD}[FINAL]   {frame.text}{RESET}",
                flush=True,
            )

        # Propaga todos os frames normalmente
        await self.push_frame(frame, direction)


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline principal
# ─────────────────────────────────────────────────────────────────────────────
async def run_transcritor(websocket: WebSocket, call_id: str) -> None:
    logger.info(f"[{call_id}] Transcritor iniciando")

    # ── Consome metadata JSON inicial do mod_audio_fork ───────────────────
    call_metadata: dict = {}
    try:
        first_msg = await asyncio.wait_for(websocket.receive(), timeout=3.0)
        raw = first_msg.get("text") if isinstance(first_msg, dict) else None
        if raw:
            incoming = json.loads(raw)
            if isinstance(incoming, dict):
                call_metadata = incoming
                caller = incoming.get("caller", "?")
                dest   = incoming.get("destination", "?")
                uuid   = incoming.get("uuid", call_id)
                print(
                    f"\n{CYAN}{BOLD}═══ CHAMADA CONECTADA ══════════════════════{RESET}",
                    flush=True,
                )
                print(f"{CYAN}  UUID:       {uuid}{RESET}", flush=True)
                print(f"{CYAN}  Caller:     {caller}{RESET}", flush=True)
                print(f"{CYAN}  Destino:    {dest}{RESET}", flush=True)
                print(
                    f"{CYAN}  Áudio RX:   /tmp/rx_{call_id[:8]}.wav{RESET}",
                    flush=True,
                )
                print(
                    f"{CYAN}════════════════════════════════════════════{RESET}\n",
                    flush=True,
                )
    except asyncio.TimeoutError:
        logger.warning(f"[{call_id}] Timeout aguardando metadata")
    except Exception as e:
        logger.warning(f"[{call_id}] Metadata inválida: {e}")

    # ── Gravador ──────────────────────────────────────────────────────────
    recorder = AudioRecorder(
        path=f"/tmp/rx_{call_id[:8]}.wav",
        sample_rate=16000,
    )

    # ── Transport ─────────────────────────────────────────────────────────
    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=False,   # sem saída de áudio
            add_wav_header=False,
            serializer=RxOnlySerializer(recorder=recorder),
            audio_in_sample_rate=16000,
        ),
    )

    # ── STT ───────────────────────────────────────────────────────────────
    stt = DeepgramSTTService(
        api_key=os.environ["DEEPGRAM_API_KEY"],
        live_options=LiveOptions(
            model=os.getenv("STT_MODEL", "nova-3"),
            language="pt-BR",
            encoding="linear16",
            sample_rate=16000,
            channels=1,
            smart_format=True,
            numerals=True,
            interim_results=True,
            endpointing=500,
            utterance_end_ms="1500",
        ),
    )

    # ── Pipeline mínimo: entrada → STT → printer ──────────────────────────
    printer = TranscriptPrinter(call_id=call_id)

    pipeline = Pipeline([
        transport.input(),
        stt,
        printer,
    ])

    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            allow_interruptions=False,
            idle_timeout_secs=300,
        ),
    )

    @transport.event_handler("on_client_disconnected")
    async def on_disconnected(transport, client):
        logger.info(f"[{call_id}] Desconectado")
        recorder.close()
        print(
            f"\n{CYAN}{BOLD}═══ CHAMADA ENCERRADA ══════════════════════{RESET}",
            flush=True,
        )
        await task.cancel()

    async def _watcher():
        while websocket.client_state != WebSocketState.DISCONNECTED:
            await asyncio.sleep(0.3)
        recorder.close()
        await task.cancel()

    watcher = asyncio.create_task(_watcher(), name=f"watcher-{call_id[:8]}")

    try:
        runner = PipelineRunner(handle_sigint=False)
        await runner.run(task)
    except Exception as e:
        logger.error(f"[{call_id}] Erro: {e}")
    finally:
        watcher.cancel()
        await task.cancel()
        recorder.close()
        logger.info(f"[{call_id}] Pipeline encerrada")


# ─────────────────────────────────────────────────────────────────────────────
# FastAPI
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Astra Transcritor")


@app.websocket("/ws/{call_id}")
async def ws_audio(websocket: WebSocket, call_id: str) -> None:
    await websocket.accept()
    logger.info(f"Bridge conectou: {call_id}")
    try:
        await run_transcritor(websocket, call_id)
    except WebSocketDisconnect:
        logger.info(f"[{call_id}] Desconectou (hangup normal)")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": "Astra Transcritor"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("BOT_WS_PORT", "8000"))
    print(f"{CYAN}{BOLD}Transcritor iniciando na porta {port}...{RESET}")
    print(f"{CYAN}Ctrl+C para parar.{RESET}\n")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
