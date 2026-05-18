# Voice AI Full-Duplex (FreeSWITCH + Pipecat)

Projeto de voz em tempo real com FreeSWITCH (SIP/RTP) + bot FastAPI/Pipecat, usando `mod_audio_fork` custom para streaming bidirecional por WebSocket.

## Estado atual (implementado)

1. Stack Docker com `redis`, `bot` e `freeswitch`.
2. Caminho full-duplex em produção local:
- RX (usuário -> bot): RTP 8kHz -> `mod_audio_fork` -> resample SpeexDSP 8k->16k -> WS binário.
- TX (bot -> usuário): WS binário PCM16 8kHz -> ring buffer -> injeção RTP 8kHz.
3. `mod_audio_fork` com:
- app de dialplan `audio_fork`;
- reconexão (`max_reconnects=N`, incluindo `-1`);
- metadata inicial em frame WS `TEXT`;
- headers extras no handshake WS;
- métricas por sessão (`uuid_audio_fork_stats <uuid>`);
- parada operacional (`uuid_audio_fork_stop <uuid>`);
- jitter adaptativo TX e lifecycle seguro de thread/service.
4. Speex AGC/denoise removidos do pipeline (não usados no estado atual).
5. CNG bridge no RX está **opcional e desligado por padrão**:
- `AF_RX_CNG_BRIDGE_ENABLE=0`.

## Bot (`bot/bot.py`)

1. WebSocket FastAPI em `/ws/{call_id}`.
2. Health endpoint em `/health`.
3. Pipeline STT -> LLM -> TTS com Pipecat.
4. Tool calling (`datetime` e `web_search`).
5. Tratamento de encerramento de sessão e fallback operacional.

## Arquivos principais

1. `docker-compose.yml`
2. `bot/bot.py`
3. `bot/tools/datetime_tool.py`
4. `bot/tools/web_search_tool.py`
5. `freeswitch/Dockerfile`
6. `freeswitch/entrypoint.sh`
7. `freeswitch/conf/dialplan/06_audio_bridge.xml`
8. `freeswitch/mod_audio_fork_build/mod_audio_fork.c`
9. `freeswitch/mod_audio_fork_build/audio_pipe.cpp`

## Como subir

1. Preencha `.env` com as chaves necessárias (`DEEPGRAM_API_KEY`, `OPENAI_API_KEY`, `OPENAI_API_BASE`, `OPENAI_MODEL`, `CARTESIA_API_KEY`, `SERPAPI_API_KEY`, `SIGNALWIRE_TOKEN`).
2. Confira toggles de áudio:
- `AF_RX_CNG_BRIDGE_ENABLE=0` (recomendado para evitar artefato metálico no RX).
3. Suba:

```bash
docker compose up -d --build
```

4. Valide bot:

```bash
curl -s http://127.0.0.1:8000/health
```

5. Faça uma chamada para o ramal configurado no dialplan.

## Operação e diagnóstico rápido

1. Snapshot de métricas da sessão:

```bash
fs_cli -x "uuid_audio_fork_stats <uuid>"
```

2. Encerrar uma sessão ativa:

```bash
fs_cli -x "uuid_audio_fork_stop <uuid>"
```
