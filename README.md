# Voice AI Full-Duplex (FreeSWITCH + Pipecat)

Projeto de voz em tempo real com chamada SIP no FreeSWITCH e pipeline de IA no bot Python.

## O que já está implementado

- Stack Docker com 3 serviços:
1. `redis` (redis-stack-server)
2. `bot` (FastAPI + Pipecat)
3. `freeswitch` (com `mod_audio_fork` custom compilado)

- Fluxo full-duplex em produção local:
1. Chamada SIP entra no FreeSWITCH
2. Dialplan do ramal `509` executa `audio_fork` bloqueante
3. `mod_audio_fork` envia RX por WebSocket para o bot (PCM 16kHz, após upsample)
4. Bot processa com STT -> LLM -> TTS
5. Bot devolve TX por WebSocket (PCM 8kHz)
6. `mod_audio_fork` injeta TX no RTP da chamada

- Bot (`bot/bot.py`) implementado com:
1. FastAPI WebSocket em `/ws/{call_id}`
2. Healthcheck em `/health`
3. `BridgeSerializer` para conversão de frames de áudio
4. STT com Deepgram (`nova-3`, `pt-BR`, `interim_results`)
5. LLM com OpenAI-compatible endpoint
6. TTS com Cartesia (8kHz PCM)
7. VAD com Silero integrado no aggregator
8. Interrupções habilitadas (`allow_interruptions=True`)
9. Encerramento robusto por `on_client_disconnected` + watcher fallback

- Tool calling funcional:
1. Tool de data/hora (`get_current_datetime`)
2. Tool de busca web (`search_web` via SerpAPI)
3. Fallback para evitar vazamento de JSON técnico no TTS
4. Mensagens amigáveis em falhas de tool/LLM

- `mod_audio_fork` custom (`freeswitch/mod_audio_fork_build`) com:
1. App de dialplan `audio_fork`
2. Reconexão configurável (`max_reconnects=N`)
3. Métricas por sessão via API `uuid_audio_fork_stats <uuid>`
4. API de parada `uuid_audio_fork_stop <uuid>`
5. Resampler RX com SpeexDSP (8kHz -> 16kHz)
6. Jitter buffer adaptativo no caminho TX
7. Envio de metadata JSON inicial da chamada para o bot
8. Suporte a headers HTTP extras no handshake WS

- Infra de execução:
1. `freeswitch/entrypoint.sh` aplica conf custom, injeta variáveis e aguarda bot
2. Healthchecks configurados para `redis`, `bot` e `freeswitch`
3. `network_mode: host` para bot e freeswitch

## Arquivos principais

- `docker-compose.yml`
- `bot/bot.py`
- `bot/tools/datetime_tool.py`
- `bot/tools/web_search_tool.py`
- `freeswitch/Dockerfile`
- `freeswitch/entrypoint.sh`
- `freeswitch/dialplan/07_audio_bridge.xml` (rota padrão atual)
- `freeswitch/mod_audio_fork_build/mod_audio_fork.c`
- `freeswitch/mod_audio_fork_build/audio_pipe.cpp`

## Como subir

1. Preencha `.env` com as chaves obrigatórias:
- `DEEPGRAM_API_KEY`
- `OPENAI_API_KEY`
- `OPENAI_API_BASE`
- `OPENAI_MODEL`
- `CARTESIA_API_KEY`
- `SERPAPI_API_KEY`
- `SIGNALWIRE_TOKEN` (build do FreeSWITCH)

2. Suba os containers:

```bash
docker compose up -d --build
```

3. Verifique saúde do bot:

```bash
curl -s http://127.0.0.1:8000/health
```

4. Ligue para o ramal `509` no contexto configurado do FreeSWITCH.

## TODO (pendente de implementação)

- [ ] RAG real (base vetorial, retrieval e injeção de contexto)
- [ ] Integração com Qdrant ou outro vector store
- [ ] Memória conversacional persistente entre chamadas
- [ ] Streaming parcial de resposta para reduzir mais a latência percebida
- [ ] Observabilidade centralizada (dashboards/alertas para métricas do mod_audio_fork e bot)
- [ ] Testes automatizados (unitários e integração fim-a-fim)
- [ ] CI/CD para build/test/publicação das imagens
- [ ] Hardening de segurança (auth mútua no WS, secrets management, políticas de rede)
- [ ] Multi-agentes/roteamento inteligente
- [ ] Deploy distribuído (ex.: Kubernetes)
- [ ] Estratégia de custo (cache, truncamento e controle de tokens)

## Observações

- A rota de dialplan padrão atual é `freeswitch/dialplan/07_audio_bridge.xml`.
- O arquivo `freeswitch/conf/dialplan/06_audio_bridge.xml` permanece no repositório como referência de configuração anterior.
- O README anterior citava componentes não presentes no código atual (como RAG/Qdrant já em execução). Esta versão foi alinhada ao estado real do repositório.
