# GEMINI.md — Astra Voice Bot

## 🤖 Visão Geral do Projeto
O **Astra Voice Bot** é um sistema de Inteligência Artificial Conversacional em tempo real integrado à telefonia (SIP). Ele utiliza o **FreeSWITCH** como servidor de mídia e um pipeline de IA baseado no framework **Pipecat** para processar áudio bidirecionalmente com baixa latência.

### 🏗️ Arquitetura Core
1.  **Telefonia (SIP/RTP)**: Gerenciada pelo FreeSWITCH.
2.  **Captura/Injeção de Áudio**: Realizada pelo módulo `mod_audio_fork` (full-duplex via WebSocket).
3.  **Processamento de IA (Bot)**:
    -   **Framework**: Pipecat (v1.1.0+).
    -   **STT**: Deepgram (Modelo: Nova-3) @ 16kHz.
    -   **LLM**: Inception Labs (Modelo: Mercury-2).
    -   **TTS**: Cartesia (Modelo: Sonic-turbo) @ 8kHz.
    -   **Transporte**: WebSockets (FastAPI).

---

## 🛠️ Tecnologias e Versões
-   **Linguagem**: Python 3.10+ (Bot) / C++ (mod_audio_fork).
-   **Media Server**: FreeSWITCH (Custom Build).
-   **Orquestração**: Docker Compose.
-   **Estado/Cache**: Redis.
-   **Principais Bibliotecas**: `pipecat-ai`, `fastapi`, `websockets`.

---

## 📜 Convenções e Regras de Desenvolvimento
1.  **Áudio Full-Duplex**: Toda a comunicação de áudio deve preferencialmente ocorrer via WebSocket através do `mod_audio_fork`. O uso de ESL (`uuid_displace`) para injeção de áudio é desencorajado devido à instabilidade e latência.
2.  **Taxas de Amostragem (Sample Rates)**:
    -   **RX (Bot Input)**: 16kHz Mono (L16 PCM). O `mod_audio_fork` deve fazer o upsample se necessário.
    -   **TX (Bot Output)**: 8kHz Mono (L16 PCM). Padrão para telefonia legada e compatibilidade RTP.
3.  **VAD (Voice Activity Detection)**: Integrado diretamente no pipeline do Pipecat (Silero VAD) para suporte a *Barge-in* (interrupção).
4.  **Gerenciamento de Contexto**: Utilizar `LLMContextAggregatorPair` para manter o histórico da conversa e sincronizar frames do assistente e usuário.
5.  **Atualização de Memória**: Sempre que houver mudanças na arquitetura, fluxos ou bibliotecas core, este arquivo (`GEMINI.md`) **DEVE** ser atualizado.

---

## 🚀 Comandos Úteis
### Docker
```bash
docker-compose build            # Build da stack
docker-compose up -d            # Iniciar em background
docker-compose logs -f bot      # Logs do bot de IA
docker-compose logs -f freeswitch # Logs do servidor de mídia
```

### FreeSWITCH (fs_cli)
```bash
docker exec -it freeswitch fs_cli
# Comandos úteis dentro do fs_cli:
# show channels                 - Ver chamadas ativas
# module_exists mod_audio_fork  - Verificar se o módulo está carregado
# uuid_audio_fork <uuid> stop   - Parar o streaming manualmente
```

---

## 📝 Histórico de Alterações (Log de Atividades)
-   **2026-04-30**: Criação inicial do `GEMINI.md` com mapeamento da arquitetura FreeSWITCH + Pipecat (Full-duplex). Identificada transição de ESL para WebSocket Bidirecional.
-   **2026-04-30**: Implementação de *Parallel Tool Calling* para obter data e hora atual. Criado `bot/tools/datetime_tool.py` e integrado ao `bot/bot.py` via `OpenAILLMService`.
-   **2026-04-30**: Adicionada tratativa de erro e timeout de 7 segundos para as chamadas de ferramentas em `bot/tools/datetime_tool.py`. O `SYSTEM_PROMPT` foi atualizado para instruir o LLM a informar ao usuário em caso de falha da ferramenta.
