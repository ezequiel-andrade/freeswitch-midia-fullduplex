# Audio Bridge — Diagnóstico, Arquitetura e Guia de Implementação

## 1. Diagnóstico: Erros de Módulos no FreeSWITCH

### Causas mais comuns (checklist objetivo)



O que está acontecendo

Seu fluxo está assim:

Call entra no FreeSWITCH
mod_audio_fork conecta no bot ✅
silence_stream://-1 mantém RTP ✅
Bot responde (TTS ok) ✅
ESL (Event Socket) cai ❌



```bash
# Rodar dentro do container FreeSWITCH:
docker exec -it freeswitch bash

# 1. Checar se mod_audio_fork foi compilado
ls -la /usr/lib/freeswitch/mod/mod_audio_fork.so
# SE NÃO EXISTIR: precisa compilar — veja seção 3

# 2. Checar erros de carregamento no log
grep -i "error\|failed\|cannot" /var/log/freeswitch/freeswitch.log | grep -i "mod_audio_fork"

# 3. Testar ESL
fs_cli -x "module_exists mod_audio_fork"
fs_cli -x "module_exists mod_event_socket"

# 4. Checar dependência libwebsockets
ldd /usr/lib/freeswitch/mod/mod_audio_fork.so
# SE "not found": apt-get install libwebsockets-dev

# 5. Verificar event socket rodando
ss -tlnp | grep 8021
# DEVE mostrar: 127.0.0.1:8021


# 6. Testar conectividade ESL
python3 -c "
import socket
s = socket.socket()
s.connect(('127.0.0.1', 8021))
print(s.recv(256))
s.close()
"
# DEVE retornar: b'Content-Type: auth/request\n\n'
```

### Erro: `mod_audio_fork` não carrega
```
WORKAROUND: uuid_displace funciona sem mod_audio_fork.
Para RX sem mod_audio_fork, use mod_record_session gravando para
pipe nomeado + Python lendo o pipe. Não ideal, mas funciona.

SOLUÇÃO DEFINITIVA: compilar mod_audio_fork (veja Dockerfile).
```

### Erro: ESL `+ERR invalid` no uuid_displace
```
CAUSA: UUID incorreto ou chamada não existe mais.
CHECAR:
  fs_cli -x "show channels"          # lista UUIDs ativos
  fs_cli -x "uuid_exists <uuid>"     # verifica se UUID existe
```

### Erro: FIFO bloqueando (TX_MODE=fifo não funciona)
```
DIAGNÓSTICO:
  # Testar se uuid_displace aceita FIFO no seu build de FS
  mkfifo /tmp/test.raw
  # Em outro terminal:
  cat /dev/zero > /tmp/test.raw &
  fs_cli -x "uuid_displace <uuid> start /tmp/test.raw 0 mux"
  # SE der erro, use TX_MODE=swap
```

---

## 2. Arquitetura Detalhada

### Diagrama lógico

```
LIGAÇÃO ENTRANTE
      │
      ▼
┌─────────────────────────────────────────────────────┐
│  FreeSWITCH                                         │
│  ┌─────────────┐    RTP/PCM     ┌────────────────┐  │
│  │  SIP Stack  │◄──────────────►│  Media Engine  │  │
│  │  (mod_sofia)│                │  (RTP stack)   │  │
│  └─────────────┘                └───────┬────────┘  │
│                                         │           │
│                               mod_audio_fork        │
│                               (WebSocket client)    │
└─────────────────────────────────────────────────────┘
                                         │ WS binary frames (L16, 20ms)
                                         ▼
┌─────────────────────────────────────────────────────┐
│  audio-bridge (bridge.py)                           │
│                                                     │
│  ws_handler()                                       │
│  ┌──────────────────────────────────────────────┐   │
│  │  RX loop                    TX loop          │   │
│  │                                              │   │
│  │  WS recv                    deque            │   │
│  │     │                          │             │   │
│  │     ▼                          ▼             │   │
│  │  rx_queue  ────────►  FIFO /dev/shm/         │   │
│  │     │       (futuro)    {uuid}_tx.raw        │   │
│  │     │       FastAPI                          │   │
│  └──────────────────────────────────────────────┘   │
└───────────────────────────────┬─────────────────────┘
                                │ uuid_displace (ESL)
                                ▼
┌─────────────────────────────────────────────────────┐
│  FreeSWITCH                                         │
│  uuid_displace lê FIFO continuamente                │
│  e injeta PCM na chamada (mux com RTP existente)    │
└─────────────────────────────────────────────────────┘
```

### Fluxo RX (entrada)
```
RTP ──► mod_audio_fork ──► WS binary frame (L16, 20ms, 640 bytes)
     ──► ws_handler.message (bytes)
     ──► session.rx_queue.append()
     ──► [futuro: forward_to_processor()]
```

### Fluxo TX (saída)
```
FastAPI TTS (futuro) ──► session.tx_queue.append()
                              │
                         tx_loop_fifo()
                              │
                    os.write(fd, pcm_chunk)  ← cada 100ms
                              │
                         FIFO /dev/shm/audio/{uuid}_tx.raw
                              │
                    uuid_displace (lê FIFO continuamente)
                              │
                         FreeSWITCH ──► RTP ──► Caller
```

### Buffers e filas
```
rx_queue : deque(maxlen=500)  → ~10s de áudio a 20ms/chunk
tx_queue : deque(maxlen=500)  → ~50s de áudio a 100ms/chunk
FIFO     : 64KB kernel buffer → ~2s a 16kHz/16bit
Jitter   : 200ms pre-buffer   → evita underrun no início TX
```

---

## 3. Compilando mod_audio_fork

```bash
# Dentro do container ou VM de build
apt-get update && apt-get install -y \
    git cmake build-essential \
    libwebsockets-dev \
    libfreeswitch-dev

git clone --depth=1 \
    https://github.com/drachtio/drachtio-freeswitch-modules.git
cd drachtio-freeswitch-modules/modules/mod_audio_fork
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release \
         -DFREESWITCH_INCLUDE_DIR=/usr/include/freeswitch
make -j$(nproc)
cp mod_audio_fork.so /usr/lib/freeswitch/mod/

# Recarrega no FS sem reiniciar
fs_cli -x "load mod_audio_fork"
fs_cli -x "module_exists mod_audio_fork"
```

---

## 4. Dialplan: ativando audio_fork

### Via dialplan XML (automático em todas as chamadas)
```xml
<extension name="fork-all" continue="true">
  <condition field="destination_number" expression="^(\d+)$">
    <action application="audio_fork"
            data="ws://127.0.0.1:9998 16000 mono"/>
    <action application="transfer" data="$1 XML internal"/>
  </condition>
</extension>
```

### Via ESL (ativar/desativar por chamada programaticamente)
```python
# Ativa fork em chamada específica
await esl.api(f"uuid_execute {call_uuid} audio_fork ws://127.0.0.1:9998 16000 mono")

# Para fork
await esl.api(f"uuid_execute {call_uuid} audio_fork_stop")
```

### Verificar no FS se fork está ativo
```
fs_cli -x "uuid_getvar <uuid> audio_fork_status"
```

---

## 5. Comandos ESL úteis para debug

```bash
# Conectar ao ESL interativo
fs_cli

# Ver chamadas ativas + UUID
show channels

# Testar uuid_displace com arquivo de 1s de silence
dd if=/dev/zero of=/tmp/s.raw bs=32000 count=1  # 1s @ 16kHz/16bit
uuid_displace <uuid> start /tmp/s.raw 0 mux
uuid_displace <uuid> stop /tmp/s.raw

# Ver se displace está ativo
uuid_getvar <uuid> current_application

# Checar ESL via Python (sem bridge)
python3 -c "
import asyncio, sys
sys.path.insert(0, '.')
from bridge import ESLClient

async def test():
    c = ESLClient('127.0.0.1', 8021, 'ClueCon')
    await c.connect()
    r = await c.api('show channels')
    print(r)
    await c.close()

asyncio.run(test())
"
```

---

## 6. TX: controle de latência e estabilidade

### Fontes de jitter e como mitigar

| Fonte | Causa | Mitigação |
|-------|-------|-----------|
| Event loop bloqueado | I/O síncrono no asyncio | Usar `io_executor` para FIFO writes |
| Drift acumulado | `asyncio.sleep()` não é preciso | Usar `next_tick += interval` em vez de `time.monotonic() + interval` |
| Buffer underrun | tx_queue vazia | Silence padding automático |
| ESL roundtrip lento | Rede congestionada | ESLClient persistente + `asyncio.Lock` |
| FIFO full | FS lendo mais lento que escrita | Backpressure natural do kernel |

### Parâmetros recomendados por caso de uso

```bash
# Baixa latência (STT/TTS rápido):
TX_CHUNK_MS=60
JITTER_BUFFER_MS=120

# Estabilidade máxima (rede instável):
TX_CHUNK_MS=160
JITTER_BUFFER_MS=320

# Padrão equilibrado:
TX_CHUNK_MS=100
JITTER_BUFFER_MS=200
```

---

## 7. Integração FastAPI STT/TTS

### Arquitetura alvo

```
bridge.py                          FastAPI
─────────                          ───────
_connect_processor()  ─── WS ───►  /ws/audio
                                     │ STT
forward_to_processor()               │ TTS
  ws.send(pcm_chunk)                 │
                       ◄── WS ───  ws.send(tts_pcm)
  session.tx_queue.append(tts_pcm)
```

### FastAPI exemplo mínimo

```python
# fastapi_app.py
from fastapi import FastAPI, WebSocket
import asyncio

app = FastAPI()

@app.websocket("/ws/audio")
async def audio_endpoint(ws: WebSocket):
    await ws.accept()
    try:
        async for chunk in ws.iter_bytes():
            # chunk = PCM L16, 20ms, 16kHz
            # ... STT processing ...
            tts_audio = await synthesize(text)  # seu TTS
            if tts_audio:
                await ws.send_bytes(tts_audio)
    except Exception:
        pass
```

### Ativando no bridge

```bash
# No docker-compose.yml:
FASTAPI_ENABLED=true
FASTAPI_URL=ws://fastapi:8000/ws/audio
```

A conexão WS com FastAPI é estabelecida UMA VEZ por sessão e mantida
durante toda a chamada. Não há reconexão por chunk.

---

## 8. Teste rápido sem FreeSWITCH

```bash
# Terminal 1: inicia bridge (sem ESL/FS)
docker compose up audio-bridge

# Terminal 2: simula mod_audio_fork
python3 test_bridge.py --host 127.0.0.1 --port 9998 --duration 30

# Terminal 3: monitora logs
docker logs -f audio-bridge
```

O bridge irá:
- Receber chunks de silence (RX)
- Tentar ESL (vai falhar → TX desabilitado)
- Logar chunks recebidos

Para testar TX completo, FreeSWITCH + ESL precisam estar rodando.





=======================================================================================


# 🤖 Astra Voice Bot — Arquitetura e Funcionamento

## 📌 Visão Geral

O Astra Voice Bot é um sistema de atendimento por voz em tempo real que integra telefonia SIP com um pipeline de IA (STT → LLM → TTS), permitindo conversas naturais via chamadas telefônicas.

A solução utiliza o FreeSWITCH como media server e um pipeline de IA baseado em streaming via WebSocket para processar áudio bidirecionalmente.

---

## 🏗️ Arquitetura

```
           ┌──────────────┐
           │   MicroSIP   │
           └──────┬───────┘
                  │ SIP
                  ▼
           ┌──────────────┐
           │  FreeSWITCH  │
           │              │
           │  mod_audio_fork ───────────────┐
           │              │                 │ WebSocket (audio stream)
           └──────┬───────┘                 ▼
                  │                  ┌──────────────┐
                  │                  │  Audio Bridge │
                  │                  └──────┬───────┘
                  │                         │
                  ▼                         ▼
         RTP (silence_stream)        ┌──────────────┐
                                     │     Bot      │
                                     │              │
                                     │ STT (Deepgram)
                                     │ LLM (Inception)
                                     │ TTS (Cartesia)
                                     └──────────────┘
```

---

## 🔁 Fluxo Completo da Chamada

### 1. 📞 Origem da chamada

* Um softphone (ex: MicroSIP) inicia uma chamada SIP
* A chamada é recebida pelo FreeSWITCH

---

### 2. ☎️ Atendimento da chamada (Dialplan)

O FreeSWITCH executa o dialplan:

```xml
<action application="answer"/>
<action application="set" data="bypass_media=false"/>
<action application="set" data="proxy_media=false"/>
<action application="playback" data="silence_stream://-1"/>
```

#### 🔍 O que isso faz:

* `answer` → atende a chamada
* `bypass_media=false` → força mídia passar pelo FreeSWITCH
* `proxy_media=false` → evita bypass direto
* `silence_stream://-1` → mantém fluxo RTP contínuo

> ⚠️ Isso substitui o uso de `echo()` e mantém o "clock de mídia" ativo

---

### 3. 🎧 Captura de áudio (mod_audio_fork)

O módulo `mod_audio_fork` inicia o streaming:

```bash
uuid_audio_fork <uuid> start ws://127.0.0.1:9998 mono 16000 {...}
```

#### 🔍 Características:

* Captura áudio RTP da chamada (8kHz)
* Faz resample para 16kHz
* Envia via WebSocket para o Audio Bridge
* Utiliza media bug no FreeSWITCH

---

### 4. 🌉 Audio Bridge

Responsável por:

* Receber áudio do FreeSWITCH
* Encaminhar para o Bot
* Receber áudio gerado pelo Bot (TTS)
* (Atualmente) tentar reinjetar via ESL (`uuid_displace`)

---

### 5. 🧠 Pipeline de IA (Bot)

Pipeline executado:

```
Áudio → VAD → STT → LLM → TTS → Áudio
```

#### Componentes:

* **VAD (Silero)** → detecta fala
* **STT (Deepgram)** → converte áudio em texto
* **LLM (Inception)** → gera resposta
* **TTS (Cartesia)** → converte texto em áudio

---

### 6. 🔊 Retorno de áudio

Atualmente:

* O áudio gerado pelo TTS é salvo em `.raw`
* O Audio Bridge tenta injetar via:

```bash
uuid_displace <uuid> start /dev/shm/audio/..._tx.raw 0 mux
```

---

## ⚙️ Componentes Principais

### FreeSWITCH

* Controle da chamada SIP
* Processamento de mídia
* Execução do dialplan
* Integração com `mod_audio_fork`

---

### mod_audio_fork

* Streaming de áudio em tempo real
* Comunicação via WebSocket
* Suporte a bidirecional (parcial no cenário atual)

---

### Audio Bridge

* Intermedia comunicação WS
* Gerencia arquivos de áudio temporários
* Integra com ESL

---

### Bot (Astra)

* Pipeline de IA em tempo real
* Processamento de linguagem natural
* Geração de voz

---

## ⚠️ Problemas Identificados

### 1. ❌ Dependência de ESL (instável)

Erro observado:

```
ConnectionError: ESL closed
```

#### Impacto:

* Comandos `uuid_displace` falham
* Áudio não é injetado de volta

---

### 2. ❌ Uso de `uuid_displace`

Problemas:

* Depende de timing perfeito
* Depende de conexão ESL ativa
* Pode falhar silenciosamente

---

### 3. ⚠️ Arquitetura híbrida (redundante)

Atualmente existem dois fluxos:

1. WebSocket (mod_audio_fork) ✅
2. ESL (`uuid_displace`) ❌

> Isso gera complexidade e instabilidade

---

### 4. ⚠️ Conversão de áudio (8k ↔ 16k)

* Entrada SIP: 8000 Hz (PCMU)
* Bot: 16000 Hz
* Conversão ocorre no FreeSWITCH

Impacto:

* Aumento de CPU
* Possível latência

---

## 🚧 Pendências / Próximos Passos

### 🔴 Prioridade Alta

#### 1. Remover ESL do fluxo de áudio

* Eliminar uso de `uuid_displace`
* Utilizar apenas WebSocket bidirecional

---

#### 2. Implementar áudio bidirecional no `mod_audio_fork`

Objetivo:

* Receber áudio do bot via WS
* Injetar diretamente no canal

---

### 🟡 Prioridade Média

#### 3. Otimizar latência

* Reduzir buffering
* Ajustar chunk size
* Melhorar VAD timing

---

#### 4. Melhorar controle de sessão

* Gerenciar lifecycle da chamada
* Sincronizar estados (call ↔ bot)

---

### 🟢 Prioridade Baixa

#### 5. Padronizar sample rate

Opções:

* Trabalhar tudo em 8k (mais leve)
* Ou migrar para 16k end-to-end

---

## 🚀 Arquitetura Ideal (Futura)

```
FreeSWITCH
   ↓
mod_audio_fork (WS bidirectional)
   ↓
Bot (STT → LLM → TTS)
   ↓
Áudio retorna via WS
   ↓
FreeSWITCH injeta diretamente
```

### ✔ Benefícios:

* Sem ESL
* Menor latência
* Menos pontos de falha
* Arquitetura limpa

---

## 💡 Insights Importantes

* O sistema depende de **fluxo RTP contínuo**
* `silence_stream://-1` mantém o clock de mídia ativo
* `echo()` era apenas workaround
* WebSocket é o caminho mais estável para áudio

---

## 📊 Status Atual

| Componente       | Status     |
| ---------------- | ---------- |
| SIP / Call       | ✅ OK       |
| RTP / Mídia      | ✅ OK       |
| Audio Fork       | ✅ OK       |
| STT              | ✅ OK       |
| LLM              | ✅ OK       |
| TTS              | ✅ OK       |
| Retorno de áudio | ⚠️ Parcial |
| ESL              | ❌ Instável |

---

## 🎯 Conclusão

O sistema já está funcional ponta a ponta, com captura, processamento e geração de áudio em tempo real.

O principal gargalo atual está na **injeção de áudio de volta via ESL**, que deve ser substituída por um modelo totalmente baseado em streaming WebSocket.

---

## 📌 Próximo Marco

> 🎯 Migrar para arquitetura 100% WebSocket (sem ESL)

---

## 👨‍💻 Autor

Projeto desenvolvido por Ezequiel Antônio
Especialista em VoIP, FreeSWITCH e sistemas de voz em tempo real

---

