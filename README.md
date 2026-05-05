# 📞 Voice AI Pipeline (FreeSWITCH + Streaming AI)
Diagrama lógico do fluxo de áudio e processamento (com barge-in, tool parallel e jitter control)

Este projeto implementa um pipeline de IA conversacional em tempo real para voz, integrado ao FreeSWITCH via mod_audio_fork, com suporte a full duplex, baixa latência e controle avançado de áudio.
-------------------------------------------------------------------------

### 🧠 Visão Geral da Arquitetura

```
CALL (SIP)
   │
   ▼
┌──────────────────────────────────────────────────────────────┐
│                       FreeSWITCH                             │
│                                                              │
│   ┌──────────────────────────────────────────────────────┐   │
│   │                  mod_audio_fork                      │   │
│   │                                                      │   │
│   │   RTP ↔ PCM                                          │   │
│   │                                                      │   │
│   │   RX: RTP → PCM (16k) ────────────────┐              │   │
│   │                                       │              │   │
│   │   TX: PCM (8k) → RTP (media inject) ◄─┘              │   │
│   │                                                      │   │
│   │   ⚙️ Jitter Buffer (RTP smoothing)                   │   │
│   │   ────────────────────────────────                   │   │
│   │   reorder / delay / loss handling                    │   │
│   │                                                      │   │
│   └───────────────────────────────┬──────────────────────┘   │
└───────────────────────────────────┼──────────────────────────┘
                                    │ WS (PCM binário)
                                    ▼
┌──────────────────────────────────────────────────────────────┐
│                       pipeline.py                            │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │                BridgeSerializer                        │  │
│  │                                                        │  │
│  │  RX: PCM 16k → InputAudioRawFrame (STT)                │  │
│  │  TX: OutputAudioRawFrame 8k → WS → FreeSWITCH          │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ 🎤 BARGE-IN CONTROL                                    │  │
│  │                                                        │  │
│  │  VAD / STT detect speech                               │  │
│  │       │                                                │  │
│  │       ▼                                                │  │
│  │  STOP TTS + limpar buffers                             │  │
│  │  (interrompe áudio em execução)                        │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐      │
│  │ Deepgram STT │──►│ RAG Retriever│──►│ OpenAI LLM   │──┐   │
│  └──────────────┘   └──────┬───────┘   └──────┬───────┘  │   │
│                             │                  │         │   │
│                             ▼                  ▼         │   │
│                    ┌──────────────┐   ┌──────────────┐   │   │
│                    │ vector_store │   │ context_mgr  │   │   │
│                    │ Qdrant       │   │ sliding win  │   │   │
│                    └──────────────┘   └──────────────┘   │   │
│                                             │            │   │
│                                             ▼            │   │
│                                      ┌──────────────┐    │   │
│                                      │ tools.py     │◄───┘   │
│                                      │ (async exec) │        │
│                                      └──────┬───────┘        │
│                                             │                │
│                           🧠 TOOL CALLING (PARALELO)         │
│                           ──────────────────────────         │
│                           não bloqueia o fluxo principal     │
│                                             │                │
│                                             ▼                │
│                                      ┌───────────────┐       │
│                                      │ resultado tool│       │
│                                      └───────────────┘       │
│                                                              │
│                                              ┌──────────────┐│
│                                              │ Cartesia TTS ││
│                                              └──────┬───────┘│
│                                                     │        │
│                       🔁 AUDIO BUFFER (anti-jitter WS)       │
│                       ────────────────────────────────       │
│                       fila + ordenação + flush control       │
│                                                     │        │
└─────────────────────────────────────────────────────┼────────┘
                                                      │
                                                      ▼
┌──────────────────────────────────────────────────────────────┐
│                       FreeSWITCH                             │
│                                                              │
│   reprodução do áudio (já suavizado)                         │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

O fluxo é baseado em um pipeline contínuo de áudio:

```
CALL (SIP)
   ↓
FreeSWITCH (mod_audio_fork - full duplex)
   ↓
pipeline.py (processamento IA)
   ↓
FreeSWITCH (media inject)
```

### 🔄 Fluxo principal
1. O FreeSWITCH recebe a chamada SIP (RTP).
2. O módulo mod_audio_fork envia e recebe áudio via WebSocket full duplex.
3. O pipeline.py processa o áudio com IA:
   * Transcrição (STT)
   * Contexto (RAG)
   * Geração de resposta (LLM)
   * Síntese de voz (TTS)
4. O áudio gerado é retornado ao FreeSWITCH e reproduzido na chamada.
-------------------------------------------------------------------------

### 🧩 Componentes
#### 📡 FreeSWITCH + mod_audio_fork

Responsável por:
* Gerenciar SIP/RTP
* Converter o áudio para PCM
* Enviar/receber o áudio via WebSocket (full duplex)
* Injetar o áudio de resposta na chamada (media inject)

Recursos importantes:
* Full duplex real (entrada e saída simultânea)
* Jitter buffer RTP para suavização de áudio
* Integração direta com o pipeline de IA
-------------------------------------------------------------------------

🔁 BridgeSerializer
Localizado no pipeline.py, é responsável por adaptar o áudio entre os formatos do FreeSWITCH e do pipeline:
* RX (entrada):
  * PCM 16kHz → InputAudioRawFrame (usado pelo STT)
* TX (saída):
  * OutputAudioRawFrame 8kHz → bytes → WebSocket → FreeSWITCH

Esse componente garante compatibilidade entre:
* STT (16kHz)
* TTS / RTP (8kHz)
-------------------------------------------------------------------------

## 🧠 Pipeline de IA (pipeline.py)
Responsável por todo o processamento inteligente do áudio.
Etapas:
* STT (Speech-to-Text)
  * Converte o áudio em texto em tempo real
* RAG (Retrieval Augmented Generation)
  * Recupera contexto relevante (ex: base vetorial)
* LLM (Large Language Model)
  * Gera a resposta baseada no contexto
* TTS (Text-to-Speech)
  * Converte texto em áudio
-------------------------------------------------------------------------

## ⚙️ Funcionalidades Avançadas
## 🎤 Barge-in (interrupção de fala)

Permite que o usuário interrompa o bot enquanto ele está falando.
Como funciona:
* Detecta nova fala via VAD/STT
* Interrompe imediatamente o TTS em execução
* Limpa buffers de áudio
* Prioriza a nova entrada do usuário
👉 Resultado: interação mais natural e responsiva
-------------------------------------------------------------------------

## 🧠 Tool Calling em Paralelo

Permite que o LLM execute ferramentas externas sem bloquear o fluxo de áudio.
Exemplos de ferramentas:
* APIs externas
* Banco de dados
* Serviços internos

Comportamento:
* A chamada é executada de forma assíncrona
* O TTS pode continuar (ex: “vou verificar isso…”)
* A resposta final é integrada quando pronta
👉 Evita pausas e melhora a experiência do usuário
-------------------------------------------------------------------------

## 🔁 Controle de Jitter e Buffer de Áudio

Garante estabilidade no áudio mesmo com variações de rede.
Níveis de controle:
### 1. FreeSWITCH (RTP)
* Jitter buffer nativo
* Reordenação de pacotes
* Compensação de atraso

### 2. Pipeline (WebSocket)
* Buffer de áudio
* Controle de fila
* Flush e ordenação

👉 Resultado:
* Áudio contínuo
* Redução de cortes e glitches
-------------------------------------------------------------------------

## 🎯 Objetivos do Projeto
* Baixa latência em conversação por voz
* Comunicação full duplex em tempo real
* Integração eficiente com modelos de IA
* Arquitetura modular e escalável
* Experiência de usuário natural (human-like)
-------------------------------------------------------------------------

## 🚀 Próximos Passos
* Streaming parcial (redução de latência)
* Multi-agentes (roteamento inteligente)
* Otimização de custo de tokens
* Deploy em ambiente distribuído (Kubernetes)
-------------------------------------------------------------------------

## 📌 Observações
* O WebSocket é gerenciado diretamente pelo mod_audio_fork
* O pipeline atua como consumidor/produtor de áudio
* A arquitetura foi desenhada para ambientes de produção com VoIP
