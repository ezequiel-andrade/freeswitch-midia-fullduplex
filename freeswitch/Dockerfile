# =============================================================================
# FreeSWITCH — Full-Duplex mod_audio_fork
# Base: drachtio/drachtio-freeswitch-mrf (FS 1.10 compilado do source)
#
# Multi-stage:
#   Stage builder → compila mod_audio_fork.so
#   Stage runtime → imagem final sem ferramentas de build
#
# POR QUE NÃO MAIS -Bstatic -lwebsockets?
# ─────────────────────────────────────────
# O libwebsockets.a em /usr/local/lib (compilado pelo drachtio) foi gerado
# SEM -fPIC. Uma .a sem -fPIC não pode ser linkada dentro de uma .so:
#
#   relocation R_X86_64_PC32 against `stderr@@GLIBC_2.2.5'
#   can not be used when making a shared object; recompile with -fPIC
#
# SOLUÇÃO: usar a libwebsockets.so instalada via apt (libwebsockets-dev).
# Essa versão:
#   • É compilada COM -fPIC → pode entrar em .so sem problemas
#   • NÃO usa LWS_WITH_EVLIB_PLUGINS → não tenta dlopen(evlib_uv.so)
#   • Está disponível como .so dinâmica → o linker aceita em shared objects
#
# RUNTIME: instala libwebsockets24 (ou libwebsockets19, dependendo da distro)
# no stage final para que o mod_audio_fork.so resolva a dependência.
# =============================================================================

# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM drachtio/drachtio-freeswitch-mrf:latest AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      g++ \
      libwebsockets-dev \
      libspeexdsp-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY mod_audio_fork_build/mod_audio_fork.c  .
COPY mod_audio_fork_build/audio_pipe.cpp    .
COPY mod_audio_fork_build/audio_pipe.h      .
COPY mod_audio_fork_build/agc_limiter.h     .
COPY mod_audio_fork_build/agc_wrapper.h     .
COPY mod_audio_fork_build/agc_wrapper.cpp   .

# ─────────────────────────────────────────────────────────────────────────────
# Compilação com LWS dinâmica do apt (tem -fPIC — necessário para .so)
#
# Flags de include:
#   -I/usr/local/freeswitch/include/freeswitch  → switch.h (drachtio)
#   -I/usr/include                              → libwebsockets.h (apt)
#
# Flags de link:
#   -lwebsockets   → libwebsockets.so do apt (NÃO a .a do drachtio em /usr/local/lib)
#   -lstdc++       → runtime C++ (para audio_pipe.cpp)
#   -lpthread      → pthreads (POSIX threads usadas pelo LWS)
#
# IMPORTANTE: -L/usr/local/lib foi removido intencionalmente.
# Sem ele, o linker usa /usr/lib/x86_64-linux-gnu/libwebsockets.so (apt),
# que é a versão com -fPIC. Se -L/usr/local/lib fosse mantido, o linker
# poderia preferir a .a do drachtio — que causa o erro de -fPIC.
# ─────────────────────────────────────────────────────────────────────────────
RUN g++ -std=c++17 -fPIC -O2 \
        -I/usr/local/freeswitch/include/freeswitch \
        -I/usr/include \
        -c audio_pipe.cpp -o audio_pipe.o \
 && g++ -std=c++17 -fPIC -O2 \
        -I/usr/local/freeswitch/include/freeswitch \
        -I/usr/include \
        -c agc_wrapper.cpp -o agc_wrapper.o \
 && g++ -std=c++17 -fPIC -O2 \
        -I/usr/local/freeswitch/include/freeswitch \
        -I/usr/include \
        -c mod_audio_fork.c -o mod_audio_fork.o \
 && g++ -shared -fPIC \
        -Wl,-soname,mod_audio_fork.so \
        -o mod_audio_fork.so \
        mod_audio_fork.o audio_pipe.o agc_wrapper.o \
        -lwebsockets \
        -lspeexdsp \
        -lstdc++ -lpthread \
 && echo "✔ mod_audio_fork.so compilado" \
 && ldd mod_audio_fork.so | grep websockets

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM drachtio/drachtio-freeswitch-mrf:latest

USER root

# Runtime: instala a libwebsockets.so (sem -dev) para que o módulo resolva
# a dependência em tempo de carga (dlopen pelo FreeSWITCH).
# O pacote correto varia por distro/versão:
#   Debian 11 (bullseye): libwebsockets19
#   Debian 12 (bookworm): libwebsockets24
#   Ubuntu 22.04:         libwebsockets20
# O $(apt-cache ...) detecta automaticamente o nome correto.
RUN apt-get update && apt-get install -y --no-install-recommends \
      netcat-openbsd curl iproute2 sox \
      $(apt-cache search 'libwebsockets[0-9]' 2>/dev/null | awk 'NR==1{print $1}') \
      $(apt-cache search '^libspeexdsp[0-9]+$' 2>/dev/null | awk 'NR==1{print $1}') \
    && rm -rf /var/lib/apt/lists/*

# Instala o módulo compilado no stage anterior
COPY --from=builder /build/mod_audio_fork.so \
                    /usr/local/freeswitch/mod/mod_audio_fork.so

RUN mkdir -p \
      /dev/shm/audio \
      /opt/freeswitch-conf-src \
      /var/log/freeswitch \
      /var/lib/freeswitch/db \
      /var/lib/freeswitch/recordings \
      /var/run/freeswitch

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 5060/udp 5060/tcp
EXPOSE 5080/udp 5080/tcp
EXPOSE 5061/tcp 5081/tcp
EXPOSE 16384-32768/udp
EXPOSE 8021/tcp

ENTRYPOINT ["/entrypoint.sh"]
CMD ["freeswitch", "-nonat", "-nf"]
