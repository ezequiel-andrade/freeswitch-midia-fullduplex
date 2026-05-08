# Makefile — mod_audio_fork (Full-Duplex)
# =========================================
# Dependências: libwebsockets-dev >= 4.x, g++ (C++14), freeswitch-dev
#
# Uso:
#   make              → compila mod_audio_fork.so (release)
#   make dev          → compila com ASan/TSan para detectar races e leaks
#   make install      → copia para o diretório de módulos do FreeSWITCH
#   make clean        → remove artefatos de build
#   make reload       → instala + recarrega via fs_cli (requer FS rodando)
#   make stats UUID=<uuid> → consulta métricas via fs_cli

# ─── Caminhos ────────────────────────────────────────────────────────────────
FS_INCDIR  ?= /usr/include/freeswitch
FS_MODDIR  ?= /usr/lib/freeswitch/mod

# ─── Compiladores ────────────────────────────────────────────────────────────
CC         := gcc
CXX        := g++

# ─── Flags release ───────────────────────────────────────────────────────────
CFLAGS     := -fPIC -O2 -Wall -Wextra -Wno-unused-parameter \
              -I$(FS_INCDIR)

CXXFLAGS   := -std=c++14 -fPIC -O2 -Wall -Wextra -Wno-unused-parameter \
              -I$(FS_INCDIR)

LDFLAGS    := -shared -fPIC \
              -Wl,-soname,mod_audio_fork.so \
              -lwebsockets -lstdc++ -lpthread -lrt

# ─── Flags dev (ASan + TSan — detecta races, use-after-free, leaks) ──────────
# NOTA: ASan e TSan não podem ser usados simultaneamente no mesmo binário.
#       Use SANITIZE=address para memória, SANITIZE=thread para races.
SANITIZE   ?= address
CFLAGS_DEV  = $(CFLAGS)  -O1 -g3 -fsanitize=$(SANITIZE) -fno-omit-frame-pointer
CXXFLAGS_DEV= $(CXXFLAGS) -O1 -g3 -fsanitize=$(SANITIZE) -fno-omit-frame-pointer
LDFLAGS_DEV = $(LDFLAGS) -fsanitize=$(SANITIZE)

# ─── Arquivos-fonte ──────────────────────────────────────────────────────────
TARGET     := mod_audio_fork.so
OBJS       := mod_audio_fork.o audio_pipe.o agc_wrapper.o

# ─── Regras release ──────────────────────────────────────────────────────────
.PHONY: all dev install clean reload check-deps stats

all: check-deps $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^
	@echo ""
	@echo "✔  Compilado: $(TARGET)"
	@echo "   Próximo passo: make install"

mod_audio_fork.o: mod_audio_fork.c audio_pipe.h agc_wrapper.h
	$(CC) $(CFLAGS) -c $< -o $@

audio_pipe.o: audio_pipe.cpp audio_pipe.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

agc_wrapper.o: agc_wrapper.cpp agc_wrapper.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

# ─── Build dev com sanitizers ─────────────────────────────────────────────────
dev: check-deps
	$(CC)  $(CFLAGS_DEV)  -c mod_audio_fork.c -o mod_audio_fork.o
	$(CXX) $(CXXFLAGS_DEV) -c audio_pipe.cpp   -o audio_pipe.o
	$(CXX) $(CXXFLAGS_DEV) -c agc_wrapper.cpp  -o agc_wrapper.o
	$(CXX) $(LDFLAGS_DEV)  -o $(TARGET) $(OBJS)
	@echo ""
	@echo "✔  Build dev (SANITIZE=$(SANITIZE)): $(TARGET)"
	@echo "   Execute com: ASAN_OPTIONS=detect_leaks=1 ./freeswitch ..."

# ─── Install / reload ────────────────────────────────────────────────────────
install: $(TARGET)
	cp $(TARGET) $(FS_MODDIR)/
	@echo "✔  Instalado em $(FS_MODDIR)/$(TARGET)"

reload: install
	fs_cli -x "reload mod_audio_fork" || true

clean:
	rm -f $(OBJS) agc_wrapper.o $(TARGET)

# ─── Consulta métricas em runtime ────────────────────────────────────────────
# Uso: make stats UUID=<uuid-da-chamada>
stats:
	@test -n "$(UUID)" || (echo "Uso: make stats UUID=<uuid>" && exit 1)
	fs_cli -x "uuid_audio_fork_stats $(UUID)"

# ─── Verificação de dependências ─────────────────────────────────────────────
check-deps:
	@echo "Verificando dependências..."
	@test -f $(FS_INCDIR)/switch.h || \
	  (echo "ERRO: $(FS_INCDIR)/switch.h não encontrado. Instale freeswitch-dev." && exit 1)
	@pkg-config --exists libwebsockets 2>/dev/null || \
	  (echo "AVISO: pkg-config não encontrou libwebsockets — continuando mesmo assim")
	@echo "  switch.h      : $(FS_INCDIR)/switch.h ✔"
	@echo "  libwebsockets : OK"
