# Makefile — mod_audio_fork (Full-Duplex)
# =========================================
# Dependências: libwebsockets-dev >= 4.x, g++ (C++14), freeswitch-dev
#
# Uso:
#   make            → compila mod_audio_fork.so
#   make install    → copia para o diretório de módulos do FreeSWITCH
#   make clean      → remove artefatos de build
#   make reload     → instala + recarrega via fs_cli (requer FS rodando)

# ─── Caminhos ────────────────────────────────────────────────────────────────
# Ajuste se o FreeSWITCH foi instalado em local diferente
FS_INCDIR  ?= /usr/include/freeswitch
FS_MODDIR  ?= /usr/lib/freeswitch/mod

# ─── Compiladores e flags ────────────────────────────────────────────────────
CC         := gcc
CXX        := g++

CFLAGS     := -fPIC -O2 -Wall \
              -I$(FS_INCDIR)

CXXFLAGS   := -std=c++14 -fPIC -O2 -Wall \
              -I$(FS_INCDIR)

LDFLAGS    := -shared -fPIC \
              -Wl,-soname,mod_audio_fork.so \
              -lwebsockets -lstdc++ -lpthread

# ─── Arquivos-fonte ──────────────────────────────────────────────────────────
TARGET     := mod_audio_fork.so
OBJS       := mod_audio_fork.o audio_pipe.o

# ─── Regras ──────────────────────────────────────────────────────────────────
.PHONY: all install clean reload check-deps

all: check-deps $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^
	@echo ""
	@echo "✔  Compilado: $(TARGET)"
	@echo "   Próximo passo: make install"

# Parte C do módulo (mod_audio_fork.c usa <switch.h>)
mod_audio_fork.o: mod_audio_fork.c audio_pipe.h
	$(CC) $(CFLAGS) -c $< -o $@

# Parte C++ (AudioPipe com libwebsockets)
audio_pipe.o: audio_pipe.cpp audio_pipe.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

install: $(TARGET)
	cp $(TARGET) $(FS_MODDIR)/
	@echo "✔  Instalado em $(FS_MODDIR)/$(TARGET)"

clean:
	rm -f $(OBJS) $(TARGET)

# Recarrega o módulo em um FreeSWITCH rodando (útil durante desenvolvimento)
reload: install
	fs_cli -x "reload mod_audio_fork" || true

# Verifica dependências antes de compilar
check-deps:
	@echo "Verificando dependências..."
	@test -f $(FS_INCDIR)/switch.h || \
	  (echo "ERRO: $(FS_INCDIR)/switch.h não encontrado. Instale freeswitch-dev." && exit 1)
	@pkg-config --exists libwebsockets 2>/dev/null || \
	  (echo "AVISO: pkg-config não encontrou libwebsockets — continuando mesmo assim")
	@echo "  switch.h : $(FS_INCDIR)/switch.h ✔"