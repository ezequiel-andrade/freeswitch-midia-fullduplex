#!/bin/bash
set -e

echo "======================================================"
echo "  FreeSWITCH — Full-Duplex Audio Pipeline"
echo "  Base: drachtio/drachtio-freeswitch-mrf"
echo "======================================================"

FS_PREFIX=/usr/local/freeswitch
FS_CONF=/usr/local/freeswitch/conf

# =============================================================================
# 1. IP externo
# =============================================================================
if [ "${EXTERNAL_IP}" = "auto" ]; then
  EXTERNAL_IP=$(curl -s --max-time 5 https://api.ipify.org 2>/dev/null || \
                ip route get 1.1.1.1 | awk '{print $7; exit}' 2>/dev/null || \
                echo "127.0.0.1")
fi
export EXTERNAL_IP
echo "EXTERNAL_IP = ${EXTERNAL_IP}"
echo "INTERNAL_IP = ${INTERNAL_IP:-0.0.0.0}"

# =============================================================================
# 2. Configs customizadas — copia do volume :ro para conf gravável
# =============================================================================
if [ -d "/opt/freeswitch-conf-src" ] && [ "$(ls -A /opt/freeswitch-conf-src 2>/dev/null)" ]; then
  echo "Aplicando configuracoes customizadas..."
  cp -r /opt/freeswitch-conf-src/. "${FS_CONF}/"
else
  echo "Nenhuma conf customizada — usando conf padrao da imagem drachtio."
fi

# Substitui variáveis nos XMLs
find "${FS_CONF}" -name "*.xml" -type f 2>/dev/null | while read -r f; do
  sed -i \
    "s|__EXTERNAL_IP__|${EXTERNAL_IP}|g;
     s|__INTERNAL_IP__|${INTERNAL_IP:-0.0.0.0}|g;
     s|__ESL_PASSWORD__|${ESL_PASSWORD:-ClueCon}|g" \
    "$f"
done

# =============================================================================
# 3. Garante mod_audio_fork habilitado no modules.conf.xml
# =============================================================================
MODULES_CONF="${FS_CONF}/autoload_configs/modules.conf.xml"
if [ -f "${MODULES_CONF}" ]; then
  if ! grep -q "mod_audio_fork" "${MODULES_CONF}"; then
    echo "Habilitando mod_audio_fork..."
    sed -i 's|</modules>|  <load module="mod_audio_fork"/>\n    </modules>|' "${MODULES_CONF}"
  fi
  echo "mod_audio_fork: OK"
fi

# =============================================================================
# 4. Buffers de rede para RTP em tempo real
# =============================================================================
sysctl -w net.core.rmem_max=26214400    2>/dev/null || true
sysctl -w net.core.wmem_max=26214400    2>/dev/null || true
sysctl -w net.core.rmem_default=1048576 2>/dev/null || true
sysctl -w net.ipv4.udp_rmem_min=16384  2>/dev/null || true

# =============================================================================
# 5. /dev/shm/audio para chunks TX
# =============================================================================
mkdir -p /dev/shm/audio
chmod 777 /dev/shm/audio

# =============================================================================
# 6. Aguardar bot (substituiu audio-bridge)
# =============================================================================
BOT_HOST="${BOT_HOST:-127.0.0.1}"
BOT_PORT="${BOT_PORT:-8000}"
echo "Aguardando bot em ${BOT_HOST}:${BOT_PORT}..."
COUNT=0
until nc -z "${BOT_HOST}" "${BOT_PORT}" 2>/dev/null; do
  COUNT=$((COUNT+1))
  if [ "${COUNT}" -ge 30 ]; then
    echo "AVISO: bot nao respondeu em 30s — iniciando assim mesmo."
    break
  fi
  echo "  ... ${COUNT}/30"
  sleep 1
done
[ "${COUNT}" -lt 30 ] && echo "bot OK."

# =============================================================================
# 7. Inicia FreeSWITCH
# =============================================================================
echo ""
echo "Iniciando FreeSWITCH..."
exec "$@"