#!/usr/bin/env bash
set -euo pipefail
python3 /opt/verdantflare/coturn/validate.py
test -r /etc/verdantflare/coturn/auth.conf
docker image inspect "$COTURN_IMAGE" >/dev/null
exec docker run --rm --pull=never \
  --name verdantflare-coturn \
  --label com.verdantflare.component=coturn \
  --network host \
  --user 65534:65534 --read-only \
  --cap-drop ALL --cap-add NET_BIND_SERVICE \
  --security-opt no-new-privileges \
  --cpus 1 --memory 256m --pids-limit 128 \
  --tmpfs /tmp:rw,noexec,nosuid,size=16m,mode=1777 \
  --mount type=bind,src=/etc/verdantflare/coturn/auth.conf,dst=/etc/coturn-auth/auth.conf,readonly \
  --log-opt max-size=10m --log-opt max-file=3 \
  --entrypoint turnserver "$COTURN_IMAGE" \
  -c /etc/coturn-auth/auth.conf \
  --realm=station.internal --fingerprint --use-auth-secret \
  --listening-ip="$TURN_LISTEN_IP" --relay-ip="$TURN_LISTEN_IP" \
  --listening-port=3478 --min-port=49160 --max-port=49259 \
  --denied-peer-ip=0.0.0.0-255.255.255.255 \
  --denied-peer-ip=::-ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff \
  --allowed-peer-ip="$TURN_LISTEN_IP" \
  --no-multicast-peers --no-tcp-relay --no-cli --no-tls --no-dtls \
  --stale-nonce=600 --user-quota=4 --total-quota=16 \
  --max-bps=5000000 --bps-capacity=80000000 \
  --log-file=stdout --simple-log --pidfile=/tmp/turnserver.pid
