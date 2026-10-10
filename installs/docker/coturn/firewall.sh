#!/usr/bin/env bash
set -euo pipefail
# This service exclusively owns inet vf_coturn. Never flush the host ruleset.
case "${1:-}" in
  stop)
    if nft list table inet vf_coturn >/dev/null 2>&1; then
      nft delete table inet vf_coturn
    fi
    exit 0 ;;
  start|check) ;;
  *) echo 'Usage: firewall.sh start|check|stop' >&2; exit 2 ;;
esac
python3 /opt/verdantflare/coturn/validate.py
rules=$(mktemp)
trap 'rm -f "$rules"' EXIT
if nft list table inet vf_coturn >/dev/null 2>&1; then
  echo 'delete table inet vf_coturn' > "$rules"
fi
cat >> "$rules" <<EOF
table inet vf_coturn {
  chain input {
    type filter hook input priority -10; policy accept;
    ip daddr $TURN_LISTEN_IP tcp dport 3478 ip saddr { $TURN_LAN_CIDR, $TURN_POD_CIDR } counter accept
    ip daddr $TURN_LISTEN_IP udp dport 3478 ip saddr { $TURN_LAN_CIDR, $TURN_POD_CIDR } counter accept
    ip daddr $TURN_LISTEN_IP tcp dport 3478 counter drop
    ip daddr $TURN_LISTEN_IP udp dport 3478 counter drop
    ip daddr $TURN_LISTEN_IP udp dport 49160-49259 ip saddr $TURN_LISTEN_IP udp sport 49160-49259 counter accept
    ip daddr $TURN_LISTEN_IP udp dport 49160-49259 counter drop
  }
  chain output {
    type filter hook output priority -10; policy accept;
    ip saddr $TURN_LISTEN_IP udp sport 49160-49259 ip daddr $TURN_LISTEN_IP udp dport 49160-49259 counter accept
    ip saddr $TURN_LISTEN_IP udp sport 49160-49259 counter drop
  }
}
EOF
nft --check --file "$rules"
if [[ "$1" == start ]]; then nft --file "$rules"; fi
