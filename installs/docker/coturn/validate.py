#!/usr/bin/env python3
"""Validate non-secret host relay settings before shell/nft interpolation."""
import ipaddress
import os
import re

listen = ipaddress.IPv4Address(os.environ['TURN_LISTEN_IP'])
lan = ipaddress.IPv4Network(os.environ['TURN_LAN_CIDR'])
pods = ipaddress.IPv4Network(os.environ['TURN_POD_CIDR'])
if not listen.is_private or listen.is_loopback or listen.is_link_local or listen.is_unspecified:
    raise SystemExit('TURN_LISTEN_IP must be a concrete private LAN address')
if listen not in lan or lan.prefixlen < 16 or pods.prefixlen < 16:
    raise SystemExit('Use explicit LAN and Pod networks, /16 or narrower')
if not lan.is_private or not pods.is_private or lan.overlaps(pods):
    raise SystemExit('LAN and Pod networks must be distinct private networks')
image = os.environ.get('COTURN_IMAGE', '')
if not re.fullmatch(r'[a-zA-Z0-9./_-]+:[0-9]+\.[0-9]+\.[0-9]+(?:-[a-zA-Z0-9._-]+)?', image):
    raise SystemExit('COTURN_IMAGE requires a fixed semantic version tag')
