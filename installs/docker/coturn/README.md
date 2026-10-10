# Host Docker Coturn

Approved host-network installation profile. Runtime owns these reusable assets;
environment addresses and approval/evidence live in the central design repository.
Coturn runs as a separate Docker container supervised by systemd, not inside the
Runtime Go service. No Compose plugin is required.

Prerequisites: root installation session, Docker CLI at `/usr/local/bin/docker`,
systemd, nftables, Python 3, a stable private IPv4 LAN address, known LAN and Pod
CIDRs, and free TCP/UDP 3478 plus UDP 49160–49259. Check existing listeners and
preserve the host ephemeral-port reservation; reserve the relay range if it is
not already reserved. No host reboot or Docker/Cilium restart is needed.

Install `run.sh`, `firewall.sh`, and `validate.py` as root-owned files under
`/opt/verdantflare/coturn/` (scripts 0755, Python 0644). Install the two service
units under `/etc/systemd/system/`. Create `/etc/verdantflare/coturn/` owned by
root:65534, mode 0750. Copy the environment example to `coturn.env`, replace its
addresses with approved environment values, and keep it root-owned, mode 0640.
The env file contains no credentials.

Provision `auth.conf` through the controlled credential workflow, root:65534,
mode 0640, containing `static-auth-secret=<random shared secret>` and a newline.
Keep shared secrets out of command lines, Git, ConfigMaps and logs. An existing
secret must not be overwritten implicitly. The credential issuer must use the
same secret; only time-limited HMAC credentials may reach browser/worker clients.

Load the fixed `COTURN_IMAGE` once with `docker pull` from the approved registry,
verify the version/digest and retain it locally. Startup uses `--pull=never` so
reboots and local operation do not depend on Internet access. This does not
replace the full offline Station acceptance test.

Validate shell syntax, Python settings, systemd units and nft rules before start.
Then run `systemctl daemon-reload` and
`systemctl enable --now verdantflare-coturn.service`. Its dependency installs the
firewall first; firewall startup failure prevents the container from starting.
Inspect service/container state and authenticated relay behavior. systemd owns
restart policy; the foreground container uses `--rm` and no Docker restart policy.

The firewall owns only `inet vf_coturn`. It never flushes host/Cilium rules.
Only approved LAN/Pod sources may use 3478. Both endpoints must allocate on this
same server: relay packets are limited to this host's own 49160–49259 range.
Coturn also denies all peer IPs except its configured LAN address. The output
port restriction prevents that IP exception from reaching unrelated host UDP
services. Other services and other interfaces are not exposed or reconfigured.
Pods still require valid credentials; a source CIDR is not application identity.

Verify UDP/TCP Allocate, Permission, ChannelBind, bidirectional payloads between
an external LAN client and a real worker Pod, expired/wrong credentials, forbidden
peers and host peer-port isolation. Then verify browser ICE/data transport.
Successful TURN checks do not by themselves prove Blender video/input/save.

The 16-allocation limit reserves up to 5,000,000 bytes/s per allocation, so
`bps-capacity` is 80,000,000 bytes/s. Coturn reserves this budget at Allocate;
it is not only a meter of current traffic. A 20,000,000 budget admits only four
such allocations and can reject browser gathering or renewal with 486
`Allocation Bandwidth Quota Reached`, even while actual traffic is low.
Keep the per-user limit at four and test successive desktop renewals. This is
local relay capacity, not a promise of public relay service or Internet bandwidth.

For upgrade or secret rotation, drain GUI sessions, pull/verify the new fixed
image or coordinate the new secret with issuers, then restart only
`verdantflare-coturn.service`. Allocations are not preserved across restarts.
Rollback: stop/disable that service first; stop/disable its firewall service only
after the container is gone. Preserve `auth.conf` and Station data. Restore the
previous approved assets/image if needed; do not restart unrelated runtimes.

The former `installs/yaml/coturn/` profile is retained solely for isolated
cluster-internal validation. It is not the customer LAN installation path.
