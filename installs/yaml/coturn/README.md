# Coturn installation assets

Historical cluster-internal verification profile. The approved customer LAN
installation is now [the host Docker profile](../../docker/coturn/README.md).
The initial dev-cluster instance has been retired after host-path verification.

Standalone media relay, maintained with the Runtime installation assets. It does
not run inside the Runtime Go process or require a Runtime image release. Apply
the three manifests together in the target Station namespace, after validation
and promotion to the central environment directory. Do not apply this directory
recursively alongside unrelated Runtime assets.

This initial profile is **cluster internal only**, one replica, IPv4, TURN over
UDP/TCP 3478, UDP allocations 49160–49259. The Service exposes the allocation
listener; allocated relay candidates use the Pod IP and dynamic UDP port, not
the ClusterIP. Both peers must allocate on this same Coturn instance. The peer
deny-all plus own-Pod-IP exception and NetworkPolicy restrict relay traffic to
those allocations. No Ingress, NodePort, hostNetwork, public address or TURN TLS
is installed. LAN/browser reachability is a separate network profile.

Create `coturn-auth` through a controlled Secret workflow. Its `auth.conf` key
must contain `static-auth-secret=<random shared secret>` followed by a newline.
Use a cryptographically random secret of at least 32 bytes. Do not put it in
Git, shell arguments, logs or a ConfigMap. The mounted config must be readable
by group 65534. Keep this Secret separate from Runtime database credentials.

`--use-auth-secret` uses TURN REST credentials (time-limited username and
Base64 HMAC-SHA1 password). Do not combine it with static `--user` accounts.
The application credential issuer is not part of this installation slice.
After secret rotation, coordinate issuers and restart this Deployment; file
updates alone do not prove the server has reloaded the key.

The TCP probes test process availability only. Validate authenticated Allocate,
CreatePermission, ChannelBind and bidirectional payloads with two clients over
both UDP and TCP, plus expired/wrong credentials and forbidden peers. Use
short-lived verification Pods and remove their allocations and resources after
verification. The initial per-user/total allocation caps are 4/16; byte-rate
caps are 5,000,000 per session and 20,000,000 aggregate. These are test limits,
not a production capacity promise. Coturn reserves the per-allocation budget,
so this historical aggregate cap effectively admits only four allocations; it
must not be copied into desktop renewal deployments. The host Docker profile
uses an aggregate budget aligned with its 16-allocation limit. Recreate upgrades
interrupt allocations.

Environment addresses, approval scope, verification evidence and rollback are
owned by the central design repository. Preserve the Secret during rollback;
delete only this installation's Deployment, Service and NetworkPolicy after
draining allocations. Never remove Station storage or other workloads.
