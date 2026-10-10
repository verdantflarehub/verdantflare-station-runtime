# 5090 Server Initialization Workflow

This is the server initialization workflow for a VerdantFlare Station node. It is
not a GitHub workflow and it does not publish images. Run it from an operator
session with explicit target context and host access.

## Execution order

```text
S0  capture node facts and a rollback snapshot
S1  install/configure NVIDIA container runtime and restart containerd
S2  install the single approved Kubernetes GPU device plugin
S3  initialize /data layout, Local StorageClass and Retain PVs
S4  create namespaces, quotas, service accounts, Secrets, and base datastores/registry (PostgreSQL, etcd)
S5  run gpu-probe Job and model-probe Job, save logs and UUID evidence
S6  install Station Runtime chart and verify 5052/5053 readiness
S7  reconcile one controlled workload through Runtime
S8  run real input and human quality gates; only then change Video MCP routing
```

The workflow stops on the first failed gate. It never changes the global
`kubectl` context, deletes PVCs/models/artifacts, or silently moves to another
node. GPU and model probes are recreated for each image, driver, model manifest,
Node UID, or GPU UUID change.

## Inputs

The operator must provide the node name, explicit Kubernetes context, approved
Runtime image tag, chart version, model manifest, and the controlled Secret names.
Credentials, model weights, and rendered Secret data are inputs to the execution
environment only; they are never committed to this repository.

## Optional local media relay

For approved desktop media deployments, provision the independent host Docker
Coturn profile from [docker/coturn/](docker/coturn/README.md) after S0 network facts
are known. Validate the stable LAN address, Pod/LAN source ranges, reserved relay
ports, existing firewall and controlled REST secret. Start the dedicated firewall
before the container, then verify LAN-to-worker relay and browser ICE. This does
not require restarting Docker, containerd, Cilium or GPU workloads. It does not
prove the desktop video/input quality gate or Internet reachability.

## Evidence per step

Each step writes a timestamped record containing the target context, Node UID,
command/tool versions, manifest or values digest, Pod UID, GPU UUIDs, model
manifest digest, and the result log path. A successful S2 only proves that the
device plugin registered resources; it does not prove that a workload can run.

## Recovery

- S0-S2: restore host/containerd/plugin configuration from the snapshot; do not
  touch Station data.
- S3-S4: leave PVs with `Retain`, preserve `/data/models`, `/data/projects/video`
  and `/data/station/artifacts`.
- S5-S7: release only the Runtime lease and workload created by the current
  operation; preserve probe logs and model files.
- S8: route Video MCP back to the cloud fallback and retain local attempts for
  diagnosis.
