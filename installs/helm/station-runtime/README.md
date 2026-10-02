# Station Runtime Helm chart

This chart packages only the Runtime service, ServiceAccount, least-privilege Role/RoleBinding, Service, probes, and read-only template/config mounts. It does not install PostgreSQL, the NVIDIA device plugin, StorageClass/PV, or application workloads.

```sh
helm lint installs/helm/station-runtime
helm template station-runtime installs/helm/station-runtime \
  --namespace verdantflare-station \
  --values installs/helm/station-runtime/values.yaml
```

The dev environment owns its values and ConfigMaps in `verdantflare-design/deploys/k8s.dev.verdantflarehub.com/`; do not place credentials or model weights in this chart.
