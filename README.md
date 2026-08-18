**SecretsManager** a controller that watches, rotates and mutates secrets into your workloads resulting in a real-time rollout updates once a secret changes.

## Installation
Install using Helm:
```bash
helm install secrets-manager oci://ghcr.io/naseyro/charts/secrets-manager \
  --version 0.1.2 \
  -n secrets-manager-system \
  --create-namespace
```
