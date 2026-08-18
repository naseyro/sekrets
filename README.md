# SecretsManager

SecretsManager is a Kubernetes controller that injects your secrets into your workloads and rolls them the moment a secret changes.

## How to use it

One command deploys the controller using helm:

```bash
helm install secrets-manager oci://ghcr.io/naseyro/charts/secrets-manager \
  --version 0.1.4 \
  -n secrets-manager-system \
  --create-namespace
```

Then it's three small steps:

**1. A Secret:**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: db-credentials
  namespace: staging
type: Opaque
stringData:
  username: "admin"
  password: "supersecretpassword"
```

**2. A workload:**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: frontend-app
  namespace: staging
spec:
  replicas: 1
  selector:
    matchLabels:
      app: frontend
  template:
    metadata:
      labels:
        app: frontend
    spec:
      containers:
        - name: frontend-container
          image: nginx:alpine
```

**3. A SecretsManager:**

```yaml
apiVersion: secrets.management.io/v1
kind: SecretsManager
metadata:
  name: system-secrets-manager
  namespace: dev-team
spec:
  workloads:
    - name: frontend-app
      namespace: staging
      kind: Deployment
      apiVersion: apps/v1
      secrets:
        - name: db-credentials
```

That's it: the secret lands where the workload expects it, and every time the secret changes, the workload rolls with the fresh values automatically.

**What it can do:**

1. **Three ways to inject** such as normal Secret reference. A `volume` (the default; secret files land at `/etc/secrets/<secret-name>`, or wherever you set `mountPath`), `envFrom` (every key becomes an env var), or `env` (one key into one variable, via `envName` + `secretKey`).
2. **No `mountConfig`? No worries** — you get the default volume mount with zero extra typing.
3. **Rotation on change** — the moment a secret changes, the controller rolls the workload so pods pick up the fresh values automatically.
4. **Scheduled rotation (cron)? Not yet** — today rotation is change-driven only; time-based rotation is on the roadmap.

The `SecretsManager` controller supports working with Kubernetes-native PodTemplateSpec workloads, Argo Rollouts and OpenKruise CloneSet.

**WIP**: Let the controller support every in-cluster appropriate workload automatically.

For a walkthrough of all `SecretsManager` capabilities, see `usages/` for `mountConfig`, `env`, `envFrom`, `mountPath`.