# RUNBOOK-02: Single-cluster bring-up (kind on Mac)

Per ADR-011, the hackathon submission deploys both control plane and sandbox tier
to a single local Kubernetes cluster (kind), with namespace isolation between
`ironbook` and `submissions`. The Terraform that *would* provision a Hetzner ARM
sandbox region (`deploy/terraform/envs/prod/`) ships as IaC deliverable; activate
it later with `terraform apply` once cloud budget exists.

## Prereqs

- macOS, Apple Silicon
- Docker Desktop running
- `brew install kind kubectl`
- Repo cloned, on branch `phase-1-foundation`

## Bring up

```bash
make dev-up                  # creates the kind cluster
make dev                     # applies dev overlay (cert-manager, argocd, gatekeeper, postgres, minio, submission-api, submissions-namespace)
```

## Verify

```bash
KUBECONFIG=$PWD/kubeconfig.local kubectl get nodes        # 3 Ready
KUBECONFIG=$PWD/kubeconfig.local kubectl get ns submissions -o jsonpath='{.metadata.labels}{"\n"}'
# expect: "ironbook.io/sandbox":"true", pod-security: restricted

KUBECONFIG=$PWD/kubeconfig.local kubectl -n submissions get resourcequota,limitrange,networkpolicy
# expect: 16-pod quota, 8 vCPU / 8 GiB requests, deny-cross-ns-default NetworkPolicy

KUBECONFIG=$PWD/kubeconfig.local kubectl -n ironbook get pods
# expect: postgres-0, minio-0, submission-api Running
```

## Tear down

```bash
make dev-down                # deletes the kind cluster + kubeconfig.local
```

## Activating the second region (future)

When cloud budget exists (Hetzner ~€10/mo, Oracle Cloud Always Free, etc.):

```bash
cd deploy/terraform/envs/prod
cp terraform.tfvars.example terraform.tfvars
$EDITOR terraform.tfvars      # fill in hcloud_token + hcloud_ssh_key_id
terraform init
terraform apply

# Bring up Wireguard (configs generated under .wg/)
sudo cp .wg/control/wg-quick.conf /etc/wireguard/ironbook.conf
sudo wg-quick up ironbook

# Pull the Hetzner k3s kubeconfig
SANDBOX_IP=$(terraform output -raw sandbox_public_ipv4)
ssh root@$SANDBOX_IP 'cat /etc/rancher/k3s/k3s.yaml' \
  | sed "s|127.0.0.1|10.99.0.2|" > kubeconfig.sandbox

# Activate gVisor (Layer 2) — see RUNBOOK-06.
```

The operator and manifests are unchanged; the `submissions` workloads start
landing in the Hetzner k3s cluster instead of the local kind `submissions`
namespace once `kubectl` contexts are switched.
