# IronBook Terraform

Two modules + one env composition.

## Modules

- `modules/hetzner-vm` — Hetzner Cloud server + attached data volume + firewall (SSH + WireGuard ports).
- `modules/wireguard` — Per-peer WireGuard keypair generation + `wg-quick.conf` rendering.

## Environment

- `envs/prod` — composes the modules to provision `ironbook-sandbox` (CAX21 ARM, 4 vCPU / 8 GB / 50 GB volume) and a 2-peer WG mesh (`control` = Mac, `sandbox` = Hetzner).

## One-time setup

1. Create a Hetzner Cloud project and an API token.
2. Upload your SSH public key. Note the numeric ID.
3. Install `terraform` 1.7+ and `wg` (wireguard-tools) on the operator's machine.
4. `cp envs/prod/terraform.tfvars.example envs/prod/terraform.tfvars`
   and fill in the token + ssh key id.

## Apply

```
cd envs/prod
terraform init
terraform apply
```

Outputs:
- `sandbox_public_ipv4` — VM's public IPv4
- `wireguard_addresses` — `{ control = "10.99.0.1", sandbox = "10.99.0.2" }`
- WireGuard configs are written to `.wg/<peer>/wg-quick.conf` (sensitive, gitignored).

## After apply

See `docs/runbooks/02-bring-up-hetzner.md` for the WireGuard handshake + k3s kubeconfig pull procedure.
