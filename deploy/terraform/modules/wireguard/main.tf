terraform {
  required_providers {
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
    external = {
      source  = "hashicorp/external"
      version = "~> 2.3"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
  }
}

# Generate WG keypair per peer via local-exec (Terraform has no native WG keygen).
# Requires `wg` (wireguard-tools) on the operator's machine.
resource "null_resource" "wg_keys" {
  for_each = { for p in var.peers : p.name => p }
  provisioner "local-exec" {
    command = <<-EOT
      mkdir -p .wg/${each.key}
      umask 077
      wg genkey | tee .wg/${each.key}/private.key | wg pubkey > .wg/${each.key}/public.key
    EOT
  }
}

# Read previously generated keys back into Terraform state.
data "external" "wg_pub" {
  for_each   = { for p in var.peers : p.name => p }
  depends_on = [null_resource.wg_keys]
  program    = ["bash", "-c", "echo \"{\\\"k\\\":\\\"$(cat .wg/${each.key}/public.key | tr -d '\\n')\\\"}\""]
}

data "external" "wg_priv" {
  for_each   = { for p in var.peers : p.name => p }
  depends_on = [null_resource.wg_keys]
  program    = ["bash", "-c", "echo \"{\\\"k\\\":\\\"$(cat .wg/${each.key}/private.key | tr -d '\\n')\\\"}\""]
}

locals {
  ip_for = { for i, p in var.peers : p.name => cidrhost(var.subnet, i + 1) }
}

resource "local_sensitive_file" "config" {
  for_each = { for p in var.peers : p.name => p }
  filename = ".wg/${each.key}/wg-quick.conf"
  content  = <<-EOT
    [Interface]
    PrivateKey = ${data.external.wg_priv[each.key].result.k}
    Address    = ${local.ip_for[each.key]}/24
    ListenPort = 51820

    %{for other in [for q in var.peers : q if q.name != each.key]}
    [Peer]
    # ${other.name}
    PublicKey  = ${data.external.wg_pub[other.name].result.k}
    AllowedIPs = ${join(",", concat([format("%s/32", local.ip_for[other.name])], other.allowed_ips))}
    %{if other.endpoint != null}Endpoint   = ${other.endpoint}%{endif}
    PersistentKeepalive = 25
    %{endfor}
  EOT
}
