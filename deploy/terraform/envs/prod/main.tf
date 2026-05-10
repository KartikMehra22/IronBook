terraform {
  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.47"
    }
  }
}

provider "hcloud" {
  token = var.hcloud_token
}

module "sandbox_vm" {
  source         = "../../modules/hetzner-vm"
  name           = "ironbook-sandbox"
  server_type    = "cax21" # 4 vCPU ARM, 8 GB RAM
  ssh_key_ids    = [var.hcloud_ssh_key_id]
  volume_size_gb = 50
  user_data      = file("${path.module}/cloud-init.yaml")
}

module "wireguard" {
  source = "../../modules/wireguard"
  peers = [
    {
      name        = "control"
      endpoint    = var.mac_endpoint != "" ? var.mac_endpoint : null
      allowed_ips = []
    },
    {
      name        = "sandbox"
      endpoint    = "${module.sandbox_vm.public_ipv4}:51820"
      allowed_ips = ["10.42.0.0/16"]
    },
  ]
}

output "sandbox_public_ipv4" {
  value = module.sandbox_vm.public_ipv4
}

output "sandbox_vm_id" {
  value = module.sandbox_vm.vm_id
}

output "wireguard_addresses" {
  value = module.wireguard.addresses
}
