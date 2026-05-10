terraform {
  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.47"
    }
  }
}

resource "hcloud_server" "vm" {
  name        = var.name
  server_type = var.server_type
  image       = var.image
  location    = var.location
  ssh_keys    = var.ssh_key_ids
  user_data   = var.user_data

  public_net {
    ipv4_enabled = true
    ipv6_enabled = true
  }
}

resource "hcloud_volume" "data" {
  name     = "${var.name}-data"
  size     = var.volume_size_gb
  location = var.location
  format   = "ext4"
}

resource "hcloud_volume_attachment" "data" {
  volume_id = hcloud_volume.data.id
  server_id = hcloud_server.vm.id
  automount = true
}

resource "hcloud_firewall" "fw" {
  name = "${var.name}-fw"

  # SSH from anywhere (key auth only)
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "22"
    source_ips = ["0.0.0.0/0", "::/0"]
  }
  # Wireguard
  rule {
    direction  = "in"
    protocol   = "udp"
    port       = "51820"
    source_ips = ["0.0.0.0/0", "::/0"]
  }
  # k3s API only over WG (no public exposure)

  apply_to {
    server = hcloud_server.vm.id
  }
}
