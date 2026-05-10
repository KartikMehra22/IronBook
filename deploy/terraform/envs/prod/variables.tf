variable "hcloud_token" {
  type      = string
  sensitive = true
}

variable "hcloud_ssh_key_id" {
  type = number
}

variable "mac_endpoint" {
  type        = string
  description = "control-plane public endpoint host:port for WG (or empty for client-only)"
  default     = ""
}
