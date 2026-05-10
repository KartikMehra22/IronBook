variable "name" {
  type = string
}

variable "server_type" {
  type    = string
  default = "cax11" # ARM 2 vCPU 4GB; upgrade to cax21 if needed
}

variable "image" {
  type    = string
  default = "ubuntu-24.04"
}

variable "location" {
  type    = string
  default = "fsn1"
}

variable "ssh_key_ids" {
  type = list(number)
}

variable "volume_size_gb" {
  type    = number
  default = 50
}

variable "user_data" {
  type    = string
  default = ""
}
