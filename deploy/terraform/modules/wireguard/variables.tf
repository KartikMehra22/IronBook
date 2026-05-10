variable "peers" {
  description = "list of {name, endpoint, allowed_ips}"
  type = list(object({
    name        = string
    endpoint    = optional(string)
    allowed_ips = list(string)
  }))
}

variable "subnet" {
  type    = string
  default = "10.99.0.0/24"
}
