output "subnet" {
  value = var.subnet
}

output "addresses" {
  value = local.ip_for
}

output "configs_dir" {
  value     = ".wg"
  sensitive = true
}
