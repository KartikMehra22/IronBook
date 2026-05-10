output "public_ipv4" {
  value = hcloud_server.vm.ipv4_address
}

output "public_ipv6" {
  value = hcloud_server.vm.ipv6_address
}

output "vm_id" {
  value = hcloud_server.vm.id
}
