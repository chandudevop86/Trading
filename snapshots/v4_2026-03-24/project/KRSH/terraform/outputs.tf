output "namespace" {
  value = kubernetes_namespace.krsh.metadata[0].name
}

output "service_name" {
  value = kubernetes_service_v1.krsh.metadata[0].name
}

output "ingress_host" {
  value = var.host
}
