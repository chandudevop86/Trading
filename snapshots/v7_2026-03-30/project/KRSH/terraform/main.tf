resource "kubernetes_namespace" "krsh" {
  metadata {
    name = var.namespace
  }
}

resource "kubernetes_config_map_v1" "krsh" {
  metadata {
    name      = "krsh-config"
    namespace = kubernetes_namespace.krsh.metadata[0].name
  }

  data = {
    STREAMLIT_SERVER_PORT    = "8501"
    STREAMLIT_SERVER_ADDRESS = "0.0.0.0"
    EXECUTION_MODE           = "PAPER"
    AWS_DEFAULT_REGION       = "ap-south-1"
  }
}

resource "kubernetes_secret_v1" "krsh" {
  metadata {
    name      = "krsh-secrets"
    namespace = kubernetes_namespace.krsh.metadata[0].name
  }

  type = "Opaque"

  string_data = {
    TELEGRAM_TOKEN    = var.telegram_token
    TELEGRAM_CHAT_ID  = var.telegram_chat_id
    DHAN_CLIENT_ID    = var.dhan_client_id
    DHAN_ACCESS_TOKEN = var.dhan_access_token
  }
}

resource "kubernetes_deployment_v1" "krsh" {
  metadata {
    name      = var.app_name
    namespace = kubernetes_namespace.krsh.metadata[0].name
    labels = {
      app = var.app_name
    }
  }

  spec {
    replicas = var.replicas

    selector {
      match_labels = {
        app = var.app_name
      }
    }

    template {
      metadata {
        labels = {
          app = var.app_name
        }
      }

      spec {
        container {
          image             = var.image
          image_pull_policy = "IfNotPresent"
          name              = var.app_name

          port {
            container_port = 8501
          }

          env_from {
            config_map_ref {
              name = kubernetes_config_map_v1.krsh.metadata[0].name
            }
          }

          env_from {
            secret_ref {
              name = kubernetes_secret_v1.krsh.metadata[0].name
            }
          }

          readiness_probe {
            http_get {
              path = "/"
              port = 8501
            }
            initial_delay_seconds = 20
            period_seconds        = 10
          }

          liveness_probe {
            http_get {
              path = "/"
              port = 8501
            }
            initial_delay_seconds = 30
            period_seconds        = 15
          }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "krsh" {
  metadata {
    name      = "krsh-service"
    namespace = kubernetes_namespace.krsh.metadata[0].name
  }

  spec {
    selector = {
      app = var.app_name
    }

    port {
      port        = 80
      target_port = 8501
    }
  }
}

resource "kubernetes_ingress_v1" "krsh" {
  metadata {
    name      = "krsh-ingress"
    namespace = kubernetes_namespace.krsh.metadata[0].name
    annotations = {
      "nginx.ingress.kubernetes.io/proxy-body-size" = "20m"
    }
  }

  spec {
    ingress_class_name = "nginx"

    rule {
      host = var.host
      http {
        path {
          path      = "/"
          path_type = "Prefix"
          backend {
            service {
              name = kubernetes_service_v1.krsh.metadata[0].name
              port {
                number = 80
              }
            }
          }
        }
      }
    }
  }
}
