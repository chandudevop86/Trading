variable "kubeconfig_path" {
  type        = string
  description = "Path to kubeconfig file"
  default     = "~/.kube/config"
}

variable "kube_context" {
  type        = string
  description = "Kubernetes context to use"
  default     = ""
}

variable "namespace" {
  type        = string
  default     = "krsh"
}

variable "app_name" {
  type        = string
  default     = "krsh-app"
}

variable "image" {
  type        = string
  default     = "krsh-trading:latest"
}

variable "replicas" {
  type        = number
  default     = 1
}

variable "host" {
  type        = string
  default     = "krsh.local"
}

variable "telegram_token" {
  type        = string
  default     = ""
  sensitive   = true
}

variable "telegram_chat_id" {
  type        = string
  default     = ""
  sensitive   = true
}

variable "dhan_client_id" {
  type        = string
  default     = ""
  sensitive   = true
}

variable "dhan_access_token" {
  type        = string
  default     = ""
  sensitive   = true
}
