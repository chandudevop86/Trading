output "api_repository_url" {
  value = aws_ecr_repository.vinayak_api.repository_url
}

output "ui_repository_url" {
  value = aws_ecr_repository.vinayak_ui.repository_url
}
