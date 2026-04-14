terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

resource "aws_ecr_repository" "vinayak_api" {
  name                 = "${var.name_prefix}-api"
  image_tag_mutability = "MUTABLE"
}

resource "aws_ecr_repository" "vinayak_ui" {
  name                 = "${var.name_prefix}-ui"
  image_tag_mutability = "MUTABLE"
}

resource "aws_cloudwatch_log_group" "vinayak" {
  name              = "/aws/eks/${var.name_prefix}"
  retention_in_days = 30
}
