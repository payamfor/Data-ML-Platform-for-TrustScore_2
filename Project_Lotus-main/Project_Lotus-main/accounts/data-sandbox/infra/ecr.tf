# ECR repositories and lifecycle policies for Preprocessing, Training, and Inference
locals {
  ecr_repository_names = {
    preprocessing         = "${var.project}-${var.environment}-preprocessing"
    autoencoder_training  = "${var.project}-${var.environment}-model_autoencoder_training"
    autoencoder_inference = "${var.project}-${var.environment}-model_autoencoder_inference"
  }
}

# Managed resources with force_delete enabled to bypass the "RepositoryNotEmpty" error
resource "aws_ecr_repository" "images" {
  for_each             = local.ecr_repository_names
  name                 = each.value
  image_tag_mutability = "MUTABLE"
  force_delete         = false # <--- This allows Terraform to delete/recreate even if images exist

  # Ensures safety for future updates once created
  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_ecr_lifecycle_policy" "image_lifecycle" {
  for_each   = aws_ecr_repository.images
  repository = each.value.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 10 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 10
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# Outputs for all repositories
output "preprocessing_ecr_repository_url" {
  value       = aws_ecr_repository.images["preprocessing"].repository_url
  description = "Push preprocessing images here, then reference this URI + tag in the SageMaker Processing Job."
}

output "autoencoder_training_ecr_repository_url" {
  value       = aws_ecr_repository.images["autoencoder_training"].repository_url
  description = "Push autoencoder training images here."
}

output "autoencoder_inference_ecr_repository_url" {
  value       = aws_ecr_repository.images["autoencoder_inference"].repository_url
  description = "Push autoencoder inference images here."
}