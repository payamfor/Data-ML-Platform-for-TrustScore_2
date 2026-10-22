locals {
  medallion_zones = toset(["bronze-landing", "silver-conformed", "gold-curated"])
}

resource "aws_s3_bucket" "medallion" {
  for_each = local.medallion_zones

  bucket = "${var.project}-${var.environment}-${each.key}-data"

  tags = {
    Name = "${var.project}-${var.environment}-${each.key}-data"
  }
}

resource "aws_s3_bucket_versioning" "medallion" {
  for_each = local.medallion_zones

  bucket = aws_s3_bucket.medallion[each.key].id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "medallion" {
  for_each = local.medallion_zones

  bucket = aws_s3_bucket.medallion[each.key].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "medallion" {
  for_each = local.medallion_zones

  bucket = aws_s3_bucket.medallion[each.key].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

output "bronze_bucket_arn" {
  value = aws_s3_bucket.medallion["bronze-landing"].arn
}

output "silver_bucket_arn" {
  value = aws_s3_bucket.medallion["silver-conformed"].arn
}

output "gold_bucket_arn" {
  value = aws_s3_bucket.medallion["gold-curated"].arn
}

# --- Added Sagemaker Bucket Configuration ---

locals {
  sagemaker_key = "sagemaker"
}

resource "aws_s3_bucket" "sagemaker" {
  bucket = "${var.project}-${var.environment}-${local.sagemaker_key}-data"

  tags = {
    Name = "${var.project}-${var.environment}-${local.sagemaker_key}-data"
  }
}

resource "aws_s3_bucket_versioning" "sagemaker" {
  bucket = aws_s3_bucket.sagemaker.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "sagemaker" {
  bucket = aws_s3_bucket.sagemaker.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "sagemaker" {
  bucket = aws_s3_bucket.sagemaker.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

output "sagemaker_bucket_arn" {
  value = aws_s3_bucket.sagemaker.arn
}