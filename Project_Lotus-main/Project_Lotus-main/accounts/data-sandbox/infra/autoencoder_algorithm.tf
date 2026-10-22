# SageMaker Algorithm Resource
# ==============================================
# Dynamic 
# ==============================================
locals {
  training_ecr_repository_name  = "lotus-sandbox-model_autoencoder_training"
  inference_ecr_repository_name = "lotus-sandbox-model_autoencoder_inference"
}

data "aws_ecr_repository" "autoencoder_training" {
  name = local.training_ecr_repository_name
}

data "aws_ecr_repository" "autoencoder_inference" {
  name = local.inference_ecr_repository_name
}

data "aws_ecr_image" "autoencoder_training" {
  repository_name = data.aws_ecr_repository.autoencoder_training.name
  most_recent     = true
}

data "aws_ecr_image" "autoencoder_inference" {
  repository_name = data.aws_ecr_repository.autoencoder_inference.name
  most_recent     = true
}

resource "aws_sagemaker_algorithm" "autoencoder_algorithm" {
  algorithm_name        = "lotus-sandbox-autoencoder-algorithm"
  algorithm_description = "Custom autoencoder model for anomaly detection with training, tuning, inference, and validation specifications."

  # ==========================================
  # STEP 1 & 2: Training & Tuning Specifications
  # ===========================================
  training_specification {
    training_image                    = "${data.aws_ecr_repository.autoencoder_training.repository_url}@${data.aws_ecr_image.autoencoder_training.image_digest}"
    supported_training_instance_types = ["ml.m5.xlarge", "ml.p3.2xlarge"]

    training_channels {
      name                    = "train"
      supported_content_types = ["application/x-parquet", "application/json", "text/csv"]
      description             = "Training channel for autoencoder dataset"
      supported_input_modes   = ["File", "Pipe"]
    }
  }


  # ==========================================
  # STEP 3: Inference Specifications
  # ==========================================
  inference_specification {
    containers {
      image = "${data.aws_ecr_repository.autoencoder_inference.repository_url}@${data.aws_ecr_image.autoencoder_inference.image_digest}"
    }
    supported_realtime_inference_instance_types = ["ml.m5.xlarge"]
    supported_transform_instance_types          = ["ml.m5.xlarge"]
    supported_content_types                     = ["application/x-parquet", "application/json", "text/csv"]
    supported_response_mime_types               = ["application/x-parquet", "application/json", "text/csv"]
  }

  # ==========================================
  # STEP 4: Validation Specifications
  # ==========================================
  validation_specification {
    validation_role = "arn:aws:iam::162591926854:role/lotus-sandbox-sagemaker-execution"

    validation_profiles {
      profile_name = "AutoencoderValidationProfile"

      training_job_definition {
        training_input_mode = "File"

        input_data_config {
          channel_name = "train"
          input_mode   = "File"
          data_source {
            s3_data_source {
              s3_data_type              = "S3Prefix"
              s3_uri                    = "s3://lotus-sandbox-sagemaker-data/mock-data/version1/train/non-fraud/time_window=2026-04-01_00-00-00/data/part-00000.parquet"
              s3_data_distribution_type = "FullyReplicated"
            }
          }
        }

        output_data_config {
          s3_output_path   = "s3://lotus-sandbox-sagemaker-data/demo_model_results/demo1/training_output/"
          compression_type = "NONE"
        }

        resource_config {
          instance_count               = 1
          instance_type                = "ml.m5.xlarge"
          volume_size_in_gb            = 20
          keep_alive_period_in_seconds = 0
        }

        stopping_condition {
          max_runtime_in_seconds      = 1800
          max_pending_time_in_seconds = 7200
          max_wait_time_in_seconds    = 7200 # Updated to be >= max_pending_time_in_seconds
        }
      }

      transform_job_definition {
        max_concurrent_transforms = 1
        max_payload_in_mb         = 6
        batch_strategy            = "MultiRecord"

        transform_input {
          data_source {
            s3_data_source {
              s3_data_type = "S3Prefix"
              s3_uri       = "s3://lotus-sandbox-sagemaker-data/mock-data/version1/test/fraud/NBC-v1/dt=2026-06-01/data/part-00000.parquet"
            }
          }
          content_type = "application/x-parquet"
          split_type   = "Line"
        }

        transform_output {
          s3_output_path = "s3://lotus-sandbox-sagemaker-data/demo_model_results/demo1/testing_output/"
        }

        transform_resources {
          instance_count = 1
          instance_type  = "ml.m5.xlarge"
        }
      }
    }
  }
}