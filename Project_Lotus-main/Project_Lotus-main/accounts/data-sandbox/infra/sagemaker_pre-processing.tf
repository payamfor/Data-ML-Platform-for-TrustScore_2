locals {
  preprocessing_image_uri = var.preprocessing_image_uri

  # CSV file inside the Terraform module/repository (kept as requested)
  preprocessing_input_file      = "${path.module}/telecom_fraud_prediction_100_rows_test_data.csv"
  preprocessing_input_file_name = "telecom_fraud_prediction_100_rows_test_data.csv"

  # S3 location where the CSV will be uploaded
  preprocessing_input_s3_uri  = "s3://lotus-sandbox-bronze-landing-data/opt/ml/processing/input"
  preprocessing_output_s3_uri = "s3://lotus-sandbox-sagemaker-data"
  preprocessing_job_name      = "lotus-sandbox-telecom-fraud-preprocessing-demo3"

  # New local Parquet file paths
  test_parquet_local       = "${path.module}/sample-training-data/problem=fraud_detection/testing/fraud/NBC-v1.0/dt=2026-06-01/data/part-00000.parquet"
  train_parquet_local      = "${path.module}/sample-training-data/problem=fraud_detection/training/non-fraud/time_window=2026-04-01_00-00-00/data/part-00000.parquet"
  validation_parquet_local = "${path.module}/sample-training-data/problem=fraud_detection/validation/fraud/NBC-v1.0/dt=2026-05-01/data/part-00000.parquet"

  # Base S3 destination for parquet files
  parquet_s3_base_uri = "s3://lotus-sandbox-sagemaker-data/mock-data/version1"
}


resource "terraform_data" "telecom_fraud_preprocessing" {

  # Re-run when either the Docker image or any of the input files changes
  triggers_replace = [
    local.preprocessing_image_uri,
    filemd5(local.preprocessing_input_file),
    filemd5(local.test_parquet_local),
    filemd5(local.train_parquet_local),
    filemd5(local.validation_parquet_local)
  ]

  provisioner "local-exec" {

    interpreter = ["/bin/bash", "-c"]

    command = <<-EOT
      set -e

      echo "=========================================="
      echo "Starting SageMaker Processing Job"
      echo "=========================================="

      echo "Job name: ${local.preprocessing_job_name}"
      echo "Image: ${local.preprocessing_image_uri}"

      echo "Local CSV:"
      echo "${local.preprocessing_input_file}"

      echo "S3 Input:"
      echo "${local.preprocessing_input_s3_uri}"

      echo "S3 Output:"
      echo "${local.preprocessing_output_s3_uri}"

      echo ""
      echo "Checking local files..."

      if [ ! -f "${local.preprocessing_input_file}" ]; then
        echo "ERROR: CSV file not found:"
        echo "${local.preprocessing_input_file}"
        exit 1
      fi

      if [ ! -f "${local.test_parquet_local}" ]; then
        echo "ERROR: Test parquet file not found:"
        echo "${local.test_parquet_local}"
        exit 1
      fi

      if [ ! -f "${local.train_parquet_local}" ]; then
        echo "ERROR: Train parquet file not found:"
        echo "${local.train_parquet_local}"
        exit 1
      fi

      if [ ! -f "${local.validation_parquet_local}" ]; then
        echo "ERROR: Validation parquet file not found:"
        echo "${local.validation_parquet_local}"
        exit 1
      fi

      echo "All local files found."

      echo ""
      echo "Uploading CSV to S3..."

      aws s3 cp \
        "${local.preprocessing_input_file}" \
        "${local.preprocessing_input_s3_uri}/${local.preprocessing_input_file_name}" \
        --region ca-central-1

      echo "CSV upload completed."

      echo ""
      echo "Uploading Parquet files to S3 (${local.parquet_s3_base_uri})..."

      echo "Uploading test/data.parquet..."
      aws s3 cp \
        "${local.test_parquet_local}" \
        "${local.parquet_s3_base_uri}/test/data.parquet" \
        --region ca-central-1

      echo "Uploading train/data.parquet..."
      aws s3 cp \
        "${local.train_parquet_local}" \
        "${local.parquet_s3_base_uri}/train/data.parquet" \
        --region ca-central-1

      echo "Uploading validation/data.parquet..."
      aws s3 cp \
        "${local.validation_parquet_local}" \
        "${local.parquet_s3_base_uri}/validation/data.parquet" \
        --region ca-central-1

      echo "Parquet uploads completed."

      echo ""
      echo "Starting SageMaker Processing Job..."

      aws sagemaker create-processing-job \
        --region ca-central-1 \
        --processing-job-name "${local.preprocessing_job_name}" \
        --role-arn "arn:aws:iam::162591926854:role/lotus-sandbox-sagemaker-execution" \
        --app-specification '{
          "ImageUri": "${local.preprocessing_image_uri}"
        }' \
        --processing-resources '{
          "ClusterConfig": {
            "InstanceCount": 1,
            "InstanceType": "ml.m5.xlarge",
            "VolumeSizeInGB": 30
          }
        }' \
        --processing-inputs '[
          {
            "InputName": "input",
            "S3Input": {
              "S3Uri": "${local.preprocessing_input_s3_uri}/",
              "LocalPath": "/opt/ml/processing/input",
              "S3DataType": "S3Prefix",
              "S3InputMode": "File",
              "S3CompressionType": "None"
            }
          }
        ]' \
        --processing-output-config '{
          "Outputs": [
            {
              "OutputName": "output",
              "S3Output": {
                "S3Uri": "${local.preprocessing_output_s3_uri}",
                "LocalPath": "/opt/ml/processing/output",
                "S3UploadMode": "EndOfJob"
              }
            }
          ]
        }' \
        --stopping-condition '{
          "MaxRuntimeInSeconds": 3600
        }'

      echo ""
      echo "=========================================="
      echo "SageMaker Processing Job submitted"
      echo "=========================================="

    EOT
  }
}