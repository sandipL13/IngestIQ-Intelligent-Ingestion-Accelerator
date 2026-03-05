terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = var.region
}

variable "region" { default = "us-east-1" }
variable "project" { default = "ingestiq" }

# S3 Bucket
resource "aws_s3_bucket" "data" {
  bucket = "${var.project}-data-${random_id.suffix.hex}"
}

resource "random_id" "suffix" { byte_length = 4 }

# DynamoDB Tables
resource "aws_dynamodb_table" "run_time_metadata" {
  name         = "TABLE_RUN_TIME_METADATA"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "database_name"
  range_key    = "table_name"
  
  attribute {
    name = "database_name"
    type = "S"
  }
  attribute {
    name = "table_name"
    type = "S"
  }
}

resource "aws_dynamodb_table" "load_type" {
  name         = "TABLE_LOAD_TYPE"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "database_name"
  range_key    = "table_name"
  
  attribute {
    name = "database_name"
    type = "S"
  }
  attribute {
    name = "table_name"
    type = "S"
  }
}

# IAM Role for Glue
resource "aws_iam_role" "glue" {
  name = "${var.project}-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "glue" {
  name = "${var.project}-glue-policy"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:*"]
        Resource = [aws_s3_bucket.data.arn, "${aws_s3_bucket.data.arn}/*"]
      },
      {
        Effect = "Allow"
        Action = ["dynamodb:*"]
        Resource = [aws_dynamodb_table.run_time_metadata.arn, aws_dynamodb_table.load_type.arn]
      },
      {
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue"]
        Resource = "*"
      }
    ]
  })
}

# Secrets
resource "aws_secretsmanager_secret" "mysql" { name = "glue_mysql_creds" }
resource "aws_secretsmanager_secret" "snowflake" { name = "snowflake" }

output "bucket_name" { value = aws_s3_bucket.data.bucket }
output "glue_role_arn" { value = aws_iam_role.glue.arn }