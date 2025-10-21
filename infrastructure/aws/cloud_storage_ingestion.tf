terraform {
  required_version = ">= 1.3"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

locals {
  bucket_name = var.bucket_name
}

resource "aws_s3_bucket" "raw_ingestion" {
  bucket = local.bucket_name

  tags = merge(var.default_tags, {
    Component = "cloud-ingestion"
  })
}

resource "aws_s3_bucket_notification" "raw_ingestion" {
  bucket = aws_s3_bucket.raw_ingestion.id

  dynamic "topic" {
    for_each = [aws_sns_topic.raw_ingestion.arn]

    content {
      topic_arn     = topic.value
      events        = ["s3:ObjectCreated:*"]
      filter_suffix = var.file_suffix_filter != "" ? ".${var.file_suffix_filter}" : null
    }
  }

  depends_on = [aws_sns_topic_policy.raw_ingestion]
}

resource "aws_sns_topic" "raw_ingestion" {
  name = "${var.project}-raw-ingestion"

  tags = merge(var.default_tags, {
    Component = "sns"
  })
}

resource "aws_sns_topic_policy" "raw_ingestion" {
  arn = aws_sns_topic.raw_ingestion.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
        Action   = "sns:Publish"
        Resource = aws_sns_topic.raw_ingestion.arn
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
          ArnLike = {
            "aws:SourceArn" = aws_s3_bucket.raw_ingestion.arn
          }
        }
      }
    ]
  })
}

data "aws_caller_identity" "current" {}

resource "aws_sqs_queue" "processing" {
  count = length(var.sqs_subscribers)

  name                       = "${var.project}-${var.sqs_subscribers[count.index]}"
  visibility_timeout_seconds = var.sqs_visibility_timeout_seconds
  message_retention_seconds  = var.sqs_message_retention_seconds
  receive_wait_time_seconds  = 20

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.processing_dlq[count.index].arn
    maxReceiveCount     = 5
  })

  tags = merge(var.default_tags, {
    Component = "sqs"
    Stream    = var.sqs_subscribers[count.index]
  })
}

resource "aws_sqs_queue" "processing_dlq" {
  count = length(var.sqs_subscribers)

  name                      = "${var.project}-${var.sqs_subscribers[count.index]}-dlq"
  message_retention_seconds = 1209600

  tags = merge(var.default_tags, {
    Component = "sqs-dlq"
    Stream    = var.sqs_subscribers[count.index]
  })
}

resource "aws_sqs_queue_policy" "sns_publish" {
  count = length(var.sqs_subscribers)

  queue_url = aws_sqs_queue.processing[count.index].url

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowSNSPublish"
        Effect    = "Allow"
        Principal = {
          Service = "sns.amazonaws.com"
        }
        Action   = "sqs:SendMessage"
        Resource = aws_sqs_queue.processing[count.index].arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_sns_topic.raw_ingestion.arn
          }
        }
      }
    ]
  })
}

resource "aws_sns_topic_subscription" "processing" {
  count = length(var.sqs_subscribers)

  topic_arn = aws_sns_topic.raw_ingestion.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.processing[count.index].arn

  filter_policy = jsonencode({
    stream = [var.sqs_subscribers[count.index]]
  })
}

variable "aws_region" {
  type        = string
  description = "AWS region for the deployment"
}

variable "project" {
  type        = string
  description = "Project slug used for resource naming"
}

variable "bucket_name" {
  type        = string
  description = "Name of the S3 bucket that stores raw ingestion files"
}

variable "file_suffix_filter" {
  type        = string
  description = "Optional suffix filter for S3 events (for example: 'json'). Leave empty to receive all events."
  default     = ""
}

variable "sqs_subscribers" {
  type        = list(string)
  description = "List of logical stream names that map to SQS queues"
}

variable "sqs_visibility_timeout_seconds" {
  type        = number
  description = "Visibility timeout for processing queues"
  default     = 900
}

variable "sqs_message_retention_seconds" {
  type        = number
  description = "Retention period for processing queues"
  default     = 1209600
}

variable "default_tags" {
  type        = map(string)
  description = "Default resource tags"
  default     = {}
}
