variable "project_id" {
  type        = string
  description = "The ID of the host project created by 0_project_factory."
}

variable "region" {
  type        = string
  description = "The region where the GCS bucket will be created."
  default     = "asia-northeast1"
}

variable "terraform_service_account_email" {
  type        = string
  description = "The email address of the Terraform service account created by 0_project_factory."
}

# Optional: Add if using SA key file for authentication
# variable "credentials_path" {
#   type        = string
#   description = "Path to the service account key file for Terraform authentication."
#   default     = null
# }