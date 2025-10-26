# --- Outputs ---
output "tfstate_bucket_name" {
  description = "Name of the GCS bucket created for Terraform state storage."
  value       = google_storage_bucket.tfstate_bucket_assessor.name
}