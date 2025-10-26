# --- Provider Configuration ---
provider "google" {
  project = var.project_id
  region  = var.region
  # If using SA impersonation via gcloud (recommended):
  # access_token = data.google_client_config.default.access_token
  # If using SA key file:
  # credentials = file(var.credentials_path)
}
