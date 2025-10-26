# terraform init
# terraform plan -var-file="terraform.tfvars"
# terraform apply -var-file="terraform.tfvars"
# terraform init -reconfigure

# --- Resources ---
# 1. Terraform状態(tfstate)を保存するGCSバケットを作成 (for 2_iam_assessor_deployment)
# Ensure billing is manually enabled on the project before running this factory.
resource "google_storage_bucket" "tfstate_bucket_assessor" {
  provider                    = google
  project                     = var.project_id                       # Use project ID from variable
  name                        = "${var.project_id}-tfstate-assessor" # Use project ID from variable
  location                    = var.region
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
}

# 2. Terraform実行用SAに、作成したtfstateバケットへの管理権限を付与
# This allows the SA to read/write the Terraform state file in the next stage.
resource "google_storage_bucket_iam_member" "terraform_sa_tfstate_bucket_admin" {
  provider = google
  bucket   = google_storage_bucket.tfstate_bucket_assessor.name
  role     = "roles/storage.admin"
  member   = "serviceAccount:${var.terraform_service_account_email}" # Use email from variable

  depends_on = [google_storage_bucket.tfstate_bucket_assessor]
}
