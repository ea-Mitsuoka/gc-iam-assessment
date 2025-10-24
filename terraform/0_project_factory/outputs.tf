output "audit_project_id" {
  value       = google_project.audit_project.project_id
  description = "作成された評価用プロジェクトのID。"
}

output "terraform_state_bucket_name" {
  value       = google_storage_bucket.tfstate_bucket.name
  description = "フェーズ1のtfstateを保存するGCSバケット名。"
}

output "terraform_executor_sa_email" {
  value       = google_service_account.terraform_executor_sa.email
  description = "フェーズ1で権限を借用するSAのメールアドレス。"
}
