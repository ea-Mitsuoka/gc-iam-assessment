# 1. 評価ツール専用のGCPプロジェクトを作成 (課金アカウント無し)
resource "google_project" "audit_project" {
  name       = var.project_name
  project_id = "${var.project_name}-${random_string.suffix.result}"
  folder_id  = var.folder_id
}

# プロジェクトIDは全世界でユニークである必要があるため、ランダムな接尾辞を追加して競合を防ぐ
resource "random_string" "suffix" {
  length  = 4
  special = false
  upper   = false
}

# 2. 新しいプロジェクトで、後のデプロイに必要なAPIを有効化
resource "google_project_service" "apis" {
  project = google_project.audit_project.project_id
  for_each = toset([
    "iam.googleapis.com",
    "storage.googleapis.com",
    "serviceusage.googleapis.com",
    "cloudresourcemanager.googleapis.com",
  ])
  service            = each.key
  disable_on_destroy = false
}

# 3. フェーズ1でTerraformを実行するためのサービスアカウント(SA)を作成
resource "google_service_account" "terraform_executor_sa" {
  project      = google_project.audit_project.project_id
  account_id   = "terraform-executor"
  display_name = "Terraform Executor for IAM Assessor"
  depends_on   = [google_project_service.apis]
}

# 4. フェーズ1のTerraform状態(tfstate)を保存するGCSバケットを作成
resource "google_storage_bucket" "tfstate_bucket" {
  project                     = google_project.audit_project.project_id
  name                        = "${google_project.audit_project.project_id}-tfstate"
  location                    = var.region
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
  depends_on = [google_project_service.apis]
}

# 5. 作成したSAに、このプロジェクトに対する必要な権限を付与
resource "google_project_iam_member" "terraform_sa_project_roles" {
  for_each = toset([
    # Cloud Functionsの管理権限
    "roles/cloudfunctions.admin",
    # Cloud Storageの管理権限 (ソースコードバケット用)
    "roles/storage.admin",
    # Pub/Subの管理権限
    "roles/pubsub.admin",
    # Cloud Schedulerの管理権限
    "roles/cloudscheduler.admin",
    # BigQueryの管理権限
    "roles/bigquery.admin",
    # サービスアカウントとIAMポリシーの管理権限
    "roles/iam.serviceAccountAdmin",
    "roles/resourcemanager.projectIamAdmin"
  ])

  project = google_project.audit_project.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.terraform_executor_sa.email}"
}

# 6. Terraform実行用SAが、組織レベルでIAMポリシーを設定するために必要なカスタムロールを作成
resource "google_organization_custom_role" "terraform_executor_iam_granter" {
  org_id      = var.org_id
  # 変更点: ランダムな接尾辞を追加して、ロールIDのユニーク性を保証する
  role_id     = "terraformExecutorIamGranter_${random_string.suffix.result}"
  # 変更点: titleにもプロジェクト名とランダムな接尾辞を追加
  title       = "Terraform Executor IAM Granter for ${var.project_name}-${random_string.suffix.result}"
  description = "Allows the Terraform executor to grant IAM roles at the organization level."
  permissions = [
    # 組織レベルのIAMポリシーを設定する権限
    "resourcemanager.organizations.setIamPolicy",
    # ◀◀ NEW: プロジェクトレベルのIAMポリシーを設定する権限を追加
    "resourcemanager.projects.setIamPolicy"
  ]
  # ◀◀ NEW: 依存関係を明示する
  depends_on = [random_string.suffix]
}

# 7. 作成したSAに、新しく作成したカスタムロールを付与
resource "google_organization_iam_member" "terraform_sa_custom_role_binder" {
  org_id = var.org_id
  # 変更点: "roles/resourcemanager.organizationAdmin" の代わりにカスタムロールを使用
  role   = google_organization_custom_role.terraform_executor_iam_granter.name
  member = "serviceAccount:${google_service_account.terraform_executor_sa.email}"
}
