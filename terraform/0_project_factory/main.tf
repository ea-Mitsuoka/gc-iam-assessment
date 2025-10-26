# --- Provider Configuration (Ensure you have this block) ---
provider "google" {
  # You might need to specify credentials or project if running locally outside Cloud Shell
}

# --- Resources ---

# 1. 評価ツール専用のGCPプロジェクトを作成
resource "google_project" "audit_project" {
  provider   = google
  name       = var.project_id_prefix # Use prefix from tfvars
  project_id = "${var.project_id_prefix}-${random_string.suffix.result}"
  # folder_id が空でなければフォルダに、そうでなければ組織直下に作成
  folder_id = var.folder_id != "" ? var.folder_id : null
  # billing_account は設定しない (手動で有効化)
  org_id = var.org_id
}

# プロジェクトID用のランダムな接尾辞
resource "random_string" "suffix" {
  length  = 4
  special = false
  upper   = false
}

# 2. 新しいプロジェクトで、0_project_factory に最低限必要なAPIを有効化
resource "google_project_service" "apis" {
  provider = google
  project  = google_project.audit_project.project_id
  # 修正点: APIリストを最小限に変更
  for_each = toset([
    "iam.googleapis.com",                 # SA作成/管理用
    "storage.googleapis.com",             # tfstateバケット用
    "serviceusage.googleapis.com",        # API有効化自体に必要
    "cloudresourcemanager.googleapis.com" # プロジェクト/組織操作用
  ])
  service = each.key
  # disable_on_destroy = false # Destroy時にAPIを無効化しない場合は false
  # API有効化はプロジェクト作成後に実行
  depends_on = [google_project.audit_project]
}

# 3. Terraform実行用サービスアカウント(SA)を作成
resource "google_service_account" "terraform_executor_sa" {
  provider     = google
  project      = google_project.audit_project.project_id
  account_id   = var.terraform_sa_name
  display_name = "Terraform Executor for IAM Assessor"
  # APIが有効になってから作成
  depends_on = [google_project_service.apis]
}

# 5. 作成したSAに、このホストプロジェクトに対する基本的な権限を付与
resource "google_project_iam_member" "terraform_sa_project_roles" {
  provider = google
  # 修正点: ロールを 1_iam_assessor_deployment で必要なものと分離
  # ここではホストプロジェクト自体の管理に必要な最低限のロールを付与
  for_each = toset([
    "roles/iam.serviceAccountAdmin",         # 1_... でFunction用SAを作成/管理するため
    "roles/resourcemanager.projectIamAdmin", # ホストプロジェクト内のIAM管理用 (Function用SAへの権限付与など)
    "roles/serviceusage.serviceUsageAdmin"   # 1_... でAPIを有効化するため
  ])

  project    = google_project.audit_project.project_id
  role       = each.key
  member     = "serviceAccount:${google_service_account.terraform_executor_sa.email}"
  depends_on = [google_service_account.terraform_executor_sa]
}

# 6. 作成したSAに、組織レベルで必要な事前定義ロールを付与
resource "google_organization_iam_member" "terraform_sa_org_roles" {
  provider = google # 組織レベルの操作はデフォルトプロバイダを使用
  org_id   = var.org_id
  # 修正点: カスタムロールの代わりに事前定義ロールを使用
  for_each = toset([
    # organizations.setIamPolicy を含む
    "roles/resourcemanager.organizationAdmin",
    # projects.setIamPolicy を含む (組織レベルで付与すれば配下プロジェクトにも適用される)
    "roles/resourcemanager.projectIamAdmin"
  ])

  role   = each.key
  member = "serviceAccount:${google_service_account.terraform_executor_sa.email}"
  # SA作成後に実行
  depends_on = [google_service_account.terraform_executor_sa]
}

# 7. (オプション) Terraform 実行ユーザーがSAを借用できるようにする
resource "google_service_account_iam_member" "terraform_sa_impersonator" {
  provider = google
  # SAのフルパスID (projects/PROJECT_ID/serviceAccounts/...) を指定
  service_account_id = google_service_account.terraform_executor_sa.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "user:${var.impersonate_user_email}" # tfvarsから読み込む
  depends_on         = [google_service_account.terraform_executor_sa]
}
