locals {
  functions_config = {
    for name, config in var.assessment_functions : name => {
      # pathがtfvarsで指定されていればそれ（dispatcher）を使い、
      # なければ規則（他のassessor）に従って生成する、というシンプルなロジック。
      path                  = try(config.path, "../src/${config.category}/${name}")
      entry                 = try(config.entry, "assess_${replace(name, "-", "_")}")
      service_account_roles = try(config.service_account_roles, [])
      # 修正点: environment_variablesもlocal変数に含める
      environment_variables = config.environment_variables
    }
  }
}

# --- 必要なAPIを有効化 ---
resource "google_project_service" "apis" {
  project                    = var.project_id
  for_each                   = toset(var.enabled_apis)
  service                    = each.key
  disable_dependent_services = true
}

# --- Cloud Functionのソースコードを格納する共通GCSバケットを作成 ---
resource "google_storage_bucket" "functions_source_bucket" {
  project                     = var.project_id
  name                        = "${var.project_id}-cf-source"
  location                    = var.region
  force_destroy               = var.enable_force_destroy # 変数で制御できるようにする
  uniform_bucket_level_access = true
}

# --- 評価Function群 (for_eachで動的に作成) ---
module "assessment_functions" {
  for_each = local.functions_config

  source                = "./modules/cloud_function"
  project_id            = var.project_id
  region                = var.region
  function_name         = each.key
  source_directory      = each.value.path
  entry_point           = each.value.entry
  trigger_type          = "topic"
  pubsub_topic_name     = "${each.key}-topic"
  service_account_roles = each.value.service_account_roles
  source_bucket_name    = google_storage_bucket.functions_source_bucket.name

  # 修正点: merge()関数を変更
  environment_variables = merge(
    # 全てのFunctionに共通の環境変数
    {
      BQ_PROJECT_ID = var.project_id
      BQ_DATASET_ID = var.bq_dataset_id
    },
    # group-assessorにだけ追加したい環境変数 (tfvarsに移行可能だが互換性のため残す)
    each.key == "group-assessor" ? {
      GSUITE_CUSTOMER_ID = var.gsuite_customer_id
    } : {},
    # overpermission-analyzerにだけ追加したい環境変数 (tfvarsに移行可能だが互換性のため残す)
    each.key == "overpermission-analyzer" ? {
      RECOMMENDER_PARENT = var.assessment_scope == "ORGANIZATION" ? "organizations/${var.org_id}" : "projects/${var.target_project_ids[0]}"
    } : {},

    # 修正点: tfvarsから渡されたFunction固有の環境変数をマージ
    each.value.environment_variables
  )
}

# --- 権限付与 (スコープに応じて動的に切り替え) ---
# ORGANIZATIONモード時
resource "google_organization_iam_member" "assessor_sa_org_viewer" {
  for_each = {
    for item in flatten([
      for func_name, mod in module.assessment_functions : [
        # 修正点: ロールリストの参照元を locals から var に変更
        for role in var.assessment_functions[func_name].service_account_roles : {
          function_name = func_name
          sa_email      = mod.function_sa_email
          role          = role
        }
      ]
    ]) : "${item.function_name}-${replace(item.role, ".", "-")}" => item
    if var.assessment_scope == "ORGANIZATION"
  }

  org_id = var.org_id
  role   = each.value.role
  member = "serviceAccount:${each.value.sa_email}"
}

# PROJECTモード時
resource "google_project_iam_member" "assessor_sa_project_viewer" {
  for_each = {
    for item in flatten([
      for project_id in var.target_project_ids : [
        for function_name, mod in module.assessment_functions : [
          # 修正点: ロールリストの参照元を locals から var に変更
          for role in var.assessment_functions[function_name].service_account_roles : {
            project_id    = project_id
            function_name = function_name
            sa_email      = mod.function_sa_email
            role          = role
          }
        ]
      ]
    ]) : "${item.project_id}-${item.function_name}-${replace(item.role, ".", "-")}" => item
    if var.assessment_scope == "PROJECT"
  }

  project = each.value.project_id
  role    = each.value.role
  member = "serviceAccount:${each.value.sa_email}"
}

# --- BigQuery (外部スキーマファイルを利用) ---
module "bq_storage" {
  source           = "./modules/bq_storage"
  project_id       = var.project_id
  dataset_id       = var.bq_dataset_id
  table_schemas    = var.table_schemas
  dataset_location = var.bq_dataset_location
}

resource "google_cloud_scheduler_job" "assessors" {
  for_each = var.scheduler_configs

  project     = var.project_id
  region      = var.region
  name        = "daily-iam-${each.key}"
  description = each.value.description
  schedule    = each.value.schedule
  time_zone   = var.scheduler_time_zone

  pubsub_target {
    # 修正点: モジュールの出力(trigger_topic_id)を直接参照
    # これにより、依存関係が明示的になる
    topic_name = module.assessment_functions[each.key].trigger_topic_id
    
    # 変更点: dataを動的に生成するロジックをここに記述
    data = base64encode(
      # キーが"principal-assessor"の場合のみ、スコープに応じたデータを生成
      each.key == "principal-assessor" ? (
        var.assessment_scope == "ORGANIZATION" ? "organizations/${var.org_id}" : jsonencode([for pid in var.target_project_ids : "projects/${pid}"])
      ) :
      # それ以外のスケジューラの場合は、tfvarsで定義されたデータ(またはデフォルトの"{}" )を使用
      each.value.data
    )
  }
}
