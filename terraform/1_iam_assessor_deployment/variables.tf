variable "project_id" {
  type        = string
  description = "評価ツールをデプロイするプロジェクトID（フェーズ0で作成）。"
}

variable "region" {
  type    = string
  default = "asia-northeast1"
}

variable "org_id" {
  type        = string
  description = "評価対象の組織ID。assessment_scopeが'ORGANIZATION'の場合に必須。"
  default     = null
}

variable "terraform_service_account_email" {
  type        = string
  description = "権限を借用するサービスアカウントのメールアドレス。"
}

variable "assessment_scope" {
  type        = string
  description = "評価の範囲: 'ORGANIZATION' または 'PROJECT'。"
  default     = "ORGANIZATION"
  validation {
    condition     = contains(["ORGANIZATION", "PROJECT"], var.assessment_scope)
    error_message = "assessment_scopeは 'ORGANIZATION' または 'PROJECT' のどちらかである必要があります。"
  }
}

variable "target_project_ids" {
  type        = list(string)
  description = "assessment_scopeが'PROJECT'の場合の評価対象プロジェクトIDリスト。"
  default     = []
}

variable "table_schemas" {
  type        = map(string)
  description = "作成するBigQueryテーブル名とスキーマJSONファイルへのパスのマップ。"
  default     = {}
}

variable "enabled_apis" {
  type        = list(string)
  description = "プロジェクトで有効化するAPIのリスト。"
}

variable "bq_dataset_location" {
  type        = string
  description = "評価結果を保存するBigQueryデータセットのロケーション。"
  default     = "US"
}

variable "bq_dataset_id" {
  type        = string
  description = "評価結果を保存するBigQueryデータセットの名前。"
  default     = "iam_assessment_results"
}

variable "assessment_functions" {
  type = map(object({
    # 変更点: optional()を使い、pathとentryを任意項目にする
    path     = optional(string)
    entry    = optional(string)
    category = string
    service_account_roles = optional(list(string), [])
    # 修正点: Function固有の環境変数を定義できるようにする
    environment_variables = optional(map(string), {})
  }))
  # 変更点: descriptionにデータ構造の例を追加
  description = <<-EOT
    デプロイする評価Functionの設定マップ。キーがFunction名になります。
    例:
    "gcs-assessor" = {
      category              = "assessors/resource_centric"
      entry                 = "assess_gcs_bucket_policy"
      service_account_roles = ["roles/storage.viewer", "roles/cloudidentity.groups.reader"]
      environment_variables = {
        DESTINATION_TABLE_ID = "unified_access_permissions"
      }
    }
  EOT
  default     = {}
}



variable "scheduler_time_zone" {
  type        = string
  description = "Schedulerジョブのタイムゾーン。"
  default     = "Asia/Tokyo"
}

variable "enable_force_destroy" {
  type        = bool
  description = "Cloud Storageバケットのforce_destroyを有効にするかどうか。本番環境ではfalseを推奨。"
  default     = false
}

variable "scheduler_configs" {
  type = map(object({
    description = string
    schedule    = string
    data        = optional(string, "{}")
  }))
  # 変更点: descriptionに、キーに関する制約と設定例を追加
  description = <<-EOT
    デプロイするCloud Schedulerジョブの設定マップ。
    注意: このマップのキーは、`assessment_functions`マップのキーと一致している必要があります。
    例:
    "dispatcher" = {
      description = "リソースを発見し、評価タスクを振り分けるジョブ"
      schedule    = "0 10 * * *"
    }
  EOT
# ◀◀ NEW: validationブロックを追加
  validation {
    # scheduler_configsの全てのキーが、assessment_functionsのキーに存在するかをチェック
    condition     = alltrue([
      for k in keys(var.scheduler_configs) : contains(keys(var.assessment_functions), k)
    ])
    error_message = "scheduler_configsのキーが、assessment_functionsに存在しません。キーが一致しているか確認してください。"
  }
}

variable "gsuite_customer_id" {
  type        = string
  description = "グループ情報を取得するために必要なGoogle Workspaceの顧客ID (例: C0123abcd)。"
}
