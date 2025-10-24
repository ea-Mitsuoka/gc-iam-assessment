variable "project_id" {
  type        = string
  description = "FunctionをデプロイするGoogle CloudプロジェクトID。"
}

variable "region" {
  type        = string
  description = "Functionをデプロイするリージョン。"
}

variable "function_name" {
  type        = string
  description = "デプロイするCloud Functionの名前。"
}

variable "source_directory" {
  type        = string
  description = "Functionのソースコードが含まれるディレクトリへのパス。"
}

variable "entry_point" {
  type        = string
  description = "Functionのエントリーポイント（実行される関数名）。"
}

variable "service_account_roles" {
  type        = list(string)
  description = "Functionのサービスアカウントに付与するIAMロールのリスト。"
  default     = []
}

variable "environment_variables" {
  type        = map(string)
  description = "Functionに設定する環境変数のマップ。"
  default     = {}
}

variable "trigger_type" {
  type        = string
  description = "Functionのトリガータイプ。現在は'topic'のみをサポート。"
  default     = "topic"
}

variable "pubsub_topic_name" {
  type        = string
  description = "トリガーとなるPub/Subトピックの名前。"
  default     = ""
}

variable "source_bucket_name" {
  type        = string
  description = "Functionのソースコードを格納する共通GCSバケットの名前。"
}
