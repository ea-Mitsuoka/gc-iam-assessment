variable "project_id" {
  type        = string
  description = "Google Cloud Project ID"
}
variable "dataset_id" {
  type        = string
  description = "BigQuery Dataset ID"
}

# 変更点: テーブル名をキー、スキーマファイルパスを値とするマップを受け取る変数を追加
variable "table_schemas" {
  type        = map(string)
  description = "作成するBigQueryテーブル名と、そのスキーマ定義JSONファイルへのパスのマッピング。"
  default     = {}
}

variable "dataset_location" {
  type        = string
  description = "BigQueryデータセットを作成するロケーション。"
  default     = "US"
}