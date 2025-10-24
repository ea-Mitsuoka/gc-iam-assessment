variable "org_id" {
  type        = string
  description = "評価ツールが閲覧権限を持つべきGCP組織ID。"
}

variable "folder_id" {
  type        = string
  description = "新しいプロジェクトを作成するフォルダID。"
}

variable "project_name" {
  type        = string
  description = "作成する評価用プロジェクトの表示名。"
  default     = "iam-assessment-tool-host"
}

variable "region" {
  type        = string
  description = "tfstateバケットなど、セットアップ用のリソースを作成するリージョン。"
  default     = "ASIA-NORTHEAST1"
}
