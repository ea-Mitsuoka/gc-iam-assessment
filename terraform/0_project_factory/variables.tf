variable "org_id" {
  type        = string
  description = "評価ツールが閲覧権限を持つべきGCP組織ID。"
}

variable "folder_id" {
  type        = string
  description = "新しいプロジェクトを作成するフォルダID。"
  default     = null
}

variable "project_id_prefix" {
  type        = string
  description = "作成する評価用プロジェクトのIDプレフィックス。"
  default     = "iam-assessment-tool-host"
}

variable "terraform_sa_name" {
  type        = string
  description = "Terraform実行用サービスアカウントの名前。"
  default     = "terraform-executor"
}

variable "impersonate_user_email" {
  type        = string
  description = "Terraform実行時に権限を委譲するユーザーのメールアドレス。"
}
