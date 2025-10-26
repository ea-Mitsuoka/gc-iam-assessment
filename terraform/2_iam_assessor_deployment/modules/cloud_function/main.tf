# --------------------------------------------------
# Function専用のサービスアカウントを作成
# --------------------------------------------------
resource "google_service_account" "function_sa" {
  project      = var.project_id
  account_id   = "${var.function_name}-sa"
  display_name = "SA for ${var.function_name}"
}

# --------------------------------------------------
# サービスアカウントに必要なIAMロールを付与
# --------------------------------------------------
resource "google_project_iam_member" "sa_roles" {
  for_each = toset(var.service_account_roles)
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${google_service_account.function_sa.email}"
}

# --------------------------------------------------
# FunctionのトリガーとなるPub/Subトピックを作成
# --------------------------------------------------
resource "google_pubsub_topic" "trigger_topic" {
  # trigger_typeが"topic"の場合にのみ、このリソースを作成する
  count   = var.trigger_type == "topic" ? 1 : 0
  project = var.project_id
  name    = var.pubsub_topic_name
}

# --------------------------------------------------
# ソースコードの準備 (Zip化とGCSへのアップロード)
# --------------------------------------------------
data "archive_file" "source_zip" {
  type        = "zip"
  source_dir  = var.source_directory
  output_path = "/tmp/${var.function_name}-source.zip"
}

resource "google_storage_bucket_object" "source_object" {
  # ファイル内容のハッシュを名前に含めることで、コード変更時に再アップロードを強制する
  name   = "${var.function_name}-source-${data.archive_file.source_zip.output_md5}.zip"
  bucket = var.source_bucket_name
  source = data.archive_file.source_zip.output_path
}

# --------------------------------------------------
# Cloud Function (第2世代) をデプロイ
# --------------------------------------------------
resource "google_cloudfunctions2_function" "function" {
  project  = var.project_id
  name     = var.function_name
  location = var.region

  build_config {
    runtime     = "python311"
    entry_point = var.entry_point
    source {
      storage_source {
        bucket = var.source_bucket_name
        object = google_storage_bucket_object.source_object.name
      }
    }
  }

  service_config {
    max_instance_count    = 5
    min_instance_count    = 0
    available_memory      = "256Mi"
    timeout_seconds       = 540 # タイムアウトを最大に設定
    environment_variables = var.environment_variables
    # 作成した専用サービスアカウントでFunctionを実行する
    service_account_email = google_service_account.function_sa.email
    # ◀◀ NEW: Ingress設定を追加し、内部トラフィックのみを許可する
    ingress_settings = "INGRESS_SETTINGS_INTERNAL_ONLY"
  }

  # trigger_typeが"topic"の場合にのみ、イベントトリガーを設定
  dynamic "event_trigger" {
    for_each = var.trigger_type == "topic" ? [1] : []
    content {
      trigger_region = var.region
      event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
      # モジュール内で作成したトピックのIDを参照する
      pubsub_topic = google_pubsub_topic.trigger_topic[0].id
      retry_policy = "RETRY_POLICY_RETRY"
    }
  }

  # IAMロールが付与された後にFunctionがデプロイされるように依存関係を明示
  depends_on = [google_project_iam_member.sa_roles]
}
