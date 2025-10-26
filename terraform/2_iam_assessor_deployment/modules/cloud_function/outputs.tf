output "function_sa_email" {
  description = "作成されたFunction専用サービスアカウントのメールアドレス。"
  value       = google_service_account.function_sa.email
}

# 修正点: 名前(name) ではなく ID(フルパス) を出力し、アウトプット名も変更
output "trigger_topic_id" {
  description = "作成されたトリガー用のPub/SubトピックのID (フルパス)。"
  # trigger_typeが"topic"の場合にのみ値が返され、それ以外はnullになる
  value = var.trigger_type == "topic" ? google_pubsub_topic.trigger_topic[0].id : null
}