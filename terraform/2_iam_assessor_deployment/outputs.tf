output "function_sa_email" {
  value = google_service_account.function_sa.email
}

output "trigger_topic_name" {
  value = google_pubsub_topic.trigger_topic.name
}
