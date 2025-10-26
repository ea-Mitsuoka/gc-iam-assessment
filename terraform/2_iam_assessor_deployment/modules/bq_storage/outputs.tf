output "dataset_id" {
  value = google_bigquery_dataset.assessment_dataset.dataset_id
}

# 変更点: "table_id" を "table_ids" に変更し、マップを返す
output "table_ids" {
  description = "作成された全てのテーブルのIDのマップ。"
  value = {
    for key, table in google_bigquery_table.assessment_tables : key => table.table_id
  }
}