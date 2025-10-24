resource "google_bigquery_dataset" "assessment_dataset" {
  project    = var.project_id
  dataset_id = var.dataset_id
  location   = var.dataset_location
}

# 変更点: for_eachを使ってマップの数だけテーブルを作成
resource "google_bigquery_table" "assessment_tables" {
  # table_schemasマップの各要素に対してループを実行
  for_each = var.table_schemas

  project    = var.project_id
  dataset_id = google_bigquery_dataset.assessment_dataset.dataset_id

  # マップのキー (例: "iam_policy_permissions") がテーブルIDになる
  table_id = each.key

  # file()関数で、マップの値 (例: "../schemas/iam_policy_schema.json") で
  # 指定されたファイルの内容を文字列として読み込む
  schema = file(each.value)
}