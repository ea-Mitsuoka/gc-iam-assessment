# ./src/assessors/resource_centric/bq_assessor/main.py
import os
import json
import base64
import functions_framework
from collections import defaultdict
# 修正点: グローバルインスタンスと、動的初期化用のクラスの両方をインポート
from utils.gcp_clients import bigquery_client, identity_client, BigQueryClientClass
from utils.iam_helpers import expand_member
from utils.logging_handler import get_logger

# --------------------------------------------------
# 環境変数から設定を読み込み
# --------------------------------------------------
BQ_PROJECT_ID = os.getenv('BQ_PROJECT_ID')
BQ_DATASET_ID = os.getenv('BQ_DATASET_ID')
# 修正点: ハードコードされたテーブル名を環境変数から読み込む
BQ_TABLE_ID = os.getenv('DESTINATION_TABLE_ID')

# 修正点: ロガーのみ初期化
logger = get_logger(__name__)
# 修正点: インスタンスの初期化コードを削除 (gcp_clients.py からインポート)

@functions_framework.cloud_event
def assess_iam_policy_pubsub(cloud_event):
    """
    Pub/Subメッセージをトリガーに、BigQueryデータセットのIAMポリシーを評価し、
    結果を統一テーブルに書き込む。
    """
    # 修正点: 必須の環境変数が設定されているかチェック
    if not BQ_TABLE_ID:
        msg = "Missing required environment variable: DESTINATION_TABLE_ID must be set."
        logger.error(msg)
        raise ValueError(msg) # Functionを失敗させる

    try:
        # Pub/Subメッセージからパラメータを取得
        message_data_str = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
        message_data = json.loads(message_data_str)
        scope = message_data['scope']
        resource_full_name = message_data['resource_name']
        project_id, _, dataset_id = resource_full_name.split('/')[-1].split(':')[-1].partition('.')
        assessment_timestamp = message_data['assessment_timestamp']

        logger.info(f"Assessing dataset: {project_id}.{dataset_id}")
    except (KeyError, json.JSONDecodeError) as e:
        logger.error(f"Invalid message format, skipping: {e}")
        return # メッセージが不正な場合はエラーにせず、処理を終了

    try:
        # --- ここからがメインの処理 ---

        # 1. データセットの情報を取得
        # 修正点: 動的初期化のために 'BigQueryClientClass' を使用
        bq_client_for_target = BigQueryClientClass(project=project_id)
        dataset = bq_client_for_target.get_dataset(dataset_id, timeout=30.0)

        rows_to_insert = []

        # 2. データセットのアクセスエントリを直接ループし、ポリシーを解析
        if dataset.access_entries:
            for entry in dataset.access_entries:
                member_type, member_id = "Unknown", None
                if entry.entity_type == "user":
                    member_type, member_id = "USER", entry.entity_id
                elif entry.entity_type == "groupByEmail":
                    member_type, member_id = "GROUP", entry.entity_id
                elif entry.entity_type == "serviceAccount":
                    member_type, member_id = "SERVICE_ACCOUNT", entry.entity_id
                elif entry.entity_type == "specialGroup":
                    member_type, member_id = "SPECIAL_GROUP", entry.entity_id

                if member_id:
                    # グローバルインスタンス (identity_client) を使用
                    for expanded_member in expand_member(identity_client, member_type, member_id, set()):
                        member_type_final, member_email_final = expanded_member.split(":", 1)
                        rows_to_insert.append({
                            "assessment_timestamp": assessment_timestamp,
                            "scope": scope,
                            "resource_type": "BIGQUERY_DATASET",
                            "resource_name": f"{project_id}.{dataset_id}",
                            "principal_type": member_type_final,
                            "principal_email": member_email_final,
                            "role": entry.role,
                        })

        # 3. 結果をBigQueryに書き込み
        if rows_to_insert:
            # グローバルインスタンス (bigquery_client) を使用
            # BQ_TABLE_IDは環境変数から読み込まれたものを使用
            table_ref = bigquery_client.dataset(BQ_DATASET_ID, project=BQ_PROJECT_ID).table(BQ_TABLE_ID)
            errors = bigquery_client.insert_rows_json(table_ref, rows_to_insert)
            if errors:
                raise Exception(f"BigQuery insert errors: {errors}")
            else:
                logger.info(f"Successfully wrote {len(rows_to_insert)} records for dataset {dataset_id} to BigQuery.")
        else:
            logger.info(f"No direct access entries found for dataset {dataset_id}.")

        # --- ここまでがメインの処理 ---

    except Exception as e:
        logger.error(
            f"An unexpected error occurred during BQ assessment for dataset {project_id}.{dataset_id}: {e}",
            exc_info=True
        )
        raise
