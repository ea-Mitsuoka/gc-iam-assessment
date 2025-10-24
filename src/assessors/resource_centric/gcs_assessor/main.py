# ./src/assessors/resource_centric/gcs_assessor/main.py
import os
import json
import base64
import functions_framework
# 修正点: クラスではなく、グローバルインスタンスを直接インポート
from utils.gcp_clients import storage_client, bigquery_client, identity_client
from utils.iam_helpers import expand_member
from utils.logging_handler import get_logger

# --- 環境変数 ---
BQ_PROJECT_ID = os.getenv('BQ_PROJECT_ID')
BQ_DATASET_ID = os.getenv('BQ_DATASET_ID')
# 修正点: ハードコードされたテーブル名を環境変数から読み込む
BQ_TABLE_ID = os.getenv('DESTINATION_TABLE_ID')

# 修正点: ロガーのみ初期化
logger = get_logger(__name__)
# 修正点: インスタンスの初期化コードを削除


@functions_framework.cloud_event
def assess_gcs_bucket_policy(cloud_event):
    """
    Pub/Subメッセージをトリガーに、GCSバケットのIAMポリシーを評価する。
    """
    # 修正点: 必須の環境変数が設定されているかチェック
    if not BQ_TABLE_ID:
        logger.error("Missing required environment variable: DESTINATION_TABLE_ID must be set.")
        # ここでreturnすると正常終了扱いになるため、エラーをraiseする
        raise ValueError("Missing required environment variable: DESTINATION_TABLE_ID")

    try:
        message_data_str = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
        message_data = json.loads(message_data_str)
        scope = message_data['scope']
        bucket_name = message_data['resource_name'].split('/')[-1]
        assessment_timestamp = message_data['assessment_timestamp']

        logger.info(f"Assessing bucket: {bucket_name}")
    except (KeyError, json.JSONDecodeError) as e:
        logger.error(f"Invalid message format, skipping: {e}")
        return

    try:
        # --- ここからがメインの処理 ---
        # グローバルインスタンス (storage_client) を使用
        bucket = storage_client.bucket(bucket_name)
        policy = bucket.get_iam_policy(requested_policy_version=3, timeout=30.0)

        rows_to_insert = []

        for role, members in policy.bindings.items():
            for member in members:
                member_type, member_id = member.split(":", 1)
                # グローバルインスタンス (identity_client) を使用 
                for expanded_member in expand_member(identity_client, member_type.upper(), member_id, set()):
                    e_type, e_email = expanded_member.split(":", 1)
                    rows_to_insert.append({
                        "assessment_timestamp": assessment_timestamp,
                        "scope": scope,
                        "resource_type": "GCS_BUCKET",
                        "resource_name": bucket_name,
                        "principal_type": e_type,
                        "principal_email": e_email,
                        "role": role,
                    })

        if rows_to_insert:
            # BQ_TABLE_IDは環境変数から読み込まれたものを使用
            table_ref = bigquery_client.dataset(BQ_DATASET_ID, project=BQ_PROJECT_ID).table(BQ_TABLE_ID)
            errors = bigquery_client.insert_rows_json(table_ref, rows_to_insert)
            if errors:
                logger.error(f"BigQuery insert errors for {bucket_name}: {errors}")
            else:
                logger.info(f"Successfully wrote {len(rows_to_insert)} records for bucket {bucket_name} to BigQuery.")
        else:
             logger.info(f"No IAM bindings found for bucket {bucket_name}.")

        # --- ここまでがメインの処理 ---

    except Exception as e:
        logger.error(f"An unexpected error occurred during GCS assessment for bucket {bucket_name}: {e}")
        raise
