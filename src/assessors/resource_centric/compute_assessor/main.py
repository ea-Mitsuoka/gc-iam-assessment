# ./src/assessors/resource_centric/compute_assessor/main.py
import os
import json
import base64
import functions_framework
# 修正点: クラスではなく、グローバルインスタンスを直接インポート
from utils.gcp_clients import compute_client, bigquery_client, identity_client
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
# 修正点: インスタンスの初期化コードを削除

@functions_framework.cloud_event
def assess_compute_instance_policy(cloud_event):
    """
    Pub/Subメッセージをトリガーに、Compute Engine VMインスタンスのIAMポリシーを評価する。
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
        parts = resource_full_name.split('/')
        project_id = parts[4]
        zone = parts[6]
        instance_name = parts[8]
        assessment_timestamp = message_data['assessment_timestamp']

        logger.info(f"Assessing VM instance: {instance_name} in project {project_id}")
    except (KeyError, json.JSONDecodeError, IndexError) as e:
        logger.error(f"Invalid message format, skipping: {e}")
        return # メッセージが不正な場合はエラーにせず、処理を終了

    try:
        # --- ここからがメインの処理 ---

        # 1. VMインスタンスのIAMポリシーを取得
        # グローバルインスタンス (compute_client) を使用
        policy = compute_client.get_iam_policy(project=project_id, zone=zone, resource=instance_name, timeout=30.0)

        rows_to_insert = []

        # 2. ポリシーを解析し、グループを展開
        for binding in policy.bindings:
            role = binding.role
            for member in binding.members:
                member_type, member_id = member.split(":", 1)

                # グローバルインスタンス (identity_client) を使用
                for expanded_member in expand_member(identity_client, member_type.upper(), member_id, set()):
                    e_type, e_email = expanded_member.split(":", 1)
                    rows_to_insert.append({
                        "assessment_timestamp": assessment_timestamp,
                        "scope": scope,
                        "resource_type": "COMPUTE_INSTANCE",
                        "resource_name": f"{project_id}/{zone}/{instance_name}",
                        "principal_type": e_type,
                        "principal_email": e_email,
                        "role": role,
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
                logger.info(f"Successfully wrote {len(rows_to_insert)} records for VM {instance_name} to BigQuery.")
        else:
            logger.info(f"No IAM bindings found for VM {instance_name}.")

        # --- ここまでがメインの処理 ---

    except Exception as e:
        logger.error(f"An unexpected error occurred during Compute assessment for VM {instance_name}: {e}")
        raise