# ./src/assessors/principal_centric/principal_assessor/main.py
import os
import json
import base64
import functions_framework
import datetime
# 修正点: クラスではなく、グローバルインスタンスを直接インポート
from utils.gcp_clients import asset_client, bigquery_client, identity_client
from utils.iam_helpers import expand_member
from collections import defaultdict
from utils.logging_handler import get_logger

# --- 環境変数 ---
BQ_PROJECT_ID = os.getenv('BQ_PROJECT_ID')
BQ_DATASET_ID = os.getenv('BQ_DATASET_ID')
# 修正点: ハードコードされたテーブル名を環境変数から読み込む
DESTINATION_TABLE_ID = os.getenv('DESTINATION_TABLE_ID')

# 修正点: ロガーのみ初期化
logger = get_logger(__name__)
# 修正点: インスタンスの初期化コードを削除 (gcp_clients.py からインポート)


@functions_framework.cloud_event
def assess_principal_centric(cloud_event):
    """
    Pub/Subメッセージをトリガーに、指定されたスコープ内の全IAMポリシーをプリンシパル中心に評価する。
    """
    # 修正点: 必須の環境変数が設定されているかチェック
    if not DESTINATION_TABLE_ID:
        msg = "Missing required environment variable: DESTINATION_TABLE_ID must be set."
        logger.error(msg)
        raise ValueError(msg) # Functionを失敗させる

    try:
        message_data_str = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
        scopes = []
        try:
            parsed_scopes = json.loads(message_data_str)
            if isinstance(parsed_scopes, list):
                scopes = parsed_scopes
        except json.JSONDecodeError:
            scopes.append(message_data_str)

        if not scopes:
            logger.warning("No valid assessment scopes found.")
            return
    except (KeyError, json.JSONDecodeError) as e:
        logger.error(f"Invalid message format, skipping: {e}")
        return # メッセージが不正な場合はエラーにせず、処理を終了

    logger.info(f"Starting principal-centric assessment for scopes: {scopes}")

    try:
        # --- ここからがメインの処理 ---
        principal_permissions = defaultdict(list)
        for scope in scopes:
            try:
                # グローバルインスタンス (asset_client) を使用
                all_policies = asset_client.search_all_iam_policies(scope=scope, timeout=300.0)
                for policy in all_policies:
                    for binding in policy.policy.bindings:
                        for member in binding.members:
                            principal_permissions[member].append({
                                "resource_name": policy.resource,
                                "role": binding.role,
                                "scope": scope 
                            })
            except Exception as e:
                logger.error(f"Failed to get IAM policies for scope {scope}: {e}")
                continue
        
        final_permissions = defaultdict(list)
        for principal, access_list in principal_permissions.items():
            member_type, member_id = principal.split(":", 1)
            # グローバルインスタンス (identity_client) を使用
            for expanded_member in expand_member(identity_client, member_type.upper(), member_id, set()):
                final_permissions[expanded_member].extend(access_list)
                
        final_permissions_with_scope = defaultdict(lambda: defaultdict(list))
        for principal, permissions in final_permissions.items():
            for perm in permissions:
                scope = perm['scope']
                access = {'resource_name': perm['resource_name'], 'role': perm['role']}
                final_permissions_with_scope[principal][scope].append(access)

        rows_to_insert = []
        current_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        
        for principal, scope_mappings in final_permissions_with_scope.items():
            p_type, p_email = principal.split(":", 1)
            for scope, access_list in scope_mappings.items():
                rows_to_insert.append({
                    "assessment_timestamp": current_timestamp,
                    "scope": scope,
                    "principal_type": p_type,
                    "principal_email": p_email,
                    "access_list": access_list
                })

        if rows_to_insert:
            # グローバルインスタンス (bigquery_client) を使用
            # DESTINATION_TABLE_IDは環境変数から読み込まれたものを使用
            table_ref = bigquery_client.dataset(BQ_DATASET_ID, project=BQ_PROJECT_ID).table(DESTINATION_TABLE_ID)
            errors = bigquery_client.insert_rows_json(table_ref, rows_to_insert)
            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
            else:
                logger.info(f"Successfully wrote {len(rows_to_insert)} principals to BigQuery.")
        # --- ここまでがメインの処理 ---
        
    except Exception as e:
        logger.error(f"An unexpected error occurred during principal assessment: {e}")
        raise
