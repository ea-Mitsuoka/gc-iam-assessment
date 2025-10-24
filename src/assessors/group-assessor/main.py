# ./src/assessors/group-assessor/main.py
import os
import functions_framework
import datetime
# 修正点: クラスではなく、グローバルインスタンスを直接インポート
from utils.gcp_clients import bigquery_client, identity_client
from utils.logging_handler import get_logger

# --- グローバル定数 ---
BQ_PROJECT_ID = os.getenv('BQ_PROJECT_ID')
BQ_DATASET_ID = os.getenv('BQ_DATASET_ID')
# 修正点: ハードコードされたテーブル名を環境変数から読み込む
DESTINATION_TABLE_ID = os.getenv('DESTINATION_TABLE_ID')
GSUITE_CUSTOMER_ID = os.getenv('GSUITE_CUSTOMER_ID') 

# 修正点: ロガーのみ初期化
logger = get_logger(__name__)
# 修正点: インスタンスの初期化コードを削除 (gcp_clients.py からインポート)


@functions_framework.cloud_event
def assess_all_groups(cloud_event):
    """
    Google Workspace/Cloud Identity内の全グループと
    そのメンバーシップ情報を取得し、BigQueryに書き込みます。
    """
    logger.info("Starting assessment of all groups and memberships...")

    # 修正点: 必須の環境変数が設定されているかチェック
    if not GSUITE_CUSTOMER_ID or not DESTINATION_TABLE_ID:
        msg = "Missing required environment variables: GSUITE_CUSTOMER_ID and DESTINATION_TABLE_ID must be set."
        logger.error(msg)
        raise ValueError(msg) # Functionを失敗させる

    try:
        # 1. 組織内の全グループを取得
        logger.debug(f"Searching groups for customer: {GSUITE_CUSTOMER_ID}")
        # グローバルインスタンス (identity_client) を使用
        groups_iterator = identity_client.search_groups(
            parent=f"customers/{GSUITE_CUSTOMER_ID}",
            timeout=120.0
        )
        groups = list(groups_iterator)
        logger.info(f"Found {len(groups)} groups to assess.")
        
        rows_to_insert = []
        current_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

        # 2. 各グループのメンバーを取得
        for group in groups:
            group_email = group.group_key.id
            try:
                # 修正点2: APIの効率化 (N+1クエリの解消)
                memberships_iterator = identity_client.list_memberships(
                    parent=group.name,
                    view=1, # 1 = MembershipView.FULL
                    timeout=60.0
                )
                
                for membership in memberships_iterator:
                    member_email = membership.preferred_member_key.id
                    
                    if membership.type_ == 2: # membership.Type.GROUP
                        member_type = "GROUP"
                    elif membership.type_ == 1: # membership.Type.USER
                        if '.gserviceaccount.com' in member_email:
                            member_type = "SERVICE_ACCOUNT"
                        else:
                            member_type = "USER"
                    else:
                        member_type = "UNKNOWN"
                    
                    rows_to_insert.append({
                        "assessment_timestamp": current_timestamp,
                        "group_email": group_email,
                        "member_email": member_email,
                        "member_type": member_type,
                    })
            except Exception as e:
                logger.warning(f"Failed to process memberships for group {group_email}: {e}")
        
        # 3. 結果をBigQueryに書き込み
        if rows_to_insert:
            logger.info(f"Writing {len(rows_to_insert)} group membership records to BigQuery...")
            # グローバルインスタンス (bigquery_client) を使用
            # DESTINATION_TABLE_IDは環境変数から読み込まれたものを使用
            bigquery_client.insert_rows(
                dataset_id=BQ_DATASET_ID,
                table_id=DESTINATION_TABLE_ID,
                rows=rows_to_insert,
                write_disposition="WRITE_TRUNCATE" # 毎回テーブルを洗い替える
            )
            logger.info(f"Successfully wrote {len(rows_to_insert)} records to {BQ_DATASET_ID}.{DESTINATION_TABLE_ID}.")
        else:
            logger.warning("No group memberships found or processed. No data written to BigQuery.")

    except Exception as e:
        logger.error(f"An unexpected error occurred during group assessment: {e}", exc_info=True)
        raise
