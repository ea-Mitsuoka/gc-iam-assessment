import os
import functions_framework
import json # ◀◀ JSONをパースするためにインポート
from utils.bq_helpers import run_query_and_save_results
from utils.logging_handler import get_logger

# --- 環境変数 ---
BQ_PROJECT_ID = os.getenv('BQ_PROJECT_ID')
BQ_DATASET_ID = os.getenv('BQ_DATASET_ID')
# 修正点: ハードコードされた定数を環境変数から読み込む
SOURCE_TABLE_ID = os.getenv('SOURCE_TABLE_ID')
DESTINATION_TABLE_ID = os.getenv('DESTINATION_TABLE_ID')

# 1クエリあたりの最大スキャンバイト数を設定 (例: 10GB)
MAX_BYTES_BILLED = 10 * 1024 * 1024 * 1024

logger = get_logger(__name__)

# 修正点: ロールリストも環境変数(JSON文字列)から読み込む
try:
    HIGH_RISK_ROLES_JSON = os.getenv('HIGH_RISK_ROLES_JSON', '{}')
    HIGH_RISK_ROLES = json.loads(HIGH_RISK_ROLES_JSON)
except json.JSONDecodeError:
    logger.critical("Invalid JSON format for HIGH_RISK_ROLES_JSON env var.")
    HIGH_RISK_ROLES = {} # エラー時は空にする

@functions_framework.cloud_event
def analyze_high_risk_roles(cloud_event):
    """
    Pub/Subメッセージをトリガーに、principal_access_listテーブルを分析し、
    高リスク権限を持つプリンシパルを抽出する。
    """
    logger.info(f"Starting high-risk role analysis...")

    # 修正点: 必須の環境変数が設定されているかチェック
    if not SOURCE_TABLE_ID or not DESTINATION_TABLE_ID or not HIGH_RISK_ROLES:
        logger.error("Missing required environment variables: SOURCE_TABLE_ID, DESTINATION_TABLE_ID, or HIGH_RISK_ROLES_JSON must be set.")
        raise ValueError("Missing required environment variables.")

    try:
        # --- ここからがメインの処理 ---
        source_table_fqn = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{SOURCE_TABLE_ID}`"
        high_risk_roles_tuple = tuple(HIGH_RISK_ROLES.keys())

        # HIGH_RISK_ROLESが空でないかチェック
        if not high_risk_roles_tuple:
            logger.warning("HIGH_RISK_ROLES_JSON is empty or invalid. Skipping analysis.")
            return

        # BigQueryで高リスク権限を持つプリンシパルを抽出するSQLクエリ
        query = f"SELECT t.assessment_timestamp, t.scope, t.principal_type, t.principal_email, access.resource_name, access.role AS high_risk_role, CASE access.role "
        for role, category in HIGH_RISK_ROLES.items():
            query += f" WHEN '{role}' THEN '{category}'"
        query += f" ELSE 'UNCATEGORIZED' END AS risk_category FROM {source_table_fqn} AS t, UNNEST(t.access_list) AS access WHERE access.role IN {high_risk_roles_tuple}"

        # ヘルパー関数を呼び出し、結果をテーブルに追加(APPEND)する
        run_query_and_save_results(
            query=query,
            destination_table_id=DESTINATION_TABLE_ID,
            write_disposition="WRITE_APPEND", # 前のanalyzerでTRUNCATEしている場合、APPENDでOK
            max_bytes_billed=MAX_BYTES_BILLED
        )

        logger.info(f"Successfully completed high-risk role analysis.")
        # --- ここまでがメインの処理 ---

    except Exception as e:
        logger.error(f"An unexpected error occurred during risk analysis: {e}", exc_info=True)
        raise