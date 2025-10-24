import os
import functions_framework
# 変更点: bigqueryライブラリの直接インポートは不要になり、ヘルパー関数をインポートする
from utils.bq_helpers import run_query_and_save_results
from utils.logging_handler import get_logger

# --- 環境変数 ---
BQ_PROJECT_ID = os.getenv('BQ_PROJECT_ID')
BQ_DATASET_ID = os.getenv('BQ_DATASET_ID')

# 修正点: ハードコードされた定数を環境変数から読み込む
DESTINATION_TABLE_ID = os.getenv('DESTINATION_TABLE_ID')
PRINCIPAL_TABLE_ID = os.getenv('PRINCIPAL_TABLE_ID')
GROUP_TABLE_ID = os.getenv('GROUP_TABLE_ID')

# 1クエリあたりの最大スキャンバイト数を設定 (例: 10GB)
MAX_BYTES_BILLED = 10 * 1024 * 1024 * 1024
# ロガーを初期化
logger = get_logger(__name__)

@functions_framework.cloud_event
def analyze_inheritance_risks(cloud_event):
    logger.info("Starting inheritance risk analysis...")

    # 修正点: 必須の環境変数が設定されているかチェック
    if not DESTINATION_TABLE_ID or not PRINCIPAL_TABLE_ID or not GROUP_TABLE_ID:
        msg = "Missing required environment variables: DESTINATION_TABLE_ID, PRINCIPAL_TABLE_ID, and GROUP_TABLE_ID must be set."
        logger.error(msg)
        raise ValueError(msg) # Functionを失敗させる

    try:
        # --- テーブルの完全修飾名を定義 ---
        # 環境変数から読み込まれたテーブル名を使用
        principal_table_fqn = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{PRINCIPAL_TABLE_ID}`"
        group_table_fqn = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{GROUP_TABLE_ID}`"

        # --- クエリ1: 過剰な継承の検出 ---
        logger.info("Analyzing excessive inheritance...")
        query_excessive = f"""
        SELECT
            CURRENT_TIMESTAMP() as assessment_timestamp,
            'EXCESSIVE_INHERITANCE' as risk_type,
            TO_JSON_STRING(t) as details
        FROM {principal_table_fqn} AS t, UNNEST(t.access_list) AS a
        WHERE
            (STARTS_WITH(a.resource_name, '//cloudresourcemanager.googleapis.com/organizations/')
             OR STARTS_WITH(a.resource_name, '//cloudresourcemanager.googleapis.com/folders/'))
            AND a.role IN ('roles/owner', 'roles/editor', 'roles/organization.admin')
        """
        
        # 変更点: ヘルパー関数を呼び出し、結果でテーブルを上書き(TRUNCATE)する
        run_query_and_save_results(
            query=query_excessive,
            # 環境変数から読み込まれたテーブル名を使用
            destination_table_id=DESTINATION_TABLE_ID,
            write_disposition="WRITE_TRUNCATE",
            max_bytes_billed=MAX_BYTES_BILLED
        )

        # --- クエリ2: ネストしたグループの検出 ---
        logger.info("Analyzing nested groups...")
        query_nested = f"""
        SELECT
            CURRENT_TIMESTAMP() as assessment_timestamp,
            'NESTED_GROUP' as risk_type,
            TO_JSON_STRING(t) as details
        FROM {group_table_fqn} AS t
        WHERE
            t.member_type = 'GROUP'
        """
        
        # 変更点: ヘルパー関数を呼び出し、結果をテーブルに追加(APPEND)する
        run_query_and_save_results(
            query=query_nested,
            # 環境変数から読み込まれたテーブル名を使用
            destination_table_id=DESTINATION_TABLE_ID,
            write_disposition="WRITE_APPEND",
            max_bytes_billed=MAX_BYTES_BILLED
        )

        logger.info("Successfully completed inheritance risk analysis.")

    except Exception as e:
        logger.error(f"An unexpected error occurred during inheritance analysis: {e}", exc_info=True)
        # 修正点: 'raise' は 'except' ブロックの内側に正しく配置されている (維持)
        raise
