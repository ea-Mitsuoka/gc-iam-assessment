import os
import functions_framework
# 変更点: bigqueryライブラリの直接インポートは不要になり、ヘルパー関数をインポートする
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

@functions_framework.cloud_event
def analyze_public_exposure(cloud_event):
    """
    Pub/Subメッセージをトリガーに、principal_access_listテーブルを分析し、
    外部公開されているリソースを抽出する。
    """
    logger.info(f"Starting public exposure analysis...")

    # 修正点: 必須の環境変数が設定されているかチェック
    if not SOURCE_TABLE_ID or not DESTINATION_TABLE_ID:
        msg = "Missing required environment variables: SOURCE_TABLE_ID and DESTINATION_TABLE_ID must be set."
        logger.error(msg)
        raise ValueError(msg) # Functionを失敗させる

    try:
        # --- ここからがメインの処理 ---
        # SOURCE_TABLE_IDは環境変数から読み込まれたものを使用
        source_table_fqn = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{SOURCE_TABLE_ID}`"

        # BigQueryで外部公開されているリソースを抽出するSQLクエリ
        query = f"""
        SELECT
            t.assessment_timestamp,
            t.scope,
            access.resource_name,
            t.principal_email AS public_principal,
            access.role
        FROM
            {source_table_fqn} AS t, UNNEST(t.access_list) AS access
        WHERE
            t.principal_email IN ('allUsers', 'allAuthenticatedUsers')
        """

        # 変更点: ヘルパー関数を呼び出し、結果でテーブルを上書き(TRUNCATE)する
        run_query_and_save_results(
            query=query,
            # DESTINATION_TABLE_IDは環境変数から読み込まれたものを使用
            destination_table_id=DESTINATION_TABLE_ID,
            write_disposition="WRITE_TRUNCATE", # 毎回テーブルをクリアして最新の結果で上書き
            max_bytes_billed=MAX_BYTES_BILLED
        )

        logger.info(f"Successfully completed public exposure analysis.")
        # --- ここまでがメインの処理 ---

    except Exception as e:
        logger.error(f"An unexpected error occurred during public exposure analysis: {e}", exc_info=True)
        raise
