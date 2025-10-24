# ./src/analyzers/overpermission_analyzer/main.py
import os
import functions_framework
from google.cloud import recommender_v1, bigquery
# 修正点: クラスではなく、グローバルインスタンスを直接インポート
from utils.gcp_clients import bigquery_client, recommender_client
import datetime
from utils.logging_handler import get_logger

# --- 環境変数 ---
BQ_PROJECT_ID = os.getenv('BQ_PROJECT_ID')
BQ_DATASET_ID = os.getenv('BQ_DATASET_ID')
# 修正点: ハードコードされたテーブル名を環境変数から読み込む
DESTINATION_TABLE_ID = os.getenv('DESTINATION_TABLE_ID')
RECOMMENDER_PARENT = os.getenv('RECOMMENDER_PARENT')

# 修正点: ロガーのみ初期化
logger = get_logger(__name__)
# 修正点: インスタンスの初期化コードを削除 (gcp_clients.py からインポート)

@functions_framework.cloud_event
def analyze_overpermission(cloud_event):
    """
    IAM Recommender APIから過剰な権限の推奨を取得し、結果をBigQueryに書き込む。
    """
    # 修正点: 必須の環境変数が設定されているかチェック
    if not RECOMMENDER_PARENT or not DESTINATION_TABLE_ID:
        msg = "Missing required environment variables: RECOMMENDER_PARENT and DESTINATION_TABLE_ID must be set."
        logger.error(msg)
        raise ValueError(msg) # Functionを失敗させる

    logger.info("Starting overpermission analysis using IAM Recommender...")
    try:
        recommender_name = f"{RECOMMENDER_PARENT}/recommenders/google.iam.policy.Recommender"

        # 1. IAM Recommenderから推奨を取得 (グローバルインスタンスを使用)
        recommendations = recommender_client.list_recommendations(parent=recommender_name, timeout=300.0)

        rows_to_insert = []
        current_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

        # 2. 取得した推奨を解析し、BigQueryに書き込むデータを作成
        for rec in recommendations:
            details = rec.content.overview
            
            recommended_role = _parse_recommended_role(rec.content.operations)
            
            # --- 修正点: recommender_subtype を取得 ---
            recommendation_subtype = rec.recommender_subtype
            # --- 修正点 ここまで ---

            rows_to_insert.append({
                "assessment_timestamp": current_timestamp,
                "principal_email": details.get('member'),
                "resource_name": details.get('resource'),
                "current_role": details.get('role'),
                "recommended_role": recommended_role,
                "reason": "IAM_RECOMMENDER",
                # --- 修正点: recommendation_subtype を追加 ---
                "recommendation_subtype": recommendation_subtype
                # --- 修正点 ここまで ---
            })

        # 3. 発見したリスクをoverpermission_risksテーブルに書き込み
        if rows_to_insert:
            # グローバルインスタンス (bigquery_client) を使用
            # DESTINATION_TABLE_IDは環境変数から読み込まれたものを使用
            table_ref = bigquery_client.dataset(BQ_DATASET_ID, project=BQ_PROJECT_ID).table(DESTINATION_TABLE_ID)
            # テーブルスキーマが更新されていることを前提とする
            job_config = bigquery.JobConfig(write_disposition="WRITE_TRUNCATE")
            
            errors = bigquery_client.insert_rows_json(table_ref, rows_to_insert, job_config=job_config)
            
            if not errors:
                logger.info(f"Successfully wrote {len(rows_to_insert)} recommendations to BigQuery.")
            else:
                # BigQueryへの書き込みエラーの詳細を出力
                logger.error(f"BigQuery insert errors occurred: {errors}")
                raise Exception(f"BigQuery insert errors: {errors}")
        else:
            logger.info("No overpermission recommendations found from IAM Recommender.")
            
    except Exception as e:
        logger.error(f"An unexpected error occurred during overpermission analysis: {e}", exc_info=True) # exc_info=True を追加してスタックトレースを出力
        raise

def _parse_recommended_role(operations):
    """
    Recommenderのオペレーションリストから、推奨される新しいロールを抽出する。
    """
    # operations が存在しない、またはリストでない場合は早期リターン
    if not operations or not isinstance(operations, list):
        return None # "unknown" ではなく None を返す方が良い場合も

    for op in operations:
        # op が必要な属性を持っているか確認
        if hasattr(op, 'action') and hasattr(op, 'path') and hasattr(op, 'value'):
             if op.action == 'replace' and '/bindings/role' in op.path and hasattr(op.value, 'string_value'):
                 return op.value.string_value
    return None # 見つからなかった場合も None を返す