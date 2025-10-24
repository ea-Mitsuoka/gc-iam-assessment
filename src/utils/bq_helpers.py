import os
from google.cloud import bigquery
from .gcp_clients import bigquery_client
from .logging_handler import get_logger
# 変更点: ロガーを初期化
logger = get_logger(__name__)

def run_query_and_save_results(query: str, destination_table_id: str, write_disposition: str, max_bytes_billed: int = None):
    """
    指定されたクエリを実行し、結果を指定テーブルに保存するヘルパー関数
    """
    dataset_id = os.getenv('BQ_DATASET_ID')
    dest_table_ref = bigquery_client.dataset(dataset_id).table(destination_table_id)
    
    job_config = bigquery.QueryJobConfig(
        destination=dest_table_ref,
        write_disposition=write_disposition,
        maximum_bytes_billed=max_bytes_billed
    )
    
    query_job = bigquery_client.query(query, job_config=job_config)
    query_job.result() # 完了を待つ
    
    # 変更点: print() を logger.info() に置き換え、構造化された情報をextraで渡す
    logger.info(
        f"Query completed and results saved to {destination_table_id}.",
        extra={
            "affected_rows": query_job.num_dml_affected_rows,
            "destination_table": destination_table_id
        }
    )