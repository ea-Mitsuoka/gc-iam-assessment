# ./src/dispatcher/main.py
import os
import json
import functions_framework
import datetime
# 修正点: クラスではなく、グローバルインスタンスを直接インポート
from utils.gcp_clients import asset_client, publisher_client
from utils.logging_handler import get_logger

# --------------------------------------------------
# 環境変数から設定を読み込み
# --------------------------------------------------
HOST_PROJECT_ID = os.getenv('GCP_PROJECT')
SCOPES_JSON = os.getenv('ASSESSMENT_SCOPES', '[]')
ASSESSOR_TOPICS_JSON = os.getenv('ASSESSOR_TOPIC_NAMES', '{}')

# 修正点: ロガーのみ初期化
logger = get_logger(__name__)

# 修正点: 環境変数のパースとバリデーションをグローバルスコープで行う
try:
    ASSESSMENT_SCOPES = json.loads(SCOPES_JSON)
    ASSESSOR_TOPICS = json.loads(ASSESSOR_TOPICS_JSON)

    if not isinstance(ASSESSMENT_SCOPES, list) or not ASSESSMENT_SCOPES:
        logger.critical("ASSESSMENT_SCOPES is not a valid list or is empty.")
        raise ValueError("ASSESSMENT_SCOPES must be a non-empty list.")

    if not isinstance(ASSESSOR_TOPICS, dict) or not ASSESSOR_TOPICS:
        logger.critical("ASSESSOR_TOPIC_NAMES is not a valid dict or is empty.")
        raise ValueError("ASSESSOR_TOPIC_NAMES must be a non-empty dict.")

except json.JSONDecodeError as e:
    logger.critical(f"Failed to parse environment variables: {e}")
    # Function起動時にパース失敗したら、後続の処理は実行不可能
    raise ValueError(f"Invalid JSON in environment variables: {e}")

# --------------------------------------------------
# 設定値の定義
# --------------------------------------------------
ASSET_TYPE_TO_ASSESSOR_MAP = {
    "bigquery.googleapis.com/Dataset": "bq-assessor",
    "storage.googleapis.com/Bucket": "gcs-assessor",
    "compute.googleapis.com/Instance": "compute-assessor",
}


@functions_framework.cloud_event
def discover_and_dispatch_assets(cloud_event):
    """
    指定されたスコープ内のターゲットアセットを検索し、対応する評価Functionの
    Pub/Subトピックにメッセージをディスパッチする。
    """
    logger.info(f"Starting discovery for scopes: {ASSESSMENT_SCOPES}")

    try:
        # --- ここからがメインの処理 ---
        current_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

        for scope in ASSESSMENT_SCOPES:
            logger.info(f"Processing scope: {scope}")
            try:
                # グローバルインスタンス (asset_client) を使用
                response = asset_client.search_all_resources(
                    request={"scope": scope, "asset_types": ASSET_TYPE_TO_ASSESSOR_MAP.keys()},
                    timeout=300.0
                )

                for resource in response:
                    assessor_name = ASSET_TYPE_TO_ASSESSOR_MAP.get(resource.asset_type)
                    topic_name = ASSESSOR_TOPICS.get(assessor_name)

                    if not topic_name:
                        logger.warning(f"Warning: No topic found for assessor '{assessor_name}'. Skipping resource {resource.name}")
                        continue

                    # グローバルインスタンス (publisher_client) を使用
                    topic_path = publisher_client.topic_path(HOST_PROJECT_ID, topic_name)
                    message_payload = {
                        "scope": scope,
                        "resource_name": resource.name,
                        "assessment_timestamp": current_timestamp
                    }
                    message_data = json.dumps(message_payload).encode("utf-8")

                    future = publisher_client.publish(topic_path, message_data)
                    future.result()

            except Exception as e:
                logger.error(f"Error processing scope {scope}: {e}")
                continue
        # --- ここまでがメインの処理 ---

    except Exception as e:
        logger.error(f"An unexpected error occurred during main processing: {e}")
        raise

    logger.info("All scopes processed.")
