# ./src/utils/gcp_clients.py
# 共通して使用するGoogle Cloudクライアントのクラスを定義するモジュール

from google.cloud import (
    bigquery,
    storage,
    compute_v1,
    asset_v1,
    pubsub_v1,
    identity_v1,
    recommender_v1
)

# --------------------------------------------------
# 修正点1: クラスのエイリアス (動的初期化で使うため 'Class' 接尾辞)
# --------------------------------------------------
BigQueryClientClass = bigquery.Client
StorageClientClass = storage.Client
ComputeInstancesClientClass = compute_v1.InstancesClient
AssetServiceClientClass = asset_v1.AssetServiceClient
PublisherClientClass = pubsub_v1.PublisherClient
IdentityGroupsServiceClientClass = identity_v1.GroupsServiceClient
RecommenderClientClass = recommender_v1.RecommenderClient

# --------------------------------------------------
# 修正点2: グローバルインスタンス (シングルトンとして使用)
# Cloud Functionsのベストプラクティスに基づき、ここで一元的に初期化
# --------------------------------------------------
bigquery_client = BigQueryClientClass()
storage_client = StorageClientClass()
compute_client = ComputeInstancesClientClass()
asset_client = AssetServiceClientClass()
publisher_client = PublisherClientClass()
identity_client = IdentityGroupsServiceClientClass()
recommender_client = RecommenderClientClass()


# --------------------------------------------------
# 修正点3: __all__ にインスタンスとクラスの両方を含める
# --------------------------------------------------
__all__ = [
    # クラス (例: bq_assessor で動的初期化に必要)
    "BigQueryClientClass",
    "StorageClientClass",
    "ComputeInstancesClientClass",
    "AssetServiceClientClass",
    "PublisherClientClass",
    "IdentityGroupsServiceClientClass",
    "RecommenderClientClass",
    # グローバルインスタンス
    "bigquery_client",
    "storage_client",
    "compute_client",
    "asset_client",
    "publisher_client",
    "identity_client",
    "recommender_client",
]