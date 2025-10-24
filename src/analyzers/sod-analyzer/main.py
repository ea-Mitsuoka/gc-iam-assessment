import os
import functions_framework
import json
from google.cloud.exceptions import NotFound # テーブル存在確認用
from utils.bq_helpers import run_query_and_save_results
from utils.logging_handler import get_logger

# --- 環境変数 ---
BQ_PROJECT_ID = os.getenv('BQ_PROJECT_ID')
BQ_DATASET_ID = os.getenv('BQ_DATASET_ID')
SOURCE_TABLE_ID = os.getenv('SOURCE_TABLE_ID') # principal_access_list
DESTINATION_TABLE_ID = os.getenv('DESTINATION_TABLE_ID') # sod_violations
WORKSPACE_ROLES_TABLE_ID = os.getenv('WORKSPACE_ROLES_TABLE_ID') # workspace_admin_roles (オプション)
SOD_RULES_JSON = os.getenv('SOD_RULES_JSON', '[]')

# 1クエリあたりの最大スキャンバイト数を設定 (例: 10GB)
MAX_BYTES_BILLED = 10 * 1024 * 1024 * 1024

logger = get_logger(__name__)

# SoDルールをパース (グローバルスコープ)
try:
    SOD_RULES = json.loads(SOD_RULES_JSON)
    if not isinstance(SOD_RULES, list):
        logger.warning("SOD_RULES_JSON is not a valid JSON array. Treating as empty list.")
        SOD_RULES = []
    # 修正点: 各ルールの role1, role2 がリストであることを確認 (文字列ならリストに変換)
    valid_rules = []
    for rule in SOD_RULES:
        if isinstance(rule, dict):
            # Role1
            role1 = rule.get('role1')
            if isinstance(role1, str):
                rule['role1'] = [role1] # 文字列ならリストに変換
            elif not isinstance(role1, list):
                rule['role1'] = [] # リストでも文字列でもなければ空リスト
            # Role2
            role2 = rule.get('role2')
            if isinstance(role2, str):
                rule['role2'] = [role2] # 文字列ならリストに変換
            elif not isinstance(role2, list):
                rule['role2'] = [] # リストでも文字列でもなければ空リスト
            # Role1, Role2 が空リストでなく、rule_id があれば有効なルールとする
            if rule.get('rule_id') and rule['role1'] and rule['role2']:
                 valid_rules.append(rule)
            else:
                 rule_id_log = rule.get('rule_id', 'UNKNOWN')
                 logger.warning(f"Invalid SoD rule skipped (missing id or empty roles): {rule_id_log}")
        else:
             logger.warning(f"Invalid item found in SOD_RULES_JSON (not a dict): {rule}")
    SOD_RULES = valid_rules # 有効なルールのみ保持

except json.JSONDecodeError:
    logger.warning("Invalid JSON format for SOD_RULES_JSON env var. Treating as empty list.")
    SOD_RULES = []

# BigQuery Client (グローバル) - テーブル存在確認で使用
from utils.gcp_clients import bigquery_client

# --- Helper Function to create SQL IN clause from list ---
def _create_sql_in_clause(role_list):
    """['roleA', 'roleB'] -> "('roleA','roleB')" """
    if not role_list:
        return "('')" # 空のリストの場合はマッチしないように
    # 各ロールをシングルクォートで囲む
    quoted_roles = [f"'{role.replace('\'', '\\\'')}'" for role in role_list] # SQLインジェクション対策でシングルクォートをエスケープ
    return f"({','.join(quoted_roles)})"

@functions_framework.cloud_event
def analyze_sod_violations(cloud_event):
    """
    principal_access_list テーブルと (オプションで) workspace_admin_roles テーブルを分析し、
    定義された職務分掌ルールに違反するプリンシパルを抽出する。
    Role1またはRole2に複数のロールが定義されている場合はOR条件で評価する。
    """
    logger.info(f"Starting Segregation of Duties (SoD) analysis...")

    if not SOURCE_TABLE_ID or not DESTINATION_TABLE_ID:
        msg = "Missing required environment variables: SOURCE_TABLE_ID and DESTINATION_TABLE_ID must be set."
        logger.error(msg)
        raise ValueError(msg)

    if not SOD_RULES:
         logger.warning("SOD_RULES_JSON is empty or invalid. No SoD rules to analyze.")
         # 空のテーブルを作成/上書き
         logger.info(f"Truncating destination table: {DESTINATION_TABLE_ID} due to no rules.")
         truncate_query = f"TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{DESTINATION_TABLE_ID}`"
         try:
             run_query_and_save_results(
                 query=truncate_query, destination_table_id=DESTINATION_TABLE_ID,
                 write_disposition="WRITE_TRUNCATE", max_bytes_billed=MAX_BYTES_BILLED
             )
         except Exception as e:
             logger.error(f"Failed to truncate table {DESTINATION_TABLE_ID}: {e}")
             # truncate失敗はエラーとして扱うか？ここでは続行しない
             raise
         return

    # --- Workspaceロールテーブルの確認 ---
    workspace_data_available = False
    workspace_table_fqn = ""
    if WORKSPACE_ROLES_TABLE_ID:
        workspace_table_fqn = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{WORKSPACE_ROLES_TABLE_ID}`"
        try:
            workspace_table_ref = bigquery_client.dataset(BQ_DATASET_ID).table(WORKSPACE_ROLES_TABLE_ID)
            bigquery_client.get_table(workspace_table_ref)
            workspace_data_available = True
            logger.info(f"Workspace roles table '{WORKSPACE_ROLES_TABLE_ID}' found.")
        except NotFound:
            logger.warning(f"Workspace roles table '{WORKSPACE_ROLES_TABLE_ID}' was specified but not found. Rules involving Workspace roles will be skipped.")
        except Exception as e:
            logger.error(f"Error checking for Workspace roles table: {e}. Rules involving Workspace roles will be skipped.")
    else:
        logger.info("WORKSPACE_ROLES_TABLE_ID is not set. Rules involving Workspace roles will be skipped.")


    try:
        source_table_fqn = f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{SOURCE_TABLE_ID}`"

        sub_queries = []
        skipped_rules = []
        for rule in SOD_RULES:
            rule_id = rule.get('rule_id')
            description = rule.get('description', 'N/A')
            # role1 と role2 はリストになっているはず
            role1_list = rule.get('role1', [])
            role2_list = rule.get('role2', [])
            role1_type = rule.get('role1_type', 'GCP_IAM').upper()
            role2_type = rule.get('role2_type', 'GCP_IAM').upper()

            # --- 基本チェック (リストが空でないか) ---
            if not rule_id or not role1_list or not role2_list:
                logger.warning(f"Skipping invalid SoD rule (missing id or empty roles list): {rule_id}")
                skipped_rules.append(rule_id if rule_id else "INVALID_RULE_MISSING_ROLES")
                continue

            # --- Workspaceデータチェック ---
            if ('WORKSPACE_ADMIN' in [role1_type, role2_type]) and not workspace_data_available:
                logger.warning(f"Skipping SoD rule '{rule_id}' because Workspace role data is unavailable or table not configured.")
                skipped_rules.append(rule_id)
                continue

            # --- SQL IN句を作成 ---
            role1_in_clause = _create_sql_in_clause(role1_list)
            role2_in_clause = _create_sql_in_clause(role2_list)

            # --- クエリ生成ロジック (Typeに応じて分岐) ---
            sub_query = ""
            # Case 1: GCP vs GCP
            if role1_type == 'GCP_IAM' and role2_type == 'GCP_IAM':
                sub_query = f"""
                (WITH Role1Principals AS (
                    SELECT DISTINCT principal_email, principal_type
                    FROM {source_table_fqn}, UNNEST(access_list) AS a WHERE a.role IN {role1_in_clause}
                ), Role2Principals AS (
                    SELECT DISTINCT principal_email
                    FROM {source_table_fqn}, UNNEST(access_list) AS a WHERE a.role IN {role2_in_clause}
                )
                SELECT
                    '{rule_id}' AS rule_id,
                    '{description}' AS description,
                    r1p.principal_email,
                    r1p.principal_type,
                    -- 代表的なロールを表示 (配列全体はassignmentsへ)
                    '{role1_list[0]}' AS conflicting_role1, -- 最初のロールを表示
                    '{role2_list[0]}' AS conflicting_role2, -- 最初のロールを表示
                    'GCP_IAM' AS role1_type,
                    'GCP_IAM' AS role2_type,
                    -- 関連する可能性のある全ての割り当て (role1リスト + role2リスト)
                    (SELECT ARRAY_AGG(STRUCT(a.resource_name, a.role))
                     FROM {source_table_fqn} AS t, UNNEST(t.access_list) AS a
                     WHERE t.principal_email = r1p.principal_email
                       AND a.role IN {_create_sql_in_clause(role1_list + role2_list)}) AS assignments
                FROM Role1Principals r1p
                INNER JOIN Role2Principals r2p ON r1p.principal_email = r2p.principal_email)
                """
            # Case 2: GCP vs Workspace
            elif role1_type == 'GCP_IAM' and role2_type == 'WORKSPACE_ADMIN':
                sub_query = f"""
                (WITH GcpUsers AS (
                    SELECT DISTINCT principal_email, principal_type
                    FROM {source_table_fqn}, UNNEST(access_list) AS a WHERE a.role IN {role1_in_clause}
                ), WorkspaceUsers AS (
                    SELECT DISTINCT principal_email
                    FROM {workspace_table_fqn} WHERE workspace_role_name IN {role2_in_clause}
                )
                SELECT
                    '{rule_id}' AS rule_id,
                    '{description}' AS description,
                    gcp.principal_email,
                    gcp.principal_type,
                    '{role1_list[0]}' AS conflicting_role1,
                    '{role2_list[0]}' AS conflicting_role2,
                    'GCP_IAM' AS role1_type,
                    'WORKSPACE_ADMIN' AS role2_type,
                    (SELECT ARRAY_AGG(STRUCT(a.resource_name, a.role)) FROM {source_table_fqn} AS t, UNNEST(t.access_list) AS a WHERE t.principal_email = gcp.principal_email AND a.role IN {role1_in_clause}) AS assignments
                FROM GcpUsers gcp
                INNER JOIN WorkspaceUsers ws ON gcp.principal_email = ws.principal_email)
                """
            # Case 3: Workspace vs GCP
            elif role1_type == 'WORKSPACE_ADMIN' and role2_type == 'GCP_IAM':
                 sub_query = f"""
                (WITH WorkspaceUsers AS (
                    SELECT DISTINCT principal_email
                    FROM {workspace_table_fqn} WHERE workspace_role_name IN {role1_in_clause}
                ), GcpUsers AS (
                    SELECT DISTINCT principal_email, principal_type
                    FROM {source_table_fqn}, UNNEST(access_list) AS a WHERE a.role IN {role2_in_clause}
                )
                SELECT
                    '{rule_id}' AS rule_id,
                    '{description}' AS description,
                    ws.principal_email,
                    gcp.principal_type,
                    '{role1_list[0]}' AS conflicting_role1,
                    '{role2_list[0]}' AS conflicting_role2,
                    'WORKSPACE_ADMIN' AS role1_type,
                    'GCP_IAM' AS role2_type,
                    (SELECT ARRAY_AGG(STRUCT(a.resource_name, a.role)) FROM {source_table_fqn} AS t, UNNEST(t.access_list) AS a WHERE t.principal_email = ws.principal_email AND a.role IN {role2_in_clause}) AS assignments
                FROM WorkspaceUsers ws
                INNER JOIN GcpUsers gcp ON ws.principal_email = gcp.principal_email)
                """
            # Case 4: Workspace vs Workspace
            elif role1_type == 'WORKSPACE_ADMIN' and role2_type == 'WORKSPACE_ADMIN':
                 sub_query = f"""
                 (WITH Role1Users AS (
                    SELECT DISTINCT principal_email FROM {workspace_table_fqn} WHERE workspace_role_name IN {role1_in_clause}
                 ), Role2Users AS (
                    SELECT DISTINCT principal_email FROM {workspace_table_fqn} WHERE workspace_role_name IN {role2_in_clause}
                 )
                 SELECT
                    '{rule_id}' AS rule_id,
                    '{description}' AS description,
                    r1.principal_email,
                    'USER' AS principal_type, -- 仮定
                    '{role1_list[0]}' AS conflicting_role1,
                    '{role2_list[0]}' AS conflicting_role2,
                    'WORKSPACE_ADMIN' AS role1_type,
                    'WORKSPACE_ADMIN' AS role2_type,
                    -- Workspace割り当て (代表ロールを配列に) - assignments スキーマに合わせる
                    [STRUCT(CAST(NULL AS STRING), '{role1_list[0]}'), STRUCT(CAST(NULL AS STRING), '{role2_list[0]}')] AS assignments
                 FROM Role1Users r1
                 INNER JOIN Role2Users r2 ON r1.principal_email = r2.principal_email)
                 """
            # Unsupported Case
            else:
                 logger.warning(f"Unsupported role type combination for rule '{rule_id}' ({role1_type} vs {role2_type}). Skipping.")
                 skipped_rules.append(rule_id)
                 continue

            if sub_query:
                sub_queries.append(sub_query)

        # --- クエリの結合と実行 ---
        final_query = ""
        if sub_queries:
            final_query = "\nUNION ALL\n".join(sub_queries)

        if skipped_rules:
            skipped_query_parts = []
            assignments_schema = "CAST([] AS ARRAY<STRUCT<resource_name STRING, role STRING>>)"
            for skipped_rule_id in skipped_rules:
                 rule_info = next((r for r in SOD_RULES if r.get('rule_id') == skipped_rule_id), {})
                 desc = rule_info.get('description', 'N/A') + " (SKIPPED_RULE_INVALID_OR_DATA_UNAVAILABLE)"
                 # リストの最初の要素を表示（存在すれば）
                 r1 = rule_info.get('role1', ['N/A'])[0]
                 r2 = rule_info.get('role2', ['N/A'])[0]
                 r1t = rule_info.get('role1_type', 'N/A')
                 r2t = rule_info.get('role2_type', 'N/A')
                 skipped_query_parts.append(f"SELECT '{skipped_rule_id}', '{desc}', CAST(NULL AS STRING), CAST(NULL AS STRING), '{r1}', '{r2}', '{r1t}', '{r2t}', {assignments_schema}")

            if skipped_query_parts:
                 skipped_query = "\nUNION ALL\n".join(skipped_query_parts)
                 if final_query:
                      final_query = f"{final_query}\nUNION ALL\n{skipped_query}"
                 else:
                      final_query = skipped_query

        if not final_query:
            logger.warning("No SoD rules were processed or generated query is empty.")
            logger.info(f"Truncating destination table: {DESTINATION_TABLE_ID}")
            truncate_query = f"TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{DESTINATION_TABLE_ID}`"
            try:
                run_query_and_save_results(
                    query=truncate_query, destination_table_id=DESTINATION_TABLE_ID,
                    write_disposition="WRITE_TRUNCATE", max_bytes_billed=MAX_BYTES_BILLED
                )
            except Exception as e:
                logger.error(f"Failed to truncate table {DESTINATION_TABLE_ID}: {e}")
                raise
            return

        logger.info(f"Executing SoD analysis query and saving to {DESTINATION_TABLE_ID}")
        run_query_and_save_results(
            query=final_query,
            destination_table_id=DESTINATION_TABLE_ID,
            write_disposition="WRITE_TRUNCATE",
            max_bytes_billed=MAX_BYTES_BILLED
        )

        logger.info(f"Successfully completed SoD analysis.")
        if skipped_rules:
            logger.warning(f"Skipped/Invalid SoD rules: {skipped_rules}")

    except Exception as e:
        logger.error(f"An unexpected error occurred during SoD analysis: {e}", exc_info=True)
        raise
