import os
import gspread
import pandas as pd
import json
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# --- 設定値 ---
TFVARS_PATH = 'terraform/1_iam_assessor_deployment/terraform.tfvars'
SHEET_NAME = 'IAM Assessment Tool Specification'
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
CLIENT_SECRET_FILE = 'tools/client_secrets.json'
TOKEN_FILE = 'tools/token.json'

# --- 認証 ---
def authenticate_with_user_account():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
    return gspread.authorize(creds)

# --- シート読み込み ---
def read_sheet_as_df(spreadsheet, sheet_name):
    print(f"Reading sheet: '{sheet_name}'...")
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        # 空の行を除外して読み込むように改善 (ヘッダー行は1行目と仮定)
        records = worksheet.get_all_records(head=1, empty2zero=False, value_render_option='UNFORMATTED_VALUE')
        # 全ての列が空またはNoneである行を除外
        records = [r for r in records if any(str(v).strip() for v in r.values())]
        df = pd.DataFrame(records)
        # FunctionName列が空の行も除外 (必須列とする) - Functionsシートのみ適用
        if sheet_name == 'Functions' and 'FunctionName' in df.columns:
            df = df.dropna(subset=['FunctionName'])
            df = df[df['FunctionName'] != '']
        # RuleID列が空の行も除外 (必須列とする) - SoDRulesシートのみ適用
        elif sheet_name == 'SoDRules' and 'RuleID' in df.columns:
            df = df.dropna(subset=['RuleID'])
            df = df[df['RuleID'] != '']
        # TableName列が空の行も除外 (必須列とする) - Tablesシートのみ適用
        elif sheet_name == 'Tables' and 'TableName' in df.columns:
            df = df.dropna(subset=['TableName'])
            df = df[df['TableName'] != '']
        return df
    except gspread.WorksheetNotFound:
        print(f"Warning: Worksheet '{sheet_name}' not found.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading sheet '{sheet_name}': {e}")
        return pd.DataFrame()


# --- HCL生成ヘルパー ---
def generate_hcl_string(df, map_name, key_col, value_col):
    hcl_parts = [f"{map_name} = {{"]
    for _, row in df.iterrows():
        key = row.get(key_col)
        value = row.get(value_col)
        # キーと値が両方存在し、空でない場合のみ追加
        if pd.notna(key) and key != '' and pd.notna(value) and value != '':
            hcl_parts.append(f'  "{key}" = "{value}"')
    hcl_parts.append("}")
    return "\n".join(hcl_parts)

# sod_rules_json を引数として受け取るように変更済
def generate_functions_hcl(df, sod_rules_json=None):
    """assessment_functionsのHCLを生成する"""
    hcl_parts = ["assessment_functions = {"]
    for _, row in df.iterrows():
        name = row.get('FunctionName')
        # FunctionNameがない、または空の行はスキップ
        if not name or pd.isna(name): continue

        hcl_parts.append(f'  "{name}" = {{')
        hcl_parts.append(f'    category = "{row.get("Category", "")}"')
        if pd.notna(row.get('Path')) and row.get('Path'): hcl_parts.append(f'    path = "{row.get("Path")}"')
        if pd.notna(row.get('Entry')) and row.get('Entry'): hcl_parts.append(f'    entry = "{row.get("Entry")}"')

        # IAMRoles列を処理
        roles_str = row.get('IAMRoles', '')
        roles = roles_str.strip().split('\n') if pd.notna(roles_str) else []
        hcl_parts.append(f'    service_account_roles = {json.dumps([r.strip() for r in roles if r.strip()])}')

        # EnvironmentVariables列を処理
        env_vars_json_from_sheet = row.get('EnvironmentVariables')
        env_vars_dict = {}
        if pd.notna(env_vars_json_from_sheet) and env_vars_json_from_sheet:
            try:
                env_vars_dict = json.loads(env_vars_json_from_sheet)
                if not isinstance(env_vars_dict, dict):
                    print(f"*** WARNING: EnvironmentVariables for '{name}' is not a valid JSON object. Resetting to empty. Value: {env_vars_json_from_sheet}")
                    env_vars_dict = {} # 不正な場合は空にする
            except json.JSONDecodeError:
                print(f"*** WARNING: Skipping EnvironmentVariables for '{name}'. Invalid JSON: {env_vars_json_from_sheet}")
                env_vars_dict = {} # パース失敗時も空にする

        # sod-analyzerの場合、生成したSOD_RULES_JSONをマージ
        if name == 'sod-analyzer' and sod_rules_json:
            # Functionsシート側の定義を優先せず、常に上書きする
            env_vars_dict['SOD_RULES_JSON'] = sod_rules_json

        # マージ後の環境変数を出力 (空でない場合のみ)
        if env_vars_dict:
            env_hcl_parts = ["    environment_variables = {"]
            for k, v in env_vars_dict.items():
                env_hcl_parts.append(f'      "{k}" = {json.dumps(v)}')
            env_hcl_parts.append("    }")
            hcl_parts.append("\n".join(env_hcl_parts))

        hcl_parts.append('  }')
    hcl_parts.append("}")
    return "\n".join(hcl_parts)

# FunctionsシートのDataFrameを受け取るように変更済
def generate_schedulers_hcl(df):
    """scheduler_configsのHCLを生成する (Functionsシートから)"""
    hcl_parts = ["scheduler_configs = {"]
    for _, row in df.iterrows():
        name = row.get('FunctionName') # FunctionNameをキーとして使用
        schedule = row.get('Schedule')

        # FunctionNameがあり、かつScheduleが空でない行のみ処理
        if pd.notna(name) and name != '' and pd.notna(schedule) and schedule != '':
            hcl_parts.append(f'  "{name}" = {{')
            hcl_parts.append(f'    description = "{row.get("SchedulerDescription", "")}"')
            hcl_parts.append(f'    schedule    = "{schedule}"')
            hcl_parts.append('  }')
    hcl_parts.append("}")
    return "\n".join(hcl_parts)


# --- メイン処理 ---
if __name__ == "__main__":
    client = authenticate_with_user_account()
    try:
        spreadsheet = client.open(SHEET_NAME)
    except gspread.SpreadsheetNotFound:
        print(f"Error: Spreadsheet '{SHEET_NAME}' not found.")
        exit()

    all_hcl_parts = []

    # 1. Settingsシートを処理
    settings_df = read_sheet_as_df(spreadsheet, "Settings")
    settings = {} # settings変数を初期化
    if not settings_df.empty:
        settings = pd.Series(settings_df.Value.values, index=settings_df.Key).to_dict()

        # --- 外部依存の設定 ---
        all_hcl_parts.append("# --------------------------------------------------")
        all_hcl_parts.append("# 外部依存の設定")
        all_hcl_parts.append("# --------------------------------------------------")
        if 'gsuite_customer_id' in settings:
            all_hcl_parts.append(f'gsuite_customer_id = "{settings.pop("gsuite_customer_id", "")}"')
        all_hcl_parts.append("\n")
        # --- 共通で設定が必要な変数 ---
        all_hcl_parts.append("# --------------------------------------------------")
        all_hcl_parts.append("# 共通で設定が必要な変数")
        all_hcl_parts.append("# --------------------------------------------------")
        if 'project_id' in settings:
            all_hcl_parts.append(f'project_id = "{settings.pop("project_id", "")}"')
        if 'terraform_service_account_email' in settings:
            all_hcl_parts.append(f'terraform_service_account_email = "{settings.pop("terraform_service_account_email", "")}"')
        all_hcl_parts.append("\n")
        # --- 実行モードの設定 ---
        all_hcl_parts.append("# --------------------------------------------------")
        all_hcl_parts.append("# 実行モードの設定")
        all_hcl_parts.append("# --------------------------------------------------")
        if 'assessment_scope' in settings:
            all_hcl_parts.append(f'assessment_scope = "{settings.pop("assessment_scope", "ORGANIZATION")}"')
        if 'org_id' in settings:
            all_hcl_parts.append(f'org_id           = "{settings.pop("org_id", "")}"')
        if 'target_project_ids' in settings:
            value = settings.pop('target_project_ids', '')
            items = [item.strip() for item in str(value).split(',') if item.strip()]
            all_hcl_parts.append(f'target_project_ids = {json.dumps(items)}')
        all_hcl_parts.append("\n")
        # --- インフラ設定 ---
        all_hcl_parts.append("# --------------------------------------------------")
        all_hcl_parts.append("# ⚙️ インフラ設定 (外部から変更可能になったパラメータ)")
        all_hcl_parts.append("# --------------------------------------------------")
        if 'enabled_apis' in settings:
            value = settings.pop('enabled_apis', '')
            items = [item.strip() for item in str(value).split(',') if item.strip()]
            all_hcl_parts.append(f'enabled_apis = {json.dumps(items)}')
        if 'bq_dataset_location' in settings:
            all_hcl_parts.append(f'bq_dataset_location = "{settings.pop("bq_dataset_location", "US")}"')
        if 'bq_dataset_id' in settings:
            all_hcl_parts.append(f'bq_dataset_id = "{settings.pop("bq_dataset_id", "iam_assessment_results")}"')
        if 'enable_force_destroy' in settings:
             force_destroy_val = str(settings.pop("enable_force_destroy", "false")).lower()
             all_hcl_parts.append(f'enable_force_destroy = {force_destroy_val}')
        all_hcl_parts.append("\n")
        # scheduler_time_zone は後で処理

    # 2. Tablesシートを処理 (修正: スクリプトは既に対応済み)
    tables_df = read_sheet_as_df(spreadsheet, "Tables")
    if not tables_df.empty:
        all_hcl_parts.append("# --------------------------------------------------")
        all_hcl_parts.append("# テーブル名とスキーマファイルのマッピング")
        all_hcl_parts.append("# --------------------------------------------------")
        all_hcl_parts.append(generate_hcl_string(tables_df, "table_schemas", "TableName", "SchemaPath"))
        all_hcl_parts.append("\n")
    else:
        # Tablesシートが空でもブロックだけは出力する (Terraform変数定義との整合性のため)
        all_hcl_parts.append("# --------------------------------------------------")
        all_hcl_parts.append("# テーブル名とスキーマファイルのマッピング")
        all_hcl_parts.append("# --------------------------------------------------")
        all_hcl_parts.append("table_schemas = {}")
        all_hcl_parts.append("\n")


    # SoDRulesシートを読み込み、JSON文字列を生成
    sod_rules_df = read_sheet_as_df(spreadsheet, "SoDRules")
    sod_rules_json_string = "[]" # デフォルト
    if not sod_rules_df.empty:
        expected_cols = ['RuleID', 'Description', 'Role1', 'Role2', 'Role1Type', 'Role2Type']
        rename_map = {
            'RuleID': 'rule_id', 'Description': 'description',
            'Role1': 'role1', 'Role2': 'role2',
            'Role1Type': 'role1_type', 'Role2Type': 'role2_type'
        }
        cols_to_use = [col for col in expected_cols if col in sod_rules_df.columns]
        if 'Role1' in cols_to_use and 'Role2' in cols_to_use: # Role1/2は必須
            sod_rules_list = []
            temp_df = sod_rules_df[cols_to_use].rename(columns=rename_map)
            for record in temp_df.to_dict('records'):
                roles1_str = record.get('role1', '')
                record['role1'] = [r.strip() for r in str(roles1_str).strip().split('\n') if r.strip()]
                roles2_str = record.get('role2', '')
                record['role2'] = [r.strip() for r in str(roles2_str).strip().split('\n') if r.strip()]
                if record.get('rule_id') and record['role1'] and record['role2']: # IDとRole1/2リストが空でない
                    sod_rules_list.append(record)
                else:
                    rule_id_log = record.get('rule_id', 'UNKNOWN')
                    print(f"*** WARNING: Skipping SoD rule '{rule_id_log}' due to missing ID or empty roles.")

            if sod_rules_list:
                sod_rules_json_string = json.dumps(sod_rules_list)
                print(f"Generated SOD_RULES_JSON from SoDRules sheet.")
            else:
                print("SoDRules sheet found, but no valid rules parsed. SOD_RULES_JSON will be '[]'.")
        else:
             print("SoDRules sheet found, but lacks required 'Role1' or 'Role2' columns. SOD_RULES_JSON will be '[]'.")
    else:
        print("SoDRules sheet is empty or not found. SOD_RULES_JSON will be '[]'.")

    # 3. Functionsシートを処理 (Function定義 + Scheduler定義)
    functions_df = read_sheet_as_df(spreadsheet, "Functions")
    if not functions_df.empty:
        all_hcl_parts.append("# --------------------------------------------------")
        all_hcl_parts.append("# 🚀 評価Functionの定義")
        all_hcl_parts.append("# --------------------------------------------------")
        # 生成したsod_rules_json_stringを渡す
        all_hcl_parts.append(generate_functions_hcl(functions_df, sod_rules_json_string))
        all_hcl_parts.append("\n")

        # --- Scheduler定義 (修正: スクリプトは既に対応済み) ---
        all_hcl_parts.append("# --------------------------------------------------")
        all_hcl_parts.append("# ⏰ Schedulerジョブの定義")
        all_hcl_parts.append("# --------------------------------------------------")
        if 'scheduler_time_zone' in settings:
             all_hcl_parts.append(f'scheduler_time_zone = "{settings.pop("scheduler_time_zone", "Asia/Tokyo")}"')
        # generate_schedulers_hcl に functions_df を渡す
        all_hcl_parts.append(generate_schedulers_hcl(functions_df))
        all_hcl_parts.append("\n")
    else:
        # Functionsシートが空の場合もブロックだけは出力
        all_hcl_parts.append("# --------------------------------------------------")
        all_hcl_parts.append("# 🚀 評価Functionの定義")
        all_hcl_parts.append("# --------------------------------------------------")
        all_hcl_parts.append("assessment_functions = {}")
        all_hcl_parts.append("\n")
        all_hcl_parts.append("# --------------------------------------------------")
        all_hcl_parts.append("# ⏰ Schedulerジョブの定義")
        all_hcl_parts.append("# --------------------------------------------------")
        if 'scheduler_time_zone' in settings:
             all_hcl_parts.append(f'scheduler_time_zone = "{settings.pop("scheduler_time_zone", "Asia/Tokyo")}"')
        all_hcl_parts.append("scheduler_configs = {}")
        all_hcl_parts.append("\n")


    # 4. Settingsシートの残り (未処理の設定があれば出力)
    if settings: # settings辞書が空でない場合
        all_hcl_parts.append("# --------------------------------------------------")
        all_hcl_parts.append("# その他の未分類設定 (Settingsシートより)")
        all_hcl_parts.append("# --------------------------------------------------")
        for key, value in settings.items():
             # 値が空の場合もそのまま出力 (Terraform側でデフォルト値が使われることを期待)
             all_hcl_parts.append(f'{key} = "{value}"')
        all_hcl_parts.append("\n")

    # 5. tfvarsファイルに書き込み
    print(f"Writing all configurations to {TFVARS_PATH}...")
    with open(TFVARS_PATH, 'w', encoding='utf-8') as f:
            f.write("# ------------------------------------------------------------------\n")
            f.write("# WARNING: このファイルは自動生成されました。\n")
            f.write("# 手動で編集せず、Googleスプレッドシートを更新した上で\n")
            f.write("# `python tools/generate_tfvars.py` を実行してください。\n")
            f.write("# ------------------------------------------------------------------\n\n")
            # リストの最後の要素が空行だった場合に備えてrstrip()を追加し、末尾に改行を追加
            f.write("\n".join(all_hcl_parts).rstrip() + "\n")
    print("tfvars file generation complete.")
