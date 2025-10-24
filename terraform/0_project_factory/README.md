# Phase 0: Project Factory

このディレクトリのTerraformコードは、IAM評価ツールをホストするための専用Google Cloudプロジェクトと、後続のフェーズで必要となるサービスアカウントを作成します。

## 実行手順

1.  **`terraform.tfvars`の作成:**
    `terraform.tfvars.example`をコピーして`terraform.tfvars`を作成し、以下の内容をあなたの環境に合わせて編集してください。

    ```hcl
    # terraform/0_project-factory/terraform.tfvars

    org_id    = "123456789012" # あなたの組織ID
    folder_id = "345678901234" # 新しいプロジェクトを作成するフォルダID
    # region = "us-central1" # (オプション) デフォルト(asia-northeast1)以外を使いたい場合
    ```

2.  **Terraformの実行:**
    ```bash
    terraform init
    terraform apply
    ```

3.  **出力値の保存:**
    実行後に出力される`audit_project_id`, `terraform_state_bucket_name`, `terraform_executor_sa_email`の3つの値を、Phase 1の`terraform.tfvars`で使用するために必ず控えてください。