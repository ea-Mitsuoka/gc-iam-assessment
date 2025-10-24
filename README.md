# `terraform apply`は、**2つのフェーズに分けて、必ず指定された順番で**実行する必要があります。

-----

## \#\# 手順1: 【プロジェクト作成者】 `0-project-factory`の実行 🏭

この最初のステップでは、評価ツールをホストするための**専用プロジェクト**と、後の自動化で使う**サービスアカウント**を作成します。

1.  **ディレクトリを移動します。**

    ```bash
    cd terraform/0-project-factory
    ```

2.  **設定ファイルを作成します。**
    `terraform.tfvars`という名前のファイルを作成し、あなたの環境に合わせて以下の内容を記述します。

    ```hcl
    # terraform/0-project-factory/terraform.tfvars

    org_id       = "123456789012" # あなたの組織ID
    folder_id    = "345678901234" # プロジェクトを作成したいフォルダID
    project_name = "iam-assessment-tool-host"
    ```

3.  **Terraformを初期化して実行します。**

    ```bash
    terraform init
    terraform apply
    ```

4.  **出力値を必ず控えてください。**
    実行が完了すると、画面に`outputs`として以下の3つの値が表示されます。これらは次のステップで必須です。

      * `audit_project_id`
      * `terraform_state_bucket_name`
      * `terraform_executor_sa_email`

-----

### \#\# 手動手順: 【課金管理者】 課金アカウントの紐付け 💳

Terraformの作業から一旦離れ、課金管理者が手動で作業を行います。

1.  【プロジェクト作成者】は、手順1で控えた`audit_project_id`を【課金管理者】に伝えます。
2.  【課金管理者】は、Google Cloudコンソールまたは以下の`gcloud`コマンドで、プロジェクトに課金アカウントをリンクします。
    ```bash
    gcloud beta billing projects link [控えておいたプロジェクトID] --billing-account=[あなたの課金アカウントID]
    ```

-----

### \#\# 手順2: 【開発者】 `1-iam-assessor-deployment`の実行 🚀

このステップで、実際の評価ツール群（Cloud Functions, BigQueryなど）をデプロイします。

1.  **ディレクトリを移動します。**

    ```bash
    cd terraform/1-iam-assessor-deployment
    ```

2.  **`backend.tf`を編集します。**
    [cite\_start]`terraform/1-iam-assessor-deployment/backend.tf`ファイルを開き、`bucket`の値を手順1で控えた`terraform_state_bucket_name`に書き換えます [cite: 373]。

    ```hcl
    # terraform/1-iam-assessor-deployment/backend.tf
    terraform {
      backend "gcs" {
        bucket = "[手順1で控えたバケット名]" # ◀◀ ここを書き換える
        prefix = "iam_assessor_deployment"
      }
    }
    ```

3.  **設定ファイルを作成します。**
    `terraform.tfvars.example`を参考に`terraform.tfvars`ファイルを作成し、手順1で控えた値と、あなたの評価要件に合わせて内容を記述します。

    ```hcl
    # terraform/1-iam-assessor-deployment/terraform.tfvars

    # 手順1の出力値
    project_id                    = "[手順1で控えたプロジェクトID]"
    terraform_service_account_email = "[手順1で控えたサービスアカウントのメールアドレス]"

    # 評価モードを選択 (どちらかのブロックを有効化)
    assessment_scope = "ORGANIZATION"
    org_id           = "123456789012"

    # assessment_scope   = "PROJECT"
    # target_project_ids = ["your-target-project-id"]

    # その他の設定...
    table_schemas = { ... }
    enabled_apis = [ ... ]
    assessment_functions = { ... }
    ```

4.  **Terraformを初期化して実行します。**

    ```bash
    terraform init
    terraform apply
    ```

以上の手順で、評価ツール全体のデプロイが完了します。

-----

### \#\# その他: 【開発者】 Terraformコードをコミットする前の実行

  - `terraform fmt -recursive`: Terraformコードを公式の規約に沿って自動整形します。これにより、コードのスタイルが常に統一されます。

  - `terraform validate`: コードの構文が正しいかをチェックします。applyする前の基本的な健全性チェックとして有効です。