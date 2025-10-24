### `assessment_functions`

前提として、このディレクトリのコードを実行する前に、0-project-factoryを完了させている必要があります
デプロイする評価Functionを定義するマップです。キーがFunction名となり、各オブジェクトは以下のキーを持ちます。

* `category` (string, 必須): Functionのソースコードが格納されているカテゴリディレクトリ。例: `"assessors/resource_centric"`
* `service_account_roles` (list(string), 必須): このFunctionのサービスアカウントに付与するIAMロールのリスト。
* `path` (string, オプション): `../src/${category}/${function_name}`という命名規則に従わない場合のみ、ソースディレクトリへのパスを明示的に指定します。
* `entry` (string, オプション): `assess_${function_name}`という命名規則に従わない場合のみ、エントリーポイント名を明示的に指定します。