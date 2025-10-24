terraform {
  backend "gcs" {
    prefix = "iam_assessor_deployment"
  }
}
