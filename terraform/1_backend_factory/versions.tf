terraform {
  # "~>" を使い、意図しないメジャー/マイナーアップデートを防ぎます
  required_version = "~> 1.12.2"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.48.0"
    }
  }
}