# https://docs.databricks.com/aws/en/dev-tools/terraform/
terraform {
    backend "remote" {
        hostname = "app.terraform.io"
        organization = "uam"

        workspaces {
        name = "databricks"
        }
    }

    required_providers {
        databricks = {
            source  = "databricks/databricks"
            version = "1.99.0"
        }
    }
}
