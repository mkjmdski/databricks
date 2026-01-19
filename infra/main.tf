locals {
  subdirectory = "wheelie"
  notebooks = {
    pull_wheelie = {
      notebook_filename = "pull_wheelie.ipynb"
      language          = "PYTHON"
    }
  }
}

removed {
  from = databricks_notebook.this
  lifecycle {
    destroy = false
  }
}

resource "databricks_repo" "nutter_in_home" {
  url = "https://github.com/mkjmdski/databricks.git"
  path = "/Users/a662d958-d69f-42df-b30c-66cb1c96944e/wheelie-repo"
}
