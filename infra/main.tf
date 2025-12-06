locals {
  subdirectory = "wheelie"
}


resource "databricks_notebook" "this" {
  for_each = {
    pull_wheelie = {
      notebook_filename = "pull_wheelie.py"
      language          = "PYTHON"
    }
  }
  path     = "${data.databricks_current_user.me.home}/${local.subdirectory}/${each.key}"
  language = each.value.language
  source   = "../notebooks/${each.value.notebook_filename}"
}

output "notebook_url" {
  value = databricks_notebook.this.url
}
