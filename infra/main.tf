locals {
  subdirectory = "wheelie"
  notebooks = {
    pull_wheelie = {
      notebook_filename = "pull_wheelie.ipynb"
      language          = "PYTHON"
    }
  }
}


resource "databricks_notebook" "this" {
  for_each = local.notebooks
  path     = "${data.databricks_current_user.me.home}/${local.subdirectory}/${each.key}"
  language = each.value.language
  content_base64 = base64encode(file("${path.module}/../notebooks/${each.value.notebook_filename}"))
}

output "notebook_urls" {
  value = { for k, v in databricks_notebook.this : k => databricks_notebook.this[k].url }
}
