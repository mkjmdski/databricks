data "databricks_current_user" "this" {}

resource "databricks_repo" "main" {
  url  = "https://github.com/mkjmdski/databricks.git"
  path = "/Repos/${data.databricks_current_user.this.user_name}/wheelie"
}
