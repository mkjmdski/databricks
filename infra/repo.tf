moved {
  from = databricks_repo.nutter_in_home
  to   = databricks_repo.main
}

resource "databricks_repo" "main" {
  url  = "https://github.com/mkjmdski/databricks.git"
  path = "/Repos/a662d958-d69f-42df-b30c-66cb1c96944e/wheelie-repo"
}
