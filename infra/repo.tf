moved {
  from = databricks_repo.nutter_in_home
  to   = databricks_repo.main
}

resource "databricks_repo" "main" {
  url  = "https://github.com/mkjmdski/databricks.git"
  path = "/Repos/wheelie"
}
