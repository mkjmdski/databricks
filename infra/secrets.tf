resource "databricks_secret_scope" "app" {
  name = "wheelie"
}

resource "databricks_secret" "secrets" {
  for_each = {
    MYSQL_USERNAME = var.mysql_username
    MYSQL_PASSWORD = var.mysql_password
    MYSQL_HOST     = var.mysql_host
    MYSQL_DB       = var.mysql_db
  }
  key          = each.key
  string_value = each.value
  scope        = databricks_secret_scope.app.id
}