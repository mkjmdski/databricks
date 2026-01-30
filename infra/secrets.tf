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

# filled manually on terraform ui to propagate to databricks secrets from there
variable "mysql_username" {
  description = "MySQL username"
  type        = string
}
variable "mysql_password" {
  description = "MySQL password"
  type        = string
  sensitive   = true
}
variable "mysql_host" {
  description = "MySQL host"
  type        = string
}
variable "mysql_db" {
  description = "MySQL database name"
  type        = string
}
