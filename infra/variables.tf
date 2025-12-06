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
    