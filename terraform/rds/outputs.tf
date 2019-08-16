// password value
output "db-password" {
  value = "${random_string.password.result}"
}
