variable "env" {
  description = "environment"
  default     = ""
}

variable "region" {
  description = "region"
  default     = "us-west-2"
}

variable "enclave" {
  default = "shared"
}

variable "service" {
  description = "name of the service"
  default     = "conductor"
}

module "vars" {
  source  = "git@github.com:d3sw/terraform-platform//modules//vars?ref=v1.0.3"
  region  = "${var.region}"
  enclave = "${var.enclave}"
  env     = "${var.env}"
}
