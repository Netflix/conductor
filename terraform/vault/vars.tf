variable "region" {
  default = "us-west-2"
}

variable "enclave" {
  default = "storage"
}

variable "env" {} // test/int/uat/live

module "vars" {
  source  = "git@github.com:d3sw/terraform-platform//modules//vars?ref=v1.0.7"
  region  = "${var.region}"
  enclave = "${var.enclave}"
  env     = "${var.env}"
}

