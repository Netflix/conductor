provider "aws" {
  region                  = "${var.region}"
  profile                 = "${module.vars.aws-profile}"
  shared_credentials_file = "${pathexpand("~/.aws/credentials")}"
}

provider "vault" {
  address = "http://vault.service.${module.vars.tld}:8200"
}

terraform {
  backend "s3" {
    profile = "deluxeone-corp-live"
    bucket  = "dlx-tf-state"
    region  = "us-west-2"
    encrypt = true
  }
}
