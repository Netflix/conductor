provider "aws" {
    version = "~> 1.35.0"
    region                  = "${var.region}"
    profile                 = "${module.vars.aws-profile}"
    shared_credentials_file = "${pathexpand("~/.aws/credentials")}"
    assume_role {
        role_arn            = "${module.vars.profile-arn}"
    }
}

terraform {
  backend "s3" {
    profile = "bydeluxe-eit-cl"
    bucket  = "dlx-tf-state"
    region  = "us-west-2"
    encrypt = true
  }
}