provider "aws" {
  version                 = "~> 1.60.0"
  region                  = "${var.region}"
  profile                 = "${module.vars.aws-profile}"
  shared_credentials_file = "${pathexpand("~/.aws/credentials")}"
}

# NOTE: the profile in the s3 backend may need to be first set to deluxeone-corp-live, run apply once (this will ensure existing files have the correct S3 ACL set), then set the profile to deluxeone-mgmt.
terraform {
  backend "s3" {
    profile = "deluxeone-corp-live"
    bucket  = "dlx-tf-state"
    region  = "us-west-2"
    acl     = "bucket-owner-full-control"
    encrypt = true
  }
}
