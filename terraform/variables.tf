variable "domain" {
    description = "Default Domain used, in this case default is common-aes-shared-test"
    default =  "malformed-deployment-err-1"
}

variable "service" {
    description = "Service name."
    default = "service-not-set"
}

variable "AES_VER" {
    description = "Amazon ElasticSearch Version. We use 5.6 and up for compatibility"
    default = "5.6"
}

variable "AWS_REGION" {
    description = "AWS region, default is us-west-2"
    default = "us-west-2"
}

variable "DMASTT" {
    description = "Dedicated Master Node Type. This type is best for DB instance count of 5-10"
    default = "m3.medium.elasticsearch"
}

variable "D_TAG" {
    description = "Domain Tag for targeting purposes"
    default = "terratest"
}

variable "INSTC" {
    description = "Number of instances in the cluster. Default is set to 3"
    default = 3
}

variable "INSTT" {
    description = "Instance type of data nodes in the cluster. Default is set to t2.medium.elasticsearch"
    default = "m3.medium.elasticsearch"
}

variable "TestProfile" {
    description = "This is the use-west-2-sandbox-test profile"
    default = "937904377342"
}

variable "ESZA" {
    description = "ElasticSearch Zone Awareness. When it is set to true, it will increase fault tolerance, by spreading data nodes across two AZ in the same AWS region"
    default = false
}

variable "EBSS" {
    description = "Elastic block storage Status. When it is set to true, EBS volumes will be created and attached to data node instances. Default value is true"
    default = true
}

variable "EBSVS" {
    description = "Elastic block storage volume size in Gigabytes. Minimum value is 10"
    default = 10
}

variable "EBSVT" {
    description = "Elastic block storage volume Type. default value is gp2 for as general purpose SSD"
    default = "gp2"
}

variable "region" {
  default = "us-west-2"
}

variable "enclave" {}

variable "env" {}

variable "account-id" {
    description = "AWS Account IDs"
    default = "937904377342"
}

module "vars" {
    source      = "git@github.com:d3sw/terraform-mod//vars"
    region      = "${var.region}"
    env         = "${var.enclave}"
    env-type    = "${var.env}"
    deploy-type = "none"
}

variable "allowed_subnets" {
    description = "Allowed subnets for security groups."
    default = {
      all               = [ "0.0.0.0/0" ]
      all-10-priv       = [ "10.0.0.0/8" ]
      all-172-priv      = [ "172.16.0.0/12" ]
      dlx-coresite      = [ "192.168.0.0/16" ]
      shared-test-vpc   = [ "172.31.96.0/20" ]
      shared-live-vpc   = [ "10.131.96.0/20" ]
    }
}