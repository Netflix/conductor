data "aws_region" "current" {}

data "aws_caller_identity" "current" {}

# toggle this section based on whether service-linked role exists or not
#resource "aws_iam_service_linked_role" "es" {
#  aws_service_name = "es.amazonaws.com"
#}

resource "aws_elasticsearch_domain" "es-live" {
  domain_name           = "${var.domain}"
  elasticsearch_version = "${var.AES_VER}"
  cluster_config {
    instance_type = "m4.large.elasticsearch"
    instance_count = "2"
    dedicated_master_enabled = "true"
    dedicated_master_count = "3"
    dedicated_master_type = "m3.medium.elasticsearch"
    zone_awareness_enabled = "true"  
  }

  vpc_options {
      subnet_ids = ["${element(split(",", module.vars.private_subnets), 0)}", "${element(split(",", module.vars.private_subnets), 1)}"]
      security_group_ids = ["${aws_security_group.conductor-aes-sg.id}"]
  }
  ebs_options {
      ebs_enabled = "${var.EBSS}"
      volume_type = "io1"
      volume_size = "35"
      iops = "1000"
    }

  advanced_options {
    "rest.action.multi.allow_explicit_index" = "true"
    "indices.fielddata.cache.size" = "10"
    "indices.query.bool.max_clause_count" = "1024"
  }

  access_policies = <<CONFIG
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "*"
      },
      "Action": "es:*",
      "Resource": "arn:aws:es:us-west-2:${module.vars.account-id}:domain/${var.domain}/*"
    }
  ]
}
CONFIG

  snapshot_options {
    automated_snapshot_start_hour = 23
  }

  tags {
    Domain = "${var.D_TAG}"
  }
}

resource "aws_elasticsearch_domain" "es-dev" {
  domain_name           = "${var.domain}"
  elasticsearch_version = "${var.AES_VER}"
  cluster_config {
    instance_type = "${var.INSTT}"
    instance_type = "m4.large.elasticsearch"
    instance_count = "3"
    zone_awareness_enabled = "${var.ESZA}"    
  }

  vpc_options {
      subnet_ids = ["${element(split(",", module.vars.private_subnets), 0)}"]
      security_group_ids = ["${aws_security_group.conductor-aes-sg.id}"]
  }
  ebs_options {
      ebs_enabled = "${var.EBSS}"
      volume_type = "gp2"
      volume_size = "10"
    }

  advanced_options {
    "rest.action.multi.allow_explicit_index" = "true"
    "indices.fielddata.cache.size" = "10"
    "indices.query.bool.max_clause_count" = "1024"
  }

  access_policies = <<CONFIG
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "*"
      },
      "Action": "es:*",
      "Resource": "arn:aws:es:us-west-2:${module.vars.account-id}:domain/${var.domain}/*"
    }
  ]
}
CONFIG

  snapshot_options {
    automated_snapshot_start_hour = 23
  }

  tags {
    Domain = "${var.D_TAG}"
  }
}

resource "aws_security_group" "conductor-aes-sg" {
  name            = "conductor-aes-${var.enclave}-${var.env}-${var.region}"
  description     = "One conductor RDS Security Group "
  vpc_id          = "${module.vars.vpc-id}"

  ingress {
    from_port     = 0
    to_port       = 0
    protocol      = "-1"
    cidr_blocks   = ["${var.allowed_subnets["all-10-priv"]}", "${var.allowed_subnets["all-172-priv"]}", "${var.allowed_subnets["dlx-coresite"]}"]
  }

  egress {
      from_port   = 0
      to_port     = 0
      protocol    = "-1"
      cidr_blocks = ["0.0.0.0/0"]
  }

  tags {
    Name          = "conductor-aes-${var.enclave}-${var.env}-${var.region}"
    Owner         = "conductor"
  }
}