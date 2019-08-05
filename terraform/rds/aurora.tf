resource "aws_rds_cluster_instance" "rds_cluster_instances" {
  identifier = "conductor-aurora-postgresql-${var.enclave}-${var.env}-${count.index}"

  # instance Options
  count              = "${var.cluster_size}"
  engine             = "${var.engine}"
  engine_version     = "${var.engine_version}"
  cluster_identifier = "${aws_rds_cluster.rds_cluster.id}"
  instance_class     = "${var.instance_class}"
  apply_immediately  = "${var.apply_immediately}"

  # network and Security Options
  publicly_accessible  = "${var.publicly_accessible}"
  db_subnet_group_name = "${aws_db_subnet_group.rds_db_subnet_group.id}"

  tags {
    Name        = "conductor-aurora-postgresql-${var.enclave}-${var.env}-${count.index}"
    Owner       = "${var.owner}"
    Environment = "${var.env}"
  }
}

resource "aws_rds_cluster" "rds_cluster" {
  cluster_identifier = "conductor-aurora-postgresql-${var.enclave}-${var.env}"

  # Cluster Options
  engine            = "${var.engine}"
  engine_version    = "${var.engine_version}"
  apply_immediately = "${var.apply_immediately}"

  # Database Options
  database_name   = "${lookup(var.database_name, "${var.env}")}"
  port            = "${var.database_port}"
  master_username = "${lookup(var.database_user, "${var.env}")}"
  master_password = "${lookup(var.database_password, "${var.env}")}"

  # Network and Security Options
  db_subnet_group_name   = "${aws_db_subnet_group.rds_db_subnet_group.id}"
  availability_zones     = ["${var.availability_zones}"]
  vpc_security_group_ids = ["${aws_security_group.conductor-aurora-sg.id}"]

  # Tags
  tags {
    Name        = "conductor-aurora-postgresql-${var.enclave}-${var.env}"
    Owner       = "${var.owner}"
    Environment = "${var.env}"
  }
}

resource "aws_db_subnet_group" "rds_db_subnet_group" {
  name       = "conductor-aurora-postgresql-${var.enclave}-${var.env}-subnetgrp"
  subnet_ids = ["${split(",", "${module.vars.private_subnets}")}"]
}

resource "aws_security_group" "conductor-aurora-sg" {
  name        = "conductor-aurora-postgres-${var.enclave}-${var.env}-${var.region}"
  description = "One conductor RDS Security Group "
  vpc_id      = "${module.vars.vpc-id}"

  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["${var.allowed_subnets["all-10-priv"]}", "${var.allowed_subnets["all-172-priv"]}", "${var.allowed_subnets["dlx-coresite"]}"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags {
    Name        = "conductor-aurora-postgresql-${var.enclave}-${var.env}"
    Owner       = "${var.owner}"
    Environment = "${var.env}"
  }
}
