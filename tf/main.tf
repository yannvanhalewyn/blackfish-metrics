variable "rds_password" {}

provider "aws" {
  profile    = "default"
  region     = "eu-central-1"
}

################################################################################
# VPC

resource "aws_vpc" "vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  instance_tenancy     = "default"

  tags {
    Name = "blackfish"
  }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = "${aws_vpc.vpc.id}"

  tags {
    Name = "blackfish_igw"
  }
}

resource "aws_subnet" "public1" {
  vpc_id            = "${aws_vpc.vpc.id}"
  cidr_block        = "10.0.0.0/24"
  availability_zone = "eu-central-1a"

  tags {
    Name = "blackfish_public_subnet1"
  }
}

resource "aws_subnet" "public2" {
  vpc_id            = "${aws_vpc.vpc.id}"
  cidr_block        = "10.0.1.0/24"
  availability_zone = "eu-central-1b"

  tags {
    Name = "blackfish_public_subnet2"
  }
}

resource "aws_route_table" "public" {
  vpc_id = "${aws_vpc.vpc.id}"

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = "${aws_internet_gateway.igw.id}"
  }

  tags {
    Name = "blackfish-public-routes"
  }
}

resource "aws_route_table_association" "public1" {
  route_table_id = "${aws_route_table.public.id}"
  subnet_id      = "${aws_subnet.public1.id}"
}

resource "aws_route_table_association" "public2" {
  route_table_id = "${aws_route_table.public.id}"
  subnet_id      = "${aws_subnet.public2.id}"
}


resource "aws_security_group" "allow_all" {
  name        = "allow_all"
  description = "Allow all inbound traffic"
  vpc_id      = "${aws_vpc.vpc.id}"

  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port       = 0
    to_port         = 0
    protocol        = "-1"
    cidr_blocks     = ["0.0.0.0/0"]
  }

  tags {
    Name = "allow_all"
  }
}

################################################################################
# ECS

resource "aws_ecr_repository" "repository" {
  name = "blackfish-metrics"
}

resource "aws_ecs_cluster" "cluster" {
  name = "blackfish-metrics-cluster"
}

################################################################################
# DB

resource "aws_db_instance" "instance" {
  identifier        = "blackfish-metrics"
  engine            = "postgres"
  engine_version    = "10.5"
  instance_class    = "db.t2.micro"
  allocated_storage = 20

  port     = 5432
  name     = "blackfish_metrics"
  username = "root"
  password = "${var.rds_password}"

  multi_az                    = false
  db_subnet_group_name        = "${aws_db_subnet_group.subnet_group.name}"
  vpc_security_group_ids      = ["${aws_security_group.allow_all.id}"]
  publicly_accessible         = true
  skip_final_snapshot         = false
  allow_major_version_upgrade = true
}

resource "aws_db_subnet_group" "subnet_group" {
  name        = "blackfish_metrics_db_subnet"
  description = "Subnet group for blackfish metrics db"
  subnet_ids  = ["${aws_subnet.public1.id}", "${aws_subnet.public2.id}"]
}
