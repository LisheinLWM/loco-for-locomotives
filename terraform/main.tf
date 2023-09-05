provider "aws" {
  region     = var.region
  access_key = var.access_key
  secret_key = var.secret_key
}


data "aws_vpc" "cohort-8-VPC" {
  id = "vpc-0e0f897ec7ddc230d"
}


data "aws_subnet" "cohort-8-public-subnet-1" {
  vpc_id            = data.aws_vpc.cohort-8-VPC.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = "eu-west-2a"
}


data "aws_subnet" "cohort-8-public-subnet-2" {
  vpc_id            = data.aws_vpc.cohort-8-VPC.id
  cidr_block        = "10.0.2.0/24"
  availability_zone = "eu-west-2b"
}


data "aws_subnet" "cohort-8-public-subnet-3" {
  vpc_id            = data.aws_vpc.cohort-8-VPC.id
  cidr_block        = "10.0.3.0/24"
  availability_zone = "eu-west-2c"
}


output "cohort-8-public-subnet-ids" {
  value = [
    data.aws_subnet.cohort-8-public-subnet-1.id,
    data.aws_subnet.cohort-8-public-subnet-2.id,
    data.aws_subnet.cohort-8-public-subnet-3.id,
  ]
}


resource "aws_security_group" "c8-locomotive-rds-security-group" {
  vpc_id = "vpc-0e0f897ec7ddc230d"
  name = "c8-tf-locomotive-rds-security-group"
  ingress {
    from_port = 5432
    to_port = 5432
    protocol = "tcp"
    cidr_blocks = ["86.155.163.236/32"]
  }
  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}


resource "aws_db_instance" "c8-locomotive-rds" {
  identifier="c8-tf-locomotive-rds"
  allocated_storage    = 20
  db_name              = var.database_name
  engine               = "postgres"
  engine_version       = "15"
  instance_class       = "db.t3.micro"
  username             = var.database_username
  password             = var.database_password
  parameter_group_name = "default.postgres15"
  skip_final_snapshot  = true
  performance_insights_enabled = false
  db_subnet_group_name = "public_subnet_group"
  publicly_accessible = true
  vpc_security_group_ids = ["${aws_security_group.c8-locomotive-rds-security-group.id}"]
}


# ECS Archive Pipeline
resource "aws_ecs_cluster" "cluster" {
  name = "locomotive-cluster"
}

resource "aws_security_group" "security-group-arc-pipeline" {
  name        = "locomotive-arc-pipeline-sg"
  description = "A security group for the pipeline made using terraform."

  vpc_id = data.aws_vpc.cohort-8-VPC.id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
