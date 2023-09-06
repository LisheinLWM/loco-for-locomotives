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
  name   = "c8-tf-locomotive-rds-security-group"
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
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
  identifier                   = "c8-tf-locomotive-rds"
  allocated_storage            = 20
  db_name                      = var.database_name
  engine                       = "postgres"
  engine_version               = "15"
  instance_class               = "db.t3.micro"
  username                     = var.database_username
  password                     = var.database_password
  parameter_group_name         = "default.postgres15"
  skip_final_snapshot          = true
  performance_insights_enabled = false
  db_subnet_group_name         = "public_subnet_group"
  publicly_accessible          = true
  vpc_security_group_ids       = ["${aws_security_group.c8-locomotive-rds-security-group.id}"]
}

resource "aws_ecr_repository" "loco-arc-pipeline-ecr" {
  name         = "loco-arc-tf-pipeline-ecr"
  force_delete = true

}


resource "aws_ecr_repository" "loco-live-pipeline-ecr" {
  name         = "loco-live-tf-pipeline-ecr"
  force_delete = true

}


resource "aws_ecr_repository" "loco-streamlit-ecr" {
  name         = "loco-streamlit-tf-ecr"
  force_delete = true
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

resource "aws_ecs_task_definition" "arc-pipeline-task-definition" {
  family       = "arc-pipeline-tf-task-definition"
  network_mode = "awsvpc"

  requires_compatibilities = ["FARGATE"]

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  cpu    = "1024"
  memory = "3072"

  execution_role_arn = "arn:aws:iam::129033205317:role/ecsTaskExecutionRole"

  container_definitions = jsonencode([
    {
      name  = "loco-pipeline-ecr",
      image = "129033205317.dkr.ecr.eu-west-2.amazonaws.com/loco-pipeline-ecr",

      essential = true

      portMappings = [
        {
          containerPort : 80,
          hostPort : 80
          protocol    = "tcp"
          appProtocol = "http"
        }
      ]

      environment = [
        {
          name  = "DATABASE_NAME"
          value = var.database_name
        },
        {
          name  = "DATABASE_IP"
          value = var.database_ip
        },
        {
          name  = "DATABASE_PORT"
          value = var.database_port
        },
        {
          name  = "DATABASE_USERNAME"
          value = var.database_username
        },
        {
          name  = "DATABASE_PASSWORD"
          value = var.database_password
        },
        {
          name  = "ACCESS_KEY_ID"
          value = var.access_key
        },
        {
          name  = "SECRET_ACCESS_KEY"
          value = var.secret_key
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-create-group"  = "true"
          "awslogs-group"         = "/ecs/"
          "awslogs-region"        = "eu-west-2"
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])
}

resource "aws_iam_role" "ecs-loco-task-execution-role" {
  name = "ecs-loco-task-execution-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        },
        Effect = "Allow",
        Sid    = ""
      },
      {
        Action = "sts:AssumeRole",
        Principal = {
          Service = "scheduler.amazonaws.com"
        },
        Effect = "Allow",
        Sid    = ""
      }
    ]
  })
  inline_policy {
    name = "ecs-task-inline-policy"
    policy = jsonencode({
      Version = "2012-10-17",
      Statement = [
        {
          Action   = "ecs:DescribeTaskDefinition",
          Effect   = "Allow",
          Resource = "*",
          Condition = {
            "ArnLike" : {
              "ecs:cluster" : aws_ecs_cluster.cluster.arn
            }
          }
        },
        {
          Action   = "ecs:DescribeTasks",
          Effect   = "Allow",
          Resource = "*",
          Condition = {
            "ArnLike" : {
              "ecs:cluster" : aws_ecs_cluster.cluster.arn
            }
          }
        },
        {
          Action   = "ecs:RunTask",
          Effect   = "Allow",
          Resource = "*",
          Condition = {
            "ArnLike" : {
              "ecs:cluster" : aws_ecs_cluster.cluster.arn
            }
          }
        },
        {
          Action   = "iam:PassRole",
          Effect   = "Allow",
          Resource = "*"
        }
      ]
    })
  }
}

resource "aws_iam_role_policy_attachment" "ecs-task-execution-role-policy-attachment" {
  role       = aws_iam_role.ecs-loco-task-execution-role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}


resource "aws_scheduler_schedule" "c8-loco-schedule-arc-pipeline" {
  name                         = "c8-loco-tf-schedule-arc-pipeline"
  schedule_expression_timezone = "Europe/London"
  description                  = "Every mid night at 00:05 Get the transport data and upload it into database."
  state                        = "ENABLED"
  group_name                   = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "cron(5 0 * * ? *)"

  target {
    arn      = aws_ecs_cluster.cluster.arn
    role_arn = aws_iam_role.ecs-loco-task-execution-role.arn

    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.arc-pipeline-task-definition.arn
      task_count          = 1
      launch_type         = "FARGATE"
      network_configuration {
        assign_public_ip = true
        security_groups  = [aws_security_group.security-group-arc-pipeline.id]
        subnets          = ["subnet-03b1a3e1075174995", "subnet-0cec5bdb9586ed3c4", "subnet-0667517a2a13e2a6b"]
      }
    }
  }
}
