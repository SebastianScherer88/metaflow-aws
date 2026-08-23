"""
Metaflow on AWS — Pulumi (Python)

Implements:
  - VPC (2 AZs, public + private subnets, single NAT gateway)
  - RDS Postgres                          -> metadata service's backing DB
  - S3 bucket                             -> Metaflow datastore
  - Cloud Map private DNS namespace       -> stable internal addresses for
                                              the metadata service and UI
                                              backend (used by Batch jobs /
                                              Step Functions tasks, and by
                                              the UI backend -> metadata
                                              service call)
  - ECS Fargate service: metadata + migration service (Netflix/metaflow-service)
      -> shared ALB, port 8080 listener (default), OR
      -> dev mode: task gets a public IP, SG restricted to devAllowedCidrs
      -> Cloud Map entry (for internal callers), always present
  - ECS Fargate service: UI backend + static frontend (Netflix/metaflow-ui)
      -> shared ALB, port 80 listener (default), OR
      -> dev mode: task gets a public IP, SG restricted to devAllowedCidrs
      -> Cloud Map entry (backend, for potential internal callers)

  ALB usage is controlled by `metaflow:useLoadBalancer` (default: true).
  Set it to false for cheaper dev/personal stacks — this removes the ALB(s)
  entirely and instead assigns the metadata-service and UI tasks public IPs,
  locked down via `metaflow:devAllowedCidrs` (comma-separated CIDRs, e.g.
  your home IP or a list of teammates' IPs). Internal traffic (Batch jobs,
  UI backend -> metadata service) always uses Cloud Map either way.
  - DynamoDB table                        -> Step Functions foreach/state tracking
  - AWS Batch (Fargate compute env, queue, default job definition)
                                           -> what Step Functions submits
                                              flow steps to
  - IAM roles:
      * ECS task execution roles (pull image, write logs, read DB secret)
      * metadata/UI task roles
      * Batch job role (what your flow code runs as: S3 + metadata access)
      * Batch execution role (ECS-level, for Batch's Fargate tasks)
      * Step Functions state machine role (Batch + Events + DynamoDB)

Container images for the metadata service and UI are NOT hardcoded to a
registry here — public images for these have moved around / had pull
issues historically. Build them yourself from:
    https://github.com/Netflix/metaflow-service   (metadata + migration, combined Dockerfile)
    https://github.com/Netflix/metaflow-ui         (backend)
    https://github.com/Netflix/metaflow-ui-service (or the ui-static bundle, per that repo's docs)
push to ECR, and supply the URIs via `pulumi config set`:

    pulumi config set metaflow:metadataServiceImage <ecr-uri>:<tag>
    pulumi config set metaflow:uiBackendImage <ecr-uri>:<tag>
    pulumi config set metaflow:uiFrontendImage <ecr-uri>:<tag>
    pulumi config set --secret metaflow:dbPassword <password>   # optional, else generated
"""

import json

import pulumi
import pulumi_aws as aws
import pulumi_random as random

config = pulumi.Config("metaflow-aws")
project = pulumi.get_project()
stack = pulumi.get_stack()
prefix = f"{stack}"

security_config = config.get_object("security")
metadata_service_config = config.get_object("metadata-service")
ui_config = config.get_object("ui")
metadata_store_config = config.get_object("metadata-store")
batch_config = config.get_object("batch")
stepfunctions_config = config.get_object("stepfunctions")

# metadata_service_image = config.require("metadataServiceImage")
# ui_image = config.require("uiImage")

# db_username = config.get("dbUsername") or "metaflow"
# db_name = config.get("dbName") or "metaflow"
# rds_instance_class = config.get("dbInstanceClass") or "db.t3.micro"
# batch_max_vcpus = config.get_int("batchMaxVcpus") or 16

tags = {"project": project, "stack": stack, "managed-by": "pulumi"}

# ---------------------------------------------------------------------------
# Networking
# ---------------------------------------------------------------------------

vpc = aws.ec2.Vpc(
    f"{prefix}-vpc",
    cidr_block="10.0.0.0/16",
    enable_dns_hostnames=True,
    enable_dns_support=True,
    tags={**tags, "Name": f"{prefix}-vpc"},
)

azs = aws.get_availability_zones(state="available")

igw = aws.ec2.InternetGateway(
    f"{prefix}-igw", vpc_id=vpc.id, tags={**tags, "Name": f"{prefix}-igw"}
)

public_subnets = []
private_subnets = []
for i in range(2):
    az = azs.names[i]
    public_subnets.append(
        aws.ec2.Subnet(
            f"{prefix}-public-{i}",
            vpc_id=vpc.id,
            cidr_block=f"10.0.{i}.0/24",
            availability_zone=az,
            map_public_ip_on_launch=True,
            tags={**tags, "Name": f"{prefix}-public-{i}"},
        )
    )
    private_subnets.append(
        aws.ec2.Subnet(
            f"{prefix}-private-{i}",
            vpc_id=vpc.id,
            cidr_block=f"10.0.{i + 10}.0/24",
            availability_zone=az,
            tags={**tags, "Name": f"{prefix}-private-{i}"},
        )
    )

public_rt = aws.ec2.RouteTable(
    f"{prefix}-public-rt",
    vpc_id=vpc.id,
    routes=[aws.ec2.RouteTableRouteArgs(cidr_block="0.0.0.0/0", gateway_id=igw.id)],
    tags={**tags, "Name": f"{prefix}-public-rt"},
)
for i, subnet in enumerate(public_subnets):
    aws.ec2.RouteTableAssociation(
        f"{prefix}-public-rta-{i}", subnet_id=subnet.id, route_table_id=public_rt.id
    )

nat_eip = aws.ec2.Eip(f"{prefix}-nat-eip", domain="vpc", tags=tags)
nat_gw = aws.ec2.NatGateway(
    f"{prefix}-nat",
    allocation_id=nat_eip.id,
    subnet_id=public_subnets[0].id,
    tags={**tags, "Name": f"{prefix}-nat"},
    opts=pulumi.ResourceOptions(depends_on=[igw]),
)

private_rt = aws.ec2.RouteTable(
    f"{prefix}-private-rt",
    vpc_id=vpc.id,
    routes=[aws.ec2.RouteTableRouteArgs(cidr_block="0.0.0.0/0", nat_gateway_id=nat_gw.id)],
    tags={**tags, "Name": f"{prefix}-private-rt"},
)
for i, subnet in enumerate(private_subnets):
    aws.ec2.RouteTableAssociation(
        f"{prefix}-private-rta-{i}", subnet_id=subnet.id, route_table_id=private_rt.id
    )

# ---------------------------------------------------------------------------
# Security groups
# ---------------------------------------------------------------------------

if not security_config['alb']:
    use_load_balancer = False
elif security_config['alb']:
    use_load_balancer = True
else:
    raise ValueError(f"Invalid value for alb spec: {security_config['alb']}")

# Dev fallback: comma-separated list/CIDRs of IPs allowed direct access when
# the ALB is skipped, e.g. pulumi config set metaflow:devAllowedCidrs "1.2.3.4/32,5.6.7.8/32"
allowed_cidrs = [c.strip() for c in (security_config['allowed_cidrs'])]

if use_load_balancer:
    alb_sg = aws.ec2.SecurityGroup(
        f"{prefix}-alb-sg",
        vpc_id=vpc.id,
        description="Ingress for metadata service + UI (ALB or dev direct-access)",
        ingress=(
            [
                aws.ec2.SecurityGroupIngressArgs(
                    protocol="tcp", from_port=ui_config['port'], to_port=ui_config['port'], cidr_blocks=allowed_cidrs, description="Metaflow UI"
                ),
                aws.ec2.SecurityGroupIngressArgs(
                    protocol="tcp", from_port=metadata_service_config['port'], to_port=metadata_service_config['port'], cidr_blocks=allowed_cidrs, description="Metaflow Metadata service"
                ),
            ]
        ),
        egress=[
            aws.ec2.SecurityGroupEgressArgs(
                protocol="-1", from_port=0, to_port=0, cidr_blocks=["0.0.0.0/0"]
            )
        ],
        tags={**tags, "Name": f"{prefix}-alb-sg"},
    )

ecs_services_sg = aws.ec2.SecurityGroup(
    f"{prefix}-ecs-services-sg",
    vpc_id=vpc.id,
    description="Metadata service + UI ECS tasks",
    ingress=(
        []
        if use_load_balancer
        else [
            # dev mode: tasks get public IPs and are hit directly, no ALB in front
            aws.ec2.SecurityGroupIngressArgs(
                protocol="tcp", from_port=metadata_service_config['port'], to_port=metadata_service_config['port'], cidr_blocks=allowed_cidrs
            ),
            aws.ec2.SecurityGroupIngressArgs(
                protocol="tcp", from_port=ui_config['port'], to_port=ui_config['port'], cidr_blocks=allowed_cidrs
            ),
        ]
    ),
    egress=[
        aws.ec2.SecurityGroupEgressArgs(
            protocol="-1", from_port=0, to_port=0, cidr_blocks=["0.0.0.0/0"]
        )
    ],
    tags={**tags, "Name": f"{prefix}-ecs-services-sg"},
)

batch_sg = aws.ec2.SecurityGroup(
    f"{prefix}-batch-sg",
    vpc_id=vpc.id,
    description="AWS Batch Fargate tasks running flow steps",
    egress=[
        aws.ec2.SecurityGroupEgressArgs(
            protocol="-1", from_port=0, to_port=0, cidr_blocks=["0.0.0.0/0"]
        )
    ],
    tags={**tags, "Name": f"{prefix}-batch-sg"},
)

if use_load_balancer:
    # ALB -> ECS services on their container ports
    aws.ec2.SecurityGroupRule(
        f"{prefix}-ecs-metadata-service-from-alb",
        type="ingress",
        security_group_id=ecs_services_sg.id,
        protocol="tcp",
        from_port=metadata_service_config['port'],
        to_port=metadata_service_config['port'],
        source_security_group_id=alb_sg.id,
    )
    aws.ec2.SecurityGroupRule(
        f"{prefix}-ecs-ui-from-alb",
        type="ingress",
        security_group_id=ecs_services_sg.id,
        protocol="tcp",
        from_port=ui_config['port'],
        to_port=ui_config['port'],
        source_security_group_id=alb_sg.id,
    )

# Batch job containers -> metadata service (internal, via Cloud Map)
aws.ec2.SecurityGroupRule(
    f"{prefix}-ecs-metadata-service-from-batch",
    type="ingress",
    security_group_id=ecs_services_sg.id,
    protocol="tcp",
    from_port=metadata_service_config['port'],
    to_port=metadata_service_config['port'],
    source_security_group_id=batch_sg.id,
)

rds_sg = aws.ec2.SecurityGroup(
    f"{prefix}-rds-sg",
    vpc_id=vpc.id,
    description="Postgres, reachable only from the metadata/UI ECS tasks",
    egress=[
        aws.ec2.SecurityGroupEgressArgs(
            protocol="-1", from_port=0, to_port=0, cidr_blocks=["0.0.0.0/0"]
        )
    ],
    tags={**tags, "Name": f"{prefix}-rds-sg"},
)
aws.ec2.SecurityGroupRule(
    f"{prefix}-rds-from-ecs",
    type="ingress",
    security_group_id=rds_sg.id,
    protocol="tcp",
    from_port=metadata_store_config['port'],
    to_port=metadata_store_config['port'],
    source_security_group_id=ecs_services_sg.id,
)

# ---------------------------------------------------------------------------
# S3 datastore
# ---------------------------------------------------------------------------

datastore_bucket = aws.s3.Bucket(f"{prefix}-datastore", tags=tags)
aws.s3.BucketVersioning(
    f"{prefix}-datastore-versioning",
    bucket=datastore_bucket.id,
    versioning_configuration=aws.s3.BucketVersioningVersioningConfigurationArgs(
        status="Enabled"
    ),
)
aws.s3.BucketPublicAccessBlock(
    f"{prefix}-datastore-block-public",
    bucket=datastore_bucket.id,
    block_public_acls=True,
    block_public_policy=True,
    ignore_public_acls=True,
    restrict_public_buckets=True,
)

# ---------------------------------------------------------------------------
# RDS Postgres (metadata service backend)
# ---------------------------------------------------------------------------

db_password = random.RandomPassword(
    f"{prefix}-db-password", length=24, special=False
)

db_subnet_group = aws.rds.SubnetGroup(
    f"{prefix}-db-subnets",
    subnet_ids=[s.id for s in private_subnets],
    tags=tags,
)

db_instance = aws.rds.Instance(
    f"{prefix}-db",
    engine="postgres",
    engine_version=metadata_store_config['engine-version'],
    instance_class=metadata_store_config['instance-class'],
    allocated_storage=20,
    storage_encrypted=True,
    db_name=metadata_store_config['name'],
    username=metadata_store_config['username'],
    password=db_password.result,
    port=metadata_store_config['port'],
    db_subnet_group_name=db_subnet_group.name,
    vpc_security_group_ids=[rds_sg.id],
    publicly_accessible=False,
    skip_final_snapshot=True,
    tags=tags,
)

db_secret = aws.secretsmanager.Secret(f"{prefix}-db-secret", tags=tags)
aws.secretsmanager.SecretVersion(
    f"{prefix}-db-secret-version",
    secret_id=db_secret.id,
    secret_string=pulumi.Output.json_dumps(
        {
            "username": db_instance.username,
            "password": db_password.result,
            "host": db_instance.address,
            "port": db_instance.port,
            "dbname": db_instance.db_name,
        }
    ),
)

# ---------------------------------------------------------------------------
# Dynamo
# ---------------------------------------------------------------------------

# DynamoDB table used by the Step Functions orchestrator to track
# foreach / parallel-split state across a run
sfn_state_table = aws.dynamodb.Table(
    f"{prefix}-sfn-state",
    billing_mode="PAY_PER_REQUEST",
    hash_key="pathspec",
    #range_key="foreach_stack",
    attributes=[
        aws.dynamodb.TableAttributeArgs(name="pathspec", type="S"),
        #aws.dynamodb.TableAttributeArgs(name="foreach_stack", type="S"),
    ],
    tags=tags,
)

# ---------------------------------------------------------------------------
# IAM
# ---------------------------------------------------------------------------

ecs_assume_role_policy = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
)

# Shared ECS task execution role (image pull + logs + read the DB secret)
ecs_execution_role = aws.iam.Role(
    f"{prefix}-ecs-execution-role", assume_role_policy=ecs_assume_role_policy, tags=tags
)
aws.iam.RolePolicyAttachment(
    f"{prefix}-ecs-execution-role-managed",
    role=ecs_execution_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
)
ecs_execution_secret_policy = aws.iam.RolePolicy(
    f"{prefix}-ecs-execution-role-secret",
    role=ecs_execution_role.id,
    policy=db_secret.arn.apply(
        lambda arn: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["secretsmanager:GetSecretValue"],
                        "Resource": arn,
                    }
                ],
            }
        )
    ),
)

# Metadata service task role: no AWS permissions required at runtime
metadata_task_role = aws.iam.Role(
    f"{prefix}-metadata-task-role", assume_role_policy=ecs_assume_role_policy, tags=tags
)

# UI task role: read-only S3 access (artifact previews), no metadata DB access
# beyond what it gets by calling the metadata service over HTTP
ui_task_role = aws.iam.Role(
    f"{prefix}-ui-task-role", assume_role_policy=ecs_assume_role_policy, tags=tags
)
aws.iam.RolePolicy(
    f"{prefix}-ui-task-role-s3-read",
    role=ui_task_role.id,
    policy=datastore_bucket.arn.apply(
        lambda arn: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:ListBucket"],
                        "Resource": [arn, f"{arn}/*"],
                    }
                ],
            }
        )
    ),
)

# EC2 instance profile role for compute environment
batch_e2_service_role = aws.iam.Role(
    f"{prefix}-batch-instance-role",
    assume_role_policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "ec2.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }),
)

aws.iam.RolePolicyAttachment(
    "batch-instance-role-policy",
    role=batch_e2_service_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role",
)

# Batch job role: what your @batch step code runs as — S3 datastore + can
# talk to the metadata service (no IAM needed for that, it's plain HTTP)
batch_job_role = aws.iam.Role(
    f"{prefix}-batch-job-role",
    assume_role_policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    ),
    tags=tags,
)
aws.iam.RolePolicy(
    f"{prefix}-batch-job-role-s3",
    role=batch_job_role.id,
    policy=pulumi.Output.json_dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket",],
                    "Resource": [datastore_bucket.arn, pulumi.Output.concat(datastore_bucket.arn,"/*")],
                },
                {
                    "Effect": "Allow",
                    "Action": ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:UpdateItem"],
                    "Resource": [sfn_state_table.arn]
                }
            ],
        }
    )
)

# Batch execution role: ECS-level role for Fargate Batch jobs (pull image, logs)
batch_execution_role = aws.iam.Role(
    f"{prefix}-batch-execution-role", assume_role_policy=ecs_assume_role_policy, tags=tags
)
aws.iam.RolePolicyAttachment(
    f"{prefix}-batch-execution-role-managed",
    role=batch_execution_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
)

# Batch's own service role (required by the compute environment)
batch_service_role = aws.iam.Role(
    f"{prefix}-batch-service-role",
    assume_role_policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "batch.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    ),
    tags=tags,
)
aws.iam.RolePolicyAttachment(
    f"{prefix}-batch-service-role-managed",
    role=batch_service_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole",
)

# Step Functions state machine role: submits/monitors Batch jobs, needs the
# Events permissions for the .sync ("run job, wait for completion") pattern,
# and read/write on the foreach-state DynamoDB table
sfn_role = aws.iam.Role(
    f"{prefix}-sfn-role",
    assume_role_policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "states.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    ),
    tags=tags,
)
aws.iam.RolePolicy(
    f"{prefix}-sfn-role-policy",
    role=sfn_role.id,
    policy=sfn_state_table.arn.apply(
        lambda ddb_arn: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "batch:SubmitJob",
                            "batch:DescribeJobs",
                            "batch:TerminateJob",
                        ],
                        "Resource": "*",
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "events:PutTargets",
                            "events:PutRule",
                            "events:DescribeRule",
                        ],
                        "Resource": "*",
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "dynamodb:GetItem",
                            "dynamodb:PutItem",
                            "dynamodb:UpdateItem",
                            "dynamodb:DeleteItem",
                            "dynamodb:Query",
                        ],
                        "Resource": [ddb_arn, f"{ddb_arn}/index/*"],
                    },
                ],
            }
        )
    ),
)

# IAM role assumed by Amazon EventBridge when invoking Metaflow
# Step Functions state machines.
events_sfn_role = aws.iam.Role(
    f"{prefix}-events-sfn-access-role",
    assume_role_policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {
                "Service": "events.amazonaws.com",
            },
            "Action": "sts:AssumeRole",
        }],
    }),
    tags=tags,
)

aws.iam.RolePolicy(
    f"{prefix}-events-sfn-access-policy",
    role=events_sfn_role.id,
    policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": [
                "states:StartExecution",
                "states:DescribeExecution",
                "states:ListExecutions",
                "states:DescribeStateMachine",
                "states:ListStateMachines",
            ],
            "Resource": "*",
        }],
    }),
)

# ---------------------------------------------------------------------------
# ECS cluster
# ---------------------------------------------------------------------------

cluster = aws.ecs.Cluster(f"{prefix}-cluster", tags=tags)

log_group = aws.cloudwatch.LogGroup(
    f"{prefix}-logs", retention_in_days=14, tags=tags
)

# ---------------------------------------------------------------------------
# Shared ALB (optional) — one load balancer, two listeners: 8080 for the
# metadata service, 80 for the UI. Skipped entirely in dev mode, where ECS
# tasks get public IPs and `alb_sg` is scoped to devAllowedCidrs instead.
# ---------------------------------------------------------------------------

if use_load_balancer:
    shared_alb = aws.lb.LoadBalancer(
        f"{prefix}-alb",
        internal=False,
        load_balancer_type="application",
        security_groups=[alb_sg.id],
        subnets=[s.id for s in public_subnets],
        tags=tags,
    )

    metadata_tg = aws.lb.TargetGroup(
        f"{prefix}-metadata-tg",
        port=metadata_service_config['port'],
        protocol="HTTP",
        vpc_id=vpc.id,
        target_type="ip",
        health_check=aws.lb.TargetGroupHealthCheckArgs(path="/ping", matcher="200-399"),
        tags=tags,
    )
    metadata_listener = aws.lb.Listener(
        f"{prefix}-metadata-listener",
        load_balancer_arn=shared_alb.arn,
        port=metadata_service_config['port'],
        protocol="HTTP",
        default_actions=[
            aws.lb.ListenerDefaultActionArgs(type="forward", target_group_arn=metadata_tg.arn)
        ],
    )

    ui_tg = aws.lb.TargetGroup(
        f"{prefix}-ui-tg",
        port=ui_config['port'],
        protocol="HTTP",
        vpc_id=vpc.id,
        target_type="ip",
        health_check=aws.lb.TargetGroupHealthCheckArgs(path="/", matcher="200-399"),
        tags=tags,
    )
    ui_listener = aws.lb.Listener(
        f"{prefix}-ui-listener",
        load_balancer_arn=shared_alb.arn,
        port=ui_config['port'],
        protocol="HTTP",
        default_actions=[
            aws.lb.ListenerDefaultActionArgs(type="forward", target_group_arn=ui_tg.arn)
        ],
    )
else:
    shared_alb = None
    metadata_tg = None
    metadata_listener = None
    ui_tg = None
    ui_listener = None

# ---------------------------------------------------------------------------
# Metadata + migration service (Netflix/metaflow-service combined image:
# metadata API on 8080, migration API on 8082)
# ---------------------------------------------------------------------------

metadata_namespace = aws.servicediscovery.PrivateDnsNamespace(
    f"{prefix}-namespace",
    name="metaflow.local",
    vpc=vpc.id,
    description="Private service discovery for Metaflow services",
    tags=tags,
)

metadata_discovery_service = aws.servicediscovery.Service(
    f"{prefix}-metadata-discovery",
    name="metadata",
    dns_config=aws.servicediscovery.ServiceDnsConfigArgs(
        namespace_id=metadata_namespace.id,
        routing_policy="MULTIVALUE",
        dns_records=[
            aws.servicediscovery.ServiceDnsConfigDnsRecordArgs(
                ttl=10,
                type="A",
            )
        ],
    ),
    health_check_custom_config=aws.servicediscovery.ServiceHealthCheckCustomConfigArgs(
        failure_threshold=1,
    ),
    tags=tags,
)
metadata_internal_url = pulumi.Output.concat(
    "http://",
    metadata_discovery_service.name,
    ".",
    metadata_namespace.name,
    ":",
    str(metadata_service_config["port"]),
)
service_registries=aws.ecs.ServiceServiceRegistriesArgs(
    registry_arn=metadata_discovery_service.arn,
    container_name="metadata-service",
    container_port=metadata_service_config['port'],
)

metadata_task_def = aws.ecs.TaskDefinition(
    f"{prefix}-metadata-task",
    family=f"{prefix}-metadata",
    cpu=metadata_service_config['resources']['cpu'],
    memory=metadata_service_config['resources']['memory'],
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=ecs_execution_role.arn,
    task_role_arn=metadata_task_role.arn,
    container_definitions=pulumi.Output.json_dumps(
        [
            {
                "name": "metadata-service",
                "image": metadata_service_config['image'],
                "essential": True,
                "portMappings": [
                    {"containerPort": metadata_service_config['port'], "protocol": "tcp"},
                    # {"containerPort": 8082, "protocol": "tcp"},
                ],
                "environment": [
                    {"name": "MF_METADATA_DB_HOST", "value": db_instance.address},
                    {"name": "MF_METADATA_DB_PORT", "value": db_instance.port.apply(lambda x: str(x))},
                    {"name": "MF_METADATA_DB_NAME", "value": db_instance.db_name},
                    {"name": "MF_METADATA_PORT", "value": str(metadata_service_config['port'])},
                    {"name": "MF_METADATA_HOST", "value": "0.0.0.0"},
                    {"name": "PGSSLMODE","value": "require"},
                    {
                        "name": "MF_METADATA_DB_SSL_MODE",
                        "value": "require",
                    },
                ],
                "secrets": [
                    {
                        "name": "MF_METADATA_DB_USER",
                        "valueFrom": pulumi.Output.concat(db_secret.arn,":username::"),
                    },
                    {
                        "name": "MF_METADATA_DB_PSWD",
                        "valueFrom": pulumi.Output.concat(db_secret.arn,":password::"),
                    },
                ],
                "logConfiguration": {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": log_group.name,
                        "awslogs-region": aws.get_region().name,
                        "awslogs-stream-prefix": "metadata",
                    },
                },
            }
        ]
    ),
    tags=tags,
)
    

metadata_service = aws.ecs.Service(
    f"{prefix}-metadata-service",
    cluster=cluster.arn,
    task_definition=metadata_task_def.arn,
    desired_count=1,
    launch_type="FARGATE",
    network_configuration=aws.ecs.ServiceNetworkConfigurationArgs(
        subnets=[s.id for s in public_subnets] if not use_load_balancer else [s.id for s in private_subnets],
        security_groups=[ecs_services_sg.id],
        assign_public_ip=not use_load_balancer,
    ),
    load_balancers=(
        [
            aws.ecs.ServiceLoadBalancerArgs(
                target_group_arn=metadata_tg.arn,
                container_name="metadata-service",
                container_port=metadata_service_config['port'],
            )
        ]
        if use_load_balancer
        else []
    ),
    service_registries=aws.ecs.ServiceServiceRegistriesArgs(
        registry_arn=metadata_discovery_service.arn,
        container_name="metadata-service",
        #container_port=metadata_service_config['port']
    ),
    opts=pulumi.ResourceOptions(
        depends_on=[metadata_listener] if use_load_balancer else []
    ),
    tags=tags,
)
# Internal address for this service (always available, regardless of mode):
#   metadata-service.metaflow.local:8080
# Dev-mode direct access: each task's own public IP on port 8080 (see
# `metadata_service_dev_note` output — ECS Fargate IPs aren't static across
# restarts; use Cloud Map or re-check the task's public IP after redeploys).

# ---------------------------------------------------------------------------
# UI (backend + static frontend, two containers in one task; frontend
# container is expected to reverse-proxy /api to localhost:8083). Uses the
# shared ALB's port-80 listener, or gets a public IP directly in dev mode.
# ---------------------------------------------------------------------------

ui_task_def = aws.ecs.TaskDefinition(
    f"{prefix}-ui-task",
    family=f"{prefix}-ui",
    cpu=ui_config['resources']['cpu'],
    memory=ui_config['resources']['memory'],
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=ecs_execution_role.arn,
    task_role_arn=ui_task_role.arn,
    container_definitions=pulumi.Output.json_dumps(
        [
            {
                "name": "ui",
                "image": ui_config['image'],
                "command": ["/opt/latest/bin/python3", "-m", "services.ui_backend_service.ui_server"],
                "essential": True,
                "portMappings": [{"containerPort": ui_config['port'], "protocol": "tcp"}],
                "environment": [
                    {"name": "MF_METADATA_DB_HOST", "value": db_instance.address},
                    {"name": "MF_METADATA_DB_PORT", "value": db_instance.port.apply(lambda x: str(x))},
                    {"name": "MF_METADATA_DB_NAME", "value": db_instance.db_name},
                    {"name": "MF_UI_METADATA_PORT", "value": str(ui_config['port'])},
                    {"name": "MF_UI_METADATA_HOST", "value": "0.0.0.0"},
                    {"name": "UI_ENABLED", "value": "1"},
                    {"name": "FEATURE_ARTIFACT_SEARCH", "value": "1"},
                    {"name": "FEATURE_ARTIFACT_TABLE", "value": "1"},
                    {"name": "METAFLOW_DEFAULT_DATASTORE", "value": "s3"},
                    {"name": "METAFLOW_DATASTORE_SYSROOT_S3", "value": pulumi.Output.concat("s3://",datastore_bucket.bucket)},
                    # # internal, stable address via Cloud Map:
                    # {
                    #     "name": "MF_METADATA_SERVICE_URL",
                    #     "value": f"http://{args[4]}.{args[5]}:8080",
                    # },
                ],
                "secrets": [
                    {
                        "name": "MF_METADATA_DB_USER",
                        "valueFrom": pulumi.Output.concat(db_secret.arn,":username::"),
                    },
                    {
                        "name": "MF_METADATA_DB_PSWD",
                        "valueFrom": pulumi.Output.concat(db_secret.arn,":password::"),
                    },
                ],
                "logConfiguration": {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": log_group.name,
                        "awslogs-region": aws.get_region().name,
                        "awslogs-stream-prefix": "ui-backend",
                    },
                },
            },
        ]
    ),
    tags=tags,
)

ui_service = aws.ecs.Service(
    f"{prefix}-ui-service",
    cluster=cluster.arn,
    task_definition=ui_task_def.arn,
    desired_count=1,
    launch_type="FARGATE",
    network_configuration=aws.ecs.ServiceNetworkConfigurationArgs(
        subnets=[s.id for s in public_subnets] if not use_load_balancer else [s.id for s in private_subnets],
        security_groups=[ecs_services_sg.id],
        assign_public_ip=not use_load_balancer,
    ),
    load_balancers=(
        [
            aws.ecs.ServiceLoadBalancerArgs(
                target_group_arn=ui_tg.arn,
                container_name="ui",
                container_port=ui_config['port'],
            )
        ]
        if use_load_balancer
        else []
    ),
    # service_registries=aws.ecs.ServiceServiceRegistriesArgs(
    #     registry_arn=ui_backend_discovery_service.arn,
    #     # container_name="ui-backend",
    #     # container_port=8083,
    # ),
    opts=pulumi.ResourceOptions(
        depends_on=([ui_listener] if use_load_balancer else []) + [metadata_service]
    ),
    tags=tags,
)
# Dev-mode direct access: task's own public IP on port 8083 (UI) — same
# caveat as the metadata service: not static across task restarts.

# ---------------------------------------------------------------------------
# AWS Batch — the compute layer Step Functions submits flow steps to
# ---------------------------------------------------------------------------
batch_ec2_instance_profile = aws.iam.InstanceProfile(
    "batch-instance-profile",
    role=batch_e2_service_role.name,
)

batch_queue_names = []
batch_queue_arns = []

for batch_queue_config in batch_config['queues']:

    batch_compute_env_arns = []

    for batch_compute_env_config in batch_queue_config['compute-environments']:

        batch_compute_env_name = batch_compute_env_config.pop('name')

        batch_compute_env = aws.batch.ComputeEnvironment(
            f"{prefix}-batch-{batch_compute_env_name}",
            name=f"{prefix}-{batch_compute_env_name}",
            type="MANAGED",
            service_role=batch_service_role.arn,
            compute_resources=aws.batch.ComputeEnvironmentComputeResourcesArgs(
                **batch_compute_env_config,
                instance_role=batch_ec2_instance_profile.arn if batch_compute_env_config['type'] in ('EC2','SPOT') else None,
                subnets=[s.id for s in private_subnets],
                security_group_ids=[batch_sg.id],
            ),
            opts=pulumi.ResourceOptions(depends_on=[batch_service_role]),
            tags=tags,
        )

        batch_compute_env_arns.append(batch_compute_env.arn)

    batch_queue = aws.batch.JobQueue(
        f"{prefix}-batch-{batch_queue_config['name']}",
        name=f"{prefix}-{batch_queue_config['name']}",
        priority=1,
        state="ENABLED",
        compute_environment_orders=[
            aws.batch.JobQueueComputeEnvironmentOrderArgs(
                order=order_index + 1, compute_environment=batch_compute_env_arn
            ) for order_index, batch_compute_env_arn in enumerate(batch_compute_env_arns)
        ],
        tags=tags,
    )
    batch_queue_names.append(batch_queue.name)
    batch_queue_arns.append(batch_queue.arn)

# ---------------------------------------------------------------------------
# Outputs — feed these into your local `metaflow config` / env vars
# ---------------------------------------------------------------------------
if use_load_balancer:
    load_balancer_url = pulumi.Output.concat("http://", shared_alb.dns_name)
    metadata_external_url = shared_alb.dns_name.apply(lambda d: f"http://{d}:{str(metadata_service_config['port'])}")
    ui_external_url = shared_alb.dns_name.apply(lambda d: f"http://{d}:{str(metadata_service_config['port'])}")
else:
    pulumi.export(
        "dev_mode_note",
        "No ALB deployed. ECS tasks have public IPs restricted to devAllowedCidrs. "
        "Find current IPs with: aws ecs list-tasks --cluster <cluster> then "
        "aws ecs describe-tasks / describe-network-interfaces, since Fargate "
        "public IPs are not static across task replacements.",
    )
    load_balancer_url = ""
    metadata_external_url = ""
    ui_external_url = ""


# detailed stack level outputs
pulumi.export("ui_external_url",ui_external_url)
pulumi.export("metadata_service_external_url",metadata_external_url)
pulumi.export("metadata_service_internal_url",metadata_internal_url)
pulumi.export("datastore_s3_bucket", datastore_bucket.bucket.apply(lambda b: f"s3://{b}"))
pulumi.export("ecs_exeuction_role_arn", ecs_execution_role.arn)
pulumi.export("ecs_metadata_task_role_arn", metadata_task_role.arn)
pulumi.export("ecs_ui_task_role_arn", ui_task_role.arn)
pulumi.export("batch_job_role_arn", batch_job_role.arn)
pulumi.export("batch_compute_env_arns", batch_compute_env_arns)
pulumi.export("batch_job_queue_arns", batch_queue_arns)
pulumi.export("batch_job_queue_names", batch_queue_names)
pulumi.export("batch_default_job_queue_name", batch_queue_names[0])
pulumi.export("sfn_role_arn", sfn_role.arn)
pulumi.export("events_bridge_sfn_role_arn", events_sfn_role.arn)
pulumi.export("sfn_dynamodb_table", sfn_state_table.name)
pulumi.export("rds_endpoint", db_instance.endpoint)
pulumi.export("rds_secret_arn", db_secret.arn)

# metaflow config output
metaflow_config = pulumi.Output.all(
    batch_job_queue_name=batch_queue_names[0],
    datastore_bucket=datastore_bucket.bucket,
    batch_job_role_arn=batch_job_role.arn,
    ecs_execution_role_arn=ecs_execution_role.arn,
    events_sfn_role_arn=events_sfn_role.arn,
    metadata_external_url=metadata_external_url,
    metadata_internal_url=metadata_internal_url,
    sfn_dynamodb_table=sfn_state_table.name,
    sfn_role_arn=sfn_role.arn,
    
).apply(lambda x: {
    "METAFLOW_BATCH_CONTAINER_REGISTRY": "docker.io",
    "METAFLOW_BATCH_JOB_QUEUE": x["batch_job_queue_name"],
    "METAFLOW_DATASTORE_SYSROOT_S3": f"s3://{x['datastore_bucket']}",
    "METAFLOW_DATATOOLS_S3ROOT": f"s3://{x['datastore_bucket']}/data",
    "METAFLOW_DEFAULT_DATASTORE": "s3",
    "METAFLOW_DEFAULT_METADATA": "service",
    "METAFLOW_ECS_S3_ACCESS_IAM_ROLE": x["batch_job_role_arn"],
    "METAFLOW_ECS_FARGATE_EXECUTION_ROLE": x["ecs_execution_role_arn"],
    "METAFLOW_EVENTS_SFN_ACCESS_IAM_ROLE": x["events_sfn_role_arn"],
    "METAFLOW_SERVICE_URL": x["metadata_external_url"],
    "METAFLOW_SERVICE_INTERNAL_URL": x["metadata_internal_url"],
    "METAFLOW_SFN_DYNAMO_DB_TABLE": x["sfn_dynamodb_table"],
    "METAFLOW_SFN_IAM_ROLE": x["sfn_role_arn"],
})

pulumi.export("metaflow_config", metaflow_config)