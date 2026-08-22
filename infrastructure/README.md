# Metaflow on AWS — Pulumi (Python)

Deploys:

- **VPC** — 2 AZs, public + private subnets, single NAT gateway
- **RDS Postgres** — backing DB for the metadata service
- **S3 bucket** — Metaflow datastore
- **Cloud Map private DNS namespace** (`metaflow.local`) — stable internal
  addresses for the metadata service and UI backend, used by Batch job
  containers and the UI backend's calls to the metadata service. Always
  present, regardless of ALB mode below.
- **ECS Fargate: metadata + migration service** — registered in Cloud Map
  (`metadata-service.metaflow.local:8080`), plus either an ALB listener or a
  direct public IP depending on `metaflow:useLoadBalancer` (see below)
- **ECS Fargate: UI** — backend + static frontend, same ALB-or-direct-IP
  choice, sharing the same load balancer as the metadata service when one
  is deployed

### One shared ALB, and it's optional

By default there's a **single** ALB with two listeners — port 8080 for the
metadata service, port 80 for the UI — rather than one ALB per service.
ALBs are billed hourly regardless of traffic, so for solo/dev use you can
turn it off entirely:

```bash
pulumi config set metaflow:useLoadBalancer false
pulumi config set metaflow:devAllowedCidrs "1.2.3.4/32,5.6.7.8/32"
```

With `useLoadBalancer=false`:
- No ALB, target groups, or listeners are created at all.
- The metadata-service and UI ECS tasks each get a **public IP** directly
  (Fargate `assignPublicIp`), reachable only from the CIDRs listed in
  `devAllowedCidrs` — put your own IP and any teammates'/CI IPs there.
- Internal traffic (Batch jobs → metadata service, UI backend → metadata
  service) is unaffected either way — it always goes through Cloud Map
  (`metadata-service.metaflow.local:8080`), never through the ALB.
- Trade-off: Fargate public IPs are **not static** across task replacements
  (deploys, crashes, scaling events). Re-check the current IP after any
  redeploy — the stack exports a `dev_mode_note` output with the AWS CLI
  commands to look it up. If you want a stable address in dev mode too,
  point a Route 53 record at the task's current IP manually, or re-enable
  the ALB for anything longer-lived than a single working session.
- **AWS Batch** (Fargate compute environment, queue, default job definition)
  — what Step Functions submits flow steps to
- **DynamoDB table** — Step Functions foreach/parallel-state tracking
- **IAM roles** — task execution roles, the Batch job role your flow code
  runs as, the Batch/ECS execution role, and the Step Functions state
  machine role

## Before you deploy

The metadata service and UI images aren't hardcoded to a public registry —
pull availability for the historical public images has been unreliable.
Build your own from source and push to ECR:

```
git clone https://github.com/Netflix/metaflow-service      # metadata + migration (combined Dockerfile)
git clone https://github.com/Netflix/metaflow-ui            # UI backend + frontend
```

Then set the image URIs:

```bash
pulumi config set metaflow:metadataServiceImage <account>.dkr.ecr.<region>.amazonaws.com/metaflow-metadata:<tag>
pulumi config set metaflow:uiBackendImage <account>.dkr.ecr.<region>.amazonaws.com/metaflow-ui-backend:<tag>
pulumi config set metaflow:uiFrontendImage <account>.dkr.ecr.<region>.amazonaws.com/metaflow-ui-frontend:<tag>
```

Optional config:

```bash
pulumi config set metaflow:dbInstanceClass db.t3.small   # default: db.t3.micro
pulumi config set metaflow:batchMaxVcpus 32               # default: 16
pulumi config set aws:region us-east-1                    # default: eu-west-1
```

## Deploy

```bash
pip install -r requirements.txt
pulumi stack init dev
pulumi up
```

## Configure your local Metaflow client

After `pulumi up`, use the stack outputs:

```bash
pulumi stack output metadata_service_url_external
pulumi stack output datastore_s3_bucket
pulumi stack output batch_job_queue_arn
pulumi stack output batch_job_role_arn
pulumi stack output sfn_role_arn
pulumi stack output sfn_dynamodb_table
```

Then either export env vars or run `metaflow configure aws` and fill in the
equivalents:

```bash
export METAFLOW_DEFAULT_DATASTORE=s3
export METAFLOW_DATASTORE_SYSROOT_S3=$(pulumi stack output datastore_s3_bucket)/metaflow
export METAFLOW_SERVICE_URL=$(pulumi stack output metadata_service_url_external)
export METAFLOW_DEFAULT_METADATA=service

export METAFLOW_BATCH_JOB_QUEUE=$(pulumi stack output batch_job_queue_arn)
export METAFLOW_ECS_S3_ACCESS_IAM_ROLE=$(pulumi stack output batch_job_role_arn)

export METAFLOW_SFN_IAM_ROLE=$(pulumi stack output sfn_role_arn)
export METAFLOW_SFN_DYNAMO_DB_TABLE=$(pulumi stack output sfn_dynamodb_table)
export METAFLOW_SFN_S3_BUCKET=$(pulumi stack output datastore_s3_bucket)
```

## Running flows

Ad-hoc, against Batch directly (your laptop submits the job and reports status):

```bash
python myflow.py run --with batch
```

Deploy to Step Functions for scheduled/production runs (your laptop only
pushes the state machine definition once; subsequent triggers run inside
AWS, not from your laptop):

```bash
python myflow.py step-functions create
```

## What's intentionally left out

- **TLS / custom domains** — both ALBs are plain HTTP on their AWS-assigned
  DNS names. Add ACM certs + HTTPS listeners for anything beyond local dev.
- **Auth in front of the metadata service / UI ALBs** — as written, both are
  open to the internet on their ALB DNS names. Lock down `alb_sg` ingress
  (e.g. to your VPN/office CIDR) before using this for anything real.
- **RDS Multi-AZ / backups tuned for production** — `skip_final_snapshot` is
  `True` and there's no Multi-AZ; fine for dev, not for prod.
- **A private ECR repo for your own step images** — add one if you plan to
  push custom `@batch`/flow images rather than using public base images.
