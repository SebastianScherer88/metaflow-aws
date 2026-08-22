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

# Deploy

Run

```bash
pulumi up
```

to provision the entire AWS metaflow stack.

Then run

```bash
pulumi stack output metaflow_config --json > ../.metaflowconfig/config.json
```

to generate the metaflow config file required by the local metaflow client to
point to the right AWS resources when interacting with S3, AWS Batch, 
AWS Stepfunctions, etc