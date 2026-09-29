# Kafka on EC2 with AWS CDK

Single-node Apache Kafka 4.1 (KRaft) on EC2, deployed with AWS CDK in Python. Every run on `main` deploys the stack, tests Kafka, publishes the results to the GitHub run summary and destroys everything.

## What gets deployed

- VPC with public and private subnets in two AZs and one NAT gateway
- EC2 instance in a private subnet with no public IP and no SSH key
- Security group with no inbound rules and outbound HTTPS only
- IAM role with only `AmazonSSMManagedInstanceCore`
- Encrypted gp3 root volume and IMDSv2 required
- Kafka download verified against a pinned SHA-512 checksum

The stack waits for the instance to report that Kafka is running. If setup fails, CloudFormation rolls back and nothing is left behind.

## Project layout

```
app.py                      CDK app
stacks/kafka_ec2_stack.py   VPC, security group, IAM role, EC2 instance
scripts/kafka_bootstrap.sh  Installs and starts Kafka on the instance
scripts/kafka_test.py       Runs checks on the instance over SSM and reports results
scripts/destroy.sh          Waits for the stack to settle and destroys it
tests/                      Unit tests for the synthesized template
.github/workflows/          CI, deploy and destroy pipelines
```

## Pipelines

| Workflow | Trigger | Steps |
|---|---|---|
| `ci.yml` | Pull request | Unit tests, `cdk synth` |
| `deploy.yml` | Push to `main`, manual | Unit tests, deploy, Kafka tests, upload results, destroy |
| `destroy.yml` | Manual | Destroys the stack if a run was cancelled or stopped early |

Destroy in `deploy.yml` runs whether the deploy or the tests pass or fail. Only one deploy or destroy runs at a time.

## One-time setup

Bootstrap CDK in the target account and region:

```bash
npx aws-cdk bootstrap aws://<account-id>/<region>
```

The workflows authenticate to AWS through GitHub OIDC using an existing IAM role, `kafka-ec2-cdk-github-actions-role`, that can only be assumed from the `main` branch of this repository.

Add these to the repository under Settings → Secrets and variables → Actions:

| Name | Type | Value |
|---|---|---|
| `AWS_ROLE_ARN` | Secret | ARN of the GitHub Actions role |
| `AWS_REGION` | Variable | Target region, for example `us-east-1` |

## Running locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest
```

Deploy, test and clean up with your own AWS credentials:

```bash
npx aws-cdk deploy KafkaEc2Stack
python scripts/kafka_test.py --stack-name KafkaEc2Stack
bash scripts/destroy.sh KafkaEc2Stack
```

To look around on the instance:

```bash
aws ssm start-session --target <instance-id>
sudo journalctl -u kafka
sudo cat /var/log/kafka-bootstrap.log
```
