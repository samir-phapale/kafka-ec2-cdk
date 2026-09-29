import argparse
import json
import os
import sys
import time
import uuid

import boto3
from botocore.exceptions import BotoCoreError, ClientError

KAFKA_BIN = "/opt/kafka/bin"
BOOTSTRAP_SERVER = "127.0.0.1:9092"
PENDING_STATUSES = {"Pending", "InProgress", "Delayed"}
TEST_ERRORS = (BotoCoreError, ClientError, KeyError, TimeoutError)


def parse_args():
    parser = argparse.ArgumentParser(description="Run Kafka checks against the deployed stack over SSM")
    parser.add_argument("--stack-name", required=True)
    parser.add_argument("--region", default=os.environ.get("AWS_REGION"))
    parser.add_argument("--messages", type=int, default=10)
    parser.add_argument("--output", default="kafka-test-results.json")
    return parser.parse_args()


def stack_outputs(cloudformation, stack_name):
    stack = cloudformation.describe_stacks(StackName=stack_name)["Stacks"][0]
    return {output["OutputKey"]: output["OutputValue"] for output in stack.get("Outputs", [])}


def wait_for_ssm(ssm, instance_id, timeout=300):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        instances = ssm.describe_instance_information(
            Filters=[{"Key": "InstanceIds", "Values": [instance_id]}]
        )["InstanceInformationList"]
        if instances and instances[0]["PingStatus"] == "Online":
            return
        time.sleep(10)
    raise TimeoutError(f"{instance_id} did not come online in SSM within {timeout}s")


def run_command(ssm, instance_id, commands, timeout=120):
    command_id = ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": commands, "executionTimeout": [str(timeout)]},
    )["Command"]["CommandId"]

    deadline = time.monotonic() + timeout + 60
    while time.monotonic() < deadline:
        time.sleep(3)
        try:
            invocation = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance_id)
        except ssm.exceptions.InvocationDoesNotExist:
            continue
        if invocation["Status"] not in PENDING_STATUSES:
            stdout = invocation["StandardOutputContent"].strip()
            stderr = invocation["StandardErrorContent"].strip()
            return invocation["Status"] == "Success", stdout, stderr

    raise TimeoutError(f"SSM command {command_id} did not finish within {timeout}s")


def build_checks(topic, messages, run_id):
    return [
        (
            "Kafka service is active",
            ["systemctl is-active kafka"],
            lambda out: out == "active",
        ),
        (
            "Broker responds",
            [f"{KAFKA_BIN}/kafka-broker-api-versions.sh --bootstrap-server {BOOTSTRAP_SERVER} > /dev/null"],
            lambda out: True,
        ),
        (
            f"Topic '{topic}' exists",
            [f"{KAFKA_BIN}/kafka-topics.sh --list --bootstrap-server {BOOTSTRAP_SERVER}"],
            lambda out: topic in out.split(),
        ),
        (
            f"Produce and consume {messages} messages",
            [
                "set -e",
                f"seq 1 {messages} | sed 's/^/{run_id}-/' | "
                f"{KAFKA_BIN}/kafka-console-producer.sh --bootstrap-server {BOOTSTRAP_SERVER} --topic {topic}",
                f"{KAFKA_BIN}/kafka-console-consumer.sh --bootstrap-server {BOOTSTRAP_SERVER} --topic {topic} "
                f"--from-beginning --timeout-ms 20000 2> /dev/null | grep -c '^{run_id}-' || true",
            ],
            lambda out: out.splitlines()[-1:] == [str(messages)],
        ),
    ]


def run_check(ssm, instance_id, name, commands, validate):
    try:
        succeeded, stdout, stderr = run_command(ssm, instance_id, commands)
    except TEST_ERRORS as exc:
        return {"check": name, "passed": False, "output": str(exc)}
    return {
        "check": name,
        "passed": succeeded and validate(stdout),
        "output": "\n".join(part for part in (stdout, stderr) if part),
    }


def render_markdown(report):
    lines = [
        "## Kafka test results",
        "",
        "| Check | Result |",
        "|---|---|",
    ]
    for result in report["results"]:
        status = "✅ Passed" if result["passed"] else "❌ Failed"
        lines.append(f"| {result['check']} | {status} |")

    lines += [
        "",
        f"**Stack:** `{report['stack']}` · **Instance:** `{report['instance_id']}` · "
        f"**Run ID:** `{report['run_id']}` · **Duration:** {report['duration_seconds']}s",
        "",
        "<details><summary>Command output</summary>",
        "",
    ]
    for result in report["results"]:
        lines += [f"**{result['check']}**", "", "```text", result["output"] or "(no output)", "```", ""]
    lines.append("</details>")
    return "\n".join(lines)


def main():
    args = parse_args()
    session = boto3.Session(region_name=args.region)
    cloudformation = session.client("cloudformation")
    ssm = session.client("ssm")

    run_id = uuid.uuid4().hex[:12]
    started = time.monotonic()
    instance_id = "unknown"
    results = []

    try:
        outputs = stack_outputs(cloudformation, args.stack_name)
        instance_id = outputs["InstanceId"]
        topic = outputs["TopicName"]
        wait_for_ssm(ssm, instance_id)
        results.append({"check": "Instance is online in SSM", "passed": True, "output": instance_id})
        for name, commands, validate in build_checks(topic, args.messages, run_id):
            results.append(run_check(ssm, instance_id, name, commands, validate))
    except TEST_ERRORS as exc:
        results.append({"check": "Instance is online in SSM", "passed": False, "output": str(exc)})

    report = {
        "stack": args.stack_name,
        "instance_id": instance_id,
        "run_id": run_id,
        "duration_seconds": round(time.monotonic() - started),
        "passed": all(result["passed"] for result in results),
        "results": results,
    }

    with open(args.output, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=2)

    markdown = render_markdown(report)
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    print(markdown)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as file:
            file.write(markdown + "\n")

    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
