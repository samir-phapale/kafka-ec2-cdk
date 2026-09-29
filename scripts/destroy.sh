#!/usr/bin/env bash
set -euo pipefail
export MSYS_NO_PATHCONV=1

STACK_NAME="${1:?usage: destroy.sh <stack-name>}"

stack_status() {
  local output
  if output=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
    --query "Stacks[0].StackStatus" --output text 2>&1); then
    echo "$output"
  elif [[ "$output" == *"does not exist"* ]]; then
    echo "NOT_FOUND"
  else
    echo "$output" >&2
    return 1
  fi
}

status=$(stack_status)
while [[ "$status" == *_IN_PROGRESS && "$status" != "REVIEW_IN_PROGRESS" ]]; do
  echo "Stack $STACK_NAME is $status, waiting for it to settle"
  sleep 20
  status=$(stack_status)
done

if [[ "$status" == "NOT_FOUND" ]]; then
  echo "Stack $STACK_NAME does not exist"
else
  echo "Destroying $STACK_NAME (status: $status)"
  npx --yes aws-cdk@2 destroy "$STACK_NAME" --force

  status=$(stack_status)
  if [[ "$status" != "NOT_FOUND" ]]; then
    echo "Stack $STACK_NAME still exists with status $status" >&2
    exit 1
  fi
  echo "Stack $STACK_NAME destroyed"
fi

log_groups=$(aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/${STACK_NAME}-" \
  --query "logGroups[].logGroupName" --output text)
for log_group in $log_groups; do
  echo "Deleting log group $log_group"
  aws logs delete-log-group --log-group-name "$log_group"
done
