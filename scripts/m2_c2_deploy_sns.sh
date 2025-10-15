#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="us-east-1"
STACK_NAME="calendly-sns-alerts"
TEMPLATE="infrastructure/m2_c2_sns_alerts.yaml"

echo "==> Validating template..."
aws cloudformation validate-template --template-body file://"$TEMPLATE" >/dev/null
echo "Template is valid."

echo "==> Checking if stack exists..."
if aws cloudformation describe-stacks --stack-name "$STACK_NAME" >/dev/null 2>&1; then
  echo "Stack exists. Updating..."
  set +e
  UPDATE_OUT=$(aws cloudformation update-stack \
    --stack-name "$STACK_NAME" \
    --template-body file://"$TEMPLATE" \
    --region "$AWS_REGION" 2>&1)
  STATUS=$?
  set -e
  if [ $STATUS -ne 0 ]; then
    if echo "$UPDATE_OUT" | grep -q "No updates are to be performed"; then
      echo "No updates were needed."
    else
      echo "$UPDATE_OUT"
      exit $STATUS
    fi
  else
    echo "Waiting for update to complete..."
    aws cloudformation wait stack-update-complete --stack-name "$STACK_NAME"
  fi
else
  echo "Creating stack..."
  aws cloudformation create-stack \
    --stack-name "$STACK_NAME" \
    --template-body file://"$TEMPLATE" \
    --region "$AWS_REGION"
  echo "Waiting for create to complete..."
  aws cloudformation wait stack-create-complete --stack-name "$STACK_NAME"
fi

echo "==> Stack ready. Fetching outputs..."
aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs" --output table

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
TOPIC_ARN="arn:aws:sns:${AWS_REGION}:${ACCOUNT_ID}:calendly-pipeline-alerts"

echo "==> Current subscriptions for ${TOPIC_ARN}:"
aws sns list-subscriptions-by-topic --topic-arn "$TOPIC_ARN" --output table
echo "If Endpoint shows 'PendingConfirmation', check your email and confirm the subscription."
