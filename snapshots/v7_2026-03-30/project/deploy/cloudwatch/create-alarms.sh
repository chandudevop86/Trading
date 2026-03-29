#!/usr/bin/env bash
set -euo pipefail

AWS_REGION=${AWS_REGION:-ap-south-1}
INSTANCE_ID=${INSTANCE_ID:?INSTANCE_ID is required}
SNS_TOPIC_ARN=${SNS_TOPIC_ARN:?SNS_TOPIC_ARN is required}
NAMESPACE=${NAMESPACE:-CWAgent}

aws cloudwatch put-metric-alarm \
  --alarm-name trading-high-cpu \
  --alarm-description "Trading EC2 CPU above 80%" \
  --namespace "$NAMESPACE" \
  --metric-name cpu_usage_idle \
  --comparison-operator LessThanThreshold \
  --threshold 20 \
  --evaluation-periods 5 \
  --period 60 \
  --statistic Average \
  --dimensions Name=InstanceId,Value="$INSTANCE_ID" \
  --alarm-actions "$SNS_TOPIC_ARN" \
  --region "$AWS_REGION"

aws cloudwatch put-metric-alarm \
  --alarm-name trading-high-memory \
  --alarm-description "Trading EC2 memory above 85%" \
  --namespace "$NAMESPACE" \
  --metric-name mem_used_percent \
  --comparison-operator GreaterThanThreshold \
  --threshold 85 \
  --evaluation-periods 5 \
  --period 60 \
  --statistic Average \
  --dimensions Name=InstanceId,Value="$INSTANCE_ID" \
  --alarm-actions "$SNS_TOPIC_ARN" \
  --region "$AWS_REGION"

aws cloudwatch put-metric-alarm \
  --alarm-name trading-disk-high \
  --alarm-description "Trading EC2 disk usage above 85%" \
  --namespace "$NAMESPACE" \
  --metric-name used_percent \
  --comparison-operator GreaterThanThreshold \
  --threshold 85 \
  --evaluation-periods 5 \
  --period 60 \
  --statistic Average \
  --dimensions Name=InstanceId,Value="$INSTANCE_ID" Name=path,Value=/ Name=device,Value=xvda1 Name=fstype,Value=ext4 \
  --alarm-actions "$SNS_TOPIC_ARN" \
  --region "$AWS_REGION"

echo "Baseline CloudWatch alarms created. Add log metric filters for Dhan failures and no-trade windows in the console or IaC."
