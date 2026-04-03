#!/usr/bin/env bash
set -euo pipefail

AWS_REGION=${AWS_REGION:-ap-south-1}
LOG_GROUP_BROKER=${LOG_GROUP_BROKER:-/trading/broker}
LOG_GROUP_ERRORS=${LOG_GROUP_ERRORS:-/trading/errors}
SNS_TOPIC_ARN=${SNS_TOPIC_ARN:?SNS_TOPIC_ARN is required}

aws logs put-metric-filter \
  --region "$AWS_REGION" \
  --log-group-name "$LOG_GROUP_BROKER" \
  --filter-name trading-dhan-failure-filter \
  --filter-pattern '"BROKER_ERROR" || "Dhan order placement failed" || "ERROR"' \
  --metric-transformations metricName=TradingBrokerErrors,metricNamespace=TradingApp,metricValue=1

aws logs put-metric-filter \
  --region "$AWS_REGION" \
  --log-group-name "$LOG_GROUP_ERRORS" \
  --filter-name trading-no-trade-filter \
  --filter-pattern '"No qualifying setup" || "No actionable trade candidates"' \
  --metric-transformations metricName=TradingNoTradeCycles,metricNamespace=TradingApp,metricValue=1

aws cloudwatch put-metric-alarm \
  --region "$AWS_REGION" \
  --alarm-name trading-broker-errors \
  --alarm-description "Broker/API failures detected in trading logs" \
  --namespace TradingApp \
  --metric-name TradingBrokerErrors \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --threshold 1 \
  --evaluation-periods 1 \
  --period 300 \
  --statistic Sum \
  --alarm-actions "$SNS_TOPIC_ARN"

aws cloudwatch put-metric-alarm \
  --region "$AWS_REGION" \
  --alarm-name trading-no-trade-window \
  --alarm-description "No-trade cycles detected repeatedly" \
  --namespace TradingApp \
  --metric-name TradingNoTradeCycles \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --threshold 3 \
  --evaluation-periods 1 \
  --period 3600 \
  --statistic Sum \
  --alarm-actions "$SNS_TOPIC_ARN"

echo "CloudWatch metric filters and alarms created."
