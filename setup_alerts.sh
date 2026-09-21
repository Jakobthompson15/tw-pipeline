#!/usr/bin/env bash
# Email alerts for the tw-sync pipeline. Needs roles/monitoring.notificationChannelEditor
# and roles/monitoring.alertPolicyEditor on the project. Safe to re-run (skips existing).
#   ./setup_alerts.sh you@example.com
set -euo pipefail

PROJECT=triplewhale-508216
EMAIL=${1:?usage: ./setup_alerts.sh <email>}

CHANNEL=$(gcloud beta monitoring channels list --project=$PROJECT \
  --filter="type=email AND labels.email_address=$EMAIL" --format='value(name)' | head -1)
if [ -z "$CHANNEL" ]; then
  CHANNEL=$(gcloud beta monitoring channels create --project=$PROJECT \
    --display-name="tw-sync alerts ($EMAIL)" --type=email \
    --channel-labels=email_address="$EMAIL" --format='value(name)')
fi
echo "channel $CHANNEL"

create_policy() {  # name, json
  if gcloud alpha monitoring policies list --project=$PROJECT \
       --filter="displayName=\"$1\"" --format='value(name)' | grep -q .; then
    echo "policy exists: $1"; return
  fi
  gcloud alpha monitoring policies create --project=$PROJECT --policy="$2" --format='value(name)'
}

# 1. Any tw-sync execution that ends in failure.
create_policy "tw-sync: run failed" '{
  "displayName": "tw-sync: run failed",
  "documentation": {"content": "The Triple Whale -> BigQuery sync failed. Check Cloud Run > Jobs > tw-sync > executions, or triple_whale_staging.sync_runs for which source failed.", "mimeType": "text/markdown"},
  "combiner": "OR",
  "conditions": [{
    "displayName": "failed execution",
    "conditionThreshold": {
      "filter": "resource.type = \"cloud_run_job\" AND resource.labels.job_name = \"tw-sync\" AND metric.type = \"run.googleapis.com/job/completed_execution_count\" AND metric.labels.result = \"failed\"",
      "aggregations": [{"alignmentPeriod": "300s", "perSeriesAligner": "ALIGN_SUM"}],
      "comparison": "COMPARISON_GT", "thresholdValue": 0, "duration": "0s"
    }
  }],
  "alertStrategy": {"autoClose": "86400s"},
  "notificationChannels": ["'"$CHANNEL"'"]
}'

# 2. No successful run for 26 hours (scheduler broken, job never started, etc.).
create_policy "tw-sync: no successful run in 26h" '{
  "displayName": "tw-sync: no successful run in 26h",
  "documentation": {"content": "tw-sync has not completed successfully in over 26 hours. Check Cloud Scheduler job tw-sync-daily and Cloud Run job tw-sync.", "mimeType": "text/markdown"},
  "combiner": "OR",
  "conditions": [{
    "displayName": "no succeeded executions",
    "conditionAbsent": {
      "filter": "resource.type = \"cloud_run_job\" AND resource.labels.job_name = \"tw-sync\" AND metric.type = \"run.googleapis.com/job/completed_execution_count\" AND metric.labels.result = \"succeeded\"",
      "aggregations": [{"alignmentPeriod": "3600s", "perSeriesAligner": "ALIGN_SUM"}],
      "duration": "93600s"
    }
  }],
  "notificationChannels": ["'"$CHANNEL"'"]
}'
