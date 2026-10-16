#!/usr/bin/env bash
# Stand up the Triple Whale -> BigQuery pipeline in a GCP project, start to finish.
# Safe to re-run: every step checks for what it creates first.
#
#   ./setup_gcp.sh            # run every step
#   ./setup_gcp.sh datasets   # run one step (see STEPS below)
#
# Prerequisites: gcloud + bq + docker (Cloud Shell has all three), and the roles
# listed under "roles" below already granted to you. See SETUP.md for the why.
set -euo pipefail

PROJECT=${PROJECT:-bgzdata}
REGION=${REGION:-us-central1}
BILLING=${BILLING:-014263-F7E3CB-3A07B2}
OPERATOR=${OPERATOR:-$(gcloud config get-value account 2>/dev/null)}
SA=tw-pipeline@$PROJECT.iam.gserviceaccount.com
SCHED_SA=tw-scheduler@$PROJECT.iam.gserviceaccount.com
IMAGE_TAG=${IMAGE_TAG:-v1}
STEPS="roles billing apis datasets pipeline_sa bq_grants secret registry job scheduler backfill views authorize check"

say() { printf '\n=== %s\n' "$*"; }

step_roles() {  # An admin runs this for you; printed, not executed.
  say "roles (run by a project admin for $OPERATOR)"
  cat <<EOF
for ROLE in roles/billing.projectManager roles/serviceusage.serviceUsageAdmin \\
            roles/bigquery.dataOwner roles/iam.serviceAccountCreator \\
            roles/resourcemanager.projectIamAdmin roles/secretmanager.admin \\
            roles/run.developer roles/artifactregistry.admin \\
            roles/cloudscheduler.admin roles/logging.viewer \\
            roles/monitoring.notificationChannelEditor roles/monitoring.alertPolicyEditor; do
  gcloud projects add-iam-policy-binding $PROJECT --member="user:$OPERATOR" --role="\$ROLE" --condition=None
done
EOF
}

step_billing() {
  say "billing"
  if [ "$(gcloud billing projects describe "$PROJECT" --format='value(billingEnabled)' 2>/dev/null)" = "True" ]; then
    echo "already linked"
  else
    gcloud billing projects link "$PROJECT" --billing-account="$BILLING"
  fi
}

step_apis() {
  say "apis"
  gcloud services enable bigquery.googleapis.com bigquerystorage.googleapis.com \
    secretmanager.googleapis.com cloudscheduler.googleapis.com \
    run.googleapis.com artifactregistry.googleapis.com --project="$PROJECT"
}

step_datasets() {
  say "datasets"
  for d in triple_whale_raw triple_whale_staging triple_whale_curated; do
    if bq show --format=none "$PROJECT:$d" >/dev/null 2>&1; then
      echo "$d: exists"
    else
      bq --location=US mk --dataset \
        --description="Triple Whale pipeline - ${d#triple_whale_} layer" "$PROJECT:$d"
    fi
  done
}

step_pipeline_sa() {
  say "pipeline service account (no keys: the job runs as this account)"
  gcloud iam service-accounts describe "$SA" --project="$PROJECT" >/dev/null 2>&1 \
    && echo "exists" \
    || gcloud iam service-accounts create tw-pipeline --project="$PROJECT" \
         --display-name="Triple Whale pipeline" \
         --description="Syncs Triple Whale API data into BigQuery triple_whale_raw"
}

step_bq_grants() {
  say "BigQuery grants: write on the raw dataset only, plus job execution"
  # bq add-iam-policy-binding on a dataset is allowlist gated; SQL GRANT is not.
  bq query --use_legacy_sql=false --location=US --project_id="$PROJECT" \
    "GRANT \`roles/bigquery.dataEditor\` ON SCHEMA \`$PROJECT.triple_whale_raw\` TO \"serviceAccount:$SA\""
  gcloud projects add-iam-policy-binding "$PROJECT" --condition=None \
    --member="serviceAccount:$SA" --role=roles/bigquery.jobUser >/dev/null
  bq show --format=prettyjson "$PROJECT:triple_whale_raw" | grep -A2 tw-pipeline || true
}

step_secret() {
  say "API key secret"
  if gcloud secrets describe triple-whale-api-key --project="$PROJECT" >/dev/null 2>&1; then
    echo "secret exists"
  else
    gcloud secrets create triple-whale-api-key --project="$PROJECT" \
      --replication-policy=automatic --labels=pipeline=triple-whale
  fi
  if [ "$(gcloud secrets versions list triple-whale-api-key --project="$PROJECT" --format='value(name)' | wc -l)" -eq 0 ]; then
    # Prompt hides the key: it never reaches the screen or shell history.
    read -rsp "Triple Whale API key: " TW_KEY && echo
    printf '%s' "$TW_KEY" | gcloud secrets versions add triple-whale-api-key \
      --project="$PROJECT" --data-file=-
    unset TW_KEY
  else
    echo "key already stored"
  fi
  gcloud secrets add-iam-policy-binding triple-whale-api-key --project="$PROJECT" --condition=None \
    --member="serviceAccount:$SA" --role=roles/secretmanager.secretAccessor >/dev/null
}

step_registry() {
  say "image repository"
  gcloud artifacts repositories describe tw-pipeline --location="$REGION" --project="$PROJECT" >/dev/null 2>&1 \
    && echo "exists" \
    || gcloud artifacts repositories create tw-pipeline --repository-format=docker \
         --location="$REGION" --project="$PROJECT" --description="Triple Whale sync images"
  gcloud auth configure-docker "$REGION-docker.pkg.dev" --quiet
}

step_job() {
  say "build, push and deploy the Cloud Run job"
  ./deploy.sh "$IMAGE_TAG"
}

step_scheduler() {
  say "scheduler identity and daily trigger (06:00 America/Chicago)"
  gcloud iam service-accounts describe "$SCHED_SA" --project="$PROJECT" >/dev/null 2>&1 \
    && echo "scheduler SA exists" \
    || gcloud iam service-accounts create tw-scheduler --project="$PROJECT" \
         --display-name="Triple Whale sync trigger" \
         --description="Cloud Scheduler identity for the tw-sync job"
  # Project level on purpose: IAM conditions on the job name make Scheduler 403,
  # and a job level binding needs run.jobs.setIamPolicy. See SETUP.md step 8.
  gcloud projects add-iam-policy-binding "$PROJECT" --condition=None \
    --member="serviceAccount:$SCHED_SA" --role=roles/run.invoker >/dev/null
  if gcloud scheduler jobs describe tw-sync-daily --location="$REGION" --project="$PROJECT" >/dev/null 2>&1; then
    echo "schedule exists"
  else
    gcloud scheduler jobs create http tw-sync-daily --project="$PROJECT" --location="$REGION" \
      --schedule="0 6 * * *" --time-zone="America/Chicago" --http-method=POST \
      --uri="https://run.googleapis.com/v2/projects/$PROJECT/locations/$REGION/jobs/tw-sync:run" \
      --oauth-service-account-email="$SCHED_SA" \
      --description="Daily Triple Whale -> BigQuery sync" --attempt-deadline=180s
  fi
  echo "IAM takes a few minutes to apply; test with:"
  echo "  gcloud scheduler jobs run tw-sync-daily --location=$REGION --project=$PROJECT"
  echo "  gcloud logging read 'resource.type=\"cloud_scheduler_job\"' --project=$PROJECT --freshness=10m --limit=2 --format='value(severity,httpRequest.status)'"
}

step_backfill() {
  say "full history backfill (runs in the background on Cloud Run)"
  gcloud run jobs execute tw-sync --project="$PROJECT" --region="$REGION" --async --args=--backfill \
    --format='value(metadata.name)'
  echo "watch: gcloud logging read 'resource.type=\"cloud_run_job\" AND textPayload:\"DONE\"' --project=$PROJECT --freshness=1d --limit=50 --format='value(textPayload)'"
}

step_views() {
  say "staging and curated views"
  PY=$([ -x .venv/bin/python ] && echo .venv/bin/python || echo python3)
  $PY build_views.py
  $PY build_curated.py
}

step_authorize() {
  say "authorize each layer to read the one below"
  authorize() {  # target dataset, then datasets allowed to read it through views
    local tgt=$1; shift
    bq show --format=prettyjson "$PROJECT:$tgt" > "/tmp/ds_$tgt.json"
    python3 - "$PROJECT" "$tgt" "$@" <<'PY'
import json, sys
project, tgt, *datasets = sys.argv[1:]
f = f"/tmp/ds_{tgt}.json"
access = json.load(open(f)).get("access", [])
for ds in datasets:
    if not any(a.get("dataset", {}).get("dataset", {}).get("datasetId") == ds for a in access):
        access.append({"dataset": {"dataset": {"projectId": project, "datasetId": ds},
                                   "targetTypes": ["VIEWS"]}})
json.dump({"access": access}, open(f, "w"))
PY
    bq update --source "/tmp/ds_$tgt.json" "$PROJECT:$tgt" >/dev/null && echo "authorized on $tgt"
  }
  authorize triple_whale_raw triple_whale_staging triple_whale_curated
  authorize triple_whale_staging triple_whale_curated
}

step_check() {
  say "health check"
  bq query --use_legacy_sql=false --format=pretty --project_id="$PROJECT" \
    "SELECT source, latest_data_date, days_behind, total_rows
     FROM \`$PROJECT.triple_whale_curated.pipeline_status\`
     WHERE total_rows > 0 ORDER BY days_behind DESC LIMIT 15"
  bq query --use_legacy_sql=false --format=pretty --project_id="$PROJECT" \
    "SELECT run_id, mode, status, started_at, finished_at,
            (SELECT COUNTIF(s.status = 'failed') FROM UNNEST(sources) s) AS failed
     FROM \`$PROJECT.triple_whale_raw._sync_runs\` ORDER BY started_at DESC LIMIT 5"
}

cd "$(dirname "$0")"
for s in ${@:-$STEPS}; do "step_$s"; done
say "done. Alerts still need the monitoring roles, then: ./setup_alerts.sh you@example.com"
