# Building the pipeline: the commands, step by step

Follow this in Cloud Shell (it already has `gcloud`, `bq`, `docker` and `python3`).
Each step says what it does, the exact command, and how to check it worked.
Steps map one to one to the twelve steps in the overview.

Set these once at the top of your session. Everything below uses them.

```bash
PROJECT=triplewhale-508216
REGION=us-central1
BILLING=014263-F7E3CB-3A07B2
YOU=$(gcloud config get-value account)
SA=tw-pipeline@$PROJECT.iam.gserviceaccount.com
SCHED_SA=tw-scheduler@$PROJECT.iam.gserviceaccount.com

gcloud config set project $PROJECT
```

---

## Step 1. Access

Ask a project admin to grant a narrow set of roles instead of owner. You cannot
run this yourself; send it to whoever administers the project.

```bash
for ROLE in roles/billing.projectManager roles/serviceusage.serviceUsageAdmin \
            roles/bigquery.dataOwner roles/iam.serviceAccountCreator \
            roles/resourcemanager.projectIamAdmin roles/secretmanager.admin \
            roles/run.developer roles/artifactregistry.admin \
            roles/cloudscheduler.admin roles/logging.viewer \
            roles/monitoring.notificationChannelEditor roles/monitoring.alertPolicyEditor; do
  gcloud projects add-iam-policy-binding $PROJECT --member="user:$YOU" --role="$ROLE" --condition=None
done
```

Check what you have:

```bash
gcloud projects get-iam-policy $PROJECT \
  --flatten="bindings[].members" --filter="bindings.members:$YOU" \
  --format="value(bindings.role)"
```

IAM changes can take a few minutes to take effect. A permission error right
after a grant is usually just propagation.

---

## Step 2. Billing

Confirm you are in the right project before anything else, then link billing.

```bash
gcloud config get-value project
gcloud billing projects link $PROJECT --billing-account=$BILLING
gcloud billing projects describe $PROJECT --format='value(billingEnabled)'   # expect True
```

---

## Step 3. APIs

```bash
gcloud services enable \
  bigquery.googleapis.com bigquerystorage.googleapis.com \
  secretmanager.googleapis.com cloudscheduler.googleapis.com \
  run.googleapis.com artifactregistry.googleapis.com --project=$PROJECT

gcloud services list --enabled --project=$PROJECT --format='value(config.name)' | sort
```

---

## Step 4. Datasets

Three layers: raw data, typed views, business views. The `if` check means you can
re-run this safely.

```bash
for d in triple_whale_raw triple_whale_staging triple_whale_curated; do
  if bq show --format=none "$PROJECT:$d" >/dev/null 2>&1; then
    echo "$d: exists"
  else
    bq --location=US mk --dataset \
      --description="Triple Whale pipeline - ${d#triple_whale_} layer" "$PROJECT:$d"
  fi
done

bq ls --project_id=$PROJECT
```

---

## Step 5. Pipeline identity

A service account for the sync job. No key file is created, because the job runs
as this account directly.

```bash
gcloud iam service-accounts create tw-pipeline --project=$PROJECT \
  --display-name="Triple Whale pipeline" \
  --description="Syncs Triple Whale API data into BigQuery triple_whale_raw"

gcloud iam service-accounts describe $SA --project=$PROJECT --format='value(email,disabled)'
```

---

## Step 6. Permissions for that identity

Write access to the raw dataset only, plus permission to run BigQuery jobs.

Note: `bq add-iam-policy-binding` on a dataset fails with "This feature requires
allowlisting", so the dataset grant uses SQL instead. Same result.

```bash
bq query --use_legacy_sql=false --location=US --project_id=$PROJECT \
  "GRANT \`roles/bigquery.dataEditor\`
   ON SCHEMA \`$PROJECT.triple_whale_raw\`
   TO \"serviceAccount:$SA\""

gcloud projects add-iam-policy-binding $PROJECT --condition=None \
  --member="serviceAccount:$SA" --role="roles/bigquery.jobUser"
```

Check: the account should appear as WRITER on raw, and not at all on the other
two datasets.

```bash
for d in triple_whale_raw triple_whale_staging triple_whale_curated; do
  echo "--- $d"
  bq show --format=prettyjson $PROJECT:$d | grep tw-pipeline || echo "  no access (correct)"
done
```

---

## Step 7. The API key

Create the secret, then put the key in it without the key ever appearing on
screen or in your shell history.

```bash
gcloud secrets create triple-whale-api-key --project=$PROJECT \
  --replication-policy=automatic --labels=pipeline=triple-whale

read -rsp "Triple Whale API key: " TW_KEY && echo && \
  printf '%s' "$TW_KEY" | gcloud secrets versions add triple-whale-api-key \
    --project=$PROJECT --data-file=- ; unset TW_KEY

gcloud secrets add-iam-policy-binding triple-whale-api-key --project=$PROJECT --condition=None \
  --member="serviceAccount:$SA" --role="roles/secretmanager.secretAccessor"
```

Check the version exists without printing the key:

```bash
gcloud secrets versions list triple-whale-api-key --project=$PROJECT
gcloud secrets versions access latest --secret=triple-whale-api-key --project=$PROJECT | wc -c
```

Rotating the key later is the same `versions add` command again.

---

## Step 8. Container and job

Create a private image repository, build the image in Cloud Shell, push it, and
deploy the job. Building locally avoids granting Cloud Build's own service
account extra roles.

```bash
gcloud artifacts repositories create tw-pipeline --repository-format=docker \
  --location=$REGION --project=$PROJECT --description="Triple Whale sync images"
gcloud auth configure-docker $REGION-docker.pkg.dev --quiet

IMG=$REGION-docker.pkg.dev/$PROJECT/tw-pipeline/tw-sync
docker build -t $IMG:v1 .
docker push $IMG:v1
DIGEST=$(gcloud artifacts docker images describe $IMG:v1 --format='value(image_summary.digest)')

gcloud run jobs deploy tw-sync --project=$PROJECT --region=$REGION \
  --image="$IMG@$DIGEST" \
  --service-account=$SA \
  --cpu=2 --memory=4Gi --task-timeout=24h --max-retries=1 --tasks=1
```

Deploy by digest, not by tag, so a job that is already running is never affected
by your next build. From then on `./deploy.sh v2` does all of this.

---

## Step 9. Daily schedule

A second service account whose only purpose is to start the job, plus the 6am
trigger.

```bash
gcloud iam service-accounts create tw-scheduler --project=$PROJECT \
  --display-name="Triple Whale sync trigger" \
  --description="Cloud Scheduler identity for the tw-sync job"

gcloud projects add-iam-policy-binding $PROJECT --condition=None \
  --member="serviceAccount:$SCHED_SA" --role=roles/run.invoker

gcloud scheduler jobs create http tw-sync-daily --project=$PROJECT --location=$REGION \
  --schedule="0 6 * * *" --time-zone="America/Chicago" --http-method=POST \
  --uri="https://run.googleapis.com/v2/projects/$PROJECT/locations/$REGION/jobs/tw-sync:run" \
  --oauth-service-account-email="$SCHED_SA" \
  --description="Daily Triple Whale -> BigQuery sync" --attempt-deadline=180s
```

Test the trigger end to end and read the result:

```bash
gcloud scheduler jobs run tw-sync-daily --location=$REGION --project=$PROJECT

gcloud logging read 'resource.type="cloud_scheduler_job" AND resource.labels.job_id="tw-sync-daily"' \
  --project=$PROJECT --freshness=10m --limit=2 \
  --format='value(timestamp,severity,httpRequest.status)'
```

Expect `200`. A `403` means the invoker grant has not propagated yet, or does not
match the job.

Two notes worth knowing. The grant above is project wide on purpose: restricting
it to the single job with an IAM condition made Scheduler return 403, and doing
it properly at the job level needs `run.jobs.setIamPolicy`, which the operator
account does not have. If your admin has it, they can tighten it:

```bash
gcloud run jobs add-iam-policy-binding tw-sync --region=$REGION --project=$PROJECT \
  --member="serviceAccount:$SCHED_SA" --role=roles/run.invoker
gcloud projects remove-iam-policy-binding $PROJECT --condition=None \
  --member="serviceAccount:$SCHED_SA" --role=roles/run.invoker
```

---

## Step 10. Load the data

Start the full history backfill. It runs on Google's side, so you can close the
terminal.

```bash
gcloud run jobs execute tw-sync --project=$PROJECT --region=$REGION --async --args=--backfill
```

Watch each source finish, and check for failures:

```bash
gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="tw-sync" AND textPayload:"DONE"' \
  --project=$PROJECT --freshness=1d --limit=50 --format='value(textPayload)'

gcloud run jobs executions list --job=tw-sync --region=$REGION --project=$PROJECT --limit=3 \
  --format='table(name,status.startTime,status.completionTime,status.succeededCount,status.failedCount)'
```

Re-run a single source, or a fixed date range, with the same job:

```bash
gcloud run jobs execute tw-sync --project=$PROJECT --region=$REGION --async \
  --args="^@^--backfill@--only@orders_table,ads_table"

gcloud run jobs execute tw-sync --project=$PROJECT --region=$REGION --async \
  --args="^@^--only@orders_table@--start@2026-01-01"
```

The `^@^` prefix tells gcloud to split arguments on `@` instead of commas, which
matters because the source list itself contains commas.

---

## Step 11. Reporting layers

Generate the typed views and the business views:

```bash
python3 build_views.py      # one typed view per raw table
python3 build_curated.py    # daily_kpis, orders, marketing performance, and so on
bq ls --max_results=100 $PROJECT:triple_whale_curated
```

Then authorize each layer to read the one below, so people can be given the
reporting layer without access to raw data:

```bash
authorize() {   # usage: authorize <target dataset> <datasets allowed to read it>
  local tgt=$1; shift
  bq show --format=prettyjson $PROJECT:$tgt > /tmp/ds_$tgt.json
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
  bq update --source /tmp/ds_$tgt.json $PROJECT:$tgt
}

authorize triple_whale_raw triple_whale_staging triple_whale_curated
authorize triple_whale_staging triple_whale_curated
```

Give a colleague read access to reporting only:

```bash
bq query --use_legacy_sql=false --project_id=$PROJECT \
  "GRANT \`roles/bigquery.dataViewer\`
   ON SCHEMA \`$PROJECT.triple_whale_curated\`
   TO \"user:someone@example.com\""

gcloud projects add-iam-policy-binding $PROJECT --condition=None \
  --member="user:someone@example.com" --role=roles/bigquery.jobUser
```

---

## Step 12. Health checks

How fresh is every source, and did the last run succeed?

```bash
bq query --use_legacy_sql=false --format=pretty --project_id=$PROJECT \
  "SELECT source, latest_data_date, days_behind, total_rows
   FROM \`$PROJECT.triple_whale_curated.pipeline_status\`
   WHERE total_rows > 0 ORDER BY days_behind DESC LIMIT 15"

bq query --use_legacy_sql=false --format=pretty --project_id=$PROJECT \
  "SELECT run_id, mode, status, started_at, finished_at,
          (SELECT COUNTIF(s.status = 'failed') FROM UNNEST(sources) s) AS failed_sources
   FROM \`$PROJECT.triple_whale_raw._sync_runs\`
   ORDER BY started_at DESC LIMIT 5"
```

Email alerts on failure (needs the two monitoring roles from step 1):

```bash
./setup_alerts.sh you@example.com
```

---

## If you would rather not type any of this

Every step above is in one idempotent script:

```bash
./setup_gcp.sh              # all steps
./setup_gcp.sh datasets     # or one at a time
PROJECT=other-project BILLING=XXXXXX-XXXXXX-XXXXXX ./setup_gcp.sh   # another client
```

## Cloud Shell gotcha

Cloud Shell stores your `gcloud` login in a temporary folder that belongs to that
session. When the session restarts you are signed out, and signing in from a new
tab writes to a different folder. Point the login at the session that needs it:

```bash
echo $CLOUDSDK_CONFIG    # in the session that is signed out
CLOUDSDK_CONFIG=<that path> gcloud auth login <account> --no-launch-browser
```
