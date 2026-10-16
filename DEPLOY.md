# Deploying tw-sync into `bgzdata`

These steps deploy the Triple Whale -> BigQuery sync job (`tw-sync`) into GCP project
`bgzdata`. Run them in order, from Cloud Shell, on the `bgzdata` branch of this repo.

**Already in place in `bgzdata`. Do not recreate or change these:**
- Secret `triple-whale-api-key` with its key value. This guide only grants the job's
  service account read access to it, and never reads or prints the value.
- Datasets `triple_whale_raw`, `triple_whale_staging` and `triple_whale_curated`, plus
  the staging and curated views. Do **not** run `build_views.py` or `build_curated.py`.

**Do not touch project `triplewhale-508216`.** Its 06:00 America/Chicago job keeps
running. The new job runs at 07:00 so the two don't call the Triple Whale API at the
same time.

Each step is safe to re-run: anything that already exists is reported and skipped.

---

## 0. Before you start

You need the roles listed in [Required IAM roles](#required-iam-roles) on `bgzdata`.

Open Cloud Shell, then get the code and set the variables. **Every later step uses these
variables**, so if Cloud Shell disconnects, run this block again:

```bash
# Skip the clone if ~/tw-pipeline already exists.
gh auth login          # once; choose GitHub.com > HTTPS > login with a web browser
gh repo clone <OWNER>/tw-pipeline ~/tw-pipeline
cd ~/tw-pipeline && git fetch && git switch bgzdata && git pull

export PROJECT=bgzdata
export REGION=us-central1
export SA=tw-pipeline@$PROJECT.iam.gserviceaccount.com
export SCHED_SA=tw-scheduler@$PROJECT.iam.gserviceaccount.com
gcloud config set project $PROJECT

# Sanity check: must print bgzdata, and your own account.
gcloud config get-value project; gcloud config get-value account
```

Confirm that the secret and the raw dataset exist. Both commands only print metadata,
never the key:

```bash
gcloud secrets describe triple-whale-api-key --project=$PROJECT --format='value(name)'
bq show --format=none $PROJECT:triple_whale_raw && echo "triple_whale_raw exists"
```

If either fails, stop and check with Jakob before going further.

---

## 1. Enable APIs

```bash
gcloud services enable bigquery.googleapis.com bigquerystorage.googleapis.com \
  secretmanager.googleapis.com cloudscheduler.googleapis.com \
  run.googleapis.com artifactregistry.googleapis.com iam.googleapis.com \
  --project=$PROJECT
```

## 2. Pipeline service account: `tw-pipeline`

The Cloud Run job runs as this account. It has no keys.

```bash
gcloud iam service-accounts describe $SA --project=$PROJECT >/dev/null 2>&1 \
  && echo "tw-pipeline exists" \
  || gcloud iam service-accounts create tw-pipeline --project=$PROJECT \
       --display-name="Triple Whale pipeline" \
       --description="Syncs Triple Whale API data into BigQuery triple_whale_raw"
```

New service accounts can take up to a minute to become visible to IAM. If the next
step says the account does not exist, wait a minute and run it again.

### 2a. BigQuery Job User on the project

```bash
gcloud projects add-iam-policy-binding $PROJECT --condition=None \
  --member="serviceAccount:$SA" --role=roles/bigquery.jobUser --format=none
```

### 2b. BigQuery Data Editor on `triple_whale_raw` only

This grant is on the dataset, not the project. The account gets no write access to
staging or curated.

```bash
bq query --use_legacy_sql=false --location=US --project_id=$PROJECT \
  "GRANT \`roles/bigquery.dataEditor\` ON SCHEMA \`$PROJECT.triple_whale_raw\` TO \"serviceAccount:$SA\""

# Verify: should show a WRITER / dataEditor entry for tw-pipeline.
bq show --format=prettyjson $PROJECT:triple_whale_raw | grep -B1 -A1 tw-pipeline
```

> If `triple_whale_raw` is not in the `US` location, change `--location=US` to the
> location reported by `bq show --format=prettyjson $PROJECT:triple_whale_raw | grep location`.

### 2c. Secret Accessor on `triple-whale-api-key` only

This grant is on the existing secret, not the project. It does not create or change
the secret, and does not read its value.

```bash
gcloud secrets add-iam-policy-binding triple-whale-api-key --project=$PROJECT \
  --condition=None --member="serviceAccount:$SA" \
  --role=roles/secretmanager.secretAccessor --format=none
```

## 3. Scheduler service account: `tw-scheduler`

Cloud Scheduler uses this account to start the job.

```bash
gcloud iam service-accounts describe $SCHED_SA --project=$PROJECT >/dev/null 2>&1 \
  && echo "tw-scheduler exists" \
  || gcloud iam service-accounts create tw-scheduler --project=$PROJECT \
       --display-name="Triple Whale sync trigger" \
       --description="Cloud Scheduler identity for the tw-sync job"

# Cloud Run Invoker on the project. This is a project-level grant on purpose: in
# triplewhale-508216, IAM conditions on the job name made Scheduler return 403.
gcloud projects add-iam-policy-binding $PROJECT --condition=None \
  --member="serviceAccount:$SCHED_SA" --role=roles/run.invoker --format=none
```

## 4. Build and deploy the Cloud Run job

Create the image repository (one time), then build, push and deploy with `deploy.sh`.
The script defaults to `PROJECT=bgzdata` and sets `GCP_PROJECT=bgzdata` on the job, so
the job reads the secret and writes BigQuery data in `bgzdata`.

```bash
gcloud artifacts repositories describe tw-pipeline --location=$REGION --project=$PROJECT >/dev/null 2>&1 \
  && echo "repository exists" \
  || gcloud artifacts repositories create tw-pipeline --repository-format=docker \
       --location=$REGION --project=$PROJECT --description="Triple Whale sync images"

gcloud auth configure-docker $REGION-docker.pkg.dev --quiet

./deploy.sh v1
```

Verify that the job runs as `tw-pipeline` and has `GCP_PROJECT=bgzdata`:

```bash
gcloud run jobs describe tw-sync --project=$PROJECT --region=$REGION \
  --format='yaml(spec.template.spec.template.spec.serviceAccountName,spec.template.spec.template.spec.containers[0].env)'
```

## 5. Scheduler job: 07:00 America/Chicago

```bash
if gcloud scheduler jobs describe tw-sync-daily --location=$REGION --project=$PROJECT >/dev/null 2>&1; then
  echo "schedule exists"
else
  gcloud scheduler jobs create http tw-sync-daily --project=$PROJECT --location=$REGION \
    --schedule="0 7 * * *" --time-zone="America/Chicago" --http-method=POST \
    --uri="https://run.googleapis.com/v2/projects/$PROJECT/locations/$REGION/jobs/tw-sync:run" \
    --oauth-service-account-email=$SCHED_SA \
    --description="Daily Triple Whale -> BigQuery sync" --attempt-deadline=180s
fi

# Verify: schedule should read 0 7 * * *, America/Chicago, ENABLED.
gcloud scheduler jobs describe tw-sync-daily --location=$REGION --project=$PROJECT \
  --format='value(schedule,timeZone,state)'
```

Don't use `gcloud scheduler jobs run` to test the trigger. It would start a second sync
alongside step 6. The first scheduled run is at 07:00 tomorrow, and step 7 shows how to
check it.

## 6. Run once: daily mode from 2026-10-12

This is a normal daily run (no `--backfill`), with its start date set to 2026-10-12. It
replaces 2026-10-12 through today in each dated table. It is safe to re-run.

```bash
gcloud run jobs execute tw-sync --project=$PROJECT --region=$REGION --async \
  --args=--start=2026-10-12 --format='value(metadata.name)'
```

Watch it:

```bash
# Status of recent executions (wait for Succeeded or Failed).
gcloud run jobs executions list --job=tw-sync --project=$PROJECT --region=$REGION --limit=3

# Log lines: run start, per-source DONE, and the final result.
gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="tw-sync" AND (textPayload:"mode=" OR textPayload:"DONE" OR textPayload:"FAILED" OR textPayload:"finished OK" OR textPayload:"failed:")' \
  --project=$PROJECT --freshness=1h --limit=100 --format='value(timestamp,textPayload)'
```

When it finishes, check the run record:

```bash
bq query --use_legacy_sql=false --format=pretty --project_id=$PROJECT \
  "SELECT run_id, mode, status, started_at, finished_at,
          (SELECT COUNTIF(s.status = 'failed') FROM UNNEST(sources) s) AS failed
   FROM \`$PROJECT.triple_whale_raw._sync_runs\` ORDER BY started_at DESC LIMIT 3"
```

Expected result: `mode = daily`, `status = ok`, `failed = 0`. If any source failed, list
which ones:

```bash
bq query --use_legacy_sql=false --format=pretty --project_id=$PROJECT \
  "SELECT s.source, s.error FROM \`$PROJECT.triple_whale_raw._sync_runs\` r, UNNEST(r.sources) s
   WHERE r.run_id = (SELECT run_id FROM \`$PROJECT.triple_whale_raw._sync_runs\` ORDER BY started_at DESC LIMIT 1)
     AND s.status = 'failed'"
```

## 7. Next morning: confirm the scheduled run

After 07:00 America/Chicago:

```bash
gcloud logging read 'resource.type="cloud_scheduler_job" AND resource.labels.job_id="tw-sync-daily"' \
  --project=$PROJECT --freshness=1d --limit=2 --format='value(timestamp,severity,httpRequest.status)'
gcloud run jobs executions list --job=tw-sync --project=$PROJECT --region=$REGION --limit=3
```

A `403` from the scheduler usually means the `run.invoker` grant from step 3 had not
taken effect yet. IAM changes can take a few minutes to apply. Let the next scheduled run
retry, or ask Jakob.

---

## Required IAM roles

These are the roles the person running this guide needs on project `bgzdata`. A project
Owner has all of them. For least privilege, have an admin grant these (replace the email):

| Role | Why |
|---|---|
| `roles/serviceusage.serviceUsageAdmin` | Step 1: enable APIs |
| `roles/iam.serviceAccountCreator` | Steps 2–3: create `tw-pipeline` and `tw-scheduler` |
| `roles/iam.serviceAccountUser` | Steps 4–5: deploy a job that runs as `tw-pipeline`, and create a scheduler job that authenticates as `tw-scheduler` (`iam.serviceAccounts.actAs`) |
| `roles/resourcemanager.projectIamAdmin` | Steps 2a and 3: project-level grants (`bigquery.jobUser`, `run.invoker`) |
| `roles/bigquery.dataOwner` | Step 2b: `GRANT` on the `triple_whale_raw` dataset (only needs to be on that dataset) |
| `roles/bigquery.jobUser` | Steps 2b and 6: run `bq query` |
| `roles/secretmanager.admin` | Step 2c: set the IAM policy on `triple-whale-api-key` (only needs to be on that secret). It does **not** require reading the value. |
| `roles/artifactregistry.admin` | Step 4: create the `tw-pipeline` repository and push the image |
| `roles/run.developer` | Steps 4 and 6: deploy the job and execute it with `--args` overrides |
| `roles/cloudscheduler.admin` | Step 5: create the scheduler job |
| `roles/logging.viewer` | Steps 6–7: read job and scheduler logs |

Grant command for an admin (project-level grants; `bigquery.dataOwner` and
`secretmanager.admin` can be narrowed to the dataset and secret instead):

```bash
OPERATOR=meena@example.com   # replace
for ROLE in roles/serviceusage.serviceUsageAdmin roles/iam.serviceAccountCreator \
            roles/iam.serviceAccountUser roles/resourcemanager.projectIamAdmin \
            roles/bigquery.dataOwner roles/bigquery.jobUser roles/secretmanager.admin \
            roles/artifactregistry.admin roles/run.developer \
            roles/cloudscheduler.admin roles/logging.viewer; do
  gcloud projects add-iam-policy-binding bgzdata --member="user:$OPERATOR" --role="$ROLE" --condition=None --format=none
done
```

Optional, not part of this deploy: email alerts (`./setup_alerts.sh <email>`) need
`roles/monitoring.notificationChannelEditor` and `roles/monitoring.alertPolicyEditor`.
