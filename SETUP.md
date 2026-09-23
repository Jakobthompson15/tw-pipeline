# GCP setup, start to finish

Everything that was run to stand this up in `triplewhale-508216`, in order, so it
can be audited or repeated in another project. Replace the project, billing
account, region and shop values for a new client.

```bash
PROJECT=triplewhale-508216
REGION=us-central1
BILLING=014263-F7E3CB-3A07B2
ADMIN=jakob@getdavid.ai
```

## 0. Access needed on the project

Neither the pipeline nor the operator uses an owner role. These were granted by a
project admin (two rounds, listed here as one):

| Role | For |
|---|---|
| `roles/billing.projectManager` | link billing |
| `roles/serviceusage.serviceUsageAdmin` | enable APIs |
| `roles/bigquery.dataOwner` | create datasets, grant dataset access |
| `roles/iam.serviceAccountCreator` | create the two service accounts |
| `roles/resourcemanager.projectIamAdmin` | grant project roles to those accounts |
| `roles/secretmanager.admin` | create the API key secret and its access |
| `roles/run.developer` | deploy the Cloud Run job |
| `roles/cloudbuild.builds.editor` | build images (unused: images are built locally) |
| `roles/artifactregistry.admin` | create the image repository |
| `roles/cloudscheduler.admin` | create the daily schedule |
| `roles/logging.viewer` | read job logs |
| `roles/monitoring.notificationChannelEditor`, `roles/monitoring.alertPolicyEditor` | **still outstanding**: needed for `setup_alerts.sh` |

Two permissions the operator does **not** have, which shaped the setup:
`bigquery.datasets.setIamPolicy` through `bq add-iam-policy-binding` (feature is
allowlist gated) and `run.jobs.setIamPolicy` (so the scheduler grant is
project level, see step 8).

```bash
# what an admin runs
for ROLE in roles/billing.projectManager roles/serviceusage.serviceUsageAdmin \
            roles/bigquery.dataOwner roles/iam.serviceAccountCreator \
            roles/resourcemanager.projectIamAdmin roles/secretmanager.admin \
            roles/run.developer roles/cloudbuild.builds.editor \
            roles/artifactregistry.admin roles/cloudscheduler.admin roles/logging.viewer; do
  gcloud projects add-iam-policy-binding $PROJECT --member="user:$ADMIN" --role="$ROLE" --condition=None
done
```

## 1. Billing

```bash
gcloud config get-value project                       # confirm before anything else
gcloud billing projects link $PROJECT --billing-account=$BILLING
gcloud billing projects describe $PROJECT             # expect billingEnabled: true
```

## 2. APIs

```bash
gcloud services enable \
  secretmanager.googleapis.com pubsub.googleapis.com cloudfunctions.googleapis.com \
  cloudscheduler.googleapis.com eventarc.googleapis.com --project=$PROJECT
# bigquery + bigquerystorage were already on.

# added later, once the design settled on a Cloud Run job:
gcloud services enable run.googleapis.com artifactregistry.googleapis.com --project=$PROJECT
```

`pubsub`, `cloudfunctions` and `eventarc` are enabled but unused; the job is
triggered by Cloud Scheduler over HTTP. They can be disabled.

## 3. BigQuery datasets

```bash
for d in triple_whale_raw triple_whale_staging triple_whale_curated; do
  if bq show --format=none "$PROJECT:$d" >/dev/null 2>&1; then
    echo "$d: EXISTS, skipping"
  else
    bq --location=US mk --dataset \
      --description="BGZ Triple Whale pipeline - ${d#triple_whale_} layer" "$PROJECT:$d"
  fi
done
```

## 4. Pipeline service account

```bash
gcloud iam service-accounts create tw-pipeline --project=$PROJECT \
  --display-name="Triple Whale pipeline (BGZ)" \
  --description="Syncs Triple Whale API data into BigQuery triple_whale_raw"
```

No keys are ever created: the Cloud Run job runs *as* this account.

## 5. Grants for the pipeline account

```bash
SA=tw-pipeline@$PROJECT.iam.gserviceaccount.com

# Write access to the raw dataset only. `bq add-iam-policy-binding` fails with
# "This feature requires allowlisting", so use the SQL GRANT instead.
bq query --use_legacy_sql=false --location=US --project_id=$PROJECT \
 'GRANT `roles/bigquery.dataEditor`
  ON SCHEMA `'"$PROJECT"'.triple_whale_raw`
  TO "serviceAccount:'"$SA"'"'

# Permission to run BigQuery jobs (no data access of its own).
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:$SA" --role="roles/bigquery.jobUser" --condition=None
```

Check: `bq show --format=prettyjson $PROJECT:triple_whale_raw` should list the
account as WRITER, and staging/curated should not mention it at all.

## 6. Triple Whale API key

```bash
gcloud secrets create triple-whale-api-key --project=$PROJECT \
  --replication-policy=automatic --labels=client=bgz,pipeline=triple-whale

# Store the key without it appearing on screen or in shell history:
read -rsp "Triple Whale API key: " TW_KEY && echo && \
  printf '%s' "$TW_KEY" | gcloud secrets versions add triple-whale-api-key \
  --project=$PROJECT --data-file=- ; unset TW_KEY

gcloud secrets add-iam-policy-binding triple-whale-api-key --project=$PROJECT \
  --member="serviceAccount:$SA" --role="roles/secretmanager.secretAccessor" --condition=None
```

The key is a personal Triple Whale login's key. Rotating it is one command:
`gcloud secrets versions add triple-whale-api-key --data-file=-`.

## 7. Image repository and the job

```bash
gcloud artifacts repositories create tw-pipeline --repository-format=docker \
  --location=$REGION --project=$PROJECT --description="BGZ Triple Whale sync images"
gcloud auth configure-docker $REGION-docker.pkg.dev --quiet

./deploy.sh v1     # docker build, push, then:
# gcloud run jobs deploy tw-sync --image=<digest> --service-account=$SA \
#   --cpu=2 --memory=4Gi --task-timeout=24h --max-retries=1 --tasks=1 \
#   --labels=client=bgz,pipeline=triple-whale --region=$REGION
```

Images are built locally in Cloud Shell and pushed, so Cloud Build's own service
account never needs extra roles. Deploy by digest, not tag, so a running
execution is unaffected by the next build.

## 8. Daily schedule

```bash
gcloud iam service-accounts create tw-scheduler --project=$PROJECT \
  --display-name="Triple Whale sync trigger (BGZ)" \
  --description="Cloud Scheduler identity; can only run the tw-sync job"

gcloud projects add-iam-policy-binding $PROJECT --condition=None \
  --member="serviceAccount:tw-scheduler@$PROJECT.iam.gserviceaccount.com" \
  --role=roles/run.invoker

gcloud scheduler jobs create http tw-sync-daily --project=$PROJECT --location=$REGION \
  --schedule="0 6 * * *" --time-zone="America/Chicago" \
  --uri="https://run.googleapis.com/v2/projects/$PROJECT/locations/$REGION/jobs/tw-sync:run" \
  --http-method=POST \
  --oauth-service-account-email="tw-scheduler@$PROJECT.iam.gserviceaccount.com" \
  --description="Daily Triple Whale -> BigQuery sync (BGZ)" --attempt-deadline=180s
```

The grant is project wide on purpose. Two IAM conditions restricting it to the
job (`resource.name == "projects/.../jobs/tw-sync"` and an `endsWith` variant)
both produced 403s from Scheduler, and narrowing it properly needs
`run.jobs.setIamPolicy`, which the operator does not have. `tw-sync` is the only
Cloud Run resource in the project. An admin can replace it with:

```bash
gcloud run jobs add-iam-policy-binding tw-sync --region=$REGION --project=$PROJECT \
  --member="serviceAccount:tw-scheduler@$PROJECT.iam.gserviceaccount.com" --role=roles/run.invoker
gcloud projects remove-iam-policy-binding $PROJECT --condition=None \
  --member="serviceAccount:tw-scheduler@$PROJECT.iam.gserviceaccount.com" --role=roles/run.invoker
```

Verify a trigger end to end:

```bash
gcloud scheduler jobs run tw-sync-daily --location=$REGION --project=$PROJECT
gcloud logging read 'resource.type="cloud_scheduler_job" AND resource.labels.job_id="tw-sync-daily"' \
  --project=$PROJECT --freshness=10m --limit=2 --format='value(timestamp,severity,httpRequest.status)'
# expect 200; a 403 means the invoker grant has not propagated or does not match
```

## 9. Load the data

```bash
gcloud run jobs execute tw-sync --region=$REGION --project=$PROJECT --async --args=--backfill
# a subset, or a fixed start date:
gcloud run jobs execute tw-sync --region=$REGION --project=$PROJECT --async \
  --args="^@^--backfill@--only@orders_table,ads_table"
```

Watch it:

```bash
gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="tw-sync" AND textPayload:"DONE"' \
  --project=$PROJECT --freshness=1d --limit=50 --format='value(textPayload)'
```

## 10. Views and reporting access

```bash
python build_views.py      # 52 typed views in triple_whale_staging
python build_curated.py    # 9 business views in triple_whale_curated
```

Authorize each layer to read the one below, so people can be given curated
access without raw access:

```bash
# for triple_whale_raw: authorize staging + curated; for staging: authorize curated
bq show --format=prettyjson $PROJECT:triple_whale_raw > ds.json
# append to .access:
#   {"dataset":{"dataset":{"projectId":"<project>","datasetId":"triple_whale_staging"},
#    "targetTypes":["VIEWS"]}}
bq update --source ds.json $PROJECT:triple_whale_raw
```

Grant a person read access to reporting only:

```bash
bq add-iam-policy-binding --dataset $PROJECT:triple_whale_curated \
  --member="user:someone@bgzbrands.com" --role=roles/bigquery.dataViewer   # if allowlisted, else use the SQL GRANT form
gcloud projects add-iam-policy-binding $PROJECT \
  --member="user:someone@bgzbrands.com" --role=roles/bigquery.jobUser --condition=None
```

## 11. Alerts (outstanding)

Needs the two monitoring roles from step 0, then:

```bash
./setup_alerts.sh you@example.com
```

Creates an email channel plus two policies: any failed `tw-sync` execution, and
no successful run in 26 hours.

## 12. Health checks

```sql
SELECT * FROM `triplewhale-508216.triple_whale_curated.pipeline_status`
ORDER BY days_behind DESC;

SELECT run_id, mode, status, started_at, finished_at,
       (SELECT COUNTIF(s.status = 'failed') FROM UNNEST(sources) s) AS failed
FROM `triplewhale-508216.triple_whale_raw._sync_runs`
ORDER BY started_at DESC LIMIT 10;
```

## Notes from doing this in Cloud Shell

* Cloud Shell keeps `gcloud` credentials in a per session temp dir
  (`$CLOUDSDK_CONFIG`). When the session restarts you are signed out, and a login
  in another tab writes to a *different* dir. Point it at the right one:
  `CLOUDSDK_CONFIG=<path> gcloud auth login <account> --no-launch-browser`.
* IAM changes take up to ~7 minutes to take effect; a 403 right after a grant is
  usually propagation.
* Do not run local writes against a raw table while a job execution is writing
  it; the concurrent transactions collide.
