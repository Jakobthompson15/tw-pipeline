#!/usr/bin/env bash
# Build, push and deploy the tw-sync Cloud Run job. Safe to re-run.
#   ./deploy.sh v4        # tag to build
# A running execution keeps its image; the new image applies to the next run.
set -euo pipefail

PROJECT=${PROJECT:-bgzdata}
REGION=us-central1
TAG=${1:?usage: ./deploy.sh <tag>}
IMG=$REGION-docker.pkg.dev/$PROJECT/tw-pipeline/tw-sync

cd "$(dirname "$0")"
docker build -t "$IMG:$TAG" .
for i in 1 2 3; do docker push "$IMG:$TAG" && break; sleep 10; done
DIGEST=$(gcloud artifacts docker images describe "$IMG:$TAG" --format='value(image_summary.digest)')

gcloud run jobs deploy tw-sync --project=$PROJECT --region=$REGION \
  --image="$IMG@$DIGEST" \
  --service-account=tw-pipeline@$PROJECT.iam.gserviceaccount.com \
  --cpu=2 --memory=4Gi --task-timeout=24h --max-retries=1 --tasks=1 \
  --set-env-vars=GCP_PROJECT=$PROJECT \
  --labels=client=bgz,pipeline=triple-whale

echo "deployed $IMG@$DIGEST"

# One-time setup already done (kept for reference):
#   gcloud artifacts repositories create tw-pipeline --repository-format=docker --location=$REGION
#   gcloud iam service-accounts create tw-scheduler
#   gcloud projects add-iam-policy-binding $PROJECT --condition=None \
#     --member=serviceAccount:tw-scheduler@$PROJECT.iam.gserviceaccount.com --role=roles/run.invoker
#   (IAM conditions on the job name gave Scheduler 403s.)
#   gcloud scheduler jobs create http tw-sync-daily --location=$REGION --schedule="0 6 * * *" \
#     --time-zone=America/Chicago --http-method=POST \
#     --uri=https://run.googleapis.com/v2/projects/$PROJECT/locations/$REGION/jobs/tw-sync:run \
#     --oauth-service-account-email=tw-scheduler@$PROJECT.iam.gserviceaccount.com
