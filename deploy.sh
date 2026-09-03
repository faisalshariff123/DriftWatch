#!/usr/bin/env bash
#
# Deploys Driftwatch to Cloud Run: one image, two services.
#
#   ./deploy.sh
#
# Reads DATABASE_URL / REDIS_URL / SERPAPI_KEY from the environment (or .env).
# Every flag that used to be a "remember to pass this" footgun is baked in here.
set -euo pipefail

REGION="${REGION:-us-central1}"
REPO="${REPO:-driftwatch}"
PROJECT="${PROJECT:-$(gcloud config get-value project 2>/dev/null)}"

[ -n "$PROJECT" ] || { echo "No GCP project set. Run: gcloud config set project <id>" >&2; exit 1; }

# Pick up .env if the vars aren't already exported
if [ -f .env ] && [ -z "${DATABASE_URL:-}" ]; then
  set -a; . ./.env; set +a
fi

: "${DATABASE_URL:?DATABASE_URL is not set}"
: "${REDIS_URL:?REDIS_URL is not set}"
: "${SERPAPI_KEY:?SERPAPI_KEY is not set}"

# Supabase's direct host is IPv6-only and Cloud Run is IPv4. This config works
# fine on a laptop and then times out every single query in the cloud, which
# shows up as a dashboard that never loads. Refuse to deploy it.
case "$DATABASE_URL" in
  *pooler.supabase.com*) ;;
  *supabase.co*)
    echo "ERROR: DATABASE_URL points at Supabase's direct host (db.<ref>.supabase.co)." >&2
    echo "       That host is IPv6-only; Cloud Run is IPv4-only, so every query will hang." >&2
    echo "       Use the pooler string from Supabase > Project Settings > Database > Connection pooling:" >&2
    echo "       postgresql://postgres.<ref>:<password>@aws-0-<region>.pooler.supabase.com:6543/postgres?sslmode=require" >&2
    exit 1 ;;
esac

IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/driftwatch:$(date +%Y%m%d-%H%M%S)"

ENVFILE="$(mktemp)"
trap 'rm -f "$ENVFILE"' EXIT
# A YAML file avoids gcloud's comma/delimiter parsing entirely - connection
# strings are full of commas, @ and & and will otherwise be mangled.
{
  printf 'DATABASE_URL: %s\n' "\"$DATABASE_URL\""
  printf 'REDIS_URL: %s\n'    "\"$REDIS_URL\""
  printf 'SERPAPI_KEY: %s\n'  "\"$SERPAPI_KEY\""
} > "$ENVFILE"

echo "Enabling required GCP APIs (safe to re-run, no-ops if already on) ..."
gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com

gcloud artifacts repositories describe "$REPO" --location="$REGION" >/dev/null 2>&1 || \
  gcloud artifacts repositories create "$REPO" --repository-format=docker --location="$REGION"

echo "Building $IMAGE ..."
gcloud builds submit --tag "$IMAGE"

echo "Deploying API ..."
# max-instances is no longer pinned to 1: the rate limiter lives in Redis now,
# so instances share one budget instead of each enforcing its own.
gcloud run deploy driftwatch-api \
  --image "$IMAGE" \
  --region "$REGION" \
  --env-vars-file "$ENVFILE" \
  --cpu 2 --memory 2Gi \
  --timeout 300 \
  --min-instances 0 --max-instances 4 \
  --allow-unauthenticated

echo "Deploying worker ..."
# --no-cpu-throttling: Cloud Run parks CPU between requests by default, which
#   stalls the RQ loop mid-job since it isn't serving a request.
# --min-instances 1: nothing sends this service HTTP traffic, so without a
#   floor it scales to zero and the queue is never drained.
# --max-instances 1: one consumer is plenty and keeps job ordering sane.
gcloud run deploy driftwatch-worker \
  --image "$IMAGE" \
  --region "$REGION" \
  --command python --args="-m,app.worker" \
  --env-vars-file "$ENVFILE" \
  --cpu 2 --memory 2Gi \
  --no-cpu-throttling \
  --min-instances 1 --max-instances 1 \
  --ingress internal \
  --no-allow-unauthenticated

URL="$(gcloud run services describe driftwatch-api --region "$REGION" --format='value(status.url)')"
echo
echo "API:  $URL"
echo "Check: curl -s $URL/health"
