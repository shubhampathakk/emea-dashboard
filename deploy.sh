#!/bin/bash
set -e

PROJECT_ID="pso-gdc-japac-wedevelop-df"
SERVICE_NAME="emea-dashboard"
REGION="us-central1"

echo "Deploying $SERVICE_NAME to Google Cloud Run in project $PROJECT_ID..."

gcloud run deploy $SERVICE_NAME -q \
  --source . \
  --project $PROJECT_ID \
  --region $REGION \
  --allow-unauthenticated \
  --set-env-vars="GCP_PROJECT_ID=concord-prod"

echo "Deployment initiated."
