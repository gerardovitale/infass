#!/bin/bash
set -euo pipefail

API_URL="$(gcloud run services describe infass-api-service --region=europe-southwest1 --format='value(status.url)')/products/search"
AUTH_TOKEN="$(gcloud auth print-identity-token)"
SEARCH_TERM="${1:-cerveza}"

curl -sS \
  -H "Authorization: Bearer ${AUTH_TOKEN}" \
  -H "Content-Type: application/json" \
  "${API_URL}?search_term=${SEARCH_TERM}" \
  | jq '.'
