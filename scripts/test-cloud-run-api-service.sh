#!/bin/bash
set -euo pipefail

show_help() {
  echo "Usage: $0 [search|product] [SEARCH_TERM|PRODUCT_ID]"
  echo ""
  echo "  search [SEARCH_TERM]   - Search products (default: cerveza)"
  echo "  product [PRODUCT_ID]   - Get product details (default: 3a8df4213a646a25f3861e0b65a68a1f)"
  echo ""
  echo "Examples:"
  echo "  $0 search cerveza"
  echo "  $0 product 3a8df4213a646a25f3861e0b65a68a1f"
}

API_BASE_URL="$(gcloud run services describe infass-api-service --region=europe-southwest1 --format='value(status.url)')"
AUTH_TOKEN="$(gcloud auth print-identity-token)"

ENDPOINT="${1:-search}"
ARG="${2:-}"

case "$ENDPOINT" in
  search)
    SEARCH_TERM="${ARG:-cerveza}"
    curl -sS \
      -H "Authorization: Bearer ${AUTH_TOKEN}" \
      -H "Content-Type: application/json" \
      "${API_BASE_URL}/products/search?search_term=${SEARCH_TERM}" | jq '.'
    ;;

  product)
    PRODUCT_ID="${ARG:-3a8df4213a646a25f3861e0b65a68a1f}"
    curl -sS \
      -H "Authorization: Bearer ${AUTH_TOKEN}" \
      -H "Content-Type: application/json" \
      "${API_BASE_URL}/products/${PRODUCT_ID}" | jq '.'
    ;;

  *)
    show_help
    exit 1
    ;;
esac
