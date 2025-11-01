#!/usr/bin/env bash
set -e

RESOURCE_GROUP="HQ-dev"
LOCATION="switzerlandnorth"

MODULE="${1:-main}"

if [ "$MODULE" = "main" ]; then
  TEMPLATE="infra/main.bicep"
  PARAMS="--parameters infra/parameters.json"
else
  TEMPLATE="infra/modules/${MODULE}.bicep"
  
  # Extract params from parameters.json based on module
  case "$MODULE" in
    acr)
      ACR_NAME=$(jq -r '.parameters.acrName.value' infra/parameters.json)
      ACR_SKU=$(jq -r '.parameters.acrSku.value' infra/parameters.json)
      PARAMS="--parameters name=$ACR_NAME sku=$ACR_SKU location=$LOCATION tags={}"
      ;;
    keyvault)
      KV_NAME=$(jq -r '.parameters.keyVaultName.value' infra/parameters.json)
      KV_SKU=$(jq -r '.parameters.keyVaultSku.value' infra/parameters.json)
      PARAMS="--parameters name=$KV_NAME sku=$KV_SKU location=$LOCATION"
      ;;
    *)
      echo "Module inconnu: $MODULE"
      exit 1
      ;;
  esac
fi

az deployment group create \
  --resource-group "$RESOURCE_GROUP" \
  --template-file "$TEMPLATE" \
  $PARAMS



