#!/usr/bin/env bash
set -e

RESOURCE_GROUP="HQ-dev"
LOCATION="switzerlandnorth"
STORAGE_ACCOUNT="stkltdev"
FILE_SHARE="dlt-pipelines"

# 1. Créer le Storage Account
az storage account create \
  --name "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --sku Standard_LRS \
  --kind StorageV2

# 2. Récupérer la clé
STORAGE_KEY=$(az storage account keys list \
  --account-name "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --query '[0].value' -o tsv)

# 3. Créer le File Share
az storage share create \
  --name "$FILE_SHARE" \
  --account-name "$STORAGE_ACCOUNT" \
  --account-key "$STORAGE_KEY" \
  --quota 100

echo "✅ Storage Account: $STORAGE_ACCOUNT"
echo "✅ File Share: $FILE_SHARE"
echo "🔑 Storage Key: $STORAGE_KEY"
echo ""
echo "Ajoute ce storage au Container Apps Environment:"
echo "az containerapp env storage set \\"
echo "  --name cae-klt-dev \\"
echo "  --resource-group $RESOURCE_GROUP \\"
echo "  --storage-name dlt-storage \\"
echo "  --azure-file-account-name $STORAGE_ACCOUNT \\"
echo "  --azure-file-account-key \"$STORAGE_KEY\" \\"
echo "  --azure-file-share-name $FILE_SHARE \\"
echo "  --access-mode ReadWrite"
