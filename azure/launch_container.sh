#!/usr/bin/env bash
# Exécution one-shot avec Azure Container Instances

# Supprimer le container s'il existe déjà
az container delete --resource-group HQ-dev --name klt-kobo-full-hist --yes 2>/dev/null || true

# Récupérer la clé du storage account
STORAGE_KEY=$(az storage account keys list \
  --account-name stkltdev \
  --resource-group HQ-dev \
  --query '[0].value' -o tsv)

az container create \
  --resource-group HQ-dev \
  --name klt-kobo-full-hist \
  --image acrimpact.azurecr.io/klt:latest \
  --registry-login-server acrimpact.azurecr.io \
  --registry-username acrimpact \
  --registry-password $(az acr credential show --name acrimpact --query "passwords[0].value" -o tsv) \
  --os-type Linux \
  --cpu 4 \
  --memory 16 \
  --restart-policy Never \
  --azure-file-volume-account-name stkltdev \
  --azure-file-volume-account-key "$STORAGE_KEY" \
  --azure-file-volume-share-name dlt-pipelines \
  --azure-file-volume-mount-path /app/dlt_pipelines \
  --secure-environment-variables \
    SOURCES__KOBO_SERVER='https://kobo.impact-initiatives.org/' \
    SOURCES__KOBO_TOKEN='fd289a214002efe39f4ffc888fce6d87ebd71e52' \
    DESTINATION__POSTGRES__CREDENTIALS__DATABASE='dlt_data' \
    DESTINATION__POSTGRES__CREDENTIALS__USERNAME='loader' \
    DESTINATION__POSTGRES__CREDENTIALS__PASSWORD='dlt_load!' \
    DESTINATION__POSTGRES__CREDENTIALS__HOST='atlas-pg-hq-dev.postgres.database.azure.com' \
    DESTINATION__POSTGRES__CREDENTIALS__PORT='5432' \
    DESTINATION__POSTGRES__CREDENTIALS__CONNECT_TIMEOUT='15' \
    KLT_DESTINATION=postgres \
    KLT_DATASET_NAME=kobo_full_hist \
    KLT_EARLIEST_MODIFIED_DATE=2003-01-01 \
    KLT_EARLIEST_SUBMISSION_DATE=2003-01-01 \
    DLT_DATA_DIR=/app/dlt_pipelines 

# Voir les logs
az container logs --resource-group HQ-dev --name klt-kobo-full-hist --follow

# Supprimer après test
# az container delete --resource-group HQ-dev --name klt-test --yes


az container show --resource-group HQ-dev --name klt-kobo-full-hist  --query "containers[0].instanceView.currentState" -o table

