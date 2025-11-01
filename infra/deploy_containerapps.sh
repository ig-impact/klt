#!/usr/bin/env bash
set -e

RESOURCE_GROUP="HQ-dev"
LOCATION="switzerlandnorth"

# Deploy Container Apps Environment
az deployment group create \
  --resource-group "$RESOURCE_GROUP" \
  --template-file infra/modules/containerapps-env.bicep \
  --parameters name=cae-klt-dev location="$LOCATION"

# Get Environment ID
ENV_ID=$(az deployment group show \
  --resource-group "$RESOURCE_GROUP" \
  --name containerapps-env \
  --query properties.outputs.id.value -o tsv)

# Deploy Container Apps Job
DEPLOYMENT_NAME="deploy-job-$(date +%s)"

az deployment group create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$DEPLOYMENT_NAME" \
  --template-file infra/modules/containerapps-job.bicep \
  --parameters \
    name=caj-klt-dev-5 \
    location="$LOCATION" \
    containerAppEnvId="$ENV_ID" \
    acrLoginServer=acrimpact.azurecr.io \
    imageName=acrimpact.azurecr.io/klt:latest \
    cronSchedule='30 10 * * *' \
    koboServer='https://kobo.impact-initiatives.org/' \
    koboToken='fd289a214002efe39f4ffc888fce6d87ebd71e52' \
    postgresDatabase='dlt_data' \
    postgresUsername='loader' \
    postgresPassword='dlt_load!' \
    postgresHost='atlas-pg-hq-dev.postgres.database.azure.com' \
    kltDestination='postgres' \
    kltDatasetName='azure_schedule_job_test' \
  --debug 
# --no-wait 



# Assign AcrPull role
echo "🔑 Assigning AcrPull role..."
PRINCIPAL_ID=$(az containerapp job show \
  --name caj-klt-dev-4  \
  --resource-group "$RESOURCE_GROUP" \
  --query identity.principalId -o tsv)

ACR_ID=$(az acr show --name acrimpact --query id -o tsv)

az role assignment create \
  --assignee "$PRINCIPAL_ID" \
  --role AcrPull \
  --scope "$ACR_ID"

echo "✅ Deployment complete"
