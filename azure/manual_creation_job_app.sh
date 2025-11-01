RESOURCE_GROUP="HQ-dev"
LOCATION="switzerlandnorth"
JOB_NAME="caj-test-full-historic"
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
    name="$JOB_NAME" \
    location="$LOCATION" \
    containerAppEnvId="$ENV_ID" \
    acrLoginServer=acrimpact.azurecr.io \
    imageName=acrimpact.azurecr.io/klt:latest \
    cronSchedule='' \
    koboServer='https://kobo.impact-initiatives.org/' \
    koboToken='fd289a214002efe39f4ffc888fce6d87ebd71e52' \
    postgresDatabase='dlt_data' \
    postgresUsername='loader' \
    postgresPassword='dlt_load!' \
    postgresHost='atlas-pg-hq-dev.postgres.database.azure.com' \
    kltDestination='postgres' \
    kltDatasetName='kobo_full_historic' \
    kltEarliestModifiedDate='2002-01-01' \
    kltEarliestModifiedDate='2002-01-01' \
    dltStorageName='dlt-storage' \
  --debug 

# Permission ACR
# Récupère le principal ID (même si le job n'est pas complètement déployé)
PRINCIPAL_ID=$(az resource show \
  --resource-group HQ-dev \
  --name $JOB_NAME \
  --resource-type Microsoft.App/jobs \
  --query identity.principalId -o tsv)

echo "Principal ID: $PRINCIPAL_ID"


# Assigne le rôle
ACR_ID=$(az acr show --name acrimpact --query id -o tsv)
az role assignment create \
  --assignee "$PRINCIPAL_ID" \
  --role AcrPull \
  --scope "$ACR_ID"



# Run container manual
az containerapp job start \
  --name $JOB_NAME \
  --resource-group HQ-dev



# # Démarre une session exec interactive dans le job
# az containerapp job exec \
#   --name $JOB_NAME \
#   --resource-group HQ-dev \
#   --command "/bin/bash"

# # Une fois dedans, lance marimo
# marimo edit --host 0.0.0.0 --port 2718