#!/usr/bin/env bash


# 1. Créer le workspace
az monitor log-analytics workspace create \
  --resource-group HQ-dev \
  --workspace-name law-klt-dev \
  --location switzerlandnorth

# 2. Récupérer l'ID et la clé
WORKSPACE_ID=$(az monitor log-analytics workspace show \
  --resource-group HQ-dev \
  --workspace-name law-klt-dev \
  --query customerId -o tsv)

WORKSPACE_KEY=$(az monitor log-analytics workspace get-shared-keys \
  --resource-group HQ-dev \
  --workspace-name law-klt-dev \
  --query primarySharedKey -o tsv)

# 3. Mettre à jour l'environment
az containerapp env update \
  --name cae-klt-dev \
  --resource-group HQ-dev \
  --logs-destination log-analytics \
  --logs-workspace-id "$WORKSPACE_ID" \
  --logs-workspace-key "$WORKSPACE_KEY"