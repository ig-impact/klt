#!/usr/bin/env bash

# Authentification ACR
az acr login --name acrimpact

# Build & push Image with ACR tag
docker buildx build --platform linux/amd64 -t acrimpact.azurecr.io/klt:latest --push .

# Push to Azure ACR
# docker push acrimpact.azurecr.io/klt:latest

# Verification
az acr repository list --name acrimpact
az acr repository show-tags --name acrimpact --repository klt


# Delete image
# az acr repository delete --name acrimpact --repository klt --yes
