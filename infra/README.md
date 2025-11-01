# KLT Infrastructure as Code (Bicep)

This directory contains Azure infrastructure definitions using **Bicep** (Azure's native IaC language).

## 📁 Current Structure (Step 1: ACR Only)

```
infra/
├── README.md              # This file
├── main.bicep            # Main deployment (currently: Resource Group + ACR)
├── parameters.json       # Environment-specific parameters
├── deploy.sh            # Helper script for deployment
└── modules/
    └── acr.bicep        # Azure Container Registry module
```

## 🚀 Prerequisites

1. **Azure CLI** with Bicep support
   ```bash
   az --version
   az bicep version
   ```

2. **Logged in to Azure**
   ```bash
   az login
   az account set --subscription "<subscription-name-or-id>"
   ```

## 📝 Configuration

Edit `parameters.json` to customize:
- `environment`: dev or prod
- `acrSku`: Basic, Standard, or Premium

**Note**: This deployment uses existing Resource Group `HQ-dev`. Location is inherited from the RG.

## 🎯 Deployment

### Preview changes (what-if)
```bash
./deploy.sh --what-if
```

### Deploy to dev
```bash
./deploy.sh
```

### Deploy to prod
```bash
./deploy.sh --environment prod
```

## � What Gets Deployed

| Resource Type | Name Pattern | Purpose |
|--------------|--------------|---------|
| Resource Group | `HQ-dev` | Existing shared resource group |
| Container Registry | `acrklt{env}` | Private Docker image registry (e.g., acrkltdev, acrkltprod) |

## 🔍 Validate Before Deployment

```bash
# Validate Bicep syntax
az bicep build --file main.bicep

# What-if analysis
az deployment sub what-if \
  --location switzerlandnorth \
  --template-file main.bicep \
  --parameters parameters.json
```

## 📚 Next Steps

After ACR is deployed:
1. Build and push Docker image to ACR
2. Add Key Vault module
3. Add Container Apps Job module
4. Configure CI/CD with GitHub Actions
