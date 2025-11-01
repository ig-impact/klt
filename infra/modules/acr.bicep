// =============================================================================
// Azure Container Registry Module
// =============================================================================
// Creates a private container registry for hosting Docker images

@description('The name of the container registry (alphanumeric only)')
param name string

@description('The location/region for the registry')
param location string

@description('The SKU of the container registry')
@allowed([
  'Basic'
  'Standard'
  'Premium'
])
param sku string = 'Basic'

@description('Tags to apply to the registry')
param tags object = {}

// Create the Container Registry
resource acr 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: name
  location: location
  tags: tags
  sku: {
    name: sku
  }
  properties: {
    adminUserEnabled: false
  }
}

// Outputs
@description('The resource ID of the container registry')
output id string = acr.id

@description('The login server URL')
output loginServer string = acr.properties.loginServer

@description('The name of the container registry')
output name string = acr.name
