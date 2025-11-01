targetScope = 'resourceGroup'

param acrName string
param acrSku string
param keyVaultName string
param keyVaultSku string
param containerAppEnvName string
param containerAppJobName string
param cronSchedule string
param imageName string

module acr 'modules/acr.bicep' = {
  name: 'acr'
  params: {
    name: acrName
    location: resourceGroup().location
    sku: acrSku
    tags: {}
  }
}

// module keyVault 'modules/keyvault.bicep' = {
//   name: 'keyvault'
//   params: {
//     name: keyVaultName
//     location: resourceGroup().location
//     sku: keyVaultSku
//   }
// }

module containerAppEnv 'modules/containerapps-env.bicep' = {
  name: 'containerapp-env'
  params: {
    name: containerAppEnvName
    location: resourceGroup().location
  }
}

module containerAppJob 'modules/containerapps-job.bicep' = {
  name: 'containerapp-job'
  params: {
    name: containerAppJobName
    location: resourceGroup().location
    containerAppEnvId: containerAppEnv.outputs.id
    acrLoginServer: acr.outputs.loginServer
    imageName: imageName
    cronSchedule: cronSchedule
  }
}

output acrLoginServer string = acr.outputs.loginServer
output acrName string = acr.outputs.name
// output keyVaultName string = keyVault.outputs.name
// output keyVaultUri string = keyVault.outputs.vaultUri
output containerAppJobName string = containerAppJob.outputs.name
output containerAppJobPrincipalId string = containerAppJob.outputs.principalId
