param name string
param location string
param containerAppEnvId string
param acrLoginServer string
param imageName string
param cronSchedule string = ''
@secure()
param koboServer string
@secure()
param koboToken string
@secure()
param postgresDatabase string
@secure()
param postgresUsername string
@secure()
param postgresPassword string
@secure()
param postgresHost string
param postgresPort string = '5432'
param postgresConnectTimeout string = '15'
param kltDestination string = 'postgres'
param kltDatasetName string
param kltEarliestModifiedDate string = '2025-10-20'
param kltEarliestSubmissionDate string = '2025-10-20'
param dltStorageName string = ''  // Nom du storage configuré dans l'environment


resource containerAppJob 'Microsoft.App/jobs@2023-05-01' = {
  name: name
  location: location
  properties: {
    environmentId: containerAppEnvId
    configuration: {
      triggerType: empty(cronSchedule) ? 'Manual' : 'Schedule'
      scheduleTriggerConfig: empty(cronSchedule) ? null : {
        cronExpression: cronSchedule
        parallelism: 1
        replicaCompletionCount: 1
      }
      replicaTimeout: 36000  // 10h (limite max Azure Container Apps)
      replicaRetryLimit: 3
      registries: [
        {
          server: acrLoginServer
          identity: 'system'
        }
      ]
      secrets: [
        {
          name: 'kobo-server'
          value: koboServer
        }
        {
          name: 'kobo-token'
          value: koboToken
        }
        {
          name: 'postgres-database'
          value: postgresDatabase
        }
        {
          name: 'postgres-username'
          value: postgresUsername
        }
        {
          name: 'postgres-password'
          value: postgresPassword
        }
        {
          name: 'postgres-host'
          value: postgresHost
        }
      ]
    }
    template: {
      volumes: empty(dltStorageName) ? [] : [
        {
          name: 'dlt-data'
          storageName: dltStorageName
          storageType: 'AzureFile'
        }
      ]
      containers: [
        {
          name: 'klt'
          image: imageName
          resources: {
            cpu: json('4.0')
            memory: '8Gi'
          }
          volumeMounts: empty(dltStorageName) ? [] : [
            {
              volumeName: 'dlt-data'
              mountPath: '/app/dlt_pipelines'
            }
          ]
          env: [
            {
              name: 'SOURCES__KOBO_SERVER'
              secretRef: 'kobo-server'
            }
            {
              name: 'SOURCES__KOBO_TOKEN'
              secretRef: 'kobo-token'
            }
            {
              name: 'DESTINATION__POSTGRES__CREDENTIALS__DATABASE'
              secretRef: 'postgres-database'
            }
            {
              name: 'DESTINATION__POSTGRES__CREDENTIALS__USERNAME'
              secretRef: 'postgres-username'
            }
            {
              name: 'DESTINATION__POSTGRES__CREDENTIALS__PASSWORD'
              secretRef: 'postgres-password'
            }
            {
              name: 'DESTINATION__POSTGRES__CREDENTIALS__HOST'
              secretRef: 'postgres-host'
            }
            {
              name: 'DESTINATION__POSTGRES__CREDENTIALS__PORT'
              value: postgresPort
            }
            {
              name: 'DESTINATION__POSTGRES__CREDENTIALS__CONNECT_TIMEOUT'
              value: postgresConnectTimeout
            }
            {
              name: 'KLT_DESTINATION'
              value: kltDestination
            }
            {
              name: 'KLT_DATASET_NAME'
              value: kltDatasetName
            }
            {
              name: 'KLT_EARLIEST_MODIFIED_DATE'
              value: kltEarliestModifiedDate
            }
            {
              name: 'KLT_EARLIEST_SUBMISSION_DATE'
              value: kltEarliestSubmissionDate
            }
          ]
        }
      ]
    }
  }
  identity: {
    type: 'SystemAssigned'
  }
}

output id string = containerAppJob.id
output name string = containerAppJob.name
output principalId string = containerAppJob.identity.principalId
