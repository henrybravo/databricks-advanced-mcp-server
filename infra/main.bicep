// ---------------------------------------------------------------------------
// Databricks Advanced MCP — Infrastructure
// Deploys: Azure Databricks workspace (premium SKU for Unity Catalog + SQL Warehouses)
// ---------------------------------------------------------------------------

@description('Azure region for all resources')
param location string = resourceGroup().location

@description('Unique workspace name (3-64 chars)')
@minLength(3)
@maxLength(64)
param workspaceName string

@description('Managed resource group name used by Databricks for compute/storage')
param managedResourceGroupName string = 'rg-${workspaceName}-managed'

@description('Databricks pricing tier — premium required for Unity Catalog and SQL Warehouses')
@allowed([
  'premium'
  'standard'
  'trial'
])
param pricingTier string = 'premium'

@description('Tags applied to all resources')
param tags object = {
  project: 'databricks-advanced-mcp'
  environment: 'dev'
}

// Azure Databricks workspace
resource workspace 'Microsoft.Databricks/workspaces@2024-05-01' = {
  name: workspaceName
  location: location
  tags: tags
  sku: {
    name: pricingTier
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', managedResourceGroupName)
    // Enable public network access for development
    publicNetworkAccess: 'Enabled'
    requiredNsgRules: 'AllRules'
    // Unity Catalog default catalog
    defaultCatalog: {
      initialType: 'UnityCatalog'
    }
  }
}

// ---------------------------------------------------------------------------
// Outputs — consumed by the deployment script to populate .env
// ---------------------------------------------------------------------------

@description('The Databricks workspace URL (DATABRICKS_HOST)')
output workspaceUrl string = 'https://${workspace.properties.workspaceUrl}'

@description('The Databricks workspace resource ID')
output workspaceId string = workspace.id

@description('The managed resource group name')
output managedResourceGroup string = managedResourceGroupName
