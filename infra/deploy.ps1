<#
.SYNOPSIS
    Deploy Azure Databricks workspace + SQL Warehouse, then populate .env

.DESCRIPTION
    1. Creates resource group (if needed)
    2. Deploys the Bicep template (Databricks workspace)
    3. Creates a serverless SQL warehouse via Databricks REST API
    4. Generates a personal access token (PAT)
    5. Writes all values into .env

.PARAMETER ResourceGroupName
    Resource group name. Default: rg-databricks-mcp-dev

.PARAMETER Location
    Azure region. Default: eastus2

.PARAMETER WorkspaceName
    Databricks workspace name. Default: dbw-mcp-dev

.PARAMETER SqlWarehouseName
    Name for the SQL warehouse. Default: mcp-warehouse

.PARAMETER SkipBicep
    Skip the Bicep deployment (use when workspace already exists)
#>
param(
    [string]$ResourceGroupName = "rg-databricks-mcp-dev",
    [string]$Location          = "eastus2",
    [string]$WorkspaceName     = "dbw-mcp-dev",
    [string]$SqlWarehouseName  = "mcp-warehouse",
    [switch]$SkipBicep
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$scriptDir = $PSScriptRoot
$repoRoot  = Split-Path $scriptDir -Parent

Write-Host "`n=== Databricks Advanced MCP — Azure Deployment ===" -ForegroundColor Cyan

# -----------------------------------------------------------------------
# 0. Verify az CLI login
# -----------------------------------------------------------------------
Write-Host "`n[0/5] Checking Azure CLI login..." -ForegroundColor Yellow
try {
    $account = az account show --query "{sub:id, name:name}" -o json 2>$null | ConvertFrom-Json
    Write-Host "  Subscription: $($account.name) ($($account.sub))"
} catch {
    Write-Error "Not logged in. Run 'az login' first."
}

# -----------------------------------------------------------------------
# 1. Create resource group
# -----------------------------------------------------------------------
Write-Host "`n[1/5] Ensuring resource group '$ResourceGroupName' in '$Location'..." -ForegroundColor Yellow
az group create --name $ResourceGroupName --location $Location --output none
Write-Host "  Done."

# -----------------------------------------------------------------------
# 2. Deploy Bicep template
# -----------------------------------------------------------------------
if (-not $SkipBicep) {
    $bicepFile = Join-Path $scriptDir "main.bicep"
    $paramsFile = Join-Path $scriptDir "main.parameters.json"

    Write-Host "`n[2/5] Validating Bicep deployment (what-if)..." -ForegroundColor Yellow
    az deployment group what-if `
        --resource-group $ResourceGroupName `
        --template-file $bicepFile `
        --parameters $paramsFile `
        --parameters workspaceName=$WorkspaceName

    Write-Host "`n[2/5] Deploying Bicep template..." -ForegroundColor Yellow
    $deployment = az deployment group create `
        --resource-group $ResourceGroupName `
        --template-file $bicepFile `
        --parameters $paramsFile `
        --parameters workspaceName=$WorkspaceName `
        --query "properties.outputs" `
        -o json | ConvertFrom-Json

    $workspaceUrl = $deployment.workspaceUrl.value
    Write-Host "  Workspace URL: $workspaceUrl"
} else {
    Write-Host "`n[2/5] Skipping Bicep (--SkipBicep). Looking up existing workspace..." -ForegroundColor Yellow
    $workspaceUrl = az databricks workspace show `
        --resource-group $ResourceGroupName `
        --name $WorkspaceName `
        --query "workspaceUrl" -o tsv
    $workspaceUrl = "https://$workspaceUrl"
    Write-Host "  Workspace URL: $workspaceUrl"
}

# -----------------------------------------------------------------------
# 3. Get AAD token for Databricks API (resource: 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d)
# -----------------------------------------------------------------------
Write-Host "`n[3/5] Acquiring Azure AD token for Databricks API..." -ForegroundColor Yellow
$databricksResourceId = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
$aadToken = az account get-access-token --resource $databricksResourceId --query "accessToken" -o tsv
Write-Host "  Token acquired."

$headers = @{
    "Authorization" = "Bearer $aadToken"
    "Content-Type"  = "application/json"
}

# -----------------------------------------------------------------------
# 4. Create serverless SQL warehouse
# -----------------------------------------------------------------------
Write-Host "`n[4/5] Creating SQL Warehouse '$SqlWarehouseName'..." -ForegroundColor Yellow

# Check if warehouse already exists
$warehousesResponse = Invoke-RestMethod -Uri "$workspaceUrl/api/2.0/sql/warehouses" `
    -Headers $headers -Method Get

$existing = $warehousesResponse.warehouses | Where-Object { $_.name -eq $SqlWarehouseName }

if ($existing) {
    $warehouseId = $existing.id
    Write-Host "  Warehouse already exists: $warehouseId"
} else {
    $warehouseBody = @{
        name                  = $SqlWarehouseName
        cluster_size          = "2X-Small"
        max_num_clusters      = 1
        auto_stop_mins        = 15
        warehouse_type        = "PRO"
        enable_serverless_compute = $true
        tags                  = @{
            custom_tags = @(
                @{ key = "project"; value = "databricks-advanced-mcp" }
            )
        }
    } | ConvertTo-Json -Depth 5

    $createResponse = Invoke-RestMethod -Uri "$workspaceUrl/api/2.0/sql/warehouses" `
        -Headers $headers -Method Post -Body $warehouseBody
    $warehouseId = $createResponse.id
    Write-Host "  Created warehouse: $warehouseId"
}

# -----------------------------------------------------------------------
# 5. Generate a Personal Access Token (PAT)
# -----------------------------------------------------------------------
Write-Host "`n[5/5] Generating Personal Access Token..." -ForegroundColor Yellow

$tokenBody = @{
    comment          = "databricks-advanced-mcp"
    lifetime_seconds = 7776000  # 90 days
} | ConvertTo-Json

$tokenResponse = Invoke-RestMethod -Uri "$workspaceUrl/api/2.0/token/create" `
    -Headers $headers -Method Post -Body $tokenBody
$pat = $tokenResponse.token_value
Write-Host "  PAT created (expires in 90 days)."

# -----------------------------------------------------------------------
# Write .env
# -----------------------------------------------------------------------
Write-Host "`nWriting .env file..." -ForegroundColor Cyan

$envContent = @"
# Databricks Configuration (Required)
DATABRICKS_HOST=$workspaceUrl

# Authentication
DATABRICKS_TOKEN=$pat

# SQL Warehouse (Required for SQL execution tools)
DATABRICKS_WAREHOUSE_ID=$warehouseId

# Optional — defaults for unqualified table names
DATABRICKS_CATALOG=main
DATABRICKS_SCHEMA=default
"@

$envPath = Join-Path $repoRoot ".env"
Set-Content -Path $envPath -Value $envContent -Encoding UTF8
Write-Host "  Written to: $envPath" -ForegroundColor Green

# -----------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------
Write-Host "`n=== Deployment Complete ===" -ForegroundColor Green
Write-Host "  DATABRICKS_HOST         = $workspaceUrl"
Write-Host "  DATABRICKS_WAREHOUSE_ID = $warehouseId"
Write-Host "  DATABRICKS_TOKEN        = $($pat.Substring(0,10))..."
Write-Host "  DATABRICKS_CATALOG      = main"
Write-Host "  DATABRICKS_SCHEMA       = default"
Write-Host ""
Write-Host "  Resource Group: https://portal.azure.com/#@/resource/subscriptions/$($account.sub)/resourceGroups/$ResourceGroupName/overview" -ForegroundColor Blue
Write-Host ""
