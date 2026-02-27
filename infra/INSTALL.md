# Infrastructure Installation Guide

Detailed manual for deploying the Azure Databricks workspace and all supporting resources needed by the **Databricks Advanced MCP Server**.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Architecture Overview](#architecture-overview)
3. [File Inventory](#file-inventory)
4. [Option A — Automated Deployment (deploy.ps1)](#option-a--automated-deployment-deployps1)
5. [Option B — Manual Step-by-Step Deployment](#option-b--manual-step-by-step-deployment)
   - [Step 1: Azure Login](#step-1-azure-login)
   - [Step 2: Create Resource Group](#step-2-create-resource-group)
   - [Step 3: Deploy Bicep Template](#step-3-deploy-bicep-template)
   - [Step 4: Create SQL Warehouse](#step-4-create-sql-warehouse)
   - [Step 5: Generate Personal Access Token](#step-5-generate-personal-access-token)
   - [Step 6: Populate .env](#step-6-populate-env)
6. [Bicep Template Reference](#bicep-template-reference)
7. [Parameters Reference](#parameters-reference)
8. [Post-Deployment Verification](#post-deployment-verification)
9. [Teardown](#teardown)
10. [Troubleshooting](#troubleshooting)

---

## Prerequisites

| Requirement | Minimum Version | Notes |
|---|---|---|
| **Azure CLI** | 2.60+ | Install: https://aka.ms/installazurecli |
| **Bicep CLI** | 0.25+ | Bundled with Azure CLI 2.60+; verify with `az bicep version` |
| **PowerShell** | 7.0+ | Required for `deploy.ps1`; `pwsh --version` to check |
| **Azure subscription** | — | With **Contributor** role on the target resource group scope |
| **Python** | 3.11+ | For running the MCP server after deployment |
| **uv** | 0.4+ | Python package manager; install: https://docs.astral.sh/uv/ |

> **Tip:** If you only have Reader access on the subscription, ask your admin to pre-create the resource group and grant you Contributor on that resource group.

---

## Architecture Overview

The deployment creates the following Azure resources:

```
┌─────────────────────────────────────────────────────────────┐
│  Resource Group: rg-databricks-mcp-dev                      │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  Azure Databricks Workspace (Premium SKU)             │  │
│  │  • Unity Catalog enabled (default catalog)            │  │
│  │  • Public network access (dev)                        │  │
│  │  • Serverless SQL Warehouse (auto-provisioned)        │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  Managed Resource Group: rg-dbw-mcp-dev-managed       │  │
│  │  (Databricks-managed compute, storage, networking)    │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

**What the MCP server needs from this deployment:**

| `.env` Variable | Source |
|---|---|
| `DATABRICKS_HOST` | Bicep output `workspaceUrl` |
| `DATABRICKS_TOKEN` | PAT generated via Databricks REST API |
| `DATABRICKS_WAREHOUSE_ID` | SQL Warehouse created via Databricks REST API |
| `DATABRICKS_CATALOG` | `main` (default) |
| `DATABRICKS_SCHEMA` | `default` (default) |

---

## File Inventory

```
infra/
├── main.bicep              # Bicep template — Databricks workspace
├── main.parameters.json    # Default parameter values
├── deploy.ps1              # One-click deployment script (PowerShell)
└── INSTALL.md              # This file
```

---

## Option A — Automated Deployment (deploy.ps1)

The `deploy.ps1` script performs all 5 steps automatically and writes the `.env` file.

### Usage

```powershell
# 1. Login to Azure
az login

# 2. Set your target subscription
az account set --subscription "<your-subscription-name-or-id>"

# 3. Run the deployment (from the repo root)
.\infra\deploy.ps1
```

### Custom Parameters

```powershell
.\infra\deploy.ps1 `
    -ResourceGroupName "rg-my-databricks" `
    -Location "westus2" `
    -WorkspaceName "dbw-my-project" `
    -SqlWarehouseName "my-warehouse"
```

### Skip Bicep (workspace already exists)

If the workspace was deployed separately and you only need the SQL warehouse + PAT + `.env`:

```powershell
.\infra\deploy.ps1 -SkipBicep `
    -ResourceGroupName "rg-my-databricks" `
    -WorkspaceName "dbw-my-project"
```

### What the Script Does

| Step | Action | API |
|---|---|---|
| 0 | Verify `az login` | Azure CLI |
| 1 | Create resource group | ARM |
| 2 | Deploy Bicep template (with `what-if` validation) | ARM |
| 3 | Acquire Azure AD token for Databricks | Azure CLI |
| 4 | Create serverless SQL warehouse (or reuse existing) | Databricks REST API |
| 5 | Generate 90-day PAT | Databricks REST API |
| — | Write `.env` to repo root | Local filesystem |

---

## Option B — Manual Step-by-Step Deployment

Use this approach if you prefer full control, don't have PowerShell 7, or need to adapt for CI/CD.

### Step 1: Azure Login

```bash
# Interactive login
az login

# Set the target subscription
az account set --subscription "<subscription-name-or-id>"

# Verify
az account show --query "{name:name, id:id}" -o table
```

### Step 2: Create Resource Group

```bash
az group create \
    --name rg-databricks-mcp-dev \
    --location eastus2
```

### Step 3: Deploy Bicep Template

**Validate first** with a what-if preview:

```bash
az deployment group what-if \
    --resource-group rg-databricks-mcp-dev \
    --template-file infra/main.bicep \
    --parameters infra/main.parameters.json \
    --parameters workspaceName=dbw-mcp-dev
```

Review the output, then **deploy**:

```bash
az deployment group create \
    --resource-group rg-databricks-mcp-dev \
    --template-file infra/main.bicep \
    --parameters infra/main.parameters.json \
    --parameters workspaceName=dbw-mcp-dev \
    --query "properties.outputs" \
    -o json
```

Save the `workspaceUrl` from the output — you'll need it for the next steps.

Example output:
```json
{
  "workspaceUrl": {
    "value": "https://adb-1234567890123456.7.azuredatabricks.net"
  }
}
```

> **Note:** Deployment typically takes 3–5 minutes. The Databricks workspace provisions a managed resource group (`rg-dbw-mcp-dev-managed`) automatically.

### Step 4: Create SQL Warehouse

Acquire an Azure AD token for the Databricks resource:

```bash
# Get AAD token (Databricks resource ID: 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d)
TOKEN=$(az account get-access-token \
    --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d \
    --query accessToken -o tsv)
```

Check if a warehouse already exists:

```bash
WORKSPACE_URL="https://adb-1234567890123456.7.azuredatabricks.net"

curl -s -X GET "$WORKSPACE_URL/api/2.0/sql/warehouses" \
    -H "Authorization: Bearer $TOKEN" | jq '.warehouses[] | {id, name, state}'
```

If no suitable warehouse exists, create one:

```bash
curl -s -X POST "$WORKSPACE_URL/api/2.0/sql/warehouses" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{
        "name": "mcp-warehouse",
        "cluster_size": "2X-Small",
        "max_num_clusters": 1,
        "auto_stop_mins": 15,
        "warehouse_type": "PRO",
        "enable_serverless_compute": true
    }' | jq '.id'
```

Save the warehouse `id` value.

> **Note:** Azure Databricks often auto-provisions a "Serverless Starter Warehouse" when the workspace is first created. You can reuse that warehouse instead of creating a new one.

### Step 5: Generate Personal Access Token

```bash
curl -s -X POST "$WORKSPACE_URL/api/2.0/token/create" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{
        "comment": "databricks-advanced-mcp",
        "lifetime_seconds": 7776000
    }' | jq '.token_value'
```

> **Security:** The PAT is shown only once. Store it securely. The 7776000-second lifetime equals 90 days. Adjust as needed.

### Step 6: Populate .env

Copy the template and fill in the values collected from previous steps:

```bash
cp .env.example .env
```

Edit `.env`:

```dotenv
# Databricks Configuration (Required)
DATABRICKS_HOST=https://adb-1234567890123456.7.azuredatabricks.net

# Authentication
DATABRICKS_TOKEN=dapi_your_token_here

# SQL Warehouse (Required for SQL execution tools)
DATABRICKS_WAREHOUSE_ID=abc123def456

# Optional — defaults for unqualified table names
DATABRICKS_CATALOG=main
DATABRICKS_SCHEMA=default
```

---

## Bicep Template Reference

### main.bicep

Deploys a single `Microsoft.Databricks/workspaces@2024-05-01` resource.

| Property | Value | Notes |
|---|---|---|
| SKU | `premium` | Required for Unity Catalog and SQL Warehouses |
| `publicNetworkAccess` | `Enabled` | For dev; restrict in production |
| `requiredNsgRules` | `AllRules` | Standard NSG rules for Databricks |
| `defaultCatalog.initialType` | `UnityCatalog` | Auto-provisions Unity Catalog's `main` catalog |
| Managed RG | `rg-<workspaceName>-managed` | Databricks-managed VMs, storage, networking |

### Outputs

| Output | Description |
|---|---|
| `workspaceUrl` | Full `https://` URL for `DATABRICKS_HOST` |
| `workspaceId` | ARM resource ID |
| `managedResourceGroup` | Name of the Databricks-managed resource group |

---

## Parameters Reference

### main.parameters.json

| Parameter | Default | Description |
|---|---|---|
| `workspaceName` | `dbw-mcp-dev` | Workspace name (3–64 chars, must be unique per region) |
| `pricingTier` | `premium` | `premium` / `standard` / `trial` |
| `tags` | `{project, environment}` | Azure resource tags |

### deploy.ps1

| Parameter | Default | Description |
|---|---|---|
| `-ResourceGroupName` | `rg-databricks-mcp-dev` | Target resource group |
| `-Location` | `eastus2` | Azure region |
| `-WorkspaceName` | `dbw-mcp-dev` | Databricks workspace name |
| `-SqlWarehouseName` | `mcp-warehouse` | SQL warehouse name to create |
| `-SkipBicep` | `$false` | Skip Bicep deployment (workspace exists) |

---

## Post-Deployment Verification

### 1. Verify the .env file

```bash
# Check .env is populated (token masked)
cat .env | sed 's/\(TOKEN=.\{15\}\).*/\1.../'
```

### 2. Run the test suite

```bash
uv venv .venv
uv pip install -e ".[dev]"
uv run pytest tests/ -v --tb=short
```

All 74 tests should pass (they use mocked Databricks clients and don't need a live workspace).

### 3. Smoke-test the MCP server

```bash
# Start the server
uv run databricks-mcp
```

The server starts on stdio. Connect via VS Code MCP integration or any MCP client.

### 4. Verify workspace connectivity

Open the workspace in your browser using the `DATABRICKS_HOST` URL and confirm:
- Unity Catalog `main` catalog exists
- The SQL warehouse is running (or can be started)
- You can execute a simple query: `SELECT 1`

---

## Teardown

To remove all deployed resources:

```bash
# Delete the resource group (includes the workspace and its managed RG)
az group delete --name rg-databricks-mcp-dev --yes --no-wait

# The managed resource group (rg-dbw-mcp-dev-managed) is deleted automatically
```

> **Warning:** This permanently deletes the Databricks workspace, all notebooks, jobs, and data stored in the managed resource group. This cannot be undone.

To clean up only local config:

```bash
# Reset .env to template
cp .env.example .env
```

---

## Troubleshooting

### `AuthorizationFailed` when creating the resource group

```
The client does not have authorization to perform action
'Microsoft.Resources/subscriptions/resourcegroups/write'
```

**Fix:** You need **Contributor** role on the subscription or the resource group. Either:
- Switch to a subscription where you have write access: `az account set --subscription "<name>"`
- Ask your admin to pre-create the RG and grant you Contributor on it

### Bicep deployment fails with `InvalidTemplateDeployment`

**Fix:** Check the inner error. Common causes:
- Workspace name already taken (must be globally unique within the region)
- Invalid region — not all regions support Databricks. Check [supported regions](https://learn.microsoft.com/en-us/azure/databricks/resources/supported-regions)
- Quota exceeded — request an increase via the Azure portal

### `403 Forbidden` when calling Databricks REST API

**Fix:** The Azure AD token may have expired (1-hour lifetime). Re-acquire:

```bash
TOKEN=$(az account get-access-token \
    --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d \
    --query accessToken -o tsv)
```

### PAT token not working

**Fix:**
- Ensure `DATABRICKS_HOST` does **not** have a trailing slash
- Confirm the PAT hasn't expired (default: 90 days)
- Regenerate if needed via the workspace UI: **Settings → Developer → Access Tokens**

### SQL warehouse not starting

**Fix:**
- Check the workspace pricing tier is **Premium** (required for SQL warehouses)
- Verify your subscription has enough DBU quota
- Try restarting: **SQL Warehouses → (select warehouse) → Start**

### `deploy.ps1` fails at "Acquiring Azure AD token"

**Fix:** Ensure you're authenticated with an identity that has access to the Databricks workspace:
```powershell
az login
az account set --subscription "<subscription>"
```

### Can't find the SQL Warehouse ID

Navigate to the Databricks workspace UI → **SQL Warehouses** → click your warehouse → the ID is in the URL:
```
https://adb-xxx.azuredatabricks.net/sql/warehouses/<WAREHOUSE_ID>
```

Or via API:
```bash
curl -s "$WORKSPACE_URL/api/2.0/sql/warehouses" \
    -H "Authorization: Bearer $TOKEN" | jq '.warehouses[] | {id, name}'
```
