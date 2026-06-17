# Power BI Row-Level Security (RLS) Troubleshooting & Fix Guide

## 1. The Problem

When attempting to generate a Power BI Embed Token for a specific user identity, the Power BI REST API returns a `403 Forbidden` error:

```json
{
  "error": {
    "code": "InvalidRequest",
    "message": "Creating embed token with effective identity is not supported for this datasource"
  }
}
```

This error prevents the web application from applying Row-Level Security (RLS), meaning we cannot filter the embedded dashboard for specific `customer` or `agent` identities.

## 2. Root Causes

There are two primary reasons for this error occurring simultaneously:

### A. Missing RLS Roles in the Semantic Model
The Node.js backend attempts to request an embed token for a specific role (e.g., `ROLE_CUSTOMER` or `ROLE_AGENT`). However, inside the Power BI Desktop (`.pbix`) file, these Security Roles were not defined. Power BI rejects the `identities` payload if the specified role does not exist in the dataset.

### B. Single Sign-On (SSO) Enabled on the Cloud Connection
In the Microsoft Fabric portal, the Semantic Model was configured to use a connection with **"Default Single Sign-On (Entra ID)"** enabled. 
When generating an Embed Token using a Service Principal, Power BI cannot pass the effective identity (the custom username/role we provide) through an SSO connection, as SSO expects a real Entra ID (Azure AD) user context.

---

## 3. Step-by-Step Resolution

To successfully implement RLS with a Service Principal and Fabric SQL Endpoint, follow these steps:

### Step 1: Define Roles in Power BI Desktop
1. Open the `.pbix` report file in Power BI Desktop.
2. Navigate to **Modeling** > **Manage Roles**.
3. Create the necessary roles (e.g., `ROLE_CUSTOMER`, `ROLE_AGENT`).
4. Assign the appropriate DAX filters to the relevant tables. 
   - Example for Customer: `[customer_id] = USERNAME()`
   - Example for Agent: `[agent_id] = USERNAME()`
   *(Note: If you want a role to see all data without filtering, set the DAX filter to `TRUE()`)*.
5. Save the file and **Publish** it to the Fabric Workspace.

### Step 2: Create a Non-SSO Cloud Connection in Fabric
1. Go to the Microsoft Fabric Portal and navigate to **Manage Connections and Gateways**.
2. Click **New connection** > **Cloud**.
3. Set the Connection Type to **SQL Server**.
4. Enter the SQL Analytics Endpoint server and database name.
5. Set the Authentication method to **OAuth2** and sign in with an authorized account.
6. ❌ **CRITICAL:** Ensure that the option **"Use SSO via Azure AD for DirectQuery queries" is UNCHECKED**.
7. Click **Create**.

### Step 3: Map the Semantic Model to the New Connection
1. In the Fabric Workspace, find the Semantic Model (Dataset) and open its **Settings**.
2. Expand **Gateway and cloud connections**.
3. In the dropdown for the data source, select the newly created connection (the one without SSO) instead of the default SSO connection.
4. Click **Apply**.

### Step 4: Update Application Configuration
Whenever a report is republished or duplicated to apply these fixes, its IDs might change. 
1. Extract the new `Report ID` and `Dataset (Semantic Model) ID` from the Fabric portal URLs.
2. Update the `.env.local` file in the Next.js application with the new IDs.
3. Update the `route.ts` API logic to route users to the appropriate Dashboard IDs based on their role.

---

## 4. Verification & Testing

Once the fixes are applied, the authentication and embedding flow works as follows:

1. **SQL Login Verification:** 
   When a user logs in (e.g., `CUS0001` or `AG001`), the `api/login` route queries the Fabric SQL Endpoint (`dim_customer` or `dim_agent`) to verify the ID exists. If the SQL query fails (e.g., database timeout), it gracefully falls back to checking the ID prefix.
2. **Token Generation:** 
   The frontend requests an embed token. The `api/getEmbedToken` route securely passes the `username` and `role` to the Power BI REST API.
3. **RLS Application:** 
   Because the Semantic Model has the roles defined and uses a non-SSO connection, the API successfully returns an Embed Token. The Power BI iframe renders the dashboard, and the DAX rules automatically filter the data to only show records matching the logged-in user's ID.
