# CarPro Insurance Web App - Auth Blocker Status

## What this folder is about
This directory contains the Next.js frontend application for the CarPro Insurance project. It is designed to handle user routing and serve as the secure entry point to surface embedded Power BI analytics for our users (Customers and Agents).

## What it currently has
The core Minimum Viable Product (MVP) logic is fully implemented:
- **Identity Detection:** The login interface enforces an 8-character ID limit.
- **Routing Logic:** The application successfully parses ID prefixes, correctly routing Customer (`CUS`) IDs to the Customer Dashboard and Agent (`AG`) IDs to the Agent Dashboard.
- **Dashboard Placeholder:** We are currently using a temporary Fabric `<iframe>` as a placeholder to display the Power BI report on the Customer Dashboard.

## The Issue
The current `<iframe>` approach relies on a **"User-Owns-Data"** embedding model. This model inherently requires the end user viewing the browser to authenticate directly with Microsoft to verify their permissions. 

This is a critical blocker because it forces a Microsoft login screen inside the iframe, which our Customer (`CUS`) users cannot bypass, as they do not have corporate Microsoft accounts.

## The Need
To resolve this, we must shift our architecture to the **"App-Owns-Data"** (Power BI Embedded) model using the `powerbi-client-react` library. This allows our backend server to authenticate with Microsoft on behalf of the user and pass a temporary Embed Token to the frontend, bypassing the login screen entirely.

**Action Required:**
We need an Azure App Registration (Service Principal) configured with at least "Viewer" access to our Fabric Workspace to generate these backend Embed Tokens. We are currently blocked until we receive the following Azure credentials:
- **Client ID** (Application ID)
- **Client Secret**
- **Tenant ID** (Required for the authentication flow)
