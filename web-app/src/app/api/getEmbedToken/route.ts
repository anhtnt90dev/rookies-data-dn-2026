import { NextResponse } from "next/server";
import axios from "axios";

// Helper: fetch dataset details to understand datasource type
async function fetchDatasetInfo(accessToken: string, workspaceId: string, datasetId: string) {
  try {
    console.log("[DEBUG] Fetching dataset info...");
    const url = `https://api.powerbi.com/v1.0/myorg/groups/${workspaceId}/datasets/${datasetId}`;
    const res = await axios.get(url, {
      headers: { Authorization: `Bearer ${accessToken}` }
    });
    const ds = res.data;
    console.log("[DEBUG] Dataset Info:", JSON.stringify({
      name: ds.name,
      configuredBy: ds.configuredBy,
      defaultMode: ds.defaultMode,          // Import, DirectQuery, Push, Streaming, Direct Lake...
      isEffectiveIdentityRequired: ds.isEffectiveIdentityRequired,
      isEffectiveIdentityRolesRequired: ds.isEffectiveIdentityRolesRequired,
      isOnPremGatewayRequired: ds.isOnPremGatewayRequired,
      addRowsAPIEnabled: ds.addRowsAPIEnabled,
    }, null, 2));
    return ds;
  } catch (err: any) {
    console.error("[DEBUG] Failed to fetch dataset info:", err.response?.data || err.message);
    return null;
  }
}

// Helper: fetch datasources for a dataset
async function fetchDatasources(accessToken: string, workspaceId: string, datasetId: string) {
  try {
    console.log("[DEBUG] Fetching datasources for dataset...");
    const url = `https://api.powerbi.com/v1.0/myorg/groups/${workspaceId}/datasets/${datasetId}/datasources`;
    const res = await axios.get(url, {
      headers: { Authorization: `Bearer ${accessToken}` }
    });
    console.log("[DEBUG] Datasources:", JSON.stringify(res.data.value, null, 2));
    return res.data.value;
  } catch (err: any) {
    console.error("[DEBUG] Failed to fetch datasources:", err.response?.data || err.message);
    return null;
  }
}

async function generatePowerBIToken(config: any) {
  const { tenantId, clientId, clientSecret, workspaceId, reportId, datasetId, upperId, role } = config;

  console.log("=== START POWER BI TOKEN GENERATION ===");
  console.log(`Target: User=${upperId}, Role=${role}`);
  console.log(`Config: Workspace=${workspaceId}, Report=${reportId}, Dataset=${datasetId}`);
  console.log(`Tenant=${tenantId}, ClientId=${clientId}, Secret=${clientSecret ? "***" + clientSecret.slice(-4) : "MISSING"}`);

  try {
    // 1. Get Entra ID Auth Token
    console.log("Step 1: Requesting Access Token from Entra ID...");
    const tokenUrl = `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`;
    const tokenParams = new URLSearchParams({
      grant_type: "client_credentials",
      client_id: clientId,
      client_secret: clientSecret,
      scope: "https://analysis.windows.net/powerbi/api/.default"
    });

    const authResponse = await axios.post(tokenUrl, tokenParams.toString(), {
      headers: { "Content-Type": "application/x-www-form-urlencoded" }
    });

    const accessToken = authResponse.data.access_token;
    console.log("Step 1 Success: Received Access Token.");

    // 1.5 [DEBUG] Fetch dataset info & datasources to understand the datasource type
    const datasetInfo = await fetchDatasetInfo(accessToken, workspaceId, datasetId);
    const datasources = await fetchDatasources(accessToken, workspaceId, datasetId);
    
    if (datasetInfo) {
      console.log(`[DEBUG] Dataset mode: ${datasetInfo.defaultMode}`);
      console.log(`[DEBUG] isEffectiveIdentityRequired: ${datasetInfo.isEffectiveIdentityRequired}`);
      console.log(`[DEBUG] isEffectiveIdentityRolesRequired: ${datasetInfo.isEffectiveIdentityRolesRequired}`);
    }

    // 2. Get Power BI Embed Token
    console.log("Step 2: Requesting Embed Token from Power BI REST API...");
    const embedTokenUrl = `https://api.powerbi.com/v1.0/myorg/groups/${workspaceId}/reports/${reportId}/GenerateToken`;
    
    const embedPayloadWithRLS = {
      accessLevel: "View",
      identities: [
        {
          username: upperId,
          roles: [role],
          datasets: [datasetId]
        }
      ]
    };
    
    console.log("Embed Payload (with RLS):", JSON.stringify(embedPayloadWithRLS, null, 2));

    let embedToken: string;
    let rlsApplied = true;

    try {
      const embedResponse = await axios.post(
        embedTokenUrl,
        embedPayloadWithRLS,
        {
          headers: {
            Authorization: `Bearer ${accessToken}`,
            "Content-Type": "application/json"
          }
        }
      );
      console.log("Step 2 Success: Received Embed Token WITH RLS.");
      embedToken = embedResponse.data.token;
    } catch (rlsError: any) {
      const errorCode = rlsError.response?.data?.error?.code;
      const errorMsg = rlsError.response?.data?.error?.message;
      console.warn(`⚠️ RLS embed token failed: ${errorCode} - ${errorMsg}`);

      // If it's the specific "not supported for this datasource" error, retry WITHOUT identities
      if (errorCode === "InvalidRequest" && errorMsg?.includes("not supported for this datasource")) {
        console.warn("⚠️ Retrying WITHOUT RLS identities (dashboard will show ALL data)...");
        const embedPayloadNoRLS = { accessLevel: "View" };
        
        const fallbackResponse = await axios.post(
          embedTokenUrl,
          embedPayloadNoRLS,
          {
            headers: {
              Authorization: `Bearer ${accessToken}`,
              "Content-Type": "application/json"
            }
          }
        );
        console.log("Step 2 Success: Received Embed Token WITHOUT RLS (fallback).");
        embedToken = fallbackResponse.data.token;
        rlsApplied = false;
      } else {
        // Different error, re-throw
        throw rlsError;
      }
    }

    const embedUrl = `https://app.powerbi.com/reportEmbed?reportId=${reportId}&groupId=${workspaceId}`;

    console.log(`=== END POWER BI TOKEN GENERATION (RLS applied: ${rlsApplied}) ===`);
    return {
      accessToken: embedToken,
      embedUrl,
      embedReportId: reportId,
      rlsApplied
    };
  } catch (error: any) {
    console.error("=== POWER BI API ERROR ===");
    if (error.response) {
      console.error("Status:", error.response.status);
      console.error("Error Code:", error.response.data?.error?.code);
      console.error("Error Message:", error.response.data?.error?.message);
      console.error("Full Error Data:", JSON.stringify(error.response.data, null, 2));
    } else {
      console.error("Non-HTTP Error:", error.message);
    }
    console.error("==========================");
    throw error;
  }
}


export async function POST(request: Request) {
  let userId = "";
  let dashboardType = "";
  try {
    const body = await request.json();
    userId = body.userId;
    dashboardType = body.dashboardType;
  } catch (e) {
    return NextResponse.json({ error: "Invalid request body" }, { status: 400 });
  }

  if (!userId) {
    return NextResponse.json({ error: "Missing userId in request body" }, { status: 400 });
  }

  const upperId = userId.trim().toUpperCase();
  let role = "";

  if (upperId.startsWith("CUS")) {
    role = "ROLE_CUSTOMER";
  } else if (upperId.startsWith("AG")) {
    role = "ROLE_AGENT";
  } else {
    return NextResponse.json({ error: "Invalid userId prefix. Must start with CUS or AG." }, { status: 400 });
  }

  // Production Config
  // Customer uses Dashboard-01, Agent uses Dashboard-02 (both published by rookie12 with RLS)
  const prodConfig = {
    tenantId: process.env.AZURE_TENANT_ID,
    clientId: process.env.AZURE_CLIENT_ID,
    clientSecret: process.env.AZURE_CLIENT_SECRET,
    workspaceId: dashboardType === "customer" 
      ? process.env.POWERBI_WORKSPACE_ID_DASHBOARD01 
      : process.env.POWERBI_WORKSPACE_ID_DASHBOARD02,
    reportId: dashboardType === "customer" 
      ? process.env.POWERBI_REPORT_ID_DASHBOARD01 
      : process.env.POWERBI_REPORT_ID_DASHBOARD02,
    datasetId: dashboardType === "customer" 
      ? process.env.POWERBI_DATASET_ID_DASHBOARD01 
      : process.env.POWERBI_DATASET_ID_DASHBOARD02,
    upperId,
    role
  };

  // Test Config
  const testConfig = {
    tenantId: process.env.TEST_AZURE_TENANT_ID,
    clientId: process.env.TEST_AZURE_CLIENT_ID,
    clientSecret: process.env.TEST_AZURE_CLIENT_SECRET,
    workspaceId: process.env.TEST_POWERBI_WORKSPACE_ID,
    reportId: process.env.TEST_POWERBI_REPORT_ID,
    datasetId: process.env.TEST_POWERBI_DATASET_ID,
    upperId,
    role
  };

  // Check if configs have minimum required fields
  const hasTestConfig = testConfig.tenantId && testConfig.clientId && testConfig.clientSecret && testConfig.workspaceId && testConfig.reportId && testConfig.datasetId;
  const hasProdConfig = prodConfig.tenantId && prodConfig.clientId && prodConfig.clientSecret && prodConfig.workspaceId && prodConfig.reportId && prodConfig.datasetId;

  // DEV MODE FALLBACK: If NO credentials are found, tell the frontend to use the public sample
  if (!hasProdConfig && !hasTestConfig) {
    return NextResponse.json({
      devMode: true,
      message: `Missing credentials or IDs for ${dashboardType} dashboard. Switching to Development Mode with Public Sample Report.`
    });
  }

  // Attempt Production First (if configured)
  if (hasProdConfig) {
    try {
      const result = await generatePowerBIToken(prodConfig);
      return NextResponse.json({
        devMode: false,
        isTestFallback: false,
        ...result
      });
    } catch (error: any) {
      console.warn(`Production token generation failed for ${dashboardType}:`, error.response?.data?.error?.code || error.message);
      console.warn("Attempting test fallback...");
      // Fall through to test if available
      if (!hasTestConfig) {
        return NextResponse.json(
          { error: "Failed to generate Embed Token and no test fallback available", details: error.response?.data || error.message },
          { status: 500 }
        );
      }
    }
  }

  // Attempt Test Fallback
  if (hasTestConfig) {
    try {
      const result = await generatePowerBIToken(testConfig);
      return NextResponse.json({
        devMode: false,
        isTestFallback: true,
        ...result
      });
    } catch (error: any) {
      console.error(`Test token generation also failed for ${dashboardType}:`, error.response?.data || error.message);
      return NextResponse.json(
        { error: "Failed to generate Embed Token (both prod and test failed)", details: error.response?.data || error.message },
        { status: 500 }
      );
    }
  }

  return NextResponse.json(
    { error: "Unexpected configuration state" },
    { status: 500 }
  );
}
