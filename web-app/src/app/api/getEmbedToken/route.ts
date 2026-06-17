import { NextResponse } from "next/server";
import axios from "axios";

async function generatePowerBIToken(config: any) {
  const { tenantId, clientId, clientSecret, workspaceId, reportId, datasetId, upperId, role } = config;

  // 1. Get Entra ID Auth Token
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

  // 2. Get Power BI Embed Token
  const embedTokenUrl = `https://api.powerbi.com/v1.0/myorg/groups/${workspaceId}/reports/${reportId}/GenerateToken`;

  const embedPayload = {
    accessLevel: "View",
    identities: [
      {
        username: upperId,
        roles: [role],
        datasets: [datasetId]
      }
    ]
  };

  const embedResponse = await axios.post(
    embedTokenUrl,
    embedPayload,
    {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "application/json"
      }
    }
  );

  const embedToken = embedResponse.data.token;
  const embedUrl = `https://app.powerbi.com/reportEmbed?reportId=${reportId}&groupId=${workspaceId}`;

  return {
    accessToken: embedToken,
    embedUrl,
    embedReportId: reportId
  };
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
  const prodConfig = {
    tenantId: process.env.AZURE_TENANT_ID,
    clientId: process.env.AZURE_CLIENT_ID,
    clientSecret: process.env.AZURE_CLIENT_SECRET,
    workspaceId: dashboardType === "customer" ? process.env.POWERBI_WORKSPACE_ID_CUSTOMER : process.env.POWERBI_WORKSPACE_ID_AGENT,
    reportId: dashboardType === "customer" ? process.env.POWERBI_REPORT_ID_CUSTOMER : process.env.POWERBI_REPORT_ID_AGENT,
    datasetId: dashboardType === "customer" ? process.env.POWERBI_DATASET_ID_CUSTOMER : process.env.POWERBI_DATASET_ID_AGENT,
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
