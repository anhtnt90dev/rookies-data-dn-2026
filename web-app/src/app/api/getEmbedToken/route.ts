import { NextResponse } from "next/server";
import axios from "axios";

export async function GET() {
  const tenantId = process.env.AZURE_TENANT_ID;
  const clientId = process.env.AZURE_CLIENT_ID;
  const clientSecret = process.env.AZURE_CLIENT_SECRET;
  const workspaceId = process.env.POWERBI_WORKSPACE_ID;
  const reportId = process.env.POWERBI_REPORT_ID;

  // DEV MODE FALLBACK: If credentials are missing, tell the frontend to use the public sample
  if (!tenantId || !clientId || !clientSecret) {
    return NextResponse.json({
      devMode: true,
      message: "Credentials missing. Switching to Development Mode with Public Sample Report."
    });
  }

  try {
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
    
    const embedResponse = await axios.post(
      embedTokenUrl,
      { accessLevel: "View" },
      {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          "Content-Type": "application/json"
        }
      }
    );

    const embedToken = embedResponse.data.token;
    // Note: embedUrl format for App-Owns-Data
    const embedUrl = `https://app.powerbi.com/reportEmbed?reportId=${reportId}&groupId=${workspaceId}`;

    return NextResponse.json({
      devMode: false,
      accessToken: embedToken,
      embedUrl,
      embedReportId: reportId
    });

  } catch (error: any) {
    console.error("Error generating Power BI embed token:", error.response?.data || error.message);
    return NextResponse.json(
      { error: "Failed to generate Embed Token", details: error.message },
      { status: 500 }
    );
  }
}
