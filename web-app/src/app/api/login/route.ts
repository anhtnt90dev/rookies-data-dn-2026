import { NextResponse } from "next/server";
import sql from "mssql";

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { userId } = body;

    if (!userId) {
      return NextResponse.json({ success: false, error: "Missing userId" }, { status: 400 });
    }

    const upperId = userId.trim().toUpperCase();

    const isTestDatabase = !process.env.SQL_CONNECTION_STRING && !process.env.FABRIC_SQL_SERVER;

    const sqlConfig: sql.config = {
      server: process.env.SQL_CONNECTION_STRING || process.env.FABRIC_SQL_SERVER || process.env.TEST_SQL_CONNECTION_STRING || "",
      database: process.env.SQL_DATABASE || process.env.FABRIC_SQL_DATABASE || process.env.TEST_SQL_DATABASE || "",
      authentication: {
        type: 'azure-active-directory-service-principal-secret',
        options: {
          clientId: isTestDatabase ? (process.env.TEST_AZURE_CLIENT_ID || "") : (process.env.AZURE_CLIENT_ID || process.env.TEST_AZURE_CLIENT_ID || ""),
          tenantId: isTestDatabase ? (process.env.TEST_AZURE_TENANT_ID || "") : (process.env.AZURE_TENANT_ID || process.env.TEST_AZURE_TENANT_ID || ""),
          clientSecret: isTestDatabase ? (process.env.TEST_AZURE_CLIENT_SECRET || "") : (process.env.AZURE_CLIENT_SECRET || process.env.TEST_AZURE_CLIENT_SECRET || "")
        }
      },
      options: {
        encrypt: true,
        port: 1433
      }
    };

    // If variables are missing, fallback to dev mock
    if (!sqlConfig.server || !sqlConfig.database || !sqlConfig.authentication?.options?.clientId) {
      console.warn("Missing SQL Configuration. Using mock fallback for development.");
      let role = "";
      if (upperId.startsWith("CUS")) role = "customer";
      else if (upperId.startsWith("AG")) role = "agent";
      else return NextResponse.json({ success: false, error: "Invalid User ID prefix." });
      
      return NextResponse.json({ success: true, role });
    }

    await sql.connect(sqlConfig);

    let isAuthorized = false;
    let role = "";

    if (upperId.startsWith("CUS")) {
      const result = await sql.query`SELECT TOP 1 customer_id FROM gold.dim_customer WHERE customer_id = ${upperId}`;
      if (result.recordset.length > 0) {
        isAuthorized = true;
        role = "customer";
      }
    } else if (upperId.startsWith("AG")) {
      const result = await sql.query`SELECT TOP 1 agent_id FROM gold.dim_agent WHERE agent_id = ${upperId}`;
      if (result.recordset.length > 0) {
        isAuthorized = true;
        role = "agent";
      }
    } else {
      return NextResponse.json({ success: false, error: "Invalid User ID prefix." });
    }

    if (isAuthorized) {
      return NextResponse.json({ success: true, role });
    } else {
      return NextResponse.json({ success: false, error: "ID does not exist in the system." });
    }

  } catch (error: any) {
    console.error("SQL Error:", error);
    return NextResponse.json({ success: false, error: "Database connection error. Please try again later." }, { status: 500 });
  } finally {
    try {
      await sql.close();
    } catch (e) {
      // Ignore
    }
  }
}
