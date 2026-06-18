import { NextResponse } from "next/server";

// Helper: run a SQL query against Fabric SQL Endpoint via fetch + TDS
async function queryFabricSQL(query: string): Promise<any[]> {
  const server = process.env.SQL_CONNECTION_STRING;
  const database = process.env.SQL_DATABASE;

  if (!server || !database) {
    throw new Error("SQL_CONNECTION_STRING or SQL_DATABASE not configured");
  }

  // Use dynamic import for tedious
  const { Connection, Request } = await import("tedious");

  return new Promise((resolve, reject) => {
    const rows: any[] = [];

    const config = {
      server: server,
      authentication: {
        type: "azure-active-directory-service-principal-secret" as const,
        options: {
          clientId: process.env.AZURE_CLIENT_ID!,
          clientSecret: process.env.AZURE_CLIENT_SECRET!,
          tenantId: process.env.AZURE_TENANT_ID!,
        },
      },
      options: {
        database: database,
        encrypt: true,
        port: 1433,
        connectTimeout: 15000,
        requestTimeout: 15000,
      },
    };

    const connection = new Connection(config);

    connection.on("connect", (err) => {
      if (err) {
        console.error("[SQL] Connection error:", err.message);
        reject(err);
        return;
      }

      const request = new Request(query, (err) => {
        if (err) {
          console.error("[SQL] Query error:", err.message);
          reject(err);
        } else {
          resolve(rows);
        }
        connection.close();
      });

      request.on("row", (columns) => {
        const row: any = {};
        columns.forEach((col: any) => {
          row[col.metadata.colName] = col.value;
        });
        rows.push(row);
      });

      connection.execSql(request);
    });

    connection.connect();
  });
}

export async function POST(request: Request) {
  try {
    console.log("=== START LOGIN API ===");
    const body = await request.json();
    const { userId } = body;
    console.log("Received userId:", userId);

    if (!userId) {
      console.log("Missing userId");
      return NextResponse.json({ success: false, error: "Missing userId" }, { status: 400 });
    }

    const upperId = userId.trim().toUpperCase();
    console.log("Normalized userId:", upperId);

    let role = "";

    // Determine role from prefix first
    if (upperId.startsWith("CUS")) {
      role = "customer";
    } else if (upperId.startsWith("AG")) {
      role = "agent";
    } else {
      console.log("Invalid prefix");
      return NextResponse.json({ success: false, error: "Invalid User ID prefix. Must start with CUS or AG." });
    }

    // Try SQL verification
    try {
      console.log(`Verifying ${upperId} in SQL database...`);

      let query = "";
      if (role === "customer") {
        query = `SELECT TOP 1 customer_id FROM dim_customer WHERE UPPER(customer_id) = '${upperId}'`;
      } else {
        query = `SELECT TOP 1 agent_id FROM dim_agent WHERE UPPER(agent_id) = '${upperId}'`;
      }

      console.log("SQL Query:", query);
      const results = await queryFabricSQL(query);

      if (results.length === 0) {
        console.log(`User ${upperId} NOT found in database`);
        return NextResponse.json({ success: false, error: `User ID '${userId}' not found.` });
      }

      console.log(`User ${upperId} FOUND in database. Role: ${role}`);
    } catch (sqlError: any) {
      // SQL failed - fall back to prefix-only
      console.warn(`SQL verification failed: ${sqlError.message}`);
      console.warn("Falling back to prefix-only check...");
      console.log(`Prefix matched. Role assigned: ${role} (SQL bypass)`);
    }

    console.log("Login successful, returning role:", role);
    console.log("=== END LOGIN API ===");
    return NextResponse.json({ success: true, role });

  } catch (error: any) {
    console.error("Login Error:", error);
    return NextResponse.json({ success: false, error: "System error." }, { status: 500 });
  }
}
