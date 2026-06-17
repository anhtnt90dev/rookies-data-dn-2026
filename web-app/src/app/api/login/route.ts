import { NextResponse } from "next/server";

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

    // BYPASS SQL VERIFICATION - JUST CHECK PREFIX
    console.log("Bypassing SQL. Checking prefix only...");
    
    let role = "";
    if (upperId.startsWith("CUS")) {
      role = "customer";
      console.log("Prefix matched CUS. Role assigned: customer");
    } else if (upperId.startsWith("AG")) {
      role = "agent";
      console.log("Prefix matched AG. Role assigned: agent");
    } else {
      console.log("Invalid prefix");
      return NextResponse.json({ success: false, error: "Invalid User ID prefix." });
    }

    console.log("Login successful, returning role:", role);
    console.log("=== END LOGIN API ===");
    return NextResponse.json({ success: true, role });

  } catch (error: any) {
    console.error("Login Error:", error);
    return NextResponse.json({ success: false, error: "System error." }, { status: 500 });
  }
}
