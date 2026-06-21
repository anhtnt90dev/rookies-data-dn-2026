"use client";

import { useEffect, useState, useRef, useCallback } from "react";
import { useRouter } from "next/navigation";
import dynamic from "next/dynamic";
import styles from "../dashboard.module.css";

// Dynamically import PowerBIEmbed to prevent SSR issues
const PowerBIEmbed = dynamic(
  () => import("powerbi-client-react").then((mod) => mod.PowerBIEmbed),
  { ssr: false, loading: () => <div style={{ padding: 20 }}>Loading Power BI Component...</div> }
);

export default function AgentDashboard() {
  const router = useRouter();
  const [embedConfig, setEmbedConfig] = useState<any>(null);
  const [error, setError] = useState("");
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [cooldown, setCooldown] = useState(0);
  const reportRef = useRef<any>(null);

  const handleRefresh = useCallback(async () => {
    if (!reportRef.current || isRefreshing || cooldown > 0) return;
    try {
      setIsRefreshing(true);
      await reportRef.current.refresh();
      // Start 15s cooldown after successful refresh (Power BI rate limit)
      setCooldown(15);
      const timer = setInterval(() => {
        setCooldown((prev) => {
          if (prev <= 1) { clearInterval(timer); return 0; }
          return prev - 1;
        });
      }, 1000);
    } catch (err: any) {
      console.error("Refresh failed:", err);
    } finally {
      setTimeout(() => setIsRefreshing(false), 1000);
    }
  }, [isRefreshing, cooldown]);

  useEffect(() => {
    let intervalId: NodeJS.Timeout;

    async function fetchToken() {
      try {
        const storedUserId = localStorage.getItem("carpro_userId");
        if (!storedUserId) {
          router.push("/login");
          return;
        }

        const response = await fetch("/api/getEmbedToken", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ userId: storedUserId, dashboardType: "agent" }),
        });
        
        const data = await response.json();

        // 1 = Embed Token, 0 = AAD Token
        const tokenTypeEmbed = 1;

        const { models } = await import("powerbi-client");

        if (data.devMode) {
          // WORKAROUND: Without Azure Credentials, powerbi-client-react will crash.
          // For Dev Mode, we fall back to a standard iframe.
          // To ensure you don't get stuck on a login screen, we use a truly PUBLIC 
          // "Publish to Web" Microsoft sample report as a visual placeholder.
          setEmbedConfig({
            devModeFallback: true,
            embedUrl: "https://app.powerbi.com/view?r=eyJrIjoiM2I0YWMxMTItYjdmMC00Y2EzLWFkODEtZDY0OTM5NDM5YWU1IiwidCI6IjA4YTBiODI0LTU2ZjktNDk4My1hYzhhLTNmZDM3M2Y2ODQ2NiIsImMiOjF9"
          });
        } else if (data.accessToken && data.embedUrl && data.embedReportId) {
          // REAL APP-OWNS-DATA: Once credentials are in .env.local
          setEmbedConfig({
            type: "report",
            id: data.embedReportId,
            embedUrl: data.embedUrl,
            accessToken: data.accessToken,
            tokenType: tokenTypeEmbed,
            settings: {
              layoutType: models.LayoutType.Custom,
              customLayout: {
                displayOption: models.DisplayOption.FitToWidth
              },
              panes: {
                filters: { expanded: false, visible: false },
                pageNavigation: { visible: true }
              }
            }
          });
        } else {
          setError(data.error || "Failed to load dashboard configuration.");
        }
      } catch (err) {
        setError("Error connecting to the authentication server.");
      }
    }

    fetchToken();
    // Refresh the token every 50 minutes (50 * 60 * 1000 = 3000000 ms)
    intervalId = setInterval(fetchToken, 3000000);

    return () => clearInterval(intervalId);
  }, []);

  return (
    <main className={styles.container}>
      <header className={`glass-panel ${styles.header}`}>
        <div>
          <h1>CarPro Dashboard 2</h1>
          <p>Agent Analytics</p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          {embedConfig && !embedConfig.devModeFallback && (
            <button
              id="btn-refresh-dashboard"
              onClick={handleRefresh}
              disabled={isRefreshing || cooldown > 0}
              title={cooldown > 0 ? `Wait ${cooldown}s` : "Refresh data"}
              style={{
                width: cooldown > 0 ? 'auto' : 40,
                height: 40,
                padding: cooldown > 0 ? '0 12px' : 0,
                borderRadius: cooldown > 0 ? '20px' : '50%',
                border: '1px solid var(--glass-border)',
                background: 'var(--glass-bg)',
                cursor: (isRefreshing || cooldown > 0) ? 'not-allowed' : 'pointer',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                gap: '6px',
                transition: 'all 0.2s ease',
                opacity: (isRefreshing || cooldown > 0) ? 0.6 : 1,
                fontSize: '12px',
                color: 'var(--text-secondary)',
              }}
            >
              <svg
                width="18" height="18" viewBox="0 0 24 24" fill="none"
                stroke="var(--primary)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
                style={{
                  animation: isRefreshing ? 'spin 0.8s linear infinite' : 'none',
                }}
              >
                <polyline points="23 4 23 10 17 10" />
                <polyline points="1 20 1 14 7 14" />
                <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" />
              </svg>
              {cooldown > 0 && <span>{cooldown}s</span>}
            </button>
          )}
          <button onClick={() => {
            localStorage.removeItem("carpro_userId");
            router.push("/");
          }} className="btn-primary" style={{ width: 'auto' }}>
            Sign Out
          </button>
        </div>
      </header>

      <div className={`glass-panel ${styles.iframeContainer}`} style={{ display: 'flex', flexDirection: 'column' }}>
        {error ? (
          <div style={{ padding: 20, color: "var(--error)" }}>{error}</div>
        ) : !embedConfig ? (
          <div style={{ padding: 20, color: "var(--text-primary)" }}>Authenticating securely with Power BI...</div>
        ) : embedConfig.devModeFallback ? (
          <>
            <div style={{ padding: "10px 20px", backgroundColor: "var(--error)", color: "white", borderRadius: "8px", marginBottom: "10px", textAlign: "center" }}>
              <strong>Running Offline:</strong> Azure Credentials Missing. Displaying public sample report.
            </div>
             <iframe
                title="Dashboard 2 - Agent Analytics"
                width="100%"
                height="100%"
                src={embedConfig.embedUrl}
                frameBorder="0"
                allowFullScreen={true}
                style={{ flex: 1, height: '100%', width: '100%', minHeight: '600px', border: 'none', borderRadius: '12px' }}
              ></iframe>
          </>
        ) : (
          <div style={{ flex: 1, display: 'flex', width: "100%", height: "100%", minHeight: 0 }}>
            <PowerBIEmbed
              embedConfig={embedConfig}
              cssClassName={styles.powerbiContainer}
              getEmbeddedComponent={(embeddedReport: any) => {
                reportRef.current = embeddedReport;
              }}
            />
          </div>
        )}
      </div>
    </main>
  );
}
