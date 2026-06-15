"use client";

import { useRouter } from "next/navigation";
import styles from "../dashboard.module.css";

export default function AgentDashboard() {
  const router = useRouter();

  return (
    <main className={styles.container}>
      <header className={`glass-panel ${styles.header}`}>
        <div>
          <h1>CarPro Dashboard 2</h1>
          <p>Agent Analytics</p>
        </div>
        <button onClick={() => router.push("/")} className="btn-primary" style={{ width: 'auto' }}>
          Sign Out
        </button>
      </header>
      
      <div className={`glass-panel ${styles.iframeContainer}`}>
        <iframe
          title="Dashboard 2 - Agent Analytics"
          width="100%"
          height="100%"
          src="https://app.powerbi.com/reportEmbed?reportId=placeholder&autoAuth=true&ctid=placeholder"
          frameBorder="0"
          allowFullScreen={true}
        ></iframe>
      </div>
    </main>
  );
}
