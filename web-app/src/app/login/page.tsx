"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import styles from "./login.module.css";
import Link from "next/link";

export default function Login() {
  const [userId, setUserId] = useState("");
  const [error, setError] = useState("");
  const router = useRouter();

  const handleLogin = (e: React.FormEvent) => {
    e.preventDefault();
    setError("");

    if (!userId) {
      setError("Please enter a User ID");
      return;
    }

    const upperId = userId.toUpperCase();
    
    if (upperId.startsWith("CUS")) {
      router.push("/dashboard/customer");
    } else if (upperId.startsWith("AG")) {
      router.push("/dashboard/agent");
    } else {
      setError("Invalid User ID prefix. Must start with CUS or AG.");
    }
  };

  return (
    <main className={styles.container}>
      <Link href="/" className="back-link">
        &larr; Back to Home
      </Link>
      <div className={`glass-panel ${styles.loginCard}`}>
        <div className={styles.header}>
          <h1>CarPro</h1>
          <p>Secure Portal Login</p>
        </div>
        
        <form onSubmit={handleLogin} className={styles.form}>
          <div className={styles.inputGroup}>
            <label htmlFor="userId">User ID</label>
            <input
              id="userId"
              type="text"
              className="input-field"
              placeholder="e.g. CUS12345"
              value={userId}
              onChange={(e) => setUserId(e.target.value)}
              maxLength={8}
              autoComplete="off"
            />
            {error && <p className="error-text">{error}</p>}
          </div>
          
          <button type="submit" className="btn-primary">
            Sign In
          </button>
        </form>
      </div>
    </main>
  );
}
