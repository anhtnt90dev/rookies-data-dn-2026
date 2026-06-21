"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import styles from "./login.module.css";
import Link from "next/link";

export default function Login() {
  const [userId, setUserId] = useState("");
  const [error, setError] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const router = useRouter();

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");

    if (!userId) {
      setError("Please enter a User ID");
      return;
    }

    const upperId = userId.trim().toUpperCase();

    if (!upperId.startsWith("CUS") && !upperId.startsWith("AG")) {
      setError("Invalid User ID prefix. Must start with CUS or AG.");
      return;
    }

    setIsLoading(true);

    try {
      const response = await fetch("/api/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ userId: upperId }),
      });

      const data = await response.json();

      if (response.ok && data.success) {
        localStorage.setItem("carpro_userId", upperId);
        if (data.role === "customer") {
          router.push("/dashboard/customer");
        } else if (data.role === "agent") {
          router.push("/dashboard/agent");
        }
      } else {
        setError(data.error || "ID doesn't exist in the system.");
      }
    } catch (err) {
      setError("An error occurred while checking the ID. Please try again.");
    } finally {
      setIsLoading(false);
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
              disabled={isLoading}
            />
            {error && <p className="error-text">{error}</p>}
          </div>

          <button type="submit" className="btn-primary" disabled={isLoading}>
            {isLoading ? "Checking..." : "Sign In"}
          </button>
        </form>
      </div>
    </main>
  );
}
