import Link from "next/link";
import styles from "./page.module.css";

export default function Home() {
  return (
    <main className={styles.main}>
      {/* Header */}
      <header className={styles.header}>
        <Link href="/" className={styles.logo}>
          <svg width="32" height="32" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M18.92 6.01C18.72 5.42 18.16 5 17.5 5H15L13 2H11L9.22 5H6.5C5.67 5 5 5.67 5 6.5V13H19V6.5C19 6.22 18.98 6.11 18.92 6.01ZM18.92 6.01H5V13H19V6.01ZM7 15C5.9 15 5 15.9 5 17C5 18.1 5.9 19 7 19C8.1 19 9 18.1 9 17C9 15.9 8.1 15 7 15ZM17 15C15.9 15 15 15.9 15 17C15 18.1 15.9 19 17 19C18.1 19 19 18.1 19 17C19 15.9 18.1 15 17 15Z" fill="currentColor" />
          </svg>
          Carpro Insurance
        </Link>
        <div className={styles.headerRight}>
          <Link href="/login" className={styles.loginLink}>
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"></path>
              <circle cx="12" cy="7" r="4"></circle>
            </svg>
            Login
          </Link>
          <Link href="/quote" className="btn-primary">
            Get a Quote &rarr;
          </Link>
        </div>
      </header>

      {/* Hero Section */}
      <section className={styles.hero}>
        <div className={styles.heroContent}>
          <h1 className={styles.heroTitle}>
            <span>85 years</span> for<br />
            affordable &amp;<br />
            reliable auto<br />
            insurance
          </h1>
          <div className={styles.heroSubtitle}>
            <svg className={styles.shieldIcon} viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" fill="currentColor" />
              <path d="M9 12l2 2 4-4" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
            <p className={styles.heroSubtitleText}>
              Switch to Carpro and see how much you could save on car insurance.
            </p>
          </div>
          <Link href="/quote" className={`btn-primary ${styles.quoteButton}`}>
            Get a Quote &rarr;
          </Link>
        </div>
      </section>

      {/* Footer */}
      <footer className={styles.footer}>
        <div className={styles.footerLinks}>
          <Link href="/privacy">Privacy</Link>
          <Link href="/terms">Terms &amp; conditions</Link>
          <Link href="/support">Contact &amp; Support</Link>
        </div>
        <p className={styles.copyright}>&copy; 2025 Carpro Insurance</p>
      </footer>
    </main>
  );
}
