import Link from "next/link";
import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Iceberg Ops Console",
  description: "Monitor and operate Apache Iceberg tables",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="ko">
      <body>
        <header className="topbar">
          <Link href="/" className="brand" aria-label="Iceberg Ops Console 홈">
            <span className="brand-mark">I</span>
            <span>Iceberg <strong>Ops</strong></span>
          </Link>
          <nav>
            <Link href="/">테이블 현황</Link>
            <Link href="/operations">작업 관리</Link>
            <Link href="/playground">조회 도구</Link>
          </nav>
          <div className="environment"><span /> production</div>
        </header>
        <main>{children}</main>
      </body>
    </html>
  );
}
