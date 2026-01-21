"use client";

import ResultsView from "@/components/ResultsView";
import Link from "next/link";
import Sidebar from "@/components/Sidebar";

export default function ResultsPage() {
    return (
        <div className="dashboard-layout">
            {/* Sidebar - defaulting to 'history' mode since we are viewing results */}
            <Sidebar currentMode="history" onModeChange={() => { }} />

            <div className="main-content">
                <div className="container" style={{ maxWidth: '1200px' }}>
                    <header className="header" style={{ marginBottom: '2rem', borderRadius: 'var(--radius)', border: '1px solid var(--border)', display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '1rem' }}>
                        <div className="logo" style={{ fontSize: '1.5rem', fontWeight: 'bold' }}>Test Results</div>
                        <Link href="/" className="btn btn-primary" style={{ textDecoration: 'none', padding: '0.5rem 1rem', background: 'var(--primary)', color: 'white', borderRadius: 'var(--radius)' }}>
                            New Test
                        </Link>
                    </header>

                    <main>
                        <ResultsView />
                    </main>
                </div>
            </div>
        </div>
    );
}
