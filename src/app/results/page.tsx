"use client";

import { useEffect, useState } from "react";
import ResultsView from "@/components/ResultsView";
import Link from "next/link";
import Sidebar from "@/components/Sidebar";

export default function ResultsPage() {
    const [currentMode, setCurrentMode] = useState<any>('schema');
    const [fromHistory, setFromHistory] = useState(false);

    useEffect(() => {
        const data = localStorage.getItem("testResults");
        if (data) {
            try {
                const parsed = JSON.parse(data);
                if (parsed.fromHistory || (parsed.execution_id && !parsed.is_latest)) {
                    setFromHistory(true);
                }

                if (parsed.comparison_mode) {
                    // Normalize gcs-config/scd-config back to base categories for sidebar
                    const mode = parsed.comparison_mode.toLowerCase();
                    if (mode.includes('gcs')) {
                        setCurrentMode('gcs');
                    } else if (mode.includes('scd')) {
                        setCurrentMode('scd');
                    } else if (mode === 'history') {
                        setCurrentMode('history');
                        setFromHistory(true);
                    } else {
                        setCurrentMode(mode);
                    }
                }
            } catch (e) {
                console.error("Failed to parse results for sidebar", e);
            }
        }
    }, []);

    return (
        <div className="dashboard-layout">
            {/* Sidebar - correctly highlighting the mode that triggered the results */}
            <Sidebar currentMode={currentMode} onModeChange={() => { }} />

            <div className="main-content">
                <div className="container" style={{ maxWidth: '1200px' }}>
                    <header className="header" style={{ marginBottom: '2rem', borderRadius: 'var(--radius)', border: '1px solid var(--border)', display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '1rem' }}>
                        <div className="logo" style={{ fontSize: '1.5rem', fontWeight: 'bold' }}></div>
                        <Link href={fromHistory ? "/?mode=history" : "/"} className="btn btn-primary" style={{ textDecoration: 'none', padding: '0.5rem 1rem', background: 'var(--primary)', color: 'white', borderRadius: 'var(--radius)' }}>
                            {fromHistory ? '⬅ Back to History' : 'Back to Dashboard'}
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
