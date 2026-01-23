"use client";

import { useEffect, useState } from "react";
import ResultsView from "@/components/ResultsView";
import Link from "next/link";
import Sidebar from "@/components/Sidebar";

type ComparisonMode = 'schema' | 'gcs' | 'history' | 'settings' | 'scd';

export default function ResultsPage() {
    const [currentMode, setCurrentMode] = useState<ComparisonMode>('schema');
    const [fromHistory, setFromHistory] = useState(false);
    const [projectId, setProjectId] = useState<string>("");
    const [executionId, setExecutionId] = useState<string>("");

    useEffect(() => {
        const data = localStorage.getItem("testResults");
        if (data) {
            try {
                const parsed = JSON.parse(data);

                // Get IDs
                if (parsed.project_id) setProjectId(parsed.project_id);
                else {
                    const storedPid = localStorage.getItem("projectId");
                    if (storedPid) setProjectId(storedPid);
                }

                if (parsed.execution_id) setExecutionId(parsed.execution_id);
                else if (parsed.results && Array.isArray(parsed.results) && parsed.results.length > 0 && parsed.results[0].execution_id) {
                    setExecutionId(parsed.results[0].execution_id);
                }
                else if (Array.isArray(parsed) && parsed.length > 0 && parsed[0].execution_id) {
                    setExecutionId(parsed[0].execution_id);
                }

                if (parsed.fromHistory || (parsed.execution_id && !parsed.is_latest)) {
                    setFromHistory(true);
                }

                if (parsed.comparison_mode) {
                    const mode = parsed.comparison_mode.toLowerCase();
                    if (mode.includes('gcs')) {
                        setCurrentMode('gcs');
                    } else if (mode.includes('scd')) {
                        setCurrentMode('scd');
                    } else {
                        setCurrentMode(mode);
                    }
                } else if (parsed.fromHistory) {
                    setCurrentMode('history');
                }
            } catch (e) {
                console.error("Failed to parse results for sidebar", e);
            }
        }
    }, []);

    return (
        <div className="dashboard-layout">
            <Sidebar currentMode={currentMode} onModeChange={() => { }} />

            <div className="main-content">
                <div className="container" style={{ maxWidth: '1200px' }}>
                    <header className="header" style={{
                        marginBottom: '2rem',
                        borderRadius: 'var(--radius)',
                        border: '1px solid var(--border)',
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'center',
                        padding: '1rem 1.5rem',
                        background: 'white',
                        boxShadow: 'var(--shadow-sm)'
                    }}>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
                            <div style={{ fontSize: '0.75rem', fontWeight: '700', color: '#64748b', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                                Project: <span style={{ color: 'var(--primary)', fontFamily: 'monospace' }}>{projectId || 'Unknown'}</span>
                            </div>
                            {executionId && (
                                <div style={{ fontSize: '0.875rem', fontWeight: '600', color: '#1e293b' }}>
                                    Run ID: <span style={{ fontFamily: 'monospace', color: '#64748b' }}>{executionId}</span>
                                </div>
                            )}
                        </div>

                        <Link
                            href={`/?mode=${fromHistory ? 'history' : currentMode}`}
                            className="btn btn-primary"
                            style={{ textDecoration: 'none', padding: '0.5rem 1.25rem', background: 'var(--primary)', color: 'white', borderRadius: 'var(--radius)', fontWeight: '600', fontSize: '0.875rem' }}
                        >
                            {fromHistory ? '⬅ Back to History' : '⬅ Back'}
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
