"use client";

import { useState } from "react";
import LoginButton from "@/components/LoginButton";
import DashboardForm from "@/components/DashboardForm";
import Sidebar from "@/components/Sidebar";

type ComparisonMode = 'schema' | 'gcs' | 'history' | 'scd';

export default function Home() {
    const [comparisonMode, setComparisonMode] = useState<ComparisonMode>('schema');

    return (
        <main style={{
            minHeight: '100vh',
            display: 'flex',
            flexDirection: 'column',
            position: 'relative',
            overflow: 'hidden'
        }}>
            {/* Animated background gradient */}
            <div style={{
                position: 'absolute',
                top: '-50%',
                left: '-50%',
                width: '200%',
                height: '200%',
                background: 'radial-gradient(circle at 50% 50%, rgba(99, 102, 241, 0.15) 0%, transparent 50%)',
                animation: 'gradient-shift 15s ease infinite',
                pointerEvents: 'none',
                zIndex: 0
            }} />

            <div className="dashboard-layout">
                {/* Sidebar */}
                <Sidebar currentMode={comparisonMode} onModeChange={setComparisonMode} />

                {/* Main Content Area */}
                <div className="main-content">
                    <div style={{
                        maxWidth: '1000px',
                        margin: '0 auto',
                    }} className="fade-in">
                        {/* Header */}
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '3rem' }}>
                            <div>
                                <h1 style={{
                                    fontSize: '2.5rem',
                                    fontWeight: '800',
                                    marginBottom: '0.5rem',
                                    lineHeight: '1.1',
                                    letterSpacing: '-0.02em'
                                }} className="gradient-text">
                                    Data QA Agent
                                </h1>
                                <p style={{
                                    fontSize: '1rem',
                                    color: 'var(--secondary-foreground)',
                                    opacity: 0.8
                                }}>
                                    AI-powered data quality testing
                                </p>
                            </div>
                            <LoginButton />
                        </div>

                        {/* Feature highlights (Mini) */}
                        <div style={{
                            display: 'grid',
                            gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
                            gap: '1rem',
                            marginBottom: '2rem'
                        }}>
                            <div className="card" style={{ padding: '1rem', textAlign: 'center', background: 'rgba(255,255,255,0.05)' }}>
                                <span style={{ marginRight: '0.5rem' }}>🤖</span> Gemini AI Analysis
                            </div>
                            <div className="card" style={{ padding: '1rem', textAlign: 'center', background: 'rgba(255,255,255,0.05)' }}>
                                <span style={{ marginRight: '0.5rem' }}>⚡</span> Automated Testing
                            </div>
                            <div className="card" style={{ padding: '1rem', textAlign: 'center', background: 'rgba(255,255,255,0.05)' }}>
                                <span style={{ marginRight: '0.5rem' }}>📊</span> Multi-Dataset
                            </div>
                        </div>

                        {/* Main Form */}
                        <DashboardForm comparisonMode={comparisonMode} />

                        {/* Footer */}
                        <p style={{
                            marginTop: '3rem',
                            textAlign: 'center',
                            fontSize: '0.875rem',
                            color: 'var(--secondary-foreground)',
                            opacity: 0.7
                        }}>
                            Powered by Google Cloud Vertex AI & BigQuery
                        </p>
                    </div>
                </div>
            </div>
        </main>
    );
}
