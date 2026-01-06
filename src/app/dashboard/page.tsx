"use client";

import { useState } from "react";
import DashboardForm from "@/components/DashboardForm";
import LoginButton from "@/components/LoginButton";
import Sidebar from "@/components/Sidebar";

export default function Dashboard() {
    // Mode state: 'schema' | 'gcs' | 'history' | 'scd'
    const [comparisonMode, setComparisonMode] = useState<'schema' | 'gcs' | 'history' | 'scd'>('schema');

    return (
        <main style={{ minHeight: '100vh', position: 'relative' }}>
            {/* Background elements */}
            <div style={{
                position: 'fixed',
                top: 0,
                left: 0,
                width: '100%',
                height: '100%',
                backgroundImage: 'radial-gradient(circle at 50% 0%, rgba(102, 126, 234, 0.03) 0%, rgba(250, 250, 250, 0) 50%)',
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
