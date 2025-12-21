"use client";

import React from 'react';

type ComparisonMode = 'schema' | 'gcs' | 'history' | 'scd';

interface SidebarProps {
    currentMode: ComparisonMode;
    onModeChange: (mode: ComparisonMode) => void;
}

export default function Sidebar({ currentMode, onModeChange }: SidebarProps) {
    const menuItems = [
        { id: 'schema', label: 'Schema Validation', icon: '📊' },
        { id: 'scd', label: 'SCD Validation', icon: '🔄' },
        { id: 'gcs', label: 'GCS Comparison', icon: '📁' },
        { id: 'history', label: 'History', icon: '📜' },
    ];

    return (
        <aside className="sidebar">
            <div className="sidebar-logo-container">
                {/* eslint-disable-next-line @next/next/no-img-element */}
                <img
                    src="/logo.jpg"
                    alt="Intelia Logo"
                    style={{ maxHeight: '80px', maxWidth: '100%', objectFit: 'contain' }}
                />
            </div>

            <h2 style={{ fontSize: '0.875rem', textTransform: 'uppercase', letterSpacing: '0.05em', color: 'var(--secondary-foreground)', fontWeight: '600', marginBottom: '0.5rem' }}>
                Navigation
            </h2>
            <nav className="sidebar-nav">
                {menuItems.map((item) => (
                    <button
                        key={item.id}
                        onClick={() => onModeChange(item.id as ComparisonMode)}
                        style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: '0.75rem',
                            padding: '0.75rem 1rem',
                            borderRadius: 'var(--radius)',
                            border: 'none',
                            backgroundColor: currentMode === item.id ? 'var(--primary)' : 'transparent',
                            color: currentMode === item.id ? 'white' : 'var(--foreground)',
                            cursor: 'pointer',
                            fontWeight: '500',
                            textAlign: 'left',
                            transition: 'all 0.2s ease',
                            whiteSpace: 'nowrap'
                        }}
                        onMouseEnter={(e) => {
                            if (currentMode !== item.id) {
                                e.currentTarget.style.backgroundColor = 'var(--secondary)';
                            }
                        }}
                        onMouseLeave={(e) => {
                            if (currentMode !== item.id) {
                                e.currentTarget.style.backgroundColor = 'transparent';
                            }
                        }}
                    >
                        <span>{item.icon}</span>
                        {item.label}
                    </button>
                ))}
            </nav>
        </aside>
    );
}
