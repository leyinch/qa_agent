"use client";

import React, { useEffect, useState } from "react";
import { PieChart, Pie, Cell, Tooltip, Legend, ResponsiveContainer } from "recharts";

interface TestResult {
    test_id?: string;
    test_name: string;
    category?: string;
    description: string;
    sql_query: string;
    severity: string;
    status: "PASS" | "FAIL" | "ERROR";
    rows_affected?: number;
    sample_data?: any[];
    error_message?: string;
}

interface AISuggestion {
    test_name: string;
    test_category: string;
    severity: string;
    sql_query: string;
    reasoning: string;
}

interface MappingResult {
    mapping_id: string;
    mapping_info?: {
        source: string;
        target: string;
        file_row_count: number;
        table_row_count: number;
    };
    predefined_results: TestResult[];
    ai_suggestions?: AISuggestion[];
    error?: string;
}

interface SummaryStats {
    total_mappings: number;
    passed: number;
    failed: number;
    errors: number;
    total_suggestions: number;
    total_tests?: number;
    ai_suggestions?: AISuggestion[];
}

const COLORS = {
    PASS: "#10b981", // Green
    FAIL: "#ef4444", // Red
    ERROR: "#f59e0b", // Amber
};

export default function ResultsView() {
    const [results, setResults] = useState<TestResult[]>([]);
    const [mappingResults, setMappingResults] = useState<MappingResult[]>([]);
    const [summary, setSummary] = useState<SummaryStats | null>(null);
    const [isConfigMode, setIsConfigMode] = useState(false);
    const [loading, setLoading] = useState(true);
    const [savedTests, setSavedTests] = useState<Set<string>>(new Set());
    const [projectId, setProjectId] = useState<string>("");
    const [expandedSql, setExpandedSql] = useState<{ mappingIdx: number, testIdx: number } | null>(null);
    const [expandedSingleSql, setExpandedSingleSql] = useState<number | null>(null);
    const [activeTab, setActiveTab] = useState<number>(0);

    useEffect(() => {
        const data = localStorage.getItem("testResults");
        if (data) {
            try {
                const parsed = JSON.parse(data);

                // Try to extract project_id from mapping info or summary if available
                if (parsed.project_id) {
                    setProjectId(parsed.project_id);
                } else if (parsed.results_by_mapping && parsed.results_by_mapping.length > 0) {
                    // Check if we can infer project_id from context, otherwise we might need it passed
                    // For now, let's assume it's in the local storage or context
                    // If not, we might fail to save. 
                    // Let's check where projectId comes from. It was in DashboardForm.
                    // We should modify DashboardForm to save projectId in local storage too or pass it.
                }



                // Check if it's config table mode (has results_by_mapping)
                if (parsed.results_by_mapping) {
                    setIsConfigMode(true);
                    setMappingResults(parsed.results_by_mapping);
                    setSummary(parsed.summary);
                } else if (parsed.results) {
                    // Single file or schema mode
                    setResults(parsed.results);
                } else if (parsed.predefined_results) {
                    // Single GCS file mode
                    setResults(parsed.predefined_results);
                    setSummary(parsed.summary);
                }
            } catch (e) {
                console.error("Failed to parse results", e);
            }
        }

        // Also try to get projectId from separate storage if not in results
        const storedProjectId = localStorage.getItem("projectId");
        if (storedProjectId) {
            setProjectId(storedProjectId);
        }

        setLoading(false);
    }, []);

    const handleSaveCustomTest = async (suggestion: AISuggestion, mappingId: string, targetDataset: string | null = null, targetTable: string | null = null) => {
        if (!projectId) {
            alert("Project ID not found. Cannot save custom test.");
            return;
        }

        try {
            const globalObj = (typeof window !== 'undefined' ? window : globalThis) as any;
            const env = globalObj.process?.env || {};
            const backendUrl = env.NEXT_PUBLIC_BACKEND_URL || 'https://data-qa-agent-backend2-1037417342779.us-central1.run.app';
            const payload = {
                project_id: projectId,
                test_name: suggestion.test_name,
                test_category: suggestion.test_category || "custom",
                severity: suggestion.severity,
                sql_query: suggestion.sql_query,
                description: suggestion.reasoning,
                target_dataset: targetDataset,
                target_table: targetTable
            };

            const response = await fetch(`${backendUrl}/api/custom-tests`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(payload),
            });

            if (!response.ok) {
                throw new Error("Failed to save custom test");
            }

            // Mark as saved
            const key = `${mappingId}-${suggestion.test_name}`;
            setSavedTests((prev: Set<string>) => new Set(prev).add(key));
            alert("Test case saved to Custom Tests successfully!");

        } catch (error) {
            console.error("Error saving custom test:", error);
            alert("Failed to save custom test.");
        }
    };

    if (loading) return <div style={{ padding: '2rem', textAlign: 'center' }}>Loading results...</div>;

    if (!isConfigMode && results.length === 0) {
        return <div style={{ padding: '2rem', textAlign: 'center' }}>No results found. Please run a test first.</div>;
    }

    if (isConfigMode && mappingResults.length === 0) {
        return <div style={{ padding: '2rem', textAlign: 'center' }}>No results found. Please run a test first.</div>;
    }

    // Config table mode - show results grouped by mapping
    if (isConfigMode) {
        return (
            <div style={{ padding: '2rem', maxWidth: '1400px', margin: '0 auto', animation: 'fadeIn 0.5s ease-out' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end', marginBottom: '2rem' }}>
                    <div>
                        <div style={{ color: 'var(--primary)', fontWeight: '700', fontSize: '0.875rem', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: '0.5rem' }}>
                            Validation Report
                        </div>
                        <h2 style={{ fontSize: '2.5rem', fontWeight: '800', margin: 0, letterSpacing: '-0.02em', background: 'var(--gradient-primary)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>
                            Test Results
                        </h2>
                    </div>
                    <div style={{
                        padding: '0.75rem 1.25rem',
                        background: 'rgba(255, 255, 255, 0.7)',
                        backdropFilter: 'blur(8px)',
                        borderRadius: 'var(--radius)',
                        border: '1px solid var(--border)',
                        fontSize: '0.9375rem',
                        fontWeight: '600',
                        color: 'var(--secondary-foreground)',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '0.75rem',
                        boxShadow: '0 4px 12px rgba(0,0,0,0.05)'
                    }}>
                        <span style={{ fontSize: '1.25rem' }}>👤</span>
                        <div>
                            <div style={{ fontSize: '0.75rem', opacity: 0.7, fontWeight: '400' }}>Triggered By</div>
                            <div>Manual Run</div>
                        </div>
                    </div>
                </div>

                {/* Overall Summary Cards */}
                {summary && (
                    <div style={{
                        display: 'grid',
                        gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))',
                        gap: '1.5rem',
                        marginBottom: '3rem'
                    }}>
                        <div className="card" style={{ padding: '1.5rem', position: 'relative', overflow: 'hidden' }}>
                            <div style={{ fontSize: '0.875rem', fontWeight: '600', color: 'var(--secondary-foreground)', opacity: 0.8 }}>Total Mappings</div>
                            <div style={{ fontSize: '2.5rem', fontWeight: '800', color: 'var(--primary)', marginTop: '0.5rem' }}>{summary.total_mappings}</div>
                            <div style={{ position: 'absolute', right: '-10px', bottom: '-10px', fontSize: '5rem', opacity: 0.05 }}>📊</div>
                        </div>
                        <div className="card" style={{ padding: '1.5rem', borderLeft: '4px solid #10b981' }}>
                            <div style={{ fontSize: '0.875rem', fontWeight: '600', color: '#10b981' }}>Tests Passed</div>
                            <div style={{ fontSize: '2.5rem', fontWeight: '800', color: '#10b981', marginTop: '0.5rem' }}>{summary.passed}</div>
                        </div>
                        <div className="card" style={{ padding: '1.5rem', borderLeft: '4px solid #ef4444' }}>
                            <div style={{ fontSize: '0.875rem', fontWeight: '600', color: '#ef4444' }}>Tests Failed</div>
                            <div style={{ fontSize: '2.5rem', fontWeight: '800', color: '#ef4444', marginTop: '0.5rem' }}>{summary.failed}</div>
                        </div>
                        {summary.errors > 0 && (
                            <div className="card" style={{ padding: '1.5rem', borderLeft: '4px solid #f59e0b' }}>
                                <div style={{ fontSize: '0.875rem', fontWeight: '600', color: '#f59e0b' }}>System Errors</div>
                                <div style={{ fontSize: '2.5rem', fontWeight: '800', color: '#f59e0b', marginTop: '0.5rem' }}>{summary.errors}</div>
                            </div>
                        )}
                    </div>
                )}

                {/* Tab Navigation */}
                <div style={{
                    display: 'flex',
                    gap: '0.75rem',
                    marginBottom: '2rem',
                    overflowX: 'auto',
                    padding: '0.5rem 0',
                    borderBottom: '1px solid var(--border)'
                }}>
                    {mappingResults.map((mapping: MappingResult, idx: number) => {
                        const failedCount = mapping.predefined_results.filter((r: TestResult) => r.status === 'FAIL').length;
                        const isActive = activeTab === idx;
                        return (
                            <button
                                key={idx}
                                onClick={() => setActiveTab(idx)}
                                style={{
                                    padding: '0.875rem 1.5rem',
                                    background: isActive ? 'var(--primary)' : 'transparent',
                                    color: isActive ? 'white' : 'var(--secondary-foreground)',
                                    border: isActive ? 'none' : '1px solid var(--border)',
                                    borderRadius: '12px',
                                    cursor: 'pointer',
                                    fontWeight: '700',
                                    fontSize: '0.9375rem',
                                    whiteSpace: 'nowrap',
                                    transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
                                    display: 'flex',
                                    alignItems: 'center',
                                    gap: '0.75rem',
                                    transform: isActive ? 'translateY(-2px)' : 'none',
                                    boxShadow: isActive ? '0 8px 16px rgba(0, 166, 126, 0.2)' : 'none'
                                }}
                            >
                                <span>{mapping.mapping_id}</span>
                                {failedCount > 0 && (
                                    <span style={{
                                        background: isActive ? 'rgba(255,255,255,0.25)' : '#fee2e2',
                                        color: isActive ? 'white' : '#991b1b',
                                        padding: '0.125rem 0.5rem',
                                        borderRadius: '9999px',
                                        fontSize: '0.75rem',
                                        fontWeight: '800'
                                    }}>
                                        {failedCount}
                                    </span>
                                )}
                            </button>
                        );
                    })}
                </div>

                {/* Active Tab Content - Result Cards */}
                {mappingResults[activeTab] && (
                    <div style={{ animation: 'fadeIn 0.4s ease-out' }}>
                        {/* Mapping Info Header */}
                        <div style={{
                            marginBottom: '2rem',
                            padding: '1.5rem',
                            background: 'var(--secondary)',
                            borderRadius: '16px',
                            border: '1px solid var(--border)',
                            display: 'flex',
                            justifyContent: 'space-between',
                            flexWrap: 'wrap',
                            gap: '1.5rem'
                        }}>
                            <div style={{ flex: '1', minWidth: '300px' }}>
                                <div style={{ fontSize: '0.75rem', textTransform: 'uppercase', color: 'var(--primary)', fontWeight: '800', marginBottom: '0.5rem', letterSpacing: '0.05em' }}>Current Mapping</div>
                                <h3 style={{ fontSize: '1.5rem', fontWeight: '800', margin: 0, color: 'var(--card-foreground)' }}>{mappingResults[activeTab].mapping_id}</h3>

                            </div>
                            <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: '2rem' }}>

                                <div>
                                    <div style={{ fontSize: '0.75rem', color: 'var(--text-secondary)', marginBottom: '0.25rem' }}>Target Dataset/Table</div>
                                    <div style={{ fontWeight: '600', fontSize: '0.9375rem' }}>{mappingResults[activeTab].mapping_info?.target || 'N/A'}</div>
                                </div>
                            </div>
                        </div>

                        {/* Result Cards List */}
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                            {mappingResults[activeTab].predefined_results.map((test: TestResult, testIdx: number) => {
                                const isExpanded = expandedSql?.mappingIdx === activeTab && expandedSql?.testIdx === testIdx;
                                return (
                                    <div key={testIdx} className="card" style={{
                                        padding: '0',
                                        overflow: 'hidden',
                                        border: `1px solid ${test.status === 'FAIL' ? '#fee2e2' : 'var(--border)'}`,
                                        background: test.status === 'FAIL' ? '#fffcfc' : 'var(--card)'
                                    }}>
                                        <div style={{
                                            padding: '1.25rem 1.5rem',
                                            display: 'grid',
                                            gridTemplateColumns: 'minmax(300px, 1fr) 120px 100px 120px auto',
                                            alignItems: 'center',
                                            gap: '1.5rem'
                                        }}>
                                            {/* Test Name & Status Icon */}
                                            <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
                                                <div style={{
                                                    width: '40px',
                                                    height: '40px',
                                                    borderRadius: '10px',
                                                    display: 'flex',
                                                    alignItems: 'center',
                                                    justifyContent: 'center',
                                                    background: test.status === 'PASS' ? 'rgba(16, 185, 129, 0.1)' : test.status === 'FAIL' ? 'rgba(239, 68, 68, 0.1)' : 'rgba(245, 158, 11, 0.1)',
                                                    color: test.status === 'PASS' ? '#10b981' : test.status === 'FAIL' ? '#ef4444' : '#f59e0b',
                                                    fontSize: '1.25rem',
                                                    fontWeight: 'bold'
                                                }}>
                                                    {test.status === 'PASS' ? '✓' : test.status === 'FAIL' ? '✕' : '!'}
                                                </div>
                                                <div style={{ flex: 1 }}>
                                                    <div style={{ fontWeight: '700', fontSize: '1rem', color: 'var(--card-foreground)' }}>{test.test_name}</div>
                                                    <div style={{ fontSize: '0.8125rem', color: 'var(--secondary-foreground)', opacity: 0.7, marginTop: '0.25rem' }}>{test.category || 'validation'}</div>
                                                </div>
                                            </div>

                                            {/* Status Badge */}
                                            <div>
                                                <span style={{
                                                    padding: '0.375rem 0.875rem',
                                                    borderRadius: '9999px',
                                                    fontSize: '0.75rem',
                                                    fontWeight: '800',
                                                    background: test.status === 'PASS' ? '#d1fae5' : test.status === 'FAIL' ? '#fee2e2' : '#fef3c7',
                                                    color: test.status === 'PASS' ? '#065f46' : test.status === 'FAIL' ? '#991b1b' : '#92400e',
                                                    border: `1px solid ${test.status === 'PASS' ? '#10b98144' : test.status === 'FAIL' ? '#ef444444' : '#f59e0b44'}`
                                                }}>
                                                    {test.status}
                                                </span>
                                            </div>

                                            {/* Severity */}
                                            <div>
                                                <div style={{ fontSize: '0.625rem', textTransform: 'uppercase', fontWeight: '800', color: 'var(--text-secondary)', marginBottom: '0.125rem' }}>Severity</div>
                                                <div style={{ fontWeight: '700', fontSize: '0.875rem', color: test.severity === 'HIGH' ? '#ef4444' : 'var(--secondary-foreground)' }}>{test.severity}</div>
                                            </div>

                                            {/* Affected Rows */}
                                            <div style={{ textAlign: 'center' }}>
                                                <div style={{ fontSize: '0.625rem', textTransform: 'uppercase', fontWeight: '800', color: 'var(--text-secondary)', marginBottom: '0.125rem' }}>Affected</div>
                                                <div style={{
                                                    fontSize: '1.125rem',
                                                    fontWeight: '800',
                                                    color: (test.rows_affected || 0) > 0 ? '#ef4444' : 'var(--secondary-foreground)'
                                                }}>
                                                    {test.rows_affected || 0}
                                                </div>
                                            </div>

                                            {/* Actions */}
                                            <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }}>
                                                <button
                                                    onClick={() => {
                                                        if (isExpanded) {
                                                            setExpandedSql(null);
                                                        } else {
                                                            setExpandedSql({ mappingIdx: activeTab, testIdx: testIdx });
                                                        }
                                                    }}
                                                    className="btn"
                                                    style={{
                                                        padding: '0.5rem 1rem',
                                                        fontSize: '0.75rem',
                                                        height: '32px',
                                                        background: isExpanded ? 'var(--primary)' : 'transparent',
                                                        color: isExpanded ? 'white' : 'var(--primary)',
                                                        border: `1px solid ${isExpanded ? 'var(--primary)' : 'var(--border)'}`,
                                                        fontWeight: '700'
                                                    }}
                                                >
                                                    {isExpanded ? 'Hide Details' : 'View SQL'}
                                                </button>
                                            </div>
                                        </div>

                                        {/* Expanded Details Section */}
                                        <div style={{
                                            maxHeight: isExpanded || (test.status === 'FAIL' && test.sample_data && test.sample_data.length > 0) ? '2000px' : '0',
                                            overflow: 'hidden',
                                            transition: 'max-height 0.4s cubic-bezier(0.4, 0, 0.2, 1)',
                                            background: 'rgba(0,0,0,0.02)',
                                            borderTop: (isExpanded || (test.status === 'FAIL' && test.sample_data && test.sample_data.length > 0)) ? '1px solid var(--border)' : 'none'
                                        }}>
                                            <div style={{ padding: '1.5rem' }}>
                                                <div style={{ marginBottom: '1.5rem' }}>
                                                    <div style={{ fontSize: '0.75rem', fontWeight: '800', color: 'var(--text-secondary)', textTransform: 'uppercase', marginBottom: '0.5rem' }}>Description</div>
                                                    <div style={{ fontSize: '0.9375rem', color: 'var(--card-foreground)', lineHeight: '1.6' }}>{test.error_message || test.description}</div>
                                                </div>

                                                {/* Sample Data Table - Always show if FAIL and has data */}
                                                {test.status === 'FAIL' && test.sample_data && test.sample_data.length > 0 && test.category !== 'smoke' && (
                                                    <div style={{ marginTop: '1.5rem' }}>
                                                        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem' }}>
                                                            <div style={{ fontSize: '0.75rem', fontWeight: '800', color: '#ef4444', textTransform: 'uppercase' }}>Sample Problematic Rows</div>
                                                            <div style={{ height: '1px', flex: 1, background: '#fee2e2' }}></div>
                                                        </div>
                                                        <div style={{
                                                            background: 'white',
                                                            borderRadius: '12px',
                                                            border: '1px solid #fee2e2',
                                                            overflowX: 'auto',
                                                            boxShadow: '0 4px 20px rgba(0,0,0,0.03)'
                                                        }}>
                                                            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8125rem' }}>
                                                                <thead>
                                                                    <tr style={{ background: '#f8fafc', borderBottom: '1px solid #e2e8f0' }}>
                                                                        {Object.keys(test.sample_data[0]).map((key: string) => (
                                                                            <th key={key} style={{ padding: '1rem', textAlign: 'left', fontWeight: '700', color: '#475569', whiteSpace: 'nowrap' }}>{key}</th>
                                                                        ))}
                                                                    </tr>
                                                                </thead>
                                                                <tbody>
                                                                    {test.sample_data.map((row: Record<string, any>, rIdx: number) => (
                                                                        <tr key={rIdx} style={{ borderBottom: '1px solid #f1f5f9', transition: 'background 0.2s' }}>
                                                                            {Object.values(row).map((val: any, vIdx: number) => (
                                                                                <td key={vIdx} style={{ padding: '0.875rem 1rem', color: '#334155', whiteSpace: 'nowrap' }}>
                                                                                    {val === null ? <em style={{ color: '#94a3b8' }}>NULL</em> : val.toString()}
                                                                                </td>
                                                                            ))}
                                                                        </tr>
                                                                    ))}
                                                                </tbody>
                                                            </table>
                                                        </div>
                                                    </div>
                                                )}

                                                {/* SQL View */}
                                                {isExpanded && (
                                                    <div style={{ marginTop: '1.5rem' }}>
                                                        <div style={{ fontSize: '0.75rem', fontWeight: '800', color: 'var(--primary)', textTransform: 'uppercase', marginBottom: '0.75rem' }}>SQL Query</div>
                                                        <pre style={{
                                                            padding: '1.5rem',
                                                            background: '#0f172a',
                                                            borderRadius: '12px',
                                                            overflowX: 'auto',
                                                            fontSize: '0.8125rem',
                                                            color: '#e2e8f0',
                                                            borderLeft: '4px solid var(--primary)',
                                                            fontFamily: '"JetBrains Mono", monospace',
                                                            lineHeight: '1.7',
                                                            boxShadow: 'inset 0 2px 4px rgba(0,0,0,0.2)'
                                                        }}>
                                                            {test.sql_query}
                                                        </pre>
                                                    </div>
                                                )}
                                            </div>
                                        </div>
                                    </div>
                                );
                            })}
                        </div>

                        {/* AI Suggestions Section */}
                        {mappingResults[activeTab].ai_suggestions && mappingResults[activeTab].ai_suggestions.length > 0 && (
                            <div style={{ marginTop: '3rem' }}>
                                <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '1.5rem' }}>
                                    <span style={{ fontSize: '1.5rem' }}>🤖</span>
                                    <h4 style={{ fontSize: '1.5rem', fontWeight: '800', margin: 0 }}>AI Suggested Tests <span style={{ color: 'var(--primary)', opacity: 0.6 }}>({mappingResults[activeTab].ai_suggestions.length})</span></h4>
                                </div>
                                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(400px, 1fr))', gap: '1rem' }}>
                                    {mappingResults[activeTab].ai_suggestions?.map((suggestion: AISuggestion, sugIdx: number) => {
                                        const isSaved = savedTests.has(`${mappingResults[activeTab].mapping_id}-${suggestion.test_name}`);
                                        return (
                                            <div key={sugIdx} style={{
                                                padding: '1.5rem',
                                                background: 'white',
                                                borderRadius: '16px',
                                                border: '2px dashed var(--primary)',
                                                display: 'flex',
                                                flexDirection: 'column',
                                                justifyContent: 'space-between',
                                                transition: 'all 0.3s ease',
                                                position: 'relative',
                                                overflow: 'hidden'
                                            }}>
                                                <div style={{ position: 'absolute', top: '0', right: '0', padding: '0.5rem 0.75rem', background: 'var(--primary)', color: 'white', fontSize: '0.625rem', fontWeight: '800', borderBottomLeftRadius: '12px' }}>AI RECOMMENDATION</div>
                                                <div>
                                                    <div style={{ fontWeight: '800', fontSize: '1.125rem', marginBottom: '0.75rem', color: 'var(--card-foreground)' }}>{suggestion.test_name}</div>
                                                    <div style={{ fontSize: '0.9375rem', color: 'var(--secondary-foreground)', marginBottom: '1rem', lineHeight: '1.5' }}>
                                                        {suggestion.reasoning}
                                                    </div>
                                                    <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap', marginBottom: '1.5rem' }}>
                                                        <span style={{ padding: '0.25rem 0.625rem', background: 'var(--secondary)', borderRadius: '6px', fontSize: '0.75rem', fontWeight: '700', color: 'var(--primary)' }}>
                                                            Severity: {suggestion.severity}
                                                        </span>
                                                        <span style={{ padding: '0.25rem 0.625rem', background: 'rgba(0,166,126,0.05)', borderRadius: '6px', fontSize: '0.75rem', fontWeight: '700', color: 'var(--primary)' }}>
                                                            {suggestion.test_category}
                                                        </span>
                                                    </div>
                                                </div>
                                                <button
                                                    onClick={() => {
                                                        let targetDataset = null;
                                                        let targetTable = null;
                                                        const currentMapping = mappingResults[activeTab];
                                                        if (currentMapping.mapping_info && currentMapping.mapping_info.target) {
                                                            const parts = currentMapping.mapping_info.target.split('.');
                                                            if (parts.length === 2) {
                                                                targetDataset = parts[0];
                                                                targetTable = parts[1];
                                                            }
                                                        }
                                                        handleSaveCustomTest(suggestion, currentMapping.mapping_id, targetDataset, targetTable);
                                                    }}
                                                    disabled={isSaved}
                                                    className="btn btn-primary"
                                                    style={{
                                                        width: '100%',
                                                        padding: '0.75rem',
                                                        fontSize: '0.875rem',
                                                        opacity: isSaved ? 0.7 : 1,
                                                        background: isSaved ? '#10b981' : 'var(--gradient-primary)'
                                                    }}
                                                >
                                                    {isSaved ? '✓ Added to Custom Tests' : 'Implement this Test'}
                                                </button>
                                            </div>
                                        );
                                    })}
                                </div>
                            </div>
                        )}
                    </div>
                )}
            </div>
        );
    }


    // Single file/schema mode - revamped display
    const stats = {
        PASS: results.filter((r: TestResult) => r.status === "PASS").length,
        FAIL: results.filter((r: TestResult) => r.status === "FAIL").length,
        ERROR: results.filter((r: TestResult) => r.status === "ERROR").length,
    };

    const chartData = [
        { name: "Pass", value: stats.PASS },
        { name: "Fail", value: stats.FAIL },
        { name: "Error", value: stats.ERROR },
    ];

    return (
        <div style={{ padding: '2rem', maxWidth: '1400px', margin: '0 auto', animation: 'fadeIn 0.5s ease-out' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end', marginBottom: '2rem' }}>
                <div>
                    <button
                        onClick={() => window.location.href = '/'} // Simple navigation back to home/history
                        className="btn btn-outline"
                        style={{
                            marginBottom: '1rem',
                            border: 'none',
                            padding: '0',
                            display: 'flex',
                            alignItems: 'center',
                            gap: '0.5rem',
                            color: 'var(--secondary-foreground)'
                        }}
                    >
                        <span>←</span> Back to History
                    </button>
                    <div style={{ color: 'var(--primary)', fontWeight: '700', fontSize: '0.875rem', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: '0.5rem' }}>
                        Validation Report
                    </div>
                    <h2 style={{ fontSize: '2.5rem', fontWeight: '800', margin: 0, letterSpacing: '-0.02em', background: 'var(--gradient-primary)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>
                        Test Results
                    </h2>
                </div>
                <div style={{
                    padding: '0.75rem 1.25rem',
                    background: 'rgba(255, 255, 255, 0.7)',
                    backdropFilter: 'blur(8px)',
                    borderRadius: 'var(--radius)',
                    border: '1px solid var(--border)',
                    fontSize: '0.9375rem',
                    fontWeight: '600',
                    color: 'var(--secondary-foreground)',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.75rem',
                    boxShadow: '0 4px 12px rgba(0,0,0,0.05)'
                }}>
                    <span style={{ fontSize: '1.25rem' }}>👤</span>
                    <div>
                        <div style={{ fontSize: '0.75rem', opacity: 0.7, fontWeight: '400' }}>Triggered By</div>
                        <div>Manual Run</div>
                    </div>
                </div>
            </div>

            {/* Summary Grid */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: '1.5rem', marginBottom: '2rem' }}>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
                    <div className="card" style={{ padding: '1.5rem', borderLeft: '4px solid #10b981' }}>
                        <div style={{ fontSize: '0.875rem', fontWeight: '600', color: '#10b981' }}>Passed</div>
                        <div style={{ fontSize: '2.5rem', fontWeight: '800', color: '#10b981', marginTop: '0.5rem' }}>{stats.PASS}</div>
                    </div>
                    <div className="card" style={{ padding: '1.5rem', borderLeft: '4px solid #ef4444' }}>
                        <div style={{ fontSize: '0.875rem', fontWeight: '600', color: '#ef4444' }}>Failed</div>
                        <div style={{ fontSize: '2.5rem', fontWeight: '800', color: '#ef4444', marginTop: '0.5rem' }}>{stats.FAIL}</div>
                    </div>
                    <div className="card" style={{ padding: '1.5rem', borderLeft: '4px solid #f59e0b' }}>
                        <div style={{ fontSize: '0.875rem', fontWeight: '600', color: '#f59e0b' }}>Errors</div>
                        <div style={{ fontSize: '2.5rem', fontWeight: '800', color: '#f59e0b', marginTop: '0.5rem' }}>{stats.ERROR}</div>
                    </div>
                </div>

                {/* Pie Chart Card */}
                <div className="card" style={{ padding: '1.5rem', display: 'flex', flexDirection: 'column' }}>
                    <h3 style={{ fontSize: '1.125rem', fontWeight: '700', marginBottom: '1.5rem', color: 'var(--card-foreground)' }}>Distribution</h3>
                    <div style={{ flex: 1, minHeight: '250px' }}>
                        <ResponsiveContainer width="100%" height="100%">
                            <PieChart>
                                <Pie data={chartData} dataKey="value" nameKey="name" cx="50%" cy="50%" innerRadius={60} outerRadius={80} paddingAngle={5} label>
                                    {chartData.map((entry, index) => (
                                        <Cell key={`cell-${index}`} fill={COLORS[entry.name.toUpperCase() as keyof typeof COLORS]} />
                                    ))}
                                </Pie>
                                <Tooltip contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgba(0,0,0,0.1)' }} />
                                <Legend verticalAlign="bottom" height={36} />
                            </PieChart>
                        </ResponsiveContainer>
                    </div>
                </div>
            </div>

            {/* Detailed Results List */}
            <div style={{ marginBottom: '3rem' }}>
                <h3 style={{ fontSize: '1.5rem', fontWeight: '800', marginBottom: '1.5rem', color: 'var(--card-foreground)' }}>Detailed Results</h3>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                    {results.map((test: TestResult, index: number) => {
                        const isExpanded = expandedSingleSql === index;
                        return (
                            <div key={index} className="card" style={{
                                padding: '0',
                                overflow: 'hidden',
                                border: `1px solid ${test.status === 'FAIL' ? '#fee2e2' : 'var(--border)'}`,
                                background: test.status === 'FAIL' ? '#fffcfc' : 'var(--card)'
                            }}>
                                <div style={{
                                    padding: '1.25rem 1.5rem',
                                    display: 'grid',
                                    gridTemplateColumns: 'minmax(300px, 1fr) 120px 120px auto',
                                    alignItems: 'center',
                                    gap: '1.5rem'
                                }}>
                                    {/* Test Name */}
                                    <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
                                        <div style={{
                                            width: '40px',
                                            height: '40px',
                                            borderRadius: '10px',
                                            display: 'flex',
                                            alignItems: 'center',
                                            justifyContent: 'center',
                                            background: test.status === 'PASS' ? 'rgba(16, 185, 129, 0.1)' : test.status === 'FAIL' ? 'rgba(239, 68, 68, 0.1)' : 'rgba(245, 158, 11, 0.1)',
                                            color: test.status === 'PASS' ? '#10b981' : test.status === 'FAIL' ? '#ef4444' : '#f59e0b',
                                            fontSize: '1.25rem',
                                            fontWeight: 'bold'
                                        }}>
                                            {test.status === 'PASS' ? '✓' : test.status === 'FAIL' ? '✕' : '!'}
                                        </div>
                                        <div style={{ flex: 1 }}>
                                            <div style={{ fontWeight: '700', fontSize: '1rem', color: 'var(--card-foreground)' }}>{test.test_name}</div>
                                            <div style={{ fontSize: '0.8125rem', color: 'var(--secondary-foreground)', opacity: 0.7, marginTop: '0.25rem' }}>{test.category || 'validation'}</div>
                                        </div>
                                    </div>

                                    {/* Status Badge */}
                                    <div>
                                        <span style={{
                                            padding: '0.375rem 0.875rem',
                                            borderRadius: '9999px',
                                            fontSize: '0.75rem',
                                            fontWeight: '800',
                                            background: test.status === 'PASS' ? '#d1fae5' : test.status === 'FAIL' ? '#fee2e2' : '#fef3c7',
                                            color: test.status === 'PASS' ? '#065f46' : test.status === 'FAIL' ? '#991b1b' : '#92400e',
                                            border: `1px solid ${test.status === 'PASS' ? '#10b98144' : test.status === 'FAIL' ? '#ef444444' : '#f59e0b44'}`
                                        }}>
                                            {test.status}
                                        </span>
                                    </div>

                                    {/* Severity */}
                                    <div>
                                        <div style={{ fontSize: '0.625rem', textTransform: 'uppercase', fontWeight: '800', color: 'var(--text-secondary)', marginBottom: '0.125rem' }}>Severity</div>
                                        <div style={{ fontWeight: '700', fontSize: '0.875rem', color: test.severity === 'HIGH' ? '#ef4444' : 'var(--secondary-foreground)' }}>{test.severity}</div>
                                    </div>

                                    {/* Actions */}
                                    <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }}>
                                        <button
                                            onClick={() => setExpandedSingleSql(isExpanded ? null : index)}
                                            className="btn"
                                            style={{
                                                padding: '0.5rem 1rem',
                                                fontSize: '0.75rem',
                                                height: '32px',
                                                background: isExpanded ? 'var(--primary)' : 'transparent',
                                                color: isExpanded ? 'white' : 'var(--primary)',
                                                border: `1px solid ${isExpanded ? 'var(--primary)' : 'var(--border)'}`,
                                                fontWeight: '700'
                                            }}
                                        >
                                            {isExpanded ? 'Hide Details' : 'View SQL'}
                                        </button>
                                    </div>
                                </div>

                                {/* Expanded Details Section */}
                                <div style={{
                                    maxHeight: isExpanded || (test.status === 'FAIL' && test.sample_data && test.sample_data.length > 0) ? '2000px' : '0',
                                    overflow: 'hidden',
                                    transition: 'max-height 0.4s cubic-bezier(0.4, 0, 0.2, 1)',
                                    background: 'rgba(0,0,0,0.02)',
                                    borderTop: (isExpanded || (test.status === 'FAIL' && test.sample_data && test.sample_data.length > 0)) ? '1px solid var(--border)' : 'none'
                                }}>
                                    <div style={{ padding: '1.5rem' }}>
                                        <div style={{ marginBottom: '1.5rem' }}>
                                            <div style={{ fontSize: '0.75rem', fontWeight: '800', color: 'var(--text-secondary)', textTransform: 'uppercase', marginBottom: '0.5rem' }}>Description</div>
                                            <div style={{ fontSize: '0.9375rem', color: 'var(--card-foreground)', lineHeight: '1.6' }}>{test.error_message || test.description}</div>
                                        </div>

                                        {/* Sample Data Table */}
                                        {test.status === 'FAIL' && test.sample_data && test.sample_data.length > 0 && test.category !== 'smoke' && (
                                            <div style={{ marginTop: '1.5rem' }}>
                                                <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem' }}>
                                                    <div style={{ fontSize: '0.75rem', fontWeight: '800', color: '#ef4444', textTransform: 'uppercase' }}>Sample Problematic Rows</div>
                                                    <div style={{ height: '1px', flex: 1, background: '#fee2e2' }}></div>
                                                </div>
                                                <div style={{
                                                    background: 'white',
                                                    borderRadius: '12px',
                                                    border: '1px solid #fee2e2',
                                                    overflowX: 'auto',
                                                    boxShadow: '0 4px 20px rgba(0,0,0,0.03)'
                                                }}>
                                                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8125rem' }}>
                                                        <thead>
                                                            <tr style={{ background: '#f8fafc', borderBottom: '1px solid #e2e8f0' }}>
                                                                {Object.keys(test.sample_data[0]).map((key: string) => (
                                                                    <th key={key} style={{ padding: '1rem', textAlign: 'left', fontWeight: '700', color: '#475569', whiteSpace: 'nowrap' }}>{key}</th>
                                                                ))}
                                                            </tr>
                                                        </thead>
                                                        <tbody>
                                                            {test.sample_data.map((row: Record<string, any>, rIdx: number) => (
                                                                <tr key={rIdx} style={{ borderBottom: '1px solid #f1f5f9' }}>
                                                                    {Object.values(row).map((val: any, vIdx: number) => (
                                                                        <td key={vIdx} style={{ padding: '0.875rem 1rem', color: '#334155', whiteSpace: 'nowrap' }}>{val?.toString() || 'NULL'}</td>
                                                                    ))}
                                                                </tr>
                                                            ))}
                                                        </tbody>
                                                    </table>
                                                </div>
                                            </div>
                                        )}

                                        {isExpanded && (
                                            <div style={{ marginTop: '1.5rem' }}>
                                                <div style={{ fontSize: '0.75rem', fontWeight: '800', color: 'var(--primary)', textTransform: 'uppercase', marginBottom: '0.75rem' }}>SQL Query</div>
                                                <pre style={{
                                                    padding: '1.5rem',
                                                    background: '#0f172a',
                                                    borderRadius: '12px',
                                                    overflowX: 'auto',
                                                    fontSize: '0.8125rem',
                                                    color: '#e2e8f0',
                                                    borderLeft: '4px solid var(--primary)',
                                                    fontFamily: '"JetBrains Mono", monospace',
                                                    lineHeight: '1.7'
                                                }}>
                                                    {test.sql_query}
                                                </pre>
                                            </div>
                                        )}
                                    </div>
                                </div>
                            </div>
                        );
                    })}
                </div>
            </div>

            {/* AI Suggestions (Single File Mode) */}
            {summary && summary.ai_suggestions && summary.ai_suggestions.length > 0 && (
                <div style={{ marginTop: '3rem' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '1.5rem' }}>
                        <span style={{ fontSize: '1.5rem' }}>🤖</span>
                        <h4 style={{ fontSize: '1.5rem', fontWeight: '800', margin: 0 }}>AI Suggested Tests <span style={{ color: 'var(--primary)', opacity: 0.6 }}>({summary.ai_suggestions.length})</span></h4>
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(400px, 1fr))', gap: '1rem' }}>
                        {summary.ai_suggestions.map((suggestion: AISuggestion, idx: number) => {
                            const isSaved = savedTests.has(`single-${suggestion.test_name}`);
                            return (
                                <div key={idx} style={{
                                    padding: '1.5rem',
                                    background: 'white',
                                    borderRadius: '16px',
                                    border: '2px dashed var(--primary)',
                                    display: 'flex',
                                    flexDirection: 'column',
                                    justifyContent: 'space-between',
                                    position: 'relative'
                                }}>
                                    <div>
                                        <div style={{ fontWeight: '800', fontSize: '1.125rem', marginBottom: '0.75rem', color: 'var(--card-foreground)' }}>{suggestion.test_name}</div>
                                        <div style={{ fontSize: '0.9375rem', color: 'var(--secondary-foreground)', marginBottom: '1rem', lineHeight: '1.5' }}>
                                            {suggestion.reasoning}
                                        </div>
                                        <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap', marginBottom: '1.5rem' }}>
                                            <span style={{ padding: '0.25rem 0.625rem', background: 'var(--secondary)', borderRadius: '6px', fontSize: '0.75rem', fontWeight: '700', color: 'var(--primary)' }}>
                                                Severity: {suggestion.severity}
                                            </span>
                                        </div>
                                    </div>
                                    <button
                                        onClick={() => handleSaveCustomTest(suggestion, 'single')}
                                        disabled={isSaved}
                                        className="btn btn-primary"
                                        style={{
                                            width: '100%',
                                            padding: '0.75rem',
                                            fontSize: '0.875rem',
                                            background: isSaved ? '#10b981' : 'var(--gradient-primary)'
                                        }}
                                    >
                                        {isSaved ? '✓ Added' : 'Implement this Test'}
                                    </button>
                                </div>
                            );
                        })}
                    </div>
                </div>
            )}
        </div>
    );
}
