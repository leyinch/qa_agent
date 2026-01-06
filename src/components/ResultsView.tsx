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
    const [expandedData, setExpandedData] = useState<{ mappingIdx: number, testIdx: number } | null>(null);
    const [expandedSingleSql, setExpandedSingleSql] = useState<number | null>(null);
    const [expandedSingleData, setExpandedSingleData] = useState<number | null>(null);
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
            const backendUrl = env.NEXT_PUBLIC_BACKEND_URL || 'https://data-qa-agent-backend-1037417342779.us-central1.run.app';
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
            <div style={{ padding: '2rem', maxWidth: '1200px', margin: '0 auto' }}>
                <h2 style={{ fontSize: '2rem', fontWeight: '700', marginBottom: '1rem' }}>
                    Test Results - Config Table Mode
                </h2>

                {/* Overall Summary */}
                {summary && (
                    <div style={{
                        display: 'grid',
                        gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
                        gap: '1rem',
                        marginBottom: '2rem'
                    }}>
                        <div className="card" style={{ textAlign: 'center' }}>
                            <div style={{ fontSize: '2rem', fontWeight: '700', color: 'var(--primary)' }}>
                                {summary.total_mappings}
                            </div>
                            <div style={{ color: 'var(--secondary-foreground)' }}>Total Mappings</div>
                        </div>
                        <div className="card" style={{ textAlign: 'center' }}>
                            <div style={{ fontSize: '2rem', fontWeight: '700', color: '#10b981' }}>
                                {summary.passed}
                            </div>
                            <div style={{ color: 'var(--secondary-foreground)' }}>Tests Passed</div>
                        </div>
                        <div className="card" style={{ textAlign: 'center' }}>
                            <div style={{ fontSize: '2rem', fontWeight: '700', color: '#ef4444' }}>
                                {summary.failed}
                            </div>
                            <div style={{ color: 'var(--secondary-foreground)' }}>Tests Failed</div>
                        </div>
                        <div className="card" style={{ textAlign: 'center' }}>
                            <div style={{ fontSize: '2rem', fontWeight: '700', color: '#f59e0b' }}>
                                {summary.errors}
                            </div>
                            <div style={{ color: 'var(--secondary-foreground)' }}>Errors</div>
                        </div>
                        {summary.total_suggestions > 0 && (
                            <div className="card" style={{ textAlign: 'center' }}>
                                <div style={{ fontSize: '2rem', fontWeight: '700', color: 'var(--primary)' }}>
                                    {summary.total_suggestions}
                                </div>
                                <div style={{ color: 'var(--secondary-foreground)' }}>AI Suggestions</div>
                            </div>
                        )}
                    </div>
                )}

                {/* Tab Headers */}
                <div style={{
                    display: 'flex',
                    gap: '0.5rem',
                    marginBottom: '1rem',
                    overflowX: 'auto',
                    paddingBottom: '0.5rem',
                    borderBottom: '1px solid var(--border)'
                }}>
                    {mappingResults.map((mapping: MappingResult, idx: number) => {
                        const mappingStats = {
                            FAIL: mapping.predefined_results.filter((r: TestResult) => r.status === 'FAIL').length,
                        };
                        return (
                            <button
                                key={idx}
                                onClick={() => setActiveTab(idx)}
                                style={{
                                    padding: '0.75rem 1.25rem',
                                    background: activeTab === idx ? 'var(--primary)' : 'var(--secondary)',
                                    color: activeTab === idx ? 'white' : 'var(--secondary-foreground)',
                                    border: 'none',
                                    borderRadius: 'var(--radius)',
                                    cursor: 'pointer',
                                    fontWeight: '600',
                                    whiteSpace: 'nowrap',
                                    transition: 'all 0.2s',
                                    boxShadow: activeTab === idx ? '0 2px 4px rgba(0,0,0,0.1)' : 'none'
                                }}
                            >
                                {mapping.mapping_id}
                                {mappingStats.FAIL > 0 && (
                                    <span style={{
                                        marginLeft: '0.5rem',
                                        background: activeTab === idx ? 'rgba(255,255,255,0.2)' : '#fee2e2',
                                        color: activeTab === idx ? 'white' : '#991b1b',
                                        padding: '0.125rem 0.375rem',
                                        borderRadius: '9999px',
                                        fontSize: '0.75rem'
                                    }}>
                                        {mappingStats.FAIL}
                                    </span>
                                )}
                            </button>
                        );
                    })}
                </div>

                {/* Active Tab Content */}
                {mappingResults[activeTab] && (
                    <div className="card" style={{ padding: '1.5rem' }}>
                        <div style={{ marginBottom: '1.5rem', borderBottom: '1px solid var(--border)', paddingBottom: '1rem' }}>
                            <h3 style={{ fontSize: '1.25rem', fontWeight: '700', color: 'var(--primary)' }}>
                                Mapping: {mappingResults[activeTab].mapping_id}
                            </h3>
                            {mappingResults[activeTab].mapping_info && (
                                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', marginTop: '0.5rem', fontSize: '0.875rem' }}>
                                    <div><strong>Source:</strong> {mappingResults[activeTab].mapping_info.source}</div>
                                    <div><strong>Target:</strong> {mappingResults[activeTab].mapping_info.target}</div>
                                </div>
                            )}
                        </div>

                        {/* Test Results Table */}
                        <div style={{ overflowX: 'auto' }}>
                            <table style={{ width: '100%', borderCollapse: 'collapse', tableLayout: 'fixed' }}>
                                <thead>
                                    <tr style={{ borderBottom: '2px solid var(--border)' }}>
                                        <th style={{ padding: '0.75rem', textAlign: 'left', width: '15%' }}>Test Name</th>
                                        <th style={{ padding: '0.75rem', textAlign: 'left', width: '10%' }}>Status</th>
                                        <th style={{ padding: '0.75rem', textAlign: 'left', width: '10%' }}>Severity</th>
                                        <th style={{ padding: '0.75rem', textAlign: 'left', width: '8%' }}>Affected</th>
                                        <th style={{ padding: '0.75rem', textAlign: 'left', width: '57%' }}>Details</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {mappingResults[activeTab].predefined_results.map((test: TestResult, testIdx: number) => (
                                        <tr key={testIdx} style={{ borderBottom: '1px solid var(--border)' }}>
                                            <td style={{ padding: '0.75rem' }}>{test.test_name}</td>
                                            <td style={{ padding: '0.75rem' }}>
                                                <span style={{
                                                    padding: '0.25rem 0.75rem',
                                                    borderRadius: '9999px',
                                                    fontSize: '0.875rem',
                                                    fontWeight: '600',
                                                    background: test.status === 'PASS' ? '#d1fae5' : test.status === 'FAIL' ? '#fee2e2' : '#fef3c7',
                                                    color: test.status === 'PASS' ? '#065f46' : test.status === 'FAIL' ? '#991b1b' : '#92400e'
                                                }}>
                                                    {test.status}
                                                </span>
                                            </td>
                                            <td style={{ padding: '0.75rem' }}>{test.severity}</td>
                                            <td style={{ padding: '0.75rem' }}>{test.rows_affected || 0}</td>
                                            <td style={{ padding: '0.75rem', fontSize: '0.875rem' }}>
                                                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                                                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                                                        <span>{test.error_message || test.description}</span>
                                                        <div style={{ display: 'flex', gap: '0.5rem' }}>
                                                            {test.status === 'FAIL' && test.sample_data && test.category !== 'smoke' && (
                                                                <button
                                                                    onClick={() => {
                                                                        if (expandedData?.mappingIdx === activeTab && expandedData?.testIdx === testIdx) {
                                                                            setExpandedData(null);
                                                                        } else {
                                                                            setExpandedData({ mappingIdx: activeTab, testIdx: testIdx });
                                                                        }
                                                                    }}
                                                                    style={{
                                                                        fontSize: '0.75rem',
                                                                        background: '#3b82f6',
                                                                        color: 'white',
                                                                        border: 'none',
                                                                        borderRadius: '4px',
                                                                        padding: '2px 8px',
                                                                        cursor: 'pointer',
                                                                        whiteSpace: 'nowrap'
                                                                    }}
                                                                >
                                                                    {expandedData?.mappingIdx === activeTab && expandedData?.testIdx === testIdx ? 'Hide Data' : 'View Bad Data'}
                                                                </button>
                                                            )}
                                                            <button
                                                                onClick={() => {
                                                                    if (expandedSql?.mappingIdx === activeTab && expandedSql?.testIdx === testIdx) {
                                                                        setExpandedSql(null);
                                                                    } else {
                                                                        setExpandedSql({ mappingIdx: activeTab, testIdx: testIdx });
                                                                    }
                                                                }}
                                                                style={{
                                                                    fontSize: '0.75rem',
                                                                    background: 'var(--secondary)',
                                                                    color: 'var(--secondary-foreground)',
                                                                    border: '1px solid var(--border)',
                                                                    borderRadius: '4px',
                                                                    padding: '2px 8px',
                                                                    cursor: 'pointer',
                                                                    whiteSpace: 'nowrap'
                                                                }}
                                                            >
                                                                {expandedSql?.mappingIdx === activeTab && expandedSql?.testIdx === testIdx ? 'Hide SQL' : 'Show SQL'}
                                                            </button>
                                                        </div>
                                                    </div>

                                                    {test.status === 'FAIL' && test.sample_data && test.sample_data.length > 0 && (
                                                        <>
                                                            <div style={{ fontWeight: '700', color: '#1d4ed8', marginBottom: '0.5rem', fontSize: '0.75rem' }}>
                                                                Sample problematic rows (max 10):
                                                            </div>
                                                            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.75rem' }}>
                                                                <thead>
                                                                    <tr style={{ background: '#f8fafc', borderBottom: '1px solid #e2e8f0' }}>
                                                                        {Object.keys(test.sample_data[0]).map((key: string) => (
                                                                            <th key={key} style={{ padding: '0.25rem 0.5rem', textAlign: 'left' }}>{key}</th>
                                                                        ))}
                                                                    </tr>
                                                                </thead>
                                                                <tbody>
                                                                    {test.sample_data.map((row: Record<string, any>, rIdx: number) => (
                                                                        <tr key={rIdx} style={{ borderBottom: '1px solid #f1f5f9' }}>
                                                                            {Object.values(row).map((val: any, vIdx: number) => (
                                                                                <td key={vIdx} style={{ padding: '0.25rem 0.5rem' }}>{val?.toString() || 'NULL'}</td>
                                                                            ))}
                                                                        </tr>
                                                                    ))}
                                                                </tbody>
                                                            </table>
                                                        </>
                                                    )}

                                                    {expandedSql?.mappingIdx === activeTab && expandedSql?.testIdx === testIdx && (
                                                        <pre style={{
                                                            marginTop: '0.5rem',
                                                            padding: '0.75rem',
                                                            background: '#1e293b',
                                                            borderRadius: '4px',
                                                            overflowX: 'auto',
                                                            fontSize: '0.75rem',
                                                            color: '#f8fafc',
                                                            borderLeft: '4px solid #3b82f6',
                                                            fontFamily: 'monospace',
                                                            whiteSpace: 'pre-wrap'
                                                        }}>
                                                            {test.sql_query}
                                                        </pre>
                                                    )}
                                                </div>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>

                        {/* AI Suggestions */}
                        {mappingResults[activeTab].ai_suggestions && mappingResults[activeTab].ai_suggestions.length > 0 && (
                            <div style={{ marginTop: '1.5rem' }}>
                                <h4 style={{ fontSize: '1.25rem', fontWeight: '600', marginBottom: '1rem' }}>
                                    🤖 AI Suggested Tests ({mappingResults[activeTab].ai_suggestions.length})
                                </h4>
                                {mappingResults[activeTab].ai_suggestions?.map((suggestion: AISuggestion, sugIdx: number) => {
                                    const isSaved = savedTests.has(`${mappingResults[activeTab].mapping_id}-${suggestion.test_name}`);
                                    return (
                                        <div key={sugIdx} style={{
                                            padding: '1rem',
                                            background: 'var(--secondary)',
                                            borderRadius: 'var(--radius)',
                                            marginBottom: '0.75rem',
                                            border: '2px dashed var(--primary)',
                                            display: 'flex',
                                            justifyContent: 'space-between',
                                            alignItems: 'flex-start'
                                        }}>
                                            <div>
                                                <div style={{ fontWeight: '600', marginBottom: '0.5rem' }}>{suggestion.test_name}</div>
                                                <div style={{ fontSize: '0.875rem', color: 'var(--secondary-foreground)', marginBottom: '0.5rem' }}>
                                                    {suggestion.reasoning}
                                                </div>
                                                <div style={{ fontSize: '0.75rem', color: 'var(--secondary-foreground)' }}>
                                                    Severity: {suggestion.severity}
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
                                                style={{
                                                    padding: '0.5rem 1rem',
                                                    backgroundColor: isSaved ? '#10b981' : 'var(--primary)',
                                                    color: 'white',
                                                    border: 'none',
                                                    borderRadius: 'var(--radius)',
                                                    cursor: isSaved ? 'default' : 'pointer',
                                                    fontSize: '0.875rem',
                                                    fontWeight: '600',
                                                    whiteSpace: 'nowrap',
                                                    marginLeft: '1rem',
                                                    opacity: isSaved ? 0.7 : 1
                                                }}
                                            >
                                                {isSaved ? '✓ Added' : '+ Add to Custom'}
                                            </button>
                                        </div>
                                    );
                                })}
                            </div>
                        )}
                    </div>
                )}
            </div>
        );
    }

    // Single file/schema mode - original display
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
        <div style={{ padding: '2rem', maxWidth: '1200px', margin: '0 auto' }}>
            <h2 style={{ fontSize: '2rem', fontWeight: '700', marginBottom: '2rem' }}>Test Results</h2>

            {/* Summary Cards */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '1rem', marginBottom: '2rem' }}>
                <div className="card" style={{ textAlign: 'center' }}>
                    <div style={{ fontSize: '2rem', fontWeight: '700', color: '#10b981' }}>{stats.PASS}</div>
                    <div style={{ color: 'var(--secondary-foreground)' }}>Passed</div>
                </div>
                <div className="card" style={{ textAlign: 'center' }}>
                    <div style={{ fontSize: '2rem', fontWeight: '700', color: '#ef4444' }}>{stats.FAIL}</div>
                    <div style={{ color: 'var(--secondary-foreground)' }}>Failed</div>
                </div>
                <div className="card" style={{ textAlign: 'center' }}>
                    <div style={{ fontSize: '2rem', fontWeight: '700', color: '#f59e0b' }}>{stats.ERROR}</div>
                    <div style={{ color: 'var(--secondary-foreground)' }}>Errors</div>
                </div>
            </div>

            {/* Pie Chart */}
            <div className="card" style={{ marginBottom: '2rem' }}>
                <h3 style={{ fontSize: '1.25rem', fontWeight: '600', marginBottom: '1rem' }}>Test Distribution</h3>
                <ResponsiveContainer width="100%" height={300}>
                    <PieChart>
                        <Pie data={chartData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={100} label>
                            {chartData.map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={COLORS[entry.name.toUpperCase() as keyof typeof COLORS]} />
                            ))}
                        </Pie>
                        <Tooltip />
                        <Legend />
                    </PieChart>
                </ResponsiveContainer>
            </div>

            {/* Detailed Results Table */}
            <div className="card">
                <h3 style={{ fontSize: '1.25rem', fontWeight: '600', marginBottom: '1rem' }}>Detailed Results</h3>
                <div style={{ overflowX: 'auto' }}>
                    <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                        <thead>
                            <tr style={{ borderBottom: '2px solid var(--border)' }}>
                                <th style={{ padding: '0.75rem', textAlign: 'left' }}>Test Name</th>
                                <th style={{ padding: '0.75rem', textAlign: 'left' }}>Status</th>
                                <th style={{ padding: '0.75rem', textAlign: 'left' }}>Severity</th>
                                <th style={{ padding: '0.75rem', textAlign: 'left' }}>Details</th>
                            </tr>
                        </thead>
                        <tbody>
                            {results.map((test: TestResult, index: number) => (
                                <tr key={index} style={{ borderBottom: '1px solid var(--border)' }}>
                                    <td style={{ padding: '0.75rem' }}>{test.test_name}</td>
                                    <td style={{ padding: '0.75rem' }}>
                                        <span style={{
                                            padding: '0.25rem 0.75rem',
                                            borderRadius: '9999px',
                                            fontSize: '0.875rem',
                                            fontWeight: '600',
                                            background: test.status === 'PASS' ? '#d1fae5' : test.status === 'FAIL' ? '#fee2e2' : '#fef3c7',
                                            color: test.status === 'PASS' ? '#065f46' : test.status === 'FAIL' ? '#991b1b' : '#92400e'
                                        }}>
                                            {test.status}
                                        </span>
                                    </td>
                                    <td style={{ padding: '0.75rem' }}>{test.severity}</td>
                                    <td style={{ padding: '0.75rem', fontSize: '0.875rem' }}>
                                        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                                                <span>{test.error_message || test.description}</span>
                                                <div style={{ display: 'flex', gap: '0.5rem' }}>
                                                    {test.status === 'FAIL' && (test as any).sample_data && test.category !== 'smoke' && (
                                                        <button
                                                            onClick={() => setExpandedSingleData(expandedSingleData === index ? null : index)}
                                                            style={{
                                                                fontSize: '0.75rem',
                                                                background: '#3b82f6',
                                                                color: 'white',
                                                                border: 'none',
                                                                borderRadius: '4px',
                                                                padding: '2px 8px',
                                                                cursor: 'pointer',
                                                                whiteSpace: 'nowrap'
                                                            }}
                                                        >
                                                            {expandedSingleData === index ? 'Hide Data' : 'View Bad Data'}
                                                        </button>
                                                    )}
                                                    <button
                                                        onClick={() => setExpandedSingleSql(expandedSingleSql === index ? null : index)}
                                                        style={{
                                                            fontSize: '0.75rem',
                                                            background: 'var(--secondary)',
                                                            color: 'var(--secondary-foreground)',
                                                            border: '1px solid var(--border)',
                                                            borderRadius: '4px',
                                                            padding: '2px 8px',
                                                            cursor: 'pointer',
                                                            whiteSpace: 'nowrap'
                                                        }}
                                                    >
                                                        {expandedSingleSql === index ? 'Hide SQL' : 'Show SQL'}
                                                    </button>
                                                </div>
                                            </div>

                                            {expandedSingleData === index && test.sample_data && test.sample_data.length > 0 && (
                                                <div style={{
                                                    marginTop: '0.5rem',
                                                    padding: '0.75rem',
                                                    background: '#fff',
                                                    borderRadius: '4px',
                                                    overflowX: 'auto',
                                                    border: '1px solid #3b82f6',
                                                    boxShadow: '0 2px 4px rgba(0,0,0,0.05)'
                                                }}>
                                                    <div style={{ fontWeight: '700', color: '#1d4ed8', marginBottom: '0.5rem', fontSize: '0.75rem' }}>
                                                        Sample problematic rows (max 10):
                                                    </div>
                                                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.75rem' }}>
                                                        <thead>
                                                            <tr style={{ background: '#f8fafc', borderBottom: '1px solid #e2e8f0' }}>
                                                                {Object.keys(test.sample_data[0]).map((key: string) => (
                                                                    <th key={key} style={{ padding: '0.25rem 0.5rem', textAlign: 'left' }}>{key}</th>
                                                                ))}
                                                            </tr>
                                                        </thead>
                                                        <tbody>
                                                            {test.sample_data.map((row: Record<string, any>, rIdx: number) => (
                                                                <tr key={rIdx} style={{ borderBottom: '1px solid #f1f5f9' }}>
                                                                    {Object.values(row).map((val: any, vIdx: number) => (
                                                                        <td key={vIdx} style={{ padding: '0.25rem 0.5rem' }}>{val?.toString() || 'NULL'}</td>
                                                                    ))}
                                                                </tr>
                                                            ))}
                                                        </tbody>
                                                    </table>
                                                </div>
                                            )}

                                            {expandedSingleSql === index && (
                                                <pre style={{
                                                    marginTop: '0.5rem',
                                                    padding: '0.75rem',
                                                    background: '#1e293b',
                                                    borderRadius: '4px',
                                                    overflowX: 'auto',
                                                    fontSize: '0.75rem',
                                                    color: '#f8fafc',
                                                    borderLeft: '4px solid #3b82f6',
                                                    fontFamily: 'monospace',
                                                    whiteSpace: 'pre-wrap'
                                                }}>
                                                    {test.sql_query}
                                                </pre>
                                            )}
                                        </div>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>

            {/* AI Suggestions (Single File Mode) */}
            {summary && summary.ai_suggestions && summary.ai_suggestions.length > 0 && (
                <div className="card" style={{ marginTop: '2rem' }}>
                    <h3 style={{ fontSize: '1.25rem', fontWeight: '600', marginBottom: '1rem' }}>
                        🤖 AI Suggested Tests ({summary.ai_suggestions.length})
                    </h3>
                    {summary.ai_suggestions.map((suggestion: AISuggestion, idx: number) => {
                        const isSaved = savedTests.has(`single-${suggestion.test_name}`);
                        return (
                            <div key={idx} style={{
                                padding: '1rem',
                                background: 'var(--secondary)',
                                borderRadius: 'var(--radius)',
                                marginBottom: '0.75rem',
                                border: '2px dashed var(--primary)',
                                display: 'flex',
                                justifyContent: 'space-between',
                                alignItems: 'flex-start'
                            }}>
                                <div>
                                    <div style={{ fontWeight: '600', marginBottom: '0.5rem' }}>{suggestion.test_name}</div>
                                    <div style={{ fontSize: '0.875rem', color: 'var(--secondary-foreground)', marginBottom: '0.5rem' }}>
                                        {suggestion.reasoning}
                                    </div>
                                    <div style={{ fontSize: '0.75rem', color: 'var(--secondary-foreground)' }}>
                                        Severity: {suggestion.severity}
                                    </div>
                                </div>
                                <button
                                    onClick={() => {
                                        handleSaveCustomTest(suggestion, 'single');
                                    }}
                                    disabled={isSaved}
                                    style={{
                                        padding: '0.5rem 1rem',
                                        backgroundColor: isSaved ? '#10b981' : 'var(--primary)',
                                        color: 'white',
                                        border: 'none',
                                        borderRadius: 'var(--radius)',
                                        cursor: isSaved ? 'default' : 'pointer',
                                        fontSize: '0.875rem',
                                        fontWeight: '600',
                                        whiteSpace: 'nowrap',
                                        marginLeft: '1rem',
                                        opacity: isSaved ? 0.7 : 1
                                    }}
                                >
                                    {isSaved ? '✓ Added' : '+ Add to Custom'}
                                </button>
                            </div>
                        );
                    })}
                </div>
            )}
        </div>
    );
}
