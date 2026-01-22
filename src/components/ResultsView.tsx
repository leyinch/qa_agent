"use client";

import { useEffect, useState } from "react";
import { PieChart, Pie, Cell, Tooltip, Legend, ResponsiveContainer } from "recharts";

interface TestResult {
    test_id?: string;
    test_name: string;
    description: string;
    sql_query: string;
    severity: string;
    status: "PASS" | "FAIL" | "ERROR";
    rows_affected?: number;
    error_message?: string;
    target_dataset?: string;
    target_table?: string;
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
    ai_suggestions?: any[];
    error?: string;
}

const COLORS = {
    PASS: "#10b981", // Green
    FAIL: "#ef4444", // Red
    ERROR: "#f59e0b", // Amber
};

export default function ResultsView() {
    const [results, setResults] = useState<TestResult[]>([]);
    const [mappingResults, setMappingResults] = useState<MappingResult[]>([]);
    const [summary, setSummary] = useState<any>(null);
    const [isConfigMode, setIsConfigMode] = useState(false);
    const [loading, setLoading] = useState(true);
    const [savedTests, setSavedTests] = useState<Set<string>>(new Set());
    const [projectId, setProjectId] = useState<string>("");
    const [mode, setMode] = useState<string>("");
    const [executionTimestamp, setExecutionTimestamp] = useState<string>("");
    const [executionId, setExecutionId] = useState<string>("");

    useEffect(() => {
        const data = localStorage.getItem("testResults");
        if (data) {
            try {
                const parsed = JSON.parse(data);

                // Try to extract project_id from mapping info or summary if available
                if (parsed.project_id) {
                    setProjectId(parsed.project_id);
                }

                // Check if it's config table mode (has results_by_mapping)
                if (parsed.results_by_mapping) {
                    setIsConfigMode(true);
                    setMappingResults(parsed.results_by_mapping);
                    setSummary(parsed.summary);
                    if (parsed.execution_timestamp) {
                        setExecutionTimestamp(parsed.execution_timestamp);
                    }
                    if (parsed.execution_id) {
                        setExecutionId(parsed.execution_id);
                    }
                } else if (Array.isArray(parsed)) {
                    // Handle raw array from history (granular logs)
                    // Check if we can group them by mapping_id
                    const hasMappingInfo = parsed.some(r => r.mapping_id);

                    if (hasMappingInfo) {
                        // Group by mapping_id
                        const grouped: Record<string, MappingResult> = {};

                        parsed.forEach((row: any) => {
                            const mId = row.mapping_id || 'unknown';
                            if (!grouped[mId]) {
                                grouped[mId] = {
                                    mapping_id: mId,
                                    mapping_info: {
                                        source: row.source_file || row.source || 'unknown',
                                        target: row.target_table || row.target || 'unknown',
                                        file_row_count: 0, // Info might be lost in flattening
                                        table_row_count: 0
                                    },
                                    predefined_results: [],
                                    ai_suggestions: []
                                };
                            }

                            // transform flat row back to TestResult
                            grouped[mId].predefined_results.push({
                                test_id: row.test_id,
                                test_name: row.test_name,
                                description: row.description,
                                sql_query: row.sql_query,
                                severity: row.severity,
                                status: row.status,
                                rows_affected: row.rows_affected,
                                error_message: row.error_message
                            });
                        });

                        setMappingResults(Object.values(grouped));
                        setIsConfigMode(true);
                        // Recalculate summary if missing
                        const totalTests = parsed.length;
                        const passed = parsed.filter((r: any) => r.status === 'PASS').length;
                        const failed = parsed.filter((r: any) => r.status === 'FAIL').length;
                        const errors = parsed.filter((r: any) => r.status === 'ERROR').length;

                        setSummary({
                            total_mappings: Object.keys(grouped).length,
                            total_tests: totalTests,
                            passed,
                            failed,
                            errors,
                            total_suggestions: 0
                        });


                        if ((parsed as any).execution_timestamp) {
                            setExecutionTimestamp((parsed as any).execution_timestamp);
                        } else if (parsed.length > 0 && (parsed[0] as any).execution_timestamp) {
                            setExecutionTimestamp((parsed[0] as any).execution_timestamp);
                        }

                    } else {
                        setResults(parsed);
                    }
                } else if (parsed.predefined_results) {
                    // Single GCS file mode
                    setResults(parsed.predefined_results);
                    setSummary(parsed.summary);
                }

                if (parsed.execution_id) {
                    setExecutionId(parsed.execution_id);
                } else if (Array.isArray(parsed) && parsed.length > 0 && parsed[0].execution_id) {
                    setExecutionId(parsed[0].execution_id);
                } else if (parsed.results && Array.isArray(parsed.results) && parsed.results.length > 0 && parsed.results[0].execution_id) {
                    setExecutionId(parsed.results[0].execution_id);
                }

                if (parsed.comparison_mode) {
                    setMode(parsed.comparison_mode);
                } else if (Array.isArray(parsed) && parsed.length > 0 && parsed[0].comparison_mode) {
                    setMode(parsed[0].comparison_mode);
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

    const handleSaveCustomTest = async (suggestion: any, mappingId: string, targetDataset: string | null = null, targetTable: string | null = null) => {
        if (!projectId) {
            alert("Project ID not found. Cannot save custom test.");
            return;
        }

        try {
            const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'https://data-qa-agent-backend-750147355601.us-central1.run.app';
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
            setSavedTests(prev => new Set(prev).add(key));
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
                <div style={{ marginBottom: '2rem', borderBottom: '1px solid var(--border)', paddingBottom: '1rem', display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
                        <div style={{ fontSize: '0.9rem', color: '#64748b' }}>
                            Project: <span style={{ fontFamily: 'monospace', fontWeight: '700', color: 'var(--primary)' }}>{projectId}</span>
                        </div>
                        {executionId && (
                            <div style={{ fontSize: '0.9rem', color: '#64748b' }}>
                                Execution ID: <span style={{ fontFamily: 'monospace', fontWeight: '600' }}>{executionId.substring(0, 8)}</span>
                            </div>
                        )}
                    </div>
                    {executionTimestamp && (
                        <span style={{ fontSize: '0.9rem', fontWeight: '500', color: '#64748b', textAlign: 'right' }}>
                            Ran on: {new Date(executionTimestamp).toLocaleString()}
                        </span>
                    )}
                </div>

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

                {/* Results by Mapping */}
                {mappingResults.map((mapping, idx) => {
                    const mappingStats = {
                        PASS: mapping.predefined_results.filter(r => r.status === 'PASS').length,
                        FAIL: mapping.predefined_results.filter(r => r.status === 'FAIL').length,
                        ERROR: mapping.predefined_results.filter(r => r.status === 'ERROR').length,
                    };

                    return (
                        <div key={idx} className="card" style={{ marginBottom: '2rem' }}>
                            <div style={{
                                display: 'flex',
                                justifyContent: 'space-between',
                                alignItems: 'center',
                                borderBottom: '1px solid var(--border)',
                                paddingBottom: '0.75rem',
                                marginBottom: '1rem'
                            }}>
                                <h3 style={{ fontSize: '1.25rem', fontWeight: '600', margin: 0 }}>
                                    Mapping ID: <span style={{ color: 'var(--primary)', fontFamily: 'monospace' }}>{mapping.mapping_id}</span>
                                </h3>
                                <div style={{ fontSize: '0.875rem', color: 'var(--secondary-foreground)' }}>
                                    Target: <span style={{ fontFamily: 'monospace', fontWeight: '600', color: 'var(--foreground)' }}>
                                        {mapping.mapping_info?.target || 'Unknown'}
                                    </span>
                                </div>
                            </div>

                            {mapping.mapping_info && (
                                <div style={{
                                    padding: '1rem',
                                    background: 'var(--secondary)',
                                    borderRadius: 'var(--radius)',
                                    marginBottom: '1rem',
                                    fontSize: '0.875rem',
                                    display: 'grid',
                                    gridTemplateColumns: mode?.toLowerCase().includes('scd') ? '1fr' : '1fr 1fr',
                                    gap: '0.5rem'
                                }}>
                                    {!mode?.toLowerCase().includes('scd') && mapping.mapping_info.source !== 'SCD Validation' && (
                                        <div><strong>Source:</strong> <code style={{ wordBreak: 'break-all' }}>{mapping.mapping_info.source}</code></div>
                                    )}
                                    <div><strong>Target Table:</strong> <code>{mapping.mapping_info.target}</code></div>
                                    {!mode?.toLowerCase().includes('scd') && mapping.mapping_info.source !== 'SCD Validation' && (
                                        <>
                                            <div><strong>GCS Rows:</strong> {mapping.mapping_info.file_row_count}</div>
                                            <div><strong>BigQuery Rows:</strong> {mapping.mapping_info.table_row_count}</div>
                                        </>
                                    )}
                                </div>
                            )}

                            {mapping.error && (
                                <div style={{ padding: '1rem', background: '#fef2f2', color: '#991b1b', borderRadius: 'var(--radius)', marginBottom: '1rem' }}>
                                    <strong>Error:</strong> {mapping.error}
                                </div>
                            )}

                            {/* Mapping Stats */}
                            <div style={{ display: 'flex', gap: '1rem', marginBottom: '1rem' }}>
                                <div style={{ padding: '0.5rem 1rem', background: '#d1fae5', color: '#065f46', borderRadius: 'var(--radius)', fontWeight: '600' }}>
                                    ✓ {mappingStats.PASS} Passed
                                </div>
                                <div style={{ padding: '0.5rem 1rem', background: '#fee2e2', color: '#991b1b', borderRadius: 'var(--radius)', fontWeight: '600' }}>
                                    ✗ {mappingStats.FAIL} Failed
                                </div>
                                {mappingStats.ERROR > 0 && (
                                    <div style={{ padding: '0.5rem 1rem', background: '#fef3c7', color: '#92400e', borderRadius: 'var(--radius)', fontWeight: '600' }}>
                                        ⚠ {mappingStats.ERROR} Errors
                                    </div>
                                )}
                            </div>

                            {/* Test Results Table */}
                            <div style={{ overflowX: 'auto' }}>
                                <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                                    <thead>
                                        <tr style={{ borderBottom: '2px solid var(--border)' }}>
                                            <th style={{ padding: '0.75rem', textAlign: 'left' }}>Test Name</th>
                                            <th style={{ padding: '0.75rem', textAlign: 'left' }}>Status</th>
                                            <th style={{ padding: '0.75rem', textAlign: 'left' }}>Severity</th>
                                            <th style={{ padding: '0.75rem', textAlign: 'left' }}>Rows Affected</th>
                                            <th style={{ padding: '0.75rem', textAlign: 'left' }}>Details</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {mapping.predefined_results.map((test, testIdx) => (
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
                                                    {test.error_message || test.description}
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>

                            {/* AI Suggestions */}
                            {mapping.ai_suggestions && mapping.ai_suggestions.length > 0 && (
                                <div style={{ marginTop: '1.5rem' }}>
                                    <h4 style={{ fontSize: '1.25rem', fontWeight: '600', marginBottom: '1rem' }}>
                                        🤖 AI Suggested Tests ({mapping.ai_suggestions.length})
                                    </h4>
                                    {mapping.ai_suggestions.map((suggestion, sugIdx) => {
                                        const isSaved = savedTests.has(`${mapping.mapping_id}-${suggestion.test_name}`);
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
                                                        // Extract target dataset/table from mapping info if possible
                                                        // mapping.mapping_info.target usually has "dataset.table" WITHOUT project or "project.dataset.table"
                                                        let targetDataset = null;
                                                        let targetTable = null;
                                                        if (mapping.mapping_info && mapping.mapping_info.target) {
                                                            const parts = mapping.mapping_info.target.split('.');
                                                            if (parts.length === 2) {
                                                                targetDataset = parts[0];
                                                                targetTable = parts[1];
                                                            } else if (parts.length === 3) {
                                                                // project.dataset.table
                                                                targetDataset = parts[1];
                                                                targetTable = parts[2];
                                                            }
                                                        }
                                                        handleSaveCustomTest(suggestion, mapping.mapping_id, targetDataset, targetTable);
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
                })}
            </div>
        );
    }

    // Single file/schema mode - original display
    const stats = {
        PASS: results.filter((r) => r.status === "PASS").length,
        FAIL: results.filter((r) => r.status === "FAIL").length,
        ERROR: results.filter((r) => r.status === "ERROR").length,
    };

    const chartData = [
        { name: "Pass", value: stats.PASS },
        { name: "Fail", value: stats.FAIL },
        { name: "Error", value: stats.ERROR },
    ];

    return (
        <div style={{ padding: '2rem', maxWidth: '1200px', margin: '0 auto' }}>
            <div style={{ marginBottom: '2rem', borderBottom: '1px solid var(--border)', paddingBottom: '1rem', display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
                    <div style={{ fontSize: '0.9rem', color: '#64748b' }}>
                        Project: <span style={{ fontFamily: 'monospace', fontWeight: '700', color: 'var(--primary)' }}>{projectId}</span>
                    </div>
                    {executionId && (
                        <div style={{ fontSize: '0.9rem', color: '#64748b' }}>
                            Execution ID: <span style={{ fontFamily: 'monospace', fontWeight: '600' }}>{executionId.substring(0, 8)}</span>
                        </div>
                    )}
                </div>
                {executionTimestamp && (
                    <span style={{ fontSize: '0.9rem', fontWeight: '500', color: '#64748b', textAlign: 'right' }}>
                        Ran on: {new Date(executionTimestamp).toLocaleString()}
                    </span>
                )}
            </div>

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
                                <th style={{ padding: '0.75rem', textAlign: 'left' }}>Target Table</th>
                                <th style={{ padding: '0.75rem', textAlign: 'left' }}>Test Name</th>
                                <th style={{ padding: '0.75rem', textAlign: 'left' }}>Status</th>
                                <th style={{ padding: '0.75rem', textAlign: 'left' }}>Severity</th>
                                <th style={{ padding: '0.75rem', textAlign: 'left' }}>Details</th>
                            </tr>
                        </thead>
                        <tbody>
                            {results.map((test, index) => (
                                <tr key={index} style={{ borderBottom: '1px solid var(--border)' }}>
                                    <td style={{ padding: '0.75rem', fontFamily: 'monospace', fontSize: '0.85rem' }}>
                                        {test.target_table ? `${test.target_dataset ? test.target_dataset + '.' : ''}${test.target_table}` : 'N/A'}
                                    </td>
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
                                        {test.error_message || test.description}
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
                    {summary.ai_suggestions.map((suggestion: any, idx: number) => {
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
