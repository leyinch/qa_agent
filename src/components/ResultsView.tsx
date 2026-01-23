"use client";

import { useEffect, useState } from "react";

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
    mapping_id?: string;
}

interface AiSuggestion {
    test_name: string;
    test_category?: string;
    reasoning: string;
    severity: string;
    sql_query: string;
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
    ai_suggestions?: AiSuggestion[];
    error?: string;
}

interface Summary {
    total_mappings: number;
    total_tests: number;
    passed: number;
    failed: number;
    errors: number;
    total_suggestions: number;
}

export default function ResultsView() {
    const [results, setResults] = useState<TestResult[]>([]);
    const [mappingResults, setMappingResults] = useState<MappingResult[]>([]);
    const [summary, setSummary] = useState<Summary | null>(null);
    const [isConfigMode, setIsConfigMode] = useState(false);
    const [loading, setLoading] = useState(true);
    const [savedTests, setSavedTests] = useState<Set<string>>(new Set());
    const [projectId, setProjectId] = useState<string>("");
    const [mode, setMode] = useState<string>("");

    useEffect(() => {
        const data = localStorage.getItem("testResults");
        if (data) {
            try {
                const parsed = JSON.parse(data);

                if (parsed.project_id) {
                    setProjectId(parsed.project_id);
                }

                const handleParsing = (resultsToParse: any[], originalObj: any) => {
                    const hasMappingInfo = resultsToParse.some(r => r.mapping_id);
                    if (hasMappingInfo) {
                        const grouped: Record<string, MappingResult> = {};
                        resultsToParse.forEach((row: any) => {
                            const mId = row.mapping_id || 'unknown';
                            if (!grouped[mId]) {
                                grouped[mId] = {
                                    mapping_id: mId,
                                    mapping_info: {
                                        source: row.source || 'unknown',
                                        target: row.target || 'unknown',
                                        file_row_count: 0,
                                        table_row_count: 0
                                    },
                                    predefined_results: [],
                                    ai_suggestions: row.ai_suggestions || []
                                };
                            }
                            grouped[mId].predefined_results.push({
                                test_id: row.test_id,
                                test_name: row.test_name,
                                description: row.description,
                                sql_query: row.sql_query,
                                severity: row.severity,
                                status: row.status,
                                rows_affected: row.rows_affected,
                                error_message: row.error_message,
                                target_dataset: row.target_dataset,
                                target_table: row.target_table || (row.target ? row.target.split('.').pop() : null)
                            });
                        });

                        setMappingResults(Object.values(grouped));
                        setIsConfigMode(true);

                        const passed = resultsToParse.filter((r: TestResult) => r.status === 'PASS').length;
                        const failed = resultsToParse.filter((r: TestResult) => r.status === 'FAIL').length;
                        const errors = resultsToParse.filter((r: TestResult) => r.status === 'ERROR').length;

                        setSummary({
                            total_mappings: Object.keys(grouped).length,
                            total_tests: resultsToParse.length,
                            passed,
                            failed,
                            errors,
                            total_suggestions: 0
                        });
                    } else {
                        setResults(resultsToParse);
                        if (originalObj.summary) setSummary(originalObj.summary);
                    }
                };

                if (parsed.results_by_mapping) {
                    setIsConfigMode(true);
                    setMappingResults(parsed.results_by_mapping);
                    setSummary(parsed.summary);
                } else if (Array.isArray(parsed)) {
                    handleParsing(parsed, { summary: null });
                } else if (parsed.results && Array.isArray(parsed.results)) {
                    handleParsing(parsed.results, parsed);
                } else if (parsed.predefined_results) {
                    setResults(parsed.predefined_results);
                    setSummary(parsed.summary);
                } else if (parsed.details && Array.isArray(parsed.details)) {
                    handleParsing(parsed.details, parsed);
                }

                if (parsed.comparison_mode) {
                    setMode(parsed.comparison_mode);
                }
            } catch (e) {
                console.error("Failed to parse results", e);
            }
        }

        const storedProjectId = localStorage.getItem("projectId");
        if (storedProjectId) {
            setProjectId(storedProjectId);
        }

        setLoading(false);
    }, []);

    const handleSaveCustomTest = async (suggestion: AiSuggestion, mappingId: string, targetDataset: string | null = null, targetTable: string | null = null) => {
        if (!projectId) {
            alert("Project ID not found. Cannot save custom test.");
            return;
        }

        try {
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

            const response = await fetch(`/api/custom-tests`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(payload),
            });

            if (!response.ok) {
                throw new Error("Failed to save custom test");
            }

            const key = `${mappingId}-${suggestion.test_name}`;
            setSavedTests(prev => {
                const next = new Set(prev);
                next.add(key);
                return next;
            });
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

    if (isConfigMode) {
        return (
            <div style={{ padding: '0 1rem', maxWidth: '1200px', margin: '0 auto' }}>
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
                    </div>
                )}

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
                                    Mapping ID: <span style={{ color: '#0f172a', fontFamily: 'monospace' }}>{mapping.mapping_id}</span>
                                </h3>
                            </div>

                            {
                                mapping.mapping_info && (
                                    <div style={{
                                        padding: '1.25rem',
                                        background: '#f8fafc',
                                        borderRadius: 'var(--radius)',
                                        marginBottom: '1.25rem',
                                        fontSize: '0.925rem',
                                        display: 'grid',
                                        gridTemplateColumns: mode?.toLowerCase().includes('scd') ? '1fr' : '1fr 1fr',
                                        gap: '1rem',
                                        border: '1px solid var(--border)'
                                    }}>
                                        {!mode?.toLowerCase().includes('scd') && mapping.mapping_info.source && (
                                            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
                                                <span style={{ fontSize: '0.75rem', fontWeight: '700', color: '#64748b', textTransform: 'uppercase' }}>Source</span>
                                                <code style={{ wordBreak: 'break-all', color: '#0f172a', fontWeight: '600' }}>{mapping.mapping_info.source}</code>
                                            </div>
                                        )}
                                        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
                                            <span style={{ fontSize: '0.75rem', fontWeight: '700', color: '#64748b', textTransform: 'uppercase' }}>Target Table</span>
                                            <code style={{ color: '#0f172a', fontWeight: '600' }}>{mapping.mapping_info.target}</code>
                                        </div>

                                        {/* Show counts if available from Row Count test */}
                                        {mapping.predefined_results.find(r => r.test_id === 'row_count_match' || r.test_name === 'Row Count Match') && (
                                            <div style={{ gridColumn: '1 / -1', marginTop: '0.5rem', paddingTop: '0.75rem', borderTop: '1px solid #e2e8f0', display: 'flex', gap: '2rem' }}>
                                                <div style={{ display: 'flex', alignItems: 'baseline', gap: '0.5rem' }}>
                                                    <span style={{ fontSize: '0.8rem', color: '#64748b' }}>Row Count Info:</span>
                                                    <span style={{ fontWeight: '600', color: '#0f172a' }}>{mapping.predefined_results.find(r => r.test_name === 'Row Count Match')?.description || 'Available in test details'}</span>
                                                </div>
                                            </div>
                                        )}
                                    </div>
                                )
                            }

                            {
                                mapping.error && (
                                    <div style={{ padding: '1rem', background: '#fef2f2', color: '#991b1b', borderRadius: 'var(--radius)', marginBottom: '1rem' }}>
                                        <strong>Error:</strong> {mapping.error}
                                    </div>
                                )
                            }

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

                            {
                                mapping.ai_suggestions && mapping.ai_suggestions.length > 0 && (
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
                                                            let targetDataset = null;
                                                            let targetTable = null;
                                                            if (mapping.mapping_info && mapping.mapping_info.target) {
                                                                const parts = mapping.mapping_info.target.split('.');
                                                                if (parts.length === 2) {
                                                                    targetDataset = parts[0];
                                                                    targetTable = parts[1];
                                                                } else if (parts.length === 3) {
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
                                )
                            }
                        </div>
                    );
                })
                }
            </div >
        );
    }

    const stats = {
        PASS: results.filter((r) => r.status === "PASS").length,
        FAIL: results.filter((r) => r.status === "FAIL").length,
        ERROR: results.filter((r) => r.status === "ERROR").length,
    };

    return (
        <div style={{ padding: '0 1rem', maxWidth: '1200px', margin: '0 auto' }}>
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
                                        {test.target_table ? `${test.target_dataset ? test.target_dataset + '.' : ''}${test.target_table}` : (test.mapping_id || 'N/A')}
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
        </div>
    );
}
