"use client";

import { useEffect, useState } from "react";

interface TestResult {
    test_id?: string;
    test_name: string;
    category?: string;
    description: string;
    sql_query: string;
    severity: string;
    status: "PASS" | "FAIL" | "ERROR";
    rows_affected?: number;
    error_message?: string;
    sample_data?: any[];
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
    PASS: "#10b981",
    FAIL: "#ef4444",
    ERROR: "#f59e0b",
};

export default function ResultsView() {
    const [results, setResults] = useState<TestResult[]>([]);
    const [mappingResults, setMappingResults] = useState<MappingResult[]>([]);
    const [summary, setSummary] = useState<any>(null);
    const [isConfigMode, setIsConfigMode] = useState(false);
    const [loading, setLoading] = useState(true);
    const [savedTests, setSavedTests] = useState<Set<string>>(new Set());
    const [projectId, setProjectId] = useState<string>("");
    const [activeTab, setActiveTab] = useState<number>(0);
    const [expandedSql, setExpandedSql] = useState<string | null>(null);

    useEffect(() => {
        const data = localStorage.getItem("testResults");
        if (data) {
            try {
                const parsed = JSON.parse(data);
                setProjectId(parsed.project_id || localStorage.getItem("projectId") || "");

                let currentSummary = parsed.summary || {};
                let currentMappingResults = parsed.results_by_mapping || [];
                let currentResults = parsed.results || parsed.predefined_results || [];

                // Defensive Aggregation: If summary is missing or empty, calculate it from results
                if (!currentSummary.total_tests || currentSummary.total_tests === 0) {
                    if (currentMappingResults.length > 0) {
                        const allTests = currentMappingResults.flatMap((m: any) => m.predefined_results || []);
                        currentSummary = {
                            total_tests: allTests.length,
                            passed: allTests.filter((t: any) => t.status === 'PASS').length,
                            failed: allTests.filter((t: any) => t.status === 'FAIL').length,
                            errors: allTests.filter((t: any) => t.status === 'ERROR').length
                        };
                    } else if (currentResults.length > 0) {
                        currentSummary = {
                            total_tests: currentResults.length,
                            passed: currentResults.filter((t: any) => t.status === 'PASS').length,
                            failed: currentResults.filter((t: any) => t.status === 'FAIL').length,
                            errors: currentResults.filter((t: any) => t.status === 'ERROR').length
                        };
                    }
                }

                if (currentMappingResults.length > 0) {
                    setIsConfigMode(true);
                    setMappingResults(currentMappingResults);
                } else {
                    setResults(currentResults);
                }
                setSummary(currentSummary);

                // If we have metadata for run type
                if (parsed.executed_by) {
                    setSummary((prev: any) => ({ ...prev, executed_by: parsed.executed_by }));
                }

            } catch (e) {
                console.error("Failed to parse results", e);
            }
        }
        setLoading(false);
    }, []);

    const handleSaveCustomTest = async (suggestion: any, target: string) => {
        if (!projectId) return;
        try {
            const [ds, tbl] = target.split('.');
            const payload = {
                project_id: projectId,
                test_name: suggestion.test_name,
                test_category: suggestion.test_category || "custom",
                severity: suggestion.severity,
                sql_query: suggestion.sql_query,
                description: suggestion.reasoning || suggestion.description,
                target_dataset: ds,
                target_table: tbl
            };

            const response = await fetch('/api/custom-tests', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });

            if (response.ok) {
                setSavedTests(prev => new Set(prev).add(suggestion.test_name));
                alert("Saved to Custom Tests!");
            }
        } catch (error) {
            console.error("Error saving:", error);
        }
    };

    if (loading) return <div style={{ padding: '2rem', textAlign: 'center' }}>Loading results...</div>;
    if (!isConfigMode && results.length === 0) return <div style={{ padding: '2rem', textAlign: 'center' }}>No results.</div>;

    const renderTestCard = (test: TestResult, idx: number, mappingId?: string) => {
        const isExpanded = expandedSql === `${mappingId}-${idx}`;
        const hasSample = test.status === 'FAIL' && test.sample_data && test.sample_data.length > 0;

        return (
            <div key={idx} className="card" style={{ padding: '0', overflow: 'hidden', marginBottom: '1rem', border: `1px solid ${test.status === 'FAIL' ? '#fee2e2' : 'var(--border)'}` }}>
                <div style={{ padding: '1rem 1.5rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
                        <div style={{ color: test.status === 'PASS' ? '#10b981' : '#ef4444', fontWeight: 'bold' }}>
                            {test.status === 'PASS' ? '✅' : '❌'}
                        </div>
                        <div>
                            <div style={{ fontWeight: '700' }}>{test.test_name}</div>
                            <div style={{ fontSize: '0.75rem', opacity: 0.7 }}>{test.category}</div>
                        </div>
                    </div>
                    <div style={{ display: 'flex', gap: '2rem', alignItems: 'center' }}>
                        <div style={{ textAlign: 'center' }}>
                            <div style={{ fontSize: '0.625rem', opacity: 0.6 }}>AFFECTED</div>
                            <div style={{ fontWeight: '800' }}>{test.rows_affected || 0}</div>
                        </div>
                        <button
                            className="btn btn-outline"
                            style={{ padding: '0.25rem 0.75rem', fontSize: '0.75rem' }}
                            onClick={() => setExpandedSql(isExpanded ? null : `${mappingId}-${idx}`)}
                        >
                            {isExpanded ? 'Hide' : 'View'}
                        </button>
                    </div>
                </div>

                {(isExpanded || (hasSample && test.category !== 'smoke')) && (
                    <div style={{ padding: '1.5rem', background: 'rgba(0,0,0,0.02)', borderTop: '1px solid var(--border)' }}>
                        <div style={{ fontSize: '0.875rem', marginBottom: '1rem' }}>{test.error_message || test.description}</div>

                        {hasSample && test.category !== 'smoke' && (
                            <div style={{ marginTop: '1rem', overflowX: 'auto' }}>
                                <div style={{ fontSize: '0.75rem', fontWeight: '800', color: '#ef4444', marginBottom: '0.5rem' }}>SAMPLE FAILING ROWS</div>
                                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.75rem', background: 'white', border: '1px solid #fee2e2' }}>
                                    <thead>
                                        <tr style={{ background: '#f8fafc', borderBottom: '1px solid #e2e8f0' }}>
                                            {Object.keys(test.sample_data![0]).map(k => <th key={k} style={{ padding: '0.5rem', textAlign: 'left' }}>{k}</th>)}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {test.sample_data!.slice(0, 5).map((row, rIdx) => (
                                            <tr key={rIdx} style={{ borderBottom: '1px solid #f1f5f9' }}>
                                                {Object.values(row).map((v: any, vIdx) => <td key={vIdx} style={{ padding: '0.5rem' }}>{String(v)}</td>)}
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        )}

                        {isExpanded && (
                            <div style={{ marginTop: '1rem' }}>
                                <div style={{ fontSize: '0.75rem', fontWeight: '800', color: 'var(--primary)', marginBottom: '0.5rem' }}>SQL</div>
                                <pre style={{ padding: '1rem', background: '#1e293b', color: '#e2e8f0', borderRadius: '8px', fontSize: '0.75rem', overflowX: 'auto' }}>{test.sql_query}</pre>
                            </div>
                        )}
                    </div>
                )}
            </div>
        );
    };

    return (
        <div style={{ padding: '2rem', maxWidth: '1200px', margin: '0 auto' }}>
            <div style={{ marginBottom: '2rem', display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end' }}>
                <div>
                    <h1 style={{ fontSize: '2.5rem', fontWeight: '800', marginBottom: '0.25rem' }} className="gradient-text">Test Results</h1>
                    {projectId && <div style={{ color: 'var(--secondary-foreground)', opacity: 0.8 }}>Project: {projectId}</div>}
                </div>
                {summary?.executed_by && (
                    <div style={{ background: 'var(--secondary)', padding: '0.5rem 1rem', borderRadius: '20px', fontSize: '0.875rem', fontWeight: '600', display: 'flex', alignItems: 'center', gap: '0.5rem', border: '1px solid var(--border)' }}>
                        <span style={{ opacity: 0.6 }}>Triggered By:</span>
                        <span style={{ color: 'var(--primary)' }}>{summary.executed_by}</span>
                    </div>
                )}
            </div>

            {summary && (
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '1.5rem', marginBottom: '2rem' }}>
                    <div className="card" style={{ textAlign: 'center' }}>
                        <div style={{ fontSize: '0.75rem', opacity: 0.7 }}>TOTAL</div>
                        <div style={{ fontSize: '1.5rem', fontWeight: '800' }}>{summary.total_tests}</div>
                    </div>
                    <div className="card" style={{ textAlign: 'center', borderBottom: '4px solid #10b981' }}>
                        <div style={{ fontSize: '0.75rem', color: '#10b981' }}>PASSED</div>
                        <div style={{ fontSize: '1.5rem', fontWeight: '800', color: '#10b981' }}>{summary.passed}</div>
                    </div>
                    <div className="card" style={{ textAlign: 'center', borderBottom: '4px solid #ef4444' }}>
                        <div style={{ fontSize: '0.75rem', color: '#ef4444' }}>FAILED</div>
                        <div style={{ fontSize: '1.5rem', fontWeight: '800', color: '#ef4444' }}>{summary.failed}</div>
                    </div>
                </div>
            )}

            {isConfigMode ? (
                <>
                    <div style={{ display: 'flex', gap: '0.5rem', overflowX: 'auto', marginBottom: '2rem', paddingBottom: '0.5rem' }}>
                        {mappingResults.map((m, idx) => (
                            <button
                                key={idx}
                                onClick={() => setActiveTab(idx)}
                                className={`btn ${activeTab === idx ? 'btn-primary' : 'btn-outline'}`}
                                style={{ whiteSpace: 'nowrap', display: 'flex', alignItems: 'center', gap: '0.75rem' }}
                            >
                                <span>{m.mapping_id}</span>
                                <span style={{
                                    background: m.predefined_results.some(r => r.status === 'FAIL') ? '#ef4444' : '#10b981',
                                    color: 'white',
                                    padding: '2px 8px',
                                    borderRadius: '10px',
                                    fontSize: '0.7rem',
                                    fontWeight: '800'
                                }}>
                                    {m.predefined_results.filter(r => r.status === 'FAIL').length || m.predefined_results.length}
                                </span>
                            </button>
                        ))}
                    </div>

                    {mappingResults[activeTab] && (
                        <div>
                            <div style={{
                                marginBottom: '1.5rem',
                                padding: '1.25rem',
                                background: 'white',
                                borderRadius: '12px',
                                border: '1px solid var(--border)',
                                display: 'flex',
                                justifyContent: 'space-between',
                                alignItems: 'center'
                            }}>
                                <div>
                                    <div style={{ fontSize: '0.75rem', fontWeight: '800', color: 'var(--primary)', letterSpacing: '0.05em', marginBottom: '0.25rem' }}>CURRENT MAPPING</div>
                                    <div style={{ fontSize: '1.5rem', fontWeight: '800' }}>{mappingResults[activeTab].mapping_id}</div>
                                </div>
                                <div style={{ textAlign: 'right' }}>
                                    <div style={{ fontSize: '0.75rem', opacity: 0.6, marginBottom: '0.25rem' }}>Target Dataset/Table</div>
                                    <div style={{ fontWeight: '700' }}>{mappingResults[activeTab].mapping_info?.target || 'N/A'}</div>
                                    <div style={{ marginTop: '0.5rem', display: 'flex', gap: '1rem', justifyContent: 'flex-end', fontSize: '0.875rem' }}>
                                        <span style={{ color: '#10b981' }}>PASSED: <b>{mappingResults[activeTab].predefined_results.filter(r => r.status === 'PASS').length}</b></span>
                                        <span style={{ color: '#ef4444' }}>FAILED: <b>{mappingResults[activeTab].predefined_results.filter(r => r.status === 'FAIL').length}</b></span>
                                    </div>
                                </div>
                            </div>

                            {mappingResults[activeTab].predefined_results.map((t, i) => renderTestCard(t, i, mappingResults[activeTab].mapping_id))}

                            {mappingResults[activeTab].ai_suggestions && mappingResults[activeTab].ai_suggestions.length > 0 && (
                                <div style={{ marginTop: '2rem' }}>
                                    <h3 style={{ marginBottom: '1rem' }}>🤖 AI Recommendations</h3>
                                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '1rem' }}>
                                        {mappingResults[activeTab].ai_suggestions!.map((s, i) => (
                                            <div key={i} className="card" style={{ border: '2px dashed var(--primary)' }}>
                                                <div style={{ fontWeight: '700', marginBottom: '0.5rem' }}>{s.test_name}</div>
                                                <div style={{ fontSize: '0.875rem', marginBottom: '1rem' }}>{s.reasoning}</div>
                                                <button
                                                    className="btn btn-primary" style={{ width: '100%' }}
                                                    onClick={() => handleSaveCustomTest(s, mappingResults[activeTab].mapping_info!.target)}
                                                    disabled={savedTests.has(s.test_name)}
                                                >
                                                    {savedTests.has(s.test_name) ? '✓ Saved' : 'Add to Custom'}
                                                </button>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            )}
                        </div>
                    )}
                </>
            ) : (
                results.map((t, i) => renderTestCard(t, i, 'single'))
            )}
        </div>
    );
}
