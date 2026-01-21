"use client";

import { useEffect, useState } from "react";
import React from "react";

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

                // Unified Aggregation for accurate counts
                if (currentMappingResults.length > 0) {
                    const allTests = currentMappingResults.flatMap((m: any) => m.predefined_results || []);
                    currentSummary = {
                        ...currentSummary,
                        total_tests: allTests.length,
                        passed: allTests.filter((t: any) => t.status === 'PASS').length,
                        failed: allTests.filter((t: any) => t.status === 'FAIL').length,
                        errors: allTests.filter((t: any) => t.status === 'ERROR').length,
                        total_mappings: currentMappingResults.length
                    };

                    // Fallback: If total_tests is 0 but we have passed/failed, recalc
                    if (currentSummary.total_tests === 0 && (currentSummary.passed > 0 || currentSummary.failed > 0)) {
                        currentSummary.total_tests = currentSummary.passed + currentSummary.failed + currentSummary.errors;
                    }

                    setIsConfigMode(true);
                    setMappingResults(currentMappingResults);
                } else {
                    currentSummary = {
                        ...currentSummary,
                        total_tests: currentResults.length,
                        passed: currentResults.filter((t: any) => t.status === 'PASS').length,
                        failed: currentResults.filter((t: any) => t.status === 'FAIL').length,
                        errors: currentResults.filter((t: any) => t.status === 'ERROR').length,
                        total_mappings: 1
                    };
                    setResults(currentResults);
                }

                if (parsed.executed_by) {
                    currentSummary.executed_by = parsed.executed_by;
                }

                setSummary(currentSummary);
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
        const isPass = test.status === 'PASS';

        return (
            <div key={idx} className="card" style={{ padding: '0', overflow: 'hidden', marginBottom: '1rem', border: `1px solid ${!isPass ? '#fee2e2' : 'var(--border)'}`, boxShadow: '0 2px 4px rgba(0,0,0,0.02)' }}>
                <div style={{ padding: '1.25rem 1.5rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '1.25rem' }}>
                        <div style={{
                            width: '32px', height: '32px', borderRadius: '8px',
                            background: isPass ? 'rgba(16, 185, 129, 0.1)' : 'rgba(239, 68, 68, 0.1)',
                            color: isPass ? '#10b981' : '#ef4444',
                            display: 'flex', alignItems: 'center', justifyContent: 'center'
                        }}>
                            {isPass ? '✓' : '✕'}
                        </div>
                        <div>
                            <div style={{ fontWeight: '700', fontSize: '1rem' }}>{test.test_name}</div>
                            <div style={{ fontSize: '0.75rem', opacity: 0.6, textTransform: 'lowercase' }}>{test.category}</div>
                        </div>
                    </div>
                    <div style={{ display: 'flex', gap: '2.5rem', alignItems: 'center' }}>
                        <div style={{
                            background: isPass ? 'rgba(16, 185, 129, 0.1)' : 'rgba(239, 68, 68, 0.1)',
                            color: isPass ? '#10b981' : '#ef4444',
                            padding: '2px 10px', borderRadius: '12px', fontSize: '0.625rem', fontWeight: '800'
                        }}>
                            {test.status}
                        </div>
                        <div style={{ textAlign: 'center' }}>
                            <div style={{ fontSize: '0.625rem', opacity: 0.5, fontWeight: '700' }}>SEVERITY</div>
                            <div style={{ fontWeight: '800', color: test.severity === 'HIGH' ? '#ef4444' : 'inherit', fontSize: '0.75rem' }}>{test.severity}</div>
                        </div>
                        <div style={{ textAlign: 'center' }}>
                            <div style={{ fontSize: '0.625rem', opacity: 0.5, fontWeight: '700' }}>AFFECTED</div>
                            <div style={{ fontWeight: '800', fontSize: '1.1rem', color: !isPass ? '#ef4444' : 'inherit' }}>{test.rows_affected || 0}</div>
                        </div>
                        <button
                            className="btn btn-outline"
                            style={{ padding: '0.4rem 1rem', fontSize: '0.75rem', borderRadius: '8px' }}
                            onClick={() => setExpandedSql(isExpanded ? null : `${mappingId}-${idx}`)}
                        >
                            View SQL
                        </button>
                    </div>
                </div>

                {(isExpanded || (hasSample && test.category !== 'smoke')) && (
                    <div style={{ padding: '1.5rem', background: '#fcfdfe', borderTop: '1px solid var(--border)' }}>
                        <div style={{ marginBottom: '1.5rem' }}>
                            <div style={{ fontSize: '0.75rem', fontWeight: '800', opacity: 0.6, marginBottom: '0.5rem' }}>DESCRIPTION</div>
                            <div style={{ fontSize: '0.875rem', color: '#4b5563' }}>{test.description}</div>
                        </div>

                        {hasSample && test.category !== 'smoke' && (
                            <div style={{ marginTop: '1rem', overflowX: 'auto' }}>
                                <div style={{ fontSize: '0.75rem', fontWeight: '800', color: '#ef4444', marginBottom: '0.75rem', letterSpacing: '0.05em' }}>SAMPLE PROBLEMATIC ROWS</div>
                                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.75rem', background: 'white', borderRadius: '8px', overflow: 'hidden', border: '1px solid #fee2e2' }}>
                                    <thead>
                                        <tr style={{ background: '#fef2f2', borderBottom: '1px solid #fee2e2', textAlign: 'left' }}>
                                            {Object.keys(test.sample_data![0]).map(k => <th key={k} style={{ padding: '0.75rem', fontWeight: '700' }}>{k}</th>)}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {test.sample_data!.slice(0, 5).map((row, rIdx) => (
                                            <tr key={rIdx} style={{ borderBottom: '1px solid #f1f5f9' }}>
                                                {Object.values(row).map((v: any, vIdx) => <td key={vIdx} style={{ padding: '0.75rem' }}>{String(v)}</td>)}
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        )}

                        {isExpanded && (
                            <div style={{ marginTop: '1.5rem' }}>
                                <div style={{ fontSize: '0.75rem', fontWeight: '800', color: 'var(--primary)', marginBottom: '0.5rem' }}>QUERY</div>
                                <pre style={{ padding: '1.25rem', background: '#0f172a', color: '#e2e8f0', borderRadius: '12px', fontSize: '0.75rem', overflowX: 'auto', lineHeight: '1.5' }}>{test.sql_query}</pre>
                            </div>
                        )}
                    </div>
                )}
            </div>
        );
    };

    return (
        <div style={{ padding: '2rem', maxWidth: '1200px', margin: '0 auto' }}>
            <div style={{ marginBottom: '2.5rem', display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                <div>
                    <div style={{ fontSize: '0.75rem', fontWeight: '800', color: '#10b981', letterSpacing: '0.1em', marginBottom: '0.5rem' }}>VALIDATION REPORT</div>
                    <h1 style={{ fontSize: '2.75rem', fontWeight: '800' }} className="gradient-text">Test Results v2</h1>
                </div>
                {summary?.executed_by && (
                    <div className="card" style={{ padding: '0.75rem 1.25rem', display: 'flex', alignItems: 'center', gap: '1rem', background: 'white', border: '1px solid var(--border)' }}>
                        <div style={{ width: '32px', height: '32px', borderRadius: '50%', background: 'var(--secondary)', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--primary)' }}>
                            👤
                        </div>
                        <div>
                            <div style={{ fontSize: '0.625rem', opacity: 0.5, fontWeight: '700' }}>TRIGGERED BY</div>
                            <div style={{ fontSize: '0.875rem', fontWeight: '700' }}>{summary.executed_by}</div>
                        </div>
                    </div>
                )}
            </div>

            {summary && (
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))', gap: '1.5rem', marginBottom: '3rem' }}>
                    <div className="card" style={{ padding: '1.5rem', background: 'white' }}>
                        <div style={{ fontSize: '0.875rem', fontWeight: '600', opacity: 0.6, marginBottom: '0.5rem' }}>Total Tests</div>
                        <div style={{ fontSize: '2.5rem', fontWeight: '800' }}>{summary.total_tests || (summary.passed + summary.failed)}</div>
                        <div style={{ height: '4px', background: 'var(--border)', marginTop: '1rem', borderRadius: '2px' }} />
                    </div>
                    <div className="card" style={{ padding: '1.5rem', background: 'white', position: 'relative', overflow: 'hidden' }}>
                        <div style={{ position: 'absolute', left: 0, top: 0, bottom: 0, width: '4px', background: '#10b981' }} />
                        <div style={{ fontSize: '0.875rem', fontWeight: '600', color: '#10b981', marginBottom: '0.5rem' }}>Tests Passed</div>
                        <div style={{ fontSize: '2.5rem', fontWeight: '800', color: '#10b981' }}>{summary.passed}</div>
                    </div>
                    <div className="card" style={{ padding: '1.5rem', background: 'white', position: 'relative', overflow: 'hidden' }}>
                        <div style={{ position: 'absolute', left: 0, top: 0, bottom: 0, width: '4px', background: '#ef4444' }} />
                        <div style={{ fontSize: '0.875rem', fontWeight: '600', color: '#ef4444', marginBottom: '0.5rem' }}>Tests Failed</div>
                        <div style={{ fontSize: '2.5rem', fontWeight: '800', color: '#ef4444' }}>{summary.failed}</div>
                    </div>
                </div>
            )}

            {isConfigMode ? (
                <>
                    <div style={{ display: 'flex', gap: '0.75rem', overflowX: 'auto', marginBottom: '2.5rem', paddingBottom: '0.5rem' }}>
                        {mappingResults.map((m, idx) => {
                            const failCount = m.predefined_results.filter(r => r.status === 'FAIL').length;
                            const isActive = activeTab === idx;
                            return (
                                <button
                                    key={idx}
                                    onClick={() => setActiveTab(idx)}
                                    className={`btn ${isActive ? 'btn-primary' : 'btn-outline'}`}
                                    style={{
                                        whiteSpace: 'nowrap', display: 'flex', alignItems: 'center', gap: '0.75rem',
                                        padding: '0.6rem 1.25rem', borderRadius: '12px'
                                    }}
                                >
                                    <span>{m.mapping_id}</span>
                                    <span style={{
                                        background: failCount > 0 ? (isActive ? 'white' : '#ef4444') : (isActive ? 'white' : '#10b981'),
                                        color: failCount > 0 ? (isActive ? '#ef4444' : 'white') : (isActive ? '#10b981' : 'white'),
                                        width: '20px', height: '20px', borderRadius: '50%',
                                        fontSize: '0.7rem', fontWeight: '800', display: 'flex', alignItems: 'center', justifyContent: 'center'
                                    }}>
                                        {failCount || m.predefined_results.length}
                                    </span>
                                </button>
                            );
                        })}
                    </div>

                    {mappingResults[activeTab] && (
                        <div>
                            <div style={{
                                marginBottom: '2rem', padding: '1.5rem', background: '#f8fafc',
                                borderRadius: '16px', border: '1px solid var(--border)',
                                display: 'flex', justifyContent: 'space-between', alignItems: 'center'
                            }}>
                                <div>
                                    <div style={{ fontSize: '0.7rem', fontWeight: '800', color: 'var(--primary)', letterSpacing: '0.1em', marginBottom: '0.25rem' }}>CURRENT MAPPING</div>
                                    <div style={{ fontSize: '1.75rem', fontWeight: '800', letterSpacing: '-0.02em' }}>{mappingResults[activeTab].mapping_id}</div>
                                </div>
                                <div style={{ textAlign: 'right' }}>
                                    <div style={{ fontSize: '0.7rem', opacity: 0.5, fontWeight: '700', marginBottom: '0.25rem' }}>Target Dataset/Table</div>
                                    <div style={{ fontWeight: '700', color: '#1e293b' }}>{mappingResults[activeTab].mapping_info?.target || 'N/A'}</div>
                                    <div style={{ marginTop: '0.75rem', display: 'flex', gap: '1rem', justifyContent: 'flex-end' }}>
                                        <div style={{ fontSize: '0.75rem', border: '1px solid #10b98140', padding: '2px 8px', borderRadius: '6px', color: '#10b981', fontWeight: '700' }}>
                                            PASS: {mappingResults[activeTab].predefined_results.filter(r => r.status === 'PASS').length}
                                        </div>
                                        <div style={{ fontSize: '0.75rem', border: '1px solid #ef444440', padding: '2px 8px', borderRadius: '6px', color: '#ef4444', fontWeight: '700' }}>
                                            FAIL: {mappingResults[activeTab].predefined_results.filter(r => r.status === 'FAIL').length}
                                        </div>
                                    </div>
                                </div>
                            </div>

                            {mappingResults[activeTab].predefined_results.map((t, i) => renderTestCard(t, i, mappingResults[activeTab].mapping_id))}

                            {mappingResults[activeTab].ai_suggestions && mappingResults[activeTab].ai_suggestions.length > 0 && (
                                <div style={{ marginTop: '3rem' }}>
                                    <div style={{ fontSize: '0.75rem', fontWeight: '800', opacity: 0.6, letterSpacing: '0.1em', marginBottom: '1rem' }}>AI RECOMMENDATIONS</div>
                                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(350px, 1fr))', gap: '1.5rem' }}>
                                        {mappingResults[activeTab].ai_suggestions!.map((s, i) => (
                                            <div key={i} className="card" style={{ border: '1px dashed var(--primary)', background: '#f5f7ff' }}>
                                                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '1rem' }}>
                                                    <div style={{ fontWeight: '700', fontSize: '1.1rem' }}>{s.test_name}</div>
                                                    <span style={{ fontSize: '0.625rem', padding: '2px 8px', borderRadius: '8px', background: 'var(--primary)', color: 'white', fontWeight: '800' }}>{s.severity}</span>
                                                </div>
                                                <div style={{ fontSize: '0.875rem', marginBottom: '1.5rem', color: '#4b5563', lineHeight: '1.5' }}>{s.reasoning}</div>
                                                <button
                                                    className="btn btn-primary" style={{ width: '100%', borderRadius: '10px' }}
                                                    onClick={() => handleSaveCustomTest(s, mappingResults[activeTab].mapping_info!.target)}
                                                    disabled={savedTests.has(s.test_name)}
                                                >
                                                    {savedTests.has(s.test_name) ? '✓ Saved to Custom Tests' : 'Add to Collection'}
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
