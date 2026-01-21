"use client";

import { useEffect, useState } from "react";

interface HistoryItem {
    execution_id: string;
    execution_timestamp: string;
    timestamp?: string; // Compatibility
    project_id: string;
    comparison_mode: string;
    source?: string;
    target?: string;
    target_dataset?: string;
    target_table?: string;
    status: string;
    total_tests: number;
    passed_tests: number;
    failed_tests: number;
    details?: any[];
}

interface HistoryListProps {
    projectId: string;
    onViewResult: (details: any) => void;
    showToast: (message: string, type: 'success' | 'error' | 'info') => void;
}

export default function HistoryList({ projectId, onViewResult, showToast }: HistoryListProps) {
    const [history, setHistory] = useState<HistoryItem[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");

    const fetchHistory = async () => {
        if (!projectId) return;
        setLoading(true);
        setError("");
        try {
            const apiBase = '/api'; // Backend merged to unified /api
            const res = await fetch(`${apiBase}/history?project_id=${projectId}&limit=500`);
            if (!res.ok) throw new Error("Failed to fetch history");

            const rawData = await res.json();

            // Group by execution_id
            const grouped: Record<string, HistoryItem> = {};

            rawData.forEach((row: any) => {
                const execId = row.execution_id || 'manual_' + (row.timestamp || row.execution_timestamp);

                if (!grouped[execId]) {
                    grouped[execId] = {
                        execution_id: execId,
                        execution_timestamp: row.execution_timestamp || row.timestamp,
                        project_id: row.project_id,
                        comparison_mode: row.comparison_mode,
                        source: row.source,
                        target: row.target,
                        target_dataset: row.target_dataset,
                        target_table: row.target_table,
                        status: 'PASS',
                        total_tests: 0,
                        passed_tests: 0,
                        failed_tests: 0,
                        details: []
                    };
                }

                const group = grouped[execId];
                group.total_tests += row.total_tests || 0;
                group.passed_tests += row.passed_tests || 0;
                group.failed_tests += row.failed_tests || 0;
                group.details?.push(row);

                if (row.status !== 'PASS') {
                    group.status = 'FAIL';
                }
            });

            const aggregatedHistory = Object.values(grouped).sort((a, b) =>
                new Date(b.execution_timestamp).getTime() - new Date(a.execution_timestamp).getTime()
            );

            setHistory(aggregatedHistory);
        } catch (err: any) {
            setError(err.message || "An error occurred");
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchHistory();
    }, [projectId]);

    const handleClearHistory = async () => {
        if (!projectId) return;
        if (!confirm("Are you sure you want to clear the ENTIRE execution history for this project?")) return;

        setLoading(true);
        try {
            const res = await fetch(`/api/history?project_id=${projectId}`, { method: 'DELETE' });
            if (!res.ok) throw new Error("Failed to clear history");
            fetchHistory();
            showToast("History cleared successfully", "success");
        } catch (err: any) {
            showToast(err.message, "error");
        } finally {
            setLoading(false);
        }
    };

    const handleViewClick = (run: HistoryItem) => {
        // Normalize for ResultsView
        const isBatchMode = run.comparison_mode === 'scd-config' || run.comparison_mode === 'gcs-config';

        const normalized: any = {
            summary: {
                total_tests: run.total_tests,
                passed: run.passed_tests,
                failed: run.failed_tests,
                errors: (run.total_tests - run.passed_tests - run.failed_tests)
            },
            comparison_mode: run.comparison_mode,
            project_id: run.project_id,
            target_table: run.target_table || run.target,
        };

        if (isBatchMode) {
            // Group details back into mappings if possible
            const mappingGroups: Record<string, any> = {};
            run.details?.forEach(d => {
                const mid = d.mapping_id || 'unknown';
                if (!mappingGroups[mid]) {
                    mappingGroups[mid] = {
                        mapping_id: mid,
                        mapping_info: { source: d.source, target: d.target, file_row_count: 0, table_row_count: 0 },
                        predefined_results: []
                    };
                }
                mappingGroups[mid].predefined_results.push(d);
            });
            normalized.results_by_mapping = Object.values(mappingGroups);
            normalized.summary.total_mappings = normalized.results_by_mapping.length;
        } else {
            normalized.predefined_results = run.details;
        }

        onViewResult(normalized);
    };

    const getStatusBadge = (status: string) => {
        const isPass = status === 'PASS';
        return (
            <span style={{
                backgroundColor: isPass ? 'rgba(34, 197, 94, 0.1)' : 'rgba(239, 68, 68, 0.1)',
                color: isPass ? 'var(--success-text)' : 'var(--error-text)',
                padding: '0.25rem 0.75rem',
                borderRadius: '9999px',
                fontSize: '0.75rem',
                fontWeight: '600',
                border: `1px solid ${isPass ? '#22c55e40' : '#ef444440'}`,
                display: 'inline-flex',
                alignItems: 'center',
                gap: '0.25rem'
            }}>
                {isPass ? '✅ PASS' : '❌ FAIL'}
            </span>
        );
    };

    if (loading && history.length === 0) return <div style={{ textAlign: 'center', padding: '2rem' }}>Loading...</div>;

    return (
        <div style={{ width: '100%' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem' }}>
                <h3 style={{ fontSize: '1.25rem', fontWeight: '600', margin: 0 }}>History</h3>
                <div style={{ display: 'flex', gap: '0.75rem' }}>
                    <button type="button" onClick={handleClearHistory} className="btn btn-outline" style={{ color: 'var(--error-text)', borderColor: 'var(--error-text)' }}>🗑️ Clear</button>
                    <button type="button" onClick={fetchHistory} className="btn btn-outline">🔄 Refresh</button>
                </div>
            </div>

            {history.length === 0 ? (
                <div style={{ textAlign: 'center', color: 'var(--secondary-foreground)', padding: '2rem' }}>No history.</div>
            ) : (
                <div style={{ overflowX: 'auto', borderRadius: 'var(--radius)', border: '1px solid var(--border)' }}>
                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.875rem' }}>
                        <thead>
                            <tr style={{ background: 'var(--secondary)', borderBottom: '1px solid var(--border)', textAlign: 'left' }}>
                                <th style={{ padding: '0.75rem 1rem' }}>Time</th>
                                <th style={{ padding: '0.75rem 1rem' }}>Mode</th>
                                <th style={{ padding: '0.75rem 1rem' }}>Target</th>
                                <th style={{ padding: '0.75rem 1rem' }}>Status</th>
                                <th style={{ padding: '0.75rem 1rem', textAlign: 'center' }}>Progress</th>
                                <th style={{ padding: '0.75rem 1rem', textAlign: 'center' }}>Action</th>
                            </tr>
                        </thead>
                        <tbody>
                            {history.map((run) => (
                                <tr key={run.execution_id} style={{ borderBottom: '1px solid var(--border)' }}>
                                    <td style={{ padding: '0.75rem 1rem' }}>{new Date(run.execution_timestamp).toLocaleString()}</td>
                                    <td style={{ padding: '0.75rem 1rem', textTransform: 'capitalize' }}>{run.comparison_mode}</td>
                                    <td style={{ padding: '0.75rem 1rem' }}>{run.target_dataset ? `${run.target_dataset}.` : ''}{run.target_table || run.target || '-'}</td>
                                    <td style={{ padding: '0.75rem 1rem' }}>{getStatusBadge(run.status)}</td>
                                    <td style={{ padding: '0.75rem 1rem' }}>
                                        <div style={{ display: 'flex', gap: '4px', height: '8px', background: 'var(--secondary)', borderRadius: '4px', overflow: 'hidden' }}>
                                            <div style={{ width: `${(run.passed_tests / run.total_tests) * 100}%`, background: '#22c55e' }} />
                                            <div style={{ width: `${(run.failed_tests / run.total_tests) * 100}%`, background: '#ef4444' }} />
                                        </div>
                                    </td>
                                    <td style={{ padding: '0.75rem 1rem', textAlign: 'center' }}>
                                        <button onClick={() => handleViewClick(run)} style={{ background: 'none', border: 'none', color: 'var(--primary)', cursor: 'pointer', fontWeight: '600' }}>View</button>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}
        </div>
    );
}
