"use client";

import React, { useEffect, useState } from "react";
import { PieChart, Pie, Cell, Tooltip as RechartsTooltip } from "recharts";

interface HistoryItem {
    execution_id: string;
    timestamp: string;
    project_id: string;
    comparison_mode: string;
    source: string;
    target: string;
    status: string;
    total_tests: number;
    passed_tests: number;
    failed_tests: number;
    details: Record<string, unknown> | unknown[];
}

interface HistoryListProps {
    projectId: string;
    onViewResult: (details: unknown, fromHistory?: boolean) => void;
}

export default function HistoryList({ projectId, onViewResult }: HistoryListProps) {
    const [history, setHistory] = useState<HistoryItem[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");

    const fetchHistory = async () => {
        setLoading(true);
        setError("");
        try {
            const res = await fetch(`/api/history?project_id=${projectId}&limit=50`);
            if (!res.ok) {
                throw new Error("Failed to fetch history");
            }
            const data = await res.json();
            // Backend now returns pre-aggregated history items compatible with HistoryItem interface
            setHistory(data);
        } catch (err: unknown) {
            const message = err instanceof Error ? err.message : "An error occurred";
            setError(message);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchHistory();
    }, []);

    const handleViewClick = (run: HistoryItem) => {
        // Pass the whole run object so we can extract metadata like execution_id
        onViewResult(run, true);
    };

    const getStatusColor = (status: string) => {
        switch (status?.toUpperCase()) {
            case 'PASS': return 'var(--success-text)';
            case 'FAIL': return 'var(--error-text)';
            case 'AT_RISK': return 'var(--warning-text)';
            default: return 'var(--secondary-foreground)';
        }
    };

    const getStatusBadge = (status: string) => {
        const color = getStatusColor(status);
        const bg = status === 'PASS' ? 'rgba(34, 197, 94, 0.1)' :
            status === 'FAIL' ? 'rgba(239, 68, 68, 0.1)' : 'rgba(234, 179, 8, 0.1)';

        return (
            <span style={{
                backgroundColor: bg,
                color: color,
                padding: '0.25rem 0.75rem',
                borderRadius: '9999px',
                fontSize: '0.75rem',
                fontWeight: '600',
                border: `1px solid ${color}40`,
                display: 'inline-flex',
                alignItems: 'center',
                gap: '0.25rem'
            }}>
                {status === 'PASS' && '✅'}
                {status === 'FAIL' && '❌'}
                {status === 'AT_RISK' && '⚠️'}
                {status || 'UNKNOWN'}
            </span>
        );
    };

    if (loading && history.length === 0) {
        return <div style={{ textAlign: 'center', padding: '2rem' }}>Loading history...</div>;
    }

    if (error) {
        return (
            <div style={{ padding: '1rem', color: 'var(--error-text)', textAlign: 'center' }}>
                Error: {error}
                <br />
                <button
                    onClick={fetchHistory}
                    className="btn btn-outline"
                    style={{ marginTop: '1rem' }}
                >
                    Retry
                </button>
            </div>
        );
    }

    return (
        <div style={{ width: '100%' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem' }}>
                <div></div> {/* Empty div to keep Refresh button on the right */}
                <div style={{ display: 'flex', gap: '0.5rem' }}>
                    <button
                        onClick={async () => {
                            setLoading(true);
                            try {
                                const res = await fetch(`/api/history?project_id=${projectId}`, { method: 'DELETE' });
                                if (res.ok) {
                                    // Clear the history state immediately
                                    setHistory([]);
                                    // Then fetch fresh data
                                    await fetchHistory();
                                } else {
                                    const errorText = await res.text();
                                    console.error('Failed to clear history:', errorText);
                                    alert('Failed to clear history');
                                }
                            } catch (err) {
                                console.error('Delete all failed:', err);
                                alert('Error clearing history');
                            } finally {
                                setLoading(false);
                            }
                        }}
                        className="btn btn-outline"
                        style={{
                            padding: '0.5rem 1rem',
                            fontSize: '0.875rem',
                            backgroundColor: '#fee2e2',
                            color: '#991b1b',
                            border: '1px solid #fecaca'
                        }}
                        disabled={loading}
                    >
                        Delete All
                    </button>
                    <button
                        onClick={fetchHistory}
                        className="btn btn-outline"
                        style={{ padding: '0.5rem 1rem', fontSize: '0.875rem' }}
                        disabled={loading}
                    >
                        {loading ? 'Refreshing...' : 'Refresh'}
                    </button>
                </div>
            </div>

            {
                history.length === 0 ? (
                    <div style={{ textAlign: 'center', color: 'var(--secondary-foreground)', padding: '2rem' }}>
                        No execution history found.
                    </div>
                ) : (
                    <div style={{ width: '100%', borderRadius: 'var(--radius)', border: '1px solid var(--border)' }}>
                        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.875rem', tableLayout: 'fixed' }}>
                            <thead>
                                <tr style={{ background: 'var(--secondary)', borderBottom: '1px solid var(--border)', textAlign: 'left' }}>
                                    <th style={{ padding: '0.75rem 1rem', width: '90px' }}>ID</th>
                                    <th style={{ padding: '0.75rem 1rem', width: '180px' }}>Execution Time</th>
                                    <th style={{ padding: '0.75rem 1rem', width: '90px' }}>Mode</th>
                                    <th style={{ padding: '0.75rem 1.5rem 0.75rem 1rem' }}>Source / Target</th>
                                    <th style={{ padding: '0.75rem 1rem', width: '140px' }}>Status</th>
                                    <th style={{ padding: '0.75rem 1rem', textAlign: 'center', width: '120px' }}>Distribution</th>
                                    <th style={{ padding: '0.75rem 1rem', textAlign: 'center', width: '120px' }}>Action</th>
                                </tr>
                            </thead>
                            <tbody>
                                {history.map((run) => {
                                    const passed = run.passed_tests || 0;
                                    const failed = run.failed_tests || 0;
                                    const other = (run.total_tests || 0) - passed - failed; // e.g. errors or skipped
                                    const chartData = [
                                        { name: 'Passed', value: passed, color: '#10b981' },
                                        { name: 'Failed', value: failed, color: '#ef4444' },
                                        ...(other > 0 ? [{ name: 'Other', value: other, color: '#94a3b8' }] : [])
                                    ];

                                    return (
                                        <tr key={run.execution_id} style={{ borderBottom: '1px solid var(--border)' }}>
                                            <td style={{ padding: '0.75rem 1rem', fontFamily: 'monospace', fontSize: '0.7rem' }} title={run.execution_id}>
                                                {run.execution_id?.substring(0, 8)}
                                            </td>
                                            <td style={{ padding: '0.75rem 1rem', whiteSpace: 'nowrap' }}>
                                                {(() => {
                                                    const cleanTimestamp = run.timestamp
                                                        .replace(' UTC', '')
                                                        .replace('Z', '')
                                                        .replace(' ', 'T');
                                                    return new Date(cleanTimestamp).toLocaleString([], {
                                                        year: 'numeric',
                                                        month: 'numeric',
                                                        day: 'numeric',
                                                        hour: '2-digit',
                                                        minute: '2-digit'
                                                    });
                                                })()}
                                            </td>
                                            <td style={{ padding: '0.75rem 1rem', textTransform: 'uppercase', fontWeight: '600', color: 'var(--primary)' }}>
                                                {run.comparison_mode?.toLowerCase().includes('scd') ? 'SCD' : run.comparison_mode?.replace('_', ' ')}
                                            </td>
                                            <td style={{ padding: '0.75rem 1rem' }}>
                                                {!run.comparison_mode?.toLowerCase().includes('scd') && run.source !== 'SCD Validation' && run.source !== 'SCD' && (
                                                    <div style={{ display: 'flex', alignItems: 'flex-start', gap: '0.5rem', marginBottom: '0.35rem' }}>
                                                        <span style={{
                                                            fontSize: '10px',
                                                            fontWeight: '800',
                                                            backgroundColor: '#dbeafe',
                                                            color: '#1e40af',
                                                            padding: '0.1rem 0.4rem',
                                                            borderRadius: '4px',
                                                            minWidth: '34px',
                                                            textAlign: 'center',
                                                            display: 'inline-block',
                                                            lineHeight: '1.4'
                                                        }}>SRC</span>
                                                        <span style={{ color: 'var(--foreground)', fontSize: '0.75rem', wordBreak: 'break-word', lineHeight: '1.4' }}>{run.source}</span>
                                                    </div>
                                                )}
                                                <div style={{ display: 'flex', alignItems: 'flex-start', gap: '0.5rem' }}>
                                                    <span style={{
                                                        fontSize: '10px',
                                                        fontWeight: '800',
                                                        backgroundColor: '#f3e8ff',
                                                        color: '#6b21a8',
                                                        padding: '0.1rem 0.4rem',
                                                        borderRadius: '4px',
                                                        minWidth: '34px',
                                                        textAlign: 'center',
                                                        display: 'inline-block',
                                                        lineHeight: '1.4'
                                                    }}>TGT</span>
                                                    <span style={{ color: 'var(--foreground)', fontSize: '0.75rem', wordBreak: 'break-word', lineHeight: '1.4' }}>{run.target}</span>
                                                </div>
                                            </td>
                                            <td style={{ padding: '0.75rem 1rem' }}>
                                                {getStatusBadge(run.status)}
                                            </td>
                                            <td style={{ padding: '0.75rem 1rem', textAlign: 'center', minWidth: '100px' }}>
                                                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '0.5rem' }}>
                                                    <PieChart width={50} height={50}>
                                                        <Pie
                                                            data={chartData}
                                                            cx={25}
                                                            cy={25}
                                                            innerRadius={10}
                                                            outerRadius={25}
                                                            paddingAngle={2}
                                                            dataKey="value"
                                                            stroke="none"
                                                        >
                                                            {chartData.map((entry, index) => (
                                                                <Cell key={`cell-${index}`} fill={entry.color} />
                                                            ))}
                                                        </Pie>
                                                        <RechartsTooltip />
                                                    </PieChart>
                                                    <div style={{ fontSize: '0.75rem', color: 'var(--secondary-foreground)', display: 'flex', flexDirection: 'column', alignItems: 'flex-start' }}>
                                                        <span style={{ color: '#10b981' }}>Pass: {passed}</span>
                                                        {(failed > 0) && <span style={{ color: '#ef4444' }}>Fail: {failed}</span>}
                                                    </div>
                                                </div>
                                            </td>
                                            <td style={{ padding: '0.75rem 1rem', textAlign: 'center' }}>
                                                <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'center', alignItems: 'center' }}>
                                                    <button
                                                        onClick={() => handleViewClick(run)}
                                                        style={{
                                                            background: 'none',
                                                            border: 'none',
                                                            color: 'var(--primary)',
                                                            cursor: 'pointer',
                                                            textDecoration: 'underline',
                                                            fontWeight: '600',
                                                            fontSize: '0.875rem'
                                                        }}
                                                    >
                                                        View Result
                                                    </button>
                                                    <button
                                                        onClick={async (e: React.MouseEvent) => {
                                                            e.stopPropagation();
                                                            if (confirm('Are you sure you want to delete this execution history?')) {
                                                                try {
                                                                    const res = await fetch(`/api/history/${run.execution_id}?project_id=${projectId}`, {
                                                                        method: 'DELETE',
                                                                    });
                                                                    if (res.ok) {
                                                                        fetchHistory();
                                                                    } else {
                                                                        alert('Failed to delete history');
                                                                    }
                                                                } catch (err) {
                                                                    console.error("Delete failed", err);
                                                                    alert('Error deleting history');
                                                                }
                                                            }
                                                        }}
                                                        style={{
                                                            background: 'none',
                                                            border: 'none',
                                                            cursor: 'pointer',
                                                            fontSize: '1rem'
                                                        }}
                                                        title="Delete Execution"
                                                    >
                                                        🗑️
                                                    </button>
                                                </div>
                                            </td>
                                        </tr>
                                    );
                                })}
                            </tbody>
                        </table>
                    </div>
                )
            }
        </div >
    );
}
