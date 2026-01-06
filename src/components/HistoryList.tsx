"use client";

import { useEffect, useState } from "react";
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
    details: any;
}

interface HistoryListProps {
    projectId: string;
    onViewResult: (details: any) => void;
}

export default function HistoryList({ projectId, onViewResult }: HistoryListProps) {
    const [history, setHistory] = useState<HistoryItem[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");

    const fetchHistory = async () => {
        if (!projectId) return;
        setLoading(true);
        setError("");
        try {
            const res = await fetch(`/api/history?project_id=${projectId}&limit=50`);
            if (!res.ok) {
                throw new Error("Failed to fetch history");
            }
            const data = await res.json();
            setHistory(data);
        } catch (err: any) {
            setError(err.message || "An error occurred");
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchHistory();
    }, []);

    const handleViewClick = (run: HistoryItem) => {
        let details = run.details;
        if (typeof details === 'string') {
            try {
                details = JSON.parse(details);
            } catch (e) {
                console.error("Failed to parse details JSON", e);
                alert("Error parsing result details.");
                return;
            }
        }
        onViewResult(details);
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

    if (!projectId) {
        return (
            <div style={{ textAlign: 'center', padding: '3rem', background: 'var(--secondary)', borderRadius: 'var(--radius)', border: '1px solid var(--border)' }}>
                <div style={{ fontSize: '2rem', marginBottom: '1rem' }}>🆔</div>
                <h3 style={{ fontSize: '1.125rem', fontWeight: '600', marginBottom: '0.5rem' }}>Project ID Required</h3>
                <p style={{ color: 'var(--secondary-foreground)', fontSize: '0.875rem' }}>
                    Please enter a Google Cloud Project ID above to see execution history.
                </p>
            </div>
        );
    }

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
                <h3 style={{ fontSize: '1.25rem', fontWeight: '600', margin: 0 }}>Execution History</h3>
                <button
                    onClick={fetchHistory}
                    className="btn btn-outline"
                    style={{ padding: '0.5rem 1rem', fontSize: '0.875rem' }}
                    disabled={loading}
                >
                    {loading ? 'Refreshing...' : '🔄 Refresh'}
                </button>
            </div>

            {history.length === 0 ? (
                <div style={{ textAlign: 'center', color: 'var(--secondary-foreground)', padding: '2rem' }}>
                    No execution history found.
                </div>
            ) : (
                <div style={{ overflowX: 'auto', borderRadius: 'var(--radius)', border: '1px solid var(--border)' }}>
                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.875rem' }}>
                        <thead>
                            <tr style={{ background: 'var(--secondary)', borderBottom: '1px solid var(--border)', textAlign: 'left' }}>
                                <th style={{ padding: '0.75rem 1rem' }}>Time</th>
                                <th style={{ padding: '0.75rem 1rem' }}>Mode</th>
                                <th style={{ padding: '0.75rem 1rem' }}>Source / Target</th>
                                <th style={{ padding: '0.75rem 1rem' }}>Status</th>
                                <th style={{ padding: '0.75rem 1rem', textAlign: 'center' }}>Distribution</th>
                                <th style={{ padding: '0.75rem 1rem', textAlign: 'center' }}>Action</th>
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
                                        <td style={{ padding: '0.75rem 1rem' }}>
                                            {new Date(run.timestamp).toLocaleString()}
                                        </td>
                                        <td style={{ padding: '0.75rem 1rem', textTransform: 'capitalize' }}>
                                            {run.comparison_mode?.replace('_', ' ')}
                                        </td>
                                        <td style={{ padding: '0.75rem 1rem' }}>
                                            <div style={{ fontSize: '0.75rem', color: 'var(--secondary-foreground)' }}>Src: <span style={{ color: 'var(--foreground)' }}>{run.source}</span></div>
                                            <div style={{ fontSize: '0.75rem', color: 'var(--secondary-foreground)' }}>Tgt: <span style={{ color: 'var(--foreground)' }}>{run.target}</span></div>
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
                                        </td>
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>
                </div>
            )}
        </div>
    );
}
