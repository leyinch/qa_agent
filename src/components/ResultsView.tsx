"use client";

import { useEffect, useState } from "react";
import { PieChart, Pie, Cell, Tooltip, Legend, ResponsiveContainer, BarChart, Bar, XAxis, YAxis, CartesianGrid } from "recharts";

interface TestResult {
    test_name: string;
    description: string;
    sql_query: string;
    severity: string;
    status: "PASS" | "FAIL" | "ERROR";
    failed_rows?: number;
    error_message?: string;
}

const COLORS = {
    PASS: "#10b981", // Green
    FAIL: "#ef4444", // Red
    ERROR: "#f59e0b", // Amber
};

export default function ResultsView() {
    const [results, setResults] = useState<TestResult[]>([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const data = localStorage.getItem("testResults");
        if (data) {
            try {
                const parsed = JSON.parse(data);
                setResults(parsed.results || []);
            } catch (e) {
                console.error("Failed to parse results", e);
            }
        }
        setLoading(false);
    }, []);

    if (loading) return <div>Loading results...</div>;
    if (results.length === 0) return <div>No results found. Please run a test first.</div>;

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
        <div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '2rem', marginBottom: '2rem' }}>
                {/* Summary Cards */}
                <div className="card" style={{ textAlign: 'center' }}>
                    <h3>Total Tests</h3>
                    <p style={{ fontSize: '3rem', fontWeight: 'bold' }}>{results.length}</p>
                </div>
                <div className="card" style={{ textAlign: 'center', color: COLORS.PASS }}>
                    <h3>Passed</h3>
                    <p style={{ fontSize: '3rem', fontWeight: 'bold' }}>{stats.PASS}</p>
                </div>
                <div className="card" style={{ textAlign: 'center', color: COLORS.FAIL }}>
                    <h3>Failed</h3>
                    <p style={{ fontSize: '3rem', fontWeight: 'bold' }}>{stats.FAIL}</p>
                </div>
            </div>

            <div className="card" style={{ marginBottom: '2rem', height: '400px' }}>
                <h3>Test Results Overview</h3>
                <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                        <Pie
                            data={chartData}
                            cx="50%"
                            cy="50%"
                            innerRadius={60}
                            outerRadius={100}
                            fill="#8884d8"
                            paddingAngle={5}
                            dataKey="value"
                        >
                            {chartData.map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={COLORS[entry.name.toUpperCase() as keyof typeof COLORS]} />
                            ))}
                        </Pie>
                        <Tooltip />
                        <Legend />
                    </PieChart>
                </ResponsiveContainer>
            </div>

            <div className="card">
                <h3>Detailed Results</h3>
                <div style={{ overflowX: 'auto' }}>
                    <table style={{ width: '100%', borderCollapse: 'collapse', marginTop: '1rem' }}>
                        <thead>
                            <tr style={{ borderBottom: '2px solid var(--border)', textAlign: 'left' }}>
                                <th style={{ padding: '1rem' }}>Status</th>
                                <th style={{ padding: '1rem' }}>Test Name</th>
                                <th style={{ padding: '1rem' }}>Severity</th>
                                <th style={{ padding: '1rem' }}>Failed Rows</th>
                                <th style={{ padding: '1rem' }}>Description</th>
                                <th style={{ padding: '1rem' }}>Error/Details</th>
                            </tr>
                        </thead>
                        <tbody>
                            {results.map((result, idx) => (
                                <>
                                    <tr key={idx} style={{ borderBottom: '1px solid var(--border)' }}>
                                        <td style={{ padding: '1rem', fontWeight: 'bold', color: COLORS[result.status] }}>
                                            {result.status}
                                        </td>
                                        <td style={{ padding: '1rem' }}>{result.test_name}</td>
                                        <td style={{ padding: '1rem' }}>{result.severity}</td>
                                        <td style={{ padding: '1rem' }}>{result.failed_rows ?? 0}</td>
                                        <td style={{ padding: '1rem' }}>{result.description}</td>
                                        <td style={{ padding: '1rem' }}>
                                            {result.status === 'ERROR' && result.error_message && (
                                                <div style={{
                                                    backgroundColor: 'var(--secondary)',
                                                    padding: '0.5rem',
                                                    borderRadius: '4px',
                                                    fontSize: '0.875rem',
                                                    color: '#ef4444',
                                                    maxWidth: '400px',
                                                    overflow: 'auto'
                                                }}>
                                                    {result.error_message}
                                                </div>
                                            )}
                                            {result.status !== 'ERROR' && (
                                                <details style={{ cursor: 'pointer' }}>
                                                    <summary style={{ fontSize: '0.875rem', color: 'var(--primary)' }}>
                                                        View SQL
                                                    </summary>
                                                    <pre style={{
                                                        backgroundColor: 'var(--secondary)',
                                                        padding: '0.5rem',
                                                        borderRadius: '4px',
                                                        fontSize: '0.75rem',
                                                        overflow: 'auto',
                                                        marginTop: '0.5rem'
                                                    }}>
                                                        {result.sql_query}
                                                    </pre>
                                                </details>
                                            )}
                                        </td>
                                    </tr>
                                </>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    );
}
