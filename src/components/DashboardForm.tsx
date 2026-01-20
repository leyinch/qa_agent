"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import HistoryList from "./HistoryList";

type ComparisonMode = 'schema' | 'gcs' | 'history' | 'settings';
type FileFormat = 'csv' | 'json' | 'parquet' | 'avro';

// ... types remain ...

interface DashboardFormProps {
    comparisonMode: ComparisonMode;
}

export default function DashboardForm({ comparisonMode }: DashboardFormProps) {
    const router = useRouter();

    // Common fields
    const [projectId, setProjectId] = useState("");
    const [loading, setLoading] = useState(false);

    // Schema mode state
    const [datasets, setDatasets] = useState<string[]>(['']);
    const [erdDescription, setErdDescription] = useState("");

    // GCS mode state
    // GCS mode state (Config Table only)
    const [configDataset, setConfigDataset] = useState("");
    const [configTable, setConfigTable] = useState("");



    const addDataset = () => setDatasets([...datasets, '']);

    const removeDataset = (index: number) => {
        const newDatasets = datasets.filter((_, i) => i !== index);
        setDatasets(newDatasets);
    };

    const handleDatasetChange = (index: number, value: string) => {
        const newDatasets = [...datasets];
        newDatasets[index] = value;
        setDatasets(newDatasets);
    };

    const handleViewResult = (details: any) => {
        if (!details) {
            alert("No details available for this historical run.");
            return;
        }

        // Normalize details for GCS Single File mode (backward compatibility)
        if (details.results && !details.predefined_results) {
            details.predefined_results = details.results;
            if (!details.summary) {
                const total = details.results.length;
                const passed = details.results.filter((r: any) => r.status === 'PASS').length;
                const failed = details.results.filter((r: any) => r.status === 'FAIL').length;
                const errors = details.results.filter((r: any) => r.status === 'ERROR').length;
                details.summary = { total_tests: total, passed, failed, errors };
            }
        }

        localStorage.setItem("testResults", JSON.stringify(details));
        router.push("/results");
    };

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setLoading(true);

        try {
            const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:8000';
            const endpoint = `${backendUrl}/api/generate-tests`;

            let payload: any = {
                project_id: projectId,
                comparison_mode: comparisonMode
            };

            if (comparisonMode === 'schema') {
                const validDatasets = datasets.filter(d => d.trim() !== '');
                if (validDatasets.length === 0) {
                    throw new Error("Please provide at least one dataset.");
                }
                payload = {
                    ...payload,
                    datasets: validDatasets,
                    erd_description: erdDescription
                };
            } else if (comparisonMode === 'gcs') {
                // Use GCS Config Table mode exclusively
                payload = {
                    ...payload,
                    comparison_mode: 'gcs-config',
                    config_dataset: configDataset,
                    config_table: configTable
                };
            }

            const response = await fetch(endpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(payload),
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.detail || 'Failed to generate tests');
            }

            const data = await response.json();
            localStorage.setItem("projectId", projectId);
            handleViewResult(data);

            // Trigger notification if execution_id is present
            if (data.execution_id) {
                // Ensure we have a summary structure even if backend returns different formats
                let summary = data.summary;
                if (!summary && data.results) {
                    // Calculate summary client-side if missing (fallback)
                    const total = data.results.length;
                    const passed = data.results.filter((r: any) => r.status === 'PASS').length;
                    const failed = data.results.filter((r: any) => r.status === 'FAIL').length;
                    const errors = data.results.filter((r: any) => r.status === 'ERROR').length;
                    summary = { total_tests: total, passed, failed, errors };
                }

                if (summary) {
                    try {
                        const notifyRes = await fetch(`${backendUrl}/api/notify`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                project_id: projectId,
                                execution_id: data.execution_id,
                                summary: summary
                            })
                        });
                        console.log("Notification trigger response:", notifyRes.status);
                    } catch (notifyErr) {
                        console.error("Failed to trigger notification:", notifyErr);
                    }
                }
            }

        } catch (error: any) {
            console.error("Error generating tests:", error);
            alert(error.message || "An error occurred while generating tests.");
        } finally {
            setLoading(false);
        }
    };

    // Settings state
    const [alertEmails, setAlertEmails] = useState("");
    const [teamsWebhook, setTeamsWebhook] = useState("");
    const [alertOnFailure, setAlertOnFailure] = useState(true);

    const fetchSettings = async () => {
        if (!projectId) return;
        try {
            const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:8000';
            const res = await fetch(`${backendUrl}/api/settings?project_id=${projectId}`);
            if (res.ok) {
                const data = await res.json();
                setAlertEmails((data.alert_emails || []).join(', '));
                setTeamsWebhook(data.teams_webhook_url || '');
                setAlertOnFailure(data.alert_on_failure !== undefined ? data.alert_on_failure : true);
            }
        } catch (e) {
            console.error("Failed to fetch settings", e);
        }
    };

    const handleSaveSettings = async (e: React.FormEvent) => {
        e.preventDefault();
        if (!projectId) {
            alert("Project ID is required");
            return;
        }
        setLoading(true);
        try {
            const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:8000';
            const payload = {
                project_id: projectId,
                alert_emails: alertEmails.split(',').map(s => s.trim()).filter(Boolean),
                teams_webhook_url: teamsWebhook,
                alert_on_failure: alertOnFailure
            };

            const res = await fetch(`${backendUrl}/api/settings`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            if (!res.ok) throw new Error("Failed to save settings");
            alert("Settings saved successfully!");
            // No need to close settings, user can navigate away using sidebar
        } catch (error: any) {
            alert(error.message);
        } finally {
            setLoading(false);
        }
    };

    return (
        <form onSubmit={comparisonMode === 'settings' ? handleSaveSettings : handleSubmit} className="card fade-in" style={{ width: '100%', maxWidth: '800px' }}>

            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem', borderBottom: '1px solid var(--border)', paddingBottom: '0.75rem' }}>
                <h2 style={{ fontSize: '1.5rem', fontWeight: '700', margin: 0 }}>
                    {comparisonMode === 'settings' ? 'Alert Settings' :
                        comparisonMode === 'schema' ? 'Schema Validation Setup' :
                            comparisonMode === 'gcs' ? 'GCS Comparison Setup' :
                                'Execution History'
                    }
                </h2>
                {/* Settings button removed, use Sidebar */}
                {comparisonMode === 'settings' && (
                    <button
                        type="button"
                        onClick={fetchSettings} // Allow manually refreshing settings
                        style={{
                            padding: '0.5rem 1rem',
                            background: 'var(--secondary)',
                            border: '1px solid var(--border)',
                            borderRadius: 'var(--radius)',
                            cursor: 'pointer',
                            fontSize: '0.875rem'
                        }}
                    >
                        🔄 Refresh
                    </button>
                )}
            </div>

            {/* Project ID (common field) */}
            <div style={{ marginBottom: '1.75rem' }}>
                <label className="label" htmlFor="projectId">
                    <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                        🔑 Google Cloud Project ID
                    </span>
                </label>
                <input
                    id="projectId"
                    type="text"
                    className="input"
                    value={projectId}
                    onChange={(e) => setProjectId(e.target.value)}
                    required
                    placeholder="e.g., miruna-sandpit"
                    disabled={comparisonMode === 'settings'}
                />
            </div>

            {comparisonMode === 'settings' ? (
                <div className="fade-in">
                    <div style={{ marginBottom: '1.75rem' }}>
                        <label className="label" htmlFor="alertEmails">
                            📧 Alert Emails (Comma separated)
                        </label>
                        <input
                            id="alertEmails"
                            type="text"
                            className="input"
                            value={alertEmails}
                            onChange={(e) => setAlertEmails(e.target.value)}
                            placeholder="user@example.com, input@test.com"
                        />
                    </div>

                    <div style={{ marginBottom: '1.75rem' }}>
                        <label className="label" htmlFor="teamsWebhook">
                            💬 Teams Webhook URL
                        </label>
                        <input
                            id="teamsWebhook"
                            type="text"
                            className="input"
                            value={teamsWebhook}
                            onChange={(e) => setTeamsWebhook(e.target.value)}
                            placeholder="https://miruna.webhook.office.com/webhookb2/..."
                        />
                    </div>

                    <div style={{ marginBottom: '2rem', display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                        <input
                            id="alertOnFailure"
                            type="checkbox"
                            checked={alertOnFailure}
                            onChange={(e) => setAlertOnFailure(e.target.checked)}
                            style={{ width: '1.25rem', height: '1.25rem' }}
                        />
                        <label htmlFor="alertOnFailure" style={{ fontSize: '1rem', cursor: 'pointer' }}>
                            Enable Alerts on Test Failure
                        </label>
                    </div>

                    <button
                        type="submit"
                        className="btn btn-primary"
                        style={{ width: '100%', fontSize: '1rem', padding: '1rem' }}
                        disabled={loading}
                    >
                        {loading ? 'Saving...' : '💾 Save Settings'}
                    </button>
                </div>
            ) : comparisonMode === 'history' ? (
                <HistoryList projectId={projectId} onViewResult={handleViewResult} />
            ) : (
                <>
                    {/* Schema Comparison Mode Fields */}
                    {comparisonMode === 'schema' && (
                        <>
                            {/* Datasets */}
                            <div style={{ marginBottom: '1.75rem' }}>
                                <label className="label">
                                    <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                        📊 BigQuery Datasets
                                    </span>
                                </label>
                                <p style={{ fontSize: '0.875rem', color: 'var(--secondary-foreground)', marginBottom: '1rem' }}>
                                    Add one or more datasets to test. You can test across multiple datasets.
                                </p>

                                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                                    {datasets.map((dataset, index) => (
                                        <div key={index} style={{
                                            display: 'flex',
                                            gap: '0.75rem',
                                            padding: '0.75rem',
                                            background: 'var(--secondary)',
                                            borderRadius: 'var(--radius)',
                                            border: '1px solid var(--border)'
                                        }}>
                                            <span style={{
                                                display: 'flex',
                                                alignItems: 'center',
                                                justifyContent: 'center',
                                                minWidth: '2rem',
                                                height: '2.5rem',
                                                background: 'var(--gradient-primary)',
                                                borderRadius: '8px',
                                                color: 'white',
                                                fontWeight: '600',
                                                fontSize: '0.875rem'
                                            }}>
                                                {index + 1}
                                            </span>
                                            <input
                                                type="text"
                                                className="input"
                                                value={dataset}
                                                onChange={(e) => handleDatasetChange(index, e.target.value)}
                                                placeholder={`Dataset ${index + 1} (e.g., ecommerce_data)`}
                                                style={{ flex: 1, marginBottom: 0 }}
                                            />
                                            {datasets.length > 1 && (
                                                <button
                                                    type="button"
                                                    onClick={() => removeDataset(index)}
                                                    style={{
                                                        padding: '0 1rem',
                                                        backgroundColor: 'var(--error)',
                                                        color: 'white',
                                                        border: 'none',
                                                        borderRadius: 'var(--radius)',
                                                        cursor: 'pointer',
                                                        fontWeight: '600',
                                                        fontSize: '0.875rem',
                                                        transition: 'all 0.2s ease',
                                                        minWidth: '80px'
                                                    }}
                                                    onMouseEnter={(e) => e.currentTarget.style.transform = 'scale(1.05)'}
                                                    onMouseLeave={(e) => e.currentTarget.style.transform = 'scale(1)'}
                                                >
                                                    Remove
                                                </button>
                                            )}
                                        </div>
                                    ))}
                                </div>

                                <button
                                    type="button"
                                    onClick={addDataset}
                                    style={{
                                        marginTop: '1rem',
                                        padding: '0.75rem 1.25rem',
                                        backgroundColor: 'var(--secondary)',
                                        color: 'var(--primary)',
                                        border: '2px dashed var(--primary)',
                                        borderRadius: 'var(--radius)',
                                        cursor: 'pointer',
                                        fontWeight: '600',
                                        fontSize: '0.875rem',
                                        width: '100%',
                                        transition: 'all 0.2s ease'
                                    }}
                                    onMouseEnter={(e) => {
                                        e.currentTarget.style.background = 'var(--primary)';
                                        e.currentTarget.style.color = 'white';
                                    }}
                                    onMouseLeave={(e) => {
                                        e.currentTarget.style.background = 'var(--secondary)';
                                        e.currentTarget.style.color = 'var(--primary)';
                                    }}
                                >
                                    + Add Another Dataset
                                </button>
                            </div>

                            {/* ERD Description */}
                            <div style={{ marginBottom: '2rem' }}>
                                <label className="label" htmlFor="erdDescription">
                                    <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                        📝 ER Diagram Description / Schema
                                    </span>
                                </label>
                                <textarea
                                    id="erdDescription"
                                    className="input"
                                    value={erdDescription}
                                    onChange={(e) => setErdDescription(e.target.value)}
                                    required
                                    placeholder="Describe your table relationships, primary keys, foreign keys, and expected data constraints..."
                                    rows={8}
                                    style={{
                                        resize: 'vertical',
                                        fontFamily: 'JetBrains Mono, monospace',
                                        fontSize: '0.875rem',
                                        lineHeight: '1.6'
                                    }}
                                />
                                <p style={{ fontSize: '0.8125rem', color: 'var(--secondary-foreground)', marginTop: '0.75rem', fontStyle: 'italic' }}>
                                    💡 Tip: Describe table relationships across all datasets for comprehensive testing
                                </p>
                            </div>
                        </>
                    )}

                    {/* GCS Comparison Mode Fields - Config Table ONLY */}
                    {comparisonMode === 'gcs' && (
                        <>
                            <div style={{ marginBottom: '1.75rem' }}>
                                <label className="label" htmlFor="configDataset">
                                    <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                        📁 Config Dataset
                                    </span>
                                </label>
                                <input
                                    id="configDataset"
                                    type="text"
                                    className="input"
                                    value={configDataset}
                                    onChange={(e) => setConfigDataset(e.target.value)}
                                    required
                                    placeholder="e.g., config"
                                />
                            </div>

                            <div style={{ marginBottom: '1.75rem' }}>
                                <label className="label" htmlFor="configTable">
                                    <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                        📊 Config Table Name
                                    </span>
                                </label>
                                <input
                                    id="configTable"
                                    type="text"
                                    className="input"
                                    value={configTable}
                                    onChange={(e) => setConfigTable(e.target.value)}
                                    required
                                    placeholder="e.g., data_load_config"
                                />
                                <p style={{ fontSize: '0.8125rem', color: 'var(--secondary-foreground)', marginTop: '0.75rem', fontStyle: 'italic' }}>
                                    💡 The config table contains all GCS-to-BigQuery mappings and test configurations
                                </p>
                            </div>
                        </>
                    )}

                    {/* Submit Button */}
                    <button
                        type="submit"
                        className="btn btn-primary"
                        style={{ width: '100%', fontSize: '1rem', padding: '1rem' }}
                        disabled={loading}
                    >
                        {loading ? (
                            <span style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                                <span className="loading">⏳</span>
                                {comparisonMode === 'gcs' ? 'Comparing GCS File...' : 'Generating Test Cases...'}
                            </span>
                        ) : (
                            <span style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                                <span>🚀</span>
                                {comparisonMode === 'gcs' ? 'Compare & Test' : 'Generate & Run Tests'}
                            </span>
                        )}
                    </button>
                </>
            )}
        </form>
    );
}
