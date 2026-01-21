"use client";

import { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import HistoryList from "./HistoryList";

type ComparisonMode = 'schema' | 'gcs' | 'scd' | 'history' | 'settings';
type FileFormat = 'csv' | 'json' | 'parquet' | 'avro';
type SCDMode = 'config' | 'manual';
type GCSMode = 'single' | 'config';

interface CustomTest {
    name: string;
    description: string;
    sql: string;
    severity: "LOW" | "MEDIUM" | "HIGH";
}

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

    // GCS / SCD Config common
    const [configDataset, setConfigDataset] = useState("config");
    const [configTable, setConfigTable] = useState("scd_validation_config");

    // SCD mode state
    const [scdMode, setScdMode] = useState<SCDMode>('config');
    const [scdType, setScdType] = useState<'scd1' | 'scd2'>('scd2');
    const [scdTargetDataset, setScdTargetDataset] = useState("");
    const [scdTargetTable, setScdTargetTable] = useState("");
    const [primaryKeys, setPrimaryKeys] = useState("");
    const [surrogateKey, setSurrogateKey] = useState("");
    const [beginDateColumn, setBeginDateColumn] = useState("DWBeginEffDateTime");
    const [endDateColumn, setEndDateColumn] = useState("DWEndEffDateTime");
    const [activeFlagColumn, setActiveFlagColumn] = useState("DWCurrentRowFlag");
    const [customTests, setCustomTests] = useState<CustomTest[]>([]);

    // Settings state
    const [alertEmails, setAlertEmails] = useState("");
    const [teamsWebhook, setTeamsWebhook] = useState("");
    const [alertOnFailure, setAlertOnFailure] = useState(true);

    // Toast / Feedback
    const [toast, setToast] = useState<{ message: string; type: 'success' | 'error' | 'info' } | null>(null);
    const showToast = (message: string, type: 'success' | 'error' | 'info' = 'info') => {
        setToast({ message, type });
        setTimeout(() => setToast(null), 5000);
    };

    // Auto-fetch settings when in settings mode
    useEffect(() => {
        if (comparisonMode === 'settings' && projectId) {
            fetchSettings();
        }
    }, [comparisonMode, projectId]);

    const fetchSettings = async () => {
        try {
            const res = await fetch(`/api/settings?project_id=${projectId}`);
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
        setLoading(true);
        try {
            const payload = {
                project_id: projectId,
                alert_emails: alertEmails.split(',').map(s => s.trim()).filter(Boolean),
                teams_webhook_url: teamsWebhook,
                alert_on_failure: alertOnFailure
            };
            const res = await fetch(`/api/settings`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            if (!res.ok) throw new Error("Failed to save settings");
            showToast("Settings saved successfully!", "success");
        } catch (error: any) {
            showToast(error.message, "error");
        } finally {
            setLoading(false);
        }
    };

    const handleViewResult = (details: any) => {
        localStorage.setItem("testResults", JSON.stringify(details));
        localStorage.setItem("projectId", projectId);
        router.push("/results");
    };

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setLoading(true);

        try {
            let payload: any = {
                project_id: projectId,
                comparison_mode: comparisonMode
            };

            if (comparisonMode === 'schema') {
                payload.datasets = datasets.filter(d => d.trim() !== '');
                payload.erd_description = erdDescription;
            } else if (comparisonMode === 'gcs') {
                payload.comparison_mode = 'gcs-config';
                payload.config_dataset = configDataset;
                payload.config_table = configTable;
            } else if (comparisonMode === 'scd') {
                if (scdMode === 'config') {
                    payload.comparison_mode = 'scd-config';
                    payload.config_dataset = configDataset;
                    payload.config_table = configTable;
                } else {
                    payload.comparison_mode = 'scd';
                    payload.target_dataset = scdTargetDataset;
                    payload.target_table = scdTargetTable;
                    payload.scd_type = scdType;
                    payload.primary_keys = primaryKeys.split(',').map(k => k.trim());
                    payload.surrogate_key = surrogateKey || null;
                    if (scdType === 'scd2') {
                        payload.begin_date_column = beginDateColumn;
                        payload.end_date_column = endDateColumn;
                        payload.active_flag_column = activeFlagColumn;
                    }
                }
            }

            const response = await fetch('/api/generate-tests', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });

            if (!response.ok) {
                const errData = await response.json();
                throw new Error(errData.detail || "Failed to generate tests");
            }

            const data = await response.json();
            handleViewResult(data);

        } catch (error: any) {
            showToast(error.message, "error");
        } finally {
            setLoading(false);
        }
    };

    const addDataset = () => setDatasets([...datasets, '']);
    const handleDatasetChange = (idx: number, val: string) => {
        const d = [...datasets];
        d[idx] = val;
        setDatasets(d);
    };

    return (
        <div style={{ width: '100%', maxWidth: '800px', margin: '0 auto' }}>
            {toast && (
                <div style={{
                    position: 'fixed', top: '2rem', right: '2rem', padding: '1rem 2rem',
                    background: toast.type === 'error' ? 'var(--error-text)' : 'var(--primary)',
                    color: 'white', borderRadius: 'var(--radius)', zIndex: 1000,
                    boxShadow: '0 4px 12px rgba(0,0,0,0.1)', animation: 'slideIn 0.3s ease-out'
                }}>
                    {toast.message}
                </div>
            )}

            <form onSubmit={comparisonMode === 'settings' ? handleSaveSettings : handleSubmit} className="card fade-in">
                <h2 style={{ fontSize: '1.5rem', fontWeight: '700', marginBottom: '1.5rem', borderBottom: '1px solid var(--border)', paddingBottom: '0.75rem' }}>
                    {comparisonMode === 'schema' && 'Schema Validation'}
                    {comparisonMode === 'gcs' && 'GCS Comparison'}
                    {comparisonMode === 'scd' && 'SCD Validation'}
                    {comparisonMode === 'history' && 'Execution History'}
                    {comparisonMode === 'settings' && 'Alert Settings'}
                </h2>

                <div style={{ marginBottom: '1.5rem' }}>
                    <label className="label">🔑 Project ID</label>
                    <input
                        className="input" value={projectId}
                        onChange={e => setProjectId(e.target.value)}
                        placeholder="your-google-cloud-project" required
                    />
                </div>

                {comparisonMode === 'history' && <HistoryList projectId={projectId} onViewResult={handleViewResult} showToast={showToast} />}

                {comparisonMode === 'settings' && (
                    <div className="fade-in">
                        <div style={{ marginBottom: '1.5rem' }}>
                            <label className="label">📧 Alert Emails (comma separated)</label>
                            <input className="input" value={alertEmails} onChange={e => setAlertEmails(e.target.value)} placeholder="user@example.com" />
                        </div>
                        <div style={{ marginBottom: '1.5rem' }}>
                            <label className="label">💬 Teams Webhook URL</label>
                            <input className="input" value={teamsWebhook} onChange={e => setTeamsWebhook(e.target.value)} placeholder="https://outlook.office.com/webhook/..." />
                        </div>
                        <div style={{ marginBottom: '2rem', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                            <input type="checkbox" checked={alertOnFailure} onChange={e => setAlertOnFailure(e.target.checked)} />
                            <label>Enable alerts on failure</label>
                        </div>
                        <button type="submit" className="btn btn-primary" style={{ width: '100%' }} disabled={loading}>{loading ? 'Saving...' : 'Save Settings'}</button>
                    </div>
                )}

                {comparisonMode === 'schema' && (
                    <div className="fade-in">
                        <label className="label">📊 Datasets</label>
                        {datasets.map((d, i) => (
                            <input key={i} className="input" style={{ marginBottom: '0.5rem' }} value={d} onChange={e => handleDatasetChange(i, e.target.value)} placeholder={`Dataset ${i + 1}`} />
                        ))}
                        <button type="button" onClick={addDataset} className="btn btn-outline" style={{ width: '100%', marginBottom: '1.5rem' }}>+ Add Dataset</button>
                        <label className="label">📝 ERD Description</label>
                        <textarea className="input" rows={6} value={erdDescription} onChange={e => setErdDescription(e.target.value)} placeholder="Describe your table relationships..." />
                    </div>
                )}

                {(comparisonMode === 'gcs' || (comparisonMode === 'scd' && scdMode === 'config')) && (
                    <div className="fade-in">
                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
                            <div>
                                <label className="label">📂 Config Dataset</label>
                                <input className="input" value={configDataset} onChange={e => setConfigDataset(e.target.value)} placeholder="config" />
                            </div>
                            <div>
                                <label className="label">📄 Config Table</label>
                                <input className="input" value={configTable} onChange={e => setConfigTable(e.target.value)} placeholder="validation_config" />
                            </div>
                        </div>
                    </div>
                )}

                {comparisonMode === 'scd' && (
                    <div className="fade-in">
                        <div style={{ display: 'flex', gap: '1rem', marginBottom: '1.5rem' }}>
                            <button type="button" className={`btn ${scdMode === 'config' ? 'btn-primary' : 'btn-outline'}`} onClick={() => setScdMode('config')} style={{ flex: 1 }}>Config Table</button>
                            <button type="button" className={`btn ${scdMode === 'manual' ? 'btn-primary' : 'btn-outline'}`} onClick={() => setScdMode('manual')} style={{ flex: 1 }}>Manual Entry</button>
                        </div>

                        {scdMode === 'manual' && (
                            <>
                                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', marginBottom: '1rem' }}>
                                    <div>
                                        <label className="label">Target Dataset</label>
                                        <input className="input" value={scdTargetDataset} onChange={e => setScdTargetDataset(e.target.value)} placeholder="dw_dataset" />
                                    </div>
                                    <div>
                                        <label className="label">Target Table</label>
                                        <input className="input" value={scdTargetTable} onChange={e => setScdTargetTable(e.target.value)} placeholder="dim_customer" />
                                    </div>
                                </div>
                                <div style={{ marginBottom: '1rem' }}>
                                    <label className="label">SCD Type</label>
                                    <select className="input" value={scdType} onChange={e => setScdType(e.target.value as any)}>
                                        <option value="scd1">SCD Type 1</option>
                                        <option value="scd2">SCD Type 2</option>
                                    </select>
                                </div>
                                <div style={{ marginBottom: '1rem' }}>
                                    <label className="label">Primary Keys (comma separated)</label>
                                    <input className="input" value={primaryKeys} onChange={e => setPrimaryKeys(e.target.value)} placeholder="customer_id" />
                                </div>
                                {scdType === 'scd2' && (
                                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '0.5rem' }}>
                                        <div>
                                            <label className="label" style={{ fontSize: '0.75rem' }}>Begin Date</label>
                                            <input className="input" value={beginDateColumn} onChange={e => setBeginDateColumn(e.target.value)} />
                                        </div>
                                        <div>
                                            <label className="label" style={{ fontSize: '0.75rem' }}>End Date</label>
                                            <input className="input" value={endDateColumn} onChange={e => setEndDateColumn(e.target.value)} />
                                        </div>
                                        <div>
                                            <label className="label" style={{ fontSize: '0.75rem' }}>Active Flag</label>
                                            <input className="input" value={activeFlagColumn} onChange={e => setActiveFlagColumn(e.target.value)} />
                                        </div>
                                    </div>
                                )}
                            </>
                        )}
                    </div>
                )}

                {comparisonMode !== 'history' && comparisonMode !== 'settings' && (
                    <button type="submit" className="btn btn-primary" style={{ width: '100%', marginTop: '2rem', padding: '1rem', fontSize: '1.25rem' }} disabled={loading}>
                        {loading ? '🚀 Processing...' : '✨ Generate & Run Tests'}
                    </button>
                )}
            </form>
        </div>
    );
}
