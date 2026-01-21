"use client";

import React, { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import HistoryList from "./HistoryList";

type ComparisonMode = 'schema' | 'gcs' | 'history' | 'scd' | 'settings';
type FileFormat = 'csv' | 'json' | 'parquet' | 'avro';
type GCSMode = 'single' | 'config';
type SCDMode = 'direct' | 'config';

interface CustomTest {
    name: string;
    sql: string;
    description: string;
    severity: string;
}

interface DashboardFormProps {
    comparisonMode: ComparisonMode;
}

export default function DashboardForm({ comparisonMode }: DashboardFormProps) {
    const router = useRouter();

    // Common fields
    const [projectId, setProjectId] = useState("");
    const [loading, setLoading] = useState(false);

    // Persistence: load from localStorage on mount
    useEffect(() => {
        const savedProjectId = localStorage.getItem("projectId");
        if (savedProjectId) {
            setProjectId(savedProjectId);
        }
    }, []);

    // Schema mode state
    const [datasets, setDatasets] = useState<string[]>(['']);
    const [erdDescription, setErdDescription] = useState("");

    // GCS mode state
    const [gcsMode, setGcsMode] = useState<GCSMode>('single');
    const [configDataset, setConfigDataset] = useState("config");
    const [configTable, setConfigTable] = useState("scd_validation_config");
    const [gcsBucket, setGcsBucket] = useState("");
    const [gcsFilePath, setGcsFilePath] = useState("");
    const [fileFormat, setFileFormat] = useState<FileFormat>('csv');
    const [targetDataset, setTargetDataset] = useState("");
    const [targetTable, setTargetTable] = useState("");

    // SCD mode state
    const [scdMode, setScdMode] = useState<SCDMode>('config');
    const [scdType, setScdType] = useState<'scd1' | 'scd2'>('scd2');
    const [primaryKeys, setPrimaryKeys] = useState("");
    const [surrogateKey, setSurrogateKey] = useState("");
    const [beginDateColumn, setBeginDateColumn] = useState("DWBeginEffDateTime");
    const [endDateColumn, setEndDateColumn] = useState("DWEndEffDateTime");
    const [activeFlagColumn, setActiveFlagColumn] = useState("DWCurrentRowFlag");
    const [customTests, setCustomTests] = useState<CustomTest[]>([]);

    // Settings state (from Test1)
    const [alertEmails, setAlertEmails] = useState("");
    const [teamsWebhook, setTeamsWebhook] = useState("");
    const [alertOnFailure, setAlertOnFailure] = useState(true);

    // Column fetching state
    const [availableColumns, setAvailableColumns] = useState<string[]>([]);
    const [scdTargetDataset, setScdTargetDataset] = useState("");
    const [scdTargetTable, setScdTargetTable] = useState("");

    // New config form state (SCD)
    const [showAddConfig, setShowAddConfig] = useState(false);
    const [newConfigId, setNewConfigId] = useState("");
    const [newTargetDataset, setNewTargetDataset] = useState("");
    const [newTargetTable, setNewTargetTable] = useState("");
    const [newScdType, setNewScdType] = useState<'scd1' | 'scd2'>('scd2');
    const [newPrimaryKeys, setNewPrimaryKeys] = useState("");
    const [newSurrogateKey, setNewSurrogateKey] = useState("");
    const [newBeginDateColumn, setNewBeginDateColumn] = useState("DWBeginEffDateTime");
    const [newEndDateColumn, setNewEndDateColumn] = useState("DWEndEffDateTime");
    const [newActiveFlagColumn, setNewActiveFlagColumn] = useState("DWCurrentRowFlag");
    const [newDescription, setNewDescription] = useState("");
    const [newCustomTests, setNewCustomTests] = useState<CustomTest[]>([]);
    const [isEditingExisting, setIsEditingExisting] = useState(false);
    const [fetchingConfig, setFetchingConfig] = useState(false);

    // Toast notification state
    const [toast, setToast] = useState<{ message: string; type: 'success' | 'error' | 'info' } | null>(null);
    const showToast = (message: string, type: 'success' | 'error' | 'info' = 'info') => {
        setToast({ message, type });
        setTimeout(() => setToast(null), 5000);
    };

    // --- Effects & Handlers ---

    // Fetch Settings (Test1)
    const fetchSettings = async () => {
        if (!projectId) return;
        try {
            const endpoint = `/api/settings?project_id=${projectId}`;
            const res = await fetch(endpoint);
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

    // Auto-fetch settings when entering settings mode or changing project
    useEffect(() => {
        if (comparisonMode === 'settings' && projectId) {
            fetchSettings();
        }
    }, [comparisonMode, projectId]);

    // Fetch Columns (Test3)
    useEffect(() => {
        const fetchColumns = async () => {
            if (!projectId || !scdTargetDataset || !scdTargetTable) {
                setAvailableColumns([]);
                return;
            }
            try {
                const endpoint = `/api/table-metadata?project_id=${projectId}&dataset_id=${scdTargetDataset}&table_id=${scdTargetTable}`;
                const response = await fetch(endpoint);
                if (response.ok) {
                    const data = await response.json();
                    if (data.columns) setAvailableColumns(data.columns);
                } else {
                    setAvailableColumns([]);
                }
            } catch (err) {
                setAvailableColumns([]);
            }
        };
        const timeoutId = setTimeout(fetchColumns, 1000);
        return () => clearTimeout(timeoutId);
    }, [projectId, scdTargetDataset, scdTargetTable]);

    // Auto-fill existing config logic
    const fetchExistingConfig = async (dataset: string, table: string) => {
        if (!dataset || !table || !projectId) return;
        setFetchingConfig(true);
        try {
            const response = await fetch(
                `/api/scd-config/${projectId}/config/${configTable}/${dataset}/${table}`
            );
            if (response.ok) {
                const config = await response.json();
                setNewConfigId(config.config_id || '');
                setNewScdType(config.scd_type || 'scd2');
                setNewPrimaryKeys(Array.isArray(config.primary_keys) ? config.primary_keys.join(',') : (config.primary_keys || ''));
                setNewSurrogateKey(config.surrogate_key || '');
                setNewBeginDateColumn(config.begin_date_column || 'DWBeginEffDateTime');
                setNewEndDateColumn(config.end_date_column || 'DWEndEffDateTime');
                setNewActiveFlagColumn(config.active_flag_column || 'DWCurrentRowFlag');
                setNewDescription(config.description || '');
                setNewCustomTests(config.custom_tests || []);
                setIsEditingExisting(true);
            } else {
                setIsEditingExisting(false);
            }
        } catch (error) {
            setIsEditingExisting(false);
        } finally {
            setFetchingConfig(false);
        }
    };

    // Debounced config fetch
    useEffect(() => {
        if (!newTargetDataset || !newTargetTable || !projectId) return;
        const timeoutId = setTimeout(() => fetchExistingConfig(newTargetDataset, newTargetTable), 1000);
        return () => clearTimeout(timeoutId);
    }, [newTargetDataset, newTargetTable, projectId]);


    // Handlers
    const addDataset = () => setDatasets([...datasets, '']);
    const removeDataset = (index: number) => setDatasets(datasets.filter((_, i) => i !== index));
    const handleDatasetChange = (index: number, value: string) => {
        const newDatasets = [...datasets];
        newDatasets[index] = value;
        setDatasets(newDatasets);
    };

    const addCustomTest = (isNewConfig: boolean) => {
        const emptyTest: CustomTest = { name: "", sql: "SELECT * FROM {{target}} WHERE ", description: "", severity: "HIGH" };
        if (isNewConfig) setNewCustomTests([...newCustomTests, emptyTest]);
        else setCustomTests([...customTests, emptyTest]);
    };
    const removeCustomTest = (index: number, isNewConfig: boolean) => {
        if (isNewConfig) setNewCustomTests(newCustomTests.filter((_, i) => i !== index));
        else setCustomTests(customTests.filter((_, i) => i !== index));
    };
    const handleCustomTestChange = (index: number, field: keyof CustomTest, value: string, isNewConfig: boolean) => {
        const target = isNewConfig ? [...newCustomTests] : [...customTests];
        target[index] = { ...target[index], [field]: value };
        if (isNewConfig) setNewCustomTests(target);
        else setCustomTests(target);
    };
    const handleInsertColumn = (index: number, columnName: string, isNewConfig: boolean) => {
        const target = isNewConfig ? [...newCustomTests] : [...customTests];
        target[index] = { ...target[index], sql: (target[index].sql || "") + columnName + " " };
        if (isNewConfig) setNewCustomTests(target);
        else setCustomTests(target);
    };


    const handleSaveSettings = async (e: React.FormEvent) => {
        e.preventDefault();
        if (!projectId) {
            showToast("Project ID is required", "error");
            return;
        }
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

    const handleAddConfig = async () => {
        if (!newTargetDataset || !newTargetTable || !newPrimaryKeys) {
            showToast("Please fill in all required fields (Dataset, Table, Primary Keys)", "error");
            return;
        }

        try {
            const finalConfigId = newConfigId.trim() || `${newTargetTable.toLowerCase()}_${newScdType}`;
            const payload = {
                project_id: projectId,
                config_dataset: configDataset,
                config_table: configTable,
                config_id: finalConfigId,
                target_dataset: newTargetDataset,
                target_table: newTargetTable,
                scd_type: newScdType,
                primary_keys: newPrimaryKeys.split(',').map(k => k.trim()),
                surrogate_key: newSurrogateKey || null,
                begin_date_column: newScdType === 'scd2' ? newBeginDateColumn : null,
                end_date_column: newScdType === 'scd2' ? newEndDateColumn : null,
                active_flag_column: newScdType === 'scd2' ? newActiveFlagColumn : null,
                description: newDescription,
                custom_tests: newCustomTests.length > 0 ? newCustomTests : null
            };

            const response = await fetch(`/api/scd-config`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });

            if (!response.ok) throw new Error('Failed to add configuration');
            showToast(`Configuration "${finalConfigId}" added successfully!`, "success");
            setShowAddConfig(false);
            setNewConfigId(""); setNewTargetDataset(""); setNewTargetTable(""); setNewPrimaryKeys("");
        } catch (error: any) {
            showToast(error.message, "error");
        }
    };

    const handleViewResult = (details: any) => {
        if (!details) {
            showToast("No details available.", "error");
            return;
        }
        // Normalize
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
        if (comparisonMode === 'history') return;

        setLoading(true);
        try {
            let payload: any = { project_id: projectId, comparison_mode: comparisonMode };

            if (comparisonMode === 'schema') {
                const validDatasets = datasets.filter(d => d.trim() !== '');
                if (validDatasets.length === 0) throw new Error("Please provide at least one dataset.");
                payload.datasets = validDatasets;
                payload.erd_description = erdDescription;
            } else if (comparisonMode === 'gcs') {
                if (gcsMode === 'single') {
                    payload = { ...payload, gcs_bucket: gcsBucket, gcs_file_path: gcsFilePath, file_format: fileFormat, target_dataset: targetDataset, target_table: targetTable, erd_description: erdDescription };
                } else {
                    payload = { ...payload, comparison_mode: 'gcs-config', config_dataset: configDataset, config_table: configTable };
                }
            } else if (comparisonMode === 'scd') {
                if (scdMode === 'direct') {
                    if (!targetDataset || !targetTable || !primaryKeys) throw new Error("Target dataset, table, and primary keys required.");
                    payload = {
                        ...payload,
                        target_dataset: targetDataset,
                        target_table: targetTable,
                        scd_type: scdType,
                        primary_keys: primaryKeys.split(',').map(k => k.trim()),
                        surrogate_key: surrogateKey || undefined,
                        begin_date_column: scdType === 'scd2' ? beginDateColumn : undefined,
                        end_date_column: scdType === 'scd2' ? endDateColumn : undefined,
                        active_flag_column: scdType === 'scd2' ? activeFlagColumn : undefined,
                        custom_tests: customTests.length > 0 ? customTests : undefined
                    };
                } else {
                    payload = { ...payload, comparison_mode: 'scd-config', config_dataset: configDataset, config_table: configTable };
                }
            }

            const response = await fetch('/api/generate-tests', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });

            if (!response.ok) {
                const txt = await response.text();
                throw new Error(txt.substring(0, 200));
            }

            const data = await response.json();
            localStorage.setItem("projectId", projectId);
            handleViewResult(data);

            // Trigger Notification (Test1 Feature)
            if (data.execution_id) {
                const summary = data.summary;
                if (summary) {
                    fetch('/api/notify', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ project_id: projectId, execution_id: data.execution_id, summary: summary })
                    }).catch(err => console.error(err));
                }
            }

        } catch (error: any) {
            showToast(error.message, "error");
        } finally {
            setLoading(false);
        }
    };

    return (
        <>
            <form onSubmit={comparisonMode === 'settings' ? handleSaveSettings : handleSubmit} className="card fade-in" style={{ width: '100%', maxWidth: '800px', margin: '0 auto' }}>
                <h2 style={{ fontSize: '1.5rem', fontWeight: '700', marginBottom: '1.5rem', borderBottom: '1px solid var(--border)', paddingBottom: '0.75rem' }}>
                    {comparisonMode === 'settings' ? 'Alert Settings' :
                        comparisonMode === 'schema' ? 'Schema Validation Setup' :
                            comparisonMode === 'gcs' ? 'GCS Comparison Setup' :
                                comparisonMode === 'scd' ? 'SCD Validation Setup' :
                                    'Execution History'}
                </h2>

                {/* Common Project ID */}
                <div style={{ marginBottom: '1.75rem', width: '100%' }}>
                    <label className="label" htmlFor="projectId">
                        <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                            🔑 Google Cloud Project ID<span className="required">*</span>
                        </span>
                    </label>
                    <input
                        id="projectId"
                        type="text"
                        className="input"
                        style={{ width: '100%' }}
                        value={projectId}
                        onChange={(e) => setProjectId(e.target.value)}
                        required
                        placeholder="Project with BigQuery data"
                        disabled={comparisonMode === 'settings'}
                    />
                </div>

                {comparisonMode === 'settings' ? (
                    <div className="fade-in">
                        <div style={{ marginBottom: '1.75rem' }}>
                            <label className="label" htmlFor="alertEmails">📧 Alert Emails (Comma separated)</label>
                            <input id="alertEmails" type="text" className="input" value={alertEmails} onChange={(e) => setAlertEmails(e.target.value)} placeholder="user@example.com" />
                        </div>
                        <div style={{ marginBottom: '1.75rem' }}>
                            <label className="label" htmlFor="teamsWebhook">💬 Teams Webhook URL</label>
                            <input id="teamsWebhook" type="text" className="input" value={teamsWebhook} onChange={(e) => setTeamsWebhook(e.target.value)} />
                        </div>
                        <div style={{ marginBottom: '2rem', display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                            <input id="alertOnFailure" type="checkbox" checked={alertOnFailure} onChange={(e) => setAlertOnFailure(e.target.checked)} style={{ width: '1.25rem', height: '1.25rem' }} />
                            <label htmlFor="alertOnFailure" style={{ fontSize: '1rem', cursor: 'pointer' }}>Enable Alerts on Test Failure</label>
                        </div>
                        <button type="submit" className="btn btn-primary" style={{ width: '100%', fontSize: '1rem', padding: '1rem' }} disabled={loading}>
                            {loading ? 'Saving...' : '💾 Save Settings'}
                        </button>
                    </div>
                ) : comparisonMode === 'history' ? (
                    <HistoryList projectId={projectId} onViewResult={handleViewResult} /> // showToast support? HistoryList might need update if I chnaged it
                ) : (
                    <>
                        {/* SCHEMA MODE */}
                        {comparisonMode === 'schema' && (
                            <>
                                <div style={{ marginBottom: '1.75rem' }}>
                                    <label className="label">📊 BigQuery Datasets</label>
                                    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                                        {datasets.map((dataset, index) => (
                                            <div key={index} style={{ display: 'flex', gap: '0.75rem' }}>
                                                <input type="text" className="input" value={dataset} onChange={(e) => handleDatasetChange(index, e.target.value)} placeholder="Dataset Name" style={{ flex: 1 }} />
                                                {datasets.length > 1 && <button type="button" onClick={() => removeDataset(index)} style={{ padding: '0 1rem', background: 'var(--error)', color: 'white', borderRadius: '4px', border: 'none' }}>Remove</button>}
                                            </div>
                                        ))}
                                    </div>
                                    <button type="button" onClick={addDataset} style={{ marginTop: '1rem', padding: '0.5rem', width: '100%', background: 'var(--secondary)', border: '1px dashed var(--primary)' }}>+ Add Dataset</button>
                                </div>
                                <div style={{ marginBottom: '2rem' }}>
                                    <label className="label">📝 ER Diagram Description</label>
                                    <textarea className="input" value={erdDescription} onChange={(e) => setErdDescription(e.target.value)} rows={8} placeholder="Describe table relationships..." required />
                                </div>
                            </>
                        )}

                        {/* GCS MODE */}
                        {comparisonMode === 'gcs' && (
                            <>
                                <div style={{ marginBottom: '2rem', display: 'flex', gap: '1rem' }}>
                                    <button type="button" onClick={() => setGcsMode('single')} style={{ flex: 1, padding: '0.75rem', background: gcsMode === 'single' ? 'var(--primary)' : 'var(--secondary)', color: gcsMode === 'single' ? 'white' : 'var(--foreground)', border: 'none', borderRadius: '4px' }}>📄 Single File</button>
                                    <button type="button" onClick={() => setGcsMode('config')} style={{ flex: 1, padding: '0.75rem', background: gcsMode === 'config' ? 'var(--primary)' : 'var(--secondary)', color: gcsMode === 'config' ? 'white' : 'var(--foreground)', border: 'none', borderRadius: '4px' }}>📋 Config Table</button>
                                </div>
                                {gcsMode === 'single' ? (
                                    <>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">GCS Bucket</label><input className="input" value={gcsBucket} onChange={e => setGcsBucket(e.target.value)} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">File Path</label><input className="input" value={gcsFilePath} onChange={e => setGcsFilePath(e.target.value)} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Format</label><select className="input" value={fileFormat} onChange={e => setFileFormat(e.target.value as FileFormat)}><option value="csv">CSV</option><option value="json">JSON</option><option value="parquet">Parquet</option></select></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Target Dataset</label><input className="input" value={targetDataset} onChange={e => setTargetDataset(e.target.value)} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Target Table</label><input className="input" value={targetTable} onChange={e => setTargetTable(e.target.value)} required /></div>
                                    </>
                                ) : (
                                    <>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Config Dataset</label><input className="input" value={configDataset} onChange={e => setConfigDataset(e.target.value)} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Config Table</label><input className="input" value={configTable} onChange={e => setConfigTable(e.target.value)} required /></div>
                                    </>
                                )}
                            </>
                        )}

                        {/* SCD MODE */}
                        {comparisonMode === 'scd' && (
                            <>
                                <div style={{ marginBottom: '2rem', display: 'flex', gap: '1rem' }}>
                                    <button type="button" onClick={() => setScdMode('direct')} style={{ flex: 1, padding: '0.75rem', background: scdMode === 'direct' ? 'var(--primary)' : 'var(--secondary)', color: scdMode === 'direct' ? 'white' : 'var(--foreground)', border: 'none', borderRadius: '4px' }}>✏️ Direct Input</button>
                                    <button type="button" onClick={() => setScdMode('config')} style={{ flex: 1, padding: '0.75rem', background: scdMode === 'config' ? 'var(--primary)' : 'var(--secondary)', color: scdMode === 'config' ? 'white' : 'var(--foreground)', border: 'none', borderRadius: '4px' }}>📋 Config Table</button>
                                </div>
                                {scdMode === 'config' && (
                                    <>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Config Dataset</label><input className="input" value={configDataset} onChange={e => setConfigDataset(e.target.value)} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Config Table</label><input className="input" value={configTable} onChange={e => setConfigTable(e.target.value)} required /></div>
                                        <button type="button" onClick={() => setShowAddConfig(!showAddConfig)} style={{ width: '100%', padding: '0.75rem', background: 'var(--secondary)', border: '1px solid var(--primary)', color: 'var(--primary)', marginBottom: '1rem' }}>{showAddConfig ? 'Cancel' : '+ Add New Configuration'}</button>

                                        {showAddConfig && (
                                            <div style={{ padding: '1rem', border: '1px solid var(--border)', borderRadius: '4px', marginBottom: '1rem', background: 'var(--secondary)' }}>
                                                <div style={{ marginBottom: '1rem' }}><label className="label">Target Dataset</label><input className="input" value={newTargetDataset} onChange={e => setNewTargetDataset(e.target.value)} required /></div>
                                                <div style={{ marginBottom: '1rem' }}><label className="label">Target Table</label><input className="input" value={newTargetTable} onChange={e => { setNewTargetTable(e.target.value); setScdTargetTable(e.target.value); }} required /></div>
                                                <div style={{ marginBottom: '1rem' }}><label className="label">Primary Keys</label><input className="input" value={newPrimaryKeys} onChange={e => setNewPrimaryKeys(e.target.value)} required /></div>
                                                <div style={{ marginBottom: '1rem' }}><label className="label">SCD Type</label><select className="input" value={newScdType} onChange={e => setNewScdType(e.target.value as any)}><option value="scd1">Type 1</option><option value="scd2">Type 2</option></select></div>
                                                {newScdType === 'scd2' && (
                                                    <>
                                                        <input className="input" value={newBeginDateColumn} onChange={e => setNewBeginDateColumn(e.target.value)} placeholder="Begin Date" />
                                                        <input className="input" value={newEndDateColumn} onChange={e => setNewEndDateColumn(e.target.value)} placeholder="End Date" />
                                                        <input className="input" value={newActiveFlagColumn} onChange={e => setNewActiveFlagColumn(e.target.value)} placeholder="Active Flag" />
                                                    </>
                                                )}
                                                <button type="button" onClick={handleAddConfig} style={{ width: '100%', padding: '0.75rem', background: 'var(--primary)', color: 'white', border: 'none' }}>Save Config</button>
                                            </div>
                                        )}
                                    </>
                                )}
                                {scdMode === 'direct' && (
                                    <>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Target Dataset</label><input className="input" value={targetDataset} onChange={e => setTargetDataset(e.target.value)} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Target Table</label><input className="input" value={targetTable} onChange={e => { setTargetTable(e.target.value); setScdTargetTable(e.target.value); setScdTargetDataset(targetDataset); }} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Primary Keys</label><input className="input" value={primaryKeys} onChange={e => setPrimaryKeys(e.target.value)} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">SCD Type</label><select className="input" value={scdType} onChange={e => setScdType(e.target.value as any)}><option value="scd1">Type 1</option><option value="scd2">Type 2</option></select></div>
                                        {scdType === 'scd2' && (<> <input className="input" value={beginDateColumn} onChange={e => setBeginDateColumn(e.target.value)} placeholder="Begin Date" /> <input className="input" value={endDateColumn} onChange={e => setEndDateColumn(e.target.value)} placeholder="End Date" /> <input className="input" value={activeFlagColumn} onChange={e => setActiveFlagColumn(e.target.value)} placeholder="Active Flag" /> </>)}
                                    </>
                                )}
                            </>
                        )}

                        <button type="submit" className="btn btn-primary" style={{ width: '100%', fontSize: '1rem', padding: '1rem', marginTop: '1.5rem' }} disabled={loading}>
                            {loading ? 'Running Tests...' : '🚀 Run Tests'}
                        </button>
                    </>
                )}
            </form>

            {toast && (
                <div style={{
                    position: 'fixed', bottom: '2rem', right: '2rem', padding: '1rem 2rem',
                    background: toast.type === 'error' ? 'var(--error)' : toast.type === 'success' ? '#2ecc71' : 'var(--primary)',
                    color: 'white', borderRadius: '4px', zIndex: 1000
                }}>
                    {toast.message}
                </div>
            )}
        </>
    );
}
