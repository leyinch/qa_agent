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

interface TestResult {
    test_id: string;
    test_name: string;
    category: string;
    description: string;
    status: string;
    severity: string;
    sql_query: string;
    rows_affected: number;
    sample_data?: any[];
    error_message?: string;
}

interface MappingResult {
    mapping_id: string;
    mapping_info: {
        source: string;
        target: string;
        file_row_count: number;
        table_row_count: number;
    };
    predefined_results: TestResult[];
    error?: string;
}

interface SCDResult {
    execution_id: string;
    summary: {
        total_mappings: number;
        total_tests: number;
        passed: number;
        failed: number;
        errors: number;
        total_suggestions: number;
    };
    results_by_mapping: MappingResult[];
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
        } else {
            setProjectId("leyin-sandpit");
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

    // Results state for inline viewing
    const [showResults, setShowResults] = useState(false);
    const [resultsData, setResultsData] = useState<any>(null);
    const [activeMappingIdx, setActiveMappingIdx] = useState(0);
    const [expandedTests, setExpandedTests] = useState<Set<string>>(new Set());

    const toggleTestExpansion = (mappingIdx: number, testIdx: number) => {
        const key = `${mappingIdx}-${testIdx}`;
        setExpandedTests((prev: Set<string>) => {
            const next = new Set(prev);
            if (next.has(key)) next.delete(key);
            else next.add(key);
            return next;
        });
    };

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
                // showToast("Found existing configuration.", "info");
            } else {
                // Not found - Reset to defaults for new config
                setIsEditingExisting(false);
                setNewConfigId("");
                setNewScdType('scd2');
                setNewPrimaryKeys("");
                setNewSurrogateKey("");
                setNewBeginDateColumn("DWBeginEffDateTime");
                setNewEndDateColumn("DWEndEffDateTime");
                setNewActiveFlagColumn("DWCurrentRowFlag");
                setNewDescription("");
                setNewCustomTests([]);
            }
        } catch (error) {
            console.error("Error fetching config", error);
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
    const removeDataset = (index: number) => setDatasets(datasets.filter((_: string, i: number) => i !== index));
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
        if (isNewConfig) setNewCustomTests(newCustomTests.filter((_: CustomTest, i: number) => i !== index));
        else setCustomTests(customTests.filter((_: CustomTest, i: number) => i !== index));
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
                alert_emails: alertEmails.split(',').map((s: string) => s.trim()).filter(Boolean),
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
                primary_keys: newPrimaryKeys.split(',').map((k: string) => k.trim()),
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

        // If it's SCD, show inline instead of redirecting
        if (comparisonMode === 'scd' || details.comparison_mode === 'scd' || details.comparison_mode === 'scd-config') {
            setResultsData(details);
            setShowResults(true);
            setActiveMappingIdx(0);
            setExpandedTests(new Set());
            window.scrollTo({ top: 0, behavior: 'smooth' });
        } else {
            router.push("/results");
        }
    };


    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        if (comparisonMode === 'history') return;

        setLoading(true);
        try {
            let payload: any = { project_id: projectId, comparison_mode: comparisonMode };

            if (comparisonMode === 'schema') {
                const validDatasets = datasets.filter((d: string) => d.trim() !== '');
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
                        primary_keys: primaryKeys.split(',').map((k: string) => k.trim()),
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
            <form
                onSubmit={comparisonMode === 'settings' ? handleSaveSettings : handleSubmit}
                className="card fade-in"
                style={{
                    width: '100%',
                    maxWidth: comparisonMode === 'history' ? '1200px' : '800px',
                    margin: '0 auto'
                }}
            >
                <div style={{ marginBottom: '1.5rem', borderBottom: '1px solid var(--border)', paddingBottom: '0.75rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div style={{ display: 'flex', flexDirection: 'column' }}>
                        <h2 style={{ fontSize: '1.5rem', fontWeight: '700', margin: 0, padding: 0, border: 'none' }}>
                            {comparisonMode === 'settings' ? 'Alert Settings' :
                                comparisonMode === 'schema' ? 'Schema Validation Setup' :
                                    comparisonMode === 'gcs' ? 'GCS Comparison Setup' :
                                        comparisonMode === 'scd' ? (showResults ? '🚀 SCD Validation Results' : 'SCD Validation Setup') :
                                            'Execution History'}
                        </h2>
                        <div style={{ fontSize: '0.85rem', fontWeight: '500', color: '#64748b', marginTop: '0.25rem' }}>
                            Current Project: <span style={{ fontFamily: 'monospace', fontWeight: '700', color: 'var(--primary)' }}>{projectId}</span>
                        </div>
                    </div>
                    {showResults && (
                        <button
                            type="button"
                            onClick={() => setShowResults(false)}
                            style={{ fontSize: '0.875rem', padding: '0.4rem 0.8rem', background: 'var(--secondary)', border: '1px solid var(--border)', borderRadius: 'var(--radius)', cursor: 'pointer' }}
                        >
                            ⬅ Back to Setup
                        </button>
                    )}
                </div>

                {/* Project ID input removed here as it is now displayed in the common header */}

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
                                    <textarea className="input" value={erdDescription} onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => setErdDescription(e.target.value)} rows={8} placeholder="Describe table relationships..." required />
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
                                        <div style={{ marginBottom: '1rem' }}><label className="label">GCS Bucket</label><input className="input" value={gcsBucket} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setGcsBucket(e.target.value)} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">File Path</label><input className="input" value={gcsFilePath} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setGcsFilePath(e.target.value)} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Format</label><select className="input" value={fileFormat} onChange={(e: React.ChangeEvent<HTMLSelectElement>) => setFileFormat(e.target.value as FileFormat)}><option value="csv">CSV</option><option value="json">JSON</option><option value="parquet">Parquet</option></select></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Target Dataset</label><input className="input" value={targetDataset} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setTargetDataset(e.target.value)} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Target Table</label><input className="input" value={targetTable} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setTargetTable(e.target.value)} required /></div>
                                    </>
                                ) : (
                                    <>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Config Dataset</label><input className="input" value={configDataset} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setConfigDataset(e.target.value)} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Config Table</label><input className="input" value={configTable} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setConfigTable(e.target.value)} required /></div>
                                    </>
                                )}
                            </>
                        )}

                        {/* SCD MODE */}
                        {comparisonMode === 'scd' && !showResults && (
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
                                                {fetchingConfig && <div style={{ marginBottom: '1rem', color: 'var(--secondary-foreground)', fontStyle: 'italic' }}>⏳ Checking for existing configuration...</div>}
                                                {!fetchingConfig && isEditingExisting && <div style={{ marginBottom: '1rem', color: '#10b981', fontWeight: '600' }}>✓ Loaded existing configuration. You can edit and update it below.</div>}
                                                {!fetchingConfig && !isEditingExisting && newTargetTable && newTargetDataset && <div style={{ marginBottom: '1rem', color: 'var(--primary)', fontWeight: '600' }}>🆕 New configuration will be created.</div>}

                                                <div style={{ marginBottom: '1rem' }}><label className="label">Target Dataset</label><input className="input" value={newTargetDataset} onChange={e => setNewTargetDataset(e.target.value)} required /></div>
                                                <div style={{ marginBottom: '1rem' }}><label className="label">Target Table</label><input className="input" value={newTargetTable} onChange={e => { setNewTargetTable(e.target.value); setScdTargetTable(e.target.value); }} required /></div>
                                                <div style={{ marginBottom: '1rem' }}><label className="label">Primary Keys</label><input className="input" value={newPrimaryKeys} onChange={e => setNewPrimaryKeys(e.target.value)} required /></div>
                                                <div style={{ marginBottom: '1rem' }}><label className="label">Surrogate Key</label><input className="input" value={newSurrogateKey} onChange={e => setNewSurrogateKey(e.target.value)} placeholder="Optional (if applicable)" /></div>
                                                <div style={{ marginBottom: '1rem' }}><label className="label">SCD Type</label><select className="input" value={newScdType} onChange={e => setNewScdType(e.target.value as any)}><option value="scd1">Type 1</option><option value="scd2">Type 2</option></select></div>
                                                {newScdType === 'scd2' && (
                                                    <>
                                                        <div style={{ marginBottom: '1rem' }}>
                                                            <label className="label">Begin Date Column</label>
                                                            <input className="input" value={newBeginDateColumn} onChange={e => setNewBeginDateColumn(e.target.value)} placeholder="e.g. DWBeginEffDateTime" />
                                                        </div>
                                                        <div style={{ marginBottom: '1rem' }}>
                                                            <label className="label">End Date Column</label>
                                                            <input className="input" value={newEndDateColumn} onChange={e => setNewEndDateColumn(e.target.value)} placeholder="e.g. DWEndEffDateTime" />
                                                        </div>
                                                        <div style={{ marginBottom: '1rem' }}>
                                                            <label className="label">Active Flag Column</label>
                                                            <input className="input" value={newActiveFlagColumn} onChange={e => setNewActiveFlagColumn(e.target.value)} placeholder="e.g. DWCurrentRowFlag" />
                                                        </div>
                                                    </>
                                                )}
                                                {/* Custom Business Rules - Test5 Requirement */}
                                                <div style={{ marginBottom: '1rem', borderTop: '1px solid var(--border)', paddingTop: '1rem' }}>
                                                    <label className="label" style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                                        🛠️ Custom Business Rules
                                                        <span style={{ fontSize: '0.7rem', color: 'var(--secondary-foreground)', fontWeight: 'normal' }}>(Optional)</span>
                                                    </label>
                                                    <div style={{ background: '#fff', borderRadius: '4px', border: '1px dashed var(--border)', padding: '1rem' }}>
                                                        {newCustomTests.map((test, idx) => (
                                                            <div key={idx} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.5rem', borderBottom: '1px solid #eee', fontSize: '0.85rem' }}>
                                                                <div>
                                                                    <strong>{test.name}</strong> <span style={{ color: '#666' }}>({test.severity})</span>
                                                                    <div style={{ fontSize: '0.75rem', fontFamily: 'monospace', color: '#888', maxWidth: '300px', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{test.sql}</div>
                                                                </div>
                                                                <button type="button" onClick={() => {
                                                                    const updated = [...newCustomTests];
                                                                    updated.splice(idx, 1);
                                                                    setNewCustomTests(updated);
                                                                }} style={{ color: '#ef4444', background: 'none', border: 'none', cursor: 'pointer' }}>✖</button>
                                                            </div>
                                                        ))}

                                                        <div style={{ marginTop: '1rem', display: 'grid', gap: '0.5rem' }}>
                                                            <input id="new-rule-name" className="input" placeholder="Rule Name (e.g. Sales > 0)" style={{ fontSize: '0.85rem' }} />
                                                            <textarea id="new-rule-sql" className="input" placeholder="SQL Condition (e.g. sales_amount > 0). Use {{target}} as placeholder for table name." rows={2} style={{ fontSize: '0.85rem', fontFamily: 'monospace' }} />
                                                            <div style={{ display: 'flex', gap: '0.5rem' }}>
                                                                <select id="new-rule-severity" className="input" style={{ flex: 1 }}>
                                                                    <option value="high">High</option>
                                                                    <option value="medium">Medium</option>
                                                                    <option value="low">Low</option>
                                                                </select>
                                                                <button type="button" onClick={() => {
                                                                    const nameInput = document.getElementById('new-rule-name') as HTMLInputElement;
                                                                    const sqlInput = document.getElementById('new-rule-sql') as HTMLTextAreaElement;
                                                                    const sevInput = document.getElementById('new-rule-severity') as HTMLSelectElement;

                                                                    if (nameInput.value && sqlInput.value) {
                                                                        setNewCustomTests([...newCustomTests, {
                                                                            name: nameInput.value,
                                                                            sql: sqlInput.value,
                                                                            description: nameInput.value,
                                                                            severity: sevInput.value
                                                                        }]);
                                                                        nameInput.value = '';
                                                                        sqlInput.value = '';
                                                                    }
                                                                }} style={{ padding: '0.5rem 1rem', background: 'var(--secondary)', border: '1px solid var(--primary)', color: 'var(--primary)', borderRadius: '4px', fontSize: '0.85rem' }}>+ Add Rule</button>
                                                            </div>
                                                        </div>
                                                    </div>
                                                </div>

                                                {/* Standard Validation Suite Info - Test5 Requirement */}
                                                <div style={{ marginBottom: '1.5rem', background: '#f8fafc', padding: '1rem', borderRadius: '4px', fontSize: '0.85rem', border: '1px solid #e2e8f0' }}>
                                                    <div style={{ fontWeight: '600', marginBottom: '0.5rem', color: '#475569' }}>
                                                        ℹ️ Standard Validation Suite (Included)
                                                    </div>
                                                    {newScdType === 'scd1' ? (
                                                        <ul style={{ margin: 0, paddingLeft: '1.2rem', color: '#64748b' }}>
                                                            <li>Table exists (Smoke)</li>
                                                            <li>Primary Key NOT NULL & Uniqueness</li>
                                                            <li>Surrogate Key NOT NULL & Uniqueness</li>
                                                        </ul>
                                                    ) : (
                                                        <ul style={{ margin: 0, paddingLeft: '1.2rem', color: '#64748b', display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.25rem' }}>
                                                            <li>Table exists (Smoke)</li>
                                                            <li>PK NOT NULL & Uniqueness</li>
                                                            <li>Surrogate Key Valildation</li>
                                                            <li>Begin/End Date NOT NULL</li>
                                                            <li>Current Row Flag Integrity</li>
                                                            <li>One Active Row per PK</li>
                                                            <li>Current Rows End 2099-12-31</li>
                                                            <li>No Overlaps/Gaps (Continuous)</li>
                                                            <li>Unique Begin/End Dates</li>
                                                            <li>Begin &lt; End Date Check</li>
                                                        </ul>
                                                    )}
                                                </div>

                                                <button type="button" onClick={handleAddConfig} style={{ width: '100%', padding: '0.75rem', background: isEditingExisting ? '#f59e0b' : 'var(--primary)', color: 'white', border: 'none', fontWeight: 'bold' }}>
                                                    {isEditingExisting ? 'Update Configuration' : 'Save New Configuration'}
                                                </button>
                                            </div>
                                        )}
                                    </>
                                )}
                                {scdMode === 'direct' && (
                                    <>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Target Dataset</label><input className="input" value={targetDataset} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setTargetDataset(e.target.value)} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Target Table</label><input className="input" value={targetTable} onChange={(e: React.ChangeEvent<HTMLInputElement>) => { setTargetTable(e.target.value); setScdTargetTable(e.target.value); setScdTargetDataset(targetDataset); }} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Primary Keys</label><input className="input" value={primaryKeys} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setPrimaryKeys(e.target.value)} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">SCD Type</label><select className="input" value={scdType} onChange={(e: React.ChangeEvent<HTMLSelectElement>) => setScdType(e.target.value as any)}><option value="scd1">Type 1</option><option value="scd2">Type 2</option></select></div>
                                        {scdType === 'scd2' && (<> <input className="input" value={beginDateColumn} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setBeginDateColumn(e.target.value)} placeholder="Begin Date" /> <input className="input" value={endDateColumn} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setEndDateColumn(e.target.value)} placeholder="End Date" /> <input className="input" value={activeFlagColumn} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setActiveFlagColumn(e.target.value)} placeholder="Active Flag" /> </>)}
                                    </>
                                )}
                            </>
                        )}

                        {!showResults && (
                            <button type="submit" className="btn btn-primary" style={{ width: '100%', fontSize: '1rem', padding: '1rem', marginTop: '1.5rem' }} disabled={loading}>
                                {loading ? 'Running Tests...' : '🚀 Run Tests'}
                            </button>
                        )}
                    </>
                )}

                {/* INLINE SCD RESULTS - PREMIUM UI */}
                {comparisonMode === 'scd' && showResults && resultsData && (
                    <div className="fade-in" style={{ marginTop: '1rem', borderTop: '1px solid var(--border)', paddingTop: '1.5rem' }}>

                        {/* Summary Cards */}
                        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '1rem', marginBottom: '2rem' }}>
                            <div className="card" style={{ textAlign: 'center', padding: '1rem', background: 'var(--secondary)', borderBottom: `4px solid var(--primary)` }}>
                                <div style={{ fontSize: '1.5rem', fontWeight: '700', color: 'var(--primary)' }}>{resultsData.summary.total_mappings}</div>
                                <div style={{ fontSize: '0.75rem', color: 'var(--secondary-foreground)', fontWeight: '600' }}>Mappings</div>
                            </div>
                            <div className="card" style={{ textAlign: 'center', padding: '1rem', background: '#d1fae5', borderBottom: '4px solid #10b981' }}>
                                <div style={{ fontSize: '1.5rem', fontWeight: '700', color: '#065f46' }}>{resultsData.summary.passed}</div>
                                <div style={{ fontSize: '0.75rem', color: '#065f46', fontWeight: '600' }}>Passed</div>
                            </div>
                            <div className="card" style={{ textAlign: 'center', padding: '1rem', background: '#fee2e2', borderBottom: '4px solid #ef4444' }}>
                                <div style={{ fontSize: '1.5rem', fontWeight: '700', color: '#991b1b' }}>{resultsData.summary.failed}</div>
                                <div style={{ fontSize: '0.75rem', color: '#991b1b', fontWeight: '600' }}>Failed</div>
                            </div>
                            <div className="card" style={{ textAlign: 'center', padding: '1rem', background: '#fef3c7', borderBottom: '4px solid #f59e0b' }}>
                                <div style={{ fontSize: '1.5rem', fontWeight: '700', color: '#92400e' }}>{resultsData.summary.errors}</div>
                                <div style={{ fontSize: '0.75rem', color: '#92400e', fontWeight: '600' }}>Errors</div>
                            </div>
                        </div>

                        {/* Status Legend */}
                        <div style={{ display: 'flex', gap: '1.5rem', marginBottom: '1.5rem', padding: '0.75rem 1rem', background: '#f8fafc', borderRadius: 'var(--radius)', border: '1px solid var(--border)', fontSize: '0.75rem' }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                <div style={{ width: '10px', height: '10px', borderRadius: '50%', background: '#10b981' }}></div>
                                <span style={{ fontWeight: '600' }}>Passed:</span> <span style={{ color: '#64748b' }}>Validation rules satisfied</span>
                            </div>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                <div style={{ width: '10px', height: '10px', borderRadius: '50%', background: '#ef4444' }}></div>
                                <span style={{ fontWeight: '600' }}>Failed:</span> <span style={{ color: '#64748b' }}>Data issues detected</span>
                            </div>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                <div style={{ width: '10px', height: '10px', borderRadius: '50%', background: '#f59e0b' }}></div>
                                <span style={{ fontWeight: '600' }}>Technical Error (Yellow):</span> <span style={{ color: '#64748b' }}>Execution failed (e.g. timeout, access denied)</span>
                            </div>
                        </div>

                        {/* Mappings Tabs */}
                        <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '1.5rem', overflowX: 'auto', paddingBottom: '0.5rem' }}>
                            {resultsData.results_by_mapping.map((mapping: any, idx: number) => {
                                const failed = mapping.predefined_results.filter((r: any) => r.status !== 'PASS').length;
                                const isActive = activeMappingIdx === idx;
                                return (
                                    <button
                                        key={idx}
                                        type="button"
                                        onClick={() => setActiveMappingIdx(idx)}
                                        style={{
                                            padding: '0.6rem 1.2rem',
                                            borderRadius: 'var(--radius)',
                                            border: isActive ? '1px solid var(--primary)' : '1px solid var(--border)',
                                            background: isActive ? 'var(--primary)' : 'var(--card)',
                                            color: isActive ? 'white' : 'var(--foreground)',
                                            fontWeight: '600',
                                            fontSize: '0.875rem',
                                            whiteSpace: 'nowrap',
                                            display: 'flex',
                                            alignItems: 'center',
                                            gap: '0.5rem',
                                            cursor: 'pointer',
                                            boxShadow: isActive ? '0 4px 6px -1px rgba(0, 0, 0, 0.1)' : 'none'
                                        }}
                                    >
                                        {mapping.mapping_id}
                                        {failed > 0 && (
                                            <span style={{
                                                background: isActive ? 'white' : '#ef4444',
                                                color: isActive ? 'var(--primary)' : 'white',
                                                padding: '2px 6px',
                                                borderRadius: '10px',
                                                fontSize: '0.7rem'
                                            }}>
                                                {failed}
                                            </span>
                                        )}
                                    </button>
                                );
                            })}
                        </div>

                        {/* Active Mapping Content */}
                        {resultsData.results_by_mapping[activeMappingIdx] && (
                            <div className="fade-in">
                                <div style={{
                                    padding: '1.25rem',
                                    background: '#f8fafc',
                                    borderRadius: 'var(--radius)',
                                    border: '1px solid #e2e8f0',
                                    marginBottom: '1.5rem',
                                    display: 'flex',
                                    justifyContent: 'space-between',
                                    alignItems: 'center'
                                }}>
                                    <div>
                                        <div style={{ fontSize: '0.75rem', fontWeight: '700', color: 'var(--primary)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '0.25rem' }}>Current Mapping</div>
                                        <div style={{ fontSize: '1.25rem', fontWeight: '700' }}>{resultsData.results_by_mapping[activeMappingIdx].mapping_id}</div>
                                    </div>
                                    <div style={{ textAlign: 'right' }}>
                                        <div style={{ fontSize: '0.75rem', fontWeight: '600', color: '#64748b', marginBottom: '0.25rem' }}>Target Dataset/Table</div>
                                        <div style={{ fontWeight: '700', fontFamily: 'monospace', color: '#1e293b' }}>
                                            {resultsData.results_by_mapping[activeMappingIdx].mapping_info?.target || 'Unknown'}
                                        </div>
                                    </div>
                                </div>

                                {/* Test Cards */}
                                <div style={{ display: 'grid', gap: '1rem' }}>
                                    {resultsData.results_by_mapping[activeMappingIdx].predefined_results.map((test: any, tIdx: number) => {
                                        const isExpanded = expandedTests.has(`${activeMappingIdx}-${tIdx}`);
                                        const isPass = test.status === 'PASS';
                                        const isError = test.status === 'ERROR';

                                        return (
                                            <div key={tIdx} className="card" style={{
                                                padding: 0,
                                                overflow: 'hidden',
                                                border: isExpanded ? `1px solid ${isPass ? '#10b981' : isError ? '#f59e0b' : '#ef4444'}` : '1px solid var(--border)'
                                            }}>
                                                <div style={{ padding: '1rem 1.25rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', background: isExpanded ? '#f8fafc' : 'transparent' }}>
                                                    <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', flex: 1 }}>
                                                        <div style={{
                                                            width: '32px', height: '32px', borderRadius: '8px',
                                                            display: 'flex', alignItems: 'center', justifyContent: 'center',
                                                            background: isPass ? '#d1fae5' : isError ? '#fef3c7' : '#fee2e2',
                                                            color: isPass ? '#065f46' : isError ? '#92400e' : '#991b1b',
                                                            fontSize: '1.2rem'
                                                        }}>
                                                            {isPass ? '✓' : isError ? '⚠' : '✗'}
                                                        </div>
                                                        <div>
                                                            <div style={{ fontWeight: '700', color: '#1e293b' }}>{test.test_name}</div>
                                                            <div style={{ fontSize: '0.75rem', color: '#64748b' }}>{test.category || 'validation'}</div>
                                                        </div>
                                                    </div>

                                                    <div style={{ display: 'flex', gap: '2rem', alignItems: 'center', padding: '0 1rem' }}>
                                                        <div style={{ textAlign: 'center' }}>
                                                            <div style={{ fontSize: '0.65rem', fontWeight: '700', color: '#94a3b8', textTransform: 'uppercase', marginBottom: '0.25rem' }}>Status</div>
                                                            <div style={{
                                                                fontSize: '0.7rem', fontWeight: '800',
                                                                padding: '0.2rem 0.6rem',
                                                                borderRadius: '20px',
                                                                background: isPass ? '#d1fae5' : isError ? '#fef3c7' : '#fee2e2',
                                                                color: isPass ? '#065f46' : isError ? '#92400e' : '#991b1b'
                                                            }}>{test.status}</div>
                                                        </div>
                                                        <div style={{ textAlign: 'center' }}>
                                                            <div style={{ fontSize: '0.65rem', fontWeight: '700', color: '#94a3b8', textTransform: 'uppercase', marginBottom: '0.25rem' }}>Severity</div>
                                                            <div style={{ fontSize: '0.75rem', fontWeight: '700', color: '#475569' }}>{test.severity}</div>
                                                        </div>
                                                        <div style={{ textAlign: 'center' }}>
                                                            <div style={{ fontSize: '0.65rem', fontWeight: '700', color: '#94a3b8', textTransform: 'uppercase', marginBottom: '0.25rem' }}>Affected</div>
                                                            <div style={{ fontSize: '0.875rem', fontWeight: '800', color: test.rows_affected > 0 ? '#ef4444' : '#10b981' }}>{test.rows_affected}</div>
                                                        </div>
                                                    </div>

                                                    <button
                                                        type="button"
                                                        onClick={() => toggleTestExpansion(activeMappingIdx, tIdx)}
                                                        style={{
                                                            padding: '0.5rem 1rem', background: isExpanded ? 'var(--primary)' : 'var(--secondary)',
                                                            color: isExpanded ? 'white' : 'var(--primary)', border: 'none',
                                                            borderRadius: 'var(--radius)', fontSize: '0.8rem', fontWeight: '700', cursor: 'pointer'
                                                        }}
                                                    >
                                                        {isExpanded ? 'Hide Details' : 'View Details'}
                                                    </button>
                                                </div>

                                                {isExpanded && (
                                                    <div style={{ padding: '1.25rem', borderTop: '1px solid var(--border)', background: 'white' }}>
                                                        <div style={{ marginBottom: '1rem' }}>
                                                            <div style={{ fontSize: '0.7rem', fontWeight: '700', color: '#94a3b8', textTransform: 'uppercase', marginBottom: '0.5rem' }}>Description</div>
                                                            <div style={{ fontSize: '0.9rem', color: '#475569', lineHeight: '1.5' }}>{test.description}</div>
                                                        </div>

                                                        {test.sql_query && (
                                                            <div style={{ marginBottom: '1rem' }}>
                                                                <div style={{ fontSize: '0.7rem', fontWeight: '700', color: '#94a3b8', textTransform: 'uppercase', marginBottom: '0.5rem' }}>SQL Query</div>
                                                                <div style={{
                                                                    background: '#0f172a', color: '#e2e8f0', padding: '1rem',
                                                                    borderRadius: '8px', fontSize: '0.85rem', fontFamily: 'monospace',
                                                                    overflowX: 'auto', borderLeft: '4px solid var(--primary)'
                                                                }}>
                                                                    {test.sql_query}
                                                                </div>
                                                            </div>
                                                        )}

                                                        {test.sample_data && test.sample_data.length > 0 && (
                                                            <div>
                                                                <div style={{ fontSize: '0.7rem', fontWeight: '700', color: '#ef4444', textTransform: 'uppercase', marginBottom: '0.5rem' }}>Sample Problematic Rows</div>
                                                                <div style={{ overflowX: 'auto', borderRadius: '8px', border: '1px solid #fee2e2' }}>
                                                                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85rem' }}>
                                                                        <thead style={{ background: '#fef2f2' }}>
                                                                            <tr>
                                                                                {Object.keys(test.sample_data[0]).map(key => (
                                                                                    <th key={key} style={{ padding: '0.5rem', textAlign: 'left', borderBottom: '1px solid #fee2e2' }}>{key}</th>
                                                                                ))}
                                                                            </tr>
                                                                        </thead>
                                                                        <tbody>
                                                                            {test.sample_data.map((row: any, rIdx: number) => (
                                                                                <tr key={rIdx} style={{ borderBottom: '1px solid #f8fafc' }}>
                                                                                    {Object.values(row).map((val: any, vIdx) => (
                                                                                        <td key={vIdx} style={{ padding: '0.5rem', color: val === null ? '#94a3b8' : '#475569' }}>
                                                                                            {val === null ? 'NULL' : String(val)}
                                                                                        </td>
                                                                                    ))}
                                                                                </tr>
                                                                            ))}
                                                                        </tbody>
                                                                    </table>
                                                                </div>
                                                            </div>
                                                        )}

                                                        {test.error_message && (
                                                            <div style={{ padding: '0.75rem', background: '#fee2e2', color: '#991b1b', borderRadius: '4px', fontSize: '0.85rem' }}>
                                                                <strong>Error:</strong> {test.error_message}
                                                            </div>
                                                        )}
                                                    </div>
                                                )}
                                            </div>
                                        );
                                    })}
                                </div>
                            </div>
                        )}
                    </div>
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
