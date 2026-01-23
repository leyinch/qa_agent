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
    sample_data?: unknown[];
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

export default function DashboardForm({ comparisonMode }: DashboardFormProps): React.JSX.Element {
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

    // Auto-set config table based on mode - Fix for SCD using GCS config tables
    useEffect(() => {
        if (comparisonMode === 'scd') {
            setConfigTable("scd_validation_config");
            setConfigDataset("config");
        } else if (comparisonMode === 'gcs') {
            setConfigTable("data_load_config");
            setConfigDataset("config");
        }
    }, [comparisonMode]);

    const [datasets, setDatasets] = useState<string[]>(['']);
    const [erdDescription, setErdDescription] = useState("");

    // GCS mode state
    const [gcsMode, setGcsMode] = useState<GCSMode>('config');
    const [configDataset, setConfigDataset] = useState("config");
    const [configTable, setConfigTable] = useState("data_load_config");
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

    // Settings state
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

    // Fetch Settings
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

    useEffect(() => {
        if (comparisonMode === 'settings' && projectId) {
            fetchSettings();
        }
    }, [comparisonMode, projectId]);

    // Fetch Columns
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

    // Fetch Existing SCD Config
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
        } catch (error: unknown) {
            const msg = error instanceof Error ? error.message : "Unknown error";
            showToast(msg, "error");
        } finally {
            setLoading(false);
        }
    };

    const handleAddConfig = async () => {
        if (!newTargetDataset || !newTargetTable || !newPrimaryKeys) {
            showToast("Please fill in all required fields (Dataset, Table, Primary Keys)", "error");
            return;
        }

        if (newScdType === 'scd2' && (!newBeginDateColumn || !newEndDateColumn || !newActiveFlagColumn)) {
            showToast("For SCD Type 2, Begin Date, End Date, and Active Flag columns are required.", "error");
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
        } catch (error: unknown) {
            const msg = error instanceof Error ? error.message : "Unknown error";
            showToast(msg, "error");
        }
    };

    const handleViewResult = (data: unknown, fromHistory: boolean = false) => {
        if (!data || typeof data !== 'object') {
            showToast("No data available.", "error");
            return;
        }

        // Use a type assertion or helper to access properties safely
        const dataObj = data as any;
        let details: any = dataObj;
        let executionId = "";
        let comparisonModeFromData = "";

        if (fromHistory && dataObj.execution_id) {
            details = dataObj.details;
            executionId = dataObj.execution_id;
            comparisonModeFromData = dataObj.comparison_mode;
        }

        if (!details) {
            showToast("No details available.", "error");
            return;
        }

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

        let detectedMode = comparisonModeFromData || comparisonMode;
        if (Array.isArray(details) && details.length > 0 && details[0].comparison_mode) {
            detectedMode = details[0].comparison_mode;
        } else if (details.comparison_mode) {
            detectedMode = details.comparison_mode;
        }

        const isArray = Array.isArray(details);
        const isMappingResult = isArray && details.length > 0 && 'predefined_results' in details[0];

        const dataToSave = isMappingResult
            ? { results: details, results_by_mapping: details, fromHistory, comparison_mode: detectedMode, execution_id: executionId }
            : (isArray
                ? { results: details, fromHistory, comparison_mode: detectedMode, execution_id: executionId }
                : { ...details, fromHistory, comparison_mode: details.comparison_mode || detectedMode, execution_id: details.execution_id || executionId }
            );

        localStorage.setItem("testResults", JSON.stringify(dataToSave));

        const isSCD = detectedMode?.toLowerCase().includes('scd') ||
            comparisonMode === 'scd' ||
            (Array.isArray(details) && details.length > 0 && 'predefined_results' in details[0]);

        if (isSCD) {
            // Show SCD results inline (whether from fresh run or history)
            setResultsData(dataToSave);
            setShowResults(true);
            setActiveMappingIdx(0);
            setExpandedTests(new Set());
            window.scrollTo({ top: 0, behavior: 'smooth' });
        } else {
            // For GCS comparison, navigate to results page
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

        } catch (error: unknown) {
            const msg = error instanceof Error ? error.message : "Unknown error";
            showToast(msg, "error");
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
                                        comparisonMode === 'scd' ? (showResults ? ' SCD Validation Results' : 'SCD Validation Setup') :
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
                            Back
                        </button>
                    )}
                </div>

                {comparisonMode === 'settings' ? (
                    <div className="fade-in">
                        <div style={{ marginBottom: '1.75rem' }}>
                            <label className="label" htmlFor="alertEmails">Alert Emails (Comma separated)</label>
                            <input id="alertEmails" type="text" className="input" value={alertEmails} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setAlertEmails(e.target.value)} placeholder="user@example.com" />
                        </div>
                        <div style={{ marginBottom: '1.75rem' }}>
                            <label className="label" htmlFor="teamsWebhook">Teams Webhook URL</label>
                            <input id="teamsWebhook" type="text" className="input" value={teamsWebhook} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setTeamsWebhook(e.target.value)} />
                        </div>
                        <div style={{ marginBottom: '2rem', display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                            <input id="alertOnFailure" type="checkbox" checked={alertOnFailure} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setAlertOnFailure(e.target.checked)} style={{ width: '1.25rem', height: '1.25rem' }} />
                            <label htmlFor="alertOnFailure" style={{ fontSize: '1rem', cursor: 'pointer' }}>Enable Alerts on Test Failure</label>
                        </div>
                        <button type="submit" className="btn btn-primary" style={{ width: '100%', fontSize: '1rem', padding: '1rem' }} disabled={loading}>
                            {loading ? 'Saving...' : 'Save Settings'}
                        </button>
                    </div>
                ) : comparisonMode === 'history' ? (
                    !showResults ? <HistoryList projectId={projectId} onViewResult={handleViewResult} /> : null
                ) : (
                    <>
                        {comparisonMode === 'schema' && (
                            <>
                                <div style={{ marginBottom: '1.75rem' }}>
                                    <label className="label">BigQuery Datasets</label>
                                    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                                        {datasets.map((dataset, index) => (
                                            <div key={index} style={{ display: 'flex', gap: '0.75rem' }}>
                                                <input type="text" className="input" value={dataset} onChange={(e) => handleDatasetChange(index, e.target.value)} placeholder="Dataset Name" style={{ flex: 1 }} />
                                                {datasets.length > 1 && <button type="button" onClick={() => removeDataset(index)} style={{ padding: '0 1rem', background: 'var(--error)', color: 'white', borderRadius: '4px', border: 'none' }}>Remove</button>}
                                            </div>
                                        ))}
                                    </div>
                                    <button type="button" onClick={addDataset} style={{ marginTop: '1rem', padding: '0.5rem', width: '100%', background: 'var(--secondary)', border: '1px dashed var(--primary)' }}>Add Dataset</button>
                                </div>
                                <div style={{ marginBottom: '2rem' }}>
                                    <label className="label">ER Diagram Description</label>
                                    <textarea className="input" value={erdDescription} onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => setErdDescription(e.target.value)} rows={8} placeholder="Describe table relationships..." required />
                                </div>
                            </>
                        )}

                        {comparisonMode === 'gcs' && (
                            <>
                                <div style={{ marginBottom: '2rem', display: 'flex', gap: '1rem' }}>
                                    <button type="button" onClick={() => setGcsMode('single')} style={{ flex: 1, padding: '0.75rem', background: gcsMode === 'single' ? 'var(--primary)' : 'var(--secondary)', color: gcsMode === 'single' ? 'white' : 'var(--foreground)', border: 'none', borderRadius: '4px' }}>Single File</button>
                                    <button type="button" onClick={() => setGcsMode('config')} style={{ flex: 1, padding: '0.75rem', background: gcsMode === 'config' ? 'var(--primary)' : 'var(--secondary)', color: gcsMode === 'config' ? 'white' : 'var(--foreground)', border: 'none', borderRadius: '4px' }}>Config Table</button>
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

                        {comparisonMode === 'scd' && !showResults && (
                            <>
                                <div style={{ marginBottom: '2rem', display: 'flex', gap: '1rem' }}>
                                    <button type="button" onClick={() => setScdMode('direct')} style={{ flex: 1, padding: '0.75rem', background: scdMode === 'direct' ? 'var(--primary)' : 'var(--secondary)', color: scdMode === 'direct' ? 'white' : 'var(--foreground)', border: 'none', borderRadius: '4px' }}>Direct Input</button>
                                    <button type="button" onClick={() => setScdMode('config')} style={{ flex: 1, padding: '0.75rem', background: scdMode === 'config' ? 'var(--primary)' : 'var(--secondary)', color: scdMode === 'config' ? 'white' : 'var(--foreground)', border: 'none', borderRadius: '4px' }}>Config Table</button>
                                </div>
                                {scdMode === 'config' && (
                                    <>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Config Dataset</label><input className="input" value={configDataset} onChange={e => setConfigDataset(e.target.value)} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Config Table</label><input className="input" value={configTable} onChange={e => setConfigTable(e.target.value)} required /></div>
                                        <button type="button" onClick={() => setShowAddConfig(!showAddConfig)} style={{ width: '100%', padding: '0.75rem', background: 'var(--secondary)', border: '1px solid var(--primary)', color: 'var(--primary)', marginBottom: '1rem' }}>{showAddConfig ? 'Cancel' : 'Add / Update Configuration'}</button>

                                        {showAddConfig && (
                                            <div style={{ padding: '1rem', border: '1px solid var(--border)', borderRadius: '4px', marginBottom: '1rem', background: 'var(--secondary)' }}>
                                                <div style={{ marginBottom: '1rem' }}><label className="label">Target Dataset</label><input className="input" value={newTargetDataset} onChange={e => setNewTargetDataset(e.target.value)} required /></div>
                                                <div style={{ marginBottom: '1rem' }}><label className="label">Target Table</label><input className="input" value={newTargetTable} onChange={e => setNewTargetTable(e.target.value)} required /></div>
                                                <div style={{ marginBottom: '1rem' }}><label className="label">Primary Keys</label><input className="input" value={newPrimaryKeys} onChange={e => setNewPrimaryKeys(e.target.value)} required /></div>
                                                <div style={{ marginBottom: '1rem' }}><label className="label">Surrogate Key</label><input className="input" value={newSurrogateKey} onChange={e => setNewSurrogateKey(e.target.value)} placeholder="Optional" /></div>

                                                {newScdType === 'scd2' && (
                                                    <>
                                                        <div style={{ marginBottom: '1rem' }}>
                                                            <label className="label">Begin Date Column *</label>
                                                            <input className="input" value={newBeginDateColumn} onChange={e => setNewBeginDateColumn(e.target.value)} required />
                                                        </div>
                                                        <div style={{ marginBottom: '1rem' }}>
                                                            <label className="label">End Date Column *</label>
                                                            <input className="input" value={newEndDateColumn} onChange={e => setNewEndDateColumn(e.target.value)} required />
                                                        </div>
                                                        <div style={{ marginBottom: '1rem' }}>
                                                            <label className="label">Active Flag Column *</label>
                                                            <input className="input" value={newActiveFlagColumn} onChange={e => setNewActiveFlagColumn(e.target.value)} required />
                                                        </div>
                                                    </>
                                                )}

                                                {/* Custom Business Rules Section */}
                                                <div style={{ marginBottom: '1rem', padding: '1rem', border: '1px solid var(--border)', borderRadius: '4px', background: '#f8fafc' }}>
                                                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.75rem' }}>
                                                        <label className="label" style={{ margin: 0 }}>Custom Business Rules</label>
                                                        <button
                                                            type="button"
                                                            onClick={() => {
                                                                setNewCustomTests([...newCustomTests, {
                                                                    name: '',
                                                                    description: '',
                                                                    sql: '',
                                                                    severity: 'HIGH'
                                                                }]);
                                                            }}
                                                            style={{
                                                                padding: '0.4rem 0.75rem',
                                                                background: 'var(--primary)',
                                                                color: 'white',
                                                                border: 'none',
                                                                borderRadius: '4px',
                                                                fontSize: '0.75rem',
                                                                fontWeight: '600',
                                                                cursor: 'pointer'
                                                            }}
                                                        >
                                                            + Add Rule
                                                        </button>
                                                    </div>

                                                    {newCustomTests.length === 0 && (
                                                        <div style={{ fontSize: '0.875rem', color: '#94a3b8', fontStyle: 'italic', padding: '0.5rem' }}>
                                                            No custom rules defined. Click "+ Add Rule" to create one.
                                                        </div>
                                                    )}

                                                    {newCustomTests.map((test, idx) => (
                                                        <div key={idx} style={{ marginBottom: '1rem', padding: '0.75rem', border: '1px solid #e2e8f0', borderRadius: '4px', background: 'white' }}>
                                                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                                                                <div style={{ fontSize: '0.75rem', fontWeight: '600', color: '#64748b' }}>Rule {idx + 1}</div>
                                                                <button
                                                                    type="button"
                                                                    onClick={() => {
                                                                        const updated = newCustomTests.filter((_, i) => i !== idx);
                                                                        setNewCustomTests(updated);
                                                                    }}
                                                                    style={{
                                                                        padding: '0.25rem 0.5rem',
                                                                        background: '#fee2e2',
                                                                        color: '#991b1b',
                                                                        border: '1px solid #fecaca',
                                                                        borderRadius: '4px',
                                                                        fontSize: '0.7rem',
                                                                        cursor: 'pointer'
                                                                    }}
                                                                >
                                                                    Remove
                                                                </button>
                                                            </div>
                                                            <div style={{ marginBottom: '0.5rem' }}>
                                                                <label style={{ fontSize: '0.75rem', fontWeight: '600', color: '#475569', display: 'block', marginBottom: '0.25rem' }}>Test Name</label>
                                                                <input
                                                                    className="input"
                                                                    value={test.name}
                                                                    onChange={e => {
                                                                        const updated = [...newCustomTests];
                                                                        updated[idx].name = e.target.value;
                                                                        setNewCustomTests(updated);
                                                                    }}
                                                                    placeholder="e.g., check_balance_positive"
                                                                    style={{ fontSize: '0.875rem' }}
                                                                />
                                                            </div>
                                                            <div style={{ marginBottom: '0.5rem' }}>
                                                                <label style={{ fontSize: '0.75rem', fontWeight: '600', color: '#475569', display: 'block', marginBottom: '0.25rem' }}>Description</label>
                                                                <input
                                                                    className="input"
                                                                    value={test.description}
                                                                    onChange={e => {
                                                                        const updated = [...newCustomTests];
                                                                        updated[idx].description = e.target.value;
                                                                        setNewCustomTests(updated);
                                                                    }}
                                                                    placeholder="Describe what this rule validates"
                                                                    style={{ fontSize: '0.875rem' }}
                                                                />
                                                            </div>
                                                            <div style={{ marginBottom: '0.5rem' }}>
                                                                <label style={{ fontSize: '0.75rem', fontWeight: '600', color: '#475569', display: 'block', marginBottom: '0.25rem' }}>SQL Query</label>
                                                                <textarea
                                                                    className="input"
                                                                    value={test.sql}
                                                                    onChange={e => {
                                                                        const updated = [...newCustomTests];
                                                                        updated[idx].sql = e.target.value;
                                                                        setNewCustomTests(updated);
                                                                    }}
                                                                    placeholder="SELECT * FROM {table} WHERE balance < 0"
                                                                    rows={3}
                                                                    style={{ fontSize: '0.875rem', fontFamily: 'monospace' }}
                                                                />
                                                            </div>
                                                            <div>
                                                                <label style={{ fontSize: '0.75rem', fontWeight: '600', color: '#475569', display: 'block', marginBottom: '0.25rem' }}>Severity</label>
                                                                <select
                                                                    className="input"
                                                                    value={test.severity}
                                                                    onChange={e => {
                                                                        const updated = [...newCustomTests];
                                                                        updated[idx].severity = e.target.value as 'HIGH' | 'MEDIUM' | 'LOW';
                                                                        setNewCustomTests(updated);
                                                                    }}
                                                                    style={{ fontSize: '0.875rem' }}
                                                                >
                                                                    <option value="HIGH">High</option>
                                                                    <option value="MEDIUM">Medium</option>
                                                                    <option value="LOW">Low</option>
                                                                </select>
                                                            </div>
                                                        </div>
                                                    ))}
                                                </div>

                                                <div style={{ marginBottom: '1rem' }}><label className="label">SCD Type</label><select className="input" value={newScdType} onChange={e => setNewScdType(e.target.value as any)}><option value="scd1">Type 1</option><option value="scd2">Type 2</option></select></div>
                                                <button type="button" onClick={handleAddConfig} style={{ width: '100%', padding: '0.75rem', background: 'var(--primary)', color: 'white', border: 'none', fontWeight: 'bold' }}>Save Configuration</button>
                                            </div>
                                        )}
                                    </>
                                )}
                                {scdMode === 'direct' && (
                                    <>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Target Dataset</label><input className="input" value={targetDataset} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setTargetDataset(e.target.value)} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Target Table</label><input className="input" value={targetTable} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setTargetTable(e.target.value)} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">Primary Keys</label><input className="input" value={primaryKeys} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setPrimaryKeys(e.target.value)} required /></div>
                                        <div style={{ marginBottom: '1rem' }}><label className="label">SCD Type</label><select className="input" value={scdType} onChange={(e: React.ChangeEvent<HTMLSelectElement>) => setScdType(e.target.value as any)}><option value="scd1">Type 1</option><option value="scd2">Type 2</option></select></div>
                                    </>
                                )}
                            </>
                        )}

                        {!showResults && (
                            <button type="submit" className="btn btn-primary" style={{ width: '100%', fontSize: '1rem', padding: '1rem', marginTop: '1.5rem' }} disabled={loading}>
                                {loading ? 'Running Tests...' : 'Run Tests'}
                            </button>
                        )}
                    </>
                )}

                {(comparisonMode === 'scd' || (showResults && resultsData?.comparison_mode === 'scd')) && showResults && resultsData && (
                    <div className="fade-in" style={{ marginTop: '2rem' }}>

                        {/* Summary Section */}
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '2rem' }}>
                            <div>
                                {resultsData.execution_id && (
                                    <div style={{ fontSize: '0.75rem', color: '#94a3b8', fontFamily: 'monospace' }} title={resultsData.execution_id}>
                                        Run ID: <span style={{ color: '#64748b', fontWeight: '600' }}>{resultsData.execution_id.substring(0, 8)}</span>
                                    </div>
                                )}
                            </div>
                            <div style={{ padding: '0.5rem 1rem', background: '#f8fafc', borderRadius: '8px', border: '1px solid #e2e8f0', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                <div style={{ width: '24px', height: '24px', background: '#3b82f6', borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'white', fontSize: '0.75rem', fontWeight: 'bold' }}>
                                    U
                                </div>
                                <div>
                                    <div style={{ fontSize: '0.75rem', color: '#94a3b8' }}>Triggered By</div>
                                    <div style={{ fontSize: '0.875rem', fontWeight: '600' }}>Manual Run</div>
                                </div>
                            </div>
                        </div>

                        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '1.5rem', marginBottom: '3rem' }}>
                            <div className="card" style={{ padding: '1.5rem', display: 'flex', flexDirection: 'column', justifyContent: 'space-between', height: '140px' }}>
                                <div style={{ fontSize: '0.875rem', fontWeight: '600', color: '#64748b' }}>Total Mappings</div>
                                <div style={{ fontSize: '2rem', fontWeight: '800', color: '#10b981' }}>{resultsData.summary.total_mappings}</div>
                            </div>
                            <div className="card" style={{ padding: '1.5rem', display: 'flex', flexDirection: 'column', justifyContent: 'space-between', height: '140px', borderLeft: '4px solid #10b981' }}>
                                <div style={{ fontSize: '0.875rem', fontWeight: '600', color: '#10b981' }}>Tests Passed</div>
                                <div style={{ fontSize: '2rem', fontWeight: '800', color: '#10b981' }}>{resultsData.summary.passed}</div>
                            </div>
                            <div className="card" style={{ padding: '1.5rem', display: 'flex', flexDirection: 'column', justifyContent: 'space-between', height: '140px', borderLeft: '4px solid #ef4444' }}>
                                <div style={{ fontSize: '0.875rem', fontWeight: '600', color: '#ef4444' }}>Tests Failed</div>
                                <div style={{ fontSize: '2rem', fontWeight: '800', color: '#ef4444' }}>{resultsData.summary.failed + resultsData.summary.errors}</div>
                            </div>
                        </div>

                        {/* Mapping Selector Pills */}
                        <div style={{ display: 'flex', gap: '1rem', marginBottom: '2rem', overflowX: 'auto', paddingBottom: '0.5rem' }}>
                            {resultsData.results_by_mapping.map((mapping: MappingResult, idx: number) => {
                                const passed = mapping.predefined_results.filter((r: TestResult) => r.status === 'PASS').length;
                                const failed = mapping.predefined_results.filter((r: TestResult) => r.status !== 'PASS').length;
                                const isActive = activeMappingIdx === idx;
                                return (
                                    <button
                                        key={idx}
                                        type="button"
                                        onClick={() => setActiveMappingIdx(idx)}
                                        style={{
                                            padding: '0.75rem 1.5rem',
                                            borderRadius: '8px',
                                            border: 'none',
                                            background: isActive ? '#10b981' : '#f8fafc',
                                            color: isActive ? 'white' : '#64748b',
                                            fontWeight: '700',
                                            fontSize: '0.9rem',
                                            cursor: 'pointer',
                                            display: 'flex',
                                            alignItems: 'center',
                                            gap: '0.5rem',
                                            boxShadow: isActive ? '0 4px 6px -1px rgba(16, 185, 129, 0.2)' : 'none',
                                            transition: 'all 0.2s ease'
                                        }}
                                    >
                                        <span>{mapping.mapping_id}</span>
                                        <div style={{ display: 'flex', gap: '0.5rem' }}>
                                            {passed > 0 && (
                                                <span style={{
                                                    background: isActive ? 'rgba(255,255,255,0.2)' : '#d1fae5',
                                                    color: isActive ? 'white' : '#10b981',
                                                    padding: '2px 8px',
                                                    borderRadius: '12px',
                                                    fontSize: '0.75rem',
                                                    fontWeight: '600'
                                                }}>
                                                    {passed}
                                                </span>
                                            )}
                                            {failed > 0 && (
                                                <span style={{
                                                    background: isActive ? 'rgba(255,255,255,0.2)' : '#fee2e2',
                                                    color: isActive ? 'white' : '#ef4444',
                                                    padding: '2px 8px',
                                                    borderRadius: '12px',
                                                    fontSize: '0.75rem',
                                                    fontWeight: '600'
                                                }}>
                                                    {failed}
                                                </span>
                                            )}
                                        </div>
                                    </button>
                                );
                            })}
                        </div>

                        {resultsData.results_by_mapping[activeMappingIdx] && (
                            <div className="fade-in">
                                {/* Current Mapping Header */}
                                <div style={{
                                    padding: '1.5rem 2rem',
                                    background: '#f8fafc',
                                    borderRadius: '12px',
                                    border: '1px solid #e2e8f0',
                                    marginBottom: '2rem',
                                    display: 'flex',
                                    justifyContent: 'space-between',
                                    alignItems: 'center'
                                }}>
                                    <div>
                                        <div style={{ fontSize: '0.75rem', fontWeight: '700', color: '#64748b', marginBottom: '0.5rem', letterSpacing: '0.05em' }}>CURRENT MAPPING</div>
                                        <div style={{ fontSize: '1.5rem', fontWeight: '800', color: '#0f172a' }}>{resultsData.results_by_mapping[activeMappingIdx].mapping_id}</div>
                                    </div>
                                    <div style={{ textAlign: 'right' }}>
                                        <div style={{ fontSize: '0.75rem', color: '#64748b' }}>Target Dataset/Table</div>
                                        <div style={{ fontSize: '1rem', fontWeight: '600', color: '#0f172a' }}>{resultsData.results_by_mapping[activeMappingIdx].mapping_info?.target}</div>
                                    </div>
                                </div>

                                {/* Test Cards */}
                                <div style={{ display: 'grid', gap: '1rem' }}>
                                    {resultsData.results_by_mapping[activeMappingIdx].predefined_results.map((test: TestResult, tIdx: number) => {
                                        const isExpanded = expandedTests.has(`${activeMappingIdx}-${tIdx}`);
                                        const isPass = test.status === 'PASS';

                                        return (
                                            <div key={tIdx} className="card" style={{ padding: '0', overflow: 'hidden', border: '1px solid #e2e8f0', boxShadow: '0 1px 3px rgba(0,0,0,0.05)' }}>
                                                {/* Header Row */}
                                                <div
                                                    style={{ padding: '1.25rem 1.5rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}
                                                >
                                                    <div style={{ display: 'flex', alignItems: 'center', gap: '1.25rem' }}>
                                                        <div style={{
                                                            width: '36px', height: '36px',
                                                            background: isPass ? '#dcfce7' : '#fee2e2',
                                                            borderRadius: '8px',
                                                            display: 'flex', alignItems: 'center', justifyContent: 'center',
                                                            fontSize: '1.25rem',
                                                            color: isPass ? '#10b981' : '#ef4444'
                                                        }}>
                                                            {isPass ? '✓' : '✗'}
                                                        </div>
                                                        <div>
                                                            <div style={{ fontSize: '1rem', fontWeight: '700', color: '#0f172a', marginBottom: '0.2rem' }}>{test.test_name}</div>
                                                            <div style={{ fontSize: '0.8rem', color: '#94a3b8' }}>{test.category || 'validation'}</div>
                                                        </div>
                                                    </div>

                                                    <div style={{ display: 'flex', alignItems: 'center', gap: '3rem' }}>


                                                        <div style={{ textAlign: 'center' }}>
                                                            <div style={{ fontSize: '0.65rem', fontWeight: '700', color: '#64748b', marginBottom: '0.2rem' }}>SEVERITY</div>
                                                            <div style={{ fontSize: '0.85rem', fontWeight: '700', color: '#ef4444' }}>{test.severity}</div>
                                                        </div>

                                                        <div style={{ textAlign: 'center' }}>
                                                            <div style={{ fontSize: '0.65rem', fontWeight: '700', color: '#64748b', marginBottom: '0.2rem' }}>AFFECTED</div>
                                                            <div style={{ fontSize: '1rem', fontWeight: '700', color: test.rows_affected > 0 ? '#ef4444' : '#0f172a' }}>{test.rows_affected}</div>
                                                        </div>

                                                        <button
                                                            type="button"
                                                            onClick={() => toggleTestExpansion(activeMappingIdx, tIdx)}
                                                            style={{
                                                                padding: '0.5rem 1rem',
                                                                background: 'white',
                                                                border: '1px solid #e2e8f0',
                                                                borderRadius: '6px',
                                                                fontSize: '0.8rem',
                                                                fontWeight: '600',
                                                                color: '#10b981',
                                                                cursor: 'pointer',
                                                                whiteSpace: 'nowrap'
                                                            }}
                                                        >
                                                            {isExpanded ? 'Hide Details' : 'View Details'}
                                                        </button>
                                                    </div>
                                                </div>

                                                {/* Expanded Details */}
                                                {isExpanded && (
                                                    <div style={{ borderTop: '1px solid #e2e8f0', padding: '1.5rem', background: '#f8fafc' }}>
                                                        <div style={{ marginBottom: '1.5rem' }}>
                                                            <div style={{ fontSize: '0.75rem', fontWeight: '700', color: '#64748b', marginBottom: '0.5rem' }}>DESCRIPTION</div>
                                                            <div style={{ color: '#334155' }}>{test.description}</div>
                                                        </div>

                                                        {test.error_message && (
                                                            <div style={{ marginBottom: '1.5rem', padding: '1rem', background: '#fee2e2', borderRadius: '6px', fontSize: '0.9rem', color: '#b91c1c' }}>
                                                                <strong>Error:</strong> {test.error_message}
                                                            </div>
                                                        )}

                                                        {Array.isArray(test.sample_data) && test.sample_data.length > 0 && (
                                                            <div>
                                                                <div style={{ fontSize: '0.75rem', fontWeight: '700', color: '#ef4444', marginBottom: '0.75rem', textTransform: 'uppercase' }}>Sample Problematic Rows</div>
                                                                <div style={{ overflowX: 'auto', background: 'white', borderRadius: '6px', border: '1px solid #e2e8f0' }}>
                                                                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85rem' }}>
                                                                        <thead>
                                                                            <tr style={{ background: '#f1f5f9', borderBottom: '1px solid #e2e8f0' }}>
                                                                                {Object.keys(test.sample_data[0] as object).map((key, k) => (
                                                                                    <th key={k} style={{ padding: '0.75rem 1rem', textAlign: 'left', fontWeight: '600', color: '#475569' }}>{key}</th>
                                                                                ))}
                                                                            </tr>
                                                                        </thead>
                                                                        <tbody>
                                                                            {test.sample_data.map((row: any, rIdx) => (
                                                                                <tr key={rIdx} style={{ borderBottom: rIdx < test.sample_data!.length - 1 ? '1px solid #f1f5f9' : 'none' }}>
                                                                                    {Object.values(row).map((val: any, vIdx) => (
                                                                                        <td key={vIdx} style={{ padding: '0.75rem 1rem', color: '#334155' }}>
                                                                                            {val === null || val === undefined ? <span style={{ color: '#94a3b8', fontStyle: 'italic' }}>NULL</span> : String(val)}
                                                                                        </td>
                                                                                    ))}
                                                                                </tr>
                                                                            ))}
                                                                        </tbody>
                                                                    </table>
                                                                </div>
                                                            </div>
                                                        )}

                                                        <div style={{ marginTop: '1.5rem' }}>
                                                            <div style={{ fontSize: '0.75rem', fontWeight: '700', color: '#64748b', marginBottom: '0.5rem' }}>SQL QUERY</div>
                                                            <div style={{ background: '#1e293b', color: '#e2e8f0', padding: '1rem', borderRadius: '6px', fontFamily: 'monospace', fontSize: '0.85rem', overflowX: 'auto' }}>
                                                                {test.sql_query}
                                                            </div>
                                                        </div>
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
                <div style={{ position: 'fixed', bottom: '2rem', right: '2rem', padding: '1rem 2rem', background: toast.type === 'error' ? 'var(--error)' : 'var(--primary)', color: 'white', borderRadius: 'var(--radius)', boxShadow: 'var(--shadow-lg)' }}>
                    {toast.message}
                </div>
            )}
        </>
    );
}
