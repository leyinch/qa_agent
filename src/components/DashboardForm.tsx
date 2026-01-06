"use client";

import React, { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import HistoryList from "./HistoryList";

type ComparisonMode = 'schema' | 'gcs' | 'history' | 'scd';
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
    const [configDataset, setConfigDataset] = useState("");
    const [configTable, setConfigTable] = useState("");
    const [gcsBucket, setGcsBucket] = useState("");
    const [gcsFilePath, setGcsFilePath] = useState("");
    const [fileFormat, setFileFormat] = useState<FileFormat>('csv');
    const [targetDataset, setTargetDataset] = useState("");
    const [targetTable, setTargetTable] = useState("");

    // SCD mode state
    const [scdMode, setScdMode] = useState<SCDMode>('direct');
    const [scdType, setScdType] = useState<'scd1' | 'scd2'>('scd2');
    const [primaryKeys, setPrimaryKeys] = useState("");
    const [surrogateKey, setSurrogateKey] = useState("");
    const [beginDateColumn, setBeginDateColumn] = useState("DWBeginEffDateTime");
    const [endDateColumn, setEndDateColumn] = useState("DWEndEffDateTime");
    const [activeFlagColumn, setActiveFlagColumn] = useState("DWCurrentRowFlag");
    const [customTests, setCustomTests] = useState<CustomTest[]>([]);

    // New config form state
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

    const addCustomTest = (isNewConfig: boolean) => {
        const emptyTest: CustomTest = { name: "", sql: "", description: "", severity: "HIGH" };
        if (isNewConfig) {
            setNewCustomTests([...newCustomTests, emptyTest]);
        } else {
            setCustomTests([...customTests, emptyTest]);
        }
    };

    const removeCustomTest = (index: number, isNewConfig: boolean) => {
        if (isNewConfig) {
            setNewCustomTests(newCustomTests.filter((_, i) => i !== index));
        } else {
            setCustomTests(customTests.filter((_, i) => i !== index));
        }
    };

    const handleCustomTestChange = (index: number, field: keyof CustomTest, value: string, isNewConfig: boolean) => {
        if (isNewConfig) {
            const updated = [...newCustomTests];
            updated[index] = { ...updated[index], [field]: value };
            setNewCustomTests(updated);
        } else {
            const updated = [...customTests];
            updated[index] = { ...updated[index], [field]: value };
            setCustomTests(updated);
        }
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
            const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'https://data-qa-agent-backend-1037417342779.us-central1.run.app';
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
                if (gcsMode === 'single') {
                    payload = {
                        ...payload,
                        gcs_bucket: gcsBucket,
                        gcs_file_path: gcsFilePath,
                        file_format: fileFormat,
                        target_dataset: targetDataset,
                        target_table: targetTable,
                        erd_description: erdDescription // Optional
                    };
                } else if (gcsMode === 'config') {
                    payload = {
                        ...payload,
                        comparison_mode: 'gcs-config',
                        config_dataset: configDataset,
                        config_table: configTable
                    };
                }
            } else if (comparisonMode === 'scd') {
                if (scdMode === 'direct') {
                    if (!targetDataset || !targetTable || !primaryKeys) {
                        throw new Error("Target dataset, table, and primary keys are required for SCD validation.");
                    }
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
                } else if (scdMode === 'config') {
                    payload = {
                        ...payload,
                        comparison_mode: 'scd-config',
                        config_dataset: configDataset,
                        config_table: configTable
                    };
                }
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

        } catch (error: any) {
            console.error("Error generating tests:", error);
            alert(error.message || "An error occurred while generating tests.");
        } finally {
            setLoading(false);
        }
    };

    const handleAddConfig = async () => {
        try {
            if (!newConfigId || !newTargetDataset || !newTargetTable || !newPrimaryKeys) {
                alert("Please fill in all required fields (Config ID, Dataset, Table, Primary Keys)");
                return;
            }

            const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'https://data-qa-agent-backend-1037417342779.us-central1.run.app';
            const endpoint = `${backendUrl}/api/scd-config`;

            const payload = {
                project_id: projectId,
                config_dataset: configDataset,
                config_table: configTable,
                config_id: newConfigId,
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

            const response = await fetch(endpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(payload),
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.detail || 'Failed to add configuration');
            }

            alert(`Configuration "${newConfigId}" added successfully!`);

            // Reset form
            setShowAddConfig(false);
            setNewConfigId("");
            setNewTargetDataset("");
            setNewTargetTable("");
            setNewPrimaryKeys("");
            setNewSurrogateKey("");
            setNewDescription("");

        } catch (error: any) {
            console.error("Error adding config:", error);
            alert(error.message || "An error occurred while adding the configuration.");
        }
    };

    return (
        <form onSubmit={handleSubmit} className="card fade-in" style={{ width: '100%', maxWidth: '800px' }}>
            {/* Header Removed */}

            {/* Comparison Mode Toggle Removed */}

            <h2 style={{ fontSize: '1.5rem', fontWeight: '700', marginBottom: '1.5rem', borderBottom: '1px solid var(--border)', paddingBottom: '0.75rem' }}>
                {comparisonMode === 'schema' && 'Schema Validation Setup'}
                {comparisonMode === 'gcs' && 'GCS Comparison Setup'}
                {comparisonMode === 'scd' && 'SCD Validation Setup'}
                {comparisonMode === 'history' && 'Execution History'}
            </h2>

            {/* Project ID (common field) */}
            {/* ... */}

            {/* Project ID (common field) */}
            <div style={{ marginBottom: '1.75rem' }}>
                <label className="label" htmlFor="projectId">
                    <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                        🔑 Google Cloud Project ID<span className="required">*</span>
                    </span>
                </label>
                <input
                    id="projectId"
                    type="text"
                    className="input"
                    value={projectId}
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) => setProjectId(e.target.value)}
                    required
                    placeholder="Project with BigQuery data (e.g., miruna-sandpit)"
                />
            </div>

            {/* History Mode */}
            {comparisonMode === 'history' ? (
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
                                                onChange={(e: React.ChangeEvent<HTMLInputElement>) => handleDatasetChange(index, e.target.value)}
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
                                                    onMouseEnter={(e: React.MouseEvent<HTMLButtonElement>) => (e.currentTarget.style.transform = 'scale(1.05)')}
                                                    onMouseLeave={(e: React.MouseEvent<HTMLButtonElement>) => (e.currentTarget.style.transform = 'scale(1)')}
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
                                    onMouseEnter={(e: React.MouseEvent<HTMLButtonElement>) => {
                                        e.currentTarget.style.background = 'var(--primary)';
                                        e.currentTarget.style.color = 'white';
                                    }}
                                    onMouseLeave={(e: React.MouseEvent<HTMLButtonElement>) => {
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
                                    onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => setErdDescription(e.target.value)}
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

                    {/* GCS Comparison Mode Fields */}
                    {comparisonMode === 'gcs' && (
                        <>
                            {/* GCS Mode Toggle */}
                            <div style={{ marginBottom: '2rem' }}>
                                <label className="label">Data Source</label>
                                <div style={{ display: 'flex', gap: '1rem', marginTop: '0.5rem' }}>
                                    <button
                                        type="button"
                                        onClick={() => setGcsMode('single')}
                                        style={{
                                            flex: 1,
                                            padding: '0.75rem',
                                            background: gcsMode === 'single' ? 'var(--gradient-primary)' : 'var(--secondary)',
                                            color: gcsMode === 'single' ? 'white' : 'var(--foreground)',
                                            border: gcsMode === 'single' ? 'none' : '2px solid var(--border)',
                                            borderRadius: 'var(--radius)',
                                            cursor: 'pointer',
                                            fontWeight: '600',
                                            transition: 'all 0.2s ease'
                                        }}
                                    >
                                        📄 Single File
                                    </button>
                                    <button
                                        type="button"
                                        onClick={() => setGcsMode('config')}
                                        style={{
                                            flex: 1,
                                            padding: '0.75rem',
                                            background: gcsMode === 'config' ? 'var(--gradient-primary)' : 'var(--secondary)',
                                            color: gcsMode === 'config' ? 'white' : 'var(--foreground)',
                                            border: gcsMode === 'config' ? 'none' : '2px solid var(--border)',
                                            borderRadius: 'var(--radius)',
                                            cursor: 'pointer',
                                            fontWeight: '600',
                                            transition: 'all 0.2s ease'
                                        }}
                                    >
                                        📋 Config Table
                                    </button>
                                </div>
                            </div>

                            {/* Config Table Mode */}
                            {gcsMode === 'config' && (
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
                                            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setConfigDataset(e.target.value)}
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
                                            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setConfigTable(e.target.value)}
                                            required
                                            placeholder="e.g., data_load_config"
                                        />
                                        <p style={{ fontSize: '0.8125rem', color: 'var(--secondary-foreground)', marginTop: '0.75rem', fontStyle: 'italic' }}>
                                            💡 The config table contains all GCS-to-BigQuery mappings and test configurations
                                        </p>
                                    </div>
                                </>
                            )}

                            {/* Single File Mode */}
                            {gcsMode === 'single' && (
                                <>
                                    {/* GCS Bucket */}
                                    <div style={{ marginBottom: '1.75rem' }}>
                                        <label className="label" htmlFor="gcsBucket">
                                            <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                                🪣 GCS Bucket Name
                                            </span>
                                        </label>
                                        <input
                                            id="gcsBucket"
                                            type="text"
                                            className="input"
                                            value={gcsBucket}
                                            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setGcsBucket(e.target.value)}
                                            required
                                            placeholder="e.g., my-data-bucket"
                                        />
                                    </div>

                                    {/* GCS File Path */}
                                    <div style={{ marginBottom: '1.75rem' }}>
                                        <label className="label" htmlFor="gcsFilePath">
                                            <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                                📄 File Path in Bucket
                                            </span>
                                        </label>
                                        <input
                                            id="gcsFilePath"
                                            type="text"
                                            className="input"
                                            value={gcsFilePath}
                                            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setGcsFilePath(e.target.value)}
                                            required
                                            placeholder="e.g., raw/customers_2024.csv or data/*.csv"
                                        />
                                        <p style={{ fontSize: '0.8125rem', color: 'var(--secondary-foreground)', marginTop: '0.5rem', fontStyle: 'italic' }}>
                                            💡 Supports wildcards for multiple files
                                        </p>
                                    </div>

                                    {/* File Format */}
                                    <div style={{ marginBottom: '1.75rem' }}>
                                        <label className="label" htmlFor="fileFormat">
                                            <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                                📋 File Format
                                            </span>
                                        </label>
                                        <select
                                            id="fileFormat"
                                            className="input"
                                            value={fileFormat}
                                            onChange={(e: React.ChangeEvent<HTMLSelectElement>) => setFileFormat(e.target.value as FileFormat)}
                                            required
                                            style={{ cursor: 'pointer' }}
                                        >
                                            <option value="csv">CSV</option>
                                            <option value="json">JSON</option>
                                            <option value="parquet">Parquet</option>
                                            <option value="avro">Avro</option>
                                        </select>
                                    </div>

                                    {/* Target Dataset */}
                                    <div style={{ marginBottom: '1.75rem' }}>
                                        <label className="label" htmlFor="targetDataset">
                                            <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                                🎯 Target BigQuery Dataset
                                            </span>
                                        </label>
                                        <input
                                            id="targetDataset"
                                            type="text"
                                            className="input"
                                            value={targetDataset}
                                            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setTargetDataset(e.target.value)}
                                            required
                                            placeholder="e.g., analytics"
                                        />
                                    </div>

                                    {/* Target Table */}
                                    <div style={{ marginBottom: '1.75rem' }}>
                                        <label className="label" htmlFor="targetTable">
                                            <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                                📊 Target BigQuery Table
                                            </span>
                                        </label>
                                        <input
                                            id="targetTable"
                                            type="text"
                                            className="input"
                                            value={targetTable}
                                            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setTargetTable(e.target.value)}
                                            required
                                            placeholder="e.g., customers"
                                        />
                                    </div>

                                    {/* Optional ERD Description for GCS mode */}
                                    <div style={{ marginBottom: '2rem' }}>
                                        <label className="label" htmlFor="erdDescriptionGcs">
                                            <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                                📝 Expected Schema (Optional)
                                            </span>
                                        </label>
                                        <textarea
                                            id="erdDescriptionGcs"
                                            className="input"
                                            value={erdDescription}
                                            onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => setErdDescription(e.target.value)}
                                            placeholder="Describe expected schema, data types, and constraints..."
                                            rows={6}
                                            style={{
                                                resize: 'vertical',
                                                fontFamily: 'JetBrains Mono, monospace',
                                                fontSize: '0.875rem',
                                                lineHeight: '1.6'
                                            }}
                                        />
                                        <p style={{ fontSize: '0.8125rem', color: 'var(--secondary-foreground)', marginTop: '0.75rem', fontStyle: 'italic' }}>
                                            💡 Optional: Helps AI generate better validation tests
                                        </p>
                                    </div>
                                </>
                            )}
                        </>
                    )}

                    {/* SCD Validation Mode Fields */}
                    {comparisonMode === 'scd' && (
                        <>
                            {/* SCD Mode Toggle */}
                            <div style={{ marginBottom: '2rem' }}>
                                <label className="label">Validation Source</label>
                                <div style={{ display: 'flex', gap: '1rem', marginTop: '0.5rem' }}>
                                    <button
                                        type="button"
                                        onClick={() => setScdMode('direct')}
                                        style={{
                                            flex: 1,
                                            padding: '0.75rem',
                                            background: scdMode === 'direct' ? 'var(--gradient-primary)' : 'var(--secondary)',
                                            color: scdMode === 'direct' ? 'white' : 'var(--foreground)',
                                            border: scdMode === 'direct' ? 'none' : '2px solid var(--border)',
                                            borderRadius: 'var(--radius)',
                                            cursor: 'pointer',
                                            fontWeight: '600',
                                            transition: 'all 0.2s ease'
                                        }}
                                    >
                                        ✏️ Direct Input
                                    </button>
                                    <button
                                        type="button"
                                        onClick={() => setScdMode('config')}
                                        style={{
                                            flex: 1,
                                            padding: '0.75rem',
                                            background: scdMode === 'config' ? 'var(--gradient-primary)' : 'var(--secondary)',
                                            color: scdMode === 'config' ? 'white' : 'var(--foreground)',
                                            border: scdMode === 'config' ? 'none' : '2px solid var(--border)',
                                            borderRadius: 'var(--radius)',
                                            cursor: 'pointer',
                                            fontWeight: '600',
                                            transition: 'all 0.2s ease'
                                        }}
                                    >
                                        📋 Config Table
                                    </button>
                                </div>
                            </div>

                            {/* Config Table Mode */}
                            {scdMode === 'config' && (
                                <>
                                    <div style={{ marginBottom: '1.75rem' }}>
                                        <label className="label" htmlFor="scdConfigDataset">
                                            <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                                📁 Config Dataset
                                            </span>
                                        </label>
                                        <input
                                            id="scdConfigDataset"
                                            type="text"
                                            className="input"
                                            value={configDataset}
                                            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setConfigDataset(e.target.value)}
                                            required
                                            placeholder="e.g., transform_config"
                                        />
                                    </div>

                                    <div style={{ marginBottom: '1.75rem' }}>
                                        <label className="label" htmlFor="scdConfigTable">
                                            <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                                📊 Config Table Name
                                            </span>
                                        </label>
                                        <input
                                            id="scdConfigTable"
                                            type="text"
                                            className="input"
                                            value={configTable}
                                            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setConfigTable(e.target.value)}
                                            required
                                            placeholder="e.g., scd_validation_config"
                                        />
                                        <p style={{ fontSize: '0.8125rem', color: 'var(--secondary-foreground)', marginTop: '0.75rem', fontStyle: 'italic' }}>
                                            💡 The config table contains all SCD dimension table configurations
                                        </p>
                                    </div>

                                    {/* Add New Table Button */}
                                    <button
                                        type="button"
                                        onClick={() => setShowAddConfig(!showAddConfig)}
                                        style={{
                                            marginBottom: '1.5rem',
                                            padding: '0.75rem 1.25rem',
                                            backgroundColor: showAddConfig ? 'var(--secondary)' : 'var(--primary)',
                                            color: showAddConfig ? 'var(--primary)' : 'white',
                                            border: showAddConfig ? '2px solid var(--primary)' : 'none',
                                            borderRadius: 'var(--radius)',
                                            cursor: 'pointer',
                                            fontWeight: '600',
                                            fontSize: '0.875rem',
                                            width: '100%',
                                            transition: 'all 0.2s ease'
                                        }}
                                    >
                                        {showAddConfig ? '✖️ Cancel' : '➕ Add New Table Configuration'}
                                    </button>

                                    {/* Add New Config Form */}
                                    {showAddConfig && (
                                        <div style={{
                                            padding: '1.5rem',
                                            background: 'var(--secondary)',
                                            borderRadius: 'var(--radius)',
                                            border: '2px solid var(--primary)',
                                            marginBottom: '1.75rem'
                                        }}>
                                            <h3 style={{ fontSize: '1rem', fontWeight: '600', marginBottom: '1rem' }}>📝 New Table Configuration</h3>

                                            {/* Config ID */}
                                            <div style={{ marginBottom: '1rem' }}>
                                                <label className="label" htmlFor="newConfigId">Config ID *</label>
                                                <input
                                                    id="newConfigId"
                                                    type="text"
                                                    className="input"
                                                    value={newConfigId}
                                                    onChange={(e: React.ChangeEvent<HTMLInputElement>) => setNewConfigId(e.target.value)}
                                                    placeholder="e.g., my_table_scd2"
                                                />
                                            </div>

                                            {/* Target Dataset & Table */}
                                            <div style={{ display: 'flex', gap: '1rem', marginBottom: '1rem' }}>
                                                <div style={{ flex: 1 }}>
                                                    <label className="label" htmlFor="newTargetDataset">Target Dataset *</label>
                                                    <input
                                                        id="newTargetDataset"
                                                        type="text"
                                                        className="input"
                                                        value={newTargetDataset}
                                                        onChange={(e: React.ChangeEvent<HTMLInputElement>) => setNewTargetDataset(e.target.value)}
                                                        placeholder="e.g., DW_Dimensions"
                                                    />
                                                </div>
                                                <div style={{ flex: 1 }}>
                                                    <label className="label" htmlFor="newTargetTable">Target Table *</label>
                                                    <input
                                                        id="newTargetTable"
                                                        type="text"
                                                        className="input"
                                                        value={newTargetTable}
                                                        onChange={(e: React.ChangeEvent<HTMLInputElement>) => setNewTargetTable(e.target.value)}
                                                        placeholder="e.g., D_MyTable_WD"
                                                    />
                                                </div>
                                            </div>

                                            {/* SCD Type Toggle */}
                                            <div style={{ marginBottom: '1rem' }}>
                                                <label className="label">SCD Type *</label>
                                                <div style={{ display: 'flex', gap: '0.5rem' }}>
                                                    <button
                                                        type="button"
                                                        onClick={() => setNewScdType('scd1')}
                                                        style={{
                                                            flex: 1,
                                                            padding: '0.5rem',
                                                            background: newScdType === 'scd1' ? 'var(--primary)' : 'var(--background)',
                                                            color: newScdType === 'scd1' ? 'white' : 'var(--foreground)',
                                                            border: '1px solid var(--border)',
                                                            borderRadius: 'var(--radius)',
                                                            cursor: 'pointer',
                                                            fontSize: '0.875rem'
                                                        }}
                                                    >
                                                        Type 1
                                                    </button>
                                                    <button
                                                        type="button"
                                                        onClick={() => setNewScdType('scd2')}
                                                        style={{
                                                            flex: 1,
                                                            padding: '0.5rem',
                                                            background: newScdType === 'scd2' ? 'var(--primary)' : 'var(--background)',
                                                            color: newScdType === 'scd2' ? 'white' : 'var(--foreground)',
                                                            border: '1px solid var(--border)',
                                                            borderRadius: 'var(--radius)',
                                                            cursor: 'pointer',
                                                            fontSize: '0.875rem'
                                                        }}
                                                    >
                                                        Type 2
                                                    </button>
                                                </div>
                                            </div>

                                            {/* Primary Keys */}
                                            <div style={{ marginBottom: '1rem' }}>
                                                <label className="label" htmlFor="newPrimaryKeys">Primary Keys *</label>
                                                <input
                                                    id="newPrimaryKeys"
                                                    type="text"
                                                    className="input"
                                                    value={newPrimaryKeys}
                                                    onChange={(e: React.ChangeEvent<HTMLInputElement>) => setNewPrimaryKeys(e.target.value)}
                                                    placeholder="e.g., UserId (comma-separated for composite)"
                                                />
                                            </div>

                                            {/* Surrogate Key */}
                                            <div style={{ marginBottom: '1rem' }}>
                                                <label className="label" htmlFor="newSurrogateKey">Surrogate Key (Optional)</label>
                                                <input
                                                    id="newSurrogateKey"
                                                    type="text"
                                                    className="input"
                                                    value={newSurrogateKey}
                                                    onChange={(e: React.ChangeEvent<HTMLInputElement>) => setNewSurrogateKey(e.target.value)}
                                                    placeholder="e.g., DWMyTableID"
                                                />
                                            </div>

                                            {/* SCD2 Fields */}
                                            {newScdType === 'scd2' && (
                                                <div style={{ marginBottom: '1rem', padding: '1rem', background: 'var(--background)', borderRadius: 'var(--radius)' }}>
                                                    <h4 style={{ fontSize: '0.875rem', fontWeight: '600', marginBottom: '0.75rem' }}>SCD Type 2 Columns</h4>
                                                    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                                                        <div>
                                                            <label className="label" htmlFor="newBeginDate">Begin Date Column</label>
                                                            <input
                                                                id="newBeginDate"
                                                                type="text"
                                                                className="input"
                                                                value={newBeginDateColumn}
                                                                onChange={(e: React.ChangeEvent<HTMLInputElement>) => setNewBeginDateColumn(e.target.value)}
                                                                placeholder="DWBeginEffDateTime"
                                                            />
                                                        </div>
                                                        <div>
                                                            <label className="label" htmlFor="newEndDate">End Date Column</label>
                                                            <input
                                                                id="newEndDate"
                                                                type="text"
                                                                className="input"
                                                                value={newEndDateColumn}
                                                                onChange={(e: React.ChangeEvent<HTMLInputElement>) => setNewEndDateColumn(e.target.value)}
                                                                placeholder="DWEndEffDateTime"
                                                            />
                                                        </div>
                                                        <div>
                                                            <label className="label" htmlFor="newActiveFlag">Active Flag Column</label>
                                                            <input
                                                                id="newActiveFlag"
                                                                type="text"
                                                                className="input"
                                                                value={newActiveFlagColumn}
                                                                onChange={(e: React.ChangeEvent<HTMLInputElement>) => setNewActiveFlagColumn(e.target.value)}
                                                                placeholder="DWCurrentRowFlag"
                                                            />
                                                        </div>
                                                    </div>
                                                </div>
                                            )}

                                            {/* Description */}
                                            <div style={{ marginBottom: '1rem' }}>
                                                <label className="label" htmlFor="newDescription">Description (Optional)</label>
                                                <input
                                                    id="newDescription"
                                                    type="text"
                                                    className="input"
                                                    value={newDescription}
                                                    onChange={(e: React.ChangeEvent<HTMLInputElement>) => setNewDescription(e.target.value)}
                                                    placeholder="e.g., Customer dimension table"
                                                />
                                            </div>

                                            {/* Custom Business Rules */}
                                            <div style={{ marginBottom: '1.5rem' }}>
                                                <label className="label">
                                                    <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                                        🛠️ Custom Business Rules
                                                    </span>
                                                </label>
                                                <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                                                    {newCustomTests.map((test, index) => (
                                                        <div key={index} style={{
                                                            padding: '1rem',
                                                            background: 'var(--background)',
                                                            borderRadius: 'var(--radius)',
                                                            border: '1px solid var(--border)',
                                                            position: 'relative'
                                                        }}>
                                                            <button
                                                                type="button"
                                                                onClick={() => removeCustomTest(index, true)}
                                                                style={{
                                                                    position: 'absolute',
                                                                    top: '0.5rem',
                                                                    right: '0.5rem',
                                                                    background: 'none',
                                                                    border: 'none',
                                                                    color: 'var(--error)',
                                                                    cursor: 'pointer',
                                                                    fontSize: '1.25rem'
                                                                }}
                                                            >
                                                                ×
                                                            </button>
                                                            <div style={{ display: 'flex', gap: '1rem', marginBottom: '0.75rem' }}>
                                                                <div style={{ flex: 1 }}>
                                                                    <label className="label" style={{ fontSize: '0.75rem' }}>Rule Name</label>
                                                                    <input
                                                                        type="text"
                                                                        className="input"
                                                                        value={test.name}
                                                                        onChange={(e: React.ChangeEvent<HTMLInputElement>) => handleCustomTestChange(index, 'name', e.target.value, true)}
                                                                        placeholder="e.g., CreatedDtm NOT NULL"
                                                                        style={{ marginBottom: 0 }}
                                                                    />
                                                                </div>
                                                                <div style={{ width: '120px' }}>
                                                                    <label className="label" style={{ fontSize: '0.75rem' }}>Severity</label>
                                                                    <select
                                                                        className="input"
                                                                        value={test.severity}
                                                                        onChange={(e: React.ChangeEvent<HTMLSelectElement>) => handleCustomTestChange(index, 'severity', e.target.value, true)}
                                                                        style={{ marginBottom: 0, padding: '0.55rem' }}
                                                                    >
                                                                        <option value="HIGH">HIGH</option>
                                                                        <option value="MEDIUM">MEDIUM</option>
                                                                        <option value="LOW">LOW</option>
                                                                    </select>
                                                                </div>
                                                            </div>
                                                            <div style={{ marginBottom: '0.75rem' }}>
                                                                <label className="label" style={{ fontSize: '0.75rem' }}>Description</label>
                                                                <input
                                                                    type="text"
                                                                    className="input"
                                                                    value={test.description}
                                                                    onChange={(e: React.ChangeEvent<HTMLInputElement>) => handleCustomTestChange(index, 'description', e.target.value, true)}
                                                                    placeholder="Describe the purpose of this rule..."
                                                                    style={{ marginBottom: 0 }}
                                                                />
                                                            </div>
                                                            <div>
                                                                <label className="label" style={{ fontSize: '0.75rem' }}>SQL Query (Use {'{{target}}'} for table name)</label>
                                                                <textarea
                                                                    className="input"
                                                                    value={test.sql}
                                                                    onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => handleCustomTestChange(index, 'sql', e.target.value, true)}
                                                                    placeholder="SELECT COUNT(0) = 0 FROM {{target}} WHERE CreatedDtm IS NULL"
                                                                    rows={3}
                                                                    style={{ marginBottom: 0, resize: 'vertical', fontFamily: 'monospace' }}
                                                                />
                                                            </div>
                                                        </div>
                                                    ))}
                                                    <button
                                                        type="button"
                                                        onClick={() => addCustomTest(true)}
                                                        style={{
                                                            padding: '0.5rem',
                                                            background: 'none',
                                                            border: '2px dashed var(--primary)',
                                                            color: 'var(--primary)',
                                                            borderRadius: 'var(--radius)',
                                                            cursor: 'pointer',
                                                            fontWeight: '600',
                                                            fontSize: '0.875rem'
                                                        }}
                                                    >
                                                        + Add Business Rule
                                                    </button>
                                                </div>
                                            </div>

                                            {/* Save Button */}
                                            <button
                                                type="button"
                                                onClick={handleAddConfig}
                                                style={{
                                                    width: '100%',
                                                    padding: '0.75rem',
                                                    background: 'var(--gradient-primary)',
                                                    color: 'white',
                                                    border: 'none',
                                                    borderRadius: 'var(--radius)',
                                                    cursor: 'pointer',
                                                    fontWeight: '600',
                                                    fontSize: '0.875rem'
                                                }}
                                            >
                                                💾 Save Configuration
                                            </button>
                                        </div>
                                    )}
                                </>
                            )}

                            {/* Direct Input Mode */}
                            {scdMode === 'direct' && (
                                <>
                                    {/* Target Dataset & Table */}
                                    <div style={{ display: 'flex', gap: '1.75rem' }}>
                                        <div style={{ flex: 1, marginBottom: '1.75rem' }}>
                                            <label className="label" htmlFor="targetDatasetScd">
                                                <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                                    🎯 Target Dataset
                                                </span>
                                            </label>
                                            <input
                                                id="targetDatasetScd"
                                                type="text"
                                                className="input"
                                                value={targetDataset}
                                                onChange={(e: React.ChangeEvent<HTMLInputElement>) => setTargetDataset(e.target.value)}
                                                required
                                                placeholder="e.g., DW_Dimensions"
                                            />
                                        </div>
                                        <div style={{ flex: 1, marginBottom: '1.75rem' }}>
                                            <label className="label" htmlFor="targetTableScd">
                                                <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                                    📊 Target Table
                                                </span>
                                            </label>
                                            <input
                                                id="targetTableScd"
                                                type="text"
                                                className="input"
                                                value={targetTable}
                                                onChange={(e: React.ChangeEvent<HTMLInputElement>) => setTargetTable(e.target.value)}
                                                required
                                                placeholder="e.g., D_Employee_WD"
                                            />
                                        </div>
                                    </div>

                                    {/* SCD Type Toggle */}
                                    <div style={{ marginBottom: '1.75rem' }}>
                                        <label className="label">SCD Type</label>
                                        <div style={{ display: 'flex', gap: '1rem', marginTop: '0.5rem' }}>
                                            <button
                                                type="button"
                                                onClick={() => setScdType('scd1')}
                                                style={{
                                                    flex: 1,
                                                    padding: '0.75rem',
                                                    background: scdType === 'scd1' ? 'var(--gradient-primary)' : 'var(--secondary)',
                                                    color: scdType === 'scd1' ? 'white' : 'var(--foreground)',
                                                    border: scdType === 'scd1' ? 'none' : '2px solid var(--border)',
                                                    borderRadius: 'var(--radius)',
                                                    cursor: 'pointer',
                                                    fontWeight: '600',
                                                    transition: 'all 0.2s ease'
                                                }}
                                            >
                                                🔢 SCD Type 1
                                            </button>
                                            <button
                                                type="button"
                                                onClick={() => setScdType('scd2')}
                                                style={{
                                                    flex: 1,
                                                    padding: '0.75rem',
                                                    background: scdType === 'scd2' ? 'var(--gradient-primary)' : 'var(--secondary)',
                                                    color: scdType === 'scd2' ? 'white' : 'var(--foreground)',
                                                    border: scdType === 'scd2' ? 'none' : '2px solid var(--border)',
                                                    borderRadius: 'var(--radius)',
                                                    cursor: 'pointer',
                                                    fontWeight: '600',
                                                    transition: 'all 0.2s ease'
                                                }}
                                            >
                                                🕒 SCD Type 2
                                            </button>
                                        </div>
                                    </div>

                                    {/* Primary Keys */}
                                    <div style={{ marginBottom: '1.75rem' }}>
                                        <label className="label" htmlFor="primaryKeys">
                                            <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                                🔑 Primary Keys
                                            </span>
                                        </label>
                                        <input
                                            id="primaryKeys"
                                            type="text"
                                            className="input"
                                            value={primaryKeys}
                                            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setPrimaryKeys(e.target.value)}
                                            required
                                            placeholder="e.g., UserId (comma separate for composite)"
                                        />
                                        <p style={{ fontSize: '0.8125rem', color: 'var(--secondary-foreground)', marginTop: '0.5rem', fontStyle: 'italic' }}>
                                            💡 Primary business identifier(s) used for comparison
                                        </p>
                                    </div>

                                    {/* Surrogate Key */}
                                    <div style={{ marginBottom: '1.75rem' }}>
                                        <label className="label" htmlFor="surrogateKey">
                                            <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                                🆔 Surrogate Key (Optional)
                                            </span>
                                        </label>
                                        <input
                                            id="surrogateKey"
                                            type="text"
                                            className="input"
                                            value={surrogateKey}
                                            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setSurrogateKey(e.target.value)}
                                            placeholder="e.g., DWEmployeeID"
                                        />
                                    </div>

                                    {/* SCD2 Specific Fields */}
                                    {scdType === 'scd2' && (
                                        <div style={{
                                            padding: '1.25rem',
                                            background: 'var(--secondary)',
                                            borderRadius: 'var(--radius)',
                                            border: '1px solid var(--border)',
                                            marginBottom: '1.75rem'
                                        }}>
                                            <h3 style={{ fontSize: '1rem', fontWeight: '600', marginBottom: '1rem' }}>📜 History Tracking Columns</h3>

                                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '1rem' }}>
                                                <div style={{ flex: '1 1 200px' }}>
                                                    <label className="label" htmlFor="beginDate">Begin Date</label>
                                                    <input
                                                        id="beginDate"
                                                        type="text"
                                                        className="input"
                                                        value={beginDateColumn}
                                                        onChange={(e: React.ChangeEvent<HTMLInputElement>) => setBeginDateColumn(e.target.value)}
                                                        placeholder="DWBeginEffDateTime"
                                                    />
                                                </div>
                                                <div style={{ flex: '1 1 200px' }}>
                                                    <label className="label" htmlFor="endDate">End Date</label>
                                                    <input
                                                        id="endDate"
                                                        type="text"
                                                        className="input"
                                                        value={endDateColumn}
                                                        onChange={(e: React.ChangeEvent<HTMLInputElement>) => setEndDateColumn(e.target.value)}
                                                        placeholder="DWEndEffDateTime"
                                                    />
                                                </div>
                                                <div style={{ flex: '1 1 100%' }}>
                                                    <label className="label" htmlFor="activeFlag">Active Row Flag</label>
                                                    <input
                                                        id="activeFlag"
                                                        type="text"
                                                        className="input"
                                                        value={activeFlagColumn}
                                                        onChange={(e: React.ChangeEvent<HTMLInputElement>) => setActiveFlagColumn(e.target.value)}
                                                        placeholder="DWCurrentRowFlag"
                                                    />
                                                </div>
                                            </div>
                                        </div>
                                    )}

                                    {/* Custom Business Rules (Direct Mode) */}
                                    <div style={{ marginBottom: '1.75rem' }}>
                                        <label className="label">
                                            <span style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                                🛠️ Custom Business Rules
                                            </span>
                                        </label>
                                        <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                                            {customTests.map((test, index) => (
                                                <div key={index} style={{
                                                    padding: '1rem',
                                                    background: 'var(--secondary)',
                                                    borderRadius: 'var(--radius)',
                                                    border: '1px solid var(--border)',
                                                    position: 'relative'
                                                }}>
                                                    <button
                                                        type="button"
                                                        onClick={() => removeCustomTest(index, false)}
                                                        style={{
                                                            position: 'absolute',
                                                            top: '0.5rem',
                                                            right: '0.5rem',
                                                            background: 'none',
                                                            border: 'none',
                                                            color: 'var(--error)',
                                                            cursor: 'pointer',
                                                            fontSize: '1.25rem'
                                                        }}
                                                    >
                                                        ×
                                                    </button>
                                                    <div style={{ display: 'flex', gap: '1rem', marginBottom: '0.75rem' }}>
                                                        <div style={{ flex: 1 }}>
                                                            <label className="label" style={{ fontSize: '0.75rem' }}>Rule Name</label>
                                                            <input
                                                                type="text"
                                                                className="input"
                                                                value={test.name}
                                                                onChange={(e: React.ChangeEvent<HTMLInputElement>) => handleCustomTestChange(index, 'name', e.target.value, false)}
                                                                placeholder="e.g., CreatedDtm NOT NULL"
                                                                style={{ marginBottom: 0 }}
                                                            />
                                                        </div>
                                                        <div style={{ width: '120px' }}>
                                                            <label className="label" style={{ fontSize: '0.75rem' }}>Severity</label>
                                                            <select
                                                                className="input"
                                                                value={test.severity}
                                                                onChange={(e: React.ChangeEvent<HTMLSelectElement>) => handleCustomTestChange(index, 'severity', e.target.value, false)}
                                                                style={{ marginBottom: 0, padding: '0.55rem' }}
                                                            >
                                                                <option value="HIGH">HIGH</option>
                                                                <option value="MEDIUM">MEDIUM</option>
                                                                <option value="LOW">LOW</option>
                                                            </select>
                                                        </div>
                                                    </div>
                                                    <div style={{ marginBottom: '0.75rem' }}>
                                                        <label className="label" style={{ fontSize: '0.75rem' }}>Description</label>
                                                        <input
                                                            type="text"
                                                            className="input"
                                                            value={test.description}
                                                            onChange={(e: React.ChangeEvent<HTMLInputElement>) => handleCustomTestChange(index, 'description', e.target.value, false)}
                                                            placeholder="Describe the purpose of this rule..."
                                                            style={{ marginBottom: 0 }}
                                                        />
                                                    </div>
                                                    <div>
                                                        <label className="label" style={{ fontSize: '0.75rem' }}>SQL Query (Use {'{{target}}'} for table name)</label>
                                                        <textarea
                                                            className="input"
                                                            value={test.sql}
                                                            onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => handleCustomTestChange(index, 'sql', e.target.value, false)}
                                                            placeholder="SELECT COUNT(0) = 0 FROM {{target}} WHERE CreatedDtm IS NULL"
                                                            rows={3}
                                                            style={{ marginBottom: 0, resize: 'vertical', fontFamily: 'monospace' }}
                                                        />
                                                    </div>
                                                </div>
                                            ))}
                                            <button
                                                type="button"
                                                onClick={() => addCustomTest(false)}
                                                style={{
                                                    padding: '0.5rem',
                                                    background: 'none',
                                                    border: '2px dashed var(--primary)',
                                                    color: 'var(--primary)',
                                                    borderRadius: 'var(--radius)',
                                                    cursor: 'pointer',
                                                    fontWeight: '600',
                                                    fontSize: '0.875rem'
                                                }}
                                            >
                                                + Add Business Rule
                                            </button>
                                        </div>
                                    </div>
                                </>
                            )}
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
