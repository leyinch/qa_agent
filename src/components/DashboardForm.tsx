"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";

type ComparisonMode = 'schema' | 'gcs';
type FileFormat = 'csv' | 'json' | 'parquet' | 'avro';

export default function DashboardForm() {
    const router = useRouter();

    // Common fields
    const [projectId, setProjectId] = useState("");
    const [loading, setLoading] = useState(false);
    const [comparisonMode, setComparisonMode] = useState<ComparisonMode>('schema');

    // Schema comparison mode fields
    const [datasets, setDatasets] = useState<string[]>([""]);
    const [erdDescription, setErdDescription] = useState("");

    // GCS comparison mode fields
    const [gcsBucket, setGcsBucket] = useState("");
    const [gcsFilePath, setGcsFilePath] = useState("");
    const [fileFormat, setFileFormat] = useState<FileFormat>('csv');
    const [targetDataset, setTargetDataset] = useState("");
    const [targetTable, setTargetTable] = useState("");

    const handleDatasetChange = (index: number, value: string) => {
        const newDatasets = [...datasets];
        newDatasets[index] = value;
        setDatasets(newDatasets);
    };

    const addDataset = () => {
        setDatasets([...datasets, ""]);
    };

    const removeDataset = (index: number) => {
        if (datasets.length > 1) {
            const newDatasets = datasets.filter((_, i) => i !== index);
            setDatasets(newDatasets);
        }
    };

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();

        if (comparisonMode === 'schema') {
            const validDatasets = datasets.filter(d => d.trim() !== "");

            if (validDatasets.length === 0) {
                alert("Please enter at least one dataset");
                return;
            }

            setLoading(true);

            try {
                const res = await fetch("/api/generate-tests", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify({
                        projectId,
                        datasets: validDatasets,
                        erdDescription,
                        comparisonMode: 'schema'
                    }),
                });

                if (res.ok) {
                    const data = await res.json();
                    localStorage.setItem("testResults", JSON.stringify(data));
                    router.push("/results");
                } else {
                    const errorData = await res.json();
                    alert(`Failed to generate tests: ${errorData.error || 'Unknown error'}`);
                }
            } catch (error) {
                console.error(error);
                alert("An error occurred");
            } finally {
                setLoading(false);
            }
        } else {
            // GCS comparison mode
            if (!gcsBucket || !gcsFilePath || !targetDataset || !targetTable) {
                alert("Please fill in all GCS comparison fields");
                return;
            }

            setLoading(true);

            try {
                const res = await fetch("/api/generate-tests", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify({
                        projectId,
                        comparisonMode: 'gcs',
                        gcsBucket,
                        gcsFilePath,
                        fileFormat,
                        targetDataset,
                        targetTable,
                        erdDescription
                    }),
                });

                if (res.ok) {
                    const data = await res.json();
                    localStorage.setItem("testResults", JSON.stringify(data));
                    router.push("/results");
                } else {
                    const errorData = await res.json();
                    alert(`Failed to generate tests: ${errorData.error || 'Unknown error'}`);
                }
            } catch (error) {
                console.error(error);
                alert("An error occurred");
            } finally {
                setLoading(false);
            }
        }
    };

    return (
        <form onSubmit={handleSubmit} className="card fade-in" style={{ maxWidth: '700px', margin: '0 auto' }}>
            {/* Form Header */}
            <div style={{ marginBottom: '2rem', textAlign: 'center' }}>
                <h2 style={{ fontSize: '1.75rem', fontWeight: '700', marginBottom: '0.5rem' }}>
                    Configure Test Generation
                </h2>
                <p style={{ color: 'var(--secondary-foreground)', fontSize: '0.9375rem' }}>
                    Set up your data quality tests with AI-powered validation
                </p>
            </div>

            {/* Comparison Mode Toggle */}
            <div style={{ marginBottom: '2rem' }}>
                <label className="label">Comparison Mode</label>
                <div style={{ display: 'flex', gap: '1rem', marginTop: '0.5rem' }}>
                    <button
                        type="button"
                        onClick={() => setComparisonMode('schema')}
                        style={{
                            flex: 1,
                            padding: '1rem',
                            background: comparisonMode === 'schema' ? 'var(--gradient-primary)' : 'var(--secondary)',
                            color: comparisonMode === 'schema' ? 'white' : 'var(--foreground)',
                            border: comparisonMode === 'schema' ? 'none' : '2px solid var(--border)',
                            borderRadius: 'var(--radius)',
                            cursor: 'pointer',
                            fontWeight: '600',
                            transition: 'all 0.2s ease'
                        }}
                    >
                        📊 Schema Validation
                    </button>
                    <button
                        type="button"
                        onClick={() => setComparisonMode('gcs')}
                        style={{
                            flex: 1,
                            padding: '1rem',
                            background: comparisonMode === 'gcs' ? 'var(--gradient-primary)' : 'var(--secondary)',
                            color: comparisonMode === 'gcs' ? 'white' : 'var(--foreground)',
                            border: comparisonMode === 'gcs' ? 'none' : '2px solid var(--border)',
                            borderRadius: 'var(--radius)',
                            cursor: 'pointer',
                            fontWeight: '600',
                            transition: 'all 0.2s ease'
                        }}
                    >
                        📁 GCS File Comparison
                    </button>
                </div>
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
                />
            </div>

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

            {/* GCS Comparison Mode Fields */}
            {comparisonMode === 'gcs' && (
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
                            onChange={(e) => setGcsBucket(e.target.value)}
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
                            onChange={(e) => setGcsFilePath(e.target.value)}
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
                            onChange={(e) => setFileFormat(e.target.value as FileFormat)}
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
                            onChange={(e) => setTargetDataset(e.target.value)}
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
                            onChange={(e) => setTargetTable(e.target.value)}
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
                            onChange={(e) => setErdDescription(e.target.value)}
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
        </form>
    );
}
