import { NextResponse } from "next/server";
import { BigQuery } from "@google-cloud/bigquery";
import { Storage } from "@google-cloud/storage";
import { VertexAI } from "@google-cloud/vertexai";
import Papa from "papaparse";

// Initialize clients
const bigquery = new BigQuery();
const storage = new Storage();

// Helper function to count rows in CSV file
async function countCSVRows(bucket: string, filePath: string): Promise<number> {
    const file = storage.bucket(bucket).file(filePath);
    const [exists] = await file.exists();

    if (!exists) {
        throw new Error(`File not found: gs://${bucket}/${filePath}`);
    }

    return new Promise((resolve, reject) => {
        let rowCount = 0;
        const stream = file.createReadStream();

        Papa.parse(stream, {
            header: true,
            step: () => {
                rowCount++;
            },
            complete: () => {
                resolve(rowCount);
            },
            error: (error) => {
                reject(error);
            }
        });
    });
}

// Helper function to sample CSV data
async function sampleCSVData(bucket: string, filePath: string, limit: number = 100): Promise<any[]> {
    const file = storage.bucket(bucket).file(filePath);
    const [exists] = await file.exists();

    if (!exists) {
        throw new Error(`File not found: gs://${bucket}/${filePath}`);
    }

    return new Promise((resolve, reject) => {
        const rows: any[] = [];
        const stream = file.createReadStream();

        Papa.parse(stream, {
            header: true,
            step: (result) => {
                if (rows.length < limit) {
                    rows.push(result.data);
                }
            },
            complete: () => {
                resolve(rows);
            },
            error: (error) => {
                reject(error);
            }
        });
    });
}

// Helper function to get CSV headers
async function getCSVHeaders(bucket: string, filePath: string): Promise<string[]> {
    const file = storage.bucket(bucket).file(filePath);
    const [exists] = await file.exists();

    if (!exists) {
        throw new Error(`File not found: gs://${bucket}/${filePath}`);
    }

    return new Promise((resolve, reject) => {
        const stream = file.createReadStream();
        let headers: string[] = [];

        Papa.parse(stream, {
            header: true,
            preview: 1,
            complete: (results) => {
                headers = results.meta.fields || [];
                resolve(headers);
            },
            error: (error) => {
                reject(error);
            }
        });
    });
}

export async function POST(req: Request) {
    try {
        const body = await req.json();
        const { projectId, comparisonMode = 'schema' } = body;

        if (!projectId) {
            return NextResponse.json(
                { error: "Missing required field: projectId" },
                { status: 400 }
            );
        }

        // Handle GCS comparison mode
        if (comparisonMode === 'gcs') {
            const { gcsBucket, gcsFilePath, fileFormat, targetDataset, targetTable, erdDescription } = body;

            if (!gcsBucket || !gcsFilePath || !targetDataset || !targetTable) {
                return NextResponse.json(
                    { error: "Missing required fields for GCS comparison" },
                    { status: 400 }
                );
            }

            // Currently only support CSV
            if (fileFormat !== 'csv') {
                return NextResponse.json(
                    { error: "Currently only CSV format is supported" },
                    { status: 400 }
                );
            }

            try {
                // 1. Get GCS file information
                console.log(`Reading GCS file: gs://${gcsBucket}/${gcsFilePath}`);
                const fileRowCount = await countCSVRows(gcsBucket, gcsFilePath);
                const gcsHeaders = await getCSVHeaders(gcsBucket, gcsFilePath);
                const gcsSample = await sampleCSVData(gcsBucket, gcsFilePath, 5);

                // 2. Get BigQuery table information
                const fullTableName = `${projectId}.${targetDataset}.${targetTable}`;
                console.log(`Querying BigQuery table: ${fullTableName}`);

                const [countResult] = await bigquery.query(`
                    SELECT COUNT(*) as count FROM \`${fullTableName}\`
                `);
                const bqRowCount = parseInt(countResult[0].count);

                const [sampleResult] = await bigquery.query(`
                    SELECT * FROM \`${fullTableName}\` LIMIT 5
                `);

                const table = bigquery.dataset(targetDataset, { projectId }).table(targetTable);
                const [metadata] = await table.getMetadata();
                const bqSchema = metadata.schema;

                // 3. Initialize Vertex AI
                const vertex_ai = new VertexAI({ project: projectId, location: "us-central1" });
                const model = vertex_ai.preview.getGenerativeModel({
                    model: "gemini-2.5-flash",
                });

                // 4. Construct comparison prompt
                const prompt = `
You are a Data Quality Assurance Agent comparing a GCS file to a BigQuery table.

**Source File (GCS):**
- Location: gs://${gcsBucket}/${gcsFilePath}
- Format: ${fileFormat}
- Row Count: ${fileRowCount}
- Columns: ${gcsHeaders.join(', ')}
- Sample Data (first 5 rows): ${JSON.stringify(gcsSample, null, 2)}

**Target Table (BigQuery):**
- Table: ${fullTableName}
- Row Count: ${bqRowCount}
- Schema: ${JSON.stringify(bqSchema.fields, null, 2)}
- Sample Data (first 5 rows): ${JSON.stringify(sampleResult, null, 2)}

${erdDescription ? `**Expected Schema/Description:**\n${erdDescription}\n` : ''}

**Instructions:**
Generate SQL test cases to validate the data load from GCS to BigQuery. Focus on:

1. **Row Count Validation**: Check if row counts match
2. **Schema Validation**: Verify all expected columns exist with correct types
3. **Data Completeness**: Check for null values in mandatory fields
4. **Data Quality**: Validate data ranges, formats, and constraints
5. **Duplicate Detection**: Check for duplicate records

For each test, return a SQL query that returns rows that FAIL the test. If the query returns 0 rows, the test passes.

**CRITICAL**: All table references must use the full table name: \`${fullTableName}\`

Return output as a JSON array with this structure:
[
  {
    "test_name": "string",
    "description": "string",
    "sql_query": "string",
    "severity": "HIGH" | "MEDIUM" | "LOW"
  }
]

Also include a special test for row count comparison (this doesn't need a SQL query):
{
  "test_name": "Row Count Match",
  "description": "GCS file: ${fileRowCount} rows, BigQuery: ${bqRowCount} rows",
  "sql_query": "",
  "severity": "HIGH"
}

Just return the JSON array. No markdown formatting.
`;

                // 5. Generate tests with AI
                const result = await model.generateContent(prompt);
                const response = result.response;
                let testCases = [];

                try {
                    const text = response.candidates?.[0]?.content?.parts?.[0]?.text || "";
                    const cleanedText = text.replace(/```json\n?/g, "").replace(/```\n?/g, "").trim();
                    testCases = JSON.parse(cleanedText);
                } catch (parseError) {
                    console.error("Failed to parse AI response:", parseError);
                    return NextResponse.json(
                        { error: "Failed to parse AI-generated test cases" },
                        { status: 500 }
                    );
                }

                // 6. Execute test cases
                const results = [];

                // Add row count comparison test
                results.push({
                    test_name: "Row Count Match",
                    description: `GCS file has ${fileRowCount} rows, BigQuery has ${bqRowCount} rows`,
                    status: fileRowCount === bqRowCount ? "PASS" : "FAIL",
                    severity: "HIGH",
                    sql_query: "",
                    rows_affected: Math.abs(fileRowCount - bqRowCount),
                    error_message: fileRowCount !== bqRowCount ? `Row count mismatch: ${Math.abs(fileRowCount - bqRowCount)} rows difference` : null
                });

                // Execute SQL test cases
                for (const test of testCases) {
                    if (!test.sql_query || test.sql_query.trim() === "") {
                        continue; // Skip tests without SQL
                    }

                    try {
                        const [rows] = await bigquery.query(test.sql_query);
                        const rowCount = rows.length;

                        results.push({
                            test_name: test.test_name,
                            description: test.description,
                            status: rowCount === 0 ? "PASS" : "FAIL",
                            severity: test.severity,
                            sql_query: test.sql_query,
                            rows_affected: rowCount,
                            error_message: null
                        });
                    } catch (error: any) {
                        results.push({
                            test_name: test.test_name,
                            description: test.description,
                            status: "ERROR",
                            severity: test.severity,
                            sql_query: test.sql_query,
                            rows_affected: 0,
                            error_message: error.message
                        });
                    }
                }

                return NextResponse.json({ results });

            } catch (error: any) {
                console.error("GCS comparison error:", error);
                return NextResponse.json(
                    { error: `GCS comparison failed: ${error.message}` },
                    { status: 500 }
                );
            }
        }

        // Handle schema comparison mode (original functionality)
        const { datasets, erdDescription } = body;

        if (!datasets || !Array.isArray(datasets) || datasets.length === 0 || !erdDescription) {
            return NextResponse.json(
                { error: "Missing required fields. Provide projectId, datasets (array), and erdDescription." },
                { status: 400 }
            );
        }

        // 1. Fetch Table Schemas from all BigQuery Datasets
        const allSchemas: any[] = [];

        for (const datasetId of datasets) {
            try {
                const [tables] = await bigquery.dataset(datasetId, { projectId }).getTables();
                const schemas = await Promise.all(
                    tables.map(async (table) => {
                        const [metadata] = await table.getMetadata();
                        return {
                            datasetId: datasetId,
                            tableId: table.id,
                            fullTableName: `${projectId}.${datasetId}.${table.id}`,
                            schema: metadata.schema,
                        };
                    })
                );
                allSchemas.push(...schemas);
            } catch (error: any) {
                console.error(`Error fetching tables from dataset ${datasetId}:`, error);
                return NextResponse.json(
                    { error: `Failed to fetch tables from dataset '${datasetId}': ${error.message}` },
                    { status: 500 }
                );
            }
        }

        if (allSchemas.length === 0) {
            return NextResponse.json(
                { error: "No tables found in the specified datasets" },
                { status: 404 }
            );
        }

        // 2. Initialize Vertex AI
        const vertex_ai = new VertexAI({ project: projectId, location: "us-central1" });
        const model = vertex_ai.preview.getGenerativeModel({
            model: "gemini-2.5-flash",
        });

        // 3. Construct Prompt
        const datasetList = datasets.join(', ');
        const tableList = allSchemas.map(s => `  - ${s.fullTableName}`).join('\n');

        const prompt = `
      You are a Data Quality Assurance Agent.
      
      Your task is to generate BigQuery SQL test cases to verify data quality based on an ER Diagram and the actual BigQuery Schema.
      
      **Project ID:** ${projectId}
      **Datasets:** ${datasetList}
      
      **ER Diagram Description:**
      ${erdDescription}
      
      **Available Tables:**
${tableList}
      
      **Actual BigQuery Schemas:**
      ${JSON.stringify(allSchemas, null, 2)}
      
      **Instructions:**
      1. Verify if the actual schema matches the ERD (column names, types).
      2. Generate SQL queries to check for:
         - Null values in mandatory fields.
         - Referential integrity (foreign keys) if implied by ERD, even across datasets.
         - Duplicate records (primary keys).
         - Invalid values (e.g., negative prices).
      3. CRITICAL: All table references in SQL queries MUST use the FULL table name from the "Available Tables" list above.
         Example: SELECT * FROM \`${projectId}.${datasets[0]}.customers\` WHERE email IS NULL
         For cross-dataset joins, use appropriate full table names for each table.
      4. Return the output as a JSON array of test cases. Each test case should have:
         - "test_name": string
         - "description": string
         - "sql_query": string (The SQL query should return rows that FAIL the test. If count > 0, test fails.)
         - "severity": "HIGH" | "MEDIUM" | "LOW"
      
      **Output Format:**
      Just return the JSON array. No markdown formatting.
    `;

        // 4. Generate Test Cases with AI
        const result = await model.generateContent(prompt);
        const response = result.response;
        let testCases = [];

        try {
            const text = response.candidates?.[0]?.content?.parts?.[0]?.text || "";
            const cleanedText = text.replace(/```json\n?/g, "").replace(/```\n?/g, "").trim();
            testCases = JSON.parse(cleanedText);
        } catch (parseError) {
            console.error("Failed to parse AI response:", parseError);
            return NextResponse.json(
                { error: "Failed to parse AI-generated test cases" },
                { status: 500 }
            );
        }

        // 5. Execute Test Cases
        const results = [];
        for (const test of testCases) {
            try {
                const [rows] = await bigquery.query(test.sql_query);
                const rowCount = rows.length;

                results.push({
                    test_name: test.test_name,
                    description: test.description,
                    status: rowCount === 0 ? "PASS" : "FAIL",
                    severity: test.severity,
                    sql_query: test.sql_query,
                    rows_affected: rowCount,
                    error_message: null
                });
            } catch (error: any) {
                results.push({
                    test_name: test.test_name,
                    description: test.description,
                    status: "ERROR",
                    severity: test.severity,
                    sql_query: test.sql_query,
                    rows_affected: 0,
                    error_message: error.message
                });
            }
        }

        return NextResponse.json({ results });

    } catch (error: any) {
        console.error("Error in generate-tests API:", error);
        return NextResponse.json(
            { error: error.message || "Internal server error" },
            { status: 500 }
        );
    }
}
