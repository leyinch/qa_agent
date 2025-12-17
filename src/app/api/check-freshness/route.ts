import { NextResponse } from "next/server";
import { BigQuery } from "@google-cloud/bigquery";
import fs from 'fs';
import path from 'path';
import yaml from 'js-yaml';

const bigquery = new BigQuery();

interface TestResult {
    test_name: string;
    description: string;
    sql_query: string;
    severity: string;
    status: "PASS" | "FAIL" | "ERROR";
    failed_rows?: number;
    error_message?: string;
}

interface FreshnessConfig {
    defaultThresholdMinutes: number;
    defaultUnit: 'minutes' | 'hours';
    layers: {
        name: string;
        tablePrefix: string;
        threshold?: number;
        unit?: 'minutes' | 'hours';
    }[];
}

const loadConfig = (): FreshnessConfig => {
    try {
        const configPath = path.join(process.cwd(), 'src', 'config', 'freshnessConfig.yaml');
        const fileContents = fs.readFileSync(configPath, 'utf8');
        return yaml.load(fileContents) as FreshnessConfig;
    } catch (e) {
        console.error("Failed to load freshness config:", e);
        // Fallback defaults
        return {
            defaultThresholdMinutes: 1440,
            defaultUnit: 'minutes',
            layers: [
                {
                    name: 'Store',
                    tablePrefix: 'Store_',
                    threshold: 1440,
                    unit: 'minutes'
                }
            ]
        };
    }
};

export async function POST(req: Request) {
    const config = loadConfig();

    try {
        const body = await req.json();
        const {
            projectId,
            dataset,
            freshnessThreshold = config.defaultThresholdMinutes,
            freshnessUnit = config.defaultUnit
        } = body;

        if (!projectId || !dataset) {
            return NextResponse.json(
                { error: "Missing required fields: projectId, dataset" },
                { status: 400 }
            );
        }

        let thresholdMinutes = parseFloat(freshnessThreshold);

        // Handle unit conversion if necessary
        if (freshnessUnit === 'hours') {
            thresholdMinutes = thresholdMinutes * 60;
        }

        console.log(`Checking freshness for ${projectId}.${dataset} with threshold ${thresholdMinutes} minutes (${freshnessThreshold} ${freshnessUnit})`);

        // 1. Get all tables in the dataset
        // We use the BigQuery API to list tables
        const [tables] = await bigquery.dataset(dataset, { projectId }).getTables();

        const results = [];
        let tablesFound = false;

        // Iterate through each configured layer
        for (const layer of config.layers) {
            const layerTables = tables.filter(table => table.id.startsWith(layer.tablePrefix));

            if (layerTables.length > 0) {
                tablesFound = true;

                // Determine threshold for this layer
                let layerThresholdMinutes = layer.threshold || config.defaultThresholdMinutes;

                // If global override is provided in request, it takes precedence (treating it as manual override)
                // Note: The variable 'thresholdMinutes' comes from request body 'freshnessThreshold'
                if (body.freshnessThreshold) {
                    layerThresholdMinutes = thresholdMinutes;
                } else if (layer.unit === 'hours') {
                    // If config layer uses hours, convert to minutes
                    layerThresholdMinutes = layerThresholdMinutes * 60;
                }

                console.log(`Checking freshness for layer '${layer.name}' (prefix: ${layer.tablePrefix}) with threshold ${layerThresholdMinutes} minutes`);

                for (const table of layerTables) {
                    const tableId = table.id;
                    const fullTableName = `\`${projectId}.${dataset}.${tableId}\``;

                    try {
                        const query = `
                            SELECT 
                                MAX(elt_load_time) as last_load_time,
                                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(elt_load_time), MINUTE) as minutes_since_load
                            FROM ${fullTableName}
                        `;

                        const [rows] = await bigquery.query({ query, location: 'US' });

                        if (rows.length === 0 || rows[0].last_load_time === null) {
                            results.push({
                                test_name: `Data Freshness - ${tableId}`,
                                description: `Check if ${tableId} has data and recent elt_load_time`,
                                status: "FAIL",
                                severity: "HIGH",
                                sql_query: query,
                                error_message: "Table is empty or elt_load_time is null"
                            });
                            continue;
                        }

                        const minutesSinceLoad = rows[0].minutes_since_load;
                        const lastLoadTime = rows[0].last_load_time.value;

                        const isFresh = minutesSinceLoad <= layerThresholdMinutes;

                        results.push({
                            test_name: `Data Freshness - ${tableId}`,
                            description: `Data should be loaded within last ${layerThresholdMinutes} minutes (Actual: ${minutesSinceLoad} minutes ago)`,
                            status: isFresh ? "PASS" : "FAIL",
                            severity: "HIGH",
                            sql_query: query,
                            rows_affected: isFresh ? 0 : 1,
                            error_message: isFresh ? null : `Data is stale. Last load: ${lastLoadTime} (${minutesSinceLoad} minutes ago)`
                        });

                    } catch (err: any) {
                        results.push({
                            test_name: `Data Freshness - ${tableId}`,
                            description: `Check freshness for ${tableId}`,
                            status: "ERROR",
                            severity: "HIGH",
                            sql_query: `SELECT MAX(elt_load_time) FROM ${fullTableName}`,
                            error_message: err.message
                        });
                    }
                }
            }
        }

        if (!tablesFound) {
            return NextResponse.json({
                results: [{
                    test_name: "Freshness Check Table Discovery",
                    description: `Check for tables matching configured prefixes in ${dataset}`,
                    status: "FAIL",
                    severity: "HIGH",
                    sql_query: "",
                    error_message: `No tables found matching any configured prefixes in dataset ${dataset}`
                }]
            });
        }

        return NextResponse.json({ results });

    } catch (error: any) {
        console.error("Error in check-freshness API:", error);
        return NextResponse.json(
            { error: error.message || "Internal server error" },
            { status: 500 }
        );
    }
}
