export const freshnessConfig = {
    // Default threshold in minutes (1440 minutes = 24 hours)
    defaultThresholdMinutes: 1440,

    // Default unit for the API to assume if not provided
    defaultUnit: 'minutes' as const,

    // Layer configurations
    layers: [
        {
            name: 'Store',
            tablePrefix: 'Store_',
            threshold: 1440,
            unit: 'minutes' as const
        },
        {
            name: 'Dummy',
            tablePrefix: 'Dummy_',
            threshold: 60,
            unit: 'minutes' as const
        }
    ]
};
