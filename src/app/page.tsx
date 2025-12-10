import LoginButton from "@/components/LoginButton";

export default function Home() {
    return (
        <main style={{
            minHeight: '100vh',
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            padding: '2rem',
            position: 'relative',
            overflow: 'hidden'
        }}>
            {/* Animated background gradient */}
            <div style={{
                position: 'absolute',
                top: '-50%',
                left: '-50%',
                width: '200%',
                height: '200%',
                background: 'radial-gradient(circle at 50% 50%, rgba(99, 102, 241, 0.15) 0%, transparent 50%)',
                animation: 'gradient-shift 15s ease infinite',
                pointerEvents: 'none'
            }} />

            <div style={{
                maxWidth: '800px',
                textAlign: 'center',
                position: 'relative',
                zIndex: 1
            }} className="fade-in">
                {/* Main heading with gradient */}
                <h1 style={{
                    fontSize: 'clamp(2.5rem, 8vw, 4.5rem)',
                    fontWeight: '800',
                    marginBottom: '1.5rem',
                    lineHeight: '1.1',
                    letterSpacing: '-0.02em'
                }} className="gradient-text">
                    Data QA Agent
                </h1>

                {/* Subtitle */}
                <p style={{
                    fontSize: 'clamp(1.125rem, 3vw, 1.5rem)',
                    color: 'var(--secondary-foreground)',
                    marginBottom: '3rem',
                    lineHeight: '1.6',
                    fontWeight: '400'
                }}>
                    AI-powered data quality testing across BigQuery datasets.
                    <br />
                    <span style={{ fontSize: '1rem', opacity: 0.8 }}>
                        Verify schemas, generate test cases, and ensure data integrity automatically.
                    </span>
                </p>

                {/* Feature highlights */}
                <div style={{
                    display: 'grid',
                    gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
                    gap: '1.5rem',
                    marginBottom: '3rem'
                }}>
                    <div className="card" style={{ padding: '1.5rem', textAlign: 'center' }}>
                        <div style={{ fontSize: '2rem', marginBottom: '0.5rem' }}>🤖</div>
                        <h3 style={{ fontSize: '1rem', fontWeight: '600', marginBottom: '0.5rem' }}>AI-Powered</h3>
                        <p style={{ fontSize: '0.875rem', color: 'var(--secondary-foreground)' }}>
                            Gemini analyzes your schemas
                        </p>
                    </div>

                    <div className="card" style={{ padding: '1.5rem', textAlign: 'center' }}>
                        <div style={{ fontSize: '2rem', marginBottom: '0.5rem' }}>⚡</div>
                        <h3 style={{ fontSize: '1rem', fontWeight: '600', marginBottom: '0.5rem' }}>Automated</h3>
                        <p style={{ fontSize: '0.875rem', color: 'var(--secondary-foreground)' }}>
                            Generate & run tests instantly
                        </p>
                    </div>

                    <div className="card" style={{ padding: '1.5rem', textAlign: 'center' }}>
                        <div style={{ fontSize: '2rem', marginBottom: '0.5rem' }}>📊</div>
                        <h3 style={{ fontSize: '1rem', fontWeight: '600', marginBottom: '0.5rem' }}>Multi-Dataset</h3>
                        <p style={{ fontSize: '0.875rem', color: 'var(--secondary-foreground)' }}>
                            Test across multiple datasets
                        </p>
                    </div>
                </div>

                {/* CTA Button */}
                <LoginButton />

                {/* Additional info */}
                <p style={{
                    marginTop: '2rem',
                    fontSize: '0.875rem',
                    color: 'var(--secondary-foreground)',
                    opacity: 0.7
                }}>
                    Powered by Google Cloud Vertex AI & BigQuery
                </p>
            </div>
        </main>
    );
}
