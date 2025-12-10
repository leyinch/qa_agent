import DashboardForm from "@/components/DashboardForm";

export default function Dashboard() {
    return (
        <div className="container">
            <header className="header" style={{ marginBottom: '2rem', borderRadius: 'var(--radius)', border: '1px solid var(--border)' }}>
                <div className="logo">Data QA Agent</div>
                <div>
                    {/* User info could go here */}
                </div>
            </header>

            <main>
                <h2 style={{ fontSize: '2rem', fontWeight: 'bold', marginBottom: '1rem', textAlign: 'center' }}>
                    Configure Test Generation
                </h2>
                <p style={{ textAlign: 'center', marginBottom: '2rem', color: 'var(--secondary-foreground)' }}>
                    Upload a mermaid diagram image or provide the details of your BigQuery dataset and the expected schema (ERD).
                </p>
                <DashboardForm />
            </main>
        </div>
    );
}
