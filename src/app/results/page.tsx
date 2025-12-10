import ResultsView from "@/components/ResultsView";
import Link from "next/link";

export default function ResultsPage() {
    return (
        <div className="container">
            <header className="header" style={{ marginBottom: '2rem', borderRadius: 'var(--radius)', border: '1px solid var(--border)' }}>
                <div className="logo">Test Results</div>
                <Link href="/dashboard" className="btn btn-primary">
                    New Test
                </Link>
            </header>

            <main>
                <ResultsView />
            </main>
        </div>
    );
}
