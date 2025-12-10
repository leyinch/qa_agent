import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
    title: "Data QA Agent",
    description: "AI-powered data quality testing across BigQuery datasets",
};

export default function RootLayout({
    children,
}: Readonly<{
    children: React.ReactNode;
}>) {
    return (
        <html lang="en">
            <body>{children}</body>
        </html>
    );
}
