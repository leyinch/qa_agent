import { NextResponse } from "next/server";

export async function POST(req: Request) {
    try {
        const body = await req.json();
        const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:8000';

        console.log(`Proxying request to: ${backendUrl}/api/generate-tests`);

        const res = await fetch(`${backendUrl}/api/generate-tests`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify(body),
        });

        if (!res.ok) {
            const errorText = await res.text();
            throw new Error(`Backend error (${res.status}): ${errorText}`);
        }

        const data = await res.json();
        return NextResponse.json(data);

    } catch (error: any) {
        console.error("Proxy error:", error);
        return NextResponse.json(
            { error: error.message || "An error occurred while connecting to the backend" },
            { status: 500 }
        );
    }
}
