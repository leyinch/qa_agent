import { NextRequest, NextResponse } from 'next/server';

/**
 * Dynamic Proxy Route
 * Proxies requests from /api/python/* to BACKEND_URL/api/*
 * This ensures runtime environment variables are honored in Cloud Run.
 */

const getBackendUrl = () => {
    // Priority: 
    // 1. BACKEND_URL (set via --set-env-vars)
    // 2. Default to localhost for local development
    const url = process.env.BACKEND_URL || 'http://localhost:8000';
    // Remove trailing slash if present
    return url.endsWith('/') ? url.slice(0, -1) : url;
};

async function proxyRequest(req: NextRequest, { params }: { params: { path: string[] } }) {
    const backendUrl = getBackendUrl();
    const path = params.path.join('/');
    const searchParams = req.nextUrl.search;

    const targetUrl = `${backendUrl}/api/${path}${searchParams}`;

    console.log(`Proxying ${req.method} ${req.nextUrl.pathname} -> ${targetUrl}`);

    try {
        const body = req.method !== 'GET' && req.method !== 'HEAD'
            ? await req.text()
            : undefined;

        const response = await fetch(targetUrl, {
            method: req.method,
            headers: {
                'Content-Type': 'application/json',
                // Explicitly pass through headers if needed, but fetch handles most
            },
            body: body,
            // Next.js specific fetch options to ensure no caching for API proxy
            cache: 'no-store'
        });

        const data = await response.text();

        let jsonData;
        try {
            jsonData = JSON.parse(data);
        } catch (e) {
            // Not JSON, return as text
            return new NextResponse(data, {
                status: response.status,
                headers: { 'Content-Type': 'text/plain' }
            });
        }

        return NextResponse.json(jsonData, { status: response.status });

    } catch (error: any) {
        console.error(`Proxy error for ${targetUrl}:`, error);
        return NextResponse.json(
            { detail: `Proxy Error: ${error.message}` },
            { status: 502 }
        );
    }
}

export async function GET(req: NextRequest, context: any) {
    return proxyRequest(req, context);
}

export async function POST(req: NextRequest, context: any) {
    return proxyRequest(req, context);
}

export async function PUT(req: NextRequest, context: any) {
    return proxyRequest(req, context);
}

export async function DELETE(req: NextRequest, context: any) {
    return proxyRequest(req, context);
}
