
import { NextResponse } from 'next/server';

export async function GET() {
    return NextResponse.json({
        backendurl: process.env.BACKEND_URL || 'NOT_SET',
        next_public_backendurl: process.env.NEXT_PUBLIC_BACKEND_URL || 'NOT_SET',
        project: process.env.GOOGLE_CLOUD_PROJECT || 'NOT_SET',
        node_env: process.env.NODE_ENV
    });
}
