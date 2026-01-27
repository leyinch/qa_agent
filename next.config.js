/** @type {import('next').NextConfig} */
const nextConfig = {
    output: "standalone",
    async rewrites() {
        const backendUrl = process.env.BACKEND_URL || process.env.NEXT_PUBLIC_BACKEND_URL;
        console.log(`Configuring rewrites with Backend URL: ${backendUrl || 'NOT SET'}`);

        return [
            {
                source: "/api/:path*",
                // In Cloud Run, we should use the full URL. 
                // Fallback to localhost only if strictly necessary.
                destination: `${backendUrl || "http://127.0.0.1:8000"}/api/:path*`,
            },
        ];
    },
};

module.exports = nextConfig;
