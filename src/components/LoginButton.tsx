"use client";

import { signIn } from "next-auth/react";

export default function LoginButton() {
    return (
        <button
            className="btn btn-primary"
            onClick={() => signIn("google", { callbackUrl: "/dashboard" })}
        >
            Sign in with Google
        </button>
    );
}
