"use client";
// UI-only auth form. Structured for Supabase Auth: swap the stubs in
// onSubmit / onMagicLink / onOAuth for supabase.auth.* calls (see src/lib/supabase/README.md).
import Link from "next/link";
import { useState } from "react";

type Mode = "login" | "signup";

export function AuthForm({ mode }: { mode: Mode }) {
  const [status, setStatus] = useState<string | null>(null);
  const isSignup = mode === "signup";

  function onSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    // TODO(supabase): isSignup ? supabase.auth.signUp({ email, password, options: { data: { display_name } } })
    //                          : supabase.auth.signInWithPassword({ email, password })
    setStatus("Mockup only: auth isn't connected yet.");
  }
  function onMagicLink() {
    // TODO(supabase): supabase.auth.signInWithOtp({ email })
    setStatus("Mockup only: a magic link would be emailed here.");
  }
  function onOAuth(provider: "google" | "apple") {
    // TODO(supabase): supabase.auth.signInWithOAuth({ provider })
    setStatus(`Mockup only: would continue with ${provider}.`);
  }

  return (
    <div className="card w-full max-w-md p-5 md:p-8 shadow-2xl shadow-black/40">
      <h1 className="text-2xl font-bold">{isSignup ? "Create your account" : "Welcome back"}</h1>
      <p className="mt-1 text-sm text-zinc-400">
        {isSignup ? "Free: daily slate, public ledger and 1 custom model." : "Log in to see your slate, models and alerts."}
      </p>

      <div className="mt-6 grid grid-cols-2 gap-3">
        <button type="button" onClick={() => onOAuth("google")} className="rounded-lg border border-ink-600 bg-ink-850 min-h-12 text-sm font-medium hover:bg-ink-800">
          Google
        </button>
        <button type="button" onClick={() => onOAuth("apple")} className="rounded-lg border border-ink-600 bg-ink-850 min-h-12 text-sm font-medium hover:bg-ink-800">
          Apple
        </button>
      </div>
      <div className="my-6 flex items-center gap-3 text-xs text-zinc-500">
        <div className="h-px flex-1 bg-ink-700" /> or with email <div className="h-px flex-1 bg-ink-700" />
      </div>

      <form onSubmit={onSubmit} className="space-y-4">
        {isSignup && (
          <Field label="Display name" name="display_name" type="text" placeholder="hoopsguy" autoComplete="nickname" />
        )}
        <Field label="Email" name="email" type="email" placeholder="you@example.com" autoComplete="email" />
        <Field
          label="Password"
          name="password"
          type="password"
          placeholder="••••••••"
          autoComplete={isSignup ? "new-password" : "current-password"}
        />
        {isSignup ? (
          <label className="flex items-start gap-2 text-xs text-zinc-400">
            <input type="checkbox" required className="mt-0.5 accent-orange-500" />
            <span>
              I&apos;m 21+ and agree to the{" "}
              <Link href="/terms" className="font-semibold text-court-400 hover:underline">Terms</Link>
              {" "}and{" "}
              <Link href="/privacy" className="font-semibold text-court-400 hover:underline">Privacy Policy</Link>.
              Projections are informational, not betting advice.
            </span>
          </label>
        ) : (
          <div className="flex flex-wrap items-center justify-between gap-2 text-xs">
            <label className="flex items-center gap-2 text-zinc-400">
              <input type="checkbox" className="accent-orange-500" /> Remember me
            </label>
            <button type="button" onClick={onMagicLink} className="min-h-10 text-court-400 hover:underline">
              Email me a magic link
            </button>
          </div>
        )}
        <button type="submit" className="min-h-12 w-full rounded-lg bg-court-500 font-semibold text-ink-950 hover:bg-court-400">
          {isSignup ? "Create free account" : "Log in"}
        </button>
      </form>

      {status && <p className="mt-4 rounded-lg bg-ink-800 px-3 py-2 text-center text-xs text-amber-300">{status}</p>}

      <p className="mt-6 text-center text-sm text-zinc-400">
        {isSignup ? "Already have an account? " : "New to CZarp? "}
        <Link href={isSignup ? "/login" : "/signup"} className="font-semibold text-court-400 hover:underline">
          {isSignup ? "Log in" : "Sign up free"}
        </Link>
      </p>
    </div>
  );
}

function Field(props: { label: string; name: string; type: string; placeholder: string; autoComplete: string }) {
  return (
    <label className="block">
      <span className="text-xs font-medium text-zinc-300">{props.label}</span>
      <input
        name={props.name}
        type={props.type}
        placeholder={props.placeholder}
        autoComplete={props.autoComplete}
        required
        className="mt-1.5 min-h-12 w-full rounded-lg border border-ink-600 bg-ink-950 px-3 text-base outline-none placeholder:text-zinc-600 focus:border-court-500 focus:ring-2 focus:ring-court-500/30"
      />
    </label>
  );
}
