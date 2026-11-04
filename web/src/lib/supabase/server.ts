import { cookies } from "next/headers";
import { createServerClient } from "@supabase/ssr";
import { SUPABASE_ANON_KEY, SUPABASE_URL, supabaseConfigured } from "./env";

/**
 * Server-side Supabase client for Server Components / Route Handlers.
 * Uses the anon key plus the visitor's auth cookies, so RLS applies per user
 * (all picks are public). Returns null when env vars are missing.
 */
export async function createSupabaseServerClient() {
  if (!supabaseConfigured) return null;
  const cookieStore = await cookies();
  return createServerClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
    cookies: {
      getAll() {
        return cookieStore.getAll();
      },
      setAll(toSet) {
        try {
          toSet.forEach(({ name, value, options }) => cookieStore.set(name, value, options));
        } catch {
          // Called from a Server Component: cookies are read-only there. Session refresh
          // belongs in middleware once auth is wired; safe to ignore for read-only pages.
        }
      },
    },
  });
}
