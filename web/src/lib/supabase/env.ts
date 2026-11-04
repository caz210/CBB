// Public (anon) Supabase settings. Both are safe to ship to the browser; RLS does the gating.
// Never put the service-role key here.
export const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
export const SUPABASE_ANON_KEY = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
export const supabaseConfigured = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
