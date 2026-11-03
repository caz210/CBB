# Supabase wiring (not connected in the mockup)

Source of truth for the database: **`/workspace/CBB/db/schema.sql`** (CZarp repo, branch `czarp-v2`).
The front end only reads the views below plus `profiles` / `user_models`; the Python pipeline
writes everything else with the service role. Types mirroring these rows live in
`src/lib/example-data.ts`; enum → label mapping lives in `src/lib/labels.ts`.

| View / table   | Used by                     | Notes |
|----------------|-----------------------------|-------|
| `slate_games`  | `/[sport]/slate`, landing   | numeric `game_id`, `tipoff` timestamptz, `game` = 'Away @ Home', `proj_spread` in home-line notation, `archetypes` jsonb `{home, away, home_tentative, away_tentative, tentative, notes}`, `market_source` lowercase |
| `value_bets`   | slate value cards / sheet   | `market` ∈ spread/total/moneyline, `prob_edge` (model − market prob), `line_edge_pct` (0.08 = 8%), `kelly_quarter_units` (1u = 1% bankroll). RLS hides rows until tip-off unless `is_pro()` |
| `ledger`       | `/[sport]/results`          | `result` 'W'/'L'/'P' or null (pending), `stake_units`, `profit_units`, `is_paper` |
| `pick_record`  | results KPIs + by-market    | wins/losses/pushes/pending/staked/pnl/avg_clv per market |
| `profiles`     | tier gating                 | `tier` ∈ free/pro/syndicate; written server-side only |
| `user_models`  | `/models`                   | per-user CRUD under RLS; `features` same shape as `model_configs.features` |

Archetype names (exact strings from `czarp/teamstats.py`): Addicted to 3s, Arc Wall, Board Crashers,
Careless, Don't Blink, Free Throw Merchants, Glass Leak, Lock Down, No Rebounds for You, Open Arc,
Passive D, Sieve, Turnover Machine. The UI styles weaknesses (Careless, Glass Leak, Open Arc, Passive D, Sieve)
differently and renders `*_tentative` teams' tags with a dashed "tent." chip.

## To go live
1. `npm i @supabase/supabase-js @supabase/ssr`
2. `.env.local`: `NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_ANON_KEY` (anon key only, never the
   service key or the KenPom/Odds keys).
3. Implement `client.ts` / `server.ts` with `createBrowserClient` / `createServerClient`.
4. Replace the `TODO(supabase)` stubs in `AuthForm.tsx` and the page loaders.
5. `middleware.ts` refreshes the session; read `profiles.tier` server-side for gating.

## Saved-model limit (Free = 1, Pro/Syndicate = unlimited)
The UI checks `MODEL_LIMIT` (`src/lib/example-data.ts`), but the database must enforce it too.
Proposed addition to `schema.sql` (not applied anywhere yet):

```sql
create or replace function public.enforce_model_limit() returns trigger
language plpgsql security definer set search_path = public as $$
begin
  if not public.is_pro()
     and (select count(*) from public.user_models where user_id = new.user_id) >= 1 then
    raise exception 'Free plan allows 1 saved model. Upgrade to Pro for unlimited.'
      using errcode = 'P0001';
  end if;
  return new;
end $$;
drop trigger if exists user_models_limit on user_models;
create trigger user_models_limit before insert on user_models
  for each row execute function public.enforce_model_limit();
```
On downgrade, existing extra models stay readable but further inserts are blocked; the UI can let
the user pick which one stays "active".
