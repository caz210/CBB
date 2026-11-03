# CZarpAnalytics.com: front-end mockup

Next.js 15 (App Router) + Tailwind v4, **mobile-first** (designed at 390px, scales up to desktop).
Every number on the site is fictional **example data** (`src/lib/example-data.ts`), shaped exactly like
CZarp's Supabase views (`/workspace/CBB/db/schema.sql`, branch `czarp-v2`).

```bash
npm install
npm run dev                     # http://localhost:3000   (or: npm run build && npm start)
node scripts/screenshots.mjs    # server running → screenshots/mobile/*.png (390px) + screenshots/desktop/*.png
```

Routes: `/`, `/login`, `/signup`, `/[sport]/slate`, `/[sport]/results`, `/models`, `/pricing`.
Phone navigation is a bottom tab bar (Slate · Results · My Models · Plans); the sport switcher sits
under the header. Sports registry: `src/lib/sports.ts` (cbb live; nba/wnba coming soon).
Data wiring, view columns, and the saved-model limit trigger: `src/lib/supabase/README.md`.

## Tiers
| | Free | Pro | Syndicate (placeholder) |
|---|---|---|---|
| Slate, projections, archetypes, public ledger | ✓ | ✓ | ✓ |
| Value bets | revealed at tip-off (RLS) | before tip-off | before tip-off |
| ¼-Kelly stakes, alerts | — | ✓ | ✓ |
| Saved custom models | **1** | **unlimited** | unlimited |

`profiles.tier` ('free' | 'pro' | 'syndicate') is the single switch. It's never writable from the browser
(RLS revokes client writes); it's set by the Stripe webhook or by an admin.

## Idea: admin-comped Pro for friends
Friends and early testers get Pro without paying by having an admin set their tier by hand. No Stripe involved:

```sql
-- Supabase SQL editor (runs as service role)
update public.profiles
set tier = 'pro', updated_at = now()
where user_id = (select id from auth.users where email = 'friend@example.com');
```

Nice-to-haves if comps become common (proposed, not in `schema.sql` yet):
- `alter table profiles add column comped boolean not null default false, add column comp_note text, add column comp_expires_at timestamptz;`
  so comps are visible, explainable ("Christian's friend, beta") and can expire automatically.
- The Stripe webhook must **skip rows where `comped = true`** so a cancelled or never-created
  subscription doesn't downgrade a comped friend.
- A daily job (or the 9 AM pipeline) resets expired comps: `update profiles set tier='free', comped=false where comped and comp_expires_at < now();`
- Later, a tiny `/admin` page (gated to an `is_admin` flag) could flip this instead of using SQL.
