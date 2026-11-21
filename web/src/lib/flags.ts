/** Product flags, kept as plain constants so they can be flipped in one place.
 *
 *  Other flags live next to the data they gate:
 *  - SHOW_TRACKED_TIER in ./pickTier
 *  - SHOW_METHODOLOGY_PLACEHOLDERS in ./methodology
 *
 *  Keep this module free of heavy imports so middleware can read SHOW_ACCOUNTS.
 */

/** Account, login, and signup UI. Auth routes and AuthForm stay in the tree.
 *  Flip to true to bring Log in / Sign up and the account CTAs back.
 */
export const SHOW_ACCOUNTS = false;
