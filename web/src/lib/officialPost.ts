/** Official morning-run window in America/Chicago.
 *  Official picks / the Blue Bird feed post at 6:30 AM CT; the pipeline run is 6:00 AM CT.
 *  A single 7:00 AM CT catch‑up can post official picks only for markets that had no line
 *  at the 6:00 AM CT run. Markets that already had a line at 6:00 AM CT stay Late Value.
 *  Keep every public mention of the post and catch‑up times on these constants so the copy cannot drift. */
export const OFFICIAL_POST_TIME = "6:30 AM CT";
export const OFFICIAL_POST_MINUTES_CT = 6 * 60 + 30;
export const OFFICIAL_CATCHUP_TIME = "7:00 AM CT";
export const OFFICIAL_POST_HOME_NOTE =
  `Official plays post every morning by about ${OFFICIAL_POST_TIME}, with a single ${OFFICIAL_CATCHUP_TIME} catch‑up for markets that had no line yet. Follow @CZarpAnalytics on X.`;
