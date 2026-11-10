import type { Metadata } from "next";
import { H, LegalPage } from "@/components/LegalPage";

export const metadata: Metadata = { title: "Privacy Policy · CZarp Analytics" };

export default function PrivacyPage() {
  return (
    <LegalPage
      eyebrow="Legal"
      title="Privacy Policy"
      banner="TEMPLATE — pending owner review. This is a placeholder describing intended practices, not a finished policy."
    >
      <p>Last updated: November 9, 2026. This draft will be reviewed by the site owner before it is treated as the official policy.</p>
      <H>Information we may collect</H>
      <p>If you create an account: email, display name, and authentication data from a provider (for example Google or Apple). If you use the site as a guest: standard server logs (IP, browser, pages) and optional local data such as a watchlist stored in your browser.</p>
      <H>How we use it</H>
      <p>To run the site, keep you signed in, remember preferences, improve the product, and communicate about your account. We do not sell your personal information.</p>
      <H>Sharing</H>
      <p>We may use processors such as hosting, database, and authentication providers (for example Vercel and Supabase) to operate the service. We share data with them only as needed to provide it, or if required by law.</p>
      <H>Cookies and local storage</H>
      <p>We may use cookies for sessions and localStorage for preferences (for example a device watchlist) until accounts can sync them.</p>
      <H>Retention and your choices</H>
      <p>You may ask to access, correct, or delete account data once authentication is live. Contact will be listed here after owner review.</p>
      <H>Children</H>
      <p>This service is for adults 21 and older. We do not knowingly collect information from anyone under 21.</p>
    </LegalPage>
  );
}
