import type { Metadata } from "next";
import { ContactEmail, H, LegalPage } from "@/components/LegalPage";
import { pageMetadata } from "@/lib/seo";

export const metadata: Metadata = pageMetadata({
  title: "Terms of Use",
  description: "Terms for using CZarp Analytics: the site is for information and entertainment, not a sportsbook or betting advice, and the public record is an early sample.",
  path: "/terms",
});

export default function TermsPage() {
  return (
    <LegalPage
      eyebrow="Legal"
      title="Terms of Use"
    >
      <p>Last updated: November 9, 2026.</p>
      <H>1. What this site is</H>
      <p>CZarp Analytics publishes model-driven sports projections and a public record of official picks. It is for information and entertainment. It is not a sportsbook, not a wagering service, and not betting advice.</p>
      <H>2. Eligibility</H>
      <p>You must be 21 or older to create an account or use the service. If you are in a jurisdiction where sports betting is restricted, you are responsible for complying with local law.</p>
      <H>3. No guarantee of results</H>
      <p>Projections, edges, and records can be wrong. An edge does not mean you will win tonight. The record is still an early sample.</p>
      <H>4. Accounts and acceptable use</H>
      <p>You are responsible for your account. Do not scrape, resell, or misrepresent our data as a sure thing. We may suspend access if the service is abused.</p>
      <H>5. Intellectual property</H>
      <p>Model outputs, copy, and design on this site are owned by CZarp Analytics unless noted. Third-party raw data is not redistributed.</p>
      <H>6. Limitation of liability</H>
      <p>To the extent allowed by law, the site is provided “as is,” without warranties of any kind. We are not liable for betting losses or decisions you make using this information.</p>
      <H>7. Changes</H>
      <p>We may update these terms. The date at the top will change when a reviewed version is published.</p>
      <p>Questions about these terms: contact us at <ContactEmail />.</p>
    </LegalPage>
  );
}
