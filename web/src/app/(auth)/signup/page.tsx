import type { Metadata } from "next";
import { redirect } from "next/navigation";
import { pageMetadata } from "@/lib/seo";
import { AuthForm } from "@/components/AuthForm";
import { SHOW_ACCOUNTS } from "@/lib/flags";

export const metadata: Metadata = pageMetadata({
  title: "Sign up",
  description: "Create a CZarp Analytics account to save models and follow official plays.",
  path: "/signup",
});

export default function SignupPage() {
  if (!SHOW_ACCOUNTS) redirect("/");
  return <AuthForm mode="signup" />;
}
