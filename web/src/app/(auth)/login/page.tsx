import type { Metadata } from "next";
import { redirect } from "next/navigation";
import { pageMetadata } from "@/lib/seo";
import { AuthForm } from "@/components/AuthForm";
import { SHOW_ACCOUNTS } from "@/lib/flags";

export const metadata: Metadata = pageMetadata({
  title: "Log in",
  description: "Log in to your CZarp Analytics account.",
  path: "/login",
});

export default function LoginPage() {
  if (!SHOW_ACCOUNTS) redirect("/");
  return <AuthForm mode="login" />;
}
