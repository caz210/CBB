import { redirect } from "next/navigation";
import { AuthForm } from "@/components/AuthForm";
import { SHOW_ACCOUNTS } from "@/lib/flags";

export const metadata = { title: "Log in" };

export default function LoginPage() {
  if (!SHOW_ACCOUNTS) redirect("/");
  return <AuthForm mode="login" />;
}
