import { redirect } from "next/navigation";
import { AuthForm } from "@/components/AuthForm";
import { SHOW_ACCOUNTS } from "@/lib/flags";

export const metadata = { title: "Sign up" };

export default function SignupPage() {
  if (!SHOW_ACCOUNTS) redirect("/");
  return <AuthForm mode="signup" />;
}
