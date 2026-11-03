import Link from "next/link";

export function Logo() {
  return (
    <Link href="/" className="flex shrink-0 items-center gap-2 font-semibold tracking-tight">
      <span className="grid h-8 w-8 place-items-center rounded-lg bg-gradient-to-br from-court-400 to-court-500 text-sm font-black text-ink-950">
        CZ
      </span>
      <span className="text-base md:text-lg">
        CZarp<span className="text-court-400">Analytics</span>
      </span>
    </Link>
  );
}
