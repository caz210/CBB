import Image from "next/image";
import Link from "next/link";

/** Header / nav wordmark. Transparent cut of the on-palette mark; sized for a 56–64px bar. */
export function Logo() {
  return (
    <Link href="/" className="flex shrink-0 items-center" aria-label="CZarp Analytics">
      <Image
        src="/brand/wordmark@2x.webp"
        alt="CZarp Analytics"
        width={188}
        height={72}
        priority
        className="h-7 w-auto min-[360px]:h-8 md:h-9"
      />
    </Link>
  );
}
