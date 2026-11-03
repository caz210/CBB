export default function AuthLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="grid-bg relative flex overflow-hidden items-start justify-center px-4 py-6 md:min-h-[680px] md:items-center md:px-6 md:py-16">
      <div className="pointer-events-none absolute left-1/2 top-10 h-80 w-[600px] -translate-x-1/2 rounded-full bg-court-500/10 blur-3xl" />
      <div className="relative w-full max-w-md">{children}</div>
    </div>
  );
}
