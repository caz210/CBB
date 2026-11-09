"use client";
import { useCallback, useEffect, useState } from "react";

const KEY = "czarp-watchlist";

function read(): number[] {
  if (typeof window === "undefined") return [];
  try {
    const raw = localStorage.getItem(KEY);
    if (!raw) return [];
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr.map(Number).filter((n) => Number.isFinite(n)) : [];
  } catch {
    return [];
  }
}

function write(ids: number[]) {
  try {
    localStorage.setItem(KEY, JSON.stringify([...new Set(ids)]));
  } catch { /* quota / private mode */ }
}

/** Starred game ids in localStorage (auth isn't real yet). */
export function useWatchlist() {
  const [ids, setIds] = useState<number[]>([]);
  const [ready, setReady] = useState(false);
  useEffect(() => {
    setIds(read());
    setReady(true);
  }, []);
  const has = useCallback((id: number) => ids.includes(id), [ids]);
  const toggle = useCallback((id: number) => {
    setIds((prev) => {
      const next = prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id];
      write(next);
      return next;
    });
  }, []);
  return { ids, has, toggle, ready, count: ids.length };
}
