import { useEffect, useRef } from "react";

/** Whether a key event comes from a place where typing must not be hijacked. */
function typing(e: KeyboardEvent): boolean {
  const el = e.target as HTMLElement | null;
  if (!el) return false;
  const tag = el.tagName;
  return tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || el.isContentEditable;
}

/**
 * useShortcuts binds single-key shortcuts (such as "j", "Enter" or "?") on
 * the document while enabled. Keys pressed with Ctrl, Alt or Meta, keys
 * typed into form fields, and keys inside an open dialog are left alone.
 */
export function useShortcuts(bindings: Record<string, () => void>, enabled = true) {
  const ref = useRef(bindings);
  ref.current = bindings;
  useEffect(() => {
    if (!enabled) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.ctrlKey || e.altKey || e.metaKey || typing(e)) return;
      if ((e.target as HTMLElement | null)?.closest?.("[role=dialog]")) return;
      const fn = ref.current[e.key];
      if (fn) {
        e.preventDefault();
        fn();
      }
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [enabled]);
}
