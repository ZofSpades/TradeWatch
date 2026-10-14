import { useEffect, useId, useRef, type ReactNode } from "react";

interface Props {
  title: string;
  onClose: () => void;
  children: ReactNode;
}

/**
 * Dialog is a modal: it takes focus, keeps Tab inside itself, closes on
 * Escape, and gives focus back to whatever had it when it opened.
 */
export function Dialog({ title, onClose, children }: Props) {
  const ref = useRef<HTMLDivElement>(null);
  const titleId = useId();

  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null;
    const el = ref.current;
    const focusables = () =>
      Array.from(el?.querySelectorAll<HTMLElement>("button, textarea, input, select, a[href], [tabindex]:not([tabindex='-1'])") ?? []).filter(
        (f) => !f.hasAttribute("disabled"),
      );
    (el?.querySelector<HTMLElement>("[autofocus], textarea, input") ?? focusables()[0])?.focus();
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.preventDefault();
        onClose();
      } else if (e.key === "Tab") {
        const f = focusables();
        if (f.length === 0) return;
        const first = f[0]!;
        const last = f[f.length - 1]!;
        if (e.shiftKey && document.activeElement === first) {
          e.preventDefault();
          last.focus();
        } else if (!e.shiftKey && document.activeElement === last) {
          e.preventDefault();
          first.focus();
        }
      }
    };
    el?.addEventListener("keydown", onKey);
    return () => {
      el?.removeEventListener("keydown", onKey);
      previous?.focus?.();
    };
  }, [onClose]);

  return (
    <div className="backdrop" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <div className="dialog" role="dialog" aria-modal="true" aria-labelledby={titleId} ref={ref}>
        <h2 id={titleId}>{title}</h2>
        {children}
      </div>
    </div>
  );
}
