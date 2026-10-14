import { useState, type FormEvent } from "react";

import { Dialog } from "./Dialog";

interface Props {
  title: string;
  action: string;
  /** Whether a note must be given; escalating and dismissing need a reason. */
  required: boolean;
  onSubmit: (note: string) => Promise<void>;
  onClose: () => void;
}

/** NoteDialog asks for a note before a status change or as a comment. */
export function NoteDialog({ title, action, required, onSubmit, onClose }: Props) {
  const [note, setNote] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function submit(e: FormEvent) {
    e.preventDefault();
    if (required && note.trim() === "") {
      setError("A reason is required.");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      await onSubmit(note.trim());
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setBusy(false);
    }
  }

  return (
    <Dialog title={title} onClose={onClose}>
      <form onSubmit={submit}>
        <label htmlFor="note">{required ? "Reason (required)" : "Note (optional)"}</label>
        <textarea
          id="note"
          rows={4}
          maxLength={4000}
          value={note}
          onChange={(e) => setNote(e.target.value)}
          aria-invalid={error ? true : undefined}
          aria-describedby={error ? "note-error" : undefined}
          autoFocus
        />
        {error && (
          <p id="note-error" className="error" role="alert">
            {error}
          </p>
        )}
        <div className="dialog-actions">
          <button type="button" onClick={onClose}>
            Cancel
          </button>
          <button type="submit" className="primary" disabled={busy}>
            {busy ? "Saving…" : action}
          </button>
        </div>
      </form>
    </Dialog>
  );
}
