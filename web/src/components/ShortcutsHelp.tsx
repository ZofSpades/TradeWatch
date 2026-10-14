import { Dialog } from "./Dialog";

const SHORTCUTS: [string, string][] = [
  ["j / k", "Next / previous alert"],
  ["t", "Triage the selected alert"],
  ["e", "Escalate the selected alert"],
  ["d", "Dismiss the selected alert"],
  ["c", "Comment on the selected alert"],
  ["r", "Reload the queue"],
  ["/", "Filter by symbol"],
  ["1 / 2 / 3", "Alerts / Rules / Simulator"],
  ["?", "This help"],
  ["Esc", "Close a dialog"],
];

export function ShortcutsHelp({ onClose }: { onClose: () => void }) {
  return (
    <Dialog title="Keyboard shortcuts" onClose={onClose}>
      <table className="shortcuts">
        <tbody>
          {SHORTCUTS.map(([keys, what]) => (
            <tr key={keys}>
              <td>
                {keys.split(" / ").map((k, i) => (
                  <span key={k}>
                    {i > 0 && " / "}
                    <kbd>{k}</kbd>
                  </span>
                ))}
              </td>
              <td>{what}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="dialog-actions">
        <button type="button" className="primary" onClick={onClose} autoFocus>
          Close
        </button>
      </div>
    </Dialog>
  );
}
