import { useEffect, useState } from "react";

import type { RuleHistory, RuleSet } from "../api/types";
import { useClient } from "../auth";
import { fmtAgo, typeLabel } from "../format";

interface Props {
  selected?: string;
  onSelect: (id?: string) => void;
}

/** RulesPage shows the rules in force and every version of each. */
export function RulesPage({ selected, onSelect }: Props) {
  const client = useClient();
  const [set, setSet] = useState<RuleSet | null>(null);
  const [history, setHistory] = useState<RuleHistory | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    client
      .rules()
      .then(setSet)
      .catch((err: unknown) => setError(err instanceof Error ? err.message : String(err)));
  }, [client]);

  useEffect(() => {
    setHistory(null);
    if (!selected) return;
    client
      .ruleHistory(selected)
      .then(setHistory)
      .catch((err: unknown) => setError(err instanceof Error ? err.message : String(err)));
  }, [client, selected]);

  return (
    <div className="rules-layout">
      <section aria-labelledby="rules-title">
        <h2 id="rules-title">Rules in force</h2>
        {error && (
          <p className="error" role="alert">
            {error}
          </p>
        )}
        {set && (
          <>
            <p className="muted">
              Loaded by <code>{set.loaded_by}</code> {fmtAgo(set.loaded_at)} · rule set <code>{set.ruleset_digest.slice(0, 12)}</code>
            </p>
            <table className="rules-table">
              <thead>
                <tr>
                  <th>Rule</th>
                  <th>Name</th>
                  <th>Version</th>
                  <th>Detects</th>
                  <th>Taxonomy</th>
                  <th>Enabled</th>
                </tr>
              </thead>
              <tbody>
                {set.rules.map((r) => (
                  <tr
                    key={r.rule_id}
                    className={r.rule_id === selected ? "selected" : ""}
                    aria-selected={r.rule_id === selected}
                    tabIndex={0}
                    onClick={() => onSelect(r.rule_id)}
                    onKeyDown={(e) => e.key === "Enter" && onSelect(r.rule_id)}
                  >
                    <td>
                      <code>{r.rule_id}</code>
                    </td>
                    <td>{r.name}</td>
                    <td>
                      v{r.version}
                      {r.version_reused && (
                        <span className="warn" title="Another version of this rule has the same version number but different content">
                          {" "}
                          reused
                        </span>
                      )}
                    </td>
                    <td>{typeLabel(r.detector)}</td>
                    <td>{r.taxonomy_code}</td>
                    <td>{r.enabled ? "yes" : "no"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        )}
      </section>

      {history && (
        <section aria-labelledby="history-title">
          <h2 id="history-title">
            <code>{history.rule_id}</code> history
          </h2>
          {history.versions.map((v) => (
            <details key={v.digest} open={v === history.versions[0]}>
              <summary>
                v{v.version} · first loaded {fmtAgo(v.first_seen_at)} · digest <code>{v.digest.slice(0, 12)}</code>
                {v.version_reused && <span className="warn"> · edited without a version bump</span>}
              </summary>
              <pre className="source">{v.source}</pre>
            </details>
          ))}
        </section>
      )}
    </div>
  );
}
