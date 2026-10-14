import type { AlertSummary } from "./types";

export interface LiveEvent {
  type: "alert.raised" | "alert.updated";
  alert: AlertSummary;
}

export type StreamState = "connecting" | "live" | "offline";

interface RawEvent {
  event: string;
  data: string;
}

/**
 * parseEvents splits a text/event-stream buffer into its complete events
 * and returns them with the unconsumed remainder. Comment lines (keep-alives)
 * are skipped; multi-line data is joined with newlines.
 */
export function parseEvents(buffer: string): { events: RawEvent[]; rest: string } {
  const normalized = buffer.replace(/\r\n?/g, "\n");
  const events: RawEvent[] = [];
  let start = 0;
  for (;;) {
    const end = normalized.indexOf("\n\n", start);
    if (end < 0) break;
    const block = normalized.slice(start, end);
    start = end + 2;
    let event = "message";
    const data: string[] = [];
    for (const line of block.split("\n")) {
      if (line === "" || line.startsWith(":")) continue;
      const colon = line.indexOf(":");
      const field = colon < 0 ? line : line.slice(0, colon);
      const value = colon < 0 ? "" : line.slice(colon + 1).replace(/^ /, "");
      if (field === "event") event = value;
      else if (field === "data") data.push(value);
    }
    if (data.length > 0) events.push({ event, data: data.join("\n") });
  }
  return { events, rest: normalized.slice(start) };
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * openStream follows the API's server-sent events with the bearer token in a
 * header (which EventSource cannot send), reconnecting with backoff. It
 * returns a function that closes the stream.
 */
export function openStream(
  token: string,
  onEvent: (e: LiveEvent) => void,
  onState: (s: StreamState) => void,
): () => void {
  let stopped = false;
  let ctrl: AbortController | undefined;
  let delay = 1000;

  async function run() {
    while (!stopped) {
      onState("connecting");
      ctrl = new AbortController();
      try {
        const res = await fetch("/api/v1/stream", {
          headers: { Authorization: `Bearer ${token}`, Accept: "text/event-stream" },
          signal: ctrl.signal,
        });
        if (res.status === 401) {
          onState("offline");
          return; // the session has ended; the auth layer logs out
        }
        if (!res.ok || !res.body) throw new Error(`stream: HTTP ${res.status}`);
        onState("live");
        delay = 1000;
        const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
        let buffer = "";
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          buffer += value;
          const { events, rest } = parseEvents(buffer);
          buffer = rest;
          for (const e of events) {
            if (e.event !== "alert.raised" && e.event !== "alert.updated") continue;
            try {
              onEvent({ type: e.event, alert: JSON.parse(e.data) as AlertSummary });
            } catch {
              // a malformed event is skipped
            }
          }
        }
      } catch {
        if (stopped) return;
      }
      onState("offline");
      await sleep(delay);
      delay = Math.min(delay * 2, 30_000);
    }
  }

  void run();
  return () => {
    stopped = true;
    ctrl?.abort();
  };
}
