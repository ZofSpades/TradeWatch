import { describe, expect, it } from "vitest";

import { parseEvents } from "./stream";

describe("parseEvents", () => {
  it("splits complete events and keeps the remainder", () => {
    const { events, rest } = parseEvents('event: alert.raised\ndata: {"key":"a"}\n\nevent: alert.upd');
    expect(events).toEqual([{ event: "alert.raised", data: '{"key":"a"}' }]);
    expect(rest).toBe("event: alert.upd");
  });

  it("skips comments such as keep-alives", () => {
    const { events } = parseEvents(": connected\n\n: keep-alive\n\nevent: alert.updated\ndata: {}\n\n");
    expect(events).toEqual([{ event: "alert.updated", data: "{}" }]);
  });

  it("handles CRLF line endings and multi-line data", () => {
    const { events, rest } = parseEvents("event: x\r\ndata: line 1\r\ndata: line 2\r\n\r\n");
    expect(events).toEqual([{ event: "x", data: "line 1\nline 2" }]);
    expect(rest).toBe("");
  });

  it("reassembles an event split across chunks", () => {
    let buffer = "";
    const seen: string[] = [];
    for (const chunk of ["event: alert.rai", 'sed\ndata: {"k', '":1}\n', "\n"]) {
      buffer += chunk;
      const { events, rest } = parseEvents(buffer);
      buffer = rest;
      seen.push(...events.map((e) => `${e.event} ${e.data}`));
    }
    expect(seen).toEqual(['alert.raised {"k":1}']);
  });
});
