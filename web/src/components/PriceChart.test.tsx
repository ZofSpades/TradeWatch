import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { detail } from "../test/fakeApi";
import { PriceChart } from "./PriceChart";

describe("PriceChart", () => {
  it("draws the prices, the window and a marker per priced evidence event", () => {
    const ev = detail(1).evidence;
    const { container } = render(
      <PriceChart points={ev.price_context} events={ev.timeline} windowStart="2026-10-14T10:00:00Z" windowEnd="2026-10-14T10:00:05Z" />,
    );
    expect(container.querySelector(".chart-last")?.getAttribute("d")).toMatch(/^M[\d.]+ [\d.]+ L/);
    expect(container.querySelectorAll(".chart-marker")).toHaveLength(2);
    expect(container.querySelector(".chart-window")).not.toBeNull();
    expect(screen.getByText("trade sell 400 @ 100.01 (genuine_fill)")).toBeInTheDocument();
  });

  it("says so when there is too little data", () => {
    render(<PriceChart points={detail(1).evidence.price_context.slice(0, 1)} events={[]} windowStart="" windowEnd="" />);
    expect(screen.getByText(/Not enough market data/)).toBeInTheDocument();
  });
});
