import "@testing-library/jest-dom/vitest";
import { cleanup, configure } from "@testing-library/react";
import { afterEach } from "vitest";

// Waits allow for slow renders on a busy machine, like testTimeout.
configure({ asyncUtilTimeout: 5_000 });

afterEach(() => {
  cleanup();
  sessionStorage.clear();
  window.location.hash = "";
});
