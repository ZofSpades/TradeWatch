import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// The dashboard calls the API on its own origin: nginx proxies /api in the
// compose stack, and the dev server proxies it to the API on localhost:8080.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: { "/api": { target: "http://localhost:8080", changeOrigin: false } },
  },
  build: { sourcemap: true },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["src/test/setup.ts"],
    // A file's first test renders cold, which takes seconds on a busy
    // machine. A hung test still fails.
    testTimeout: 20_000,
  },
});
