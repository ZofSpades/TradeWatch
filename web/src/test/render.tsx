import { act, render } from "@testing-library/react";
import type { ReactElement } from "react";

import type { LiveEvent } from "../api/stream";
import { AuthProvider, type Session } from "../auth";
import { LiveProvider, usePublish } from "../live";
import { session as defaultSession } from "./fakeApi";

/**
 * renderWithSession renders ui signed in, with a live feed that the test
 * drives by hand instead of a stream.
 */
export function renderWithSession(ui: ReactElement, s: Session | null = defaultSession) {
  let publish: (e: LiveEvent) => void = () => {};
  function Capture() {
    publish = usePublish();
    return null;
  }
  const utils = render(
    <AuthProvider initial={s}>
      <LiveProvider connect={false}>
        <Capture />
        {ui}
      </LiveProvider>
    </AuthProvider>,
  );
  return { ...utils, publish: (e: LiveEvent) => act(() => publish(e)) };
}
