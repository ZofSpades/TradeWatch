import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";

import { openStream, type LiveEvent, type StreamState } from "./api/stream";
import { useAuth } from "./auth";

type Listener = (e: LiveEvent) => void;

interface LiveValue {
  state: StreamState;
  subscribe: (fn: Listener) => () => void;
  /** publish delivers an event to the subscribers, for tests. */
  publish: (e: LiveEvent) => void;
}

const LiveContext = createContext<LiveValue | null>(null);

/**
 * LiveProvider follows the API's event stream for the current session and
 * shares it with every subscribed component. With connect=false it opens
 * no stream, which tests use to publish events by hand.
 */
export function LiveProvider({ children, connect = true }: { children: ReactNode; connect?: boolean }) {
  const { session } = useAuth();
  const [state, setState] = useState<StreamState>(connect ? "connecting" : "offline");
  const listeners = useRef(new Set<Listener>());

  const publish = useCallback((e: LiveEvent) => {
    for (const fn of listeners.current) fn(e);
  }, []);

  useEffect(() => {
    if (!connect || !session) return;
    return openStream(session.token, publish, setState);
  }, [connect, session, publish]);

  const subscribe = useCallback((fn: Listener) => {
    listeners.current.add(fn);
    return () => {
      listeners.current.delete(fn);
    };
  }, []);

  const value = useMemo(() => ({ state, subscribe, publish }), [state, subscribe, publish]);
  return <LiveContext.Provider value={value}>{children}</LiveContext.Provider>;
}

function useLiveContext(): LiveValue {
  const v = useContext(LiveContext);
  if (!v) throw new Error("live hooks outside LiveProvider");
  return v;
}

/** useLiveEvents calls fn for every live event while the component is mounted. */
export function useLiveEvents(fn: Listener) {
  const { subscribe } = useLiveContext();
  const ref = useRef(fn);
  ref.current = fn;
  useEffect(() => subscribe((e) => ref.current(e)), [subscribe]);
}

export function useStreamState(): StreamState {
  return useLiveContext().state;
}

export function usePublish(): (e: LiveEvent) => void {
  return useLiveContext().publish;
}
