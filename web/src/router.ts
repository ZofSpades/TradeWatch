import { useCallback, useEffect, useState } from "react";

/** The dashboard's pages, addressed by the URL hash so that links work. */
export type Route =
  | { page: "alerts"; key?: string }
  | { page: "rules"; id?: string }
  | { page: "system" }
  | { page: "simulator" };

export function parseHash(hash: string): Route {
  const parts = hash.replace(/^#\/?/, "").split("/").filter(Boolean).map(decodeURIComponent);
  switch (parts[0]) {
    case "rules":
      return parts[1] ? { page: "rules", id: parts[1] } : { page: "rules" };
    case "system":
      return { page: "system" };
    case "simulator":
      return { page: "simulator" };
    default:
      return parts[0] === "alerts" && parts[1] ? { page: "alerts", key: parts[1] } : { page: "alerts" };
  }
}

export function href(r: Route): string {
  switch (r.page) {
    case "rules":
      return r.id ? `#/rules/${encodeURIComponent(r.id)}` : "#/rules";
    case "system":
      return "#/system";
    case "simulator":
      return "#/simulator";
    default:
      return r.key ? `#/alerts/${encodeURIComponent(r.key)}` : "#/alerts";
  }
}

/** useRoute follows the URL hash. */
export function useRoute(): [Route, (r: Route) => void] {
  const [route, setRoute] = useState<Route>(() => parseHash(window.location.hash));
  useEffect(() => {
    const onChange = () => setRoute(parseHash(window.location.hash));
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  const navigate = useCallback((r: Route) => {
    const h = href(r);
    if (window.location.hash !== h) window.location.hash = h;
    setRoute(r);
  }, []);
  return [route, navigate];
}
