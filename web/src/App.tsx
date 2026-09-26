import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { lazy, Suspense, useEffect } from "react";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import { useOverview } from "./api/hooks";
import { liveStore, useLive } from "./api/live";
import { Shell, State } from "./ui";

const Overview = lazy(() => import("./screens/Overview"));
const LotDetail = lazy(() => import("./screens/LotDetail"));
const Alerts = lazy(() => import("./screens/Alerts"));
const Pipeline = lazy(() => import("./screens/Pipeline"));

const client = new QueryClient({ defaultOptions: { queries: { retry: 1, staleTime: 2000 } } });

function Frame() {
  const live = useLive();
  useEffect(() => {
    liveStore.start();
    return () => liveStore.stop();
  }, []);
  const overview = useOverview();
  return (
    <Shell status={live.status} alertCount={overview.data?.active_alerts}>
      <Suspense fallback={<State kind="loading" title="Loading" />}>
        <Routes>
          <Route path="/" element={<Overview />} />
          <Route path="/lots/:lotId" element={<LotDetail />} />
          <Route path="/alerts" element={<Alerts />} />
          <Route path="/pipeline" element={<Pipeline />} />
          <Route path="*" element={<State kind="empty" title="No such screen" hint="Use the navigation on the left." />} />
        </Routes>
      </Suspense>
    </Shell>
  );
}

export function App() {
  return (
    <QueryClientProvider client={client}>
      <BrowserRouter>
        <Frame />
      </BrowserRouter>
    </QueryClientProvider>
  );
}
