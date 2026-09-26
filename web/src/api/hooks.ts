import { useQuery } from "@tanstack/react-query";
import { api } from "./client";

const POLL_MS = 5000;

export const useOverview = () => useQuery({ queryKey: ["overview"], queryFn: api.overview, refetchInterval: POLL_MS });
export const useLotDetail = (id: string) => useQuery({ queryKey: ["lot", id], queryFn: () => api.lot(id), refetchInterval: POLL_MS });
export const useHistory = (id: string) =>
  useQuery({ queryKey: ["history", id], queryFn: () => api.history(id), refetchInterval: POLL_MS });
export const useAlerts = (f: { kind?: string; status?: string; limit: number; offset: number }) =>
  useQuery({ queryKey: ["alerts", f], queryFn: () => api.alerts(f), refetchInterval: 15_000 });
export const usePipelineRest = () => useQuery({ queryKey: ["pipeline"], queryFn: api.pipeline, refetchInterval: 10_000 });
