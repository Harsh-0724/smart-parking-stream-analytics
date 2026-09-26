import type { AlertPage, History, LotDetail, Overview, PipelineStats } from "./types";

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

async function get<T>(path: string, params?: Record<string, string | number | undefined>): Promise<T> {
  const query = new URLSearchParams();
  for (const [k, v] of Object.entries(params ?? {})) if (v !== undefined && v !== "") query.set(k, String(v));
  const url = `/api${path}${query.size ? `?${query}` : ""}`;
  const response = await fetch(url);
  if (!response.ok) throw new ApiError(response.status, `${response.status} ${response.statusText} for ${url}`);
  return (await response.json()) as T;
}

export const api = {
  overview: () => get<Overview>("/overview"),
  lot: (id: string) => get<LotDetail>(`/lots/${id}`),
  history: (id: string, resolution: "5m" | "1h" = "5m") => get<History>(`/lots/${id}/history`, { resolution }),
  alerts: (f: { kind?: string; status?: string; limit?: number; offset?: number }) => get<AlertPage>("/alerts", f),
  pipeline: () => get<PipelineStats>("/pipeline"),
};
