export type LiveStatus = "live" | "connecting" | "reconnecting";

const TEXT: Record<LiveStatus, string> = {
  live: "Live",
  connecting: "Connecting",
  reconnecting: "Reconnecting",
};

/** Filled accent dot = receiving data; hollow = not. The word is always shown. */
export function LiveDot({ status }: { status: LiveStatus }) {
  return (
    <span className={`livedot livedot--${status}`} role="status">
      <span className="livedot__dot" aria-hidden="true" />
      {TEXT[status]}
    </span>
  );
}
