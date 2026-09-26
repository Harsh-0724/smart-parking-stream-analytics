import { Glyph } from "./Glyph";
import { Button } from "./Button";

interface Props {
  kind: "loading" | "empty" | "error";
  title: string;
  hint?: string;
  onRetry?: () => void;
}

/** Loading, empty and error are designed states: left-aligned, plain, and say what to do next. */
export function State({ kind, title, hint, onRetry }: Props) {
  return (
    <div className={`state state--${kind}`} role={kind === "error" ? "alert" : "status"}>
      <div className="state__title">
        {kind === "error" && <Glyph shape="diamond" />}
        {title}
      </div>
      {hint && <div className="state__hint">{hint}</div>}
      {onRetry && (
        <Button onClick={onRetry} className="state__retry">
          Retry
        </Button>
      )}
    </div>
  );
}
