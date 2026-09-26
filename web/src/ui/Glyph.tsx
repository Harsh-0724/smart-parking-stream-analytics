export type Shape = "square-outline" | "square" | "triangle" | "diamond" | "circle";

/** Status is never colour alone: each state also has its own shape. */
export function Glyph({ shape, size = 10 }: { shape: Shape; size?: number }) {
  const s = size;
  const common = { width: s, height: s, viewBox: "0 0 10 10", "aria-hidden": true, className: "glyph" } as const;
  switch (shape) {
    case "square":
      return (
        <svg {...common}>
          <rect x="1" y="1" width="8" height="8" fill="currentColor" />
        </svg>
      );
    case "square-outline":
      return (
        <svg {...common}>
          <rect x="1.5" y="1.5" width="7" height="7" fill="none" stroke="currentColor" strokeWidth="1.25" />
        </svg>
      );
    case "triangle":
      return (
        <svg {...common}>
          <path d="M5 1 9.2 8.8H0.8Z" fill="none" stroke="currentColor" strokeWidth="1.25" strokeLinejoin="round" />
        </svg>
      );
    case "diamond":
      return (
        <svg {...common}>
          <path d="M5 0.6 9.4 5 5 9.4 0.6 5Z" fill="currentColor" />
        </svg>
      );
    case "circle":
      return (
        <svg {...common}>
          <circle cx="5" cy="5" r="3.5" fill="none" stroke="currentColor" strokeWidth="1.25" />
        </svg>
      );
  }
}
