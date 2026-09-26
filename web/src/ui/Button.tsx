import type { ButtonHTMLAttributes, ReactNode } from "react";

interface Props extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "default" | "primary" | "ghost";
  icon?: ReactNode;
  pressed?: boolean;
}

export function Button({ variant = "default", icon, pressed, children, className = "", ...rest }: Props) {
  return (
    <button
      type="button"
      {...rest}
      aria-pressed={pressed}
      className={`btn btn--${variant}${pressed ? " is-pressed" : ""} ${className}`}
    >
      {icon}
      {children}
    </button>
  );
}
