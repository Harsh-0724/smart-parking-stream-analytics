/** Literal design values, mirrored from tokens.css. Change both together (a test enforces it). */

export const accent = { light: "#0b7a6e", dark: "#3cc8b4" } as const;

export const fontSizes = { xs: 12, sm: 13, md: 16, lg: 24, xl: 48 } as const;

/** 4px grid */
export const space = { 1: 4, 2: 8, 3: 12, 4: 16, 5: 24, 6: 32, 7: 48 } as const;

export const fonts = { ui: "IBM Plex Sans", mono: "IBM Plex Mono" } as const;

export const radius = { sm: 4, lg: 6 } as const;
