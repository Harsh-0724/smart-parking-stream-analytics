import "@testing-library/jest-dom/vitest";

// jsdom has no layout engine: give components the observer API they expect.
class ResizeObserverStub {
  observe() {}
  unobserve() {}
  disconnect() {}
}
globalThis.ResizeObserver = ResizeObserverStub as unknown as typeof ResizeObserver;
