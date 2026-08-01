import { defineConfig } from 'vitest/config';

// Root vitest project: exactly the repo-level tests (the signature test).
// Workspace packages run their own suites via their own scripts.
export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    testTimeout: 120_000,
    hookTimeout: 240_000,
  },
});
