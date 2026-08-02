import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

// Vite bundles React + deck.gl into a self-contained build — no CDN at runtime
// (the deployment's CSP forbids it). Dev server and preview both on 4620 (the
// repo's 46xx family, clear of Tally's 48xx).
export default defineConfig({
  plugins: [react()],
  server: { port: 4620, host: true },
  preview: { port: 4620, host: true },
});
