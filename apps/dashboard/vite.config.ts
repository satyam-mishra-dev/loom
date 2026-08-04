import { fileURLToPath, URL } from 'node:url';
import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

// Vite bundles React + deck.gl + Tailwind into a self-contained build — no CDN
// at runtime (the deployment's CSP forbids it; fonts ship via @fontsource, the
// map renders on a plain colored ground with no external tile server). Dev
// server and preview both on 4620 (the repo's 46xx family, clear of Tally's 48xx).
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: { '@': fileURLToPath(new URL('./src', import.meta.url)) },
  },
  server: { port: 4620, host: true },
  preview: { port: 4620, host: true },
});
