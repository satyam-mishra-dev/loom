// Zero-dependency static file server for the built dashboard (dist/). Keeps the
// dashboard runtime image tiny — no node_modules, no nginx, no extra deps. SPA:
// unknown paths fall back to index.html.
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';

const DIST = fileURLToPath(new URL('./dist', import.meta.url));
const PORT = Number(process.env.PORT ?? 4620);

const TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.woff2': 'font/woff2',
};

async function send(res, path) {
  const body = await readFile(path);
  res.writeHead(200, { 'content-type': TYPES[extname(path)] ?? 'application/octet-stream' });
  res.end(body);
}

createServer((req, res) => {
  if (req.url === '/healthz') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end('{"status":"ok"}');
    return;
  }
  // Strip query, prevent path traversal, default to index.html.
  const rel = normalize(decodeURIComponent((req.url ?? '/').split('?')[0])).replace(/^(\.\.[/\\])+/, '');
  const target = rel === '/' || rel === '' ? 'index.html' : rel.replace(/^\//, '');
  send(res, join(DIST, target)).catch(() =>
    // SPA fallback: serve index.html for unknown routes.
    send(res, join(DIST, 'index.html')).catch(() => {
      res.writeHead(404);
      res.end('not found');
    }),
  );
}).listen(PORT, '0.0.0.0', () => {
  process.stdout.write(`dashboard static server on :${PORT}\n`);
});
