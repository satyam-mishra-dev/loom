# syntax=docker/dockerfile:1

# ---- deps: install the whole workspace once, cached on the lockfile ----
FROM node:20-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
COPY packages/core/package.json packages/core/package.json
COPY packages/db/package.json packages/db/package.json
COPY apps/gateway/package.json apps/gateway/package.json
COPY apps/matcher/package.json apps/matcher/package.json
COPY apps/simulator/package.json apps/simulator/package.json
COPY apps/read-model/package.json apps/read-model/package.json
COPY apps/dashboard/package.json apps/dashboard/package.json
RUN npm ci

# ---- app: the Node services (gateway, matcher, simulator, read-model, migrate).
# Run straight from TS via tsx — one image, the compose `command` picks the app. ----
FROM node:20-alpine AS app
WORKDIR /app
ENV NODE_ENV=production
COPY --from=deps /app/node_modules ./node_modules
COPY . .
USER node
EXPOSE 8080 8090 4600
CMD ["npm", "run", "start", "--workspace=@loom/gateway"]

# ---- dashboard build: Vite bundles React + deck.gl into static assets.
# VITE_READMODEL_URL is baked at build time (the browser hits the host port). ----
FROM deps AS dashboard-build
COPY . .
ARG VITE_READMODEL_URL=http://localhost:4600
ENV VITE_READMODEL_URL=$VITE_READMODEL_URL
# Demo-grade token baked into the bundle so the dashboard can reach the read
# model's now-gated /events + /spawn. Must match the read model's READ_MODEL_TOKEN.
ARG VITE_READMODEL_TOKEN=loom-demo-token
ENV VITE_READMODEL_TOKEN=$VITE_READMODEL_TOKEN
# Optional: base GitHub URL for the didactic {} deep-links (ARCHITECTURE.md +
# tests) and the commit SHA for the authenticity footer. Empty ⇒ the panels show
# on-disk paths instead of links, and the footer hides.
ARG VITE_REPO_URL=
ENV VITE_REPO_URL=$VITE_REPO_URL
ARG VITE_COMMIT_SHA=
ENV VITE_COMMIT_SHA=$VITE_COMMIT_SHA
RUN npm run build --workspace=@loom/dashboard

# ---- dashboard: serve the static bundle with a zero-dep node server ----
FROM node:20-alpine AS dashboard
WORKDIR /app
ENV NODE_ENV=production
COPY --from=dashboard-build /app/apps/dashboard/dist ./dist
COPY apps/dashboard/serve.mjs ./serve.mjs
USER node
EXPOSE 4620
CMD ["node", "serve.mjs"]
