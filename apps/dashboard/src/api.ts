import type { CrashResult, ProofResult, TripHistory } from './types.js';

// The read model's URL + demo token are baked at build time (VITE_*). The token
// gates /events + /spawn + /proof + /fault + /trip; empty = the read model runs
// open (bare local dev).
const READMODEL_URL = (import.meta.env['VITE_READMODEL_URL'] as string | undefined) ?? 'http://localhost:4600';
const READMODEL_TOKEN = (import.meta.env['VITE_READMODEL_TOKEN'] as string | undefined) ?? '';

/** Append the demo token to a read-model path. */
export function readUrl(path: string): string {
  if (READMODEL_TOKEN === '') return `${READMODEL_URL}${path}`;
  const sep = path.includes('?') ? '&' : '?';
  return `${READMODEL_URL}${path}${sep}token=${encodeURIComponent(READMODEL_TOKEN)}`;
}

export const CENTER = {
  lat: Number(import.meta.env['VITE_CENTER_LAT'] ?? 37.7749),
  lng: Number(import.meta.env['VITE_CENTER_LNG'] ?? -122.4194),
};

/** Deployed commit SHA (baked at build, for the authenticity footer). */
export const COMMIT_SHA = (import.meta.env['VITE_COMMIT_SHA'] as string | undefined) ?? '';

async function postJson<T>(path: string, body?: unknown): Promise<T> {
  const init: RequestInit = { method: 'POST', headers: { 'content-type': 'application/json' } };
  if (body !== undefined) init.body = JSON.stringify(body);
  const res = await fetch(readUrl(path), init);
  if (!res.ok) throw new Error(`${path} → ${res.status}`);
  return (await res.json()) as T;
}

export function spawnRequests(n: number, hotspot: boolean): Promise<{ spawned: number }> {
  return postJson('/spawn', { n, hotspot });
}

/** Run the real 200-concurrent signature scenario against the live sandbox. */
export function runProof(): Promise<ProofResult> {
  return postJson('/proof');
}

/** Inject a real abandoned-claim fault and report the live janitor's recovery. */
export function crashMatcher(): Promise<CrashResult> {
  return postJson('/fault/abandon-claim');
}

/** Real event history + offer cascade + surge for one trip. */
export async function fetchTrip(tripId: string): Promise<TripHistory> {
  const res = await fetch(readUrl(`/trip/${encodeURIComponent(tripId)}/events`));
  if (!res.ok) throw new Error(`trip ${tripId} → ${res.status}`);
  return (await res.json()) as TripHistory;
}
