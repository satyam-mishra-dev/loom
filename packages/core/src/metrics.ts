/**
 * Homegrown metrics shared by the services. A histogram is exact count/sum
 * plus quantiles over a bounded reservoir of recent samples — enough for
 * p50/p99 without prom-client.
 */
export class Hist {
  count = 0;
  sum = 0;
  private readonly samples: number[] = [];
  private idx = 0;

  constructor(private readonly cap = 10_000) {}

  observe(value: number): void {
    this.count++;
    this.sum += value;
    if (this.samples.length < this.cap) {
      this.samples.push(value);
    } else {
      this.samples[this.idx] = value;
      this.idx = (this.idx + 1) % this.cap;
    }
  }

  quantile(q: number): number {
    if (this.samples.length === 0) return 0;
    const sorted = [...this.samples].sort((a, b) => a - b);
    return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] ?? 0;
  }
}

/** Prometheus text-format lines for a counter/gauge. */
export function renderCounter(name: string, value: number, type: 'counter' | 'gauge' = 'counter'): string[] {
  return [`# TYPE ${name} ${type}`, `${name} ${value}`];
}

/** Prometheus text-format lines for a Hist as a summary with p50/p99. */
export function renderSummary(name: string, h: Hist): string[] {
  return [
    `# TYPE ${name} summary`,
    `${name}_count ${h.count}`,
    `${name}_sum ${h.sum}`,
    `${name}{quantile="0.5"} ${h.quantile(0.5)}`,
    `${name}{quantile="0.99"} ${h.quantile(0.99)}`,
  ];
}
