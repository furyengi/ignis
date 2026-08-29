/**
 * Latency accounting.
 *
 * Percentiles come from a sorted-on-read sample buffer rather than an HDR
 * histogram: at benchmark scale (10^4-10^5 samples) the exactness is worth
 * more than the constant factor, and it avoids a dependency.
 */

export interface Percentiles {
  count: number;
  min: number;
  p50: number;
  p90: number;
  p95: number;
  p99: number;
  max: number;
  mean: number;
}

const EMPTY: Percentiles = {
  count: 0, min: 0, p50: 0, p90: 0, p95: 0, p99: 0, max: 0, mean: 0,
};

export class Histogram {
  private samples: number[] = [];
  private sum = 0;

  record(valueMs: number): void {
    this.samples.push(valueMs);
    this.sum += valueMs;
  }

  get count(): number {
    return this.samples.length;
  }

  /**
   * Nearest-rank percentiles over a copy of the samples. O(n log n) per call,
   * so call it once at the end of a run, not per request.
   */
  snapshot(): Percentiles {
    const n = this.samples.length;
    if (n === 0) return { ...EMPTY };
    const sorted = [...this.samples].sort((a, b) => a - b);
    const at = (q: number): number => {
      const rank = Math.ceil(q * n);
      const idx = Math.min(n - 1, Math.max(0, rank - 1));
      return sorted[idx]!;
    };
    return {
      count: n,
      min: sorted[0]!,
      p50: at(0.5),
      p90: at(0.9),
      p95: at(0.95),
      p99: at(0.99),
      max: sorted[n - 1]!,
      mean: this.sum / n,
    };
  }

  reset(): void {
    this.samples = [];
    this.sum = 0;
  }
}

/** Named counters, for things that are events rather than durations. */
export class Counters {
  private readonly values = new Map<string, number>();

  inc(name: string, by = 1): void {
    this.values.set(name, (this.values.get(name) ?? 0) + by);
  }

  get(name: string): number {
    return this.values.get(name) ?? 0;
  }

  toJSON(): Record<string, number> {
    return Object.fromEntries(this.values);
  }
}

/** The metric set the runtime exposes on /metrics. */
export class RuntimeMetrics {
  readonly coldStart = new Histogram();
  readonly warmLatency = new Histogram();
  readonly coldLatency = new Histogram();
  readonly queueWait = new Histogram();
  readonly handler = new Histogram();
  readonly counters = new Counters();

  toJSON() {
    return {
      counters: this.counters.toJSON(),
      coldStartMs: this.coldStart.snapshot(),
      coldInvokeMs: this.coldLatency.snapshot(),
      warmInvokeMs: this.warmLatency.snapshot(),
      queueWaitMs: this.queueWait.snapshot(),
      handlerMs: this.handler.snapshot(),
    };
  }

  /** Prometheus text exposition, so this drops into an existing scrape config. */
  toPrometheus(): string {
    const lines: string[] = [];
    for (const [name, value] of Object.entries(this.counters.toJSON())) {
      lines.push(`# TYPE ignis_${name} counter`, `ignis_${name} ${value}`);
    }
    const hists: Array<[string, Histogram]> = [
      ['cold_start_ms', this.coldStart],
      ['cold_invoke_ms', this.coldLatency],
      ['warm_invoke_ms', this.warmLatency],
      ['queue_wait_ms', this.queueWait],
      ['handler_ms', this.handler],
    ];
    for (const [name, h] of hists) {
      const s = h.snapshot();
      lines.push(`# TYPE ignis_${name} summary`);
      for (const q of ['50', '90', '95', '99'] as const) {
        const key = `p${q}` as keyof Percentiles;
        lines.push(`ignis_${name}{quantile="0.${q}"} ${s[key]}`);
      }
      lines.push(`ignis_${name}_count ${s.count}`);
    }
    return lines.join('\n') + '\n';
  }
}
