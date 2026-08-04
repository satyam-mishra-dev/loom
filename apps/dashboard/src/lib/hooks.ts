import { useEffect, useRef, useState } from 'react';

/** True when the viewer asked for reduced motion — signature moments go static. */
export function useReducedMotion(): boolean {
  const [reduced, setReduced] = useState(false);
  useEffect(() => {
    const mq = window.matchMedia('(prefers-reduced-motion: reduce)');
    setReduced(mq.matches);
    const on = (): void => setReduced(mq.matches);
    mq.addEventListener('change', on);
    return () => mq.removeEventListener('change', on);
  }, []);
  return reduced;
}

/** Count up to `target` over `ms`. Reduced motion or ms=0 jumps straight there. */
export function useCountUp(target: number, ms: number, active: boolean): number {
  const reduced = useReducedMotion();
  const [value, setValue] = useState(active ? 0 : target);
  const raf = useRef<number>(0);
  useEffect(() => {
    if (!active) {
      setValue(target);
      return;
    }
    if (reduced || ms <= 0) {
      setValue(target);
      return;
    }
    const start = performance.now();
    const tick = (now: number): void => {
      const t = Math.min(1, (now - start) / ms);
      const eased = 1 - Math.pow(1 - t, 3);
      setValue(Math.round(eased * target));
      if (t < 1) raf.current = requestAnimationFrame(tick);
    };
    raf.current = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf.current);
  }, [target, ms, active, reduced]);
  return value;
}
