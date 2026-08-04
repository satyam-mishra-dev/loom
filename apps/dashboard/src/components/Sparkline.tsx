import type { ReactElement } from 'react';

/** A 60-sample line, no axes — pure trend. Color is a token passed by the caller. */
export function Sparkline({
  data,
  color,
  width = 96,
  height = 22,
}: {
  data: number[];
  color: string;
  width?: number;
  height?: number;
}): ReactElement {
  if (data.length < 2) {
    return <svg width={width} height={height} aria-hidden="true" />;
  }
  const max = Math.max(...data, 1);
  const min = Math.min(...data, 0);
  const span = max - min || 1;
  const step = width / (data.length - 1);
  const pts = data.map((v, i) => {
    const x = i * step;
    const y = height - 2 - ((v - min) / span) * (height - 4);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });
  const d = `M${pts.join(' L')}`;
  const area = `${d} L${width},${height} L0,${height} Z`;
  return (
    <svg width={width} height={height} aria-hidden="true" className="overflow-visible">
      <path d={area} fill={color} opacity={0.12} />
      <path
        d={d}
        fill="none"
        stroke={color}
        strokeWidth={1.5}
        strokeLinejoin="round"
        strokeLinecap="round"
      />
      <circle
        cx={width}
        cy={height - 2 - ((data[data.length - 1]! - min) / span) * (height - 4)}
        r={1.8}
        fill={color}
      />
    </svg>
  );
}
