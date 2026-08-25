import type { MetricPoint } from "@/lib/api";

export function MetricSparkline({ points, color = "#73d4ff" }: { points: MetricPoint[]; color?: string }) {
  if (points.length < 2) return <div className="metric-empty">시계열 데이터가 없습니다.</div>;
  const values = points.map((point) => point.metricValue);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const path = points.map((point, index) => {
    const x = (index / (points.length - 1)) * 100;
    const y = 38 - ((point.metricValue - min) / range) * 32;
    return `${index === 0 ? "M" : "L"}${x.toFixed(2)},${y.toFixed(2)}`;
  }).join(" ");

  return (
    <svg className="sparkline" viewBox="0 0 100 42" preserveAspectRatio="none" role="img" aria-label="메트릭 추이">
      <path d={`${path} L100,42 L0,42 Z`} fill={color} opacity="0.09" />
      <path d={path} fill="none" stroke={color} strokeWidth="1.5" vectorEffect="non-scaling-stroke" />
    </svg>
  );
}
