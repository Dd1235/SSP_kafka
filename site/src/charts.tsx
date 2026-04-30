import type { ReactNode } from "react";

export type Series = {
  label: string;
  color: string;
  values: Array<{ x: number; y: number | null | undefined }>;
};

export type BarDatum = {
  label: string;
  value: number | null | undefined;
  color?: string;
};

const WIDTH = 760;
const HEIGHT = 260;
const PAD = { top: 18, right: 20, bottom: 38, left: 58 };

function finite(values: Array<number | null | undefined>) {
  return values.filter((value): value is number => typeof value === "number" && Number.isFinite(value));
}

function niceMax(value: number) {
  if (value <= 0 || !Number.isFinite(value)) {
    return 1;
  }
  const pow = 10 ** Math.floor(Math.log10(value));
  return Math.ceil(value / pow) * pow;
}

function fmt(value: number, unit = "") {
  if (Math.abs(value) >= 1000000) return `${(value / 1000000).toFixed(1)}M${unit}`;
  if (Math.abs(value) >= 1000) return `${(value / 1000).toFixed(1)}k${unit}`;
  if (Math.abs(value) >= 100) return `${value.toFixed(0)}${unit}`;
  if (Math.abs(value) >= 10) return `${value.toFixed(1)}${unit}`;
  return `${value.toFixed(2)}${unit}`;
}

export function EmptyChart({ children }: { children: ReactNode }) {
  return <div className="empty-chart">{children}</div>;
}

export function LineChart({
  title,
  series,
  yUnit = "",
  yMax,
}: {
  title: string;
  series: Series[];
  yUnit?: string;
  yMax?: number;
}) {
  const allX = finite(series.flatMap((item) => item.values.map((point) => point.x)));
  const allY = finite(series.flatMap((item) => item.values.map((point) => point.y)));
  if (allX.length === 0 || allY.length === 0) {
    return <EmptyChart>{title}: no samples</EmptyChart>;
  }

  const xMin = Math.min(...allX);
  const xMax = Math.max(...allX);
  const maxY = yMax ?? niceMax(Math.max(...allY));
  const plotW = WIDTH - PAD.left - PAD.right;
  const plotH = HEIGHT - PAD.top - PAD.bottom;
  const scaleX = (value: number) => PAD.left + ((value - xMin) / Math.max(xMax - xMin, 1)) * plotW;
  const scaleY = (value: number) => PAD.top + plotH - (value / Math.max(maxY, 1)) * plotH;
  const ticks = Array.from({ length: 5 }, (_, index) => (maxY / 4) * index);

  return (
    <figure className="chart">
      <figcaption>{title}</figcaption>
      <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img" aria-label={title}>
        {ticks.map((tick) => (
          <g key={tick}>
            <line className="grid-line" x1={PAD.left} x2={WIDTH - PAD.right} y1={scaleY(tick)} y2={scaleY(tick)} />
            <text className="axis-label" x={PAD.left - 10} y={scaleY(tick) + 4} textAnchor="end">
              {fmt(tick, yUnit)}
            </text>
          </g>
        ))}
        <line className="axis-line" x1={PAD.left} x2={WIDTH - PAD.right} y1={PAD.top + plotH} y2={PAD.top + plotH} />
        <text className="axis-label" x={PAD.left} y={HEIGHT - 10} textAnchor="start">
          {fmt(xMin, "s")}
        </text>
        <text className="axis-label" x={WIDTH - PAD.right} y={HEIGHT - 10} textAnchor="end">
          {fmt(xMax, "s")}
        </text>
        {series.map((item) => {
          const points = item.values
            .filter((point) => typeof point.y === "number" && Number.isFinite(point.y))
            .map((point) => `${scaleX(point.x)},${scaleY(point.y as number)}`)
            .join(" ");
          return <polyline key={item.label} className="chart-line" points={points} stroke={item.color} />;
        })}
      </svg>
      <ChartLegend series={series.map(({ label, color }) => ({ label, color }))} />
    </figure>
  );
}

export function BarChart({
  title,
  data,
  unit = "",
  max,
}: {
  title: string;
  data: BarDatum[];
  unit?: string;
  max?: number;
}) {
  const values = finite(data.map((item) => item.value));
  if (values.length === 0) {
    return <EmptyChart>{title}: no values</EmptyChart>;
  }
  const maxY = max ?? niceMax(Math.max(...values));
  const plotW = WIDTH - PAD.left - PAD.right;
  const plotH = HEIGHT - PAD.top - PAD.bottom;
  const barGap = 12;
  const barW = Math.max(20, (plotW - barGap * (data.length - 1)) / data.length);
  const scaleY = (value: number) => PAD.top + plotH - (value / Math.max(maxY, 1)) * plotH;
  const ticks = Array.from({ length: 5 }, (_, index) => (maxY / 4) * index);

  return (
    <figure className="chart">
      <figcaption>{title}</figcaption>
      <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img" aria-label={title}>
        {ticks.map((tick) => (
          <g key={tick}>
            <line className="grid-line" x1={PAD.left} x2={WIDTH - PAD.right} y1={scaleY(tick)} y2={scaleY(tick)} />
            <text className="axis-label" x={PAD.left - 10} y={scaleY(tick) + 4} textAnchor="end">
              {fmt(tick, unit)}
            </text>
          </g>
        ))}
        {data.map((item, index) => {
          const value = typeof item.value === "number" && Number.isFinite(item.value) ? item.value : 0;
          const x = PAD.left + index * (barW + barGap);
          const y = scaleY(value);
          const h = PAD.top + plotH - y;
          return (
            <g key={item.label}>
              <rect className="chart-bar" x={x} y={y} width={barW} height={h} fill={item.color ?? "#2f6fed"} />
              <text className="bar-value" x={x + barW / 2} y={Math.max(PAD.top + 12, y - 6)} textAnchor="middle">
                {fmt(value, unit)}
              </text>
              <text className="axis-label" x={x + barW / 2} y={HEIGHT - 10} textAnchor="middle">
                {item.label}
              </text>
            </g>
          );
        })}
      </svg>
    </figure>
  );
}

export function ChartLegend({ series }: { series: Array<{ label: string; color: string }> }) {
  return (
    <div className="chart-legend">
      {series.map((item) => (
        <span key={item.label}>
          <i style={{ backgroundColor: item.color }} />
          {item.label}
        </span>
      ))}
    </div>
  );
}
