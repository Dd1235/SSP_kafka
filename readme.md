# Kafka Bench Explorer

Final course project for CSE-732 Software and Systems Performance.

The final benchmark runner lives in `4/`. The paper source, generated plots,
and canonical result JSON files live in `paper/`.

## Public dashboard

The packaged frontend is a static GitHub Pages site:

https://dd1235.github.io/SSP_kafka/

It reads the existing benchmark reports from `paper/data/*.json`, builds a
static `/data/index.json`, and exposes the raw result JSON files for download.
It does not run Kafka in the browser.

## Local reproduction

```bash
cd 4
./run.sh start
./run.sh bench
./run.sh sweep-compression
```

See `4/README.md` for the full runner guide and scenario list.

## Frontend development

```bash
cd site
pnpm install
pnpm dev
pnpm build
```

`pnpm build` regenerates `site/public/data/` from `paper/data/` before building
the Vite app.
