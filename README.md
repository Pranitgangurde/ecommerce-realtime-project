# Real-Time E-Commerce Analytics Pipeline

[![CI](https://github.com/yourname/ecommerce-realtime-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/yourname/ecommerce-realtime-pipeline/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

End-to-end Lambda architecture pipeline that ingests synthetic e-commerce
clickstream events through Kafka, processes them with Spark Structured Streaming
and Apache Flink, lands them in an Iceberg lakehouse, transforms them with dbt,
and serves them through Metabase, Grafana, and FastAPI.

![Architecture](docs/images/architecture-diagram.png)

## Quickstart

```bash
git clone https://github.com/yourname/ecommerce-realtime-pipeline.git
cd ecommerce-realtime-pipeline
cp .env.example .env
make install
make up                    # Starts Kafka, Postgres, Metabase
make produce &             # Begins generating events (background)
make consume               # Spark reads from Kafka, writes to Postgres
```

Open Metabase at http://localhost:3000 to see live dashboards.

## Architecture

[... detailed architecture section, link to docs/architecture.md ...]

## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Apache Kafka (KRaft mode) |
| ... | ... |

## Project Phases

- [x] **Phase 1** — MVP: Kafka → Spark → Postgres → Metabase
- [ ] **Phase 2** — Lakehouse: MinIO + Iceberg + medallion + dbt
- [ ] **Phase 3** — Streaming: Flink jobs + Grafana real-time dashboards
- [ ] **Phase 4** — Quality: Airflow + Great Expectations + Debezium CDC
- [ ] **Phase 5** — Production: Terraform + GitHub Actions + observability

## Local Development

[... detailed setup, troubleshooting ...]

## Architecture Decisions

See [docs/adr/](docs/adr/) for the rationale behind every major technical choice.