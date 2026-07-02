# Domo API endpoints (Mode B — live pull)

Only for Mode B (credentials present). This engagement defaults to Mode A (provided export), so
this is a stub for later. Endpoints below were **verified** against a real Domo workspace during
Step-1 extraction work — the OAuth-only `/v1/*` paths from Domo's public docs return an HTML login
page when hit with a developer token, so use the internal `/api/*` paths with a dev token.

| Purpose | Endpoint | Notes |
|---|---|---|
| List dataflows | `GET /api/dataprocessing/v1/dataflows` | ✅ |
| Dataflow definition | `GET /api/dataprocessing/v2/dataflows/{id}?validationType=PREVIEW` | ✅ the tile DAG |
| List datasets | `GET /api/data/v3/datasources?limit=&offset=` | `{dataSources:[{id,name,...}]}` |
| List streams | `GET /api/data/v1/streams?limit=&offset=` | |
| Beast modes | `POST /api/query/v1/functions/template` (likely) | `/api/content/v2/beast-modes` 404s |

Auth: developer token in the standard Domo auth header. A dev token is typically **read-only** on
data APIs. For dataset create/load (test fixtures only) an OAuth API client is needed
(`POST api.domo.com/oauth/token?grant_type=client_credentials&scope=data%20user`, Basic auth).

DomoStats/governance (lineage, schedules, row counts, usage) come from the governance datasets —
wire these into the normalized graph's `schedule` and completeness fields when available.

> Full endpoint notes and the read-only/OAuth split live in the `domo-migration` repo's Step-1
> extraction notebook (`01_extract_domo_inventory.py`).
