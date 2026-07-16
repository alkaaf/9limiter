# Rate Limiter — Catatan Eksplorasi

## Referensi: LiteLLM

Repo: https://github.com/BerriAI/litellm

### Opsi Install
- pip: `pip install litellm`
- Docker: `ghcr.io/berriai/litellm:main-latest`
- Docker Compose: ada `docker-compose.yml`
- Helm: deploy ke K8s
- Terraform: AWS ECS / GCP Cloud Run
- Render: one-click via `render.yaml`

### Rate Limiting — 2 Layer

| Layer | Limit | Mekanisme |
|-------|-------|-----------|
| Per deployment (model instance) | `rpm`, `tpm` di config YAML | Redis counter per menit, router pilih deployment paling low usage |
| Per virtual key | `rpm_limit`, `tpm_limit`, `max_budget`, `max_parallel_requests` | Diset pas create key, enforce di proxy middleware |

### Cara Kerja Rate Limiter (di `router_strategy/lowest_tpm_rpm_v2.py`)
- Key = `{model_id}:{deployment}:rpm:{HH-MM}`
- Cek cache lokal dulu → kalau kena limit langsung 429
- Kalau aman, `INCR` di Redis (atomic, multi-instance safe)
- Routing pilih deployment dengan TPM paling rendah
- Cooldown: deployment error/rate limited otomatis di-sembunyikan dari routing sementara
- Redis `mget` untuk batch check RPM/TPM semua deployment

### UI Dashboard
- Stack: Next.js 16, React 18, Ant Design, Tailwind, Tremor, TanStack, Recharts
- Halaman: Key management, spend tracking, model management, AI Hub, MCP integration
- **Dari UI bisa:** per-key, per-team, per-user rate limits & budget
- **Gak bisa dari UI:** per-deployment (model) RPM/TPM — cuma dari config YAML

### Routing Strategy
- `usage-based-routing-v2` — pilih deployment dengan TPM/RPM terendah
- Bisa juga: lowest latency, lowest cost, least busy, simple shuffle, tag-based

## Keputusan: Bikin Sendiri vs LiteLLM

**Pake LiteLLM kalau butuh:**
- Unified API format (1 endpoint buat semua provider)
- Admin UI buat manage keys & spend
- Multi-provider failover / fallback
- Routing strategy (lowest latency, cost-based)

**Bikin sendiri kalau:**
- Udah punya router sendiri
- Cuma butuh rate limiting + budget tracking
- Redis udah jalan
- Gak mau dependensi berat + overhead proxy gateway

Inti rate limiter cukup 50 baris:
```
key = "{user_id}:rpm:{current_minute}"
count = redis.incr(key, 1, ttl=60)
if count > limit: return 429
```

(Source: hasil eksplorasi repo LiteLLM commit terbaru, Juli 2026)
