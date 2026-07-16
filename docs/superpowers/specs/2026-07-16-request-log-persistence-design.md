# Request Log Persistence Design

> **Duration:** Tiap request disimpan di SQLite, dashboard refresh gak ilang.
> **Filter:** 24h / 3d / 7d / 15d / 30d — pilih di dropdown.
> **Live:** WebSocket tetap update realtime seperti sekarang.

## Data Model

Satu tabel baru `request_logs` di file SQLite yang sama (`~/.9limiter/stats.db`):

```sql
CREATE TABLE IF NOT EXISTS request_logs (
    id         TEXT PRIMARY KEY,       -- UUID dari proxy.rs
    api_key    TEXT NOT NULL,
    owner      TEXT NOT NULL DEFAULT '',
    model      TEXT NOT NULL,
    method     TEXT NOT NULL,
    path       TEXT NOT NULL,
    status     INTEGER NOT NULL DEFAULT 0,
    latency_ms INTEGER NOT NULL DEFAULT 0,
    timestamp  TEXT NOT NULL            -- RFC3339, timezone-aware
);
```

Filter range pakai lexicographic `timestamp` comparison (sama seperti stats `usage.hour`).

Retention: 2 bulan. Cleanup dijalankan bersamaan dengan cleanup stats (tiap jam via `timer` di background writer). Query:

```sql
DELETE FROM request_logs WHERE timestamp < datetime('now', '-60 days')
```

## Backend Architecture

### Komponen Baru: `RequestLogCollector`

Ikut pola `StatsCollector` yang sudah ada:

```
proxy.rs (request end event — setelah dapat status+latency)
  → mpsc::UnboundedSender<RequestLog> 
  → background writer flush tiap 5 detik
  → INSERT OR IGNORE (karena id primary key)
  ↔ WS broadcast tetap jalan seperti sekarang
```

Karena kedua writer (stats + request_logs) pakai file SQLite yang sama, cukup satu `Connection::open()` per flush — SQLite handle concurrent WAL reads dengan baik.

### `RequestLog` Struct

```rust
struct RequestLog {
    id: String,
    api_key: String,
    owner: String,
    model: String,
    method: String,
    path: String,
    status: u16,
    latency_ms: u64,
    timestamp: String,
}
```

### Endpoint Baru

```
GET /api/requests?range=24h
```

Range value: `24h | 3d | 7d | 15d | 30d`.

Backend parse range jadi timestamp `start` (NOW - range), `end` (NOW). Query:

```sql
SELECT * FROM request_logs
WHERE timestamp >= ?1 AND timestamp <= ?2
ORDER BY timestamp DESC
LIMIT 200
```

Return JSON array of request objects.

### Sender Disimpan di AppState

```rust
request_log_tx: mpsc::UnboundedSender<RequestLog>,
```

Send dari `proxy.rs` setelah request selesai (sebelum atau sesudah WS `RequestEnd` event — urutan gak penting).

## Frontend

### Filter Dropdown

Di `top-bar` dashboard.html, tambah elemen:

```
[24 Hours ▾]   (dropdown, bukan select native biar stylable)
```

Opsi: 24h (default) / 3d / 7d / 15d / 30d.

### Initial Load

Pas WebSocket connect (atau pas halaman pertama load), fetch:

```js
fetch('/api/requests?range=24h')
  .then(r => r.json())
  .then(requests => { /* populate table, replace existing */ })
```

Populate table dengan data dari API. Overwrite semua row yang ada.

### Live Update

WS tetap push `request` (RequestStartEvent) dan `request_end` (RequestEndEvent) seperti sekarang. Handler `renderRow` / `updateRow` jalan normal.

### Interaksi

1. User ganti filter dropdown → fetch ulang `/api/requests?range=X` → replace table
2. WS tetap nambah row baru di atas (prepend)
3. Row count cap tetap 200 (MAX_LOG)

## Testing

- [ ] `test_request_log_flush`: insert batch, query by range, match count
- [ ] `test_request_log_range_parsing`: 24h, 3d, 7d, 15d, 30d → correct SQL start/end
- [ ] `test_request_log_empty`: query gak ada data → empty array
- [ ] `test_request_log_cleanup`: insert old data, cleanup, query → old gone
