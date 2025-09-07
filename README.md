# Credits API — P2P-Friendly, Concurrency-Safe

A Django service that demonstrates a **credit-based usage model** built for **high concurrency**.  
Key ideas:
- **Optimistic, row-locked accounting** with `F()` updates.
- **Idempotency** for at-least-once request semantics.
- **Decorator-based metering** that charges **only successful requests**.
- **Sweeper** (Celery Beat) that auto-reverses stale reservations (crash-safety).

---

## ⚡ TL;DR (Quick Start)

```bash
cp env.example .env
docker compose up --build
```

Create test data (in another terminal):
```bash
docker compose exec web python manage.py shell
```

```py
from credits.models import ApiKey, Wallet
wallet = Wallet.objects.create(name="demo", balance=20)
api_key = ApiKey.objects.create(wallet=wallet, key=ApiKey.generate(), label="dev")
print(api_key.key)  # copy this value
```

Then test (inside container `web` shell or your host if you have curl):
```bash
API_KEY=<paste value>

curl -s -H "X-API-Key: $API_KEY" http://localhost:8000/api/balance
# → {"wallet":"demo","balance":20}

curl -s -H "X-API-Key: $API_KEY" http://localhost:8000/api/echo
# → {"ok": true, "message": "Service did its job!"}

curl -s -H "X-API-Key: $API_KEY" http://localhost:8000/api/balance
# → {"wallet":"demo","balance":19}
```

You can also create a superuser and inspect all objects in **Django Admin**:
```bash
docker compose exec web python manage.py createsuperuser
# open http://127.0.0.1:8000/admin/
# Credits → Api keys, Credit transactions, Wallets
```

---

**Compose services:**
- `web` – Django dev server
- `worker` – Celery worker (consumes default queue)
- `beat` – Celery Beat (schedules the sweeper every 60s)
- `db` – Postgres
- `redis` – Redis broker

---

## 🔐 Auth

Middleware resolves `request.wallet` from header **`X-API-Key`** (active keys only).  
Idempotency (optional) via header **`Idempotency-Key`** for metered endpoints.

---

## 📡 API

### `GET /api/balance`
Returns the wallet name and current balance for the provided API key.
```json
{"wallet":"demo","balance":19}
```

### `GET /api/echo`  (metered)
Charges **1 credit** on success (2xx/3xx).  
Returns a hello payload.

Headers:
```
X-API-Key: <key>
Idempotency-Key: <optional-string>
```

---

## 🧪 Tests

Run all tests:
```bash
docker compose exec web python manage.py test
# Ran 51 tests in 5.698s
# OK
```

### What’s covered

- **Services**
  - `TopUpServiceTests` (unit)
  - `TopUpConcurrencyTests` (concurrency via `TransactionTestCase`)
  - `ReverseCreditsTests` (unit)
  - `ReserveCreditsConcurrencyTests` (concurrency)
  - `CommitReservationTests` (unit)
  - `CommitReservationConcurrencyTests` (concurrency)
  - `ReverseReservationUnitTests` / `ReverseReservationConcurrencyTests`
  - `SweepStaleReservationsTests` / `SweepStaleReservationsConcurrencyTests`
- **API (integration)**
  - `credits/tests/test_credits_api.py`
    - `BalanceApiTests`
    - `EchoApiTests` (+ decorator idempotency/commit/reverse behavior)

> **Note about concurrency tests:** They use real threads and `TransactionTestCase` to commit. Each worker thread calls `connection.close()` to avoid lingering DB sessions that would block test DB teardown.

---

## 🧹 Celery sweeper (stale reservations)

Task definition:
```py
@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 5},
    acks_late=True,
    time_limit=60,
)
def sweep_pending_tx(self, chunk_size: int | None = 500) -> int:
    return sweep_stale_reservations(chunk_size=chunk_size)
```

Beat schedule (every 60s):
```py
app.conf.beat_schedule = {
    "sweep-stale-reservations": {
        "task": "credits.tasks.sweep_pending_tx",
        "schedule": schedule(60.0),
    }
}
```

**Logs example:**
```
beat    | ... Scheduler: Sending due task sweep-stale-reservations (credits.tasks.sweep_pending_tx)
worker  | Task credits.tasks.sweep_pending_tx[...] received
worker  | credits.services.sweep_stale_reservations sweep.done total=0
worker  | Task credits.tasks.sweep_pending_tx[...] succeeded in 0.024458334082737565s: 0
```


### Seed **brand-new** stale reservations

Script creates fresh reservations and backdates them:

```py
# credits/scripts/make_stale.py
def make_stale(wallet_id: int, *, count=20, amount=1, seconds_ago=600, ensure_funds=True, tag="make_stale"):
    ...
```

Run it:
```bash
docker compose exec web python manage.py shell -c \
"from credits.scripts.make_stale import make_stale; make_stale(wallet_id=1, count=25, seconds_ago=900)"
```

**Logs example:**
```
beat    | ... Scheduler: Sending due task sweep-stale-reservations (credits.tasks.sweep_pending_tx)
worker  | Task credits.tasks.sweep_pending_tx[...] received
worker  | credits.services.sweep_stale_reservations: sweep.done total=25
worker  | Task credits.tasks.sweep_pending_tx[...] succeeded in 0.02s: 25
```

---

## ✅ Contracts (for quick manual validation)

- `GET /api/balance` (requires `X-API-Key`) → wallet & integer `balance`.
- `GET /api/echo` (requires `X-API-Key`) → charges **1** on success.
- Setting `Idempotency-Key` makes repeated calls charge **once**.

Last transaction example for a successful `/api/echo`:
```py
from credits.models import CreditTransaction
tx = CreditTransaction.objects.last()
vars(tx)
# {
#  'id': 2109, 'wallet_id': 2, 'delta': -1,
#  'tx_type': 'debit', 'tx_status': 'committed',
#  'idempotency_key': None, 'request_id': '...', 'note': 'api-request',
#  'created_at': datetime(..., tzinfo=UTC)
# }
```

---
