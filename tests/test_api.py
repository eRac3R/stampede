from app.config import Settings
from app.ratelimit import RateLimiter

CARD = "4242424242424242"


async def admitted(client, user):
    body = (await client.post("/events/test-drop/queue", headers={"X-User-Id": user})).json()
    assert body["admitted"]
    return {"X-User-Id": user, "X-Admission-Pass": body["pass"]}


async def test_full_purchase_flow_over_http(client, event):
    h = await admitted(client, "alice-1")
    r = await client.post("/events/test-drop/holds", json={"seat_id": "A1"}, headers=h)
    assert r.status_code == 200 and r.json()["status"] == "HELD"
    r = await client.post("/events/test-drop/checkout", json={"seat_id": "A1", "card": CARD},
                          headers=h | {"Idempotency-Key": "http-key-0001"})
    assert r.status_code == 200, r.text
    info = (await client.get("/events/test-drop")).json()
    assert info["seats"]["A1"] == "sold" and info["counts"]["sold"] == 1


async def test_seat_picking_requires_a_valid_pass(client, event):
    r = await client.post("/events/test-drop/holds", json={"seat_id": "A1"}, headers={"X-User-Id": "alice-1"})
    assert r.status_code == 403
    bob = await admitted(client, "bob-123")
    r = await client.post("/events/test-drop/holds", json={"seat_id": "A1"},
                          headers={"X-User-Id": "alice-1", "X-Admission-Pass": bob["X-Admission-Pass"]})
    assert r.status_code == 403  # Bob's pass does not work for Alice


async def test_errors_are_clear(client, event):
    a, b = await admitted(client, "alice-1"), await admitted(client, "bob-123")
    await client.post("/events/test-drop/holds", json={"seat_id": "A1"}, headers=a)
    r = await client.post("/events/test-drop/holds", json={"seat_id": "A1"}, headers=b)
    assert r.status_code == 409 and r.json()["detail"]["code"] == "TAKEN"
    assert (await client.post("/events/test-drop/holds", json={"seat_id": "Z9"}, headers=b)).status_code == 404
    assert (await client.get("/events/nope")).status_code == 404
    assert (await client.post("/events/test-drop/queue", headers={"X-User-Id": "x"})).status_code == 400


async def test_resync_repairs_redis_from_postgres(client, svc, event):
    h = await admitted(client, "alice-1")
    await client.post("/events/test-drop/holds", json={"seat_id": "A1"}, headers=h)
    await client.post("/events/test-drop/checkout", json={"seat_id": "A1", "card": CARD},
                      headers=h | {"Idempotency-Key": "http-key-0001"})
    await svc.redis.flushdb()
    assert (await client.get("/events/test-drop")).json()["seats"]["A1"] == "free"  # Redis forgot
    assert (await client.post("/events/test-drop/resync")).json() == {"sold": 1}
    assert (await client.get("/events/test-drop")).json()["seats"]["A1"] == "sold"


async def test_token_bucket_blocks_bursts_then_refills(svc):
    limiter = RateLimiter(svc.redis, capacity=3, refill_per_sec=20)
    assert [await limiter.allow("ip-1") for _ in range(4)] == [True, True, True, False]
    assert await limiter.allow("ip-2")  # buckets are per client
    import asyncio
    await asyncio.sleep(0.1)
    assert await limiter.allow("ip-1")


async def test_metrics_endpoint(client, event):
    h = await admitted(client, "alice-1")
    await client.post("/events/test-drop/holds", json={"seat_id": "A1"}, headers=h)
    assert "stampede_hold_held_total 1" in (await client.get("/metrics")).text


def test_settings_never_read_real_env_in_tests():
    assert Settings().redis_url.startswith("redis://localhost")
