"""Round-robin ticket assignment webhook service for Zammad."""
import hashlib
import hmac
import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

import zammad
from db import claim_next_user, init_db, set_last_user

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("rr.main")

SWEEP_INTERVAL = int(os.getenv("SWEEP_INTERVAL_SECONDS", "300"))  # 0 disables
SWEEP_MIN_AGE = int(os.getenv("SWEEP_MIN_AGE_SECONDS", "60"))
HEALTH_CACHE_SECONDS = 30

# Surfaced on /health so "silently stopped" becomes visible.
status = {
    "last_assignment_ok_at": None,
    "last_error": None,
    "last_error_at": None,
    "last_sweep_at": None,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class AssignmentError(Exception):
    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def _to_assignment_error(exc: zammad.ZammadError) -> AssignmentError:
    if isinstance(exc, zammad.ZammadAuthError):
        return AssignmentError(
            502,
            f"Zammad rejected the API token (HTTP {exc.status}); "
            "check token expiry, user state and permissions",
        )
    if isinstance(exc, zammad.ZammadTransientError):
        return AssignmentError(503, f"Zammad unavailable: {exc}")
    if isinstance(exc, zammad.ZammadNotFound):
        return AssignmentError(404, f"Not found in Zammad: {exc}")
    if isinstance(exc, zammad.ZammadConfigError):
        return AssignmentError(500, f"Server misconfiguration: {exc}")
    return AssignmentError(502, f"Zammad error: {exc}")


# --------------------------------------------------------------------------- core
def _assign(ticket_id: int, group_id: int, source: str, ticket: dict | None = None) -> dict:
    started = time.monotonic()
    if ticket is None:
        ticket = zammad.get_ticket(ticket_id)

    owner = ticket.get("owner_id")
    if owner and owner != zammad.UNASSIGNED_OWNER_ID:
        log.info("ticket=%s already owned by %s, skipping (source=%s)", ticket_id, owner, source)
        return {
            "ticket_id": ticket_id,
            "group_id": group_id,
            "owner_id": owner,
            "assigned_to": None,
            "skipped": "already_owned",
        }

    agents = zammad.get_group_agents(group_id)
    if not agents:
        raise AssignmentError(400, "No assignable agents found for group")

    agents = sorted(agents, key=lambda a: a["id"])
    by_id = {a["id"]: a for a in agents}
    agent_ids = list(by_id)

    # Atomic read+advance of the cursor, then try that agent and the following
    # ones in order until somebody accepts the ticket.
    first = claim_next_user(group_id, agent_ids)
    start = agent_ids.index(first)
    candidates = agent_ids[start:] + agent_ids[:start]

    attempts = 0
    for agent_id in candidates:
        attempts += 1
        try:
            updated = zammad.assign_ticket(ticket_id, agent_id)
        except zammad.ZammadRejected as exc:
            # Zammad refused this specific agent (e.g. 422); try the next one.
            log.warning("ticket=%s agent=%s rejected, trying next: %s", ticket_id, agent_id, exc)
            continue
        # Auth / transient / not-found errors propagate: trying other agents
        # cannot fix them and would only prolong the outage.

        if updated.get("owner_id") != agent_id:
            # Don't trust the PUT echo alone: confirm with a fresh GET, as the
            # original code did, and log both values to show which one is wrong.
            fresh = zammad.get_ticket(ticket_id)
            if fresh.get("owner_id") != agent_id:
                log.warning(
                    "ticket=%s agent=%s not applied: PUT owner_id=%s, GET owner_id=%s, "
                    "state_id=%s, trying next",
                    ticket_id, agent_id, updated.get("owner_id"),
                    fresh.get("owner_id"), fresh.get("state_id"),
                )
                continue
            log.warning(
                "ticket=%s agent=%s: PUT response was stale (owner_id=%s) but GET confirms the assignment",
                ticket_id, agent_id, updated.get("owner_id"),
            )

        if agent_id != first:
            set_last_user(group_id, agent_id)  # keep cursor on who really got it

        agent = by_id[agent_id]
        name = (
            f"{agent.get('firstname', '')} {agent.get('lastname', '')}".strip()
            or agent.get("login")
        )
        log.info(
            "assigned ticket=%s group=%s agent=%s source=%s attempts=%d ms=%d",
            ticket_id, group_id, agent_id, source, attempts,
            int((time.monotonic() - started) * 1000),
        )
        return {
            "ticket_id": ticket_id,
            "group_id": group_id,
            "owner_id": agent_id,
            "assigned_to": name,
        }

    raise AssignmentError(502, "No eligible agent accepted the ticket")


def run_assignment(ticket_id: int, group_id: int, source: str = "webhook", ticket=None) -> dict:
    try:
        result = _assign(ticket_id, group_id, source, ticket)
    except zammad.ZammadError as exc:
        err = _to_assignment_error(exc)
        _record_error(ticket_id, group_id, source, err)
        raise err from exc
    except AssignmentError as err:
        _record_error(ticket_id, group_id, source, err)
        raise
    status["last_assignment_ok_at"] = _now_iso()
    return result


def _record_error(ticket_id, group_id, source, err: AssignmentError):
    status["last_error"] = f"HTTP {err.status_code}: {err.detail}"[:300]
    status["last_error_at"] = _now_iso()
    log.error(
        "assignment FAILED ticket=%s group=%s source=%s http=%s detail=%s",
        ticket_id, group_id, source, err.status_code, err.detail,
    )


# --------------------------------------------------------------------------- sweeper
def _parse_ts(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def sweep_once() -> int:
    """Assign new tickets that no webhook call managed to assign. Returns count."""
    status["last_sweep_at"] = _now_iso()
    assigned = 0
    now = datetime.now(timezone.utc)
    for tid in zammad.search_unassigned_tickets():
        ticket = zammad.get_ticket(tid)
        owner = ticket.get("owner_id")
        if owner and owner != zammad.UNASSIGNED_OWNER_ID:
            continue
        created = _parse_ts(ticket.get("created_at"))
        if created and (now - created).total_seconds() < SWEEP_MIN_AGE:
            continue  # the webhook is probably still handling it
        group_id = ticket.get("group_id")
        if not group_id:
            continue
        try:
            result = run_assignment(tid, group_id, "sweeper", ticket)
        except AssignmentError as err:
            if err.status_code in (500, 502, 503):
                log.error("sweep aborted, Zammad problem: %s", err.detail)
                break
            continue
        if not result.get("skipped"):
            assigned += 1
    if assigned:
        log.warning("sweeper assigned %d ticket(s) the webhook missed", assigned)
    return assigned


def _sweeper_loop(stop: threading.Event):
    while not stop.wait(SWEEP_INTERVAL):
        try:
            sweep_once()
        except Exception:  # never let the thread die
            log.exception("sweep failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    stop = threading.Event()
    if SWEEP_INTERVAL > 0:
        threading.Thread(target=_sweeper_loop, args=(stop,), daemon=True, name="sweeper").start()
        log.info("sweeper enabled, every %ss", SWEEP_INTERVAL)
    yield
    stop.set()


app = FastAPI(lifespan=lifespan)


# --------------------------------------------------------------------------- HTTP
def verify_hmac(signature_header: str | None, body: bytes) -> bool:
    """Verify Zammad's `X-Hub-Signature: sha1=<hex>` header."""
    secret = os.getenv("HMAC_SECRET")
    if not secret:
        raise HTTPException(500, "Server misconfiguration: HMAC_SECRET not set")
    if not signature_header:
        return False
    if signature_header.startswith("sha1="):
        signature_header = signature_header[5:]
    computed = hmac.new(secret.encode(), body, hashlib.sha1).hexdigest()
    return hmac.compare_digest(computed.encode(), signature_header.encode())


@app.post("/assignment")
async def assign(request: Request):
    body = await request.body()
    if not verify_hmac(request.headers.get("X-Hub-Signature"), body):
        raise HTTPException(401, "Invalid signature")

    try:
        data = json.loads(body)
        ticket_id = int(data["ticket_id"])  # Zammad sends "#{ticket.id}" as a string
        group_id = int(data["group_id"])
    except (ValueError, TypeError, KeyError):
        raise HTTPException(400, "Invalid JSON body. Required: ticket_id, group_id")

    try:
        # Blocking HTTP/SQLite work runs in the threadpool so one slow Zammad
        # call can never freeze the event loop (and /health with it).
        return await run_in_threadpool(run_assignment, ticket_id, group_id, "webhook")
    except AssignmentError as err:
        raise HTTPException(err.status_code, err.detail)


_health = {"at": 0.0, "ok": False, "detail": "not checked yet"}


@app.get("/health")
def health():
    """Readiness: verifies the Zammad token actually works (cached 30s)."""
    now = time.monotonic()
    if now - _health["at"] > HEALTH_CACHE_SECONDS:
        try:
            me = zammad.get_me()
            if me.get("active") is False:
                _health.update(ok=False, detail="token user is inactive")
            else:
                _health.update(ok=True, detail="ok")
        except zammad.ZammadError as exc:
            _health.update(ok=False, detail=f"{type(exc).__name__} (HTTP {exc.status})")
        _health["at"] = now
        if not _health["ok"]:
            log.error("health check failed: %s", _health["detail"])

    body = {"status": "ok" if _health["ok"] else "degraded", "zammad": _health["detail"], **status}
    return JSONResponse(body, status_code=200 if _health["ok"] else 503)


@app.get("/livez")
def livez():
    """Liveness: the process is up (does not call Zammad)."""
    return {"status": "ok"}
