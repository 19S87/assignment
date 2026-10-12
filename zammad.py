"""Zammad REST client used by the round-robin assignment service.

Design notes
- Every call has a connect+read timeout. Note: `requests` timeouts do not cover
  DNS resolution, so a hung resolver can still stall a single call.
- Errors are classified so callers can react correctly:
    ZammadAuthError       401/403  -> permanent, never retried, surfaced loudly
    ZammadTransientError  network / 429 / 5xx / bad JSON -> retried with backoff
    ZammadNotFound        404
    ZammadRejected        other 4xx (e.g. 422 "agent can't own this ticket")
    ZammadConfigError     missing URL / token
- The API token is read on every call. If you provide it via
  ZAMMAD_API_TOKEN_FILE (e.g. a Docker/K8s secret) a rotated token is picked up
  without restarting the service.
"""
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime

import requests

log = logging.getLogger("rr.zammad")

TIMEOUT = (
    float(os.getenv("ZAMMAD_CONNECT_TIMEOUT", "5")),
    float(os.getenv("ZAMMAD_READ_TIMEOUT", "15")),
)
MAX_ATTEMPTS = int(os.getenv("ZAMMAD_MAX_ATTEMPTS", "3"))
BACKOFF_SECONDS = float(os.getenv("ZAMMAD_BACKOFF_SECONDS", "1"))
AGENT_CACHE_TTL = float(os.getenv("AGENT_CACHE_TTL_SECONDS", "60"))
AGENT_ROLE_NAMES = {
    n.strip().lower()
    for n in os.getenv("AGENT_ROLE_NAMES", "Agent").split(",")
    if n.strip()
}
# OPTIONAL filter, off by default. If set (e.g. "full,change"), members whose
# *direct* group access has none of these levels are skipped. Leave unset if
# agents get access through roles or parent groups: that isn't visible here.
OWNER_ACCESS = {
    a.strip().lower()
    for a in os.getenv("OWNER_GROUP_ACCESS", "").split(",")
    if a.strip()
}
# Zammad's "-" system user (tickets with no owner have owner_id == 1).
UNASSIGNED_OWNER_ID = int(os.getenv("ZAMMAD_UNASSIGNED_OWNER_ID", "1"))


# --------------------------------------------------------------------------- errors
class ZammadError(Exception):
    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status


class ZammadConfigError(ZammadError):
    pass


class ZammadAuthError(ZammadError):
    pass


class ZammadTransientError(ZammadError):
    pass


class ZammadNotFound(ZammadError):
    pass


class ZammadRejected(ZammadError):
    pass


# --------------------------------------------------------------------------- transport
_local = threading.local()


def _session() -> requests.Session:
    # One Session per worker thread: connection reuse without sharing state.
    s = getattr(_local, "session", None)
    if s is None:
        s = requests.Session()
        _local.session = s
    return s


def _base_url() -> str:
    url = os.getenv("ZAMMAD_URL", "").strip().rstrip("/")
    if not url:
        raise ZammadConfigError("ZAMMAD_URL is not set")
    return url


def _token() -> str:
    path = os.getenv("ZAMMAD_API_TOKEN_FILE")
    if path:
        try:
            with open(path, encoding="utf-8") as fh:
                token = fh.read().strip()
        except OSError as exc:
            raise ZammadConfigError(f"cannot read ZAMMAD_API_TOKEN_FILE: {exc}") from exc
    else:
        token = os.getenv("ZAMMAD_API_TOKEN", "").strip()
    if not token:
        raise ZammadConfigError("ZAMMAD_API_TOKEN is not set")
    return token


def _request(method: str, path: str, **kwargs):
    """Single attempt. Returns parsed JSON or raises a classified ZammadError."""
    url = f"{_base_url()}{path}"
    headers = {
        "Authorization": f"Token token={_token()}",
        "Content-Type": "application/json",
    }
    started = time.monotonic()
    try:
        resp = _session().request(method, url, headers=headers, timeout=TIMEOUT, **kwargs)
    except requests.RequestException as exc:
        raise ZammadTransientError(f"{method} {path}: {type(exc).__name__}: {exc}") from exc

    elapsed_ms = int((time.monotonic() - started) * 1000)
    status = resp.status_code
    if status < 400:
        try:
            return resp.json()
        except ValueError as exc:
            raise ZammadTransientError(
                f"{method} {path}: HTTP {status} but body is not JSON "
                f"(proxy/login page?): {resp.text[:120]!r}",
                status,
            ) from exc

    snippet = resp.text[:200].replace("\n", " ")
    msg = f"{method} {path}: HTTP {status} in {elapsed_ms}ms: {snippet}"
    if status in (401, 403):
        raise ZammadAuthError(msg, status)
    if status == 404:
        raise ZammadNotFound(msg, status)
    if status == 429 or status >= 500:
        raise ZammadTransientError(msg, status)
    raise ZammadRejected(msg, status)


def _call(method: str, path: str, **kwargs):
    """_request with exponential backoff for transient failures only."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return _request(method, path, **kwargs)
        except ZammadTransientError as exc:
            if attempt == MAX_ATTEMPTS:
                raise
            delay = BACKOFF_SECONDS * (2 ** (attempt - 1))
            log.warning(
                "transient Zammad error (attempt %d/%d), retrying in %.1fs: %s",
                attempt, MAX_ATTEMPTS, delay, exc,
            )
            time.sleep(delay)


# --------------------------------------------------------------------------- API
def get_me() -> dict:
    """Cheap authenticated call used by /health. Single attempt, no retries."""
    return _request("GET", "/api/v1/users/me")


def get_ticket(ticket_id: int) -> dict:
    return _call("GET", f"/api/v1/tickets/{ticket_id}")


def assign_ticket(ticket_id: int, owner_id: int) -> dict:
    """Set the owner and return the updated ticket (Zammad echoes it back)."""
    return _call("PUT", f"/api/v1/tickets/{ticket_id}", json={"owner_id": owner_id})


def search_unassigned_tickets(limit: int = 100) -> list:
    """Ids of new tickets that still have no owner (used by the sweeper)."""
    query = os.getenv(
        "SWEEP_QUERY", f"state.name:new AND owner_id:{UNASSIGNED_OWNER_ID}"
    )
    data = _call("GET", "/api/v1/tickets/search", params={"query": query, "limit": limit})
    if isinstance(data, dict):
        items = data.get("tickets", [])
    else:
        items = data
    ids = []
    for item in items or []:
        tid = item.get("id") if isinstance(item, dict) else item
        if isinstance(tid, int):
            ids.append(tid)
    return ids


# --------------------------------------------------------------------------- agents
def _parse_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except ValueError:
        return None


def _is_out_of_office(user: dict) -> bool:
    if not user.get("out_of_office"):
        return False
    today = date.today()
    start = _parse_date(user.get("out_of_office_start_at"))
    end = _parse_date(user.get("out_of_office_end_at"))
    if start and today < start:
        return False
    if end and today > end:
        return False
    return True


_warned: set = set()


def _lacks_owner_access(user: dict, group_id: int) -> bool:
    """
    True when Zammad lists this user's access to the group and it is too low to
    own tickets (e.g. only ["overview", "read"]). Zammad silently ignores an
    owner change to such a user: HTTP 200, owner_id unchanged.

    Access granted through a role (not listed per user) can't be seen here, so
    those users are kept; the failover loop handles any that Zammad refuses.
    """
    if not OWNER_ACCESS:
        return False  # opt-in only: the user object doesn't show role/inherited access
    access = (user.get("group_ids") or {}).get(str(group_id))
    if access is None:
        return False
    return not (set(access) & OWNER_ACCESS)


def _exclusion_reason(user: dict, group_id: int):
    """None if the user can be assigned tickets, else a short reason."""
    if _lacks_owner_access(user, group_id):
        return f"direct group access {sorted(user['group_ids'][str(group_id)])} has none of {sorted(OWNER_ACCESS)}"
    if not user.get("active"):
        return "inactive"
    roles = user.get("roles")  # present because we request expand=true
    if roles is not None:
        if not ({str(r).lower() for r in roles} & AGENT_ROLE_NAMES):
            return f"no agent role (roles={roles})"
    elif not user.get("role_ids"):
        return "no roles"
    if _is_out_of_office(user):
        return "out of office"
    return None


def _is_assignable(user: dict, group_id: int) -> bool:
    return _exclusion_reason(user, group_id) is None


_cache: dict = {}
_cache_lock = threading.Lock()


def get_group_agents(group_id: int) -> list:
    """
    Active, non-out-of-office users with an agent role in the group.

    Results are cached for AGENT_CACHE_TTL_SECONDS so a burst of tickets doesn't
    cost 1 + N API calls each. Members are fetched concurrently.
    """
    now = time.monotonic()
    with _cache_lock:
        hit = _cache.get(group_id)
        if hit and now - hit[0] < AGENT_CACHE_TTL:
            return list(hit[1])

    group = _call("GET", f"/api/v1/groups/{group_id}")
    user_ids = group.get("user_ids") or []
    if not user_ids:
        return []

    def fetch(uid):
        try:
            return _call("GET", f"/api/v1/users/{uid}", params={"expand": "true"})
        except ZammadNotFound:
            return None  # deleted user
        except ZammadAuthError as exc:
            return exc  # decided below

    with ThreadPoolExecutor(max_workers=min(8, len(user_ids))) as pool:
        results = list(pool.map(fetch, user_ids))

    auth_errors = [r for r in results if isinstance(r, ZammadAuthError)]
    if auth_errors and len(auth_errors) == len(user_ids):
        raise auth_errors[0]  # token is bad, not just one unreadable user
    for err in auth_errors:
        log.warning("skipping a group member the token may not read: %s", err)

    agents, excluded = [], {}
    for u in results:
        if not isinstance(u, dict):
            continue
        reason = _exclusion_reason(u, group_id)
        if reason is None:
            agents.append(u)
        else:
            excluded[u.get("id")] = reason
    # Zammad's group.user_ids lists only members with *direct* access; agents who
    # get access through a role or a parent group are not in it.
    log.info(
        "group=%s direct members=%d eligible=%s excluded=%s",
        group_id, len(user_ids), [a["id"] for a in agents], excluded,
    )
    if not agents:
        log.error("group=%s: none of the %d direct member(s) is assignable", group_id, len(user_ids))
    if agents:
        with _cache_lock:
            _cache[group_id] = (now, agents)
    return list(agents)
