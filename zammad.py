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
- Who is an assignable agent for a group, with no dependency on a role called
  "Agent":
    * GROUP_ROLES='{"46": [10]}' (role ids or names) -> the pool is exactly the
      holders of those roles, e.g. "IT L1 Application Agent" for group 46;
    * groups without an entry -> their direct members (previous behaviour).
  Either way candidates must be active, not out of office, and hold a role with
  the `ticket.agent` permission (checked when roles/permissions are readable).
"""
import json
import logging
import os
import threading
import time
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
ROLE_CACHE_TTL = float(os.getenv("ROLE_CACHE_TTL_SECONDS", "300"))
USERS_PER_PAGE = int(os.getenv("USERS_PER_PAGE", "200"))
MAX_USER_PAGES = 50
# OPTIONAL filter, off by default. If set (e.g. "full,change"), candidates whose
# known group access (direct + via roles) has none of these levels are skipped.
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


# --------------------------------------------------------------------------- caching
_cache: dict = {}
_cache_lock = threading.Lock()
_warned: set = set()


def _cached(key, ttl: float, loader):
    now = time.monotonic()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
    value = loader()
    with _cache_lock:
        _cache[key] = (now, value)
    return value


def _warn_once(key: str, message: str):
    if key not in _warned:
        _warned.add(key)
        log.warning(message)


# --------------------------------------------------------------------------- directory
def _load_roles():
    """All roles, or None if the token may not read them (needs admin rights)."""
    try:
        data = _call("GET", "/api/v1/roles")
    except (ZammadAuthError, ZammadNotFound) as exc:
        _warn_once(
            "roles",
            f"cannot read /api/v1/roles ({exc}); role-based group members can only be "
            "found with GROUP_ROLES=<role ids>, otherwise only direct members are used",
        )
        return None
    return [r for r in data if isinstance(r, dict)]


def _load_permissions():
    """{permission id: name}, or None if unreadable."""
    try:
        data = _call("GET", "/api/v1/permissions")
    except (ZammadAuthError, ZammadNotFound) as exc:
        _warn_once(
            "perms",
            f"cannot read /api/v1/permissions ({exc}); not verifying the ticket.agent "
            "permission, relying on group access only",
        )
        return None
    return {p["id"]: p.get("name") for p in data if isinstance(p, dict) and "id" in p}


def _load_users():
    """Every user, via the paginated index."""
    users, seen = [], set()
    for page in range(1, MAX_USER_PAGES + 1):
        batch = _call("GET", "/api/v1/users", params={"page": page, "per_page": USERS_PER_PAGE})
        if not isinstance(batch, list) or not batch:
            break
        new = [u for u in batch if isinstance(u, dict) and u.get("id") not in seen]
        if not new:
            break  # server ignored pagination and repeated the first page
        for u in new:
            seen.add(u["id"])
            users.append(u)
    if users and not any("role_ids" in u for u in users):
        log.error("users list has no role_ids field; role-based membership cannot be resolved")
    return users


def _agent_role_ids(roles, perms):
    """Ids of roles holding ticket.agent, or None when that can't be determined."""
    if roles is None or perms is None:
        return None
    if not any("permission_ids" in r for r in roles):
        return None
    agent_perm_ids = {pid for pid, name in perms.items() if name == "ticket.agent"}
    return {r["id"] for r in roles if set(r.get("permission_ids") or []) & agent_perm_ids}


def _group_access(obj: dict, group_id: int):
    """
    Access levels `obj` (a user or role) has on the group: a list (possibly empty
    when the level isn't exposed), or None when it has no access at all.
    """
    gids = obj.get("group_ids")
    if isinstance(gids, dict):
        acc = gids.get(str(group_id))
        return None if acc is None else list(acc)
    if isinstance(gids, list) and str(group_id) in {str(g) for g in gids}:
        return []
    return None


def _explicit_roles(group_id: int):
    """GROUP_ROLES='{"46": ["IT L1 Application Agent", 12]}' -> (ids, lowercase names)."""
    raw = os.getenv("GROUP_ROLES", "").strip()
    if not raw:
        return None
    try:
        mapping = json.loads(raw)
    except ValueError:
        _warn_once("group_roles_json", "GROUP_ROLES is not valid JSON; ignoring it")
        return None
    items = mapping.get(str(group_id)) if isinstance(mapping, dict) else None
    if items is None:
        return None
    ids = {i for i in items if isinstance(i, int)}
    names = {str(i).strip().lower() for i in items if not isinstance(i, int)}
    return ids, names


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


def _exclusion_reason(user, group_id, via_roles, group_roles, agent_roles):
    """None if the candidate can be assigned tickets, else a short reason."""
    if not user.get("active"):
        return "inactive"
    if agent_roles is not None and not (set(user.get("role_ids") or []) & agent_roles):
        return "none of the user's roles has the ticket.agent permission"
    if _is_out_of_office(user):
        return "out of office"
    if OWNER_ACCESS:
        levels = {a.lower() for a in (_group_access(user, group_id) or [])}
        for rid in via_roles:
            levels |= {a.lower() for a in group_roles[rid][1]}
        if levels and not (levels & OWNER_ACCESS):
            return f"group access {sorted(levels)} has none of {sorted(OWNER_ACCESS)}"
    return None


def _compute_group_agents(group_id: int) -> list:
    group = _call("GET", f"/api/v1/groups/{group_id}")
    direct_ids = set(group.get("user_ids") or [])

    roles = _cached("roles", ROLE_CACHE_TTL, _load_roles)
    perms = _cached("perms", ROLE_CACHE_TTL, _load_permissions) if roles is not None else None
    agent_roles = _agent_role_ids(roles, perms)

    # role id -> (name, access levels on this group)
    group_roles: dict = {}
    explicit = _explicit_roles(group_id)
    if explicit is not None:
        direct_ids = set()  # pinned roles define the pool: direct members are not added
        ids, names = explicit
        if roles is None:
            group_roles = {rid: ("?", []) for rid in ids}
        else:
            for r in roles:
                if r.get("active", True) and (
                    r["id"] in ids or str(r.get("name", "")).lower() in names
                ):
                    group_roles[r["id"]] = (r.get("name"), _group_access(r, group_id) or [])
    else:
        # No GROUP_ROLES entry: legacy behaviour, direct group members only.
        # Deliberately NOT guessing from role group permissions: in many setups
        # every IT role (L2, L3, admins...) has permissions on every IT group.
        _warn_once(
            f"unpinned-{group_id}",
            f"group={group_id} has no entry in GROUP_ROLES; using direct group members only",
        )

    users = _cached("users", AGENT_CACHE_TTL, _load_users)

    agents, excluded = [], {}
    for u in users:
        uid = u.get("id")
        via_roles = set(u.get("role_ids") or []) & group_roles.keys()
        if not via_roles and uid not in direct_ids:
            continue
        reason = _exclusion_reason(u, group_id, via_roles, group_roles, agent_roles)
        if reason is None:
            agents.append(u)
        else:
            excluded[uid] = reason

    log.info(
        "group=%s roles=%s direct_members=%d eligible=%s excluded=%s",
        group_id, {rid: v[0] for rid, v in group_roles.items()}, len(direct_ids),
        [a["id"] for a in agents], excluded,
    )
    if not agents:
        log.error(
            "group=%s: no assignable agents. Check that a role has group permissions "
            "for this group (Admin > Roles), or set GROUP_ROLES", group_id,
        )
    return agents


def get_group_agents(group_id: int) -> list:
    """
    Assignable agents for a group, derived from custom roles and direct access.
    Cached for AGENT_CACHE_TTL_SECONDS (roles/permissions: ROLE_CACHE_TTL_SECONDS).
    """
    return list(_cached(("agents", group_id), AGENT_CACHE_TTL, lambda: _compute_group_agents(group_id)))
