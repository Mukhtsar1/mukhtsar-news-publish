#!/usr/bin/env python3
"""
Mukhtsar News — Instagram publisher.

Runs inside GitHub Actions on an hourly cron. Reads queue.json, finds entries
whose publish_at has passed, pushes them to Instagram via the Graph API, and
writes the results back to queue.json (the workflow commits the change).

Environment:
    IG_USER_ID        Instagram professional account ID (numeric)
    IG_ACCESS_TOKEN   System user token, never-expiring
    MEDIA_BASE_URL    Public base URL for media/ (no trailing slash)
    GRAPH_VERSION     optional, defaults to v21.0

Usage:
    python publish.py            # publish everything due
    python publish.py --check    # verify token + account, publish nothing
    python publish.py --dry-run  # show what would publish
    python publish.py --force ID # publish one entry regardless of publish_at
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests

ROOT = Path(__file__).resolve().parent
QUEUE_PATH = ROOT / "queue.json"

GRAPH_VERSION = os.environ.get("GRAPH_VERSION", "v21.0")
GRAPH = f"https://graph.facebook.com/{GRAPH_VERSION}"

# Reel containers need video transcoding on Meta's side. Poll until FINISHED.
POLL_INTERVAL = 6          # seconds between status checks
POLL_TIMEOUT = 600         # give up after 10 minutes
MAX_ATTEMPTS = 3           # per queue entry, across runs, before marking failed
PUBLISH_GAP = 8            # seconds to wait after a publish before the next one


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(value: str) -> datetime:
    """Accept '2026-08-20T17:00:00Z' or any ISO 8601 string."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def log(msg: str) -> None:
    print(f"[{now_utc():%Y-%m-%d %H:%M:%S}Z] {msg}", flush=True)


def load_queue() -> list[dict]:
    if not QUEUE_PATH.exists():
        log("queue.json not found — nothing to do.")
        return []
    # utf-8-sig strips a byte-order mark if present. PowerShell's
    # Set-Content -Encoding utf8 writes one, and plain utf-8 chokes on it.
    with QUEUE_PATH.open(encoding="utf-8-sig") as fh:
        data = json.load(fh)
    return data.get("items", data) if isinstance(data, dict) else data


def save_queue(items: list[dict]) -> None:
    payload = {"items": items}
    with QUEUE_PATH.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


class GraphError(RuntimeError):
    """Graph API returned an error we can surface cleanly."""


def graph_post(path: str, token: str, **params) -> dict:
    params["access_token"] = token
    resp = requests.post(f"{GRAPH}/{path}", data=params, timeout=90)
    body = resp.json() if resp.content else {}
    if "error" in body:
        err = body["error"]
        raise GraphError(
            f"{err.get('type')} {err.get('code')}/{err.get('error_subcode', '-')}: "
            f"{err.get('message')}"
        )
    resp.raise_for_status()
    return body


def graph_get(path: str, token: str, **params) -> dict:
    params["access_token"] = token
    resp = requests.get(f"{GRAPH}/{path}", params=params, timeout=60)
    body = resp.json() if resp.content else {}
    if "error" in body:
        err = body["error"]
        raise GraphError(f"{err.get('code')}: {err.get('message')}")
    resp.raise_for_status()
    return body


def media_url(base: str, relative: str) -> str:
    """Build the public URL Meta will fetch.

    Filenames carry Arabic characters, and the URL is handed to the Graph API
    as a plain string for Meta's own fetcher to resolve — so it has to be
    percent-encoded here. Unencoded, Meta requests a path that doesn't match,
    GitHub Pages answers with its 404 HTML page, and Meta reports
    "Only photo or video can be accepted as media type" because it downloaded
    HTML instead of a JPEG.
    """
    if relative.startswith(("http://", "https://")):
        return relative
    path = quote(relative.lstrip("/"), safe="/")
    return f"{base.rstrip('/')}/{path}"


# --------------------------------------------------------------------------
# container lifecycle
# --------------------------------------------------------------------------

def wait_for_container(container_id: str, token: str) -> None:
    """Block until a container finishes processing, or raise."""
    deadline = time.monotonic() + POLL_TIMEOUT
    last = None
    while time.monotonic() < deadline:
        info = graph_get(container_id, token, fields="status_code,status")
        status = info.get("status_code")
        if status != last:
            log(f"  container {container_id}: {status}")
            last = status
        if status == "FINISHED":
            return
        if status in ("ERROR", "EXPIRED"):
            raise GraphError(
                f"container {container_id} ended as {status}: "
                f"{info.get('status', 'no detail')}"
            )
        time.sleep(POLL_INTERVAL)
    raise GraphError(f"container {container_id} still processing after {POLL_TIMEOUT}s")


def publish_reel(ig_user: str, token: str, entry: dict, base: str) -> str:
    url = media_url(base, entry["media"])
    log(f"  reel source: {url}")

    params = {
        "media_type": "REELS",
        "video_url": url,
        "caption": entry.get("caption", ""),
        # keeps the reel visible on the grid, not only in the Reels tab
        "share_to_feed": "true",
    }
    if entry.get("cover"):
        params["cover_url"] = media_url(base, entry["cover"])
    elif entry.get("thumb_offset") is not None:
        params["thumb_offset"] = str(entry["thumb_offset"])

    container = graph_post(f"{ig_user}/media", token, **params)["id"]
    wait_for_container(container, token)
    return graph_post(f"{ig_user}/media_publish", token, creation_id=container)["id"]


def publish_carousel(ig_user: str, token: str, entry: dict, base: str) -> str:
    slides = entry.get("media") or []
    if not 2 <= len(slides) <= 10:
        raise GraphError(f"carousel needs 2-10 slides, got {len(slides)}")

    children: list[str] = []
    for index, slide in enumerate(slides, start=1):
        url = media_url(base, slide)
        log(f"  slide {index}/{len(slides)}: {url}")
        child = graph_post(
            f"{ig_user}/media",
            token,
            image_url=url,
            is_carousel_item="true",
        )["id"]
        # Images normally return FINISHED almost immediately, but Meta still
        # has to fetch them — checking here surfaces a bad URL before we build
        # the parent container and burn the whole post.
        wait_for_container(child, token)
        children.append(child)

    container = graph_post(
        f"{ig_user}/media",
        token,
        media_type="CAROUSEL",
        children=",".join(children),
        caption=entry.get("caption", ""),
    )["id"]
    wait_for_container(container, token)
    return graph_post(f"{ig_user}/media_publish", token, creation_id=container)["id"]


# --------------------------------------------------------------------------
# preflight
# --------------------------------------------------------------------------

def check(ig_user: str, token: str) -> int:
    try:
        me = graph_get(ig_user, token, fields="id,username,name,followers_count")
        log(f"account: @{me.get('username')} ({me.get('id')}) "
            f"— {me.get('followers_count', '?')} followers")

        limit = graph_get(
            f"{ig_user}/content_publishing_limit",
            token,
            fields="config,quota_usage",
        )
        row = (limit.get("data") or [{}])[0]
        quota = row.get("config", {}).get("quota_total", 25)
        used = row.get("quota_usage", 0)
        log(f"publishing quota: {used}/{quota} used in the last 24h")
        return 0
    except GraphError as exc:
        log(f"FAILED: {exc}")
        return 1


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="verify credentials only")
    ap.add_argument("--dry-run", action="store_true", help="show what is due")
    ap.add_argument("--force", metavar="ID", help="publish one entry now")
    args = ap.parse_args()

    ig_user = os.environ.get("IG_USER_ID", "").strip()
    token = os.environ.get("IG_ACCESS_TOKEN", "").strip()
    base = os.environ.get("MEDIA_BASE_URL", "").strip()

    if not ig_user or not token:
        log("IG_USER_ID and IG_ACCESS_TOKEN must be set.")
        return 1

    if args.check:
        return check(ig_user, token)

    if not base:
        log("MEDIA_BASE_URL must be set.")
        return 1

    items = load_queue()
    now = now_utc()

    due = []
    for entry in items:
        if entry.get("status") != "pending":
            continue
        if args.force:
            if entry.get("id") == args.force:
                due.append(entry)
            continue
        if parse_ts(entry["publish_at"]) <= now:
            due.append(entry)

    if not due:
        log("nothing due.")
        return 0

    log(f"{len(due)} item(s) due.")

    if args.dry_run:
        for entry in due:
            log(f"  would publish {entry['id']} ({entry['type']}) "
                f"scheduled {entry['publish_at']}")
        return 0

    changed = False
    failures = 0

    for index, entry in enumerate(due):
        entry_id = entry.get("id", "?")
        kind = entry.get("type", "reel")
        log(f"publishing {entry_id} ({kind})")

        try:
            if kind == "reel":
                media_id = publish_reel(ig_user, token, entry, base)
            elif kind == "carousel":
                media_id = publish_carousel(ig_user, token, entry, base)
            else:
                raise GraphError(f"unknown type {kind!r}")

            entry["status"] = "published"
            entry["media_id"] = media_id
            entry["published_at"] = now_utc().isoformat(timespec="seconds")
            entry.pop("error", None)
            log(f"  published as {media_id}")

        except (GraphError, requests.RequestException) as exc:
            failures += 1
            entry["attempts"] = entry.get("attempts", 0) + 1
            entry["error"] = str(exc)
            entry["last_attempt"] = now_utc().isoformat(timespec="seconds")
            if entry["attempts"] >= MAX_ATTEMPTS:
                entry["status"] = "failed"
                log(f"  FAILED permanently after {entry['attempts']} attempts: {exc}")
            else:
                log(f"  error (attempt {entry['attempts']}, will retry): {exc}")

        changed = True

        if index < len(due) - 1:
            time.sleep(PUBLISH_GAP)

    if changed:
        save_queue(items)
        log("queue.json updated.")

    # Non-zero only when something failed for good, so transient errors don't
    # turn the Actions log into a wall of red while retries are still pending.
    return 1 if any(e.get("status") == "failed" for e in due) else 0


if __name__ == "__main__":
    sys.exit(main())
