"""Shared freshness policy for Europe measurements and their consumers."""

from datetime import datetime, timedelta, timezone

PROBE_MAX_AGE_HOURS = 8
SPEED_MAX_AGE_HOURS = 36
SAMPLE_MAX_AGE_DAYS = 14


def parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(timezone.utc)


def is_fresh(value: object, hours: float, *, now: datetime) -> bool:
    stamp = parse_timestamp(value)
    return stamp is not None and timedelta(0) <= now - stamp <= timedelta(hours=hours)


def node_is_fresh(state: dict, *, now: datetime) -> bool:
    return is_fresh(state.get("last_probe_at"), PROBE_MAX_AGE_HOURS, now=now) and is_fresh(
        state.get("last_speed_at"), SPEED_MAX_AGE_HOURS, now=now
    )
