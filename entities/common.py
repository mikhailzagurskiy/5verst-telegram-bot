from datetime import date, time


def as_int(val: str) -> int:
  return int(val) if val else None


def as_bool(val: str) -> int:
  return True if val and ((isinstance(val, str) and val.lower() == 'true') or isinstance(val, int) != 0) else False


def as_date(val: str) -> date:
  return date.fromisoformat(val) if val else None


def as_time(val: str) -> time:
  return time.fromisoformat(val) if val else None
