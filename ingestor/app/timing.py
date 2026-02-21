import logging
import time
from functools import wraps


def timed_phase(phase_name):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            start = time.monotonic()
            result = fn(*args, **kwargs)
            duration_seconds = round(time.monotonic() - start, 2)
            duration_minutes = round(duration_seconds / 60, 2)
            logging.info(
                f"Phase completed: {phase_name}",
                extra={"phase": phase_name, "duration_seconds": duration_seconds, "duration_minutes": duration_minutes},
            )
            return result

        return wrapper

    return decorator
