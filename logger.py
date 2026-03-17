"""logger.py – logging setup (console + daily rotating file)."""

import logging
from datetime import datetime, timedelta
from pathlib import Path


def setup_logging() -> logging.Logger:
    """
    Configure two loggers:
      root     → console (INFO) + daily file (INFO)
      detailed → file only (verbose API traffic, no console)
    Returns the 'detailed' logger.
    """
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    today = datetime.now().strftime("%Y-%m-%d")
    today_log = log_dir / f"log_{today}.txt"

    # Удаляем логи старше 7 дней
    cutoff = datetime.now() - timedelta(days=7)
    for old in log_dir.glob("log_*.txt"):
        try:
            if datetime.strptime(old.stem[4:], "%Y-%m-%d") < cutoff:
                old.unlink()
        except (ValueError, OSError):
            pass

    fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")

    # File handler (shared)
    fh = logging.FileHandler(today_log, mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()
    root.addHandler(ch)
    root.addHandler(fh)

    # Write run separator to file
    with today_log.open("a", encoding="utf-8") as f:
        f.write(f"\n{'='*60}\n"
                f"  Run {datetime.now():%Y-%m-%d %H:%M:%S}\n"
                f"{'='*60}\n")

    # Detailed logger → file only
    detailed = logging.getLogger("detailed")
    detailed.setLevel(logging.DEBUG)
    detailed.propagate = False
    if not detailed.handlers:
        detailed.addHandler(fh)

    logging.getLogger(__name__).info("Логи: %s", today_log)
    return detailed
