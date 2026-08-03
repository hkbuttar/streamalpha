"""Periodic persistence of per-ticker model state (streaming/models.py's
TickerModels instances), so a consumer restart resumes learning instead of
starting cold. Without this, every restart pays the same early-life
noisiness described in models.py's module docstring all over again --
this is the anomaly-detection analogue of streaming/consumer.py's manual
offset commits, applied to in-memory model state instead of Kafka offsets.

Plain pickle to a local file, not Postgres: persisting a whole in-memory
model object graph (HalfSpaceTrees' trees, BOCPD's hypothesis list) is a
different concern from persisting anomaly result rows once a durable sink
for those exists, and conflating the two would make both harder to reason
about. Verified river's pipeline objects and this project's own BOCPD
implementation both round-trip correctly through pickle before building
on that assumption.
"""

from __future__ import annotations

import logging
import os
import pickle
import tempfile
from pathlib import Path

from streaming.models import TickerModels

log = logging.getLogger(__name__)

DEFAULT_STATE_PATH = Path("model_state.pkl")


def save_state(
    models_by_symbol: dict[str, TickerModels], path: Path | str = DEFAULT_STATE_PATH
) -> None:
    """Atomically write all per-ticker model state to disk.

    Writes to a temp file in the same directory and renames over the
    target, so a crash mid-write can't leave a corrupt/partial state file
    for the next startup to choke on -- os.replace is atomic on both
    POSIX and Windows.
    """
    path = Path(path)
    fd, tmp_path = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "wb") as f:
            pickle.dump(models_by_symbol, f)
        os.replace(tmp_path, path)
    except BaseException:
        os.unlink(tmp_path)
        raise


def load_state(path: Path | str = DEFAULT_STATE_PATH) -> dict[str, TickerModels]:
    """Load previously saved model state, or an empty dict if none exists.

    A missing, corrupt, or incompatible state file (e.g. after a
    TickerModels field is added/removed across a deploy) does not prevent
    the consumer from starting -- it just starts cold for the affected
    ticker(s), the same as a genuinely fresh deployment. That's considered
    acceptable degradation, not a condition worth crashing over.
    """
    path = Path(path)
    if not path.exists():
        return {}
    try:
        with path.open("rb") as f:
            return pickle.load(f)
    except Exception:
        log.exception("failed to load model state from %s, starting cold", path)
        return {}
