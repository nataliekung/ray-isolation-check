import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
from ray.util import state

from ray_isolation_check.check import run_check

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"
RAY = [sys.executable, "-m", "ray.scripts.scripts"]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def cluster():
    gcs, dash = _free_port(), _free_port()
    subprocess.run(
        [*RAY, "start", "--head", f"--port={gcs}", f"--dashboard-port={dash}", "--num-cpus=4"],
        check=True,
        capture_output=True,
    )
    dashboard = f"http://127.0.0.1:{dash}"
    deadline = time.monotonic() + 60
    while True:
        try:
            state.list_jobs(address=dashboard)
            break
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)
    yield f"127.0.0.1:{gcs}", dashboard
    subprocess.run([*RAY, "stop", "--force"], capture_output=True, check=False)


def test_actor_in_a_fixed_namespace_is_reported_as_shared(cluster):
    report = run_check([sys.executable, str(EXAMPLES / "fixed_namespace_job.py")], *cluster, timeout_s=120)
    assert report.exit_codes == {"a": 0, "b": 0}
    assert [f.kind for f in report.findings] == ["shared"]
    assert "'Controller' in namespace shared_queue" in report.findings[0].detail


def test_actors_in_each_job_namespace_are_not_reported(cluster):
    report = run_check([sys.executable, str(EXAMPLES / "job_namespace_job.py")], *cluster, timeout_s=120)
    assert report.exit_codes == {"a": 0, "b": 0}
    assert report.findings == []
    assert len(report.actors["a"]) == len(report.actors["b"]) == 1
