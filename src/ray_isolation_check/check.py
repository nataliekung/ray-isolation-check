"""Run one job command twice against the same Ray cluster and compare what each run created or opened."""

import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field

import psutil
from ray.util import state

RUN_LABELS = ("a", "b")
DEFAULT_WATCH_ROOTS = (tempfile.gettempdir(), "/tmp", "/dev/shm")


@dataclass
class NamedActor:
    name: str
    namespace: str
    job_id: str


@dataclass
class Run:
    label: str
    process: subprocess.Popen
    job_ids: set[str] = field(default_factory=set)


@dataclass
class Finding:
    kind: str
    detail: str


@dataclass
class Observation:
    actors: dict[str, NamedActor] = field(default_factory=dict)
    paths: dict[str, set[str]] = field(default_factory=lambda: {label: set() for label in RUN_LABELS})


@dataclass
class Report:
    exit_codes: dict[str, int]
    actors: dict[str, list[NamedActor]]
    paths: dict[str, set[str]]
    findings: list[Finding]


def require_state_api(address: str) -> None:
    """Fail early with a clear message when the Ray state API (served by the dashboard) is unreachable."""
    try:
        state.list_jobs(address=address, timeout=10)
    except Exception as e:
        raise RuntimeError(
            f"cannot reach the Ray state API at {address}. It is served by the Ray dashboard: start the head "
            "with `ray start --head` from a `ray[default]` install and pass its address with --dashboard."
        ) from e


def start_runs(command: list[str], ray_address: str, stagger_s: float) -> list[Run]:
    """Start the two copies; each sees RAY_ADDRESS and ISOLATION_CHECK_RUN=a or b.

    The second copy starts `stagger_s` later: two drivers calling ray.init() in the same
    instant can race in Ray's local process scan, and the jobs still overlap.
    """
    runs = []
    for label in RUN_LABELS:
        if runs:
            time.sleep(stagger_s)
        env = {**os.environ, "RAY_ADDRESS": ray_address, "ISOLATION_CHECK_RUN": label}
        runs.append(Run(label, subprocess.Popen(command, env=env)))
    return runs


def poll(runs: list[Run], address: str, roots: list[str], timeout_s: float, interval_s: float) -> Observation:
    """Sample named actors and open files under `roots` while the runs are alive."""
    seen = Observation()
    deadline = time.monotonic() + timeout_s
    while any(r.process.poll() is None for r in runs) and time.monotonic() < deadline:
        _match_jobs(runs, address)
        actors = state.list_actors(address=address, detail=True, limit=10000, raise_on_missing_output=False)
        for actor in actors:
            if actor.name:
                seen.actors[actor.actor_id] = NamedActor(actor.name, actor.ray_namespace, actor.job_id)
        for r in runs:
            pids = {actor.pid for actor in actors if actor.job_id in r.job_ids and actor.pid}
            seen.paths[r.label] |= _open_paths(r.process.pid, pids, roots)
        time.sleep(interval_s)
    for r in runs:
        if r.process.poll() is None:
            r.process.kill()
    return seen


def _open_paths(driver_pid: int, actor_pids: set[int], roots: list[str]) -> set[str]:
    """Files under `roots` held open by the driver, its children, or the run's actors.

    Ray's own session directory is shared by design and skipped.
    """
    pids = set(actor_pids)
    try:
        driver = psutil.Process(driver_pid)
        pids |= {driver_pid, *(c.pid for c in driver.children(recursive=True))}
    except psutil.Error:
        pass
    paths = set()
    for pid in pids:
        try:
            files = psutil.Process(pid).open_files()
        except psutil.Error:
            continue
        for f in files:
            path = os.path.realpath(f.path)
            if any(path.startswith(root + os.sep) for root in roots) and f"{os.sep}ray{os.sep}session_" not in path:
                paths.add(path)
    return paths


def _match_jobs(runs: list[Run], address: str) -> None:
    """Map Ray driver jobs to runs through the driver process id."""
    pids = {r.process.pid: r for r in runs}
    for job in state.list_jobs(address=address, raise_on_missing_output=False):
        driver = job.driver_info
        if driver is not None and int(driver.pid) in pids:
            pids[int(driver.pid)].job_ids.add(job.job_id)


def analyze(runs: list[Run], seen: Observation) -> Report:
    by_run = {r.label: [a for a in seen.actors.values() if a.job_id in r.job_ids] for r in runs}
    names = {label: {a.name for a in actors} for label, actors in by_run.items()}
    findings = []
    a, b = RUN_LABELS
    for name in sorted(names[a] ^ names[b]):
        creator = a if name in names[a] else b
        other = b if creator == a else a
        namespaces = sorted({x.namespace for x in by_run[creator] if x.name == name})
        findings.append(
            Finding(
                "shared",
                f"named actor {name!r} in namespace {', '.join(namespaces)} was created only by run {creator}; "
                f"run {other} either attached to it or never created it",
            )
        )
    for name in sorted(names[a] & names[b]):
        ns_a = {x.namespace for x in by_run[a] if x.name == name}
        ns_b = {x.namespace for x in by_run[b] if x.name == name}
        if ns_a & ns_b:
            findings.append(
                Finding("collision", f"both runs created {name!r} in namespace {', '.join(sorted(ns_a & ns_b))}")
            )
    for path in sorted(seen.paths[a] & seen.paths[b]):
        findings.append(Finding("shared_path", f"{path} was held open by both runs"))
    exit_codes = {r.label: r.process.returncode for r in runs}
    return Report(exit_codes, by_run, seen.paths, findings)


def run_check(
    command: list[str],
    ray_address: str,
    dashboard: str,
    timeout_s: float = 600,
    interval_s: float = 0.5,
    stagger_s: float = 1.0,
    watch_roots: tuple[str, ...] = DEFAULT_WATCH_ROOTS,
) -> Report:
    require_state_api(dashboard)
    roots = sorted({os.path.realpath(r) for r in watch_roots if os.path.isdir(r)})
    runs = start_runs(command, ray_address, stagger_s)
    seen = poll(runs, dashboard, roots, timeout_s, interval_s)
    for r in runs:
        r.process.wait()
    _match_jobs(runs, dashboard)
    return analyze(runs, seen)


def print_report(report: Report, out=sys.stdout) -> None:
    for label, code in report.exit_codes.items():
        actors = sorted(f"{a.namespace}/{a.name}" for a in report.actors[label])
        print(f"run {label}: exit {code}, named actors: {actors or 'none'}", file=out)
        print(f"run {label}: open files under watched roots: {sorted(report.paths[label]) or 'none'}", file=out)
    if not report.findings:
        print("no shared named actors or files found", file=out)
    for f in report.findings:
        print(f"{f.kind.upper()}: {f.detail}", file=out)
