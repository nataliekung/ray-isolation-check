"""Run one job command twice against the same Ray cluster and compare the named actors each run created."""

import os
import subprocess
import sys
import time
from dataclasses import dataclass, field

from ray.util import state

RUN_LABELS = ("a", "b")


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
class Report:
    exit_codes: dict[str, int]
    actors: dict[str, list[NamedActor]]
    findings: list[Finding]


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


def poll(runs: list[Run], address: str, timeout_s: float, interval_s: float) -> dict[str, NamedActor]:
    """Record every named actor seen while the runs are alive, keyed by actor id."""
    seen: dict[str, NamedActor] = {}
    deadline = time.monotonic() + timeout_s
    while any(r.process.poll() is None for r in runs) and time.monotonic() < deadline:
        _match_jobs(runs, address)
        for actor in state.list_actors(address=address, detail=True, limit=10000, raise_on_missing_output=False):
            if actor.name:
                seen[actor.actor_id] = NamedActor(actor.name, actor.ray_namespace, actor.job_id)
        time.sleep(interval_s)
    for r in runs:
        if r.process.poll() is None:
            r.process.kill()
    return seen


def _match_jobs(runs: list[Run], address: str) -> None:
    """Map Ray driver jobs to runs through the driver process id."""
    pids = {r.process.pid: r for r in runs}
    for job in state.list_jobs(address=address, raise_on_missing_output=False):
        driver = job.driver_info
        if driver is not None and int(driver.pid) in pids:
            pids[int(driver.pid)].job_ids.add(job.job_id)


def analyze(runs: list[Run], seen: dict[str, NamedActor]) -> Report:
    by_run = {r.label: [a for a in seen.values() if a.job_id in r.job_ids] for r in runs}
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
    exit_codes = {r.label: r.process.returncode for r in runs}
    return Report(exit_codes, by_run, findings)


def run_check(
    command: list[str],
    ray_address: str,
    dashboard: str,
    timeout_s: float = 600,
    interval_s: float = 0.5,
    stagger_s: float = 1.0,
) -> Report:
    runs = start_runs(command, ray_address, stagger_s)
    seen = poll(runs, dashboard, timeout_s, interval_s)
    for r in runs:
        r.process.wait()
    _match_jobs(runs, dashboard)
    return analyze(runs, seen)


def print_report(report: Report, out=sys.stdout) -> None:
    for label, code in report.exit_codes.items():
        actors = sorted(f"{a.namespace}/{a.name}" for a in report.actors[label])
        print(f"run {label}: exit {code}, named actors: {actors or 'none'}", file=out)
    if not report.findings:
        print("no shared named actors found", file=out)
    for f in report.findings:
        print(f"{f.kind.upper()}: {f.detail}", file=out)
