import argparse
import sys

from ray_isolation_check.check import print_report, run_check


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="ray-isolation-check",
        description="Run a job command twice at once on one Ray cluster and report named actors the two runs share.",
    )
    parser.add_argument(
        "--address", required=True, help="Ray cluster address the jobs connect to, for example 127.0.0.1:6379"
    )
    parser.add_argument("--dashboard", default="http://127.0.0.1:8265", help="Ray dashboard address for the state API")
    parser.add_argument("--timeout", type=float, default=600, help="seconds before both runs are killed")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="job command, after --")
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("give the job command after --")
    report = run_check(command, args.address, args.dashboard, timeout_s=args.timeout)
    print_report(report)
    return 1 if report.findings or any(report.exit_codes.values()) else 0


if __name__ == "__main__":
    sys.exit(main())
