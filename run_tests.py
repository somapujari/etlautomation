import subprocess
import sys

# Automatically install pytest if not present
subprocess.check_call([sys.executable, "-m", "pip", "install", "pytest"])


import argparse
import pytest


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Run Pytest with custom options.")

    parser.add_argument("--env", default="dev", help="Specify the environment to run tests (default: dev).")
    parser.add_argument("--marker", default="retest", help="Run tests with a specific marker (e.g., 'count_check').")
    parser.add_argument("--output-dir", default="reports", help="Specify the directory to save the test report.")
    parser.add_argument("--html-report", action="store_true", help="Generate an HTML report.")
    parser.add_argument("--test-dir", default="tests/table12", help="Specify the directory containing test files.")
    parser.add_argument("--maxfail", type=int, default=None, help="Stop after the specified number of failures.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")

    args = parser.parse_args()

    # Prepare Pytest arguments
    pytest_args = [args.test_dir, "--env", args.env]

    if args.marker:
        pytest_args.extend(["-m", args.marker])
    if args.output_dir:
        pytest_args.extend(["--junitxml", f"{args.output_dir}/results.xml"])
    if args.html_report:
        pytest_args.extend(["--html", f"{args.output_dir}/report.html"])
    if args.maxfail:
        pytest_args.extend(["--maxfail", str(args.maxfail)])
    if args.verbose:
        pytest_args.append("-v")

    # 🔥 Trigger the tests
    exit(pytest.main(pytest_args))


if __name__ == "__main__":
    main()
