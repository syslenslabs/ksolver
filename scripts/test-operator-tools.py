#!/usr/bin/env python3
"""Run the shadow/operator Python tool checks used by CI."""

from __future__ import annotations

import argparse
import pathlib
import shutil
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parent.parent
DASHBOARD_HTML = "ksolver/static/shadow.html"

PYTHON_FILES = sorted(
    str(path.relative_to(ROOT))
    for path in (ROOT / "scripts").glob("*.py")
)

SHELL_FILES = sorted(
    str(path.relative_to(ROOT))
    for pattern in ("*.sh", "*.bash")
    for path in (ROOT / "scripts").glob(pattern)
)

TEST_FILES = sorted(
    str(path.relative_to(ROOT))
    for path in (ROOT / "scripts").glob("test_*.py")
)


def run(argv: list[str]) -> None:
    print("+ " + " ".join(argv), flush=True)
    subprocess.run(argv, cwd=ROOT, check=True)


def inline_dashboard_scripts(html: str) -> list[str]:
    scripts: list[str] = []
    cursor = 0
    while True:
        start = html.find("<script>", cursor)
        if start < 0:
            break
        body_start = start + len("<script>")
        end = html.find("</script>", body_start)
        if end < 0:
            raise RuntimeError("dashboard inline script missing closing tag")
        scripts.append(html[body_start:end])
        cursor = end + len("</script>")
    return scripts


def check_dashboard_javascript() -> None:
    node = shutil.which("node")
    if not node:
        print(f"+ node --check {DASHBOARD_HTML} (skipped: node not found)", flush=True)
        return
    html = (ROOT / DASHBOARD_HTML).read_text(encoding="utf-8")
    scripts = inline_dashboard_scripts(html)
    if not scripts:
        raise RuntimeError(f"{DASHBOARD_HTML} has no inline <script> blocks")
    for idx, script in enumerate(scripts):
        print(f"+ node --check {DASHBOARD_HTML}#script-{idx}", flush=True)
        subprocess.run(
            [node, "--check", "-"],
            cwd=ROOT,
            input=script,
            text=True,
            check=True,
        )


def print_plan() -> None:
    groups = [
        ("shell syntax", SHELL_FILES),
        ("python compile", PYTHON_FILES),
        ("dashboard javascript", [DASHBOARD_HTML]),
        ("unit tests", TEST_FILES),
    ]
    for label, files in groups:
        print(f"{label}: {len(files)}")
        for file in files:
            print(f"  {file}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run CI-equivalent checks for the shadow/operator scripts."
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="print discovered files without running checks",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.list:
        print_plan()
        return 0
    try:
        for shell_file in SHELL_FILES:
            run(["bash", "-n", shell_file])
        check_dashboard_javascript()
        run([sys.executable, "-m", "py_compile", *PYTHON_FILES])
        run([sys.executable, "-m", "unittest", *TEST_FILES])
    except subprocess.CalledProcessError as exc:
        return exc.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
