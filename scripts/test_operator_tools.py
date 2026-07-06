#!/usr/bin/env python3
"""Unit tests for the CI/local operator-tool runner."""

from __future__ import annotations

import contextlib
import io
import importlib.util
import pathlib
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "test_operator_tools_runner", ROOT / "test-operator-tools.py"
)
assert SPEC and SPEC.loader
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class OperatorToolRunnerTests(unittest.TestCase):
    def test_runner_compiles_itself_and_tests(self) -> None:
        self.assertIn("scripts/test-operator-tools.py", runner.PYTHON_FILES)
        for test_file in runner.TEST_FILES:
            self.assertIn(test_file, runner.PYTHON_FILES)

    def test_runner_compiles_every_top_level_python_script(self) -> None:
        expected = {
            f"scripts/{path.name}"
            for path in ROOT.glob("*.py")
        }
        self.assertEqual(set(runner.PYTHON_FILES), expected)

    def test_runner_syntax_checks_every_top_level_shell_script(self) -> None:
        expected = {
            f"scripts/{path.name}"
            for pattern in ("*.sh", "*.bash")
            for path in ROOT.glob(pattern)
        }
        self.assertEqual(set(runner.SHELL_FILES), expected)

    def test_runner_includes_all_operator_test_files(self) -> None:
        expected = {
            f"scripts/{path.name}"
            for path in ROOT.glob("test_*.py")
        }
        self.assertEqual(set(runner.TEST_FILES), expected)

    def test_list_mode_prints_discovered_files_without_running_checks(self) -> None:
        stream = io.StringIO()
        with mock.patch.object(runner, "run") as run, contextlib.redirect_stdout(stream):
            self.assertEqual(runner.main(["--list"]), 0)
        run.assert_not_called()
        output = stream.getvalue()
        self.assertIn("shell syntax:", output)
        self.assertIn("python compile:", output)
        self.assertIn("dashboard javascript:", output)
        self.assertIn("unit tests:", output)
        self.assertIn("ksolver/static/shadow.html", output)
        self.assertIn("scripts/test-operator-tools.py", output)
        self.assertIn("scripts/test_operator_tools.py", output)

    def test_runner_extracts_dashboard_inline_scripts(self) -> None:
        scripts = runner.inline_dashboard_scripts(
            "<script>function one(){ return 1; }</script><main></main><script>const two = 2;</script>"
        )
        self.assertEqual(len(scripts), 2)
        self.assertIn("function one", scripts[0])
        self.assertIn("const two", scripts[1])

    def test_runner_rejects_unclosed_dashboard_inline_script(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "missing closing tag"):
            runner.inline_dashboard_scripts("<script>function broken(){")

    def test_runner_dashboard_javascript_check_uses_node_when_available(self) -> None:
        with (
            mock.patch.object(runner.shutil, "which", return_value="/usr/bin/node"),
            mock.patch.object(runner.subprocess, "run") as run,
        ):
            runner.check_dashboard_javascript()
        self.assertTrue(run.called)
        self.assertEqual(run.call_args.args[0][:2], ["/usr/bin/node", "--check"])

    def test_runner_dashboard_javascript_check_skips_when_node_missing(self) -> None:
        stream = io.StringIO()
        with (
            mock.patch.object(runner.shutil, "which", return_value=None),
            mock.patch.object(runner.subprocess, "run") as run,
            contextlib.redirect_stdout(stream),
        ):
            runner.check_dashboard_javascript()
        run.assert_not_called()
        self.assertIn("skipped: node not found", stream.getvalue())

    def test_readme_documents_runner_list_mode(self) -> None:
        readme = (ROOT.parent / "README.md").read_text(encoding="utf-8")
        self.assertIn("python3 scripts/test-operator-tools.py", readme)
        self.assertIn("python3 scripts/test-operator-tools.py --list", readme)
        self.assertIn("dashboard inline JavaScript", readme)

    def test_readme_documents_reservation_pressure_runbook(self) -> None:
        readme = (ROOT.parent / "README.md").read_text(encoding="utf-8")
        self.assertIn("reservation_pressure", readme)
        self.assertIn("pending or reserved GPU capacity", readme)
        self.assertIn("binding risky even when GPUs look free", readme)
        self.assertIn("scripts/shadow-doctor.py", readme)
        self.assertIn("scripts/demo-gate.py", readme)


if __name__ == "__main__":
    unittest.main()
