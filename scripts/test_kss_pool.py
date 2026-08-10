#!/usr/bin/env python3
"""Unit tests for kss-pool.sh using fake docker/curl binaries."""

from __future__ import annotations

import os
import pathlib
import stat
import subprocess
import tempfile
import textwrap
import unittest


ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "kss-pool.sh"


def write_executable(path: pathlib.Path, body: str) -> None:
    path.write_text(textwrap.dedent(body).lstrip(), encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


class KssPoolScriptTests(unittest.TestCase):
    def run_with_fakes(self, docker_body: str, curl_body: str, *args: str) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as tmp:
            bindir = pathlib.Path(tmp)
            write_executable(bindir / "docker", docker_body)
            write_executable(bindir / "curl", curl_body)
            env = os.environ.copy()
            env["PATH"] = f"{bindir}:{env['PATH']}"
            return subprocess.run(
                [str(SCRIPT), *args],
                cwd=ROOT,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

    def test_status_uses_docker_published_ready_ports_for_export_command(self) -> None:
        docker = r"""
        #!/usr/bin/env bash
        set -euo pipefail
        case "$1" in
          info) exit 0 ;;
          port)
            case "$2" in
              ksolver-kss-0-server) echo "127.0.0.1:12130" ;;
              ksolver-kss-1-server) echo "127.0.0.1:12131" ;;
              *) exit 1 ;;
            esac
            ;;
          inspect)
            name="${@: -1}"
            case "$name" in
              ksolver-kss-0-*|ksolver-kss-1-*) echo "running" ;;
              *) echo "" ; exit 1 ;;
            esac
            ;;
          *) echo "unexpected docker command: $*" >&2; exit 9 ;;
        esac
        """
        curl = r"""
        #!/usr/bin/env bash
        set -euo pipefail
        case "$*" in
          *127.0.0.1:12130/api/v1/export*|*127.0.0.1:12131/api/v1/export*) exit 0 ;;
          *) exit 22 ;;
        esac
        """
        result = self.run_with_fakes(
            docker,
            curl,
            "status",
            "4",
            "12120",
            "/tmp/ksolver-kss-cache",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("http://127.0.0.1:12130", result.stdout)
        self.assertIn("http://127.0.0.1:12131", result.stdout)
        self.assertIn("ksolver-kss-2-cluster", result.stdout)
        self.assertIn(
            "export KSOLVER_GPU_SCENARIO_SIMULATOR_POOL=http://127.0.0.1:12130,http://127.0.0.1:12131",
            result.stdout,
        )
        self.assertIn(
            '--simulator-pool "http://127.0.0.1:12130,http://127.0.0.1:12131"',
            result.stdout,
            )

    def test_urls_rejects_zero_count_before_arithmetic(self) -> None:
        docker = r"""
        #!/usr/bin/env bash
        echo "docker should not be called" >&2
        exit 9
        """
        curl = r"""
        #!/usr/bin/env bash
        echo "curl should not be called" >&2
        exit 9
        """
        result = self.run_with_fakes(docker, curl, "urls", "0", "12120")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("count must be greater than 0", result.stderr)

    def test_ready_urls_rejects_non_integer_count_before_docker(self) -> None:
        docker = r"""
        #!/usr/bin/env bash
        echo "docker should not be called" >&2
        exit 9
        """
        curl = r"""
        #!/usr/bin/env bash
        echo "curl should not be called" >&2
        exit 9
        """
        result = self.run_with_fakes(docker, curl, "ready-urls", "many", "12120")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("count must be a non-negative integer", result.stderr)
        self.assertNotIn("docker should not be called", result.stderr)

    def test_ready_urls_rejects_negative_base_port_before_docker(self) -> None:
        docker = r"""
        #!/usr/bin/env bash
        echo "docker should not be called" >&2
        exit 9
        """
        curl = r"""
        #!/usr/bin/env bash
        echo "curl should not be called" >&2
        exit 9
        """
        result = self.run_with_fakes(docker, curl, "ready-urls", "1", "-1")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("base_port must be a non-negative integer", result.stderr)
        self.assertNotIn("docker should not be called", result.stderr)

    def test_status_reports_no_ready_endpoints_when_export_probe_fails(self) -> None:
        docker = r"""
        #!/usr/bin/env bash
        set -euo pipefail
        case "$1" in
          info) exit 0 ;;
          port) echo "127.0.0.1:12120" ;;
          inspect) echo "running" ;;
          *) echo "unexpected docker command: $*" >&2; exit 9 ;;
        esac
        """
        curl = r"""
        #!/usr/bin/env bash
        exit 22
        """
        result = self.run_with_fakes(docker, curl, "status", "1", "12120")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("not-ready", result.stdout)
        self.assertIn("No ready simulator endpoints", result.stdout)
        self.assertNotIn("export KSOLVER_GPU_SCENARIO_SIMULATOR_POOL=", result.stdout)

    def test_ready_urls_prints_only_export_ready_published_ports(self) -> None:
        docker = r"""
        #!/usr/bin/env bash
        set -euo pipefail
        case "$1" in
          info) exit 0 ;;
          port)
            case "$2" in
              ksolver-kss-0-server) echo "127.0.0.1:12130" ;;
              ksolver-kss-1-server) echo "127.0.0.1:12131" ;;
              ksolver-kss-2-server) echo "127.0.0.1:12132" ;;
              *) exit 1 ;;
            esac
            ;;
          *) echo "unexpected docker command: $*" >&2; exit 9 ;;
        esac
        """
        curl = r"""
        #!/usr/bin/env bash
        set -euo pipefail
        case "$*" in
          *127.0.0.1:12130/api/v1/export*|*127.0.0.1:12132/api/v1/export*) exit 0 ;;
          *) exit 22 ;;
        esac
        """
        result = self.run_with_fakes(docker, curl, "ready-urls", "3", "12120")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(),
            "http://127.0.0.1:12130,http://127.0.0.1:12132",
        )

    def test_ready_urls_prints_blank_line_when_no_export_probe_passes(self) -> None:
        docker = r"""
        #!/usr/bin/env bash
        set -euo pipefail
        case "$1" in
          info) exit 0 ;;
          port) echo "127.0.0.1:12120" ;;
          *) echo "unexpected docker command: $*" >&2; exit 9 ;;
        esac
        """
        curl = r"""
        #!/usr/bin/env bash
        exit 22
        """
        result = self.run_with_fakes(docker, curl, "ready-urls", "1", "12120")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "\n")

    def test_require_ready_urls_prints_ready_urls_when_available(self) -> None:
        docker = r"""
        #!/usr/bin/env bash
        set -euo pipefail
        case "$1" in
          info) exit 0 ;;
          port) echo "127.0.0.1:12130" ;;
          *) echo "unexpected docker command: $*" >&2; exit 9 ;;
        esac
        """
        curl = r"""
        #!/usr/bin/env bash
        exit 0
        """
        result = self.run_with_fakes(docker, curl, "require-ready-urls", "1", "12120")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "http://127.0.0.1:12130")

    def test_require_ready_urls_exits_two_when_no_endpoint_is_ready(self) -> None:
        docker = r"""
        #!/usr/bin/env bash
        set -euo pipefail
        case "$1" in
          info) exit 0 ;;
          port) echo "127.0.0.1:12120" ;;
          *) echo "unexpected docker command: $*" >&2; exit 9 ;;
        esac
        """
        curl = r"""
        #!/usr/bin/env bash
        exit 22
        """
        result = self.run_with_fakes(docker, curl, "require-ready-urls", "1", "12120")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("no ready kube-scheduler-simulator endpoints", result.stderr)

    def test_wait_ready_urls_polls_until_an_endpoint_is_ready(self) -> None:
        docker = r"""
        #!/usr/bin/env bash
        set -euo pipefail
        case "$1" in
          info) exit 0 ;;
          port) echo "127.0.0.1:12130" ;;
          *) echo "unexpected docker command: $*" >&2; exit 9 ;;
        esac
        """
        curl = r"""
        #!/usr/bin/env bash
        set -euo pipefail
        state="${TMPDIR:-/tmp}/ksolver-kss-wait-ready-count"
        count=0
        if [[ -f "$state" ]]; then
          count="$(cat "$state")"
        fi
        count=$((count + 1))
        echo "$count" > "$state"
        if (( count >= 2 )); then
          exit 0
        fi
        exit 22
        """
        result = self.run_with_fakes(docker, curl, "wait-ready-urls", "1", "12120", "/tmp/cache", "5")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "http://127.0.0.1:12130")

    def test_wait_ready_urls_exits_two_after_timeout(self) -> None:
        docker = r"""
        #!/usr/bin/env bash
        set -euo pipefail
        case "$1" in
          info) exit 0 ;;
          port) echo "127.0.0.1:12130" ;;
          *) echo "unexpected docker command: $*" >&2; exit 9 ;;
        esac
        """
        curl = r"""
        #!/usr/bin/env bash
        exit 22
        """
        result = self.run_with_fakes(docker, curl, "wait-ready-urls", "1", "12120", "/tmp/cache", "0")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("within 0s", result.stderr)
        self.assertIn("kss-pool.sh status 1 12120 /tmp/cache", result.stderr)

    def test_wait_ready_urls_rejects_negative_timeout_before_docker(self) -> None:
        docker = r"""
        #!/usr/bin/env bash
        echo "docker should not be called" >&2
        exit 9
        """
        curl = r"""
        #!/usr/bin/env bash
        echo "curl should not be called" >&2
        exit 9
        """
        result = self.run_with_fakes(docker, curl, "wait-ready-urls", "1", "12120", "/tmp/cache", "-1")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("wait_timeout_seconds must be a non-negative integer", result.stderr)
        self.assertNotIn("docker should not be called", result.stderr)

    def test_wait_ready_urls_rejects_non_integer_timeout_before_docker(self) -> None:
        docker = r"""
        #!/usr/bin/env bash
        echo "docker should not be called" >&2
        exit 9
        """
        curl = r"""
        #!/usr/bin/env bash
        echo "curl should not be called" >&2
        exit 9
        """
        result = self.run_with_fakes(docker, curl, "wait-ready-urls", "1", "12120", "/tmp/cache", "soon")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("wait_timeout_seconds must be a non-negative integer", result.stderr)
        self.assertNotIn("docker should not be called", result.stderr)

    def test_start_passes_f2_apiserver_fixes_to_cluster_container(self):
        # Regression guard for the F2 fix: the KWOK cluster MUST be started with both apiserver
        # workarounds, or live baselines silently break (pod import 500s / reset never drains).
        # These are two easily-deletable flags in a shell script, so pin them with a test.
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp)
            bindir = tmp / "bin"
            bindir.mkdir()
            runlog = tmp / "docker-runs.log"
            docker_body = f"""
            #!/usr/bin/env bash
            set -uo pipefail
            case "$1" in
              info) exit 0 ;;
              image) exit 0 ;;      # image inspect -> images present (preflight passes)
              network) exit 0 ;;    # network create/inspect
              rm) exit 0 ;;
              run) echo "$*" >> "{runlog}"; echo fakeid; exit 0 ;;
              exec)
                # discover_etcd_port: netstat probe -> a port; wget /version health check -> ok
                if printf '%s' "$*" | grep -q netstat; then echo 2379; fi
                exit 0 ;;
              *) echo "unexpected docker: $*" >&2; exit 9 ;;
            esac
            """
            write_executable(bindir / "docker", docker_body)
            write_executable(bindir / "curl", "#!/usr/bin/env bash\nexit 0\n")
            env = os.environ.copy()
            env["PATH"] = f"{bindir}:{env['PATH']}"
            env["KSOLVER_KSS_POOL_STATE_DIR"] = str(tmp / "state")
            result = subprocess.run(
                [str(SCRIPT), "start", "1", "12120", str(tmp / "cache"), "5"],
                cwd=ROOT,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            log = runlog.read_text(encoding="utf-8") if runlog.exists() else ""
            cluster_runs = [
                line for line in log.splitlines() if "ksolver-kss-0-cluster" in line
            ]
            self.assertTrue(
                cluster_runs,
                f"cluster container was not `docker run`; rc={result.returncode} "
                f"stderr={result.stderr[:400]} log={log[:400]}",
            )
            line = cluster_runs[0]
            self.assertIn("--kube-admission=false", line, "F2: ServiceAccount admission must be off")
            self.assertIn(
                "kube-apiserver=etcd-prefix=/kube-scheduler-simulator",
                line,
                "F2: apiserver etcd-prefix must match reset.go so /reset drains",
            )


if __name__ == "__main__":
    unittest.main()
