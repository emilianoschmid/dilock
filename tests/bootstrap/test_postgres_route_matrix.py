"""Static guards for the PostgreSQL route catalog and setup transaction boundary."""
from __future__ import annotations

import json
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]

ACTIVE_SDKS = {
    "c", "clojure", "cpp", "crystal", "d", "dart", "dotnet", "elixir",
    "erlang", "fortran", "fsharp", "go", "groovy", "haskell", "java",
    "javascript", "julia", "kotlin", "lua", "nim", "ocaml", "perl", "php",
    "powershell", "python", "r", "ruby", "rust", "scala", "swift",
    "typescript", "vbnet", "zig",
}

NATIVE_PROVISIONERS = {
    "sdk/cpp/src/postgres.cpp": r"BEGIN",
    "sdk/dotnet/Dilock.Postgres/PostgresProvider.cs": r"BeginTransaction",
    "sdk/dart/postgres/lib/dilock_postgres.dart": r"BEGIN",
    "sdk/elixir/lib/dilock/postgres.ex": r"Postgrex\.transaction",
    "sdk/go/postgres/provider.go": r"Begin\(",
    "sdk/java/postgres/src/main/java/org/dilock/postgres/PostgresProvider.java": r"setAutoCommit\(false\)",
    "sdk/julia/src/Postgres.jl": r'"BEGIN"',
    "sdk/lua/generic/lib/dilock/postgres.lua": r'"BEGIN"',
    "sdk/perl/lib/Dilock/Postgres.pm": r"AutoCommit => 0",
    "sdk/php/src/PostgresProvider.php": r"beginTransaction",
    "sdk/r/R/postgres.R": r"dbBegin",
    "sdk/ruby/lib/dilock/postgres.rb": r"\.transaction do",
    "sdk/rust/postgres/src/lib.rs": r"\.transaction\(\)",
    "sdk/swift/Sources/DilockPostgres/PostgresProvider.swift": r'"BEGIN"',
    "sdk/typescript/packages/postgres/src/postgres.ts": r'"BEGIN"',
}

EXPECTED_POSTGRES_CI_JOBS = {
    "postgres-advisory",
    "postgres-advisory-typescript",
    "postgres-advisory-cross-language",
    "postgres-advisory-dotnet",
    "postgres-advisory-cpp",
    "postgres-advisory-cabi-facades",
    "postgres-advisory-haskell",
    "postgres-advisory-java",
    "postgres-advisory-scala",
    "postgres-advisory-kotlin",
    "postgres-advisory-groovy",
    "postgres-advisory-go",
    "postgres-advisory-rust",
    "postgres-advisory-elixir",
    "postgres-advisory-dart",
    "postgres-advisory-swift",
    "postgres-advisory-julia",
    "postgres-advisory-php",
    "postgres-advisory-ruby",
    "postgres-advisory-perl",
    "postgres-advisory-r",
    "postgres-advisory-clojure",
}

EXPECTED_POSTGRES_CI_SDK_COVERAGE = {
    "postgres-advisory": {"python"},
    "postgres-advisory-typescript": {"typescript", "javascript"},
    "postgres-advisory-cross-language": set(),
    "postgres-advisory-dotnet": {"dotnet", "fsharp", "vbnet", "powershell"},
    "postgres-advisory-cpp": {"cpp", "c"},
    "postgres-advisory-cabi-facades": {
        "crystal", "d", "fortran", "lua", "nim", "ocaml", "zig",
    },
    "postgres-advisory-haskell": {"haskell"},
    "postgres-advisory-java": {"java"},
    "postgres-advisory-scala": {"scala"},
    "postgres-advisory-kotlin": {"kotlin"},
    "postgres-advisory-groovy": {"groovy"},
    "postgres-advisory-go": {"go"},
    "postgres-advisory-rust": {"rust"},
    "postgres-advisory-elixir": {"elixir", "erlang"},
    "postgres-advisory-dart": {"dart"},
    "postgres-advisory-swift": {"swift"},
    "postgres-advisory-julia": {"julia"},
    "postgres-advisory-php": {"php"},
    "postgres-advisory-ruby": {"ruby"},
    "postgres-advisory-perl": {"perl"},
    "postgres-advisory-r": {"r"},
    "postgres-advisory-clojure": {"clojure"},
}


class PostgresRouteMatrixTests(unittest.TestCase):
    def test_project_marks_postgresql_experimental(self) -> None:
        project = json.loads((ROOT / "project.json").read_text(encoding="utf-8"))
        self.assertIn("postgresql", project["experimental_backends"])
        self.assertNotIn("postgresql", project["planned_backends"])

    def test_postgresql_cases_cover_every_active_sdk(self) -> None:
        cases = json.loads((ROOT / "conformance/cases.json").read_text(encoding="utf-8"))["cases"]
        postgresql = [case for case in cases if case["id"].startswith("PG-")]
        self.assertEqual(
            [case["id"] for case in postgresql],
            ["PG-001", "PG-002", "PG-003", "PG-004", "PG-005", "PG-006"],
        )
        for case in postgresql:
            self.assertEqual(set(case["sdks"]), ACTIVE_SDKS, case["id"])

    def test_each_native_provisioner_serializes_first_time_metadata_setup(self) -> None:
        for relative_path, transaction_pattern in NATIVE_PROVISIONERS.items():
            source = (ROOT / relative_path).read_text(encoding="utf-8")
            self.assertIn("CREATE TABLE IF NOT EXISTS dilock_namespace_meta", source, relative_path)
            self.assertIn("pg_advisory_xact_lock", source, relative_path)
            self.assertRegex(source, re.compile(transaction_pattern), relative_path)

    def test_swift_ci_selects_swift_testing_runner(self) -> None:
        workflow = (ROOT / ".github/workflows/pr.yml").read_text(encoding="utf-8")
        self.assertIn(
            "DILOCK_POSTGRES_PORT=5432 swift test --disable-xctest --filter DilockPostgresTests",
            workflow,
        )

    def test_typescript_postgres_client_observes_idle_session_errors(self) -> None:
        source = (ROOT / "sdk/typescript/packages/postgres/src/postgres.ts").read_text(encoding="utf-8")
        self.assertIn('client.on?.("error"', source)

    def test_cabi_facade_job_uses_compatible_runtime_baseline(self) -> None:
        workflow = (ROOT / ".github/workflows/pr.yml").read_text(encoding="utf-8")
        for job_id, next_job_id in (
            ("postgres-advisory-cabi-facades", "postgres-advisory-haskell"),
            ("postgres-advisory-haskell", "postgres-advisory-java"),
        ):
            match = re.search(
                rf"(?ms)^  {job_id}:\n(?P<body>.*?)(?=^  {next_job_id}:)",
                workflow,
            )
            self.assertIsNotNone(match, job_id)
            self.assertIn("runs-on: ubuntu-22.04", match.group("body"), job_id)

    def test_ci_has_a_versioned_and_required_job_for_each_postgres_route_group(self) -> None:
        workflow = (ROOT / ".github/workflows/pr.yml").read_text(encoding="utf-8")
        job_ids = re.findall(r"^  (postgres-advisory[^:]*):", workflow, flags=re.MULTILINE)
        self.assertEqual(len(job_ids), 22)
        self.assertEqual(set(job_ids), EXPECTED_POSTGRES_CI_JOBS)
        self.assertEqual(set(job_ids), set(EXPECTED_POSTGRES_CI_SDK_COVERAGE))
        covered_sdks = set().union(*EXPECTED_POSTGRES_CI_SDK_COVERAGE.values())
        self.assertEqual(covered_sdks, ACTIVE_SDKS)
        self.assertEqual(
            workflow.count("Record PostgreSQL client and server versions"),
            len(job_ids),
        )
        required_line = next(
            line for line in workflow.splitlines()
            if "needs:" in line and "postgres-advisory" in line
        )
        for job_id in job_ids:
            self.assertIn(job_id, required_line, job_id)


if __name__ == "__main__":
    unittest.main()
