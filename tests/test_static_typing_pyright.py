from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest


def _run_pyright(args: list[str]) -> subprocess.CompletedProcess[str]:
    try:
        import pyright  # noqa: F401
    except Exception:
        pytest.skip("pyright is not installed")

    return subprocess.run(
        [sys.executable, "-m", "pyright", "--pythonpath", sys.executable, *args],
        text=True,
        capture_output=True,
        check=False,
    )


def test_pyright_accepts_col_inner_types() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    project = repo_root / "pyrightconfig.json"
    case = repo_root / "tests" / "typecheck_cases" / "col_types_ok.py"

    res = _run_pyright(["--project", str(project), str(case)])
    assert res.returncode == 0, f"pyright should pass.\n{res.stdout}\n{res.stderr}"


def test_pyright_rejects_mismatched_col_inner_types() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    project = repo_root / "pyrightconfig.json"
    case = repo_root / "tests" / "typecheck_cases" / "col_types_bad.py"

    res = _run_pyright(["--project", str(project), str(case)])
    assert res.returncode != 0, "pyright should fail when Col[T] inner types are mismatched"

    combined = (res.stdout + "\n" + res.stderr).lower()
    # Wording varies by pyright version; keep this check intentionally broad.
    assert (
        "is not assignable" in combined
        or "cannot be assigned" in combined
        or "incompatible" in combined
    ), f"Unexpected pyright output:\n{res.stdout}\n{res.stderr}"
