from __future__ import annotations

import os
from pathlib import Path

import pytest


def _run_mypy(args: list[str], *, mypy_path: str | None = None) -> tuple[str, str, int]:
    pytest.importorskip("mypy")
    from mypy import api as mypy_api  # type: ignore[import-not-found]

    old_mypypath = os.environ.get("MYPYPATH")
    if mypy_path is not None:
        os.environ["MYPYPATH"] = mypy_path
    try:
        out, err, status = mypy_api.run(args)
    finally:
        if old_mypypath is None:
            os.environ.pop("MYPYPATH", None)
        else:
            os.environ["MYPYPATH"] = old_mypypath

    combined = (out + "\n" + err).lower()
    if (
        'library stubs not installed for "pandas"' in combined
        or 'cannot find implementation or library stub for module named "pandas"' in combined
    ):
        pytest.skip(
            "pandas stubs not installed; install dev deps (pandas-stubs) to run static typing tests"
        )

    return out, err, status


def test_mypy_accepts_col_inner_types() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    case = repo_root / "tests" / "typecheck_cases" / "col_types_ok.py"

    out, err, status = _run_mypy(
        [
            "--config-file",
            str(repo_root / "pyproject.toml"),
            str(case),
        ],
        mypy_path=str(repo_root / "src"),
    )

    assert status == 0, f"mypy should pass. out=\n{out}\nerr=\n{err}"


def test_mypy_rejects_mismatched_col_inner_types() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    case = repo_root / "tests" / "typecheck_cases" / "col_types_bad.py"

    out, err, status = _run_mypy(
        [
            "--config-file",
            str(repo_root / "pyproject.toml"),
            str(case),
        ],
        mypy_path=str(repo_root / "src"),
    )

    # mypy returns non-zero when it finds type errors
    assert status != 0, "mypy should fail when Col[T] inner types are mismatched"

    combined = (out + "\n" + err).lower()
    assert "incompatible types" in combined or "incompatible type" in combined, (
        f"unexpected mypy output. out=\n{out}\nerr=\n{err}"
    )
