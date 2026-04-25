from __future__ import annotations

import sys

import pytest

from agent4quant import cli
from agent4quant.errors import DependencyUnavailableError, ExternalProviderError


def test_cli_prints_clean_external_provider_error(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "fetch_dataset",
        lambda **_kwargs: (_ for _ in ()).throw(
            ExternalProviderError(
                provider="yfinance",
                category="rate_limit",
                message="yfinance rate limited for symbol=AAPL: Too Many Requests",
            )
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "a4q",
            "data",
            "fetch",
            "--provider",
            "yfinance",
            "--symbol",
            "AAPL",
            "--start",
            "2025-01-02",
            "--end",
            "2025-01-03",
            "--interval",
            "1d",
            "--output",
            "output/test.json",
            "--format",
            "json",
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        cli.main()

    captured = capsys.readouterr()
    assert exc_info.value.code == 1
    assert "error[yfinance/rate_limit]" in captured.err
    assert "Traceback" not in captured.err


def test_cli_prints_clean_dependency_error(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "generate_report",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            DependencyUnavailableError("parquet", "Parquet support requires optional pyarrow dependency.")
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "a4q",
            "report",
            "generate",
            "--input",
            "input.json",
            "--output",
            "output/report.pdf",
            "--format",
            "pdf",
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        cli.main()

    captured = capsys.readouterr()
    assert exc_info.value.code == 1
    assert "error[parquet/dependency]" in captured.err
    assert "Traceback" not in captured.err


def test_cli_data_providers_outputs_capability_matrix(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "a4q",
            "data",
            "providers",
            "--format",
            "text",
        ],
    )

    cli.main()

    captured = capsys.readouterr()
    assert "Primary online providers:" in captured.out
    assert "- akshare:" in captured.out
    assert "- yfinance:" in captured.out
    assert "Compatibility providers:" in captured.out


def test_cli_commercial_plans_outputs_text(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "a4q",
            "commercial",
            "plans",
            "--format",
            "text",
        ],
    )

    cli.main()

    captured = capsys.readouterr()
    assert "community:" in captured.out
    assert "professional:" in captured.out


def test_cli_commercial_account_create_outputs_text(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "create_account",
        lambda **_kwargs: {"account_id": "acct-1", "plan": "professional"},
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "a4q",
            "commercial",
            "account-create",
            "--name",
            "Alice",
            "--email",
            "alice@example.com",
            "--plan",
            "professional",
        ],
    )

    cli.main()

    captured = capsys.readouterr()
    assert "Account created: acct-1" in captured.out
