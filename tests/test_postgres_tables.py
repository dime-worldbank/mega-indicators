"""utils.read_table / write_table / table_exists with DB_BACKEND=postgres.

Needs a PostgreSQL database named prd_mega; the tests are skipped unless
TEST_POSTGRES_DSN points at one, e.g.

    docker run -d -e POSTGRES_PASSWORD=test -e POSTGRES_DB=prd_mega -p 5432:5432 postgres:16
    TEST_POSTGRES_DSN=postgresql://postgres:test@localhost:5432/prd_mega pytest tests/ -v

They create and drop the schema indicator_pgtest.
"""
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

DSN = os.environ.get("TEST_POSTGRES_DSN")
if not DSN:
    pytest.skip("TEST_POSTGRES_DSN is not set", allow_module_level=True)

import psycopg
from psycopg.conninfo import make_conninfo

SCHEMA = "indicator_pgtest"


@pytest.fixture(scope="module")
def utils(tmp_path_factory):
    """utils and config imported afresh in postgres mode, and forgotten afterwards."""
    with pytest.MonkeyPatch.context() as mp:
        mp.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
        mp.delenv("COUNTRY_NAME", raising=False)
        mp.setenv("DATA_ROOT", str(tmp_path_factory.mktemp("data")))
        mp.setenv("DB_BACKEND", "postgres")
        mp.setenv("POSTGRES_DSN", DSN)
        mp.syspath_prepend(str(Path(__file__).resolve().parent.parent))
        for name in ("utils", "config"):
            mp.delitem(sys.modules, name, raising=False)
        import utils
        yield utils
        for name in ("utils", "config"):
            sys.modules.pop(name, None)
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")


def columns_and_types(table):
    with psycopg.connect(DSN) as conn:
        return conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (SCHEMA, table),
        ).fetchall()


def test_round_trip_keeps_column_names_values_and_nulls(utils):
    fetched_at = datetime(2026, 10, 7, 12, 30, tzinfo=timezone.utc)
    df = pd.DataFrame({
        "Country": ["Namibia", "Togo", "Ghana"],
        "country_code_iso2": ["NA", "TG", None],
        "PI-01": [2.5, None, 3.0],
        "year": pd.array([2021, None, 2023], dtype="Int32"),
        "is_aggregate": [False, True, False],
        "fetched_at": [fetched_at] * 3,
    })
    utils.write_table(df, f"prd_mega.{SCHEMA}.Round_Trip")

    assert columns_and_types("round_trip") == [
        ("Country", "text"),
        ("country_code_iso2", "text"),
        ("PI-01", "double precision"),
        ("year", "bigint"),
        ("is_aggregate", "boolean"),
        ("fetched_at", "timestamp with time zone"),
    ]
    out = utils.read_table(f"prd_mega.{SCHEMA}.round_trip")
    assert list(out.columns) == list(df.columns)
    assert out["country_code_iso2"].tolist()[:2] == ["NA", "TG"]
    assert pd.isna(out["country_code_iso2"][2])
    assert out["PI-01"][0] == 2.5 and pd.isna(out["PI-01"][1])
    assert out["year"][0] == 2021 and pd.isna(out["year"][1])
    assert out["is_aggregate"].tolist() == [False, True, False]
    assert (out["fetched_at"] == pd.Timestamp(fetched_at)).all()


def test_columns_selects_a_subset(utils):
    utils.write_table(pd.DataFrame({"a": [1], "B": ["x"], "c": [0.5]}), f"prd_mega.{SCHEMA}.subset")
    assert columns_and_types("subset") == [("a", "bigint"), ("B", "text"), ("c", "double precision")]
    out = utils.read_table(f"prd_mega.{SCHEMA}.subset", columns=["B", "a"])
    assert list(out.columns) == ["B", "a"]
    assert out.values.tolist() == [["x", 1]]


def test_table_exists_ignores_table_name_case(utils):
    assert not utils.table_exists(f"prd_mega.{SCHEMA}.missing")
    utils.write_table(pd.DataFrame({"a": [1]}), f"prd_mega.{SCHEMA}.Health_GHO")
    assert columns_and_types("health_gho") == [("a", "bigint")]
    assert utils.table_exists(f"prd_mega.{SCHEMA}.health_gho")
    assert utils.table_exists(f"prd_mega.{SCHEMA}.Health_GHO")


def test_rewrite_replaces_rows_and_follows_new_columns(utils):
    table = f"prd_mega.{SCHEMA}.rewrite"
    utils.write_table(pd.DataFrame({"a": [1, 2, 3]}), table)
    utils.write_table(pd.DataFrame({"a": [4]}), table)
    assert utils.read_table(table)["a"].tolist() == [4]

    utils.write_table(pd.DataFrame({"a": ["four"], "b": [True]}), table)
    assert columns_and_types("rewrite") == [("a", "text"), ("b", "boolean")]
    assert utils.read_table(table).values.tolist() == [["four", True]]


def test_object_column_of_booleans_with_nulls_stays_boolean(utils):
    df = pd.DataFrame({"is_forecast": pd.Series([True, None, False], dtype=object)})
    utils.write_table(df, f"prd_mega.{SCHEMA}.object_bool")
    assert columns_and_types("object_bool") == [("is_forecast", "boolean")]


def test_versioned_dataframe_snapshots_and_reads_back(utils, monkeypatch):
    class Response:
        content = b"Country,PI-01,code\nNamibia,A,NA\nTogo,,TG\n"
        def raise_for_status(self):
            pass
    monkeypatch.setattr(utils.requests, "get", lambda url, timeout: Response())
    table = f"prd_mega.{SCHEMA}.pefa_bronze"
    df = utils.versioned_dataframe("https://example.org/pefa.csv", table, update_version=True)
    assert columns_and_types("pefa_bronze")[-1] == ("fetched_at", "timestamp with time zone")

    def unreachable(url, timeout):
        raise AssertionError("an existing snapshot must be read without fetching")
    monkeypatch.setattr(utils.requests, "get", unreachable)
    cached = utils.versioned_dataframe("https://example.org/pefa.csv", table, update_version=False)
    assert list(cached.columns) == ["Country", "PI-01", "code"]
    assert cached["PI-01"][0] == "A" and pd.isna(cached["PI-01"][1])
    pd.testing.assert_frame_equal(cached, df)


def test_country_name_restricts_rows(utils, monkeypatch):
    monkeypatch.setattr(utils, "COUNTRY_NAME", "Togo")
    df = pd.DataFrame({"country_name": ["Togo", "Ghana", "Togo"], "year": [2021, 2021, 2022]})
    utils.write_table(df, f"prd_mega.{SCHEMA}.one_country")
    assert utils.read_table(f"prd_mega.{SCHEMA}.one_country")["year"].tolist() == [2021, 2022]


def test_table_of_another_catalog_does_not_exist(utils):
    # country.py falls back to a built-in table when the corporate one is absent
    corporate = "prd_corpdata.dm_reference_gold.v_dim_country_currency_exchange_rate"
    assert not utils.table_exists(corporate)
    with pytest.raises(RuntimeError, match="named 'prd_corpdata'"):
        utils.read_table(corporate)


def test_database_must_be_named_after_the_catalog(utils, monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", make_conninfo(DSN, dbname="postgres"))
    with pytest.raises(RuntimeError, match="named 'prd_mega'"):
        utils.read_table(f"prd_mega.{SCHEMA}.subset")


def test_overlong_column_name_is_refused(utils):
    with pytest.raises(ValueError, match="63-byte"):
        utils.write_table(pd.DataFrame({"x" * 64: [1]}), f"prd_mega.{SCHEMA}.too_long")
