import os

import pandas as pd

from sqlalchemy import (
    create_engine,
    inspect,
    text,
)
from sqlalchemy.engine import Engine


def get_postgres_engine() -> Engine:
    """Create a PostgreSQL SQLAlchemy engine from environment variables."""

    host = os.environ["POSTGRES_HOST"]
    port = os.environ["POSTGRES_PORT"]
    database = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]

    connection_url = (
        f"postgresql+psycopg2://"
        f"{user}:{password}@{host}:{port}/{database}"
    )

    return create_engine(
        connection_url,
        pool_pre_ping=True,
    )


def get_postgres_dtype(dtype):
    """Map a Pandas dtype to a PostgreSQL data type."""

    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"

    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"

    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"

    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"

    if pd.api.types.is_object_dtype(dtype):
        return "TEXT"

    return "TEXT"

def create_table_from_dataframe(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    unique_columns: list[str] | None = None,
):
    """Create a PostgreSQL table dynamically from a DataFrame."""

    if df.empty:
        raise ValueError(
            "Cannot create a table from an empty DataFrame."
        )

    columns = []

    for column in df.columns:

        postgres_type = get_postgres_dtype(
            df[column].dtype
        )

        columns.append(
            f'"{column}" {postgres_type}'
        )

    if unique_columns:

        unique_columns_sql = ", ".join(
            f'"{column}"'
            for column in unique_columns
        )

        columns.append(
            f"UNIQUE ({unique_columns_sql})"
        )

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            {", ".join(columns)}
        );
    """

    with engine.begin() as connection:
        connection.execute(
            text(create_table_sql)
        )


def table_exists(
    table_name: str,
    engine: Engine,
) -> bool:
    """Check whether a PostgreSQL table exists."""

    inspector = inspect(engine)

    return inspector.has_table(table_name)


def insert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    unique_columns: list[str],
):
    """Insert a DataFrame into PostgreSQL and skip duplicate rows."""

    if df.empty:
        raise ValueError(
            "Cannot insert an empty DataFrame."
        )

    records = df.to_dict(orient="records")

    with engine.begin() as connection:

        columns = ", ".join(
            f'"{column}"'
            for column in df.columns
        )

        values = ", ".join(
            f":{column}"
            for column in df.columns
        )

        conflict_columns = ", ".join(
            f'"{column}"'
            for column in unique_columns
        )

        sql = text(
            f"""
            INSERT INTO "{table_name}"
            ({columns})
            VALUES ({values})
            ON CONFLICT ({conflict_columns})
            DO NOTHING;
            """
        )

        connection.execute(
            sql,
            records,
        )