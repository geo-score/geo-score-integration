"""Database schema and table management utilities."""

from rich.console import Console
from sqlalchemy import text

from settings.db import engine

console = Console()


def ensure_schema(schema: str):
    """Create a PostgreSQL schema if it doesn't exist."""
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.commit()


def delete_existing_departments(qualified_table: str, departements: list[str]):
    """Delete rows for given departments (idempotent upsert)."""
    with engine.connect() as conn:
        table_exists = conn.execute(
            text("SELECT to_regclass(:t)"),
            {"t": qualified_table},
        ).scalar()

        if table_exists:
            dep_list = ",".join(f"'{d}'" for d in departements)
            conn.execute(text(
                f"DELETE FROM {qualified_table} WHERE departement IN ({dep_list})"
            ))
            conn.commit()
            console.print(f"  Cleared existing data for departments {departements}")
