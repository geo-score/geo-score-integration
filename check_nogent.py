"""Quick diagnostic: list all sections in Nogent-sur-Marne (94052) with their DVF prices."""

from sqlalchemy import text

from settings.db import engine

with engine.connect() as conn:
    print("=== sections table for 94052 ===")
    rows = conn.execute(text(
        "SELECT section_id, code FROM dvf_prices.sections WHERE commune='94052' ORDER BY section_id"
    )).fetchall()
    print(f"{len(rows)} rows")
    for r in rows[:5]:
        print(f"  {r.section_id}")

    print("\n=== y2023 rows for 94052 (any) ===")
    rows = conn.execute(text(
        "SELECT section_id, prix_m2_median, nb_ventes FROM dvf_prices.y2023 WHERE section_id LIKE '94052%' ORDER BY section_id"
    )).fetchall()
    print(f"{len(rows)} rows")
    for r in rows[:10]:
        print(f"  {r.section_id}  median={r.prix_m2_median}  n={r.nb_ventes}")

    print("\n=== total y2023 rows (all deps) ===")
    count = conn.execute(text("SELECT COUNT(*) FROM dvf_prices.y2023")).scalar()
    print(f"{count} rows")

    print("\n=== sample y2023 section_ids ===")
    rows = conn.execute(text(
        "SELECT section_id FROM dvf_prices.y2023 LIMIT 10"
    )).fetchall()
    for r in rows:
        print(f"  {r.section_id}")
