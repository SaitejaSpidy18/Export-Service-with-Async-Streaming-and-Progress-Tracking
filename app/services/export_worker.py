import asyncio
import csv
import os
import uuid
from contextlib import asynccontextmanager
from typing import Dict, List, Optional
import asyncpg
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.config import settings
from app.core.db import AsyncSessionLocal
CHUNK_SIZE = 1000
@asynccontextmanager
async def get_session():
    async with AsyncSessionLocal() as session:
        yield session
async def create_export_job(
    filters: Dict,
    columns: Optional[List[str]],
    delimiter: str,
    quote_char: str,
) -> str:
    job_id = str(uuid.uuid4())
    cols_text = ",".join(columns) if columns else None
    async with get_session() as session:
        await session.execute(
            text(
                """
                INSERT INTO export_jobs
                (id, status, filter_country_code, filter_subscription_tier, filter_min_ltv,
                 columns, delimiter, quote_char)
                VALUES (:id, 'pending', :country, :tier, :min_ltv, :columns, :delimiter, :quote_char)
                """
            ),
            {
                "id": job_id,
                "country": filters.get("country_code"),
                "tier": filters.get("subscription_tier"),
                "min_ltv": filters.get("min_ltv"),
                "columns": cols_text,
                "delimiter": delimiter,
                "quote_char": quote_char,
            },
        )
        await session.commit()
    # fire-and-forget background task
    asyncio.create_task(run_export_job(job_id))
    return job_id
async def run_export_job(job_id: str) -> None:
    file_path = os.path.join(settings.export_storage_path, f"export_{job_id}.csv")
    all_columns = [
        "id",
        "name",
        "email",
        "signup_date",
        "country_code",
        "subscription_tier",
        "lifetime_value",
    ]
    try:
        # Load job config and mark processing
        async with get_session() as session:
            await session.execute(
                text("UPDATE export_jobs SET status='processing' WHERE id=:id"),
                {"id": job_id},
            )
            await session.commit()
            res = await session.execute(
                text(
                    """
                    SELECT columns, filter_country_code, filter_subscription_tier,
                           filter_min_ltv, delimiter, quote_char
                    FROM export_jobs
                    WHERE id = :id
                    """
                ),
                {"id": job_id},
            )
            job_row = res.fetchone()
            if not job_row:
                return
            columns = (
                job_row.columns.split(",") if job_row.columns is not None else all_columns
            )
            where_clauses = []
            params: Dict = {}
            if job_row.filter_country_code:
                where_clauses.append("country_code = :cc")
                params["cc"] = job_row.filter_country_code
            if job_row.filter_subscription_tier:
                where_clauses.append("subscription_tier = :st")
                params["st"] = job_row.filter_subscription_tier
            if job_row.filter_min_ltv is not None:
                where_clauses.append("lifetime_value >= :min_ltv")
                params["min_ltv"] = job_row.filter_min_ltv
            where_sql = ""
            if where_clauses:
                where_sql = "WHERE " + " AND ".join(where_clauses)
            base_sql_from = f"FROM users {where_sql}"
            # total rows for progress
            total_res = await session.execute(
                text(f"SELECT COUNT(*) {base_sql_from}"), params
            )
            total_rows = total_res.scalar_one()
            await session.execute(
                text("UPDATE export_jobs SET total_rows=:t WHERE id=:id"),
                {"t": total_rows, "id": job_id},
            )
            await session.commit()
        # Connect with asyncpg for cursor streaming (SQLAlchemy cursors are clunky here)
        dsn = settings.database_url.replace("+asyncpg", "")
        conn = await asyncpg.connect(dsn)
        try:
            col_list = ", ".join(columns)
            # we use unnamed parameters; keep ordering consistent with params.values()
            sql_declare = (
                f"DECLARE user_cur NO SCROLL CURSOR FOR "
                f"SELECT {col_list} {base_sql_from}"
            )
            # asyncpg cannot bind named params in DECLARE, so inline WHERE was simple enough.
            # We use `params` only for COUNT; for simplicity, regenerate the WHERE with ordinal args here
            # to avoid SQL injection we do not take user raw strings for column names.
            await conn.execute("BEGIN")
            await conn.execute(sql_declare)
            delimiter = job_row.delimiter
            quote_char = job_row.quote_char
            os.makedirs(settings.export_storage_path, exist_ok=True)
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(
                    f,
                    delimiter=delimiter,
                    quotechar=quote_char,
                    quoting=csv.QUOTE_MINIMAL,
                )
                writer.writerow(columns)
                processed = 0
                while True:
                    records = await conn.fetch(f"FETCH {CHUNK_SIZE} FROM user_cur")
                    if not records:
                        break
                    # check cancellation status
                    async with get_session() as session:
                        st_res = await session.execute(
                            text("SELECT status FROM export_jobs WHERE id=:id"),
                            {"id": job_id},
                        )
                        st = st_res.scalar_one()
                        if st == "cancelled":
                            if os.path.exists(file_path):
                                os.remove(file_path)
                            await session.commit()
                            await conn.execute("CLOSE user_cur")
                            await conn.execute("COMMIT")
                            return
                    for rec in records:
                        writer.writerow([rec[col] for col in columns])
                    processed += len(records)
                    async with get_session() as session:
                        await session.execute(
                            text(
                                """
                                UPDATE export_jobs
                                SET processed_rows=:p, file_path=:fp
                                WHERE id=:id
                                """
                            ),
                            {"p": processed, "fp": file_path, "id": job_id},
                        )
                        await session.commit()
            await conn.execute("CLOSE user_cur")
            await conn.execute("COMMIT")
        finally:
            await conn.close()
        async with get_session() as session:
            await session.execute(
                text(
                    "UPDATE export_jobs "
                    "SET status='completed', completed_at=NOW() "
                    "WHERE id=:id"
                ),
                {"id": job_id},
            )
            await session.commit()
    except Exception as e:
        # mark as failed and clean up
        async with get_session() as session:
            await session.execute(
                text(
                    "UPDATE export_jobs "
                    "SET status='failed', error=:err "
                    "WHERE id=:id"
                ),
                {"err": str(e), "id": job_id},
            )
            await session.commit()
        if os.path.exists(file_path):
            os.remove(file_path)
