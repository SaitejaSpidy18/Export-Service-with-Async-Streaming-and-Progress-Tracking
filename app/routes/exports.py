import gzip
import os
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.db import get_db
from app.services.export_worker import create_export_job
router = APIRouter(prefix="/exports", tags=["exports"])
@router.post("/csv", status_code=202)
async def initiate_export(
    country_code: Optional[str] = None,
    subscription_tier: Optional[str] = None,
    min_ltv: Optional[float] = None,
    columns: Optional[str] = None,
    delimiter: str = ",",
    quoteChar: str = "\"",
):
    if len(delimiter) != 1 or len(quoteChar) != 1:
        raise HTTPException(
            status_code=400,
            detail="delimiter and quoteChar must be single characters",
        )
    cols_list = columns.split(",") if columns else None
    filters = {
        "country_code": country_code,
        "subscription_tier": subscription_tier,
        "min_ltv": min_ltv,
    }
    export_id = await create_export_job(filters, cols_list, delimiter, quoteChar)
    return {"exportId": export_id, "status": "pending"}
@router.get("/{export_id}/status")
async def export_status(export_id: str, db: AsyncSession = Depends(get_db)):
    res = await db.execute(
        text(
            """
            SELECT id, status, total_rows, processed_rows, error,
                   created_at, completed_at
            FROM export_jobs
            WHERE id = :id
            """
        ),
        {"id": export_id},
    )
    row = res.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Export not found")
    total = row.total_rows or 0
    processed = row.processed_rows or 0
    pct = float(processed) / total * 100 if total > 0 else 0.0
    return {
        "exportId": row.id,
        "status": row.status,
        "progress": {
            "totalRows": total,
            "processedRows": processed,
            "percentage": pct,
        },
        "error": row.error,
        "createdAt": row.created_at.isoformat(),
        "completedAt": row.completed_at.isoformat() if row.completed_at else None,
    }
def iter_file_range(path: str, start: int, end: Optional[int]):
    with open(path, "rb") as f:
        f.seek(start)
        remaining = None if end is None else end - start + 1
        chunk_size = 8192
        while True:
            if remaining is not None and remaining <= 0:
                break
            to_read = chunk_size if remaining is None else min(chunk_size, remaining)
            data = f.read(to_read)
            if not data:
                break
            if remaining is not None:
                remaining -= len(data)
            yield data
def gzip_stream(generator):
    compressor = gzip.compressobj()
    for chunk in generator:
        data = compressor.compress(chunk)
        if data:
            yield data
    tail = compressor.flush()
    if tail:
        yield tail
@router.get("/{export_id}/download")
async def download_export(
    export_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    res = await db.execute(
        text("SELECT status, file_path FROM export_jobs WHERE id=:id"),
        {"id": export_id},
    )
    row = res.fetchone()
    if not row or row.status != "completed":
        # spec: 404 or 425 before ready
        raise HTTPException(status_code=404, detail="Export not ready")
    file_path = row.file_path
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    file_size = os.path.getsize(file_path)
    headers = {
        "Content-Type": "text/csv",
        "Content-Disposition": f'attachment; filename="export_{export_id}.csv"',
        "Accept-Ranges": "bytes",
    }
    range_header = request.headers.get("Range")
    start = 0
    end = file_size - 1
    status_code = 200
    if range_header:
        # e.g. "bytes=0-1023"
        try:
            units, _, range_spec = range_header.partition("=")
            if units.strip().lower() == "bytes":
                start_str, _, end_str = range_spec.partition("-")
                if start_str:
                    start = int(start_str)
                if end_str:
                    end = int(end_str)
        except ValueError:
            raise HTTPException(status_code=416, detail="Invalid Range header")
        if start > end or start < 0 or end >= file_size:
            raise HTTPException(status_code=416, detail="Invalid range")
        status_code = 206
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
        headers["Content-Length"] = str(end - start + 1)
    else:
        headers["Content-Length"] = str(file_size)
    generator = iter_file_range(file_path, start, end)
    accept_encoding = request.headers.get("Accept-Encoding", "")
    if "gzip" in accept_encoding.lower():
        # gzip on the fly; omit Content-Length and use chunked transfer
        headers.pop("Content-Length", None)
        headers["Content-Encoding"] = "gzip"
        return StreamingResponse(
            gzip_stream(generator),
            status_code=status_code,
            headers=headers,
        )
    return StreamingResponse(generator, status_code=status_code, headers=headers)
@router.delete("/{export_id}", status_code=204)
async def cancel_export(export_id: str, db: AsyncSession = Depends(get_db)):
    res = await db.execute(
        text("SELECT status, file_path FROM export_jobs WHERE id=:id"),
        {"id": export_id},
    )
    row = res.fetchone()
    if not row:
        return Response(status_code=204)
    if row.status in ("completed", "failed", "cancelled"):
        return Response(status_code=204)
    await db.execute(
        text("UPDATE export_jobs SET status='cancelled' WHERE id=:id"),
        {"id": export_id},
    )
    await db.commit()
    # worker will delete the file when it observes status='cancelled'
    return Response(status_code=204)
