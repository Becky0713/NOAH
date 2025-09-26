from __future__ import annotations

import asyncio
from typing import Any, Dict, List

import httpx

from .config import settings
from .db import create_table_if_not_exists, insert_rows


async def fetch_all_columns_and_rows() -> None:
    # 读取元数据，确定全部字段
    meta_url = f"{settings.socrata_base_url}/api/views/{settings.socrata_dataset_id}"
    headers = {"X-App-Token": settings.socrata_app_token} if settings.socrata_app_token else {}

    async with httpx.AsyncClient(timeout=60) as client:
        meta = (await client.get(meta_url, headers=headers)).json()
        columns = [c["fieldName"] for c in meta.get("columns", []) if c.get("fieldName")]
        if not columns:
            raise RuntimeError("未能从 Socrata 元数据解析到字段列表")

        create_table_if_not_exists(columns)

        # 分页拉取所有数据
        base_url = f"{settings.socrata_base_url}/resource/{settings.socrata_dataset_id}.json"
        limit = 50000  # Socrata 上限通常 50k
        offset = 0
        while True:
            params = {"$select": ", ".join(columns), "$limit": limit, "$offset": offset}
            resp = await client.get(base_url, headers=headers, params=params)
            resp.raise_for_status()
            batch: List[Dict[str, Any]] = resp.json()
            if not batch:
                break
            rows = []
            for record in batch:
                rows.append(tuple(record.get(col) for col in columns))
            insert_rows(columns, rows)
            offset += limit


def main() -> None:
    asyncio.run(fetch_all_columns_and_rows())


if __name__ == "__main__":
    main()


