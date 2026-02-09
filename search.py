# -*- coding: utf-8 -*-
"""
End-to-end: User Query -> Milvus Hybrid Search -> Table selection loop via Ollama -> MySQL schema -> Ollama SQL -> Execute
"""

import os
import json
import time
import re
from typing import List, Dict, Any, Tuple, Optional

import numpy as np
import requests
import csv2recap

# ---- BGE-M3 ----
from FlagEmbedding import BGEM3FlagModel

# ---- Milvus ----
from pymilvus import MilvusClient, AnnSearchRequest, WeightedRanker

# ---- MySQL ----
import mysql.connector

# =========================
# 1) Milvus Hybrid Search
# =========================
class MilvusHybridSearcher:
    def __init__(self, uri: str, collection_name: str):
        self.client = MilvusClient(uri=uri)
        self.collection_name = collection_name

    def hybrid_search_tables(
        self,
        query: str,
        limit: int = 10,
        exclude_filenames: Optional[set] = None,
        dense_weight: float = 0.3,
        sparse_weight: float = 0.7,
    ) -> List[Dict[str, Any]]:
        exclude_filenames = exclude_filenames or set()

        q_dense, q_sparse = csv2recap.generate_embeddings([query])

        req_dense = AnnSearchRequest(
            data=q_dense,
            anns_field="dense_vector",
            param={"metric_type": "L2", "params": {"nprobe": 10}},
            limit=limit,
        )

        req_sparse = AnnSearchRequest(
            data=q_sparse,
            anns_field="sparse_vector",
            param={"metric_type": "IP", "params": {"drop_ratio_search": 0.2}},
            limit=limit,
        )

        ranker = WeightedRanker(dense_weight, sparse_weight)

        results = self.client.hybrid_search(
            collection_name=self.collection_name,
            reqs=[req_dense, req_sparse],
            ranker=ranker,
            limit=limit,
            output_fields=["filename", "text"],
        )

        # pymilvus returns list-of-list; normalize
        hits = []
        for batch in results:
            for h in batch:
                row = {
                    "filename": h.get("entity", {}).get("filename"),
                    "text": h.get("entity", {}).get("text"),
                    "score": h.get("score"),
                }
                if row["filename"] and row["filename"] not in exclude_filenames:
                    hits.append(row)

        # sort by score desc (just in case)
        hits.sort(key=lambda x: (x["score"] is None, x["score"]), reverse=True)

        # de-dup by filename, keep best
        seen = set()
        uniq = []
        for r in hits:
            fn = r["filename"]
            if fn in seen:
                continue
            seen.add(fn)
            uniq.append(r)

        return uniq


# =========================
# 2) Ollama Client + Prompting
# =========================
class OllamaClient:
    def __init__(self, base_url: str = "http://localhost:11434", model: str = "gpt-oss:20b", timeout: int = 120):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout

    def chat(self, messages: List[Dict[str, str]], temperature: float = 0.1) -> str:
        url = f"{self.base_url}/api/chat"
        payload = {
            "model": self.model,
            "messages": messages,
            "options": {"temperature": temperature},
            "stream": False,
        }
        r = requests.post(url, json=payload, timeout=self.timeout)
        r.raise_for_status()
        data = r.json()
        return data["message"]["content"]

def _extract_json_strict(text: str) -> Dict[str, Any]:
    """
    Extract first JSON object from model output; fail loudly if not possible.
    """
    # try direct
    text = text.strip()
    if text.startswith("{") and text.endswith("}"):
        return json.loads(text)

    # fallback: find json block
    m = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not m:
        raise ValueError(f"Model output has no JSON object:\n{text}")
    return json.loads(m.group(0))


def build_prompt_need_more_tables(
    user_query: str,
    selected_tables: List[Dict[str, Any]],
) -> List[Dict[str, str]]:
    """
    Step 2 prompt:
    - decide if more tables are needed
    - if yes, produce a better Milvus search query that will surface missing tables
    """
    tables_brief = [
        {
            "filename": t["filename"],
            "text": (t.get("text") or "")[:600],
        }
        for t in selected_tables
    ]

    system = (
        "You are a data engineer assistant. "
        "Given a user information need and currently selected MySQL tables (with short descriptions), "
        "decide whether additional tables are required to answer the query correctly.\n\n"
        "You MUST output ONLY valid JSON with the following schema:\n"
        "{\n"
        '  "need_more": boolean,\n'
        '  "reason": string,\n'
        '  "milvus_query": string\n'
        "}\n\n"
        "Rules:\n"
        "- If need_more is false, milvus_query MUST be an empty string.\n"
        "- If need_more is true, milvus_query should be a short Korean search query suitable for Milvus (user-intent + missing concepts).\n"
        "- Do not mention tables that are already selected as missing; focus on missing dimensions.\n"
    )

    user = (
        f"USER_QUERY:\n{user_query}\n\n"
        f"SELECTED_TABLES (filename=text snippet):\n{json.dumps(tables_brief, ensure_ascii=False, indent=2)}\n"
    )

    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def build_prompt_generate_mysql_sql(
    user_query: str,
    table_schemas: Dict[str, List[Dict[str, str]]],
) -> List[Dict[str, str]]:
    """
    Step 6 prompt: generate MySQL SELECT query only, with safe constraints.
    """
    system = (
        "You are an expert MySQL query writer.\n"
        "Given a user query and table schemas, write a single MySQL SELECT statement that answers the query.\n\n"
        "You MUST output ONLY valid JSON with the following schema:\n"
        "{\n"
        '  "sql": string,\n'
        '  "notes": string\n'
        "}\n\n"
        "Rules (critical):\n"
        "- Output ONLY a SELECT query (no INSERT/UPDATE/DELETE/DDL).\n"
        "- Prefer explicit column names (avoid SELECT *).\n"
        "- Use LIMIT 200 unless the user explicitly asks for all rows.\n"
        "- If date filtering is implied (e.g., '2024년5월'), implement it robustly.\n"
        "- If you need to union multiple tables, do it carefully with aligned columns.\n"
    )

    user = (
        f"USER_QUERY:\n{user_query}\n\n"
        f"TABLE_SCHEMAS (table -> columns):\n{json.dumps(table_schemas, ensure_ascii=False, indent=2)}\n"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


# =========================
# 5) MySQL schema + execution
# =========================
class MySQLRunner:
    def __init__(self, host: str, user: str, password: str, database: str, port: int = 3306):
        self.conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database, port=port
        )

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def describe_table(self, table: str) -> List[Dict[str, str]]:
        """
        Returns list like: [{"Field":..., "Type":..., "Null":..., "Key":..., "Default":..., "Extra":...}, ...]
        """
        cur = self.conn.cursor(dictionary=True)
        cur.execute(f"DESCRIBE `{table}`")
        rows = cur.fetchall()
        cur.close()
        return rows

    def run_select(self, sql: str) -> List[Dict[str, Any]]:
        cur = self.conn.cursor(dictionary=True)
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows


def is_safe_select(sql: str) -> bool:
    """
    Very basic guardrail:
    - must start with SELECT or WITH ... SELECT
    - reject semicolon + multiple statements
    - reject obvious DDL/DML keywords
    """
    s = sql.strip()
    if ";" in s[:-1]:  # allow trailing semicolon only
        return False

    lowered = re.sub(r"\s+", " ", s.lower())
    bad = ["insert ", "update ", "delete ", "drop ", "alter ", "create ", "truncate ", "grant ", "revoke "]
    if any(k in lowered for k in bad):
        return False

    return lowered.startswith("select ") or lowered.startswith("with ")


# =========================
# Orchestration (1~6)
# =========================
def main():
    # ---- Config ----
    milvus_uri = os.getenv("MILVUS_URI", "http://localhost:19530")
    collection_name = os.getenv("MILVUS_COLLECTION", "seoul_transport")

    ollama_url = os.getenv("OLLAMA_URL", "http://localhost:11434")
    ollama_model = os.getenv("OLLAMA_MODEL", "gpt-oss:20b")

    mysql_host = os.getenv("MYSQL_HOST", "localhost")
    mysql_user = os.getenv("MYSQL_USER", "root")
    mysql_password = os.getenv("MYSQL_PASSWORD", "Passw0rd1!")
    mysql_db = os.getenv("MYSQL_DB", "seoul_transport")
    mysql_port = int(os.getenv("MYSQL_PORT", "3306"))

    # ---- Inputs ----
    user_query = os.getenv("USER_QUERY", "2024년5월과 6월  지하철 망포 총승차승객수는?")
    #user_query = os.getenv("USER_QUERY", "2024년1월9701번 버스  총승차승객수는?")

    # ---- Clients ----
    searcher = MilvusHybridSearcher(uri=milvus_uri, collection_name=collection_name)
    llm = OllamaClient(base_url=ollama_url, model=ollama_model)
    mysql = MySQLRunner(host=mysql_host, user=mysql_user, password=mysql_password, database=mysql_db, port=mysql_port)

    try:
        # 1) initial milvus query -> pick best table (top-1), but keep top-k candidates
        exclude = set()
        initial_hits = searcher.hybrid_search_tables(user_query, limit=10, exclude_filenames=exclude)
        if not initial_hits:
            raise RuntimeError("Milvus search returned no tables. Check collection content/embeddings.")

        selected = [initial_hits[0]]
        exclude.add(initial_hits[0]["filename"])

        # 2~4) loop until no missing tables
        max_rounds = 5
        round_idx = 0
        while True:
            round_idx += 1
            if round_idx > max_rounds:
                print(f"[WARN] Reached max_rounds={max_rounds}. Proceeding with current tables.")
                break

            # 2) ask ollama if more tables needed; if yes, request milvus_query
            msgs = build_prompt_need_more_tables(user_query, selected)
            out = llm.chat(msgs, temperature=0.1)
            decision = _extract_json_strict(out)

            need_more = bool(decision.get("need_more", False))
            reason = str(decision.get("reason", ""))
            milvus_query = str(decision.get("milvus_query", "") or "").strip()

            print(f"\n[ROUND {round_idx}] need_more={need_more} reason={reason}")
            if not need_more:
                break

            if not milvus_query:
                print("[WARN] need_more=True but milvus_query empty. Fallback to original user_query.")
                milvus_query = user_query

            # 3) search missing tables via milvus_query; exclude already selected
            new_hits = searcher.hybrid_search_tables(milvus_query, limit=10, exclude_filenames=exclude)
            if not new_hits:
                print("[WARN] No additional tables found from Milvus for milvus_query:", milvus_query)
                break

            # pick top-1 new table each iteration (you can also add multiple if desired)
            new_table = new_hits[0]
            selected.append(new_table)
            exclude.add(new_table["filename"])
            print(f"[ADD] {new_table['filename']} score={new_table.get('score')}")

            # 4) loop continues to step 2 with updated selected list

        # 5) describe each selected table in MySQL
        table_schemas: Dict[str, List[Dict[str, str]]] = {}
        for t in selected:
            table = t["filename"]
            desc = mysql.describe_table(table)
            # store only key fields to keep prompt small
            table_schemas[table] = [
                {
                    "Field": r.get("Field"),
                    "Type": r.get("Type"),
                    "Null": r.get("Null"),
                    "Key": r.get("Key"),
                }
                for r in desc
            ]

        # 6) generate mysql SQL via ollama and execute
        msgs_sql = build_prompt_generate_mysql_sql(user_query, table_schemas)
        out_sql = llm.chat(msgs_sql, temperature=0.1)
        sql_obj = _extract_json_strict(out_sql)
        sql = (sql_obj.get("sql") or "").strip()
        notes = (sql_obj.get("notes") or "").strip()

        print("\n[Ollama Notes]\n", notes)
        print("\n[Generated SQL]\n", sql)

        if not is_safe_select(sql):
            raise RuntimeError("Generated SQL failed safety check (must be a single SELECT). Refusing to execute.")

        rows = mysql.run_select(sql)
        print(f"user query : {user_query}")
        print(f"\n[RESULT] rows={len(rows)}")
        # print a small preview
        preview_n = min(20, len(rows))
        for i in range(preview_n):
            print(rows[i])

    finally:
        mysql.close()


if __name__ == "__main__":
    main()

