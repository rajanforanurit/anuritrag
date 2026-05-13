from __future__ import annotations

import logging
import os
from typing import List, Optional

logger = logging.getLogger(__name__)

SEARCH_ENDPOINT  = os.getenv("AZURE_SEARCH_ENDPOINT", "")
SEARCH_API_KEY   = os.getenv("AZURE_SEARCH_KEY", "")
INDEX_NAME       = os.getenv("AZURE_SEARCH_INDEX", "rag-chunks")
API_VERSION      = "2024-07-01"
EMBEDDING_DIM    = int(os.getenv("EMBEDDING_DIM", "384"))  


def _headers() -> dict:
    return {
        "Content-Type": "application/json",
        "api-key": SEARCH_API_KEY,
    }


def _check_config():
    if not SEARCH_ENDPOINT or not SEARCH_API_KEY:
        raise ValueError(
            "AZURE_SEARCH_ENDPOINT and AZURE_SEARCH_KEY must be set in environment variables."
        )


# ── Index management ───────────────────────────────────────────────────────────

def create_index() -> dict:
    """
    Create (or update) the Azure AI Search index.
    Run this ONCE when setting up a new environment.
    Safe to re-run — uses create-or-update semantics.
    """
    import requests

    _check_config()

    url = f"{SEARCH_ENDPOINT}/indexes/{INDEX_NAME}?api-version={API_VERSION}"

    schema = {
        "name": INDEX_NAME,
        "fields": [
            # Primary key — must be unique across ALL clients
            {
                "name": "id",
                "type": "Edm.String",
                "key": True,
                "filterable": True,
                "searchable": False,
            },
            # Tenant isolation field — always filter on this
            {
                "name": "client_id",
                "type": "Edm.String",
                "filterable": True,
                "searchable": False,
                "sortable": False,
                "facetable": False,
            },
            # Document grouping — used for deletion by doc
            {
                "name": "doc_id",
                "type": "Edm.String",
                "filterable": True,
                "searchable": False,
            },
            # Full chunk text — searched by BM25 and semantic ranker
            {
                "name": "text",
                "type": "Edm.String",
                "searchable": True,
                "filterable": False,
                "sortable": False,
                "facetable": False,
                "analyzer": "standard.lucene",
            },
            # Vector embedding — searched by HNSW
            {
                "name": "embedding",
                "type": "Collection(Edm.Single)",
                "searchable": True,
                "filterable": False,
                "retrievable": False,   # save bandwidth — we only need text back
                "dimensions": EMBEDDING_DIM,
                "vectorSearchProfile": "hnsw-profile",
            },
            # Metadata fields — returned in results
            {
                "name": "source_file",
                "type": "Edm.String",
                "filterable": True,
                "searchable": False,
            },
            {
                "name": "chunk_index",
                "type": "Edm.Int32",
                "filterable": True,
                "searchable": False,
            },
            {
                "name": "page",
                "type": "Edm.Int32",
                "filterable": True,
                "searchable": False,
            },
            {
                "name": "uploaded_at",
                "type": "Edm.String",
                "filterable": True,
                "searchable": False,
            },
            {
                "name": "source_type",
                "type": "Edm.String",
                "filterable": True,
                "searchable": False,
            },
        ],
        "vectorSearch": {
            "algorithms": [
                {
                    "name": "hnsw-algo",
                    "kind": "hnsw",
                    "hnswParameters": {
                        "metric": "cosine",
                        "m": 4,
                        "efConstruction": 400,
                        "efSearch": 500,
                    },
                }
            ],
            "profiles": [
                {
                    "name": "hnsw-profile",
                    "algorithm": "hnsw-algo",
                }
            ],
        },
        "semantic": {
            "configurations": [
                {
                    "name": "semantic-config",
                    "prioritizedFields": {
                        "contentFields": [{"fieldName": "text"}],
                    },
                }
            ]
        },
    }

    resp = requests.put(url, headers=_headers(), json=schema)

    if resp.status_code in (200, 201):
        logger.info("Azure AI Search index '%s' created/updated.", INDEX_NAME)
        return {"ok": True, "index": INDEX_NAME}

    logger.error("Index creation failed: %s %s", resp.status_code, resp.text)
    raise Exception(f"Index creation failed ({resp.status_code}): {resp.text}")


# ── Ingestion ──────────────────────────────────────────────────────────────────

def upload_chunks(chunks, client_id: str) -> dict:
    """
    Upload a list of Chunk objects (with .embedding attached) to Azure AI Search.
    Batch size is capped at 1000 (Azure limit per request).
    Uses mergeOrUpload so re-ingesting a file updates existing docs.
    """
    import requests

    _check_config()

    if not chunks:
        return {"uploaded": 0, "failed": 0}

    url = (
        f"{SEARCH_ENDPOINT}/indexes/{INDEX_NAME}"
        f"/docs/index?api-version={API_VERSION}"
    )

    BATCH = 1000
    total_uploaded = 0
    total_failed   = 0

    for i in range(0, len(chunks), BATCH):
        batch = chunks[i : i + BATCH]
        documents = []

        for c in batch:
            if c.embedding is None:
                logger.warning("Chunk %s has no embedding — skipping.", c.chunk_id)
                total_failed += 1
                continue

            documents.append({
                "@search.action": "mergeOrUpload",
                # Unique ID: prefix with client_id to prevent cross-tenant collisions
                "id":           f"{client_id}__{c.chunk_id}",
                "client_id":    client_id,
                "doc_id":       c.doc_id,
                "text":         c.text,
                "embedding":    c.embedding.tolist(),
                "source_file":  c.source_file,
                "chunk_index":  c.chunk_index,
                "page":         c.page,
                "uploaded_at":  c.uploaded_at,
                "source_type":  c.source_type,
            })

        if not documents:
            continue

        resp = requests.post(url, headers=_headers(), json={"value": documents})

        if resp.status_code not in (200, 207):
            logger.error(
                "Search upload batch failed: %s %s",
                resp.status_code,
                resp.text[:300],
            )
            total_failed += len(documents)
            continue

        # 207 = partial success — count per-item statuses
        result_items = resp.json().get("value", [])
        for item in result_items:
            if item.get("status"):
                total_uploaded += 1
            else:
                total_failed += 1

        logger.info(
            "Uploaded batch %d–%d (%d docs) to Azure AI Search.",
            i + 1,
            min(i + BATCH, len(chunks)),
            len(documents),
        )

    return {"uploaded": total_uploaded, "failed": total_failed}


# ── Deletion ───────────────────────────────────────────────────────────────────

def delete_chunks_by_doc(doc_id: str, client_id: str) -> dict:
    """
    Delete all chunks for a specific doc_id under a client.
    Call before re-ingesting a document to avoid duplicates.
    """
    import requests

    _check_config()

    search_url = (
        f"{SEARCH_ENDPOINT}/indexes/{INDEX_NAME}"
        f"/docs/search?api-version={API_VERSION}"
    )

    # Find all chunk IDs for this doc
    resp = requests.post(
        search_url,
        headers=_headers(),
        json={
            "filter": f"client_id eq '{client_id}' and doc_id eq '{doc_id}'",
            "select": "id",
            "top": 1000,
            "count": True,
        },
    )

    if not resp.ok:
        logger.error("Search query for deletion failed: %s", resp.text[:200])
        return {"deleted": 0, "error": resp.text}

    ids = [d["id"] for d in resp.json().get("value", [])]

    if not ids:
        logger.info("No chunks found for doc_id='%s' client_id='%s'.", doc_id, client_id)
        return {"deleted": 0}

    return _delete_by_ids(ids)


def delete_chunks_by_client(client_id: str) -> dict:
    """
    Delete ALL chunks for a client (e.g. when deleting a client account).
    Paginates through all results automatically.
    """
    import requests

    _check_config()

    search_url = (
        f"{SEARCH_ENDPOINT}/indexes/{INDEX_NAME}"
        f"/docs/search?api-version={API_VERSION}"
    )

    all_ids: list[str] = []
    skip = 0
    PAGE = 1000

    while True:
        resp = requests.post(
            search_url,
            headers=_headers(),
            json={
                "filter": f"client_id eq '{client_id}'",
                "select": "id",
                "top": PAGE,
                "skip": skip,
            },
        )

        if not resp.ok:
            break

        items = resp.json().get("value", [])
        if not items:
            break

        all_ids.extend(d["id"] for d in items)
        skip += PAGE

        if len(items) < PAGE:
            break

    if not all_ids:
        return {"deleted": 0}

    return _delete_by_ids(all_ids)


def _delete_by_ids(ids: list[str]) -> dict:
    import requests

    url = (
        f"{SEARCH_ENDPOINT}/indexes/{INDEX_NAME}"
        f"/docs/index?api-version={API_VERSION}"
    )

    BATCH = 1000
    total_deleted = 0

    for i in range(0, len(ids), BATCH):
        batch = ids[i : i + BATCH]
        delete_docs = [{"@search.action": "delete", "id": id_} for id_ in batch]

        resp = requests.post(url, headers=_headers(), json={"value": delete_docs})

        if resp.status_code in (200, 207):
            total_deleted += len(batch)
            logger.info("Deleted %d chunks from Azure AI Search.", len(batch))
        else:
            logger.error("Delete batch failed: %s %s", resp.status_code, resp.text[:200])

    return {"deleted": total_deleted}


# ── Search ─────────────────────────────────────────────────────────────────────

def hybrid_search(
    query_text: str,
    query_vector: Optional[List[float]],
    client_id: str,
    top_k: int = 6,
) -> List[dict]:
    """
    Hybrid search: BM25 keyword + vector (HNSW) + semantic reranking.
    Always filters by client_id for tenant isolation.

    Returns a list of dicts with keys:
        text, source_file, chunk_index, page, doc_id, score
    """
    import requests

    _check_config()

    url = (
        f"{SEARCH_ENDPOINT}/indexes/{INDEX_NAME}"
        f"/docs/search?api-version={API_VERSION}"
    )

    body: dict = {
        "search":      query_text,          # BM25 keyword search
        "filter":      f"client_id eq '{client_id}'",
        "select":      "text,source_file,chunk_index,page,doc_id",
        "top":         top_k,
        "queryType":   "semantic",
        "semanticConfiguration": "semantic-config",
        "captions":    "none",
    }

    # Add vector query if embedding is available
    if query_vector:
        body["vectorQueries"] = [
            {
                "kind":   "vector",
                "vector": query_vector,
                "fields": "embedding",
                "k":      top_k * 2,        # oversample then rerank
            }
        ]

    resp = requests.post(url, headers=_headers(), json=body, timeout=10)

    if not resp.ok:
        logger.error(
            "Azure AI Search query failed: %s %s",
            resp.status_code,
            resp.text[:300],
        )
        resp.raise_for_status()

    results = []
    for doc in resp.json().get("value", []):
        results.append({
            "text":        doc.get("text", ""),
            "source_file": doc.get("source_file", "unknown"),
            "chunk_index": doc.get("chunk_index", 0),
            "page":        doc.get("page", 1),
            "doc_id":      doc.get("doc_id", ""),
            "_score":      doc.get("@search.rerankerScore")
                           or doc.get("@search.score", 0.0),
        })

    logger.info(
        "Search '%s...' → %d results for client '%s'",
        query_text[:50],
        len(results),
        client_id,
    )

    return results


def ping() -> dict:
    """Quick connectivity check — returns index stats."""
    import requests

    try:
        _check_config()
        url = (
            f"{SEARCH_ENDPOINT}/indexes/{INDEX_NAME}"
            f"/stats?api-version={API_VERSION}"
        )
        resp = requests.get(url, headers=_headers(), timeout=5)

        if resp.ok:
            data = resp.json()
            return {
                "ok":          True,
                "index":       INDEX_NAME,
                "doc_count":   data.get("documentCount", 0),
                "storage_bytes": data.get("storageSize", 0),
            }

        return {"ok": False, "error": f"{resp.status_code}: {resp.text[:100]}"}

    except Exception as exc:
        return {"ok": False, "error": str(exc)}
