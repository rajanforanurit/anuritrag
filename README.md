# RAG Ingestion Pipeline API

A production-ready document ingestion pipeline that:
1. **Fetches** files from local folders, Google Drive, or SharePoint
2. **Extracts** text from 35+ file types
3. **Chunks** text into overlapping segments
4. **Embeds** chunks using a sentence-transformer model
5. **Stores** everything in Azure Blob Storage (`vectordbforrag` container)

Deployed as a FastAPI service on Render via Docker.

---

## Supported File Types

| Category      | Extensions |
|---------------|------------|
| Documents     | `.pdf` `.docx` `.doc` `.txt` `.rtf` `.odt` |
| Spreadsheets  | `.xlsx` `.xls` `.ods` `.csv` `.tsv` |
| Presentations | `.pptx` `.ppt` |
| Web / Markup  | `.html` `.htm` `.xml` `.md` `.markdown` `.rst` |
| Data          | `.json` `.jsonl` `.yaml` `.yml` `.toml` |
| Code          | `.py` `.js` `.ts` `.jsx` `.tsx` `.java` `.cpp` `.c` `.cs` `.go` `.rb` `.php` `.swift` `.kt` `.r` `.sql` `.sh` |
| eBook         | `.epub` |
| Email         | `.eml` |

---

## API Endpoints

### Ingestion
| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/ingest/local-directory` | Ingest all files from a local folder |
| `POST` | `/ingest/upload-file` | Upload and ingest a single file |
| `POST` | `/ingest/google-drive` | Fetch + ingest from a Google Drive folder |
| `POST` | `/ingest/sharepoint` | Fetch + ingest from a SharePoint folder |
| `POST` | `/ingest/scan-directory` | Preview local folder without ingesting |

### Storage & Documents
| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET`  | `/storage/status` | Azure connection health + stats |
| `GET`  | `/documents` | List all ingested documents |
| `GET`  | `/document/{doc_id}` | Metadata + chunk list for a document |
| `DELETE` | `/document/{doc_id}` | Delete all blobs for a document |
| `GET`  | `/chunks/{doc_id}` | Full chunk text for a document |
| `POST` | `/rebuild-index` | Re-embed one or all documents |

### Health
| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET`  | `/` | API root + source list |
| `GET`  | `/health` | Liveness probe |

Full interactive docs: `http://localhost:8000/docs`

---

## Authentication

All endpoints require an API key:
```
Authorization: Bearer <API_KEY>
```

> **Note:** JWT authentication is intentionally excluded from this pipeline.
> JWT will be added in the admin panel project (a separate application).

---

## Quick Start

### 1. Clone and set up environment
```bash
git clone <your-repo>
cd rag-ingestion-pipeline
cp .env.example .env
# Edit .env with your Azure credentials and API key
```

### 2. Run locally
```bash
pip install torch==2.2.0+cpu --index-url https://download.pytorch.org/whl/cpu
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### 3. Test ingestion
```bash
# Ingest a local folder
curl -X POST http://localhost:8000/ingest/local-directory \
  -H "Authorization: Bearer your-api-key" \
  -H "Content-Type: application/json" \
  -d '{"directory_path": "/path/to/your/docs", "label": "Test Batch"}'

# Upload a single file
curl -X POST http://localhost:8000/ingest/upload-file \
  -H "Authorization: Bearer your-api-key" \
  -F "file=@report.pdf" \
  -F "label=Q1 Report"

# Ingest from Google Drive
curl -X POST http://localhost:8000/ingest/google-drive \
  -H "Authorization: Bearer your-api-key" \
  -H "Content-Type: application/json" \
  -d '{"folder_id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs", "label": "Drive Docs"}'

# Ingest from SharePoint
curl -X POST http://localhost:8000/ingest/sharepoint \
  -H "Authorization: Bearer your-api-key" \
  -H "Content-Type: application/json" \
  -d '{"site_url": "https://company.sharepoint.com/sites/HR", "folder_path": "Shared Documents/Policies"}'
```

---

## Deploy on Render

1. Push this repo to GitHub
2. In Render dashboard: **New → Web Service → Connect your repo**
3. Set **Environment** = `Docker`
4. Add environment variables in Render dashboard:
   - `AZURE_CONNECTION_STRING` (secret)
   - `AZURE_CONTAINER_NAME` = `vectordbforrag`
   - `API_KEY` (secret)
   - Optionally: `GOOGLE_SERVICE_ACCOUNT_JSON`, `SHAREPOINT_*`
5. Deploy

Or use `render.yaml` (Render Blueprint) — connect the repo and Render will auto-configure.

---

## Azure Blob Storage Layout

```
vectordbforrag/
├── raw/           ← original files as uploaded
│   ├── report.pdf
│   └── data.xlsx
├── chunks/        ← JSONL files: text + embeddings per document
│   ├── report_chunks.jsonl
│   └── data_chunks.jsonl
├── meta/          ← JSON metadata per document
│   ├── report_meta.json
│   └── data_meta.json
└── faiss/         ← FAISS index (only if ENABLE_FAISS_BACKUP=true)
```

Each chunk in the JSONL contains:
- `doc_id`, `chunk_id`, `chunk_index`
- `text` — the chunk content
- `embedding` — float32 vector array (ready for vector search)
- `page`, `source_file`, `source_type`, `uploaded_at`

---

## Google Drive Setup

1. Google Cloud Console → APIs & Services → Enable **Google Drive API**
2. IAM & Admin → Service Accounts → Create service account → Download JSON key
3. Share your Drive folder with the service account email (Viewer role)
4. Set `GOOGLE_SERVICE_ACCOUNT_JSON` in `.env` (paste the full JSON content)

---

## SharePoint Setup

1. Azure Portal → **App Registrations** → New Registration
2. API Permissions → Microsoft Graph → **Sites.Read.All** (Application)
3. Grant Admin Consent for your tenant
4. Certificates & Secrets → New client secret → copy the value
5. Set `SHAREPOINT_TENANT_ID`, `SHAREPOINT_CLIENT_ID`, `SHAREPOINT_CLIENT_SECRET` in `.env`

---

## Admin Panel (Future)

The admin panel (paste-a-link UI) will be built as a **separate project** and will:
- Call these API endpoints using the `API_KEY`
- Add JWT authentication for human users
- Provide a web UI to manage ingestion sources

This pipeline is designed to be the backend that the admin panel calls.
