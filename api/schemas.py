from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator


class PerDocumentSummary(BaseModel):
    doc_id: str
    chunk_count: int


class PipelineSummary(BaseModel):
    run_timestamp: str
    documents_processed: int
    total_chunks: int
    uploads_succeeded: int
    uploads_failed: int
    elapsed_seconds: float
    per_document: List[PerDocumentSummary]
    upload_errors: List[Dict[str, Any]]
    status: str = "success"
    message: Optional[str] = None


class IngestDirectoryRequest(BaseModel):
    client_id: str = Field(
        ...,
        description="Unique client identifier used for isolated storage",
        examples=["client-a", "acme-corp", "finance-team"],
    )

    directory_path: str = Field(
        ...,
        description="Absolute path to a local directory containing documents.",
        examples=["/data/company_docs", "C:/Users/admin/reports"],
    )

    label: Optional[str] = Field(
        None,
        description="Human-readable label stored in chunk metadata.",
    )

    extra_metadata: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Arbitrary key-value pairs attached to every chunk.",
    )

    @field_validator("client_id")
    @classmethod
    def validate_client_id(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("client_id must not be empty")
        return value.strip().lower()

    @field_validator("directory_path")
    @classmethod
    def validate_directory_path(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("directory_path must not be empty")
        return value.strip()


class IngestDirectoryResponse(BaseModel):
    request_id: str
    directory_path: str
    files_found: int
    summary: Optional[PipelineSummary] = None
    error: Optional[str] = None


class UploadFileResponse(BaseModel):
    request_id: str
    filename: str
    size_bytes: int
    doc_id: str
    summary: Optional[PipelineSummary] = None
    error: Optional[str] = None


class IngestGoogleDriveRequest(BaseModel):
    client_id: str = Field(
        ...,
        description="Unique client identifier",
        examples=["client-a"],
    )

    folder_id: str = Field(
        ...,
        description=(
            "Google Drive folder ID. Found in URL: "
            "https://drive.google.com/drive/folders/<FOLDER_ID>"
        ),
        examples=["1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs"],
    )

    label: Optional[str] = Field(
        None,
        description="Human-readable label stored in chunk metadata.",
    )

    extra_metadata: Optional[Dict[str, Any]] = Field(
        default_factory=dict
    )

    recursive: bool = Field(
        True,
        description="If True, also process files in sub-folders.",
    )

    @field_validator("client_id")
    @classmethod
    def validate_gdrive_client_id(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("client_id must not be empty")
        return value.strip().lower()

    @field_validator("folder_id")
    @classmethod
    def validate_folder_id(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("folder_id must not be empty")
        return value.strip()


class IngestGoogleDriveResponse(BaseModel):
    request_id: str
    status: str
    message: str
    poll_url: str


class IngestSharePointRequest(BaseModel):
    client_id: str = Field(
        ...,
        description="Unique client identifier",
        examples=["client-a"],
    )

    site_url: str = Field(
        ...,
        description="Full SharePoint site URL.",
        examples=["https://mycompany.sharepoint.com/sites/HR"],
    )

    folder_path: str = Field(
        ...,
        description="Relative folder path within the site library.",
        examples=["Shared Documents/Policies"],
    )

    label: Optional[str] = None

    extra_metadata: Optional[Dict[str, Any]] = Field(
        default_factory=dict
    )

    @field_validator("client_id")
    @classmethod
    def validate_sharepoint_client_id(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("client_id must not be empty")
        return value.strip().lower()

    @field_validator("site_url")
    @classmethod
    def validate_site_url(cls, value: str) -> str:
        if "sharepoint.com" not in value.lower():
            raise ValueError("site_url must be a valid SharePoint URL")
        return value.strip()


class IngestSharePointResponse(BaseModel):
    request_id: str
    site_url: str
    folder_path: str
    files_found: int
    summary: Optional[PipelineSummary] = None
    error: Optional[str] = None
    status: Optional[str] = None
    message: Optional[str] = None


class ScanDirectoryRequest(BaseModel):
    directory_path: str

    @field_validator("directory_path")
    @classmethod
    def validate_scan_path(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("directory_path must not be empty")
        return value.strip()


class FileScanItem(BaseModel):
    filename: str
    file_type: str
    size_kb: float
    subfolder: str


class ScanDirectoryResponse(BaseModel):
    directory_path: str
    files_found: int
    files: List[FileScanItem]
    supported_extensions: List[str]


class BlobPrefixStats(BaseModel):
    prefix: str
    blob_count: int
    total_bytes: int


class StorageStatusResponse(BaseModel):
    ok: bool
    account: Optional[str] = None
    container: Optional[str] = None
    connection_error: Optional[str] = None
    prefixes: List[BlobPrefixStats] = []
    llm_keys_configured: Dict[str, bool] = {}


class DocumentListItem(BaseModel):
    doc_id: str
    source_file: str
    total_chunks: int
    processed_at: str
    blob_url_meta: str


class DocumentListResponse(BaseModel):
    total: int
    documents: List[DocumentListItem]


class ChunkSummary(BaseModel):
    chunk_id: str
    chunk_index: int
    page: int
    char_count: int
    text_preview: str


class DocumentMetaResponse(BaseModel):
    doc_id: str
    source_file: str
    source_type: str
    total_pages: int
    total_chunks: int
    total_chars: int
    processed_at: str
    blob_urls: Dict[str, str]
    chunks: List[ChunkSummary]
    extra_metadata: Dict[str, Any]


class DeleteDocumentResponse(BaseModel):
    doc_id: str
    blobs_deleted: int
    status: str
    message: str


class ChunkDetail(BaseModel):
    chunk_id: str
    chunk_index: int
    page: int
    char_count: int
    source_file: str
    uploaded_at: str
    text: str


class ChunksResponse(BaseModel):
    doc_id: str
    total: int
    chunks: List[ChunkDetail]


class RebuildIndexRequest(BaseModel):
    doc_id: Optional[str] = Field(
        None,
        description="Rebuild for single doc_id. Omit for all."
    )

    force: bool = Field(
        False,
        description="Re-embed even if embeddings already exist."
    )


class RebuildIndexResponse(BaseModel):
    rebuilt: List[str]
    skipped: List[str]
    errors: List[Dict[str, Any]]
    elapsed_seconds: float
    status: str
