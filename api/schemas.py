from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, validator


class LocalDirectoryIngestRequest(BaseModel):
    client_id: str = Field(
        ...,
        min_length=2,
        max_length=100,
        description="Unique client identifier used for client-wise storage and isolation"
    )

    directory_path: str = Field(
        ...,
        min_length=1,
        description="Absolute or relative path of the local directory containing documents"
    )

    label: Optional[str] = Field(
        default=None,
        max_length=200,
        description="Optional label for ingestion tracking"
    )

    extra_metadata: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Additional metadata to attach during ingestion"
    )

    @validator("client_id")
    def validate_client_id(cls, value):
        value = value.strip()

        if not value:
            raise ValueError("client_id cannot be empty")

        allowed = set(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
        )

        if not all(char in allowed for char in value):
            raise ValueError(
                "client_id must contain only letters, numbers, hyphens, and underscores"
            )

        return value.lower()

    @validator("directory_path")
    def validate_directory_path(cls, value):
        value = value.strip()

        if not value:
            raise ValueError("directory_path cannot be empty")

        return value


class UploadFileIngestRequest(BaseModel):

    client_id: str = Field(
        ...,
        min_length=2,
        max_length=100,
        description="Unique client identifier used for client-wise storage and isolation"
    )

    label: Optional[str] = Field(
        default=None,
        max_length=200,
        description="Optional label for uploaded file ingestion"
    )

    extra_metadata: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Additional metadata for uploaded files"
    )

    @validator("client_id")
    def validate_client_id(cls, value):
        value = value.strip()

        if not value:
            raise ValueError("client_id cannot be empty")

        allowed = set(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
        )

        if not all(char in allowed for char in value):
            raise ValueError(
                "client_id must contain only letters, numbers, hyphens, and underscores"
            )

        return value.lower()


class GoogleDriveIngestRequest(BaseModel):

    client_id: str = Field(
        ...,
        min_length=2,
        max_length=100,
        description="Unique client identifier used for client-wise storage and isolation"
    )

    folder_url: str = Field(
        ...,
        min_length=10,
        description="Google Drive folder URL"
    )

    label: Optional[str] = Field(
        default=None,
        max_length=200,
        description="Optional label for Google Drive ingestion"
    )

    extra_metadata: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Additional metadata for Google Drive ingestion"
    )

    @validator("client_id")
    def validate_client_id(cls, value):
        value = value.strip()

        if not value:
            raise ValueError("client_id cannot be empty")

        allowed = set(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
        )

        if not all(char in allowed for char in value):
            raise ValueError(
                "client_id must contain only letters, numbers, hyphens, and underscores"
            )

        return value.lower()

    @validator("folder_url")
    def validate_folder_url(cls, value):
        value = value.strip()

        if not value:
            raise ValueError("folder_url cannot be empty")

        if "drive.google.com" not in value:
            raise ValueError("Invalid Google Drive folder URL")

        return value


class IngestionResponse(BaseModel):
    """
    Standard API response for ingestion operations.
    """

    success: bool = Field(
        ...,
        description="Whether ingestion completed successfully"
    )

    message: str = Field(
        ...,
        description="Human-readable result message"
    )

    client_id: Optional[str] = Field(
        default=None,
        description="Client ID associated with this ingestion"
    )

    files_processed: Optional[int] = Field(
        default=0,
        description="Number of files processed"
    )

    chunks_created: Optional[int] = Field(
        default=0,
        description="Number of chunks created"
    )

    blob_paths: Optional[List[str]] = Field(
        default_factory=list,
        description="Azure Blob paths created for this client"
    )

    metadata: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Additional response metadata"
    )

    error: Optional[str] = Field(
        default=None,
        description="Detailed error if failed"
    )
