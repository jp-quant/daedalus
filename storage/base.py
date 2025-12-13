"""
Base storage abstraction for FluxForge.

Provides unified interface for local and cloud storage backends.
All paths are relative to the storage root (local base_dir or S3 bucket).
"""
import io
import logging
from abc import ABC, abstractmethod
from pathlib import Path, PurePosixPath
from typing import Optional, List, Dict, Any, Union

logger = logging.getLogger(__name__)



class StorageBackend(ABC):
    """
    Abstract base class for storage backends.
    
    All implementations must support:
    - Path operations relative to a root (base_dir or bucket)
    - Read/write bytes and files
    - List, exists, delete operations
    - Integration with data libraries (Polars, Pandas, PyArrow)
    """
    
    def __init__(self, base_path: str):
        """
        Initialize storage backend.
        
        Args:
            base_path: Root path for all operations (local dir or S3 bucket)
        """
        self.base_path = base_path
    
    @abstractmethod
    def write_bytes(self, data: bytes, path: str) -> str:
        """
        Write bytes to storage.
        
        Args:
            data: Bytes to write
            path: Relative path from base_path
        
        Returns:
            Full path where data was written
        """
        pass
    
    @abstractmethod
    def write_file(self, local_path: Union[str, Path], remote_path: str) -> str:
        """
        Upload a local file to storage.
        
        Args:
            local_path: Local file to upload
            remote_path: Relative destination path from base_path
        
        Returns:
            Full path where file was written
        """
        pass
    
    @abstractmethod
    def read_bytes(self, path: str) -> bytes:
        """
        Read bytes from storage.
        
        Args:
            path: Relative path from base_path
        
        Returns:
            File contents as bytes
        """
        pass
    
    @abstractmethod
    def exists(self, path: str) -> bool:
        """
        Check if path exists.
        
        Args:
            path: Relative path from base_path
        
        Returns:
            True if path exists
        """
        pass
    
    @abstractmethod
    def delete(self, path: str) -> bool:
        """
        Delete a file.
        
        Args:
            path: Relative path from base_path
        
        Returns:
            True if deleted successfully
        """
        pass
    
    @abstractmethod
    def list_files(
        self, 
        path: str = "", 
        pattern: Optional[str] = None,
        recursive: bool = False
    ) -> List[Dict[str, Any]]:
        """
        List files in a directory.
        
        Args:
            path: Relative directory path from base_path
            pattern: Optional glob pattern (e.g., "*.parquet")
            recursive: Whether to list recursively
        
        Returns:
            List of file info dicts with keys: path, size, modified
        """
        pass
    
    @abstractmethod
    def get_full_path(self, path: str) -> str:
        """
        Get full path for a relative path.
        
        Args:
            path: Relative path from base_path
        
        Returns:
            Full path (file:// or s3:// URI)
        """
        pass
    
    @abstractmethod
    def get_storage_options(self) -> Optional[Dict[str, Any]]:
        """
        Get storage options for data libraries (Polars, Pandas).
        
        Returns:
            Storage options dict (for S3) or None (for local)
        """
        pass
    
    @abstractmethod
    def get_filesystem(self) -> Any:
        """
        Get filesystem object for direct access.
        
        Returns:
            Filesystem object (s3fs.S3FileSystem or None for local)
        """
        pass
    
    @property
    @abstractmethod
    def backend_type(self) -> str:
        """Return backend type identifier ('local' or 's3')."""
        pass
    
    def join_path(self, *parts: str) -> str:
        """
        Join path components using forward slashes.
        
        Works consistently across local and S3 backends.
        
        Args:
            *parts: Path components
        
        Returns:
            Joined path with forward slashes
        """
        # Filter out empty parts and join with /
        clean_parts = [p.strip("/") for p in parts if p]
        return "/".join(clean_parts)
    
    def mkdir(self, path: str, parents: bool = True, exist_ok: bool = True) -> None:
        """
        Create directory (local only, no-op for S3).
        
        Args:
            path: Relative directory path from base_path
            parents: Create parent directories if needed
            exist_ok: Don't raise error if directory exists
        """
        # Default implementation (no-op) - overridden in LocalStorage
        pass


class LocalStorage(StorageBackend):
    """Local filesystem storage backend."""
    
    def __init__(self, base_path: str):
        """
        Initialize local storage.
        
        Args:
            base_path: Base directory for all operations (e.g., "F:/")
        """
        super().__init__(base_path)
        self.base_dir = Path(base_path).resolve()
        self.base_dir.mkdir(parents=True, exist_ok=True)
    
    @property
    def backend_type(self) -> str:
        return "local"
    
    def _resolve_path(self, path: str) -> Path:
        """Convert relative path to absolute local path."""
        return self.base_dir / path
    
    def write_bytes(self, data: bytes, path: str) -> str:
        full_path = self._resolve_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(data)
        return str(full_path)
    
    def write_file(self, local_path: Union[str, Path], remote_path: str) -> str:
        local_path = Path(local_path)
        full_path = self._resolve_path(remote_path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Copy file
        import shutil
        shutil.copy2(local_path, full_path)
        return str(full_path)
    
    def read_bytes(self, path: str) -> bytes:
        full_path = self._resolve_path(path)
        return full_path.read_bytes()
    
    def exists(self, path: str) -> bool:
        return self._resolve_path(path).exists()
    
    def delete(self, path: str) -> bool:
        try:
            full_path = self._resolve_path(path)
            if full_path.exists():
                full_path.unlink()
                return True
            return False
        except Exception:
            return False
    
    def list_files(
        self, 
        path: str = "", 
        pattern: Optional[str] = None,
        recursive: bool = False
    ) -> List[Dict[str, Any]]:
        full_path = self._resolve_path(path)
        
        if not full_path.exists():
            return []
        
        # Get files
        if recursive:
            glob_pattern = f"**/{pattern}" if pattern else "**/*"
            files = full_path.rglob(pattern or "*") if not pattern else full_path.glob(glob_pattern)
        else:
            files = full_path.glob(pattern or "*")
        
        result = []
        for f in files:
            if f.is_file():
                stat = f.stat()
                result.append({
                    "path": str(f.relative_to(self.base_dir)),
                    "size": stat.st_size,
                    "modified": stat.st_mtime,
                })
        
        return result
    
    def get_full_path(self, path: str) -> str:
        return str(self._resolve_path(path))
    
    def get_storage_options(self) -> Optional[Dict[str, Any]]:
        return None  # No special options for local storage
    
    def get_filesystem(self) -> Any:
        return None  # No filesystem object for local
    
    def mkdir(self, path: str, parents: bool = True, exist_ok: bool = True) -> None:
        full_path = self._resolve_path(path)
        full_path.mkdir(parents=parents, exist_ok=exist_ok)


class S3Storage(StorageBackend):
    """AWS S3 storage backend."""
    
    def __init__(
        self,
        bucket: str,
        region: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        max_pool_connections: int = 50,
    ):
        """
        Initialize S3 storage.
        
        Args:
            bucket: S3 bucket name (this is the base_path)
            region: AWS region (auto-detected if None)
            aws_access_key_id: AWS access key (uses environment/IAM if None)
            aws_secret_access_key: AWS secret key
            aws_session_token: Session token for temporary credentials
            endpoint_url: Custom endpoint for S3-compatible services
            max_pool_connections: Max connections in boto3 pool (default 50 for threading)
        """
        super().__init__(bucket)
        self.bucket = bucket
        self.region = region
        
        # Initialize boto3 client with connection pool configuration
        import boto3
        from botocore.config import Config
        
        # Configure connection pool and retry behavior
        # max_pool_connections: Critical for ThreadPoolExecutor usage
        # Default boto3 is only 10, which causes connection pool exhaustion
        # when using max_workers > 10 or parallel operations
        boto_config = Config(
            max_pool_connections=max_pool_connections,
            retries={'max_attempts': 3, 'mode': 'adaptive'},
            connect_timeout=10,
            read_timeout=60,
        )
        
        session_kwargs = {}
        if aws_access_key_id:
            session_kwargs["aws_access_key_id"] = aws_access_key_id
        if aws_secret_access_key:
            session_kwargs["aws_secret_access_key"] = aws_secret_access_key
        if aws_session_token:
            session_kwargs["aws_session_token"] = aws_session_token
        if region:
            session_kwargs["region_name"] = region
        
        self.s3_client = boto3.client(
            "s3",
            config=boto_config,
            endpoint_url=endpoint_url,
            **session_kwargs
        )
        
        # Initialize s3fs for data library integration
        import s3fs
        
        s3fs_kwargs = {
            "anon": False,
        }
        if aws_access_key_id and aws_secret_access_key:
            s3fs_kwargs["key"] = aws_access_key_id
            s3fs_kwargs["secret"] = aws_secret_access_key
        if aws_session_token:
            s3fs_kwargs["token"] = aws_session_token
        if endpoint_url:
            s3fs_kwargs["client_kwargs"] = {"endpoint_url": endpoint_url}
        
        self.s3fs = s3fs.S3FileSystem(**s3fs_kwargs)
    
    @property
    def backend_type(self) -> str:
        return "s3"
    
    def _get_s3_key(self, path: str) -> str:
        """Convert relative path to S3 key."""
        # Normalize path separators (handle Windows paths)
        path = str(path).replace("\\", "/")
        return path.lstrip("/")
    
    def write_bytes(self, data: bytes, path: str) -> str:
        key = self._get_s3_key(path)
        
        # Use multipart upload for large files (>5MB)
        if len(data) > 5 * 1024 * 1024:
            return self._multipart_upload(data, key)
        
        # Simple put for small files with retry
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=data
                )
                return f"s3://{self.bucket}/{key}"
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to upload {key} after {max_retries} attempts: {e}")
                    raise
                logger.warning(f"Upload attempt {attempt + 1} failed for {key}, retrying...")
                import time
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def write_file(self, local_path: Union[str, Path], remote_path: str) -> str:
        key = self._get_s3_key(remote_path)
        self.s3_client.upload_file(str(local_path), self.bucket, key)
        return f"s3://{self.bucket}/{key}"
    
    def read_bytes(self, path: str) -> bytes:
        key = self._get_s3_key(path)
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
                return response["Body"].read()
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to read {key} after {max_retries} attempts: {e}")
                    raise
                logger.warning(f"Read attempt {attempt + 1} failed for {key}, retrying...")
                import time
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def exists(self, path: str) -> bool:
        try:
            key = self._get_s3_key(path)
            self.s3_client.head_object(Bucket=self.bucket, Key=key)
            return True
        except self.s3_client.exceptions.ClientError:
            return False
    
    def delete(self, path: str) -> bool:
        try:
            key = self._get_s3_key(path)
            self.s3_client.delete_object(Bucket=self.bucket, Key=key)
            return True
        except Exception as e:
            logger.info(f"Failed to delete {path}: {e}")
            return False
    
    def list_files(
        self, 
        path: str = "", 
        pattern: Optional[str] = None,
        recursive: bool = False
    ) -> List[Dict[str, Any]]:
        # Smart recursion detection
        if pattern and "**" in pattern:
            recursive = True

        prefix = self._get_s3_key(path)
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        
        # For recursive listing in S3, Delimiter should be None (or omitted)
        # For non-recursive, use "/" to emulate directory listing
        delimiter = "" if recursive else "/"
        
        result = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        
        pagination_kwargs = {
            "Bucket": self.bucket,
            "Prefix": prefix,
        }
        if not recursive:
            pagination_kwargs["Delimiter"] = delimiter

        try:
            for page in paginator.paginate(**pagination_kwargs):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    
                    # Skip the directory itself if it appears in contents
                    if key == prefix:
                        continue

                    # Apply pattern filter
                    if pattern:
                        # Get path relative to the search prefix for matching
                        rel_path = key[len(prefix):] if key.startswith(prefix) else key
                        
                        # Use PurePosixPath match which handles ** and glob patterns correctly
                        if not PurePosixPath(rel_path).match(pattern):
                            continue
                    
                    result.append({
                        "path": key,
                        "size": obj["Size"],
                        "modified": obj["LastModified"].timestamp(),
                    })
        except Exception as e:
            logger.error(f"Error listing S3 files in {prefix}: {e}")
            return []
        
        return result
    
    def get_full_path(self, path: str) -> str:
        key = self._get_s3_key(path)
        return f"s3://{self.bucket}/{key}"
    
    def get_storage_options(self) -> Optional[Dict[str, Any]]:
        """Return storage options for Polars/Pandas."""
        # Return only non-None credentials for s3fs
        # If using IAM role/env vars, return empty dict (s3fs auto-detects)
        options = {}
        if hasattr(self.s3fs, "key") and self.s3fs.key:
            options["key"] = self.s3fs.key
        if hasattr(self.s3fs, "secret") and self.s3fs.secret:
            options["secret"] = self.s3fs.secret
        if hasattr(self.s3fs, "token") and self.s3fs.token:
            options["token"] = self.s3fs.token
        return options
    
    def get_filesystem(self) -> Any:
        return self.s3fs
    
    def _multipart_upload(self, data: bytes, key: str) -> str:
        """
        Perform multipart upload for large files.
        
        More efficient and reliable for files >5MB.
        """
        import io
        
        # 5MB chunks (minimum for S3 multipart)
        chunk_size = 5 * 1024 * 1024
        
        upload_id = None
        try:
            # Initiate multipart upload
            response = self.s3_client.create_multipart_upload(
                Bucket=self.bucket,
                Key=key
            )
            upload_id = response["UploadId"]
            
            parts = []
            buffer = io.BytesIO(data)
            part_number = 1
            
            while True:
                chunk = buffer.read(chunk_size)
                if not chunk:
                    break
                
                # Upload part with retry
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        part_response = self.s3_client.upload_part(
                            Bucket=self.bucket,
                            Key=key,
                            PartNumber=part_number,
                            UploadId=upload_id,
                            Body=chunk
                        )
                        parts.append({
                            "PartNumber": part_number,
                            "ETag": part_response["ETag"]
                        })
                        break
                    except Exception as e:
                        if attempt == max_retries - 1:
                            raise
                        import time
                        time.sleep(2 ** attempt)
                
                part_number += 1
            
            # Complete multipart upload
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts}
            )
            
            return f"s3://{self.bucket}/{key}"
        
        except Exception as e:
            # Abort multipart upload on failure
            if upload_id:
                try:
                    self.s3_client.abort_multipart_upload(
                        Bucket=self.bucket,
                        Key=key,
                        UploadId=upload_id
                    )
                except:
                    pass
            logger.error(f"Multipart upload failed for {key}: {e}")
            raise
