"""S3 helpers for uploading scraped images.

Configuration is read from environment variables (optionally via a local `.env`).
"""

import logging
import time
import boto3
import os
import requests
from botocore.exceptions import ClientError
from botocore.config import Config
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
load_dotenv()

logger = logging.getLogger(__name__)


class S3Manager:
    def __init__(self):
        """Initialize an S3 client from environment variables.

        Expected environment variables:
            `BUCKET_NAME`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.

        `AWS_DEFAULT_REGION` is optional and defaults to `us-east-1`.
        """
        self.bucket_name = os.getenv('BUCKET_NAME')
        self.region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.endpoint_url = os.getenv("S3_ENDPOINT_URL") or None
        force_path_style = (os.getenv("S3_FORCE_PATH_STYLE", "false").lower() == "true")

        s3_config = Config(
            retries={"max_attempts": 10, "mode": "standard"},
            s3={"addressing_style": "path" if force_path_style else "auto"},
        )
        self.s3_client = boto3.client(
            's3',
            region_name=self.region,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            endpoint_url=self.endpoint_url,
            config=s3_config,
        )

        self._http = self._http_session()

    @staticmethod
    def _http_session() -> requests.Session:
        retry = Retry(
            total=4,
            connect=4,
            read=4,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        s = requests.Session()
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        return s

    @staticmethod
    def _content_type_for_key(s3_key: str) -> str:
        key_lower = s3_key.lower()
        if key_lower.endswith(".png"):
            return "image/png"
        if key_lower.endswith(".webp"):
            return "image/webp"
        if key_lower.endswith(".jpeg") or key_lower.endswith(".jpg"):
            return "image/jpeg"
        return "application/octet-stream"

    def object_exists(self, s3_key: str) -> bool:
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            # Be defensive: in unusual cases `ClientError.response` may be missing/None.
            response = getattr(e, "response", None) or {}
            err = response.get("Error") if isinstance(response, dict) else None
            code = str((err or {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise


    def upload_file(self, file_content: bytes, s3_key: str) -> bool:
        """Upload raw bytes to S3 at the provided key.

        Args:
            file_content: Object payload (typically image bytes).
            s3_key: Object key within the bucket (for example, `wallpapers/abc.jpg`).

        Returns:
            True on success. False if the upload fails.

        Side Effects:
            Writes to S3 and prints status messages to stdout.
        """
        # Idempotency: deterministic keys + treat "already exists" as success.
        try:
            if self.object_exists(s3_key):
                logger.info("[draenei] S3 already has key=%s (skipping upload)", s3_key)
                return True
        except Exception:
            # If head_object is blocked, proceed to upload.
            logger.warning("[draenei] S3 head_object failed for key=%s; attempting upload anyway", s3_key)

        content_type = self._content_type_for_key(s3_key)

        # Simple exponential backoff around put_object.
        delay_s = 1.0
        for attempt in range(1, 5):
            try:
                logger.info("[draenei] Uploading to S3 key=%s bytes=%s attempt=%s", s3_key, len(file_content), attempt)
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=file_content,
                    ContentType=content_type,
                )
                logger.info("[draenei] Uploaded s3://%s/%s", self.bucket_name, s3_key)
                return True
            except ClientError as e:
                logger.warning("[draenei] S3 put_object error key=%s attempt=%s err=%s", s3_key, attempt, e)
            except Exception as e:
                logger.warning("[draenei] Unexpected S3 upload error key=%s attempt=%s err=%s", s3_key, attempt, e)

            if attempt < 4:
                time.sleep(delay_s)
                delay_s *= 2

        return False

    def download_image_as_bytes(self, url: str) -> bytes:
        """Download a URL and return the response body as bytes.

        Args:
            url: Image URL to fetch.

        Returns:
            Response body bytes, or None if the request fails.

        Side Effects:
            Performs an HTTP GET request and prints error messages to stdout.
        """
        try:
            response = self._http.get(url, timeout=20)
            response.raise_for_status()
            return response.content
        except Exception as e:
            logger.warning("[draenei] Failed to download url=%s err=%s", url, e)
            return None


if __name__ == "__main__":
    # Manual smoke test (runs without Airflow).
    manager = S3Manager()

    # Example image URL.
    test_url = "https://w.wallhaven.cc/full/9m/wallhaven-9mkxdd.jpg"

    print("Test 1: Downloading...")
    file_bytes = manager.download_image_as_bytes(test_url)

    if file_bytes:
        print("Test 2: Uploading...")
        manager.upload_file(file_bytes, "test_folder/google_logo.png")
