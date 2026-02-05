"""S3 helpers for uploading scraped images.

Configuration is read from environment variables (optionally via a local `.env`).
"""

import boto3
import os
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv
load_dotenv()


class S3Manager:
    def __init__(self):
        """Initialize an S3 client from environment variables.

        Expected environment variables:
            `BUCKET_NAME`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.

        `AWS_DEFAULT_REGION` is optional and defaults to `us-east-1`.
        """
        self.bucket_name = os.getenv('BUCKET_NAME')
        self.region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

        self.s3_client = boto3.client(
            's3',
            region_name=self.region,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )


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
        try:
            print(f"🚀 Вивантажую файл у S3: {s3_key}...")

            self.s3_client.put_object(Bucket=self.bucket_name,
                                      Key=s3_key,
                                      Body=file_content,
                                      ContentType='image/jpeg',
                                      )

            print(f"✅ Успішно завантажено: s3://{self.bucket_name}/{s3_key}")
            return True

        except ClientError as e:
            print(f"❌ Помилка AWS S3: {e}")
            return False
        except Exception as e:
            print(f"❌ Невідома помилка: {e}")
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
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.content
        except Exception as e:
            print(f"❌ Не вдалося скачати файл {url}: {e}")
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
