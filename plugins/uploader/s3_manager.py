import boto3
import os
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv
load_dotenv()


class S3Manager:
    def __init__(self):
        """
        Ініціалізація клієнта AWS S3.
        Тут треба дістати ключі з змінних оточення (.env) і створити клієнт.
        """
        self.bucket_name = os.getenv('BUCKET_NAME')
        self.region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

        # ТУТ ТВІЙ КОД: створити boto3 client
        self.s3_client = boto3.client(
            's3',
            region_name=self.region,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )


    def upload_file(self, file_content: bytes, s3_key: str) -> bool:
        """
        Завантажує байти (картинку) в S3.

        :param file_content: Вміст файлу у байтах (те, що повернув requests.get().content)
        :param s3_key: Шлях, куди покласти файл в бакеті (наприклад 'wallpapers/2025/img_123.jpg')
        :return: True якщо успішно, False якщо помилка
        """
        try:
            print(f"🚀 Вивантажую файл у S3: {s3_key}...")

            # ТУТ ТВІЙ КОД: використати метод put_object
            # self.s3_client.put_object(...)
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
        """
        Допоміжний метод: просто качає картинку з інтернету в оперативну пам'ять.
        """
        try:
            # ТУТ ТВІЙ КОД: requests.get...
            # Не забудь перевірити status_code
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.content
        except Exception as e:
            print(f"❌ Не вдалося скачати файл {url}: {e}")
            return None


# Цей блок для тестування, щоб ти могла запустити файл і перевірити роботу без Airflow
if __name__ == "__main__":
    # 1. Створити менеджер
    manager = S3Manager()

    # 2. Тестовий URL (якась іконка)
    test_url = "https://w.wallhaven.cc/full/9m/wallhaven-9mkxdd.jpg"

    # 3. Спробувати скачати
    print("Test 1: Downloading...")
    file_bytes = manager.download_image_as_bytes(test_url)

    # 4. Якщо скачалось - спробувати залити в S3
    if file_bytes:
        print("Test 2: Uploading...")
        manager.upload_file(file_bytes, "test_folder/google_logo.png")