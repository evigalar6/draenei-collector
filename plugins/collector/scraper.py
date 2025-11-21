import requests
from typing import List, Dict, Any
import random


def scrape_metadata(query: str = "draenei", limit: int = 10, page: int = 1) -> List[Dict[str, Any]]:
    """
    Searches the images and returns dicts with params.
    """
    base_url = "https://wallhaven.cc/api/v1/search"

    # purity=100 (SFW), sorting=date_added
    params = {
        "q": query,
        "purity": "100",
        "sorting": "date_added",
        "order": "desc",
        "page": str(page),
        "apikey": ""
    }

    print(f"Performing search based on: '{query}'...")

    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()

        data_json = response.json()
        results = data_json.get("data", [])

        metadata_list = []

        for item in results[:limit]:

            image_data = {
                "wallhaven_id": item.get("id"),
                "url": item.get("path"),
                "resolution": item.get("resolution"),
                "category": item.get("category"),
                "purity": item.get("purity"),
                "file_size": item.get("file_size"),

            }
            metadata_list.append(image_data)

        print(f"Received metadata for {len(metadata_list)} pictures.")
        return metadata_list

    except requests.exceptions.RequestException as e:
        print(f"API error: {e}")
        return []


def scrape_random_batch(**kwargs):
    """
    Обгортка для Airflow: обирає випадкову сторінку і запускає скрапінг.
    """
    # 1 або 2 сторінка (бо їх всього мало)
    random_page = random.randint(1, 2)
    print(f"🎲 Тягнемо сторінку №{random_page}")

    return scrape_metadata(query="draenei", limit=5, page=random_page)


if __name__ == "__main__":
    # Тест
    data = scrape_metadata(limit=3)
    for img in data:
        print(img)