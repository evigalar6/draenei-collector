"""Scrape wallpaper metadata from the Wallhaven search API.

This module fetches a small set of fields used by downstream loaders.
"""

import logging
import requests
from typing import List, Dict, Any
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


def _http_session() -> requests.Session:
    # Lightweight retries/backoff for transient errors.
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


def scrape_metadata(query: str = "draenei", limit: int = 10, page: int = 1) -> List[Dict[str, Any]]:
    """Fetch wallpaper metadata from the Wallhaven search API.

    Args:
        query: Search query string.
        limit: Maximum number of items to return from the API response.
        page: 1-based page number to request.

    Returns:
        A list of metadata dicts. Returns an empty list on request/API errors.

    Side Effects:
        Prints progress and error messages to stdout.
    """
    base_url = "https://wallhaven.cc/api/v1/search"

    # Request SFW results and fetch the newest items first.
    params = {
        "q": query,
        "purity": "100",
        "sorting": "date_added",
        "order": "desc",
        "page": str(page),
        "apikey": ""
    }

    logger.info("[draenei] Wallhaven search query=%s page=%s limit=%s", query, page, limit)

    try:
        session = _http_session()
        response = session.get(base_url, params=params, timeout=15)
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

        logger.info("[draenei] Wallhaven returned %s items (using %s)", len(results), len(metadata_list))
        return metadata_list

    except requests.exceptions.RequestException as e:
        logger.exception("[draenei] Wallhaven API error: %s", e)
        return []


def scrape_random_batch(**kwargs):
    """Airflow entrypoint that scrapes a small, random page range.

    Returns:
        A list of metadata dicts returned by :func:`scrape_metadata`.
    """
    # Keep pagination bounded to reduce API load and runtime variance.
    random_page = random.randint(1, 2)
    logger.info("[draenei] Scraping random page=%s", random_page)

    return scrape_metadata(query="draenei", limit=5, page=random_page)


if __name__ == "__main__":
    # Manual smoke test.
    data = scrape_metadata(limit=3)
    for img in data:
        print(img)
