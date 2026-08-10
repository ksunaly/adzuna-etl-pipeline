import json
import logging
import math
import os
from datetime import datetime, timezone
from typing import Any

import requests
from flask import Flask, jsonify
from google.cloud import storage

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the API configuration.
ADZUNA_API_URL = "https://api.adzuna.com/v1/api/jobs/ca/search"
RESULTS_PER_PAGE = 50


def get_required_env(name: str) -> str:
    """Read a required environment variable."""

    value = os.getenv(name)

    if not value:
        raise ValueError(
            f"Missing required environment variable: {name}"
        )

    return value


def fetch_all_pages() -> list[dict[str, Any]]:
    """Fetch all Data Engineer jobs from the Adzuna API."""

    # Read API credentials from environment variables.
    app_id = get_required_env("ADZUNA_APP_ID")
    app_key = get_required_env("ADZUNA_APP_KEY")
    
    # Define the API search parameters.
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": RESULTS_PER_PAGE,
        "what_phrase": "data engineer",
        "max_days_old": 2,
        "sort_by": "date",
    }

    all_jobs = []

    # Fetch the first page and determine the total page count.
    response = requests.get(
        f"{ADZUNA_API_URL}/1",
        params=params,
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()
    all_jobs.extend(data.get("results", []))

    total_results = data.get("count", 0)
    total_pages = math.ceil(total_results / RESULTS_PER_PAGE)

    # Fetch the remaining API pages.
    for page in range(2, total_pages + 1):
        response = requests.get(
            f"{ADZUNA_API_URL}/{page}",
            params=params,
            timeout=30,
        )
        response.raise_for_status()

        page_data = response.json()
        all_jobs.extend(page_data.get("results", []))

    logger.info("Retrieved %s jobs", len(all_jobs))

    return all_jobs


def upload_jobs(jobs: list[dict[str, Any]]) -> str:
    """Save raw job postings as JSON in Cloud Storage."""

    bucket_name = get_required_env("BUCKET_NAME")

    # Create a timestamped object name.
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%d_%H%M%S")

    object_name = (
        f"raw/to_process/"
        f"adzuna_jobs_{timestamp}.json"
    )

    # Add metadata to the raw API response.
    payload = {
        "extracted_at": now.isoformat(),
        "record_count": len(jobs),
        "items": jobs,
    }

    # Upload the JSON document to Cloud Storage.
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    blob.upload_from_string(
        json.dumps(payload),
        content_type="application/json",
    )

    logger.info(
        "Uploaded file to gs://%s/%s",
        bucket_name,
        object_name,
        )

    return object_name

@app.get("/")
def health_check():
    """Check that the service is running."""

    return jsonify(
        {
            "service": "adzuna-extractor",
            "status": "healthy",
        }
    )


@app.post("/extract")
def extract_jobs():
    """Fetch jobs from Adzuna and upload them to Cloud Storage."""

    try:
        # Extract jobs from the API.
        jobs = fetch_all_pages()

        # Save raw jobs to Cloud Storage.
        object_name = upload_jobs(jobs)

        # Return information about the completed extraction.
        return jsonify(
            {
                "status": "success",
                "records_extracted": len(jobs),
                "storage_object": object_name,
            }
        )

    except requests.RequestException as error:
        logger.exception("Adzuna API request failed")

        return jsonify(
            {
                "status": "error",
                "message": str(error),
            }
        ), 502

    except Exception as error:
        logger.exception("Extraction failed")

        return jsonify(
            {
                "status": "error",
                "message": str(error),
            }
        ), 500




    