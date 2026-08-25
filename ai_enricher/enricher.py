import json
import os
from datetime import datetime, timezone
from typing import Optional

from flask import Flask, jsonify
from google import genai
from google.cloud import bigquery
from google.genai.types import GenerateContentConfig
from pydantic import BaseModel


app = Flask(__name__)

PROJECT_ID = "adzuna-etl-pipeline"
SOURCE_TABLE = "adzuna-etl-pipeline.adzuna_jobs.jobs"
TARGET_TABLE = "adzuna-etl-pipeline.adzuna_jobs.jobs_enriched"
MODEL_NAME = "gemini-2.5-flash"

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))


class JobEnrichment(BaseModel):
    seniority: str
    role_family: str
    skills: list[str]
    years_experience: Optional[int]


def get_jobs_to_enrich():
    """Get jobs that have not been enriched yet."""

    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
    SELECT
        j.job_id,
        j.title,
        j.description
    FROM `{SOURCE_TABLE}` AS j
    WHERE j.description IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM `{TARGET_TABLE}` AS e
          WHERE e.job_id = j.job_id
      )
    LIMIT {BATCH_SIZE}
    """

    return list(client.query(query).result())


def enrich_job(job):
    """Use Gemini to extract structured information from a job posting."""

    client = genai.Client(
        vertexai=True,
        project=PROJECT_ID,
        location="global",
    )

    prompt = f"""
Analyze this job posting.

Job title:
{job.title}

Job description:
{job.description}

Extract:
- seniority
- role family
- technical and professional skills
- minimum years of experience

For years_experience:
- return an integer only
- if the job says 3+ years, return 3
- if experience is not specified, return null
"""

    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt,
        config=GenerateContentConfig(
            temperature=0,
            response_mime_type="application/json",
            response_schema=JobEnrichment,
        ),
    )

    return json.loads(response.text)


def save_result(job, result):
    """Save one enriched job to BigQuery."""

    client = bigquery.Client(project=PROJECT_ID)

    row = {
        "job_id": job.job_id,
        "title": job.title,
        "seniority": result["seniority"],
        "role_family": result["role_family"],
        "skills": result["skills"],
        "years_experience": result["years_experience"],
        "model_name": MODEL_NAME,
        "enriched_at": datetime.now(timezone.utc).isoformat(),
    }

    errors = client.insert_rows_json(
        TARGET_TABLE,
        [row],
    )

    if errors:
        raise RuntimeError(errors)


@app.get("/")
def health_check():
    """Check that the AI enricher service is running."""

    return jsonify({
        "service": "adzuna-ai-enricher",
        "status": "healthy",
    })


@app.post("/enrich")
def enrich_jobs():
    """Enrich a batch of jobs with Gemini and save them to BigQuery."""

    jobs = get_jobs_to_enrich()

    if not jobs:
        return jsonify({
            "status": "success",
            "jobs_enriched": 0,
            "message": "No new jobs to enrich.",
        })

    processed = 0
    failed = 0

    for job in jobs:
        try:
            result = enrich_job(job)
            save_result(job, result)
            processed += 1

        except Exception as error:
            print(f"Failed to enrich job {job.job_id}: {error}")
            failed += 1

    return jsonify({
        "status": "success",
        "jobs_enriched": processed,
        "jobs_failed": failed,
    })