# gcp_utils.py

import os
from google.cloud import bigquery
import vertexai
import config


def init_gcp():
    """
    Initialize Vertex AI + BigQuery client with hackathon-safe auth.
    Uses GOOGLE_CLOUD_PROJECT env var if available, else falls back to config.PROJECT_ID.
    """
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", config.PROJECT_ID)
    location = config.LOCATION

    # Init Vertex AI + BigQuery
    vertexai.init(project=project_id, location=location)
    client = bigquery.Client(project=project_id, location=location)

    print(f"✅ Vertex AI + BigQuery ready (project: {project_id}, location: {location})")
    return client, project_id, location


def create_schema(client, project_id):
    """
    Create the BigQuery dataset `world_mood` if it doesn't exist.
    Sets default expiration of 60 days for all tables.
    """
    query = f"""
    CREATE SCHEMA IF NOT EXISTS `{project_id}.world_mood`
    OPTIONS(
    location="{config.LOCATION}",
    default_table_expiration_days=60
    );
    """
    client.query(query).result()
    print("Dataset created.")
