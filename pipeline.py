# pipeline.py

from gcp_utils import init_gcp
from ingestion import ingest_gdelt_events, enrich_with_gkg
from processing import (
    create_daily_country_topics,
    create_remote_models,
    generate_news_embeddings,
    create_vector_search_functions,
    create_daily_top_entities,
    create_today_for_analogs,
    run_analog_searches,
    save_daily_analogs,
    create_daily_briefings,
    create_daily_moodmap,
)
from visualization import plot_global_moodmap


def main():
    print("\n🚀 Starting Global News MoodMap pipeline...")

    # 1. Init GCP
    client, project_id, location = init_gcp()


    # 2. Ingestion
    ingest_gdelt_events(client, project_id)
    enrich_with_gkg(client, project_id)

    # 3. Processing
    create_daily_country_topics(client, project_id)
    create_remote_models(client, project_id, location)
    generate_news_embeddings(client, project_id)
    create_vector_search_functions(client, project_id)
    create_daily_top_entities(client, project_id)
    create_today_for_analogs(client, project_id)

    # 4. Analog searches
    analogs_df = run_analog_searches(client, project_id)
    save_daily_analogs(client, project_id, analogs_df)

    # 5. Analytics
    create_daily_briefings(client, project_id)
    create_daily_moodmap(client, project_id)

    # 6. Visualization
    plot_global_moodmap(client, project_id)

    print("\n✅ Pipeline complete.")


if __name__ == "__main__":
    main()
