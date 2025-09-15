# processing.py

def create_daily_country_topics(client, project_id):
    """
    Step 3: Aggregate raw GDELT events into daily country-level topics.
    Produces headline counts, tone stats, top event codes, and sample URLs.
    """
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.world_mood.daily_country_topics`
    PARTITION BY event_date
    CLUSTER BY country AS
    WITH country_day AS (
      SELECT
        event_date,
        country,
        COUNT(*) AS headline_count,
        AVG(tone) AS avg_tone,
        MIN(tone) AS min_tone,
        MAX(tone) AS max_tone,
        ARRAY_AGG(DISTINCT EventRootCode IGNORE NULLS LIMIT 20) AS top_event_types,
        ARRAY_AGG(DISTINCT url IGNORE NULLS LIMIT 30) AS sample_urls
      FROM `{project_id}.world_mood.gdelt_events_raw`
      WHERE country IS NOT NULL
        AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
      GROUP BY event_date, country
    )
    SELECT
      event_date,
      country,
      headline_count,
      avg_tone,
      min_tone,
      max_tone,
      top_event_types,
      sample_urls,
      CONCAT(
        'Country: ', country, ' | Date: ', CAST(event_date AS STRING), ' | ',
        'Headlines: ', CAST(headline_count AS STRING), ' | ',
        'Tone avg/min/max: ', CAST(ROUND(avg_tone,2) AS STRING), '/', 
                              CAST(ROUND(min_tone,2) AS STRING), '/', 
                              CAST(ROUND(max_tone,2) AS STRING), ' | ',
        'Top Event Codes: ', ARRAY_TO_STRING(top_event_types, ', '), ' | ',
        'Sample URLs: ', ARRAY_TO_STRING(sample_urls, ' | ')
      ) AS topic_doc
    FROM country_day;
    """
    client.query(query).result()
    print("✅ daily_country_topics created")


def create_remote_models(client, project_id, location):
    """
    Step 4: Create remote models in BigQuery ML for embeddings and generation.
    - Embedding model: gemini-embedding-001
    - Generative model: gemini-2.0-flash-001
    """
    # Embedding model
    embed_query = f"""
    CREATE OR REPLACE MODEL `{project_id}.world_mood.embedding_model`
      REMOTE WITH CONNECTION `{location}.kaggle-connection`
      OPTIONS (endpoint = 'gemini-embedding-001');
    """
    client.query(embed_query).result()
    print("✅ embedding_model created")

    # Generative model
    gen_query = f"""
    CREATE OR REPLACE MODEL `{project_id}.world_mood.gen_model`
      REMOTE WITH CONNECTION `{location}.kaggle-connection`
      OPTIONS (endpoint = 'gemini-2.0-flash-001');
    """
    client.query(gen_query).result()
    print("✅ gen_model created")


def generate_news_embeddings(client, project_id):
    """
    Step 5: Generate embeddings for daily country topics using the remote embedding model.
    Results are stored in the table world_mood.news_embeddings.
    """
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.world_mood.news_embeddings` AS
    SELECT
      t.event_date,
      t.country,
      t.content AS topic_doc,                     
      t.ml_generate_embedding_result AS embedding,
      CONCAT(t.country, '-', CAST(t.event_date AS STRING)) AS id
    FROM ML.GENERATE_EMBEDDING(
      MODEL `{project_id}.world_mood.embedding_model`,
      (
        SELECT
          event_date,
          country,
          SUBSTR(topic_doc, 1, 3000) AS content
        FROM `{project_id}.world_mood.daily_country_topics`
      )
    ) AS t;
    """
    client.query(query).result()
    print("✅ news_embeddings created")


def create_vector_search_functions(client, project_id):
    """
    Step 6: Create helper table functions for semantic search with embeddings.
    - fn_similar_to_text(query_text): search by free text
    - fn_similar_to_day(country_code, day): find similar past days for a given country
    """
    # Function 1: semantic search by free text query
    text_fn_query = f"""
    CREATE OR REPLACE TABLE FUNCTION `{project_id}.world_mood.fn_similar_to_text`(
      query_text STRING
    )
    RETURNS TABLE<
      event_date DATE,
      country STRING,
      topic_doc STRING,
      distance FLOAT64,
      id STRING
    >
    AS (
      SELECT
        vs.base.event_date     AS event_date,
        vs.base.country        AS country,
        vs.base.topic_doc      AS topic_doc,
        vs.distance            AS distance,
        vs.base.id             AS id
      FROM VECTOR_SEARCH(
        TABLE `{project_id}.world_mood.news_embeddings`,
        'embedding',
        (SELECT ml_generate_embedding_result AS embedding
         FROM ML.GENERATE_EMBEDDING(
           MODEL `{project_id}.world_mood.embedding_model`,
           (SELECT query_text AS content)
         )),
        'embedding',
        top_k => 10,
        distance_type => 'COSINE'
      ) AS vs
    );
    """
    client.query(text_fn_query).result()
    print("✅ fn_similar_to_text created")

    # Function 2: similar days for a given country+date
    day_fn_query = f"""
    CREATE OR REPLACE TABLE FUNCTION `{project_id}.world_mood.fn_similar_to_day`(
      country_code STRING,
      day DATE
    )
    RETURNS TABLE<
      event_date DATE,
      country STRING,
      topic_doc STRING,
      distance FLOAT64,
      id STRING
    >
    AS (
      WITH anchor AS (
        SELECT embedding
        FROM `{project_id}.world_mood.news_embeddings`
        WHERE country = country_code AND event_date = day
        LIMIT 1
      )
      SELECT
        vs.base.event_date     AS event_date,
        vs.base.country        AS country,
        vs.base.topic_doc      AS topic_doc,
        vs.distance            AS distance,
        vs.base.id             AS id
      FROM VECTOR_SEARCH(
        TABLE `{project_id}.world_mood.news_embeddings`,
        'embedding',
        (SELECT embedding FROM anchor),
        'embedding',
        top_k => 10,
        distance_type => 'COSINE'
      ) AS vs
      WHERE vs.base.event_date <> day
    );
    """
    client.query(day_fn_query).result()
    print("✅ fn_similar_to_day created")


def create_daily_top_entities(client, project_id):
    """
    Step 7: Create table of daily top entities (themes + people) per country.
    Uses exploded arrays, counts, and simple noise filters.
    """
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.world_mood.daily_top_entities`
    PARTITION BY event_date
    CLUSTER BY country AS
    WITH
    e AS (
      SELECT
        event_date,
        country,
        LOWER(SPLIT(t, ',')[OFFSET(0)]) AS theme,
        LOWER(SPLIT(p, ',')[OFFSET(0)]) AS person
      FROM `{project_id}.world_mood.gdelt_events_enriched`,
      UNNEST(themes) t,
      UNNEST(persons) p
      WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    ),
    clean_people AS (
      SELECT *
      FROM e
      WHERE person IS NOT NULL
        AND LENGTH(person) >= 3
        AND person NOT IN ('facebook','twitter','instagram',
                           'linkedin','whatsapp','youtube')
    ),
    theme_counts AS (
      SELECT event_date, country, theme, COUNT(*) AS c
      FROM e
      WHERE theme IS NOT NULL
      GROUP BY 1,2,3
    ),
    person_counts AS (
      SELECT event_date, country, person, COUNT(*) AS c
      FROM clean_people
      GROUP BY 1,2,3
    ),
    top_themes AS (
      SELECT event_date, country,
             ARRAY_AGG(STRUCT(theme, c) ORDER BY c DESC LIMIT 10) AS top_themes
      FROM theme_counts
      GROUP BY 1,2
    ),
    top_people AS (
      SELECT event_date, country,
             ARRAY_AGG(STRUCT(person, c) ORDER BY c DESC LIMIT 10) AS top_people
      FROM person_counts
      GROUP BY 1,2
    )
    SELECT
      t.event_date,
      t.country,
      top_themes,
      top_people
    FROM top_themes t
    LEFT JOIN top_people p
    USING (event_date, country);
    """
    client.query(query).result()
    print("✅ daily_top_entities created")
