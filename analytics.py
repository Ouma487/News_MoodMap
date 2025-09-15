# analytics.py

import pandas as pd
from google.cloud import bigquery

def create_today_for_analogs(client, project_id, top_n=80):
    """
    Step 8: Create a table of today's top N countries by headline count.
    Used later for analog searches / briefings.
    """
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.world_mood.today_for_analogs` AS
    WITH latest AS (
      SELECT MAX(event_date) AS d
      FROM `{project_id}.world_mood.daily_country_topics`
    )
    SELECT
      dct.event_date,
      dct.country
    FROM `{project_id}.world_mood.daily_country_topics` dct
    JOIN latest ON dct.event_date = latest.d
    ORDER BY dct.headline_count DESC
    LIMIT {top_n};
    """
    client.query(query).result()
    print(f"✅ today_for_analogs table created (top {top_n})")




def run_analog_searches(client, project_id, analog_snip_chars=400, analog_topk=5):
    """
    Step 9: For each country in today_for_analogs, find similar past days using fn_similar_to_day.
    Returns a DataFrame with event_date, country, past_date, snippet, and distance.
    """
    rows = client.query(
        f"SELECT country, event_date FROM `{project_id}.world_mood.today_for_analogs`"
    ).result()

    all_results = []
    for row in rows:
        country = row.country
        event_date = row.event_date

        sim_query = f"""
        SELECT
          '{event_date}' AS event_date,
          '{country}' AS country,
          event_date AS past_date,
          SUBSTR(REGEXP_REPLACE(topic_doc, r'https?://\\S+', ''), 1, {analog_snip_chars}) AS snippet,
          distance
        FROM `{project_id}.world_mood.fn_similar_to_day`('{country}', DATE '{event_date}')
        WHERE event_date < DATE '{event_date}'
        ORDER BY distance ASC
        LIMIT {analog_topk}
        """
        sim_rows = client.query(sim_query).result()

        for r in sim_rows:
            all_results.append((
                r.event_date,    # today’s date
                r.country,       # country code
                r.past_date,     # historical analog date
                r.snippet,       # truncated snippet
                r.distance       # cosine distance
            ))

    df = pd.DataFrame(all_results, columns=["event_date", "country", "past_date", "snippet", "distance"])
    print(f"✅ Analog searches complete ({len(df)} rows)")
    return df


def save_daily_analogs(client, project_id, df, analog_topk=5):
    """
    Step 10: Save analog search results into BigQuery.
    - daily_analogs_flat: raw rows (event_date, country, past_date, snippet, distance)
    - daily_analogs: aggregated per country/day with top-k analogs
    """

    # ✅ Ensure date types are proper
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
    df["past_date"] = pd.to_datetime(df["past_date"]).dt.date

    # BigQuery table ID
    table_id = f"{project_id}.world_mood.daily_analogs_flat"

    # Define schema + overwrite
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("event_date", "DATE"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("past_date", "DATE"),
            bigquery.SchemaField("snippet", "STRING"),
            bigquery.SchemaField("distance", "FLOAT"),
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    # Load into daily_analogs_flat
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print("✅ daily_analogs_flat loaded")

    # Create aggregated daily_analogs table
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.world_mood.daily_analogs`
    PARTITION BY event_date
    CLUSTER BY country AS
    WITH ranked AS (
      SELECT
        event_date,
        country,
        ARRAY_AGG(
          STRUCT(past_date, snippet, distance)
          ORDER BY distance ASC
          LIMIT {analog_topk}
        ) AS analogs
      FROM `{project_id}.world_mood.daily_analogs_flat`
      GROUP BY event_date, country
    )
    SELECT
      event_date,
      country,
      analogs,
      ARRAY_TO_STRING(
        ARRAY(
          SELECT
            CONCAT(CAST(a.past_date AS STRING), ': ', a.snippet)
          FROM UNNEST(analogs) AS a
          ORDER BY a.distance ASC
        ),
        '\\n- '
      ) AS analogs_txt
    FROM ranked;
    """
    client.query(query).result()
    print("✅ daily_analogs created")


def create_daily_briefings(client, project_id, top_n=80, context_chars=700, max_tokens=300, temp=0.2):
    """
    Step 11: Generate daily briefings per country using the generative model.
    Enriches with top entities and analog history.
    """
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.world_mood.daily_briefings`
    PARTITION BY event_date AS

    WITH latest AS (
      SELECT MAX(event_date) AS d
      FROM `{project_id}.world_mood.daily_country_topics`
    ),

    to_summarize AS (
      SELECT
        dct.event_date,
        dct.country,
        dct.headline_count,
        SUBSTR(REGEXP_REPLACE(dct.topic_doc, r'https?://\\S+', ''), 1, {context_chars}) AS ctx,
        ARRAY_TO_STRING(ARRAY(SELECT t.theme  FROM UNNEST(ent.top_themes) t), ', ') AS themes_top,
        ARRAY_TO_STRING(ARRAY(SELECT p.person FROM UNNEST(ent.top_people) p), ', ') AS people_top,
        analogs.analogs_txt
      FROM `{project_id}.world_mood.daily_country_topics` dct
      JOIN latest ON dct.event_date = latest.d
      LEFT JOIN `{project_id}.world_mood.daily_top_entities` ent
        ON dct.event_date = ent.event_date AND dct.country = ent.country
      LEFT JOIN `{project_id}.world_mood.daily_analogs` analogs
        ON dct.event_date = analogs.event_date AND dct.country = analogs.country
      ORDER BY dct.headline_count DESC
      LIMIT {top_n}
    ),

    normalized AS (
      SELECT
        event_date,
        country,
        headline_count,
        ctx,
        COALESCE(analogs_txt, 'None') AS analogs_txt,
        INITCAP(
          TRIM(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      LOWER(COALESCE(themes_top,'')),
                      r'(?i)uspec_', ''
                    ),
                    r'(?i)wb_\\d+_', ''
                  ),
                  r'(?i)tax_fncact_', 'tax '
                ),
                r'(?i)crisislex_[^, ]+', 'crisis response'
              ),
              r'_', ' '
            )
          )
        ) AS themes_nice,
        INITCAP(
          TRIM(
            REGEXP_REPLACE(COALESCE(people_top,''), r'(?i)\\bLos Angeles\\b,?\\s*', '')
          )
        ) AS people_nice
      FROM to_summarize
    ),

    llm AS (
      SELECT
        s.event_date,
        s.country,
        t.ml_generate_text_llm_result AS llm_text,
        TO_JSON_STRING(t) AS llm_raw
      FROM normalized s
      JOIN ML.GENERATE_TEXT(
        MODEL `{project_id}.world_mood.gen_model`,
        (
          SELECT
            event_date,
            country,
            CONCAT(
              'You are a news analyst. Country is an ISO code.', CHR(10),
              'Fill in this EXACT template with ≤90 words, no intro/outro:', CHR(10),
              '[What happened] ', CHR(10),
              '[Key drivers] ', CHR(10),
              '[Impact] ', CHR(10),
              '[Watch next] ', CHR(10),
              'Rules:', CHR(10),
              '- Only summarize events for ISO code ', country, '.', CHR(10),
              '- Use at least TWO items from "Top themes (readable)" and at least ONE name from "Top people".', CHR(10),
              '- Do NOT output raw taxonomy tokens. Use natural English phrases.', CHR(10),
              '- Do NOT mention event codes. Avoid boilerplate.', CHR(10),
              '- Include at least one concrete number if available.', CHR(10),
              '- ALWAYS include all 4 sections.', CHR(10),
              '- Limit each section to 1–2 sentences. Be concise and specific.', CHR(10),
              'Top themes (readable): ', COALESCE(themes_nice, 'none'), CHR(10),
              'Top people: ', COALESCE(people_nice, 'none'), CHR(10),
              'Context: ', ctx, CHR(10),
              'Relevant past events to consider (analog history):', CHR(10),
              analogs_txt
            ) AS prompt
          FROM normalized
        ),
        STRUCT({temp} AS temperature, {max_tokens} AS max_output_tokens, TRUE AS flatten_json_output)
      ) AS t
      ON s.event_date = t.event_date AND s.country = t.country
    ),

    fallback AS (
      SELECT
        event_date,
        country,
        llm_text,
        SAFE.PARSE_JSON(llm_raw) AS j
      FROM llm
    ),

    final AS (
      SELECT
        event_date,
        country,
        COALESCE(
          llm_text,
          JSON_VALUE(j, '$.ml_generate_text_result.candidates[0].content.parts[0].text'),
          JSON_VALUE(j, '$.ml_generate_text_result.candidates[0].content[0].text'),
          JSON_VALUE(j, '$.predictions[0].content'),
          JSON_VALUE(j, '$.text')
        ) AS briefing_text
      FROM fallback
    )

    SELECT *
    FROM final
    WHERE briefing_text IS NOT NULL;
    """
    client.query(query).result()
    print("✅ daily_briefings created (with analog enrichment)")


def create_daily_moodmap(client, project_id, temp=0.2, max_tokens=300):
    """
    Step 12: Create the final daily_moodmap table.
    Combines avg_tone + LLM sentiment, produces mood_score, and adds hover summaries.
    """
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.world_mood.daily_moodmap`
    PARTITION BY event_date AS

    WITH
    latest_date AS (
      SELECT MAX(event_date) AS d
      FROM `{project_id}.world_mood.daily_briefings`
    ),

    joined_data AS (
      SELECT
        b.event_date,
        b.country,
        b.briefing_text,
        g.avg_tone,
        ent.top_themes
      FROM `{project_id}.world_mood.daily_briefings` b
      JOIN latest_date l ON b.event_date = l.d
      LEFT JOIN `{project_id}.world_mood.daily_country_topics` g
        ON b.event_date = g.event_date AND b.country = g.country
      LEFT JOIN `{project_id}.world_mood.daily_top_entities` ent
        ON b.event_date = ent.event_date AND b.country = ent.country
    ),

    llm_outputs AS (
      SELECT
        s.event_date,
        s.country,
        s.avg_tone,
        s.top_themes,
        s.briefing_text,
        t.ml_generate_text_llm_result AS llm_text,
        TO_JSON_STRING(t) AS llm_raw
      FROM joined_data s
      JOIN ML.GENERATE_TEXT(
        MODEL `{project_id}.world_mood.gen_model`,
        (
          SELECT
            event_date,
            country,
            CONCAT(
              'You are a sentiment analysis model. Read the news briefing and respond with a single number between -1 (very negative) and 1 (very positive).\\n',
              'Briefing:\\n',
              briefing_text, '\\n',
              'Sentiment score:'
            ) AS prompt
          FROM joined_data
        ),
        STRUCT({temp} AS temperature, {max_tokens} AS max_output_tokens, TRUE AS flatten_json_output)
      ) AS t
      ON s.event_date = t.event_date AND s.country = t.country
    ),

    parsed_scores AS (
      SELECT
        event_date,
        country,
        avg_tone,
        top_themes,
        briefing_text,
        SAFE_CAST(REGEXP_EXTRACT(llm_text, r"-?\\d+\\.\\d+") AS FLOAT64) AS sentiment_score
      FROM llm_outputs
    ),

    blended_scores AS (
      SELECT
        event_date,
        country,
        ROUND((
          SAFE_CAST(sentiment_score AS FLOAT64) +
          SAFE_CAST(avg_tone AS FLOAT64) / 10
        ) / 2, 4) AS mood_score,
        top_themes,
        briefing_text
      FROM parsed_scores
    ),

    short_summary AS (
      SELECT
        s.event_date,
        s.country,
        s.mood_score,
        s.top_themes,
        s.briefing_text,
        t.ml_generate_text_llm_result AS summary_ref
      FROM blended_scores s
      JOIN ML.GENERATE_TEXT(
        MODEL `{project_id}.world_mood.gen_model`,
        (
          SELECT
            event_date,
            country,
            CONCAT(
              'Summarize today’s news mood for the ISO country code "', country, '". ',
              'Write ONE sentence (≤25 words). ',
              'Do not use labels. ',
              'Keep it concise, neutral, and hover-friendly.'
            ) AS prompt
          FROM blended_scores
        ),
        STRUCT({temp} AS temperature, 60 AS max_output_tokens, TRUE AS flatten_json_output)
      ) AS t
      ON s.event_date = t.event_date AND s.country = t.country
    )

    SELECT *
    FROM short_summary
    WHERE mood_score IS NOT NULL;
    """
    client.query(query).result()
    print("✅ daily_moodmap created (with normalized tone, sentiment score, and short hover summary)")

