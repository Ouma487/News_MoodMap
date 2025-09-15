# ingestion.py


def ingest_gdelt_events(client, project_id):
    """
    Ingest raw GDELT events (last 60 days) into BigQuery.
    Creates or replaces the partitioned & clustered table.
    """
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.world_mood.gdelt_events_raw`
    PARTITION BY event_date
    CLUSTER BY country
    AS
    SELECT
      PARSE_DATE('%Y%m%d', CAST(SQLDATE AS STRING)) AS event_date,
      COALESCE(
        ActionGeo_CountryCode,
        Actor1Geo_CountryCode,
        Actor2Geo_CountryCode
      ) AS country,
      COALESCE(
        ActionGeo_ADM1Code,
        Actor1Geo_ADM1Code,
        Actor2Geo_ADM1Code
      ) AS admin1,
      COALESCE(ActionGeo_Lat, Actor1Geo_Lat, Actor2Geo_Lat) AS lat,
      COALESCE(ActionGeo_Long, Actor1Geo_Long, Actor2Geo_Long) AS lon,
      EventCode,
      EventBaseCode,
      EventRootCode,
      AvgTone AS tone,
      SOURCEURL AS url
    FROM `gdelt-bq.gdeltv2.events`
    WHERE SQLDATE >= CAST(FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)) AS INT64);
    """
    client.query(query).result()
    print("✅ GDELT events table created.")



def enrich_with_gkg(client, project_id):
    """
    Step 2: Enrich GDELT events with GKG data (themes, persons, orgs).
    Only keeps the last 7 days to reduce cost.
    """
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.world_mood.gdelt_events_enriched`
    PARTITION BY event_date
    CLUSTER BY country AS
    WITH
      base AS (
        SELECT
          event_date,
          country,
          admin1,
          lat,
          lon,
          EventCode,
          EventBaseCode,
          EventRootCode,
          tone,
          url
        FROM `{project_id}.world_mood.gdelt_events_raw`
        WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
          AND country IS NOT NULL
          AND url IS NOT NULL
      ),
      gkg AS (
        SELECT
          PARSE_DATE('%Y%m%d', SUBSTR(CAST(date AS STRING), 1, 8)) AS gkg_date,
          DocumentIdentifier AS url,
          V2Themes,
          V2Persons,
          V2Organizations
        FROM `gdelt-bq.gdeltv2.gkg`
        WHERE date >= CAST(FORMAT_DATE('%Y%m%d',
                   DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AS INT64) * 1000000
      ),
      joined AS (
        SELECT
          b.*,
          g.V2Themes,
          g.V2Persons,
          g.V2Organizations
        FROM base b
        LEFT JOIN gkg g
        USING (url)
      )
    SELECT
      event_date, country, admin1, lat, lon,
      EventCode, EventBaseCode, EventRootCode, tone, url,
      ARRAY(
        SELECT TRIM(x) FROM UNNEST(SPLIT(COALESCE(V2Themes, ''), ';')) x
        WHERE TRIM(x) != '' LIMIT 50
      ) AS themes,
      ARRAY(
        SELECT TRIM(x) FROM UNNEST(SPLIT(COALESCE(V2Persons, ''), ';')) x
        WHERE TRIM(x) != '' LIMIT 30
      ) AS persons,
      ARRAY(
        SELECT TRIM(x) FROM UNNEST(SPLIT(COALESCE(V2Organizations, ''), ';')) x
        WHERE TRIM(x) != '' LIMIT 30
      ) AS orgs
    FROM joined;
    """
    client.query(query).result()
    print("✅ gdelt_events_enriched created (7-day, with themes/persons/orgs)")
