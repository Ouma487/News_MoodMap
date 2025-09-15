# config.py

# ---------------------------
# Project / GCP
# ---------------------------
PROJECT_ID = "your-hackathon-project-id"   # overwritten by env if set
LOCATION = "us-central1"                   # BigQuery + Vertex AI location

# ---------------------------
# Pipeline Parameters
# ---------------------------
TOP_N = 80                # number of top countries to keep for analogs / briefings
ANALOG_SNIP_CHARS = 400   # max chars for snippet text
ANALOG_TOPK = 5           # number of similar past days to retrieve

CONTEXT_CHARS = 700       # how much text from topic_doc goes into LLM prompts
MAX_TOKENS = 300          # max tokens for LLM outputs
TEMP = 0.2                # LLM temperature for determinism

# ---------------------------
# Visualization
# ---------------------------
MOOD_RANGE = (-1, 1)      # range for mood_score colorbar
COLOR_SCALE = "RdYlGn"    # plotly color scale for choropleth
