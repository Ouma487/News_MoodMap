# visualization.py

import pycountry
import plotly.express as px


def plot_global_moodmap(client, project_id: str):
    """
    Step 13: Visualize the latest daily_moodmap with a Plotly choropleth.
    - Uses mood_score for color scale
    - Shows hover with summary text
    """
    # Query the latest moodmap snapshot
    latest_sql = f"""
    SELECT *
    FROM `{project_id}.world_mood.daily_moodmap`
    WHERE event_date = (SELECT MAX(event_date) 
                        FROM `{project_id}.world_mood.daily_moodmap`)
    """
    today_df = client.query(latest_sql).to_dataframe()

    # Convert ISO-2 → ISO-3 (for Plotly choropleth)
    def iso2_to_iso3(iso2):
        try:
            return pycountry.countries.get(alpha_2=iso2).alpha_3
        except:
            return None

    today_df["iso3"] = today_df["country"].apply(iso2_to_iso3)

    # Build choropleth
    fig = px.choropleth(
        today_df,
        locations="iso3",
        locationmode="ISO-3",
        color="mood_score",
        color_continuous_scale=px.colors.diverging.RdYlGn,
        range_color=(-1, 1),
        title="🌍 Global News MoodMap"
    )

    # Custom hover: structured, with summary text
    fig.update_traces(
        hovertemplate=(
            "<b>%{hovertext}</b><br>" +
            "Mood Score: <b>%{z:.2f}</b><br><br>" +
            "<b>Summary:</b><br>%{customdata[0]}"
        ),
        hovertext=today_df["country"],          # ISO-2 code → displayed on hover
        customdata=today_df[["summary_ref"]]    # one-sentence summary
    )

    # Layout tweaks
    fig.update_layout(
        title=dict(
            text="🌍 Global News MoodMap<br><sup>Data from GDELT + BigQuery AI</sup>",
            x=0.5, xanchor="center"
        ),
        geo=dict(
            showframe=False,
            showcoastlines=True,
            coastlinecolor="LightGray",
            projection_type="natural earth",
            showcountries=True,
            countrycolor="white"
        ),
        coloraxis_colorbar=dict(
            title="Mood",
            tickvals=[-1, -0.5, 0, 0.5, 1],
            ticktext=["😡 Very Negative", "😟 Negative", "😐 Neutral", "🙂 Positive", "😃 Very Positive"],
            len=0.75
        ),
        margin=dict(l=0, r=0, t=60, b=0)
    )

    fig.show()
    return fig
