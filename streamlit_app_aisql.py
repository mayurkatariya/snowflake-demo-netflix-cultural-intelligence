"""
Netflix Cultural Intelligence Platform v2 — AI SQL Functions Edition
Powered by SocialGist Marketplace Data + Snowflake Cortex AI SQL Functions
Uses the NEW AI SQL functions: AI_COMPLETE, AI_CLASSIFY, AI_SENTIMENT,
AI_FILTER, AI_AGG, AI_SUMMARIZE_AGG (replacing legacy SNOWFLAKE.CORTEX.*)
Deployed as Streamlit-in-Snowflake (SiS).
"""

import streamlit as st
import pandas as pd
import altair as alt
import json
import _snowflake
from datetime import datetime, timedelta
from snowflake.snowpark.context import get_active_session
from snowflake.core import Root

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG — SocialGist Marketplace data
# ─────────────────────────────────────────────────────────────────────────────
DB = "NETFLIX__ROMANTASY_AIREADY_SOCIAL_DATA"
SCHEMA = "NETFLIX_TRIAL"
FQ = f"{DB}.{SCHEMA}"

# Views
CONTENT_ITEMS = f"{FQ}.CONTENT_ITEMS"
CONTENT_TEXT = f"{FQ}.CONTENT_TEXT"
CONTENT_CHUNKS = f"{FQ}.CONTENT_CHUNKS"
CONTENT_TAG = f"{FQ}.CONTENT_TAG"
TAGS = f"{FQ}.TAGS"
AUTHORS = f"{FQ}.AUTHORS"
REDDIT_AUTHORS = f"{FQ}.REDDIT_AUTHORS"
TIKTOK_POSTS = f"{FQ}.TIKTOK_POSTS"
TIKTOK_CREATORS = f"{FQ}.TIKTOK_CREATORS"
CONTENT_RELATIONSHIPS = f"{FQ}.CONTENT_RELATIONSHIPS"

# Cortex Search Services
TEXT_SEARCH_SVC = "TEXT_CHUNK_SEARCH"
AUTHOR_SEARCH_SVC = "AUTHOR_DESCRIPTION_SEARCH"

# Semantic View for Cortex Analyst
SEMANTIC_VIEW = "SOCIAL_CONVERSATIONS"

# Subreddit ID -> name mapping (from the data)
SUBREDDIT_MAP = {
    "t5_2qoxj": "r/netflix",
    "t5_afrseg": "r/Romantasy",
    "t5_3pojgw": "r/fantasyromance",
    "r/romantasy": "r/romantasy",
    "r/netflix": "r/netflix",
    "r/fantasyromance": "r/fantasyromance",
    "r/bestofnetflix": "r/bestofnetflix",
    "t5_2s1cv": "r/television",
}

# ─────────────────────────────────────────────────────────────────────────────
# NETFLIX BRAND COLORS
# ─────────────────────────────────────────────────────────────────────────────
NETFLIX_RED = "#E50914"
NETFLIX_DARK_RED = "#B20710"
NETFLIX_BLACK = "#141414"
NETFLIX_DARK_GRAY = "#1F1F1F"
NETFLIX_MID_GRAY = "#2B2B2B"
NETFLIX_LIGHT_GRAY = "#E5E5E5"
NETFLIX_WHITE = "#FFFFFF"

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Netflix Cultural Intelligence",
    page_icon="https://assets.nflxext.com/us/ffe/siteui/common/icons/nficon2023.ico",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# NETFLIX THEME CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
    /* Main background */
    .stApp {{
        background-color: {NETFLIX_BLACK};
        color: {NETFLIX_LIGHT_GRAY};
    }}

    /* Sidebar styling */
    section[data-testid="stSidebar"] {{
        background-color: {NETFLIX_DARK_GRAY};
        border-right: 2px solid {NETFLIX_RED};
    }}
    section[data-testid="stSidebar"] .stMarkdown {{
        color: {NETFLIX_LIGHT_GRAY};
    }}

    /* Headers */
    h1, h2, h3, .stSubheader {{
        color: {NETFLIX_WHITE} !important;
    }}

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 2px;
        background-color: {NETFLIX_DARK_GRAY};
        border-radius: 8px;
        padding: 4px;
    }}
    .stTabs [data-baseweb="tab"] {{
        color: {NETFLIX_LIGHT_GRAY};
        background-color: transparent;
        border-radius: 6px;
        padding: 8px 16px;
        font-weight: 500;
    }}
    .stTabs [aria-selected="true"] {{
        background-color: {NETFLIX_RED} !important;
        color: {NETFLIX_WHITE} !important;
        font-weight: 700;
    }}

    /* Metric cards */
    [data-testid="stMetric"] {{
        background-color: {NETFLIX_DARK_GRAY};
        border: 1px solid {NETFLIX_MID_GRAY};
        border-left: 4px solid {NETFLIX_RED};
        border-radius: 8px;
        padding: 12px 16px;
    }}
    [data-testid="stMetricValue"] {{
        color: {NETFLIX_WHITE} !important;
        font-weight: 700;
    }}
    [data-testid="stMetricLabel"] {{
        color: {NETFLIX_LIGHT_GRAY} !important;
    }}

    /* Buttons */
    .stButton > button {{
        background-color: {NETFLIX_RED};
        color: {NETFLIX_WHITE};
        border: none;
        border-radius: 4px;
        font-weight: 600;
        padding: 8px 24px;
    }}
    .stButton > button:hover {{
        background-color: {NETFLIX_DARK_RED};
        color: {NETFLIX_WHITE};
    }}

    /* Containers / cards */
    [data-testid="stVerticalBlock"] > div[data-testid="stExpander"],
    .stContainer {{
        border-color: {NETFLIX_MID_GRAY};
    }}

    /* DataFrames */
    .stDataFrame {{
        border: 1px solid {NETFLIX_MID_GRAY};
        border-radius: 8px;
    }}

    /* Info / warning / success boxes */
    .stAlert {{
        border-radius: 8px;
    }}

    /* Text input */
    .stTextInput > div > div > input {{
        background-color: {NETFLIX_DARK_GRAY};
        color: {NETFLIX_WHITE};
        border: 1px solid {NETFLIX_MID_GRAY};
    }}
    .stTextInput > div > div > input::placeholder {{
        color: #999999 !important;
    }}

    /* Selectbox */
    .stSelectbox > div > div {{
        background-color: {NETFLIX_DARK_GRAY};
        color: {NETFLIX_WHITE};
    }}

    /* Slider */
    .stSlider > div > div > div {{
        color: {NETFLIX_RED};
    }}

    /* Divider */
    hr {{
        border-color: {NETFLIX_MID_GRAY};
    }}

    /* ── READABILITY: Force all text to be light on dark background ── */

    /* All markdown text, paragraphs, list items */
    .stMarkdown, .stMarkdown p, .stMarkdown li, .stMarkdown span,
    .stMarkdown ul, .stMarkdown ol,
    .stMarkdown h4, .stMarkdown h5, .stMarkdown h6 {{
        color: {NETFLIX_LIGHT_GRAY} !important;
    }}

    /* Bold text inside markdown */
    .stMarkdown strong, .stMarkdown b {{
        color: {NETFLIX_WHITE} !important;
    }}

    /* Captions */
    .stCaption, [data-testid="stCaptionContainer"],
    [data-testid="stCaptionContainer"] p,
    [data-testid="stCaptionContainer"] span {{
        color: #999999 !important;
    }}

    /* Radio buttons — labels and options */
    .stRadio label, .stRadio p,
    .stRadio [data-testid="stMarkdownContainer"] p,
    .stRadio div[role="radiogroup"] label,
    .stRadio div[role="radiogroup"] label p,
    .stRadio div[role="radiogroup"] label span {{
        color: {NETFLIX_LIGHT_GRAY} !important;
    }}

    /* Multiselect, selectbox, text input labels */
    .stMultiSelect label, .stMultiSelect label p,
    .stSelectbox label, .stSelectbox label p,
    .stTextInput label, .stTextInput label p,
    .stSlider label, .stSlider label p,
    .stDateInput label, .stDateInput label p,
    .stNumberInput label, .stNumberInput label p {{
        color: {NETFLIX_LIGHT_GRAY} !important;
    }}

    /* Expander headers and content */
    [data-testid="stExpander"] summary,
    [data-testid="stExpander"] summary span,
    [data-testid="stExpander"] summary p,
    [data-testid="stExpander"] [data-testid="stMarkdownContainer"],
    [data-testid="stExpander"] [data-testid="stMarkdownContainer"] p {{
        color: {NETFLIX_LIGHT_GRAY} !important;
    }}

    /* Containers / bordered cards — ensure text inside is readable */
    [data-testid="stVerticalBlockBorderWrapper"] p,
    [data-testid="stVerticalBlockBorderWrapper"] span,
    [data-testid="stVerticalBlockBorderWrapper"] li {{
        color: {NETFLIX_LIGHT_GRAY} !important;
    }}

    /* Blockquotes (used in tag summaries) */
    .stMarkdown blockquote, .stMarkdown blockquote p {{
        color: #CCCCCC !important;
        border-left-color: {NETFLIX_RED} !important;
    }}

    /* Info, warning, success, error boxes — keep their own text color but ensure readability */
    [data-testid="stAlert"] p {{
        color: inherit !important;
    }}

    /* Spinner text */
    .stSpinner > div {{
        color: {NETFLIX_LIGHT_GRAY} !important;
    }}

    /* Tooltip / help text */
    [data-testid="stTooltipIcon"] {{
        color: {NETFLIX_LIGHT_GRAY} !important;
    }}

    /* Container borders — make them visible on dark bg */
    [data-testid="stVerticalBlockBorderWrapper"] {{
        border-color: {NETFLIX_MID_GRAY} !important;
    }}

    /* Horizontal radio group — ensure selected option stands out */
    .stRadio div[role="radiogroup"] label[data-checked="true"] p,
    .stRadio div[role="radiogroup"] label[data-checked="true"] span {{
        color: {NETFLIX_WHITE} !important;
        font-weight: 600;
    }}

    /* Tab content area text */
    .stTabs [data-baseweb="tab-panel"] p,
    .stTabs [data-baseweb="tab-panel"] span,
    .stTabs [data-baseweb="tab-panel"] li {{
        color: {NETFLIX_LIGHT_GRAY} !important;
    }}

    /* Code blocks inside AI output */
    .stMarkdown code {{
        color: {NETFLIX_RED} !important;
        background-color: {NETFLIX_DARK_GRAY} !important;
    }}
    .stMarkdown pre {{
        background-color: {NETFLIX_DARK_GRAY} !important;
        border: 1px solid {NETFLIX_MID_GRAY} !important;
    }}
    .stMarkdown pre code {{
        color: {NETFLIX_LIGHT_GRAY} !important;
    }}

    /* st.code() blocks — "View generated SQL" and similar */
    [data-testid="stCode"], .stCode {{
        background-color: {NETFLIX_DARK_GRAY} !important;
        border: 1px solid {NETFLIX_MID_GRAY} !important;
        border-radius: 8px !important;
    }}
    [data-testid="stCode"] pre,
    [data-testid="stCode"] code,
    .stCode pre, .stCode code {{
        color: {NETFLIX_LIGHT_GRAY} !important;
        background-color: {NETFLIX_DARK_GRAY} !important;
    }}
    /* Syntax-highlighted tokens inside st.code() */
    [data-testid="stCode"] pre span,
    .stCode pre span {{
        color: inherit !important;
    }}
    /* Code copy button visibility */
    [data-testid="stCode"] button,
    .stCode button {{
        color: {NETFLIX_LIGHT_GRAY} !important;
        background-color: {NETFLIX_MID_GRAY} !important;
        border: none !important;
    }}

    /* Expander — dark background with light border and text */
    [data-testid="stExpander"] {{
        background-color: {NETFLIX_DARK_GRAY} !important;
        border: 1px solid {NETFLIX_MID_GRAY} !important;
        border-radius: 8px !important;
    }}
    [data-testid="stExpander"] details {{
        background-color: {NETFLIX_DARK_GRAY} !important;
    }}
    [data-testid="stExpander"] summary {{
        color: {NETFLIX_LIGHT_GRAY} !important;
    }}
    [data-testid="stExpander"] summary:hover {{
        color: {NETFLIX_WHITE} !important;
    }}

    /* DataFrames — dark header and readable cell text */
    .stDataFrame [data-testid="glideDataEditor"],
    .stDataFrame {{
        background-color: {NETFLIX_DARK_GRAY} !important;
    }}

    /* Success / Warning / Error / Info boxes — ensure text contrast */
    [data-testid="stAlert"] {{
        border-radius: 8px !important;
    }}
    [data-testid="stAlert"] p,
    [data-testid="stAlert"] span,
    [data-testid="stAlert"] li {{
        font-weight: 500 !important;
    }}
    /* Success box */
    .stSuccess, [data-testid="stAlert"][data-baseweb*="positive"] {{
        background-color: #0e2f1a !important;
        color: #66cc88 !important;
    }}
    .stSuccess p {{ color: #66cc88 !important; }}
    /* Warning box */
    .stWarning, [data-testid="stAlert"][data-baseweb*="warning"] {{
        background-color: #2f2a0e !important;
        color: #e6c94e !important;
    }}
    .stWarning p {{ color: #e6c94e !important; }}
    /* Error box */
    .stError, [data-testid="stAlert"][data-baseweb*="negative"] {{
        background-color: #2f0e0e !important;
        color: #e66060 !important;
    }}
    .stError p {{ color: #e66060 !important; }}

    /* JSON viewer / dict displays */
    .stJson {{
        background-color: {NETFLIX_DARK_GRAY} !important;
        border: 1px solid {NETFLIX_MID_GRAY} !important;
    }}

    /* Table elements (st.table) */
    .stTable table {{
        color: {NETFLIX_LIGHT_GRAY} !important;
    }}
    .stTable th {{
        background-color: {NETFLIX_MID_GRAY} !important;
        color: {NETFLIX_WHITE} !important;
    }}
    .stTable td {{
        color: {NETFLIX_LIGHT_GRAY} !important;
        border-color: {NETFLIX_MID_GRAY} !important;
    }}

    /* Catch-all: any white/near-white background inside main content area */
    [data-testid="stAppViewContainer"] pre {{
        background-color: {NETFLIX_DARK_GRAY} !important;
        color: {NETFLIX_LIGHT_GRAY} !important;
    }}
    [data-testid="stAppViewContainer"] code {{
        background-color: {NETFLIX_DARK_GRAY} !important;
    }}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# NETFLIX HEADER WITH LOGO
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="display: flex; align-items: center; gap: 16px; margin-bottom: 8px;">
    <div style="
        font-size: 48px;
        font-weight: 900;
        color: {NETFLIX_RED};
        letter-spacing: -2px;
        font-family: 'Helvetica Neue', Arial, sans-serif;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.5);
        line-height: 1;
    ">N</div>
    <div>
        <div style="
            font-size: 28px;
            font-weight: 700;
            color: {NETFLIX_WHITE};
            font-family: 'Helvetica Neue', Arial, sans-serif;
            letter-spacing: 1px;
        ">NETFLIX</div>
        <div style="
            font-size: 14px;
            color: {NETFLIX_RED};
            font-weight: 500;
            letter-spacing: 3px;
            text-transform: uppercase;
        ">Cultural Intelligence Platform</div>
    </div>
</div>
""", unsafe_allow_html=True)

st.caption(
    "SocialGist Marketplace Data  |  Snowflake Cortex **AI SQL Functions**  |  "
    "85K+ Social Posts  |  Capabilities better than Pulsar with as needed customizations"
)

# ─────────────────────────────────────────────────────────────────────────────
# SNOWFLAKE SESSION
# ─────────────────────────────────────────────────────────────────────────────
session = get_active_session()
root = Root(session)


def run_query(sql):
    """Execute SQL and return pandas DataFrame."""
    return session.sql(sql).to_pandas()


def clean_ai_text(text):
    """Clean AI-generated text so markdown renders correctly in Streamlit.
    AI models sometimes return literal backslash-n instead of real newlines,
    or mix formatting that Streamlit's st.markdown doesn't handle well."""
    if not text or not isinstance(text, str):
        return text or ""
    # Replace literal \n with real newlines
    text = text.replace("\\n", "\n")
    # Ensure headers have a blank line before them so markdown renders
    import re
    text = re.sub(r'([^\n])\n(#{1,4} )', r'\1\n\n\2', text)
    # Ensure bullet lists have a blank line before them
    text = re.sub(r'([^\n])\n(- )', r'\1\n\n\2', text)
    # Remove excessive blank lines (more than 2)
    text = re.sub(r'\n{4,}', '\n\n\n', text)
    return text.strip()


# ─────────────────────────────────────────────────────────────────────────────
# ALTAIR NETFLIX THEME
# ─────────────────────────────────────────────────────────────────────────────
NETFLIX_ALTAIR_COLORS = [NETFLIX_RED, "#FF6B6B", "#CC0000", "#FF3333",
                         "#E55959", "#B30000", "#FF8080", "#D63333",
                         "#990000", "#FF4D4D"]

NETFLIX_CAT_COLORS = [NETFLIX_RED, "#FF6B6B", "#4DCCBD", "#FFB347",
                      "#87CEEB", "#DDA0DD", "#98D8C8", "#F7DC6F",
                      "#BB8FCE", "#F1948A"]


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR FILTERS
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"""
    <div style="text-align: center; margin-bottom: 16px;">
        <span style="font-size: 36px; font-weight: 900; color: {NETFLIX_RED};
                     letter-spacing: -2px;">NETFLIX</span>
    </div>
    """, unsafe_allow_html=True)

    st.header("Filters")

    default_end = datetime.today().date()
    default_start = default_end - timedelta(days=28)
    date_range = st.date_input(
        "Date range",
        value=(default_start, default_end),
        max_value=default_end,
    )
    if len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = default_start, default_end

    selected_platforms = st.multiselect(
        "Platforms",
        options=["reddit", "tiktok", "youtube"],
        default=["reddit", "tiktok", "youtube"],
    )

    selected_entity_types = st.multiselect(
        "Content types",
        options=["comment", "video", "link"],
        default=["comment", "video", "link"],
    )

    tag_filter = st.text_input(
        "Tag / topic filter",
        placeholder="e.g., romantasy, bridgerton, booktok",
    )

    ai_sample_size = st.slider(
        "AI sample size (rows)",
        min_value=10,
        max_value=200,
        value=50,
        step=10,
        help="Limits rows sent to Cortex AI functions to control cost.",
    )

    st.divider()
    st.markdown("**Architecture**")
    st.markdown(
        "- Data: **SocialGist** via Marketplace\n"
        "- AI: **Cortex AI SQL Functions**\n"
        "- Search: **Cortex Search Services**\n"
        "- NL Queries: **Cortex Analyst**\n"
        "- Compute: your warehouse\n"
        "- Storage cost: **$0** (zero-copy share)"
    )

    st.divider()
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, {NETFLIX_DARK_RED}, {NETFLIX_RED});
        padding: 12px;
        border-radius: 8px;
        margin-top: 8px;
    ">
        <div style="font-weight: 700; color: {NETFLIX_WHITE}; margin-bottom: 8px;">
            AI SQL Functions Used
        </div>
        <div style="font-size: 12px; color: {NETFLIX_WHITE}; line-height: 1.8;">
            AI_COMPLETE &bull; AI_CLASSIFY<br/>
            AI_SENTIMENT &bull; AI_FILTER<br/>
            AI_AGG &bull; AI_SUMMARIZE_AGG
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.divider()
    st.markdown("**Pulsar Feature Parity**")
    st.markdown(
        "- :white_check_mark: Social Listening (TRAC)\n"
        "- :white_check_mark: Audience Segmentation\n"
        "- :white_check_mark: Narrative Detection\n"
        "- :white_check_mark: Community Mapping\n"
        "- :white_check_mark: Trend Radar (TRENDS)\n"
        "- :white_check_mark: AI Search & Summaries\n"
        "- :white_check_mark: Sentiment Analysis\n"
        "- :star: Cortex Analyst (NL->SQL)\n"
        "  *(No Pulsar equivalent)*"
    )


# ─────────────────────────────────────────────────────────────────────────────
# COMMON WHERE CLAUSE BUILDER
# ─────────────────────────────────────────────────────────────────────────────
def build_where(table_alias="ci"):
    clauses = [
        f"{table_alias}.PUBLISHED_AT BETWEEN '{start_date}' AND '{end_date} 23:59:59'",
    ]
    if selected_platforms:
        pl = ", ".join(f"'{p}'" for p in selected_platforms)
        clauses.append(f"{table_alias}.PLATFORM IN ({pl})")
    if selected_entity_types:
        et = ", ".join(f"'{e}'" for e in selected_entity_types)
        clauses.append(f"{table_alias}.ENTITY_TYPE IN ({et})")
    return " AND ".join(clauses)


def build_tag_join_and_filter():
    """Returns (join_clause, where_clause) for tag filtering."""
    if not tag_filter:
        return "", ""
    safe_tag = tag_filter.replace("'", "''").lower()
    join_clause = f"""
        JOIN {CONTENT_TAG} ct ON ci.OBJECT_ID = ct.OBJECT_ID
        JOIN {TAGS} t ON ct.TAG_ID = t.TAG_ID
    """
    where_clause = f" AND t.TAG_NORMALIZED LIKE '%{safe_tag}%'"
    return join_clause, where_clause


WHERE = build_where()

# ─────────────────────────────────────────────────────────────────────────────
# TAB LAYOUT
# ─────────────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10 = st.tabs([
    "Overview",
    "Sentiment Analysis",
    "Trend Classification",
    "AI Search & Chat",
    "Conversation Explorer",
    "Audience Segments",
    "Narrative Detection",
    "Community Map",
    "Trend Radar",
    "Cortex Analyst",
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1: OVERVIEW DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.subheader("Social Conversation Overview")
    st.caption("Volume, platforms, and trending tags across the SocialGist feed")

    tag_join, tag_where = build_tag_join_and_filter()

    # KPIs
    try:
        kpi_sql = f"""
            SELECT
                COUNT(DISTINCT ci.OBJECT_ID) AS TOTAL_POSTS,
                COUNT(DISTINCT ci.AUTHOR_ID) AS UNIQUE_AUTHORS,
                COUNT(DISTINCT ci.PLATFORM)  AS PLATFORM_COUNT,
                MIN(ci.PUBLISHED_AT)         AS EARLIEST,
                MAX(ci.PUBLISHED_AT)         AS LATEST
            FROM {CONTENT_ITEMS} ci
            {tag_join}
            WHERE {WHERE}{tag_where}
        """
        kpi = run_query(kpi_sql).iloc[0]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Posts", f"{int(kpi['TOTAL_POSTS']):,}")
        c2.metric("Unique Authors", f"{int(kpi['UNIQUE_AUTHORS']):,}")
        c3.metric("Platforms", int(kpi['PLATFORM_COUNT']))
        c4.metric("Date Range", f"{str(kpi['EARLIEST'])[:10]} to {str(kpi['LATEST'])[:10]}")
    except Exception as e:
        st.warning(f"Could not load KPIs: {e}")

    col_left, col_right = st.columns(2)

    with col_left:
        with st.container(border=True):
            st.markdown("**Post Volume Over Time by Platform**")
            try:
                trend_sql = f"""
                    SELECT DATE_TRUNC('day', ci.PUBLISHED_AT) AS POST_DAY,
                           ci.PLATFORM,
                           COUNT(*) AS DAILY_POSTS
                    FROM {CONTENT_ITEMS} ci
                    {tag_join}
                    WHERE {WHERE}{tag_where}
                    GROUP BY POST_DAY, ci.PLATFORM
                    ORDER BY POST_DAY
                """
                trend_df = run_query(trend_sql)
                chart = (
                    alt.Chart(trend_df)
                    .mark_area(opacity=0.7)
                    .encode(
                        x=alt.X("POST_DAY:T", title="Date"),
                        y=alt.Y("DAILY_POSTS:Q", title="Posts", stack=True),
                        color=alt.Color("PLATFORM:N", title="Platform",
                                        scale=alt.Scale(range=[NETFLIX_RED, "#FF6B6B", "#4DCCBD"])),
                        tooltip=["POST_DAY:T", "PLATFORM:N", "DAILY_POSTS:Q"],
                    )
                    .properties(height=350)
                )
                st.altair_chart(chart, use_container_width=True)
            except Exception as e:
                st.info(f"Could not load trends: {e}")

    with col_right:
        with st.container(border=True):
            st.markdown("**Posts by Platform**")
            try:
                platform_sql = f"""
                    SELECT ci.PLATFORM, COUNT(*) AS POST_COUNT
                    FROM {CONTENT_ITEMS} ci
                    {tag_join}
                    WHERE {WHERE}{tag_where}
                    GROUP BY ci.PLATFORM
                    ORDER BY POST_COUNT DESC
                """
                plat_df = run_query(platform_sql)
                pie = (
                    alt.Chart(plat_df)
                    .mark_arc(innerRadius=50)
                    .encode(
                        theta=alt.Theta("POST_COUNT:Q"),
                        color=alt.Color("PLATFORM:N", title="Platform",
                                        scale=alt.Scale(range=[NETFLIX_RED, "#FF6B6B", "#4DCCBD"])),
                        tooltip=["PLATFORM", "POST_COUNT"],
                    )
                    .properties(height=350)
                )
                st.altair_chart(pie, use_container_width=True)
            except Exception as e:
                st.info(f"Could not load platform chart: {e}")

    # Top Tags
    with st.container(border=True):
        st.markdown("**Top 20 Trending Tags**")
        try:
            tags_sql = f"""
                SELECT t.TAG_NORMALIZED AS TAG, COUNT(*) AS TAG_COUNT
                FROM {CONTENT_ITEMS} ci
                JOIN {CONTENT_TAG} ct ON ci.OBJECT_ID = ct.OBJECT_ID
                JOIN {TAGS} t ON ct.TAG_ID = t.TAG_ID
                WHERE {WHERE}
                  AND t.TAG_NORMALIZED IS NOT NULL
                  AND LENGTH(t.TAG_NORMALIZED) > 2
                GROUP BY t.TAG_NORMALIZED
                ORDER BY TAG_COUNT DESC
                LIMIT 20
            """
            tags_df = run_query(tags_sql)
            chart = (
                alt.Chart(tags_df)
                .mark_bar()
                .encode(
                    x=alt.X("TAG_COUNT:Q", title="Frequency"),
                    y=alt.Y("TAG:N", sort="-x", title="Tag"),
                    color=alt.Color(
                        "TAG_COUNT:Q",
                        scale=alt.Scale(scheme="reds"),
                        legend=None,
                    ),
                    tooltip=["TAG", "TAG_COUNT"],
                )
                .properties(height=500)
            )
            st.altair_chart(chart, use_container_width=True)
        except Exception as e:
            st.info(f"Could not load tags: {e}")

    # Top Reddit communities
    with st.container(border=True):
        st.markdown("**Top Reddit Communities**")
        try:
            sub_sql = f"""
                SELECT ci.CHANNEL_ID, COUNT(*) AS POST_COUNT
                FROM {CONTENT_ITEMS} ci
                WHERE {WHERE} AND ci.PLATFORM = 'reddit'
                  AND ci.CHANNEL_ID IS NOT NULL
                GROUP BY ci.CHANNEL_ID
                ORDER BY POST_COUNT DESC
                LIMIT 10
            """
            sub_df = run_query(sub_sql)
            sub_df["SUBREDDIT"] = sub_df["CHANNEL_ID"].map(
                lambda x: SUBREDDIT_MAP.get(x, x)
            )
            chart = (
                alt.Chart(sub_df)
                .mark_bar()
                .encode(
                    x=alt.X("POST_COUNT:Q", title="Posts"),
                    y=alt.Y("SUBREDDIT:N", sort="-x", title="Community"),
                    color=alt.value(NETFLIX_RED),
                    tooltip=["SUBREDDIT", "POST_COUNT"],
                )
                .properties(height=300)
            )
            st.altair_chart(chart, use_container_width=True)
        except Exception as e:
            st.info(f"Could not load subreddits: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2: SENTIMENT ANALYSIS — uses AI_SENTIMENT()
# ═══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("Sentiment Analysis")
    st.caption(
        "Real-time sentiment scoring using **AI_SENTIMENT()** — "
        "the new AI SQL function replacing SNOWFLAKE.CORTEX.SENTIMENT()"
    )

    if st.button("Run Sentiment Analysis", key="btn_sentiment"):
        with st.spinner("Running AI_SENTIMENT on social posts..."):
            try:
                sentiment_sql = f"""
                    SELECT
                        ci.PLATFORM,
                        ci.ENTITY_TYPE,
                        ci.PUBLISHED_AT,
                        ct_text.TEXT_VALUE AS POST_TEXT,
                        ct_text.TEXT_TYPE,
                        AI_SENTIMENT(ct_text.TEXT_VALUE):categories[0]:sentiment::STRING AS SENTIMENT_LABEL
                    FROM {CONTENT_ITEMS} ci
                    JOIN {CONTENT_TEXT} ct_text
                      ON ci.OBJECT_ID = ct_text.OBJECT_ID
                    WHERE {WHERE}
                      AND ct_text.TEXT_TYPE IN ('body', 'title')
                      AND ct_text.LANGUAGE_CODE = 'en'
                      AND LENGTH(ct_text.TEXT_VALUE) > 20
                    LIMIT {ai_sample_size}
                """
                sent_df = run_query(sentiment_sql)

                if len(sent_df) == 0:
                    st.warning("No posts matched your filters.")
                else:
                    # Map labels to numeric scores for charting
                    label_score_map = {
                        "positive": 0.7, "negative": -0.7,
                        "neutral": 0.0, "mixed": 0.1, "unknown": 0.0,
                    }
                    sent_df["SENTIMENT_SCORE"] = sent_df["SENTIMENT_LABEL"].str.lower().map(
                        label_score_map
                    ).fillna(0.0)

                    pos_pct = (sent_df["SENTIMENT_LABEL"].str.lower() == "positive").sum() / len(sent_df) * 100
                    neg_pct = (sent_df["SENTIMENT_LABEL"].str.lower() == "negative").sum() / len(sent_df) * 100
                    mixed_pct = (sent_df["SENTIMENT_LABEL"].str.lower() == "mixed").sum() / len(sent_df) * 100

                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Positive", f"{pos_pct:.0f}%")
                    c2.metric("Negative", f"{neg_pct:.0f}%")
                    c3.metric("Mixed", f"{mixed_pct:.0f}%")
                    c4.metric("Posts Analyzed", len(sent_df))

                    col1, col2 = st.columns(2)

                    with col1:
                        with st.container(border=True):
                            st.markdown("**Sentiment Distribution**")
                            sent_counts = (
                                sent_df["SENTIMENT_LABEL"]
                                .str.lower()
                                .value_counts()
                                .reset_index()
                            )
                            sent_counts.columns = ["Sentiment", "Count"]
                            sentiment_colors = {
                                "positive": "#2ecc71", "negative": NETFLIX_RED,
                                "neutral": "#95a5a6", "mixed": "#FFB347", "unknown": "#666666",
                            }
                            chart = (
                                alt.Chart(sent_counts)
                                .mark_arc(innerRadius=50)
                                .encode(
                                    theta=alt.Theta("Count:Q"),
                                    color=alt.Color("Sentiment:N",
                                                    scale=alt.Scale(
                                                        domain=list(sentiment_colors.keys()),
                                                        range=list(sentiment_colors.values()),
                                                    )),
                                    tooltip=["Sentiment", "Count"],
                                )
                                .properties(height=300)
                            )
                            st.altair_chart(chart, use_container_width=True)

                    with col2:
                        with st.container(border=True):
                            st.markdown("**Sentiment by Platform**")
                            plat_sent = (
                                sent_df.groupby(["PLATFORM", "SENTIMENT_LABEL"])
                                .size()
                                .reset_index(name="Count")
                            )
                            plat_sent["SENTIMENT_LABEL"] = plat_sent["SENTIMENT_LABEL"].str.lower()
                            chart = (
                                alt.Chart(plat_sent)
                                .mark_bar()
                                .encode(
                                    x=alt.X("Count:Q", title="Posts"),
                                    y=alt.Y("PLATFORM:N", title="Platform"),
                                    color=alt.Color("SENTIMENT_LABEL:N", title="Sentiment",
                                                    scale=alt.Scale(
                                                        domain=list(sentiment_colors.keys()),
                                                        range=list(sentiment_colors.values()),
                                                    )),
                                    tooltip=["PLATFORM", "SENTIMENT_LABEL", "Count"],
                                )
                                .properties(height=250)
                            )
                            st.altair_chart(chart, use_container_width=True)

                    # Aspect-based sentiment with AI_SENTIMENT categories
                    with st.container(border=True):
                        st.markdown("**Aspect-Based Sentiment** (AI_SENTIMENT with categories)")
                        st.caption("AI_SENTIMENT can extract sentiment for specific aspects like content quality, app experience, etc.")
                        try:
                            aspect_sql = f"""
                                SELECT
                                    ci.PLATFORM,
                                    LEFT(ct_text.TEXT_VALUE, 200) AS POST_EXCERPT,
                                    AI_SENTIMENT(
                                        ct_text.TEXT_VALUE,
                                        ['content quality', 'app experience', 'recommendation', 'value']
                                    ) AS ASPECT_SENTIMENT
                                FROM {CONTENT_ITEMS} ci
                                JOIN {CONTENT_TEXT} ct_text
                                  ON ci.OBJECT_ID = ct_text.OBJECT_ID
                                WHERE {WHERE}
                                  AND ct_text.TEXT_TYPE IN ('body', 'title')
                                  AND ct_text.LANGUAGE_CODE = 'en'
                                  AND LENGTH(ct_text.TEXT_VALUE) > 50
                                LIMIT 10
                            """
                            aspect_df = run_query(aspect_sql)
                            if len(aspect_df) > 0:
                                # Parse the aspect sentiment results
                                rows = []
                                for _, row in aspect_df.iterrows():
                                    try:
                                        asp = json.loads(row["ASPECT_SENTIMENT"]) if isinstance(row["ASPECT_SENTIMENT"], str) else row["ASPECT_SENTIMENT"]
                                        for cat in asp.get("categories", []):
                                            rows.append({
                                                "Platform": row["PLATFORM"],
                                                "Aspect": cat["name"],
                                                "Sentiment": cat["sentiment"],
                                            })
                                    except Exception:
                                        pass
                                if rows:
                                    asp_result = pd.DataFrame(rows)
                                    asp_result = asp_result[asp_result["Aspect"] != "overall"]
                                    asp_agg = asp_result.groupby(["Aspect", "Sentiment"]).size().reset_index(name="Count")
                                    chart = (
                                        alt.Chart(asp_agg)
                                        .mark_bar()
                                        .encode(
                                            x=alt.X("Count:Q"),
                                            y=alt.Y("Aspect:N", sort="-x"),
                                            color=alt.Color("Sentiment:N",
                                                            scale=alt.Scale(
                                                                domain=list(sentiment_colors.keys()),
                                                                range=list(sentiment_colors.values()),
                                                            )),
                                            tooltip=["Aspect", "Sentiment", "Count"],
                                        )
                                        .properties(height=200)
                                    )
                                    st.altair_chart(chart, use_container_width=True)
                        except Exception as e:
                            st.caption(f"Aspect-based analysis: {e}")

                    with st.container(border=True):
                        st.markdown("**Sample Posts with Sentiment**")
                        display_df = sent_df[["PLATFORM", "POST_TEXT", "SENTIMENT_LABEL", "TEXT_TYPE"]].copy()
                        display_df.columns = ["Platform", "Post Text", "Sentiment", "Type"]
                        display_df["Post Text"] = display_df["Post Text"].str[:200]
                        st.dataframe(
                            display_df.sort_values("Sentiment"),
                            hide_index=True,
                            use_container_width=True,
                        )

            except Exception as e:
                st.error(f"Sentiment analysis failed: {e}")
    else:
        st.info("Click **Run Sentiment Analysis** to score posts using AI_SENTIMENT().")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3: TREND CLASSIFICATION — uses AI_CLASSIFY()
# ═══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("AI Trend Classification")
    st.caption(
        "Auto-classify posts into Netflix-relevant categories using "
        "**AI_CLASSIFY()** — the new managed classification function"
    )

    default_categories = (
        "Romantasy/BookTok, True Crime, K-Drama, Reality TV, "
        "Comfort Rewatches, Anime, Sci-Fi/Fantasy, App Experience/UX, Other"
    )
    categories_input = st.text_input(
        "Define your trend categories (comma-separated):",
        value=default_categories,
    )

    if st.button("Classify Posts", key="btn_classify"):
        categories_list = [c.strip() for c in categories_input.split(",") if c.strip()]
        categories_array = ", ".join(f"'{c}'" for c in categories_list)
        with st.spinner("Running AI_CLASSIFY for classification..."):
            try:
                classify_sql = f"""
                    SELECT
                        ci.PLATFORM,
                        ci.ENTITY_TYPE,
                        LEFT(ct_text.TEXT_VALUE, 300) AS POST_TEXT,
                        AI_CLASSIFY(
                            ct_text.TEXT_VALUE,
                            [{categories_array}]
                        ):labels[0]::STRING AS AI_CATEGORY
                    FROM {CONTENT_ITEMS} ci
                    JOIN {CONTENT_TEXT} ct_text
                      ON ci.OBJECT_ID = ct_text.OBJECT_ID
                    WHERE {WHERE}
                      AND ct_text.TEXT_TYPE IN ('body', 'title')
                      AND ct_text.LANGUAGE_CODE = 'en'
                      AND LENGTH(ct_text.TEXT_VALUE) > 30
                    LIMIT {ai_sample_size}
                """
                class_df = run_query(classify_sql)

                if len(class_df) == 0:
                    st.warning("No posts matched your filters.")
                else:
                    col1, col2 = st.columns(2)

                    with col1:
                        with st.container(border=True):
                            st.markdown("**Posts by AI-Classified Category**")
                            cat_counts = (
                                class_df["AI_CATEGORY"]
                                .value_counts()
                                .reset_index()
                            )
                            cat_counts.columns = ["Category", "Count"]
                            chart = (
                                alt.Chart(cat_counts)
                                .mark_arc(innerRadius=50)
                                .encode(
                                    theta=alt.Theta("Count:Q"),
                                    color=alt.Color("Category:N",
                                                    scale=alt.Scale(range=NETFLIX_CAT_COLORS)),
                                    tooltip=["Category", "Count"],
                                )
                                .properties(height=350)
                            )
                            st.altair_chart(chart, use_container_width=True)

                    with col2:
                        with st.container(border=True):
                            st.markdown("**Categories by Platform**")
                            cross = (
                                class_df.groupby(["PLATFORM", "AI_CATEGORY"])
                                .size()
                                .reset_index(name="count")
                            )
                            chart = (
                                alt.Chart(cross)
                                .mark_bar()
                                .encode(
                                    x=alt.X("count:Q", title="Post Count"),
                                    y=alt.Y("PLATFORM:N", title="Platform"),
                                    color=alt.Color("AI_CATEGORY:N", title="Category",
                                                    scale=alt.Scale(range=NETFLIX_CAT_COLORS)),
                                    tooltip=["PLATFORM", "AI_CATEGORY", "count"],
                                )
                                .properties(height=350)
                            )
                            st.altair_chart(chart, use_container_width=True)

                    with st.container(border=True):
                        st.markdown("**Classified Posts**")
                        display_df = class_df[["AI_CATEGORY", "PLATFORM", "POST_TEXT"]].copy()
                        display_df.columns = ["AI Category", "Platform", "Post Text"]
                        display_df["Post Text"] = display_df["Post Text"].str[:250]
                        st.dataframe(display_df, hide_index=True, use_container_width=True)

            except Exception as e:
                st.error(f"Classification failed: {e}")
    else:
        st.info(
            "Define categories above and click **Classify Posts** "
            "to auto-tag content using AI_CLASSIFY()."
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4: AI SEARCH & CHAT — Cortex Search + AI_COMPLETE + AI_AGG
# ═══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.subheader("AI-Powered Search & Summaries")
    st.caption(
        "Search 33K+ text chunks using **Cortex Search Service** "
        "(semantic vector search) and generate AI summaries with **AI_COMPLETE** and **AI_AGG**"
    )

    search_mode = st.radio(
        "Mode",
        ["Semantic Search", "AI Summary / Briefing"],
        horizontal=True,
    )

    if search_mode == "Semantic Search":
        search_query = st.text_input(
            "Search social conversations:",
            placeholder="e.g., 'what do people think about ACOTAR ending?' or 'Netflix app buffering issues'",
        )
        num_results = st.slider("Number of results", 3, 20, 10)

        if search_query:
            with st.spinner("Searching with Cortex Search Service..."):
                try:
                    svc = (
                        root
                        .databases[DB]
                        .schemas[SCHEMA]
                        .cortex_search_services[TEXT_SEARCH_SVC]
                    )
                    results = svc.search(
                        search_query,
                        columns=["TEXT_CHUNK", "CONTENT_ITEM_ID", "TEXT_TYPE"],
                        limit=num_results,
                    )

                    st.success(f"Found {len(results.results)} results")

                    for i, r in enumerate(results.results):
                        r = dict(r)
                        with st.expander(
                            f"Result {i+1} -- {r.get('TEXT_TYPE', 'text')} "
                            f"(item: {r.get('CONTENT_ITEM_ID', 'N/A')[:20]}...)",
                            expanded=(i < 3),
                        ):
                            st.markdown(r.get("TEXT_CHUNK", ""))
                            st.caption(f"Content ID: {r.get('CONTENT_ITEM_ID', '')} | Type: {r.get('TEXT_TYPE', '')}")

                except Exception as e:
                    st.error(f"Search failed: {e}")
        else:
            st.info("Enter a query above to search across all social conversations.")

    else:  # AI Summary / Briefing
        summary_type = st.radio(
            "Summary type",
            ["Weekly Briefing", "Topic Deep-Dive"],
            horizontal=True,
            key="summary_type_radio",
        )

        if summary_type == "Weekly Briefing":
            if st.button("Generate Weekly Briefing", key="btn_briefing"):
                with st.spinner("Generating AI briefing with AI_AGG..."):
                    try:
                        # Use AI_AGG to aggregate posts per tag with instruction
                        briefing_sql = f"""
                            WITH top_tags AS (
                                SELECT t.TAG_NORMALIZED AS TAG,
                                       COUNT(*) AS TAG_COUNT
                                FROM {CONTENT_ITEMS} ci
                                JOIN {CONTENT_TAG} ct ON ci.OBJECT_ID = ct.OBJECT_ID
                                JOIN {TAGS} t ON ct.TAG_ID = t.TAG_ID
                                WHERE {WHERE}
                                  AND t.TAG_NORMALIZED IS NOT NULL
                                  AND LENGTH(t.TAG_NORMALIZED) > 2
                                GROUP BY t.TAG_NORMALIZED
                                ORDER BY TAG_COUNT DESC
                                LIMIT 10
                            ),
                            tag_posts AS (
                                SELECT t.TAG_NORMALIZED AS TAG,
                                       ct_text.TEXT_VALUE AS POST_TEXT
                                FROM {CONTENT_ITEMS} ci
                                JOIN {CONTENT_TAG} ctag ON ci.OBJECT_ID = ctag.OBJECT_ID
                                JOIN {TAGS} t ON ctag.TAG_ID = t.TAG_ID
                                JOIN {CONTENT_TEXT} ct_text ON ci.OBJECT_ID = ct_text.OBJECT_ID
                                WHERE {WHERE}
                                  AND t.TAG_NORMALIZED IN (SELECT TAG FROM top_tags)
                                  AND ct_text.TEXT_TYPE = 'body'
                                  AND ct_text.LANGUAGE_CODE = 'en'
                                  AND LENGTH(ct_text.TEXT_VALUE) > 20
                                QUALIFY ROW_NUMBER() OVER (PARTITION BY t.TAG_NORMALIZED ORDER BY ci.PUBLISHED_AT DESC) <= 5
                            )
                            SELECT TAG,
                                   COUNT(*) AS SAMPLE_CT,
                                   AI_SUMMARIZE_AGG(POST_TEXT) AS TOPIC_SUMMARY
                            FROM tag_posts
                            GROUP BY TAG
                        """
                        briefing_df = run_query(briefing_sql)

                        if len(briefing_df) == 0:
                            st.warning("No data for briefing with current filters.")
                        else:
                            # Join back tag counts
                            tag_count_sql = f"""
                                SELECT t.TAG_NORMALIZED AS TAG, COUNT(*) AS TAG_COUNT
                                FROM {CONTENT_ITEMS} ci
                                JOIN {CONTENT_TAG} ct ON ci.OBJECT_ID = ct.OBJECT_ID
                                JOIN {TAGS} t ON ct.TAG_ID = t.TAG_ID
                                WHERE {WHERE}
                                  AND t.TAG_NORMALIZED IS NOT NULL
                                  AND LENGTH(t.TAG_NORMALIZED) > 2
                                GROUP BY t.TAG_NORMALIZED
                                ORDER BY TAG_COUNT DESC
                                LIMIT 10
                            """
                            tag_counts = run_query(tag_count_sql)
                            briefing_df = briefing_df.merge(tag_counts, on="TAG", how="left")

                            topics_context = "\n".join(
                                f"- #{row['TAG']} ({row.get('TAG_COUNT', 'N/A')} posts): {row['TOPIC_SUMMARY']}"
                                for _, row in briefing_df.iterrows()
                            )

                            safe_context = topics_context.replace("'", "''")
                            exec_sql = f"""
                                SELECT AI_COMPLETE(
                                    'llama3.1-70b',
                                    'You are a cultural trends analyst for Netflix. Write a concise weekly briefing '
                                    || 'for the content strategy team based on the following trending topics from social media. '
                                    || 'The data comes from Reddit, TikTok, and YouTube conversations. '
                                    || 'Highlight what is gaining momentum, shifting sentiment, and any emerging cultural moments. '
                                    || 'Be specific and actionable. '
                                    || 'IMPORTANT: Format your response using proper markdown with ## headers, bullet points with - , '
                                    || 'and **bold** for emphasis. Use real newline characters between sections.'
                                    || CHR(10) || CHR(10)
                                    || 'Trending topics data:'
                                    || CHR(10)
                                    || '{safe_context}'
                                ) AS EXECUTIVE_BRIEFING
                            """
                            exec_df = run_query(exec_sql)

                            with st.container(border=True):
                                st.markdown("**Executive Cultural Briefing**")
                                st.markdown(clean_ai_text(exec_df.iloc[0]["EXECUTIVE_BRIEFING"]))

                            with st.expander("View individual tag summaries"):
                                for _, row in briefing_df.iterrows():
                                    st.markdown(
                                        f"**#{row['TAG']}** -- "
                                        f"{row.get('TAG_COUNT', 'N/A')} posts"
                                    )
                                    st.markdown(f"> {row['TOPIC_SUMMARY']}")
                                    st.divider()

                    except Exception as e:
                        st.error(f"Briefing generation failed: {e}")
            else:
                st.info("Click **Generate Weekly Briefing** for an AI-written cultural trends report using AI_AGG.")

        else:  # Topic Deep-Dive
            dive_topic = st.text_input(
                "Enter a topic/tag to deep-dive:",
                placeholder="e.g., romantasy, bridgerton, loveisblind",
            )
            if dive_topic and st.button("Generate Deep-Dive", key="btn_deepdive"):
                safe_dive = dive_topic.replace("'", "''").lower()
                with st.spinner(f"Deep-diving into '{dive_topic}'..."):
                    try:
                        # Use Cortex Search to find relevant content
                        svc = (
                            root
                            .databases[DB]
                            .schemas[SCHEMA]
                            .cortex_search_services[TEXT_SEARCH_SVC]
                        )
                        search_results = svc.search(
                            dive_topic,
                            columns=["TEXT_CHUNK", "CONTENT_ITEM_ID", "TEXT_TYPE"],
                            limit=15,
                        )

                        chunks = [dict(r).get("TEXT_CHUNK", "") for r in search_results.results]
                        combined_text = " | ".join(c[:300] for c in chunks if c)

                        if not combined_text:
                            st.warning("No content found for that topic.")
                        else:
                            safe_text = combined_text.replace("'", "''")
                            dive_sql = f"""
                                SELECT
                                    AI_COMPLETE(
                                        'llama3.1-70b',
                                        'You are a cultural analyst for Netflix. Based on the following social media posts '
                                        || 'about "{safe_dive}", write a detailed analysis covering: '
                                        || '1) What is driving this conversation '
                                        || '2) Key sub-themes and book/show titles mentioned '
                                        || '3) How Netflix could leverage this trend '
                                        || '4) Risk factors or backlash signals. '
                                        || 'IMPORTANT: Format using proper markdown with ## headers, - bullet points, and **bold** for emphasis. '
                                        || 'Posts: {safe_text}'
                                    ) AS DEEP_ANALYSIS
                            """
                            dive_df = run_query(dive_sql)

                            with st.container(border=True):
                                st.markdown(f"**Deep Analysis: {dive_topic}**")
                                st.markdown(clean_ai_text(dive_df.iloc[0]["DEEP_ANALYSIS"]))

                            with st.expander("Source chunks from Cortex Search"):
                                for i, chunk in enumerate(chunks):
                                    st.markdown(f"**Chunk {i+1}:** {chunk[:500]}")
                                    st.divider()

                    except Exception as e:
                        st.error(f"Deep-dive failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5: CONVERSATION EXPLORER
# ═══════════════════════════════════════════════════════════════════════════════
with tab5:
    st.subheader("Conversation Explorer")
    st.caption("Browse posts, threads, and author profiles from the SocialGist feed")

    explore_mode = st.radio(
        "Explore",
        ["Recent Posts", "Top TikTok Posts", "Author Search"],
        horizontal=True,
    )

    if explore_mode == "Recent Posts":
        try:
            recent_sql = f"""
                SELECT
                    ci.OBJECT_ID,
                    ci.PLATFORM,
                    ci.ENTITY_TYPE,
                    ci.PUBLISHED_AT,
                    ci.URL,
                    ct_text.TEXT_VALUE AS POST_TEXT,
                    a.DISPLAY_NAME AS AUTHOR_NAME
                FROM {CONTENT_ITEMS} ci
                JOIN {CONTENT_TEXT} ct_text ON ci.OBJECT_ID = ct_text.OBJECT_ID
                LEFT JOIN {AUTHORS} a ON ci.AUTHOR_ID = a.AUTHOR_ID
                WHERE {WHERE}
                  AND ct_text.TEXT_TYPE IN ('body', 'title')
                  AND ct_text.LANGUAGE_CODE = 'en'
                  AND LENGTH(ct_text.TEXT_VALUE) > 20
                ORDER BY ci.PUBLISHED_AT DESC
                LIMIT 25
            """
            recent_df = run_query(recent_sql)

            for _, row in recent_df.iterrows():
                with st.container(border=True):
                    cols = st.columns([1, 6])
                    with cols[0]:
                        st.markdown(f"**{row['PLATFORM']}**")
                        st.caption(row['ENTITY_TYPE'])
                    with cols[1]:
                        author = row.get('AUTHOR_NAME', 'Unknown')
                        st.markdown(f"**{author}** -- {str(row['PUBLISHED_AT'])[:16]}")
                        st.markdown(str(row['POST_TEXT'])[:500])
                        if row.get('URL'):
                            st.caption(f"[View original]({row['URL']})")
        except Exception as e:
            st.error(f"Could not load recent posts: {e}")

    elif explore_mode == "Top TikTok Posts":
        try:
            tiktok_sql = f"""
                SELECT
                    tp.OBJECT_ID,
                    tp.VIEWS_CT,
                    tp.LIKES_CT,
                    tp.COMMENTS_CT,
                    tp.SHARES_CT,
                    (COALESCE(tp.LIKES_CT, 0) + COALESCE(tp.COMMENTS_CT, 0) + COALESCE(tp.SHARES_CT, 0)) AS ENGAGEMENT_SCORE,
                    tp.VIDEO_DURATION,
                    tp.IS_AD,
                    ci.URL,
                    ci.PUBLISHED_AT,
                    ct_text.TEXT_VALUE AS POST_TEXT,
                    a.DISPLAY_NAME AS CREATOR_NAME,
                    tc.FOLLOWERS AS CREATOR_FOLLOWERS
                FROM {TIKTOK_POSTS} tp
                JOIN {CONTENT_ITEMS} ci ON tp.OBJECT_ID = ci.OBJECT_ID
                LEFT JOIN {CONTENT_TEXT} ct_text
                  ON tp.OBJECT_ID = ct_text.OBJECT_ID AND ct_text.TEXT_TYPE = 'transcript'
                LEFT JOIN {AUTHORS} a ON ci.AUTHOR_ID = a.AUTHOR_ID
                LEFT JOIN {TIKTOK_CREATORS} tc ON a.DISPLAY_NAME = tc.DISPLAY_NAME
                WHERE ci.PUBLISHED_AT BETWEEN '{start_date}' AND '{end_date} 23:59:59'
                ORDER BY ENGAGEMENT_SCORE DESC NULLS LAST
                LIMIT 20
            """
            tiktok_df = run_query(tiktok_sql)

            if len(tiktok_df) == 0:
                st.info("No TikTok posts found in the selected date range.")
            else:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Top Views", f"{int(tiktok_df['VIEWS_CT'].max()):,}")
                c2.metric("Top Likes", f"{int(tiktok_df['LIKES_CT'].max()):,}")
                c3.metric("Avg Engagement", f"{tiktok_df['ENGAGEMENT_SCORE'].mean():,.0f}")
                c4.metric("Posts", len(tiktok_df))

                for _, row in tiktok_df.iterrows():
                    with st.container(border=True):
                        cols = st.columns([2, 4])
                        with cols[0]:
                            creator = row.get("CREATOR_NAME", "Unknown")
                            followers = row.get("CREATOR_FOLLOWERS")
                            st.markdown(f"**{creator}**")
                            if followers and pd.notna(followers):
                                st.caption(f"{int(followers):,} followers")
                            st.markdown(
                                f"Views: **{int(row['VIEWS_CT']):,}** | "
                                f"Likes: **{int(row['LIKES_CT']):,}**"
                            )
                            st.markdown(
                                f"Comments: {int(row['COMMENTS_CT']):,} | "
                                f"Shares: {int(row['SHARES_CT']):,}"
                            )
                            if row.get("VIDEO_DURATION") and pd.notna(row["VIDEO_DURATION"]):
                                st.caption(f"Duration: {int(row['VIDEO_DURATION'])}s")
                        with cols[1]:
                            text = row.get("POST_TEXT", "")
                            if text and pd.notna(text) and str(text).strip():
                                st.markdown(f"*Transcript:* {str(text)[:400]}")
                            else:
                                st.caption("No transcript available")
                            if row.get("URL"):
                                st.caption(f"[View on TikTok]({row['URL']})")
        except Exception as e:
            st.error(f"Could not load TikTok posts: {e}")

    else:  # Author Search
        st.markdown("Search author profiles using **Cortex Search Service**")
        author_query = st.text_input(
            "Search author descriptions:",
            placeholder="e.g., 'book reviewer', 'fantasy reader', 'Netflix fan'",
        )
        if author_query:
            with st.spinner("Searching authors..."):
                try:
                    svc = (
                        root
                        .databases[DB]
                        .schemas[SCHEMA]
                        .cortex_search_services[AUTHOR_SEARCH_SVC]
                    )
                    results = svc.search(
                        author_query,
                        columns=["DESCRIPTION", "AUTHOR_ID", "PLATFORM", "DISPLAY_NAME", "PROFILE_URL"],
                        limit=10,
                    )

                    st.success(f"Found {len(results.results)} matching authors")

                    for i, r in enumerate(results.results):
                        r = dict(r)
                        with st.container(border=True):
                            cols = st.columns([1, 5])
                            with cols[0]:
                                st.markdown(f"**{r.get('PLATFORM', '')}**")
                            with cols[1]:
                                name = r.get("DISPLAY_NAME", "Unknown")
                                st.markdown(f"**{name}**")
                                st.markdown(r.get("DESCRIPTION", "No description"))
                                url = r.get("PROFILE_URL", "")
                                if url:
                                    st.caption(f"[View profile]({url})")

                except Exception as e:
                    st.error(f"Author search failed: {e}")
        else:
            st.info("Enter a query to search across 13K+ author profiles.")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 6: AUDIENCE SEGMENTATION — uses AI_CLASSIFY + AI_SENTIMENT
# ═══════════════════════════════════════════════════════════════════════════════
with tab6:
    st.subheader("Audience Segmentation")
    st.caption(
        "AI-powered audience personas using **AI_CLASSIFY()** — "
        "equivalent to Pulsar TRAC's audience segmentation & community mapping"
    )

    if st.button("Run Audience Segmentation", key="btn_audience"):
        with st.spinner("Analyzing author personas with AI_CLASSIFY..."):
            try:
                # Build author profiles and classify with AI_CLASSIFY
                author_profile_sql = f"""
                    WITH author_activity AS (
                        SELECT
                            ci.AUTHOR_ID,
                            a.DISPLAY_NAME,
                            ci.PLATFORM,
                            COUNT(*) AS POST_COUNT,
                            COUNT(DISTINCT DATE_TRUNC('day', ci.PUBLISHED_AT)) AS ACTIVE_DAYS,
                            COUNT(DISTINCT ci.CHANNEL_ID) AS CHANNELS_ACTIVE,
                            LISTAGG(DISTINCT ci.ENTITY_TYPE, ', ') WITHIN GROUP (ORDER BY ci.ENTITY_TYPE) AS CONTENT_TYPES
                        FROM {CONTENT_ITEMS} ci
                        LEFT JOIN {AUTHORS} a ON ci.AUTHOR_ID = a.AUTHOR_ID
                        WHERE {WHERE}
                          AND ci.AUTHOR_ID IS NOT NULL
                        GROUP BY ci.AUTHOR_ID, a.DISPLAY_NAME, ci.PLATFORM
                    ),
                    author_tags AS (
                        SELECT
                            ci.AUTHOR_ID,
                            LISTAGG(DISTINCT t.TAG_NORMALIZED, ', ') WITHIN GROUP (ORDER BY t.TAG_NORMALIZED) AS TOP_TAGS
                        FROM {CONTENT_ITEMS} ci
                        JOIN {CONTENT_TAG} ct ON ci.OBJECT_ID = ct.OBJECT_ID
                        JOIN {TAGS} t ON ct.TAG_ID = t.TAG_ID
                        WHERE {WHERE}
                          AND ci.AUTHOR_ID IS NOT NULL
                          AND t.TAG_NORMALIZED IS NOT NULL
                        GROUP BY ci.AUTHOR_ID
                    ),
                    author_text AS (
                        SELECT
                            ci.AUTHOR_ID,
                            LISTAGG(LEFT(ct_text.TEXT_VALUE, 100), ' | ') WITHIN GROUP (ORDER BY ci.PUBLISHED_AT DESC) AS SAMPLE_TEXT
                        FROM {CONTENT_ITEMS} ci
                        JOIN {CONTENT_TEXT} ct_text ON ci.OBJECT_ID = ct_text.OBJECT_ID
                        WHERE {WHERE}
                          AND ci.AUTHOR_ID IS NOT NULL
                          AND ct_text.TEXT_TYPE IN ('body', 'title')
                          AND ct_text.LANGUAGE_CODE = 'en'
                          AND LENGTH(ct_text.TEXT_VALUE) > 20
                        GROUP BY ci.AUTHOR_ID
                        QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) <= {ai_sample_size}
                    )
                    SELECT
                        aa.AUTHOR_ID,
                        aa.DISPLAY_NAME,
                        aa.PLATFORM,
                        aa.POST_COUNT,
                        aa.ACTIVE_DAYS,
                        aa.CHANNELS_ACTIVE,
                        aa.CONTENT_TYPES,
                        at.TOP_TAGS,
                        AI_CLASSIFY(
                            'Platform=' || aa.PLATFORM
                            || ', Posts=' || aa.POST_COUNT::STRING
                            || ', ActiveDays=' || aa.ACTIVE_DAYS::STRING
                            || ', ContentTypes=' || aa.CONTENT_TYPES
                            || ', Tags=' || COALESCE(LEFT(at.TOP_TAGS, 200), 'none')
                            || ', SampleContent=' || COALESCE(LEFT(atxt.SAMPLE_TEXT, 300), 'none'),
                            ['BookTok Creator', 'Casual Viewer', 'Power Commenter',
                             'Romantasy Superfan', 'Reality TV Enthusiast',
                             'Anime/Sci-Fi Fan', 'Content Critic', 'Cultural Curator'],
                            {{'task_description': 'Classify this social media user into an audience persona based on their activity profile'}}
                        ):labels[0]::STRING AS PERSONA,
                        AI_SENTIMENT(COALESCE(LEFT(atxt.SAMPLE_TEXT, 500), 'neutral')):categories[0]:sentiment::STRING AS SENTIMENT_LABEL
                    FROM author_activity aa
                    LEFT JOIN author_tags at ON aa.AUTHOR_ID = at.AUTHOR_ID
                    LEFT JOIN author_text atxt ON aa.AUTHOR_ID = atxt.AUTHOR_ID
                    WHERE aa.POST_COUNT >= 2
                      AND atxt.AUTHOR_ID IS NOT NULL
                    ORDER BY aa.POST_COUNT DESC
                    LIMIT {ai_sample_size}
                """
                seg_df = run_query(author_profile_sql)

                if len(seg_df) == 0:
                    st.warning("No authors found for segmentation with current filters.")
                else:
                    # KPIs
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Authors Analyzed", len(seg_df))
                    c2.metric("Persona Types", seg_df["PERSONA"].nunique())
                    c3.metric("Avg Posts/Author", f"{seg_df['POST_COUNT'].mean():.1f}")
                    c4.metric("Top Persona", seg_df["PERSONA"].value_counts().index[0])

                    col1, col2 = st.columns(2)

                    with col1:
                        with st.container(border=True):
                            st.markdown("**Audience Persona Distribution**")
                            persona_counts = seg_df["PERSONA"].value_counts().reset_index()
                            persona_counts.columns = ["Persona", "Count"]
                            chart = (
                                alt.Chart(persona_counts)
                                .mark_arc(innerRadius=50)
                                .encode(
                                    theta=alt.Theta("Count:Q"),
                                    color=alt.Color("Persona:N",
                                                    scale=alt.Scale(range=NETFLIX_CAT_COLORS)),
                                    tooltip=["Persona", "Count"],
                                )
                                .properties(height=350)
                            )
                            st.altair_chart(chart, use_container_width=True)

                    with col2:
                        with st.container(border=True):
                            st.markdown("**Sentiment by Persona**")
                            persona_sent = (
                                seg_df.groupby(["PERSONA", "SENTIMENT_LABEL"])
                                .size()
                                .reset_index(name="Count")
                            )
                            sentiment_colors = {
                                "positive": "#2ecc71", "negative": NETFLIX_RED,
                                "neutral": "#95a5a6", "mixed": "#FFB347", "unknown": "#666666",
                            }
                            chart = (
                                alt.Chart(persona_sent)
                                .mark_bar()
                                .encode(
                                    x=alt.X("Count:Q", title="Authors"),
                                    y=alt.Y("PERSONA:N", sort="-x", title=""),
                                    color=alt.Color("SENTIMENT_LABEL:N", title="Sentiment",
                                                    scale=alt.Scale(
                                                        domain=list(sentiment_colors.keys()),
                                                        range=list(sentiment_colors.values()),
                                                    )),
                                    tooltip=["PERSONA", "SENTIMENT_LABEL", "Count"],
                                )
                                .properties(height=350)
                            )
                            st.altair_chart(chart, use_container_width=True)

                    with st.container(border=True):
                        st.markdown("**Persona by Platform**")
                        plat_persona = (
                            seg_df.groupby(["PLATFORM", "PERSONA"])
                            .size()
                            .reset_index(name="Count")
                        )
                        chart = (
                            alt.Chart(plat_persona)
                            .mark_bar()
                            .encode(
                                x=alt.X("Count:Q", title="Authors"),
                                y=alt.Y("PLATFORM:N", title="Platform"),
                                color=alt.Color("PERSONA:N", title="Persona",
                                                scale=alt.Scale(range=NETFLIX_CAT_COLORS)),
                                tooltip=["PLATFORM", "PERSONA", "Count"],
                            )
                            .properties(height=250)
                        )
                        st.altair_chart(chart, use_container_width=True)

                    with st.container(border=True):
                        st.markdown("**Engagement by Persona (Posts vs Active Days)**")
                        chart = (
                            alt.Chart(seg_df)
                            .mark_circle(opacity=0.7)
                            .encode(
                                x=alt.X("POST_COUNT:Q", title="Total Posts"),
                                y=alt.Y("ACTIVE_DAYS:Q", title="Active Days"),
                                color=alt.Color("PERSONA:N", title="Persona",
                                                scale=alt.Scale(range=NETFLIX_CAT_COLORS)),
                                size=alt.Size("CHANNELS_ACTIVE:Q", title="Channels Active"),
                                tooltip=["DISPLAY_NAME", "PLATFORM", "PERSONA", "POST_COUNT", "ACTIVE_DAYS"],
                            )
                            .properties(height=400)
                        )
                        st.altair_chart(chart, use_container_width=True)

                    with st.expander("View all segmented authors"):
                        display_df = seg_df[["DISPLAY_NAME", "PLATFORM", "PERSONA", "POST_COUNT", "ACTIVE_DAYS", "SENTIMENT_LABEL", "TOP_TAGS"]].copy()
                        display_df.columns = ["Author", "Platform", "Persona", "Posts", "Active Days", "Sentiment", "Top Tags"]
                        display_df["Top Tags"] = display_df["Top Tags"].fillna("").str[:100]
                        st.dataframe(display_df, hide_index=True, use_container_width=True)

            except Exception as e:
                st.error(f"Audience segmentation failed: {e}")
    else:
        st.info(
            "Click **Run Audience Segmentation** to classify authors into personas "
            "using AI_CLASSIFY(). This analyzes posting patterns, topics, and sentiment."
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 7: NARRATIVE DETECTION — uses AI_CLASSIFY + AI_FILTER + AI_AGG
# ═══════════════════════════════════════════════════════════════════════════════
with tab7:
    st.subheader("Narrative Detection")
    st.caption(
        "Detect and track cultural narratives using **AI_CLASSIFY()** and **AI_AGG()** — "
        "equivalent to Pulsar's Narratives AI product"
    )

    if st.button("Detect Narratives", key="btn_narratives"):
        with st.spinner("Extracting narratives with AI_CLASSIFY..."):
            try:
                # Use AI_CLASSIFY with narrative categories
                narrative_sql = f"""
                    SELECT
                        ci.PLATFORM,
                        ci.PUBLISHED_AT,
                        LEFT(ct_text.TEXT_VALUE, 300) AS POST_EXCERPT,
                        ci.CHANNEL_ID,
                        AI_CLASSIFY(
                            ct_text.TEXT_VALUE,
                            ['Adaptation Hype', 'Cancellation Backlash',
                             'BookTok Recommendations', 'Comfort Content',
                             'Platform Criticism', 'Fandom Debate',
                             'Cultural Moment', 'Creator Spotlight',
                             'Genre Discovery', 'Quality Discussion'],
                            {{'task_description': 'Classify this social media post into the dominant cultural narrative it represents'}}
                        ):labels[0]::STRING AS NARRATIVE
                    FROM {CONTENT_ITEMS} ci
                    JOIN {CONTENT_TEXT} ct_text ON ci.OBJECT_ID = ct_text.OBJECT_ID
                    WHERE {WHERE}
                      AND ct_text.TEXT_TYPE IN ('body', 'title')
                      AND ct_text.LANGUAGE_CODE = 'en'
                      AND LENGTH(ct_text.TEXT_VALUE) > 50
                    ORDER BY RANDOM()
                    LIMIT {ai_sample_size}
                """
                narr_df = run_query(narrative_sql)

                if len(narr_df) == 0:
                    st.warning("Narrative extraction returned no results.")
                else:
                    # KPIs
                    narr_counts = narr_df["NARRATIVE"].value_counts()
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Posts Analyzed", len(narr_df))
                    c2.metric("Narratives Found", narr_counts.nunique())
                    c3.metric("Dominant Narrative", narr_counts.index[0] if len(narr_counts) > 0 else "N/A")

                    col1, col2 = st.columns(2)

                    with col1:
                        with st.container(border=True):
                            st.markdown("**Narrative Distribution**")
                            nc = narr_counts.reset_index()
                            nc.columns = ["Narrative", "Count"]
                            nc = nc.head(12)
                            chart = (
                                alt.Chart(nc)
                                .mark_bar()
                                .encode(
                                    x=alt.X("Count:Q", title="Posts"),
                                    y=alt.Y("Narrative:N", sort="-x", title=""),
                                    color=alt.Color("Count:Q",
                                                    scale=alt.Scale(scheme="reds"),
                                                    legend=None),
                                    tooltip=["Narrative", "Count"],
                                )
                                .properties(height=400)
                            )
                            st.altair_chart(chart, use_container_width=True)

                    with col2:
                        with st.container(border=True):
                            st.markdown("**Narratives by Platform**")
                            narr_plat = (
                                narr_df.groupby(["PLATFORM", "NARRATIVE"])
                                .size()
                                .reset_index(name="Count")
                            )
                            top_narrs = narr_counts.head(8).index.tolist()
                            narr_plat = narr_plat[narr_plat["NARRATIVE"].isin(top_narrs)]
                            chart = (
                                alt.Chart(narr_plat)
                                .mark_bar()
                                .encode(
                                    x=alt.X("Count:Q", title="Posts"),
                                    y=alt.Y("PLATFORM:N", title=""),
                                    color=alt.Color("NARRATIVE:N", title="Narrative",
                                                    scale=alt.Scale(range=NETFLIX_CAT_COLORS)),
                                    tooltip=["PLATFORM", "NARRATIVE", "Count"],
                                )
                                .properties(height=400)
                            )
                            st.altair_chart(chart, use_container_width=True)

                    # Narrative timeline
                    with st.container(border=True):
                        st.markdown("**Narrative Momentum Over Time**")
                        narr_df["PUBLISHED_AT"] = pd.to_datetime(narr_df["PUBLISHED_AT"])
                        narr_df["DATE"] = narr_df["PUBLISHED_AT"].dt.date
                        top_5_narrs = narr_counts.head(5).index.tolist()
                        timeline_df = (
                            narr_df[narr_df["NARRATIVE"].isin(top_5_narrs)]
                            .groupby(["DATE", "NARRATIVE"])
                            .size()
                            .reset_index(name="Count")
                        )
                        if len(timeline_df) > 0:
                            chart = (
                                alt.Chart(timeline_df)
                                .mark_line(point=True, strokeWidth=2)
                                .encode(
                                    x=alt.X("DATE:T", title="Date"),
                                    y=alt.Y("Count:Q", title="Posts"),
                                    color=alt.Color("NARRATIVE:N", title="Narrative",
                                                    scale=alt.Scale(range=NETFLIX_CAT_COLORS)),
                                    tooltip=["DATE:T", "NARRATIVE:N", "Count:Q"],
                                )
                                .properties(height=300)
                            )
                            st.altair_chart(chart, use_container_width=True)
                        else:
                            st.info("Not enough temporal data for timeline.")

                    # AI synthesis using AI_AGG
                    with st.container(border=True):
                        st.markdown("**AI Narrative Synthesis** (via AI_AGG)")
                        try:
                            synth_sql = f"""
                                WITH narrative_posts AS (
                                    SELECT
                                        LEFT(ct_text.TEXT_VALUE, 200) || ' [Narrative: ' ||
                                        AI_CLASSIFY(
                                            ct_text.TEXT_VALUE,
                                            ['Adaptation Hype', 'Cancellation Backlash',
                                             'BookTok Recommendations', 'Comfort Content',
                                             'Platform Criticism', 'Fandom Debate',
                                             'Cultural Moment', 'Creator Spotlight',
                                             'Genre Discovery', 'Quality Discussion']
                                        ):labels[0]::STRING || ']' AS POST_WITH_NARRATIVE
                                    FROM {CONTENT_ITEMS} ci
                                    JOIN {CONTENT_TEXT} ct_text ON ci.OBJECT_ID = ct_text.OBJECT_ID
                                    WHERE {WHERE}
                                      AND ct_text.TEXT_TYPE IN ('body', 'title')
                                      AND ct_text.LANGUAGE_CODE = 'en'
                                      AND LENGTH(ct_text.TEXT_VALUE) > 50
                                    ORDER BY RANDOM()
                                    LIMIT 30
                                )
                                SELECT AI_AGG(
                                    POST_WITH_NARRATIVE,
                                    'You are a cultural intelligence analyst for Netflix. '
                                    || 'These are social media posts tagged with cultural narratives. '
                                    || 'Write a brief executive summary covering: '
                                    || '1) Which narratives are dominant and why, '
                                    || '2) What this tells Netflix about audience sentiment, '
                                    || '3) Actionable recommendations for the content team. '
                                    || 'Format using markdown with ## headers, - bullet points, and **bold** for emphasis.'
                                ) AS SYNTHESIS
                                FROM narrative_posts
                            """
                            synth_df = run_query(synth_sql)
                            st.markdown(clean_ai_text(synth_df.iloc[0]["SYNTHESIS"]))
                        except Exception as e:
                            st.caption(f"AI synthesis: {e}")

                    with st.expander("View posts by narrative"):
                        selected_narr = st.selectbox(
                            "Select narrative",
                            options=narr_counts.head(10).index.tolist(),
                            key="narr_select",
                        )
                        filtered = narr_df[narr_df["NARRATIVE"] == selected_narr]
                        for _, row in filtered.head(10).iterrows():
                            st.markdown(f"**[{row['PLATFORM']}]** {row['POST_EXCERPT']}")
                            st.divider()

            except Exception as e:
                st.error(f"Narrative detection failed: {e}")
    else:
        st.info(
            "Click **Detect Narratives** to identify the dominant cultural narratives "
            "in the social conversation using AI_CLASSIFY()."
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 8: COMMUNITY MAPPING
# ═══════════════════════════════════════════════════════════════════════════════
with tab8:
    st.subheader("Community Mapping")
    st.caption(
        "Map relationships between communities and identify cross-platform engagement -- "
        "equivalent to Pulsar TRAC's community mapping"
    )

    try:
        # Community size comparison
        with st.container(border=True):
            st.markdown("**Community Size by Platform & Channel**")
            community_sql = f"""
                SELECT
                    ci.PLATFORM,
                    ci.CHANNEL_ID,
                    COUNT(DISTINCT ci.OBJECT_ID) AS POST_COUNT,
                    COUNT(DISTINCT ci.AUTHOR_ID) AS AUTHOR_COUNT,
                    MIN(ci.PUBLISHED_AT) AS FIRST_POST,
                    MAX(ci.PUBLISHED_AT) AS LAST_POST
                FROM {CONTENT_ITEMS} ci
                WHERE {WHERE}
                  AND ci.CHANNEL_ID IS NOT NULL
                GROUP BY ci.PLATFORM, ci.CHANNEL_ID
                HAVING COUNT(DISTINCT ci.OBJECT_ID) >= 5
                ORDER BY POST_COUNT DESC
                LIMIT 15
            """
            comm_df = run_query(community_sql)

            if len(comm_df) > 0:
                comm_df["COMMUNITY"] = comm_df.apply(
                    lambda r: SUBREDDIT_MAP.get(r["CHANNEL_ID"], r["CHANNEL_ID"][:30]) if r["PLATFORM"] == "reddit"
                    else f"{r['PLATFORM']}/{r['CHANNEL_ID'][:20]}",
                    axis=1,
                )

                c1, c2, c3 = st.columns(3)
                c1.metric("Active Communities", len(comm_df))
                c2.metric("Total Authors", f"{comm_df['AUTHOR_COUNT'].sum():,}")
                c3.metric("Total Posts", f"{comm_df['POST_COUNT'].sum():,}")

                chart = (
                    alt.Chart(comm_df)
                    .mark_bar()
                    .encode(
                        x=alt.X("POST_COUNT:Q", title="Posts"),
                        y=alt.Y("COMMUNITY:N", sort="-x", title="Community"),
                        color=alt.Color("PLATFORM:N", title="Platform",
                                        scale=alt.Scale(range=[NETFLIX_RED, "#FF6B6B", "#4DCCBD"])),
                        tooltip=["COMMUNITY", "PLATFORM", "POST_COUNT", "AUTHOR_COUNT"],
                    )
                    .properties(height=400)
                )
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("No communities with sufficient activity found.")

        # Cross-platform authors
        with st.container(border=True):
            st.markdown("**Cross-Platform Authors** (users active on multiple platforms)")
            cross_plat_sql = f"""
                SELECT
                    ci.AUTHOR_ID,
                    a.DISPLAY_NAME,
                    LISTAGG(DISTINCT ci.PLATFORM, ', ') WITHIN GROUP (ORDER BY ci.PLATFORM) AS PLATFORMS,
                    COUNT(DISTINCT ci.PLATFORM) AS PLATFORM_COUNT,
                    COUNT(DISTINCT ci.OBJECT_ID) AS TOTAL_POSTS,
                    COUNT(DISTINCT ci.CHANNEL_ID) AS CHANNELS
                FROM {CONTENT_ITEMS} ci
                LEFT JOIN {AUTHORS} a ON ci.AUTHOR_ID = a.AUTHOR_ID
                WHERE {WHERE}
                  AND ci.AUTHOR_ID IS NOT NULL
                GROUP BY ci.AUTHOR_ID, a.DISPLAY_NAME
                HAVING COUNT(DISTINCT ci.PLATFORM) > 1
                ORDER BY TOTAL_POSTS DESC
                LIMIT 20
            """
            cross_df = run_query(cross_plat_sql)

            if len(cross_df) > 0:
                st.metric("Cross-Platform Authors", len(cross_df))
                display_df = cross_df[["DISPLAY_NAME", "PLATFORMS", "PLATFORM_COUNT", "TOTAL_POSTS", "CHANNELS"]].copy()
                display_df.columns = ["Author", "Platforms", "# Platforms", "Total Posts", "Channels"]
                st.dataframe(display_df, hide_index=True, use_container_width=True)
            else:
                st.info("No cross-platform authors found in the selected period.")

        # Community overlap via shared tags
        with st.container(border=True):
            st.markdown("**Topic Overlap Between Communities** (shared tags)")
            overlap_sql = f"""
                WITH community_tags AS (
                    SELECT
                        ci.PLATFORM,
                        ci.CHANNEL_ID,
                        t.TAG_NORMALIZED AS TAG,
                        COUNT(*) AS TAG_CT
                    FROM {CONTENT_ITEMS} ci
                    JOIN {CONTENT_TAG} ct ON ci.OBJECT_ID = ct.OBJECT_ID
                    JOIN {TAGS} t ON ct.TAG_ID = t.TAG_ID
                    WHERE {WHERE}
                      AND ci.CHANNEL_ID IS NOT NULL
                      AND t.TAG_NORMALIZED IS NOT NULL
                      AND LENGTH(t.TAG_NORMALIZED) > 2
                    GROUP BY ci.PLATFORM, ci.CHANNEL_ID, t.TAG_NORMALIZED
                    HAVING COUNT(*) >= 3
                )
                SELECT TAG, COUNT(DISTINCT CHANNEL_ID) AS COMMUNITY_COUNT,
                       LISTAGG(DISTINCT PLATFORM, ', ') WITHIN GROUP (ORDER BY PLATFORM) AS PLATFORMS,
                       SUM(TAG_CT) AS TOTAL_USAGE
                FROM community_tags
                GROUP BY TAG
                HAVING COUNT(DISTINCT CHANNEL_ID) > 1
                ORDER BY COMMUNITY_COUNT DESC, TOTAL_USAGE DESC
                LIMIT 20
            """
            overlap_df = run_query(overlap_sql)

            if len(overlap_df) > 0:
                chart = (
                    alt.Chart(overlap_df)
                    .mark_circle(opacity=0.7)
                    .encode(
                        x=alt.X("COMMUNITY_COUNT:Q", title="Communities Using This Tag"),
                        y=alt.Y("TOTAL_USAGE:Q", title="Total Usage Count"),
                        size=alt.Size("TOTAL_USAGE:Q", legend=None),
                        color=alt.Color("PLATFORMS:N", title="Platforms",
                                        scale=alt.Scale(range=[NETFLIX_RED, "#FF6B6B", "#4DCCBD"])),
                        tooltip=["TAG", "COMMUNITY_COUNT", "TOTAL_USAGE", "PLATFORMS"],
                    )
                    .properties(height=350)
                )
                st.altair_chart(chart, use_container_width=True)

                st.dataframe(
                    overlap_df.rename(columns={
                        "TAG": "Tag",
                        "COMMUNITY_COUNT": "Communities",
                        "PLATFORMS": "Platforms",
                        "TOTAL_USAGE": "Total Usage",
                    }),
                    hide_index=True,
                    use_container_width=True,
                )
            else:
                st.info("No significant tag overlap found between communities.")

        # Community activity heatmap
        with st.container(border=True):
            st.markdown("**Community Activity Over Time**")
            heatmap_sql = f"""
                SELECT
                    DATE_TRUNC('day', ci.PUBLISHED_AT) AS POST_DAY,
                    ci.PLATFORM,
                    COUNT(*) AS POSTS
                FROM {CONTENT_ITEMS} ci
                WHERE {WHERE}
                GROUP BY POST_DAY, ci.PLATFORM
                ORDER BY POST_DAY
            """
            heatmap_df = run_query(heatmap_sql)

            if len(heatmap_df) > 0:
                chart = (
                    alt.Chart(heatmap_df)
                    .mark_rect()
                    .encode(
                        x=alt.X("POST_DAY:T", title="Date"),
                        y=alt.Y("PLATFORM:N", title="Platform"),
                        color=alt.Color("POSTS:Q", scale=alt.Scale(scheme="reds"), title="Posts"),
                        tooltip=["POST_DAY:T", "PLATFORM:N", "POSTS:Q"],
                    )
                    .properties(height=150)
                )
                st.altair_chart(chart, use_container_width=True)

    except Exception as e:
        st.error(f"Community mapping failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 9: TREND RADAR
# ═══════════════════════════════════════════════════════════════════════════════
with tab9:
    st.subheader("Trend Radar")
    st.caption(
        "Track tag velocity and emerging topics -- "
        "equivalent to Pulsar TRENDS' trend discovery"
    )

    try:
        # Calculate tag velocity: current week vs previous week
        radar_sql = f"""
            WITH current_week AS (
                SELECT
                    t.TAG_NORMALIZED AS TAG,
                    COUNT(*) AS CURRENT_CT
                FROM {CONTENT_ITEMS} ci
                JOIN {CONTENT_TAG} ct ON ci.OBJECT_ID = ct.OBJECT_ID
                JOIN {TAGS} t ON ct.TAG_ID = t.TAG_ID
                WHERE ci.PUBLISHED_AT BETWEEN '{end_date}'::DATE - 7 AND '{end_date} 23:59:59'
                  AND t.TAG_NORMALIZED IS NOT NULL
                  AND LENGTH(t.TAG_NORMALIZED) > 2
                GROUP BY t.TAG_NORMALIZED
                HAVING COUNT(*) >= 2
            ),
            previous_week AS (
                SELECT
                    t.TAG_NORMALIZED AS TAG,
                    COUNT(*) AS PREVIOUS_CT
                FROM {CONTENT_ITEMS} ci
                JOIN {CONTENT_TAG} ct ON ci.OBJECT_ID = ct.OBJECT_ID
                JOIN {TAGS} t ON ct.TAG_ID = t.TAG_ID
                WHERE ci.PUBLISHED_AT BETWEEN '{end_date}'::DATE - 14 AND '{end_date}'::DATE - 7
                  AND t.TAG_NORMALIZED IS NOT NULL
                  AND LENGTH(t.TAG_NORMALIZED) > 2
                GROUP BY t.TAG_NORMALIZED
            )
            SELECT
                COALESCE(cw.TAG, pw.TAG) AS TAG,
                COALESCE(cw.CURRENT_CT, 0) AS CURRENT_WEEK,
                COALESCE(pw.PREVIOUS_CT, 0) AS PREVIOUS_WEEK,
                COALESCE(cw.CURRENT_CT, 0) - COALESCE(pw.PREVIOUS_CT, 0) AS ABSOLUTE_CHANGE,
                CASE
                    WHEN COALESCE(pw.PREVIOUS_CT, 0) = 0 AND COALESCE(cw.CURRENT_CT, 0) > 0 THEN 100.0
                    WHEN COALESCE(pw.PREVIOUS_CT, 0) = 0 THEN 0.0
                    ELSE ROUND(((COALESCE(cw.CURRENT_CT, 0) - pw.PREVIOUS_CT) / pw.PREVIOUS_CT::FLOAT) * 100, 1)
                END AS GROWTH_PCT,
                CASE
                    WHEN COALESCE(pw.PREVIOUS_CT, 0) = 0 AND COALESCE(cw.CURRENT_CT, 0) > 0 THEN 'NEW'
                    WHEN COALESCE(cw.CURRENT_CT, 0) = 0 AND COALESCE(pw.PREVIOUS_CT, 0) > 0 THEN 'GONE'
                    WHEN COALESCE(cw.CURRENT_CT, 0) > COALESCE(pw.PREVIOUS_CT, 0) * 1.3 THEN 'RISING'
                    WHEN COALESCE(cw.CURRENT_CT, 0) < COALESCE(pw.PREVIOUS_CT, 0) * 0.7 THEN 'DECLINING'
                    ELSE 'STABLE'
                END AS TREND_STATUS
            FROM current_week cw
            FULL OUTER JOIN previous_week pw ON cw.TAG = pw.TAG
            WHERE COALESCE(cw.CURRENT_CT, 0) + COALESCE(pw.PREVIOUS_CT, 0) >= 3
            ORDER BY GROWTH_PCT DESC
        """
        radar_df = run_query(radar_sql)

        if len(radar_df) == 0:
            st.info("Not enough data across two weeks to compute trend velocity.")
        else:
            # KPIs
            rising = len(radar_df[radar_df["TREND_STATUS"] == "RISING"])
            declining = len(radar_df[radar_df["TREND_STATUS"] == "DECLINING"])
            new_tags = len(radar_df[radar_df["TREND_STATUS"] == "NEW"])
            stable = len(radar_df[radar_df["TREND_STATUS"] == "STABLE"])

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Rising Topics", rising, delta=f"+{rising}")
            c2.metric("Declining Topics", declining, delta=f"-{declining}", delta_color="inverse")
            c3.metric("New Topics", new_tags)
            c4.metric("Stable Topics", stable)

            # Trend Radar scatter plot
            with st.container(border=True):
                st.markdown("**Trend Radar: Volume vs Growth**")
                st.caption("X = Current week volume, Y = Growth %. Size = absolute volume. Color = trend status.")

                radar_plot = radar_df.copy()
                radar_plot["GROWTH_PCT_CAPPED"] = radar_plot["GROWTH_PCT"].clip(-100, 200)

                trend_colors = {
                    "RISING": "#2ecc71", "STABLE": "#3498db",
                    "DECLINING": NETFLIX_RED, "NEW": "#f39c12", "GONE": "#95a5a6",
                }
                chart = (
                    alt.Chart(radar_plot)
                    .mark_circle(opacity=0.7)
                    .encode(
                        x=alt.X("CURRENT_WEEK:Q", title="Current Week Volume"),
                        y=alt.Y("GROWTH_PCT_CAPPED:Q", title="Growth % (week-over-week)"),
                        size=alt.Size("CURRENT_WEEK:Q", legend=None, scale=alt.Scale(range=[50, 500])),
                        color=alt.Color(
                            "TREND_STATUS:N",
                            title="Status",
                            scale=alt.Scale(
                                domain=list(trend_colors.keys()),
                                range=list(trend_colors.values()),
                            ),
                        ),
                        tooltip=["TAG", "CURRENT_WEEK", "PREVIOUS_WEEK", "GROWTH_PCT", "TREND_STATUS"],
                    )
                    .properties(height=450)
                )
                rule = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(strokeDash=[5, 5], color="gray").encode(y="y:Q")
                st.altair_chart(chart + rule, use_container_width=True)

            # Rising and declining tables
            col1, col2 = st.columns(2)

            with col1:
                with st.container(border=True):
                    st.markdown("**Rising Topics** :arrow_up:")
                    rising_df = radar_df[radar_df["TREND_STATUS"].isin(["RISING", "NEW"])].head(10)
                    if len(rising_df) > 0:
                        display_df = rising_df[["TAG", "CURRENT_WEEK", "PREVIOUS_WEEK", "GROWTH_PCT", "TREND_STATUS"]].copy()
                        display_df.columns = ["Tag", "This Week", "Last Week", "Growth %", "Status"]
                        st.dataframe(display_df, hide_index=True, use_container_width=True)
                    else:
                        st.info("No rising topics detected.")

            with col2:
                with st.container(border=True):
                    st.markdown("**Declining Topics** :arrow_down:")
                    declining_df = radar_df[radar_df["TREND_STATUS"].isin(["DECLINING", "GONE"])].head(10)
                    if len(declining_df) > 0:
                        display_df = declining_df[["TAG", "CURRENT_WEEK", "PREVIOUS_WEEK", "GROWTH_PCT", "TREND_STATUS"]].copy()
                        display_df.columns = ["Tag", "This Week", "Last Week", "Growth %", "Status"]
                        st.dataframe(display_df, hide_index=True, use_container_width=True)
                    else:
                        st.info("No declining topics detected.")

            # Full table
            with st.expander("View all topics with trend data"):
                display_all = radar_df[["TAG", "CURRENT_WEEK", "PREVIOUS_WEEK", "ABSOLUTE_CHANGE", "GROWTH_PCT", "TREND_STATUS"]].copy()
                display_all.columns = ["Tag", "This Week", "Last Week", "Change", "Growth %", "Status"]
                st.dataframe(display_all, hide_index=True, use_container_width=True)

    except Exception as e:
        st.error(f"Trend radar failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 10: CORTEX ANALYST — uses Semantic View REST API (Natural Language -> SQL)
# ═══════════════════════════════════════════════════════════════════════════════
with tab10:
    st.subheader("Cortex Analyst -- Natural Language Queries")
    st.caption(
        "Ask questions in plain English and get answers from the data. "
        "Uses the **SOCIAL_CONVERSATIONS** semantic view with the Cortex Analyst API. "
        "**This has no Pulsar equivalent** -- it is a unique Snowflake capability."
    )

    SEMANTIC_VIEW_FQN = f"{DB}.{SCHEMA}.SOCIAL_CONVERSATIONS"
    ANALYST_API_ENDPOINT = "/api/v2/cortex/analyst/message"
    ANALYST_TIMEOUT_MS = 50000

    st.info(
        "Cortex Analyst translates your natural language questions into SQL using "
        "the semantic view. Try questions like:\n"
        "- *What are the top 10 tags by post count?*\n"
        "- *Show me daily post volume by platform*\n"
        "- *Which authors have the most posts on Reddit?*\n"
        "- *What is the average engagement score on TikTok?*"
    )

    analyst_question = st.text_input(
        "Ask a question about the social data:",
        placeholder="e.g., What are the top subreddits by author count?",
        key="analyst_input",
    )

    if analyst_question:
        with st.spinner("Cortex Analyst is translating your question to SQL..."):
            try:
                request_body = {
                    "messages": [
                        {
                            "role": "user",
                            "content": [{"type": "text", "text": analyst_question}],
                        }
                    ],
                    "semantic_view": SEMANTIC_VIEW_FQN,
                }

                resp = _snowflake.send_snow_api_request(
                    "POST",
                    ANALYST_API_ENDPOINT,
                    {},   # headers
                    {},   # params
                    request_body,
                    None,  # request_guid
                    ANALYST_TIMEOUT_MS,
                )

                resp_status = resp.get("status", 0) if isinstance(resp, dict) else 0
                resp_content = resp.get("content", "{}") if isinstance(resp, dict) else "{}"

                if resp_status != 200:
                    reason = resp.get("reason", "Unknown") if isinstance(resp, dict) else "Unknown"
                    st.error(f"Cortex Analyst returned HTTP {resp_status}: {reason}")
                    st.code(str(resp_content)[:2000])
                else:
                    parsed = json.loads(resp_content) if isinstance(resp_content, str) else resp_content
                    analyst_message = parsed.get("message", {})
                    content_blocks = analyst_message.get("content", [])

                    generated_sql = None
                    analyst_text = ""
                    suggestions = []

                    for block in content_blocks:
                        block_type = block.get("type", "")
                        if block_type == "text":
                            analyst_text = block.get("text", "")
                        elif block_type == "sql":
                            generated_sql = block.get("statement", "")
                        elif block_type == "suggestions":
                            suggestions = block.get("suggestions", [])

                    # Show the analyst's interpretation
                    if analyst_text:
                        st.markdown(f"**Analyst:** {clean_ai_text(analyst_text)}")

                    # If SQL was returned, show and execute it
                    if generated_sql:
                        with st.expander("View generated SQL", expanded=False):
                            st.code(generated_sql, language="sql")

                        result_df = run_query(generated_sql)

                        st.success(f"Query returned {len(result_df)} rows")

                        if len(result_df) > 0:
                            st.dataframe(result_df, hide_index=True, use_container_width=True)

                            # Auto-chart if the result has a good shape for visualization
                            if len(result_df.columns) >= 2 and len(result_df) <= 50:
                                cols = result_df.columns.tolist()
                                numeric_cols = result_df.select_dtypes(include=["number"]).columns.tolist()
                                other_cols = [c for c in cols if c not in numeric_cols]

                                if numeric_cols and other_cols:
                                    with st.container(border=True):
                                        st.markdown("**Auto-generated visualization**")
                                        chart = (
                                            alt.Chart(result_df.head(20))
                                            .mark_bar()
                                            .encode(
                                                x=alt.X(f"{numeric_cols[0]}:Q", title=numeric_cols[0]),
                                                y=alt.Y(f"{other_cols[0]}:N", sort="-x", title=other_cols[0]),
                                                color=alt.value(NETFLIX_RED),
                                                tooltip=cols,
                                            )
                                            .properties(height=min(400, len(result_df) * 25 + 50))
                                        )
                                        st.altair_chart(chart, use_container_width=True)

                    # If suggestions were returned (ambiguous question), show them
                    elif suggestions:
                        st.warning("Your question was ambiguous. Try one of these suggested questions:")
                        for s in suggestions:
                            st.markdown(f"- {s}")

                    elif not generated_sql and not suggestions:
                        st.warning("Cortex Analyst could not generate SQL for this question. Try rephrasing.")

            except Exception as e:
                st.error(f"Query failed: {e}")
                st.caption(
                    "Tip: Try rephrasing your question, or make it more specific. "
                    "Cortex Analyst works best with clear, structured questions."
                )
    else:
        st.markdown(
            "**Example questions:**\n"
            "- What are the top 10 trending tags this month?\n"
            "- How many posts per day are there on each platform?\n"
            "- Who are the most active authors on TikTok?\n"
            "- Which content types get the most engagement?\n"
            "- Show me posts with the tag 'romantasy' ordered by date"
        )
