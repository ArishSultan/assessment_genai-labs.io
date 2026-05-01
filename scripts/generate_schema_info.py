import yaml
import sqlite3
import warnings
import pandas as pd

from pathlib import Path
from typing import Any, Dict

CATEGORICAL_MAX_DISTINCT = 25
CATEGORICAL_TOP_N = 8
TEXT_SAMPLE_N = 3

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "gaming_mental_health.sqlite"
DEFAULT_SCHEMA_PATH = PROJECT_ROOT / "data" / "gaming_mental_health_schema.yaml"
DEFAULT_TABLE_NAME = "gaming_mental_health"


def generate_yaml_schema(
        db_path: str | Path,
        table_name: str,
        column_descriptions: Dict[str, Dict[str, str]],
        output_yaml_path: str | Path,
        table_description: str = ""
) -> None:
    db_path = Path(db_path)
    output_yaml_path = Path(output_yaml_path)

    if not db_path.exists():
        raise FileNotFoundError(f"Database not found at {db_path}")

    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        pragma_query = f"PRAGMA table_info('{table_name}')"
        schema_df = pd.read_sql(pragma_query, conn)

        if schema_df.empty:
            raise ValueError(f"Table '{table_name}' does not exist or has no columns.")

        sql_types = {row['name']: row['type'].upper() or "TEXT" for _, row in schema_df.iterrows()}
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    table_def: dict[str, Any] = {
        "description": table_description,
        "row_count": int(len(df)),
        "Columns": {}
    }

    for col_name, sql_type in sql_types.items():
        col_def: dict[str, Any] = {"type": sql_type}

        # --- NEW DESCRIPTION LOGIC ---
        if col_name in column_descriptions:
            desc_dict = column_descriptions[col_name]
            if "tiny-description" in desc_dict:
                col_def["tiny-description"] = desc_dict["tiny-description"]
            if "description" in desc_dict:
                col_def["description"] = desc_dict["description"]
        else:
            warnings.warn(f"Missing description dict for column: '{col_name}'")

        series = df[col_name].dropna()

        if series.empty:
            col_def["kind"] = "empty"
            table_def["Columns"][col_name] = col_def
            continue

        n_distinct = int(series.nunique())
        is_numeric = pd.api.types.is_numeric_dtype(series)

        if is_numeric and n_distinct > CATEGORICAL_MAX_DISTINCT:
            col_def["kind"] = "numeric"
            col_def["min"] = float(series.min()) if "FLOAT" in sql_type or "REAL" in sql_type else int(series.min())
            col_def["max"] = float(series.max()) if "FLOAT" in sql_type or "REAL" in sql_type else int(series.max())
            col_def["median"] = float(series.median())

        elif 2 <= n_distinct <= CATEGORICAL_MAX_DISTINCT:
            col_def["kind"] = "categorical"
            col_def["n_distinct"] = n_distinct
            top_values = series.value_counts().head(CATEGORICAL_TOP_N)
            col_def["values"] = {str(k): int(v) for k, v in top_values.items()}

        else:
            col_def["kind"] = "text"
            col_def["n_distinct"] = n_distinct
            sample_size = min(TEXT_SAMPLE_N, len(series))
            col_def["samples"] = series.sample(sample_size).astype(str).tolist()

        table_def["Columns"][col_name] = col_def

    with output_yaml_path.open("w", encoding="utf-8") as f:
        yaml.dump({table_name: table_def}, f, sort_keys=False, default_flow_style=False, allow_unicode=True)


if __name__ == "__main__":
    my_descriptions = {
        "age": {
            "tiny-description": "User age in years (13–59)",
            "description": "User's age in years; ranges from 13 to 59 across the dataset."
        },
        "gender": {
            "tiny-description": "Gender identity (Male, Female, Other)",
            "description": "Self-reported gender identity; three categories: Male, Female, and Other."
        },
        "income": {
            "tiny-description": "Annual income in USD (5k–150k)",
            "description": "Annual household income in USD; ranges from $5,000 to $149,999."
        },
        "daily_gaming_hours": {
            "tiny-description": "Avg hours spent gaming per day",
            "description": "Average number of hours the user spends gaming each day."
        },
        "weekly_sessions": {
            "tiny-description": "Number of gaming sessions per week",
            "description": "Count of distinct gaming sessions the user engages in per week."
        },
        "years_gaming": {
            "tiny-description": "Years the user has been gaming",
            "description": "Total number of years the individual has been playing video games."
        },
        "multiplayer_ratio": {
            "tiny-description": "Fraction of time in multiplayer games (0–1)",
            "description": "Proportion of gaming time spent in multiplayer vs. single-player modes."
        },
        "violent_games_ratio": {
            "tiny-description": "Share of play time on violent games",
            "description": "Fraction of total gaming time spent on games rated or classified as violent."
        },
        "mobile_gaming_ratio": {
            "tiny-description": "Proportion of sessions on mobile platforms",
            "description": "Share of gaming sessions played on mobile devices vs. consoles or PC."
        },
        "night_gaming_ratio": {
            "tiny-description": "Fraction of gaming done at night",
            "description": "Proportion of gaming activity occurring during nighttime hours."
        },
        "weekend_gaming_hours": {
            "tiny-description": "Total gaming hours on weekends",
            "description": "Cumulative gaming hours logged on Saturdays and Sundays."
        },
        "competitive_rank": {
            "tiny-description": "Competitive skill rank (0–99)",
            "description": "Numeric competitive rank reflecting the user's skill level in ranked game modes."
        },
        "microtransactions_spending": {
            "tiny-description": "USD spent on in-game purchases",
            "description": "Total amount in USD spent by the user on in-game microtransactions."
        },
        "esports_interest": {
            "tiny-description": "Interest in esports (0–10 scale)",
            "description": "Self-rated interest level in esports viewing or participation, from 0 to 10."
        },
        "headset_usage": {
            "tiny-description": "Uses gaming headset (0 = No, 1 = Yes)",
            "description": "Binary flag indicating whether the user regularly uses a gaming headset."
        },
        "streaming_hours": {
            "tiny-description": "Daily hours watching game streams",
            "description": "Average hours per day spent watching gaming-related streams or content."
        },
        "anxiety_score": {
            "tiny-description": "Anxiety severity (0–10)",
            "description": "Standardized score measuring the user's anxiety level; higher = more anxious."
        },
        "depression_score": {
            "tiny-description": "Depression severity (0–10)",
            "description": "Standardized score capturing depressive symptom intensity on a 0–10 scale."
        },
        "stress_level": {
            "tiny-description": "Perceived stress level (1–10)",
            "description": "User's self-reported stress level, rated from 1 (minimal) to 10 (extreme)."
        },
        "addiction_level": {
            "tiny-description": "Gaming addiction intensity (0–10)",
            "description": "Composite score indicating degree of gaming addiction or dependency behavior."
        },
        "loneliness_score": {
            "tiny-description": "Perceived loneliness (0–10)",
            "description": "Score reflecting how lonely or socially isolated the user feels; 0–10."
        },
        "aggression_score": {
            "tiny-description": "Aggression level (0–10)",
            "description": "Measure of aggressive tendencies or anger responses on a 0–10 scale."
        },
        "happiness_score": {
            "tiny-description": "Subjective happiness (0–10)",
            "description": "User's self-reported happiness or life satisfaction scored from 0 to 10."
        },
        "sleep_hours": {
            "tiny-description": "Avg nightly sleep duration in hours",
            "description": "Average number of hours of sleep the user gets per night."
        },
        "caffeine_intake": {
            "tiny-description": "Daily caffeine consumption (arbitrary units)",
            "description": "Estimated daily caffeine consumption expressed in standardized units."
        },
        "exercise_hours": {
            "tiny-description": "Daily hours of physical exercise",
            "description": "Average hours per day the user spends on physical exercise or activity."
        },
        "bmi": {
            "tiny-description": "Body Mass Index (4.45–43.6)",
            "description": "Body Mass Index calculated from user height and weight; ranges from 4.45 to 43.6."
        },
        "screen_time_total": {
            "tiny-description": "Total daily screen time (all devices)",
            "description": "Total hours per day spent on screens across all devices including non-gaming."
        },
        "eye_strain_score": {
            "tiny-description": "Eye strain severity (0–10)",
            "description": "User-reported eye strain or visual fatigue level associated with screen usage."
        },
        "back_pain_score": {
            "tiny-description": "Back pain severity (0–10)",
            "description": "Self-reported back or posture-related pain level, likely linked to sedentary gaming."
        },
        "social_interaction_score": {
            "tiny-description": "Quality of social interactions (0–10)",
            "description": "Score reflecting the frequency and quality of the user's real-world social interactions."
        },
        "relationship_satisfaction": {
            "tiny-description": "Satisfaction with personal relationships (0–10)",
            "description": "How satisfied the user is with their personal and romantic relationships; 0–10."
        },
        "friends_gaming_count": {
            "tiny-description": "Number of real-life friends who also game",
            "description": "Count of the user's offline friends who are also active gamers."
        },
        "online_friends": {
            "tiny-description": "Number of online gaming friends (0–499)",
            "description": "Total number of friends or connections the user has within online gaming communities."
        },
        "toxic_exposure": {
            "tiny-description": "Exposure to toxic in-game behavior (0–0.95)",
            "description": "Proportion of gaming sessions where the user experienced toxic or harmful behavior from others."
        },
        "parental_supervision": {
            "tiny-description": "Level of parental oversight (0–10 scale)",
            "description": "Degree of parental monitoring or supervision over the user's gaming habits; 0–10."
        },
        "academic_performance": {
            "tiny-description": "Academic score or grade (0–100)",
            "description": "Academic performance score or grade point equivalent on a 0–100 scale."
        },
        "work_productivity": {
            "tiny-description": "Work productivity score (0–100)",
            "description": "Self-reported or estimated work output and productivity rated from 0 to 100."
        },
        "internet_quality": {
            "tiny-description": "Internet connection quality (1–10)",
            "description": "Rated quality of the user's internet connection, relevant to online gaming experience."
        }
    }

    generate_yaml_schema(
        db_path=DEFAULT_DB_PATH,
        table_name=DEFAULT_TABLE_NAME,
        column_descriptions=my_descriptions,
        output_yaml_path=DEFAULT_SCHEMA_PATH,
        # Leave this empty for now as there is no concrete use of this.
        table_description=""
    )
