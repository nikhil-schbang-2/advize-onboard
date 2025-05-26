import requests
import os
import json
import time
from datetime import datetime, date, timedelta
import psycopg2 as psy
from dotenv import load_dotenv
from typing import Set, Dict, Any, Optional, List, Tuple
import traceback

from database.db import Database

# Load environment variables
load_dotenv()

# --- Configuration ---
META_API_KEY = os.getenv("META_API_KEY")
META_AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID")

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", 5432)

INSIGHTS_BASE_URL = (
    f"https://graph.facebook.com/v22.0/act_{META_AD_ACCOUNT_ID}/insights"
)

HEADERS = {
    "Authorization": f"Bearer {META_API_KEY}",
    "Content-Type": "application/json",
}

# Define breakdown configurations for the fact table
FACT_TABLE_BREAKDOWN_CONFIGS = [
    {"type": "account", "api_breakdowns": []},
    {"type": "region", "api_breakdowns": ["region"]},
    {"type": "age,gender", "api_breakdowns": ["age", "gender"]},
    {
        "type": "platform,placement",
        "api_breakdowns": ["publisher_platform", "platform_position"],
    },
    {"type": "impression_device", "api_breakdowns": ["impression_device"]},
]

# Define metrics to retrieve from the API
INSIGHTS_FIELDS = [
    "ad_id",
    "spend",
    "impressions",
    "reach",
    "actions",
    "action_values",
    "date_start",
    "date_stop",
]

# Define time range for fetching insights (last 90 days)
today_fact = date.today()
duration = today_fact - timedelta(days=90)
FACT_FETCH_TIME_RANGE = {
    "since": duration.strftime("%Y-%m-%d"),
    "until": today_fact.strftime("%Y-%m-%d"),
}

# Define time window keys for the fact table
FACT_TABLE_TIME_WINDOWS = ["90d"]

# Mapping of API breakdown name to database table and columns
DIMENSION_MAPPING = {
    "account": {
        "table": "dim_account",
        "name_column": "account_id",
        "id_column": "account_id",
    },
    "region": {
        "table": "dim_region",
        "name_column": "region_name",
        "id_column": "region_id",
    },
    "age": {
        "table": "dim_age_group",
        "name_column": "age_range",
        "id_column": "age_id",
    },
    "gender": {
        "table": "dim_gender",
        "name_column": "gender_name",
        "id_column": "gender_id",
    },
    "publisher_platform": {
        "table": "dim_platform",
        "name_column": "platform_name",
        "id_column": "platform_id",
    },
    "platform_position": {
        "table": "dim_placement",
        "name_column": "placement_name",
        "id_column": "placement_id",
    },
    "impression_device": {
        "table": "dim_impression_device",
        "name_column": "impression_device_name",
        "id_column": "impression_device_id",
    },
}


# --- Database Helper Functions ---
def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        conn = psy.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
        )
        return conn
    except psy.OperationalError as e:
        print(f"Database connection failed: {e}")
        return None


def upsert_dimension(db: Database, table_name, name_column, name_value, id_column):
    """
    Checks if a dimension record exists. If not, inserts it. Returns the ID.
    Assumes dimension tables have a NOT NULL primary key with a DEFAULT value generator (like UUID or SERIAL).
    Special handling for dim_account where account_id is the natural key and primary key.
    """
    if name_value is None:
        return None

    try:
        # Check if the record exists
        select_sql = f"SELECT {id_column} FROM {table_name} WHERE {name_column} = %s"
        cursor = db.execute(select_sql, (name_value,))
        existing_result = cursor.fetchone() if cursor else None

        if existing_result:
            return existing_result[id_column]

        # Insert and return the ID
        insert_sql = f"""
            INSERT INTO {table_name} ({name_column})
            VALUES (%s)
            RETURNING {id_column};
        """
        cursor = db.execute(insert_sql, (name_value,), commit=True)
        new_result = cursor.fetchone() if cursor else None

        if new_result:
            return new_result[id_column]
        else:
            print(
                f"Error: Insert into {table_name} for value '{name_value}' did not return an ID."
            )
            return None

    except Exception as e:
        db.conn.rollback()
        print(f"Error upserting dimension '{name_value}' into {table_name}: {e}")
        return None


# Not is use
def upsert_date_dimension(conn, date_obj: date):
    """
    Inserts or updates a date record in dim_date. Returns the date_id.
    Assumes dim_date has a SERIAL primary key 'date_id' and unique constraint on date_value.
    """
    if date_obj is None:
        print("Warning: Skipping date dimension upsert due to None date object.")
        return None

    try:
        with conn.cursor() as cur:
            date_value = date_obj
            year = date_obj.year
            month = date_obj.month
            day = date_obj.day
            week_of_year = date_obj.isocalendar()[1]
            quarter = (date_obj.month - 1) // 3 + 1

            cur.execute(
                "SELECT date_id FROM dim_date WHERE date_value = %s", (date_value,)
            )
            existing_result = cur.fetchone()

            if existing_result:
                return existing_result[0]
            else:
                insert_sql = """
                    INSERT INTO dim_date (date_value, year, month, day, week_of_year, quarter)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING date_id;
                """
                params = (date_value, year, month, day, week_of_year, quarter)
                cur.execute(insert_sql, params)
                new_result = cur.fetchone()

                if new_result:
                    conn.commit()
                    print(
                        f"Successfully upserted date dimension '{date_value}' into dim_date. ID: {new_result[0]}"
                    )
                    return new_result[0]
                else:
                    print(
                        f"Error: Insert into dim_date for date '{date_value}' did not return an ID."
                    )
                    conn.rollback()
                    return None

    except Exception as e:
        conn.rollback()
        print(f"Error upserting date dimension '{date_obj}': {e}")
        return None


# --- Data Fetching Functions ---
def fetch_page_with_retry(
    url, params, headers, attempt=1, max_retries=5, initial_delay=120, max_delay=3600
):
    """Fetches a single page of data from a given URL with retry logic and increased delays."""
    delay = initial_delay * (2 ** (attempt - 1))
    delay = min(delay, max_delay)

    try:
        print(
            f"Fetching page (Attempt {attempt}/{max_retries}). Retrying in {delay} seconds if needed..."
        )
        response = requests.get(url, headers=headers, params=params, timeout=120)
        response.raise_for_status()
        print(f"Successfully fetched page. Status Code: {response.status_code}")
        return response.json()

    except requests.exceptions.Timeout as e:
        print(f"Request timed out. Error: {e}")
        last_error = e
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred. Status: {e.response.status_code}. Error: {e}")
        last_error = e
        print(f"API Error Response Body: {e.response.text}")
        if e.response.status_code == 429 or (
            e.response.status_code == 400
            and "too many calls" in e.response.text.lower()
        ):
            print("Rate limit hit.")
        if (
            400 <= e.response.status_code < 500
            and e.response.status_code != 429
            and not ("too many calls" in e.response.text.lower())
        ):
            return {"error": f"Client error: {e.response.status_code}"}
    except requests.exceptions.RequestException as e:
        print(f"Request failed. Error: {e}")
        last_error = e
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        last_error = e

    if attempt < max_retries:
        print(f"Retrying in {delay} seconds...")
        time.sleep(delay)
        return fetch_page_with_retry(
            url, params, headers, attempt + 1, max_retries, initial_delay, max_delay
        )
    else:
        print(
            f"Max retries reached. Failed to fetch page after multiple attempts. Last error: {last_error}"
        )
        return {
            "error": f"Failed to fetch page after {max_retries} attempts. Last error: {last_error}"
        }


def fetch_ad_account_insights_with_breakdown(
    ad_account_id, fields, breakdowns, time_range
):
    """
    Fetches all pages of insights data for the ad account, broken down by the specified dimensions
    at the ad level.
    """
    all_insights_data = []
    params = {
        "fields": ",".join(fields),
        "time_range": json.dumps(time_range),
        "level": "ad",
        "limit": 500,
        "action_breakdowns": json.dumps(["action_type"]),
    }

    if breakdowns:
        params["breakdowns"] = json.dumps(breakdowns)

    next_page_url = INSIGHTS_BASE_URL
    current_params = params

    print(
        f"Fetching insights for Ad Account {ad_account_id}, Breakdown: {breakdowns if breakdowns else 'None'}"
    )

    while next_page_url:
        response_data = fetch_page_with_retry(next_page_url, current_params, HEADERS)

        if response_data and "error" in response_data:
            print(
                f"Error fetching insights for Ad Account {ad_account_id} with breakdown {breakdowns if breakdowns else 'None'}: {response_data['error']}"
            )
            return None

        if response_data and "data" in response_data:
            all_insights_data.extend(response_data["data"])

            paging_info = response_data.get("paging")
            if paging_info and "next" in paging_info:
                next_page_url = paging_info["next"]
                current_params = None
            else:
                next_page_url = None

        else:
            print(
                f"Warning: API response missing 'data' field for Ad Account {ad_account_id}, breakdown {breakdowns if breakdowns else 'None'}."
            )
            next_page_url = None

    return all_insights_data


# --- Data Processing and Insertion Function ---
def process_and_insert_insights(
    insights_data, time_window_key, breakdown_config, db_conn
):
    """
    Processes fetched insights data and inserts it into the fact table.
    Discovers and upserts dimension values as they are encountered.
    Fetches account_id, adset_id, and campaign_id by joining dimension tables based on ad_id.
    Handles transaction rollback on insertion errors.
    Includes account_id, adset_id, campaign_id in INSERT and ON CONFLICT.
    Adds checks for None dimension IDs for specific breakdowns.
    Adds detailed logging for row processing errors.
    """
    if not insights_data:
        return

    # Define the ON CONFLICT clause based on your fact table's unique constraint
    on_conflict_clause = """
    ON CONFLICT (ad_id, account_id, adset_id, campaign_id, time_window, start_date, end_date, breakdown_type,
                 region_id, age_id, gender_id, platform_id, placement_id, impression_device_id
                )
    DO UPDATE SET
        amount_spent = EXCLUDED.amount_spent,
        video_plays_3s = EXCLUDED.video_plays_3s,
        impressions = EXCLUDED.impressions,
        reach = EXCLUDED.reach,
        thruplays = EXCLUDED.thruplays,
        link_click = EXCLUDED.link_click,
        landing_page_view = EXCLUDED.landing_page_view,
        purchase = EXCLUDED.purchase,
        lead = EXCLUDED.lead,
        post_reaction = EXCLUDED.post_reaction,
        post_shares = EXCLUDED.post_shares,
        post_save = EXCLUDED.post_save,
        purchase_revenue = EXCLUDED.purchase_revenue,
        lead_revenue = EXCLUDED.lead_revenue,
        custom_metrics = EXCLUDED.custom_metrics,
        created_at = EXCLUDED.created_at;
    """

    # INSERT statement including hierarchy and dimension IDs
    insert_sql = f"""
    INSERT INTO fact_ad_metrics_aggregated (
        time_window, start_date, end_date, ad_id, account_id, adset_id, campaign_id,
        region_id, age_id, gender_id, platform_id, placement_id, impression_device_id,
        breakdown_type,
        amount_spent, video_plays_3s, impressions, reach, thruplays,
        link_click, landing_page_view, purchase, lead, post_reaction, post_shares, post_save,
        purchase_revenue, lead_revenue,
        custom_metrics, created_at
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s,
        %s, %s
    )
    {on_conflict_clause}
    """

    # SQL query to fetch account_id, adset_id, campaign_id by joining dimension tables
    hierarchy_query = """
        SELECT
            dac.account_id,
            das.adset_id,
            dc.campaign_id
        FROM
            dim_ad da
        JOIN
            dim_adset das ON da.adset_id = das.adset_id
        JOIN
            dim_campaign dc ON das.campaign_id = dc.campaign_id
        JOIN
            dim_account dac ON dc.account_id = dac.account_id
        WHERE
            da.ad_id = %s;
    """

    rows_to_insert = []

    hierarchy_cursor = db_conn.cursor()

    print(
        f"Processing {len(insights_data)} insights rows for breakdown: '{breakdown_config['type']}'..."
    )

    for row in insights_data:
        print(f"\n--- Processing row for Ad ID: {row.get('ad_id')} ---")
        # print(f"Row data: {row}") # Keep row data for debugging if needed

        try:
            ad_id = int(row.get("ad_id"))

            # Fetch hierarchy data by joining dimension tables
            hierarchy_cursor.execute(hierarchy_query, (ad_id,))
            hierarchy_data = hierarchy_cursor.fetchone()

            if not hierarchy_data:
                print(
                    f"Warning: Could not find hierarchy data for ad_id {ad_id} by joining dimension tables. Skipping row."
                )
                continue

            account_id = hierarchy_data[0]
            adset_id = hierarchy_data[1]
            campaign_id = hierarchy_data[2]

            # print(f"Fetched Ad Dims for ad {ad_id}: Account ID = {account_id}, Adset ID = {adset_id}, Campaign ID = {campaign_id}")

            start_date_str = row.get("date_start")
            end_date_str = row.get("date_stop")
            start_date_obj = (
                datetime.strptime(start_date_str, "%Y-%m-%d").date()
                if start_date_str
                else None
            )
            end_date_obj = (
                datetime.strptime(end_date_str, "%Y-%m-%d").date()
                if end_date_str
                else None
            )
            # print(f"Dates: Start = {start_date_obj}, End = {end_date_obj}")

            amount_spent = float(row.get("spend", 0))
            impressions = int(row.get("impressions", 0))
            reach = int(row.get("reach", 0))
            # print(f"Metrics: Spend = {amount_spent}, Impressions = {impressions}, Reach = {reach}")

            actions = row.get("actions", [])
            link_click = sum(
                [
                    int(action.get("value", 0))
                    for action in actions
                    if action.get("action_type") == "link_click"
                ]
            )
            landing_page_view = sum(
                [
                    int(action.get("value", 0))
                    for action in actions
                    if action.get("action_type") == "landing_page_view"
                ]
            )
            purchase = sum(
                [
                    int(action.get("value", 0))
                    for action in actions
                    if action.get("action_type") == "omni_purchase"
                ]
            )
            lead = sum(
                [
                    int(action.get("value", 0))
                    for action in actions
                    if action.get("action_type") == "lead"
                ]
            )
            post_reaction = sum(
                [
                    int(action.get("value", 0))
                    for action in actions
                    if action.get("action_type") == "react"
                ]
            )
            post_shares = sum(
                [
                    int(action.get("value", 0))
                    for action in actions
                    if action.get("action_type") == "post"
                ]
            )
            post_save = sum(
                [
                    int(action.get("value", 0))
                    for action in actions
                    if action.get("action_type") == "onsite_conversion.post_save"
                ]
            )
            video_plays_3s = sum(
                [
                    int(action.get("value", 0))
                    for action in actions
                    if action.get("action_type") == "video_view"
                ]
            )
            thruplays = sum(
                [
                    int(action.get("value", 0))
                    for action in actions
                    if action.get("action_type") == "video_thruplay_watched"
                ]
            )
            # print(f"Actions Metrics: Link Click = {link_click}, ...")

            action_values = row.get("action_values", [])
            purchase_revenue = sum(
                [
                    float(av.get("value", 0))
                    for av in action_values
                    if av.get("action_type") == "omni_purchase"
                ]
            )
            lead_revenue = sum(
                [
                    float(av.get("value", 0))
                    for av in action_values
                    if av.get("action_type") == "lead"
                ]
            )
            # print(f"Revenue Metrics: Purchase Revenue = {purchase_revenue}, ...")

            region_id = None
            age_id = None
            gender_id = None
            platform_id = None
            placement_id = None
            impression_device_id = None

            breakdown_type = breakdown_config["type"]
            api_breakdowns = breakdown_config["api_breakdowns"]
            # print(f"Breakdown Type: {breakdown_type}")

            if breakdown_type == "account":
                account_name = META_AD_ACCOUNT_ID
                mapping = DIMENSION_MAPPING["account"]
                account_dim_id = upsert_dimension(
                    db_conn,
                    mapping["table"],
                    mapping["name_column"],
                    account_name,
                    mapping["id_column"],
                )

            elif breakdown_type == "region" and "region" in row:
                region_name = row.get("region")
                mapping = DIMENSION_MAPPING["region"]
                region_id = upsert_dimension(
                    db_conn,
                    mapping["table"],
                    mapping["name_column"],
                    region_name,
                    mapping["id_column"],
                )
                if region_id is None:
                    print(
                        f"Warning: Skipping row for ad {ad_id} due to missing region_id for breakdown '{breakdown_type}'. Region name: '{region_name}'."
                    )
                    continue

            elif breakdown_type == "age,gender" and "age" in row and "gender" in row:
                age_range = row.get("age")
                gender_name = row.get("gender")
                mapping_age = DIMENSION_MAPPING["age"]
                mapping_gender = DIMENSION_MAPPING["gender"]
                age_id = upsert_dimension(
                    db_conn,
                    mapping_age["table"],
                    mapping_age["name_column"],
                    age_range,
                    mapping_age["id_column"],
                )
                gender_id = upsert_dimension(
                    db_conn,
                    mapping_gender["table"],
                    mapping_gender["name_column"],
                    gender_name,
                    mapping_gender["id_column"],
                )
                if age_id is None or gender_id is None:
                    print(
                        f"Warning: Skipping row for ad {ad_id} due to missing age_id ({age_id}) or gender_id ({gender_id}) for breakdown '{breakdown_type}'. Age: '{age_range}', Gender: '{gender_name}'."
                    )
                    continue

            elif (
                breakdown_type == "platform,placement"
                and "publisher_platform" in row
                and "platform_position" in row
            ):
                platform_name = row.get("publisher_platform")
                placement_name = row.get("platform_position")
                mapping_platform = DIMENSION_MAPPING["publisher_platform"]
                mapping_placement = DIMENSION_MAPPING["platform_position"]
                platform_id = upsert_dimension(
                    db_conn,
                    mapping_platform["table"],
                    mapping_platform["name_column"],
                    platform_name,
                    mapping_platform["id_column"],
                )
                placement_id = upsert_dimension(
                    db_conn,
                    mapping_placement["table"],
                    mapping_placement["name_column"],
                    placement_name,
                    mapping_placement["id_column"],
                )
                if platform_id is None or placement_id is None:
                    print(
                        f"Warning: Skipping row for ad {ad_id} due to missing platform_id ({platform_id}) or placement_id ({placement_id}) for breakdown '{breakdown_type}'. Platform: '{platform_name}', Placement: '{placement_name}'."
                    )
                    continue

            elif breakdown_type == "impression_device" and "impression_device" in row:
                impression_device_name = row.get("impression_device")
                mapping = DIMENSION_MAPPING["impression_device"]
                impression_device_id = upsert_dimension(
                    db_conn,
                    mapping["table"],
                    mapping["name_column"],
                    impression_device_name,
                    mapping["id_column"],
                )
                if impression_device_id is None:
                    print(
                        f"Warning: Skipping row for ad {ad_id} due to missing impression_device_id for breakdown '{breakdown_type}'. Impression Device: '{impression_device_name}'."
                    )
                    continue

            # print(f"Dimension IDs for row: Region={region_id}, ...")

            custom_metrics = {}

            data_tuple = (
                time_window_key,
                start_date_obj,
                end_date_obj,
                ad_id,
                account_id,
                adset_id,
                campaign_id,
                region_id,
                age_id,
                gender_id,
                platform_id,
                placement_id,
                impression_device_id,
                breakdown_type,
                amount_spent,
                video_plays_3s,
                impressions,
                reach,
                thruplays,
                link_click,
                landing_page_view,
                purchase,
                lead,
                post_reaction,
                post_shares,
                post_save,
                purchase_revenue,
                lead_revenue,
                json.dumps(custom_metrics),
                datetime.now(),
            )
            rows_to_insert.append(data_tuple)

        except Exception as e:
            print(
                f"\n--- Error processing insights row for Ad ID {row.get('ad_id')} ---"
            )
            print(f"Error: {e}")
            traceback.print_exc()
            if isinstance(e, psy.Error):
                print(f"  PostgreSQL Error Code: {e.pgcode}")
                if e.diag:
                    print(f"  PostgreSQL Error Message: {e.diag.message_primary}")
                    try:
                        if e.diag.detail:
                            print(f"  PostgreSQL Error Detail: {e.diag.detail}")
                    except AttributeError:
                        pass
                    if e.diag.constraint_name:
                        print(f"  PostgreSQL Constraint Name: {e.diag.constraint_name}")
            print("----------------------------------------------------\n")

    hierarchy_cursor.close()

    if rows_to_insert:
        try:
            print(
                f"\nAttempting batch insert of {len(rows_to_insert)} rows for breakdown '{breakdown_config['type']}'..."
            )
            insert_cursor = db_conn.cursor()
            insert_cursor.executemany(insert_sql, rows_to_insert)
            db_conn.commit()
            print(
                f"Successfully inserted/updated {len(rows_to_insert)} rows for breakdown '{breakdown_config['type']}'."
            )
        except Exception as e:
            db_conn.rollback()
            print(
                f"\n--- Database insertion error for breakdown '{breakdown_config['type']}' ---"
            )
            print(f"Error: {e}")
            traceback.print_exc()
            if isinstance(e, psy.Error):
                print(f"  PostgreSQL Error Code: {e.pgcode}")
                if e.diag:
                    print(f"  PostgreSQL Error Message: {e.diag.message_primary}")
                    try:
                        if e.diag.detail:
                            print(f"  PostgreSQL Error Detail: {e.diag.detail}")
                    except AttributeError:
                        pass
                    if e.diag.constraint_name:
                        print(f"  PostgreSQL Constraint Name: {e.diag.constraint_name}")
            print("----------------------------------------------------\n")
        finally:
            insert_cursor.close()
    else:
        print(f"No valid rows to insert for breakdown '{breakdown_config['type']}'.")


# --- Main Execution Flow ---
if __name__ == "__main__":
    db_connection = get_db_connection()

    if db_connection:
        print("Database connection established.")

        # Populate dim_date for the current date
        print("\n--- Populating dim_date for current date ---")
        try:
            upsert_date_dimension(db_connection, today_fact)
            print("Finished populating dim_date for current date.")
        except Exception as e:
            print(f"Error populating dim_date: {e}")

        # Fetch all ad IDs from dim_ad table (required for the join query)
        valid_ad_ids = set()
        print("\n--- Fetching all ad IDs from dim_ad ---")
        try:
            with db_connection.cursor() as cur:
                cur.execute("SELECT ad_id FROM dim_ad;")
                valid_ad_ids = {row[0] for row in cur.fetchall()}
            print(f"Fetched {len(valid_ad_ids)} ad IDs from dim_ad.")
        except Exception as e:
            print(f"Error fetching ad IDs from dim_ad: {e}")
            print(
                "Could not fetch ad IDs from dim_ad. Please ensure dim_ad exists and is populated."
            )

        if not valid_ad_ids:
            print(
                "No ads found in dim_ad table or failed to fetch. Cannot populate fact table as hierarchy data cannot be retrieved. Exiting."
            )
            db_connection.close()
            exit()

        # Populate Fact Table
        print("\n--- Populating Fact Table (Discovering/Upserting Dimensions) ---")

        for breakdown_config in FACT_TABLE_BREAKDOWN_CONFIGS:
            print(
                f"\n--- Fetching and processing data for breakdown: '{breakdown_config['type']}' ---"
            )

            insights_data = fetch_ad_account_insights_with_breakdown(
                META_AD_ACCOUNT_ID,
                INSIGHTS_FIELDS,
                breakdown_config.get("api_breakdowns", []),
                FACT_FETCH_TIME_RANGE,
            )

            if insights_data is not None:
                for time_window_key in FACT_TABLE_TIME_WINDOWS:
                    process_and_insert_insights(
                        insights_data, time_window_key, breakdown_config, db_connection
                    )

            else:
                print(
                    f"  Skipping processing for breakdown '{breakdown_config['type']}' due to fetch errors."
                )

        db_connection.close()
        print("\nDatabase connection closed. ETL process finished.")

    else:
        print("Failed to establish database connection. Exiting.")
