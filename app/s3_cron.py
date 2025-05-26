import psycopg2
import os
import requests
import json

from app.config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from requests.exceptions import RequestException, HTTPError

# --- Configuration ---

# Database Table and Column for Image URLs
IMAGE_TABLE_NAME = "dim_image_creative"
IMAGE_ID_COLUMN = (
    "image_hash"  # Replace with your primary key column name for dim_image_creative
)
URL_COLUMN_NAME = "asset_link"

ACCOUNT_TABLE_NAME = "dim_account"
BRAND_DESCRIPTION_COLUMN = "brand_description"
ACCOUNT_ID_COLUMN = "account_id"
IS_ACTIVE_COLUMN = "is_active"
TARGET_ACCOUNT_ID = 1632040980684380  # The specific account_id to query

BRAND_TAG_DEFINITION_TABLE = "brand_tag_definition"
BRAND_TAG_SUBPARAMETER_COLUMN = "subparameter"
BRAND_TAG_MODEL_PROMPT_COLUMN = "model_prompt"
BRAND_TAG_CREATIVE_TYPE_COLUMN = "creative_type"
BRAND_TAG_IS_SEARCHABLE_COLUMN = "is_searchable"
TARGET_CREATIVE_TYPE = "image"

# AWS API Gateway Configuration
# !!! IMPORTANT: Replace with your actual API Gateway Endpoint URL !!!
# Using the example URL you provided
API_GATEWAY_URL = os.environ.get(
    "API_GATEWAY_URL",
    "https://ldk3nm584i.execute-api.ap-south-1.amazonaws.com/testing/imagedetails",
)

# --- Database Functions ---


def get_urls_from_db(db, table, id_column, url_column):
    """Fetches IDs and URLs from the specified database table and columns."""
    cursor = None
    rows = []
    try:
        # Fetch both the unique ID (image_hash) and the URL (asset_link)
        query = f"SELECT {id_column}, {url_column} FROM {table} WHERE creative_available = true;"
        cursor = db.execute(query)
        rows = cursor.fetchall()

        print(f"Fetched {len(rows)} rows (ID, URL) from the database.")

    except psycopg2.OperationalError as e:
        print(f"Database connection error while fetching URLs and IDs: {e}")
        return None
    except psycopg2.Error as e:
        print(f"Database query error while fetching URLs and IDs: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
            print("Database connection closed after fetching URLs and IDs.")
    # Return a list of tuples (id, url)
    return rows


def get_brand_description(
    db,
    table,
    description_column,
    account_id_column,
    is_active_column,
    target_account_id,
):
    """Fetches the brand_description for a specific account_id that is active."""
    cursor = None
    brand_description = None
    try:
        query = f"SELECT {description_column} FROM {table} WHERE {account_id_column} = %s AND {is_active_column} = TRUE;"

        cursor = db.execute(query, (target_account_id,))
        result = cursor.fetchone()

        if result:
            brand_description = result["brand_description"]
            print(f"Found brand description for account ID {target_account_id}.")
        else:
            print(
                f"No active brand description found for account ID {target_account_id}."
            )

    except psycopg2.OperationalError as e:
        print(f"Database connection error while fetching brand description: {e}")
        return None
    except psycopg2.Error as e:
        print(f"Database query error while fetching brand description: {e}")
        return None
    finally:
        if cursor:
            cursor.close()

            print("Database connection closed after fetching brand description.")
    return brand_description


def get_model_prompt(
    db,
    table,
    subparameter_column,
    model_prompt_column,
    account_id_column,
    creative_type_column,
    is_searchable_column,
    target_account_id,
    target_creative_type,
):
    """
    Fetches and aggregates subparameter and model_prompt into a JSON object
    from brand_tag_definition based on account_id, creative_type, and is_searchable.
    """
    cursor = None
    final_prompt_json = None
    try:
        query = f"""
            SELECT jsonb_object_agg({subparameter_column}, {model_prompt_column}) AS final_prompt_json
            FROM {table}
            WHERE ({account_id_column} IS NULL OR {account_id_column} = %s)
              AND {creative_type_column} = %s
              AND {is_searchable_column} = TRUE
            GROUP BY {account_id_column}, {creative_type_column};
        """

        print(
            f"Executing query for model prompts for account ID {target_account_id}, creative type '{target_creative_type}'..."
        )
        # Pass parameters as a tuple
        cursor = db.execute(
            query,
            (
                target_account_id,
                target_creative_type,
            ),
        )
        print("Query executed successfully.")

        result = cursor.fetchone()
        if result and result.get("final_prompt_json") is not None:
            final_prompt_json = result["final_prompt_json"]

        else:
            print("No model prompt found for the specified criteria.")

    except psycopg2.OperationalError as e:
        print(f"Database connection error while fetching model prompts: {e}")
        return None
    except psycopg2.Error as e:
        print(f"Database query error while fetching model prompts: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while fetching model prompts: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
            print("Database connection closed after fetching model prompts.")
    return final_prompt_json


# --- CORRECTED log_api_error function ---
# Added api_endpoint parameter
def log_api_error(db, error_code, error_message, api_endpoint):
    """Logs API call errors into the log_table, including the endpoint."""
    cursor = None
    try:
        query = """
        INSERT INTO log_table (error_code, error_message, api_endpoint, number_of_attempts, created_at)
        VALUES (%s, %s, %s, 1, NOW());
        """

        # Pass the tuple with 3 elements corresponding to the 3 %s placeholders
        cursor = db.execute(
            query,
            (
                error_code,
                error_message,
                api_endpoint,
            ),
            True,
        )
        print("Successfully logged error.")
        return True

    except psycopg2.OperationalError as e:
        print(f"Database connection error while logging error: {e}")
        return False

    except psycopg2.Error as e:
        print(f"Database error while logging error: {e}")
        return False

    except Exception as e:
        print(f"An unexpected error occurred while logging error: {e}")
        return False

    finally:
        if cursor:
            cursor.close()


# def update_image_creative_with_result(db_params, image_hash, lambda_response_data):
#     """
#     Updates the dim_image_creative table with analysis results from Lambda response.
#     Saves only the 'analysis_result' part into creative_tags.
#     """
#     conn = None
#     cursor = None
#     try:
#         conn = psycopg2.connect(**db_params)
#         cursor = conn.cursor()

#         # Handle cases where 'analysis_result' might not be present (e.g., Lambda returned an error structure)
#         analysis_result_data = lambda_response_data.get("analysis_result")
#         creative_tags_value = analysis_result_data

#         # Update query for dim_image_creative
#         query = """
#         UPDATE dim_image_creative
#         SET creative_tags = %s,
#             creative_tagged = TRUE,
#             updated_at = NOW()
#         WHERE image_hash = %s;
#         """

#         cursor.execute(query, (creative_tags_value, image_hash))
#         conn.commit()
#         print(f"Successfully updated image creative for hash: {image_hash}.")
#         return True

#     except psycopg2.OperationalError as e:
#         print(
#             f"Database connection error while updating image creative for {image_hash}: {e}"
#         )
#         if conn:
#             conn.rollback()
#         return False
#     except psycopg2.Error as e:
#         print(f"Database error while updating image creative for {image_hash}: {e}")
#         if conn:
#             conn.rollback()
#         return False
#     except Exception as e:
#         print(
#             f"An unexpected error occurred while updating image creative for {image_hash}: {e}"
#         )
#         if conn:
#             conn.rollback()
#         return False
#     finally:
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()
#             print("Database connection closed after updating image creative.")


def update_creative_with_result(
    db, table_name, condition_field, condition_value, lambda_response_data
):
    """
    Updates the dim_image_creative table with analysis results from Lambda response.
    Saves only the 'analysis_result' part into creative_tags.
    """
    cursor = None
    try:
        # Handle cases where 'analysis_result' might not be present (e.g., Lambda returned an error structure)
        analysis_result_data = lambda_response_data.get("analysis_result")
        creative_tags_value = analysis_result_data

        # Update query for dim_image_creative
        query = f"""
        UPDATE {table_name}
        SET creative_tags = %s,
            creative_tagged = TRUE,
            updated_at = NOW()
        WHERE {condition_field} = %s;
        """

        cursor = db.execute(
            query,
            (
                creative_tags_value,
                condition_value,
            ),
        )
        print(f"Successfully updated image creative for hash: {condition_value}.")
        return True

    except psycopg2.OperationalError as e:
        print(
            f"Database connection error while updating image creative for {condition_value}: {e}"
        )
        return False

    except psycopg2.Error as e:
        print(
            f"Database error while updating image creative for {condition_value}: {e}"
        )
        return False

    except Exception as e:
        print(
            f"An unexpected error occurred while updating image creative for {condition_value}: {e}"
        )
        return False

    finally:
        if cursor:
            cursor.close()
            print("Database connection closed after updating image creative.")


def creative_tags_lambda_request(db, table_name, id_column_name, url_column_name):
    # --- Retrieve Brand Description ---
    brand_desc = get_brand_description(
        db,
        ACCOUNT_TABLE_NAME,
        BRAND_DESCRIPTION_COLUMN,
        ACCOUNT_ID_COLUMN,
        IS_ACTIVE_COLUMN,
        TARGET_ACCOUNT_ID,
    )

    if not brand_desc:
        print(
            f"Could not retrieve brand description for Account ID {TARGET_ACCOUNT_ID}."
        )

    # --- Retrieve Model Prompt JSON ---
    model_prompt_json = get_model_prompt(
        db,
        BRAND_TAG_DEFINITION_TABLE,
        BRAND_TAG_SUBPARAMETER_COLUMN,
        BRAND_TAG_MODEL_PROMPT_COLUMN,
        ACCOUNT_ID_COLUMN,
        BRAND_TAG_CREATIVE_TYPE_COLUMN,
        BRAND_TAG_IS_SEARCHABLE_COLUMN,
        TARGET_ACCOUNT_ID,
        TARGET_CREATIVE_TYPE,
    )

    if not model_prompt_json:
        print("Could not retrieve model prompt JSON. Cannot proceed with sending data.")
        return

    # --- Fetch URLs and IDs from the database ---
    db_rows = get_urls_from_db(db, table_name, id_column_name, url_column_name)

    if db_rows is None:
        print("Failed to fetch URLs and IDs from the database. Exiting.")
        return

    total_rows = len(db_rows)
    skipped_null_empty = 0
    sent_to_api = 0
    api_send_failed = 0
    db_save_successful = 0
    db_save_failed = 0
    db_log_successful = 0
    db_log_failed = 0

    print(
        f"Processing {total_rows} image rows to send data to API Gateway and save results..."
    )

    # Iterate through the fetched rows and send data to API Gateway
    for i, row in enumerate(db_rows):
        # Unpack both image_hash (row_id) and url
        row_id = row["image_hash"] if row and row.get("image_hash") else None
        url = row["asset_link"] if row and row.get("asset_link") else None

        if row_id is not None and url and isinstance(url, str) and url.strip():
            clean_url = url.strip()

            # Include asset_link, brand_description, and model_prompt
            payload = {
                "asset_link": clean_url,
                "brand_description": brand_desc,
                "model_prompt": model_prompt_json,
            }

            try:
                # Call the Lambda function via API Gateway
                response = requests.post(API_GATEWAY_URL, json=payload, timeout=180)
                response.raise_for_status()

                print(
                    f"Successfully sent data to API Gateway. Status Code: {response.status_code}"
                )
                sent_to_api += 1

                # --- Access the Lambda's return value and save to DB ---
                try:
                    lambda_response_data = response.json()

                    # Save the analysis result to the database
                    save_success = update_creative_with_result(
                        db,
                        table_name,
                        id_column_name,
                        row_id,
                        lambda_response_data,
                    )

                    if save_success:
                        db_save_successful += 1
                    else:
                        db_save_failed += 1
                        print(f"Failed to save analysis result to DB for : {row_id}")

                except json.JSONDecodeError:
                    print(f"Received non-JSON response from Lambda: {response.text}")
                    log_success = log_api_error(
                        db,
                        422,
                        f"Non-JSON response: {response.text}",
                        API_GATEWAY_URL,
                    )
                    if log_success:
                        db_log_successful += 1
                    else:
                        db_log_failed += 1

                except Exception as e:
                    print(
                        f"Error processing or saving Lambda response for row ID {row_id}: {e}"
                    )
                    log_success = log_api_error(
                        db,
                        500,
                        f"Error processing Lambda response: {e}",
                        API_GATEWAY_URL,
                    )
                    if log_success:
                        db_log_successful += 1
                    else:
                        db_log_failed += 1

            except HTTPError as e:
                print(
                    f"HTTP error occurred while sending data for row ID {row_id}: {e}"
                )
                api_send_failed += 1
                error_code = e.response.status_code if e.response is not None else 0

                log_success = log_api_error(db, error_code, str(e), API_GATEWAY_URL)
                if log_success:
                    db_log_successful += 1
                else:
                    db_log_failed += 1

            except RequestException as e:
                print(
                    f"Request error occurred while sending data for row ID {row_id}: {e}"
                )
                api_send_failed += 1
                # Log the request error
                # Pass descriptive error message and the API endpoint URL
                log_success = log_api_error(db, 999, str(e), API_GATEWAY_URL)
                if log_success:
                    db_log_successful += 1
                else:
                    db_log_failed += 1

            except Exception as e:
                print(
                    f"An unexpected error occurred while processing row ID {row_id}: {e}"
                )
                api_send_failed += 1
                # Log the unexpected error
                # Pass descriptive error message and the API endpoint URL
                log_success = log_api_error(
                    db, 500, f"Unexpected processing error: {e}", API_GATEWAY_URL
                )
                if log_success:
                    db_log_successful += 1
                else:
                    db_log_failed += 1

        else:
            # row_id is None, or URL is null, not a string, or empty
            skipped_null_empty += 1
            print(f"Skipping row due to missing ID or empty/invalid URL: {row}")

    return {
        "row_fetched_from_db": total_rows,
        "skipped_null_empty": skipped_null_empty,
        "sent_to_api": sent_to_api,
        "api_send_failed": api_send_failed,
        "db_save_successful": db_save_successful,
        "db_save_failed": db_save_failed,
        "db_log_successful": db_log_successful,
        "db_log_failed": db_log_failed,
    }
