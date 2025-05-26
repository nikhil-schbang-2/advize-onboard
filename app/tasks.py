import requests
from app.s3_cron import creative_tags_lambda_request
from app.video_fetcher import get_main_image_from_facebook_iframe
from app.config import (
    IMAGE_TABLE,
    IMAGE_ID_COLUMN,
    IMAGE_URL_COLUMN,
    VIDEO_ID_COLUMN,
    VIDEO_TABLE,
    VIDEO_URL_COLUMN,
)

# from database.session import SessionLocal
from sqlalchemy.orm import Session
import sys
import time
import os
import json
from typing import List, Dict, Any, Optional, Tuple, Set
from sqlalchemy import text
import uuid
import re
import asyncio
import boto3
import io
from botocore.exceptions import ClientError
from app.celery_app import celery
from database.db import Database

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, BASE_DIR)

from app.config import META_API_KEY, META_AD_ACCOUNT_ID, S3_BUCKET_NAME
from app.video_fetcher import get_video_from_facebook_iframe


# celery = Celery(
#     "tasks",
#     broker="redis://localhost:6379/0",
#     backend="redis://localhost:6739/0"
# )


# Optional: Keep log_error if you plan to uncomment and use it later
# def log_error(db: Session, error_code: int, error_message: str, api_endpoint: str, attempts: int):
#     """Log errors to the log_table."""
#     try:
#         query = text("""
#             INSERT INTO log_table (error_code, error_message, api_endpoint, number_of_attempts)
#             VALUES (:error_code, :error_message, :api_endpoint, :attempts)
#         """)
#         db.execute(query, {
#             "error_code": error_code,
#             "error_message": str(error_message),
#             "api_endpoint": api_endpoint,
#             "attempts": attempts
#         })
#         db.commit()
#     except Exception as e:
#         print(f"Failed to log error: {e}")
#         db.rollback()


def determine_creative_type(creative_data: Dict[str, Any]) -> str:
    """
    Determines the creative type based on the creative data.
    """
    if not creative_data:
        return "unknown"

    if "video_id" in creative_data:
        return "video"

    if "image_hash" in creative_data:
        return "image"

    # Prioritize DPA check
    if "name" in creative_data and creative_data["name"].startswith("{{product.name}}"):
        return "DPA"

    # Check for object_story_spec
    object_story_spec = creative_data.get("object_story_spec")
    if object_story_spec:
        if (
            "link_data" in object_story_spec
            and "child_attachments" in object_story_spec["link_data"]
            and isinstance(object_story_spec["link_data"]["child_attachments"], list)
        ):
            return "carousel"
        if (
            "template_data" in object_story_spec
            and "child_attachments" in object_story_spec["template_data"]
            and isinstance(
                object_story_spec["template_data"]["child_attachments"], list
            )
        ):
            return "carousel"

    return "unknown"


def fetch_image_urls_batch(hashes_list: List[str], account_id: int) -> Dict[str, str]:
    """
    Fetches permalink URLs for a batch of image hashes from Meta API.
    Handles pagination within the batch response and implements exponential backoff.
    Returns a dictionary {hash: url}.
    """
    if not hashes_list:
        return {}

    request_batch_size = 50
    results = {}
    api_endpoint = f"https://graph.facebook.com/v22.0/act_{account_id}/adimages"

    headers = {
        "Authorization": f"Bearer {META_API_KEY}",
        "Content-Type": "application/json",
    }

    for i in range(0, len(hashes_list), request_batch_size):
        batch_hashes = hashes_list[i : i + request_batch_size]
        hashes_param = json.dumps(batch_hashes)

        params = {
            "fields": "hash,url",
            "hashes": hashes_param,
            "limit": 50,
        }

        print(f"Fetching batch of {len(batch_hashes)} image URLs (initial request)...")

        next_page_url = api_endpoint
        current_params = params
        batch_page_count = 0
        batch_data_for_request = []

        max_retries = 5
        initial_delay = 60
        max_delay = 3600

        while next_page_url:
            batch_page_count += 1
            print(f"  Fetching page {batch_page_count} for current batch...")

            delay = initial_delay
            response = None
            last_error = None

            for attempt in range(max_retries):
                try:
                    print(
                        f"  Page {batch_page_count}, Attempt {attempt + 1}/{max_retries}: Requesting {next_page_url}..."
                    )
                    if current_params:
                        response = requests.get(
                            next_page_url,
                            headers=headers,
                            params=current_params,
                            timeout=60,
                        )
                    else:
                        response = requests.get(
                            next_page_url, headers=headers, timeout=60
                        )

                    response.raise_for_status()
                    print(f"  Page {batch_page_count}, Attempt {attempt + 1}: Success.")
                    last_error = None
                    break
                except requests.exceptions.Timeout as e:
                    print(
                        f"  Page {batch_page_count}, Attempt {attempt + 1} timed out. Error: {e}"
                    )
                    last_error = e
                except requests.exceptions.RequestException as e:
                    print(
                        f"  Page {batch_page_count}, Attempt {attempt + 1} failed. Status: {e.response.status_code if e.response else 'N/A'}. Error: {e}"
                    )
                    last_error = e
                    if e.response is not None and e.response.status_code == 429:
                        print("  Rate limit hit during batch fetch page.")
                except Exception as e:
                    print(
                        f"  Page {batch_page_count}, Attempt {attempt + 1} encountered unexpected error: {e}"
                    )
                    last_error = e

                if attempt < max_retries - 1:
                    print(
                        f"  Retrying page {batch_page_count} in {min(delay, max_delay) // 60} minutes ({min(delay, max_delay)} seconds)..."
                    )
                    time.sleep(min(delay, max_delay))
                    delay *= 2
                else:
                    print(
                        f"  Page {batch_page_count}: Max retries reached. Failed to fetch page after multiple attempts. Last error: {last_error}"
                    )
                    next_page_url = None
                    break

            if response and response.status_code == 200:
                try:
                    response_json = response.json()
                    page_data = response_json.get("data", [])
                    batch_data_for_request.extend(page_data)

                    paging_info = response_json.get("paging")
                    if paging_info and "next" in paging_info:
                        next_page_url = paging_info["next"]
                        current_params = None
                    else:
                        next_page_url = None

                except json.JSONDecodeError as e:
                    print(
                        f"  Failed to decode JSON response for page {batch_page_count} of batch: {e}"
                    )
                    next_page_url = None
                except Exception as e:
                    print(
                        f"  Error processing response data for page {batch_page_count} of batch: {e}"
                    )
                    next_page_url = None
            else:
                next_page_url = None

        print(
            f"Finished fetching pages for current batch. Processing {len(batch_data_for_request)} items."
        )
        for item in batch_data_for_request:
            if "hash" in item and "url" in item:
                results[item["hash"]] = item["url"]
            else:
                print(f"Warning: Item in batch response missing hash or url: {item}")

    print(
        f"Batch fetch completed for all batches. Retrieved URLs for {len(results)} hashes."
    )
    return results


# --- Download image from URL and upload to S3 ---
def download_and_upload_image(
    image_hash: str, meta_url: str, ad_id: int, bucket_name: str
) -> Optional[str]:
    """
    Downloads an image from a Meta URL and uploads it to a specified S3 bucket.
    Includes a check to see if the image already exists in S3 before downloading/uploading.
    """
    if not meta_url or not bucket_name or not image_hash:
        print("Missing meta_url, bucket_name, or image_hash for S3 upload.")
        return None

    s3 = boto3.client("s3")
    s3_key_base = f"meta-creatives/images/{image_hash}"

    # --- Check if object exists in S3 before downloading/uploading ---
    try:
        possible_extensions = ["jpg", "jpeg", "png", "gif"]
        found_existing_key = None

        for ext in possible_extensions:
            s3_key_to_check = f"{s3_key_base}.{ext}"
            try:
                s3.head_object(Bucket=bucket_name, Key=s3_key_to_check)
                print(
                    f"Image with key {s3_key_to_check} already exists in S3. Skipping download/upload."
                )
                found_existing_key = s3_key_to_check
                break
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    continue
                else:
                    print(
                        f"An S3 error occurred while checking for object existence with key {s3_key_to_check}: {e}"
                    )
                    break
            except Exception as e:
                print(
                    f"An unexpected error occurred during S3 existence check for key {s3_key_to_check}: {e}"
                )
                break

        if found_existing_key:
            s3_location = s3.get_bucket_location(Bucket=bucket_name)[
                "LocationConstraint"
            ]
            if s3_location is None:
                s3_url = f"https://{bucket_name}.s3.amazonaws.com/{found_existing_key}"
            else:
                s3_url = f"https://{bucket_name}.s3-{s3_location}.amazonaws.com/{found_existing_key}"
            return s3_url

    except Exception as e:
        print(f"An error occurred during the S3 existence check loop: {e}")

    # --- If object was not found in S3, proceed with download and upload ---
    try:
        print(f"Attempting to download image from: {meta_url}")
        response = requests.get(meta_url, stream=True, timeout=30)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type")
        extension = "jpg"
        if content_type and "image/" in content_type:
            extension = content_type.split("/")[-1].lower()
            if extension == "jpeg":
                extension = "jpg"

        s3_key_with_extension = f"{s3_key_base}.{extension}"

        print(f"Downloading image content for hash: {image_hash}")
        image_data = io.BytesIO(response.content)

        print(
            f"Uploading image {image_hash} to S3 bucket '{bucket_name}' with key '{s3_key_with_extension}'"
        )
        s3.upload_fileobj(image_data, bucket_name, s3_key_with_extension)

        s3_location = s3.get_bucket_location(Bucket=bucket_name)["LocationConstraint"]
        if s3_location is None:
            s3_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key_with_extension}"
        else:
            s3_url = f"https://{bucket_name}.s3-{s3_location}.amazonaws.com/{s3_key_with_extension}"

        print(f"Successfully uploaded {image_hash}.S3 URL: {s3_url}")
        return s3_url

    except requests.exceptions.RequestException as e:
        print(f"Error downloading image {image_hash} from {meta_url}: {e}")

    except boto3.exceptions.S3UploadFailedError as e:
        print(f"Error uploading image {image_hash} to S3: {e}")

    except Exception as e:
        print(
            f"An unexpected error occurred during image download/upload for {image_hash}: {e}"
        )

    print("Failed to upload image to S3, Fetching from the Iframe")
    if ad_id:
        print(
            f"Source not found for Image {image_hash} via direct fetch. Attempting preview endpoint fallback using associated ad IDs."
        )
        meta_asset_link = None
        preview_formats = [
            "MOBILE_FEED_STANDARD",
            "INSTAGRAM_STANDARD",
            "DESKTOP_FEED_STANDARD",
        ]

        max_retries_preview = 5
        initial_delay_preview = 60
        max_delay_preview = 3600

        print(f"  Trying preview fallback for ad ID: {ad_id}")
        preview_base_url = f"https://graph.facebook.com/v22.0/{ad_id}/previews"

        for format in preview_formats:
            print(f"Trying preview format: {format} for ad ID {ad_id}")
            preview_params = {"ad_format": format, "fields": "body"}

            delay_preview = initial_delay_preview
            response_preview = None
            last_error_preview = None

            for attempt_preview in range(max_retries_preview):
                try:
                    print(
                        f"Preview fetch Attempt {attempt_preview + 1}/{max_retries_preview} for format {format}..."
                    )
                    response_preview = requests.get(
                        preview_base_url,
                        headers={
                            "Authorization": f"Bearer {META_API_KEY}",
                            "Content-Type": "application/json",
                        },
                        params=preview_params,
                        timeout=60,
                    )
                    response_preview.raise_for_status()
                    print(f"Preview fetch Attempt {attempt_preview + 1}: Success.")
                    last_error_preview = None

                    preview_data = response_preview.json().get("data")
                    print(f"==>> preview_data: {preview_data}")
                    if (
                        preview_data
                        and isinstance(preview_data, list)
                        and len(preview_data) > 0
                    ):
                        body = preview_data[0].get("body", "")
                        match = re.search(r'src=["\'](.*?)["\']', body)

                        if extracted_url := match.group(1):
                            decoded_url = extracted_url.replace("&amp;", "&")
                            print(
                                f"Successfully extracted URL from preview for format {format} using ad {ad_id}"
                            )
                            meta_asset_link = decoded_url
                            print(f"==>> meta_asset_link: {meta_asset_link}")

                            break
                    break

                except requests.exceptions.Timeout as e:
                    print(
                        f"Preview fetch Attempt {attempt_preview + 1} timed out. Error: {e}"
                    )
                    last_error_preview = e
                except requests.exceptions.RequestException as e:
                    print(
                        f"Preview fetch Attempt {attempt_preview + 1} failed. Status: {e.response_preview.status_code if e.response_preview else 'N/A'}. Error: {e}"
                    )
                    last_error_preview = e
                    if (
                        e.response_preview is not None
                        and e.response_preview.status_code == 429
                    ):
                        print("Rate limit hit during preview fetch.")
                except Exception as e:
                    print(
                        f"Preview fetch Attempt {attempt_preview + 1} encountered unexpected error: {e}"
                    )
                    last_error_preview = e

                if attempt_preview < max_retries_preview - 1:
                    print(
                        f"Retrying preview fetch in {min(delay_preview, max_delay_preview)} seconds..."
                    )
                    time.sleep(min(delay_preview, max_delay_preview))
                    delay_preview *= 2
                else:
                    print(
                        f"      Max retries reached for preview format {format} using ad {ad_id}. Last error: {last_error_preview}"
                    )

        if not meta_asset_link:
            print(
                f"Failed to find asset link for image {image_hash} using preview fallback."
            )
            return None

        try:
            final_image_url = asyncio.run(
                get_main_image_from_facebook_iframe(meta_asset_link)
            )
            if not final_image_url:
                print(
                    f"Failed to extract main image URL from preview iframe for image {image_hash}."
                )
                return None

            print(f"Attempting to download image from: {meta_url}")
            response = requests.get(final_image_url, stream=True, timeout=30)
            response.raise_for_status()

            content_type = response.headers.get("Content-Type")
            extension = "jpg"
            if content_type and "image/" in content_type:
                extension = content_type.split("/")[-1].lower()
                if extension == "jpeg":
                    extension = "jpg"

            s3_key_with_extension = f"{s3_key_base}.{extension}"

            print(f"Downloading image content for hash: {image_hash}")
            image_data = io.BytesIO(response.content)

            print(
                f"Uploading image {image_hash} to S3 bucket '{bucket_name}' with key '{s3_key_with_extension}'"
            )
            s3.upload_fileobj(image_data, bucket_name, s3_key_with_extension)

            s3_location = s3.get_bucket_location(Bucket=bucket_name)[
                "LocationConstraint"
            ]
            if s3_location is None:
                s3_url = (
                    f"https://{bucket_name}.s3.amazonaws.com/{s3_key_with_extension}"
                )
            else:
                s3_url = f"https://{bucket_name}.s3-{s3_location}.amazonaws.com/{s3_key_with_extension}"

            print(f"Successfully uploaded {image_hash}.S3 URL: {s3_url}")
            return s3_url

        except requests.exceptions.RequestException as e:
            print(f"Error downloading image {image_hash} from {meta_url}: {e}")
            return None
        except boto3.exceptions.S3UploadFailedError as e:
            print(f"Error uploading image {image_hash} to S3: {e}")
            return None
        except Exception as e:
            print(
                f"An unexpected error occurred during image download/upload for {image_hash}: {e}"
            )
            return None


# --- Download video from URL and upload to S3 ---
async def download_and_upload_video(
    video_id: int, video_url: str, bucket_name: str
) -> Optional[str]:
    """
    Downloads a video from a given URL and uploads it to a specified S3 bucket.
    Handles both direct video URLs and preview iframe URLs using Playwright for the latter.
    Includes a check to see if the video already exists in S3 before downloading/uploading.
    """
    if not video_url or not bucket_name or not video_id:
        print("Missing video_url, bucket_name, or video_id for S3 upload.")
        return None

    s3 = boto3.client("s3")
    s3_key_base = f"meta-creatives/videos/{video_id}"

    # --- Check if object exists in S3 before downloading/uploading ---
    try:
        possible_extensions = ["mp4", "mov", "avi", "wmv", "flv", "webm"]
        found_existing_key = None

        for ext in possible_extensions:
            s3_key_to_check = f"{s3_key_base}.{ext}"
            try:
                s3.head_object(Bucket=bucket_name, Key=s3_key_to_check)
                print(
                    f"Video with key {s3_key_to_check} already exists in S3. Skipping download/upload."
                )
                found_existing_key = s3_key_to_check
                break
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    continue
                else:
                    print(
                        f"An S3 error occurred while checking for video object existence with key {s3_key_to_check}: {e}"
                    )
                    break
            except Exception as e:
                print(
                    f"An unexpected error occurred during S3 existence check for video key {s3_key_to_check}: {e}"
                )
                break

        if found_existing_key:
            s3_location = s3.get_bucket_location(Bucket=bucket_name)[
                "LocationConstraint"
            ]
            if s3_location is None:
                s3_url = f"https://{bucket_name}.s3.amazonaws.com/{found_existing_key}"
            else:
                s3_url = f"https://{bucket_name}.s3-{s3_location}.amazonaws.com/{found_existing_key}"
            return s3_url

    except Exception as e:
        print(
            f"An error occurred during the S3 existence check loop for video {video_id}: {e}"
        )

    # --- If video was not found in S3, proceed with download and upload ---
    final_download_url = None
    extension = None

    if "preview_iframe.php" in video_url:
        print(
            f"Detected preview iframe URL for video {video_id}. Using Playwright to extract video URL."
        )
        try:
            final_download_url = await get_video_from_facebook_iframe(video_url)
            if final_download_url:
                print(
                    f"Successfully extracted video URL using Playwright for video {video_id}: {final_download_url}"
                )
            else:
                print(
                    f"Playwright failed to extract video URL from preview iframe for video {video_id}."
                )

        except Exception as e:
            print(
                f"An error occurred while using Playwright to fetch video URL for video {video_id}: {e}"
            )

    else:
        final_download_url = video_url
        print(f"Assuming direct video URL for video {video_id}: {final_download_url}")

    if not final_download_url:
        print(f"No valid download URL determined for video {video_id}.")
        return None

    try:
        print(f"Attempting to download video content from: {final_download_url}")
        response_video = requests.get(final_download_url, stream=True, timeout=120)
        response_video.raise_for_status()

        content_type = response_video.headers.get("Content-Type")
        if content_type and "video/" in content_type:
            extension = content_type.split("/")[-1].lower()
        if not extension:
            path = requests.utils.urlparse(final_download_url).path
            ext_match = re.search(r"\.([a-zA-Z0-9]+)$", path)
            if ext_match:
                extension = ext_match.group(1).lower()

        if not extension:
            extension = "mp4"

        s3_key_with_extension = f"{s3_key_base}.{extension}"

        print(f"Downloading video content for video ID: {video_id}")
        video_data = io.BytesIO()
        for chunk in response_video.iter_content(chunk_size=8192):
            video_data.write(chunk)
        video_data.seek(0)

        print(
            f"Uploading video {video_id} to S3 bucket '{bucket_name}' with key '{s3_key_with_extension}'"
        )
        s3.upload_fileobj(video_data, bucket_name, s3_key_with_extension)

        s3_location = s3.get_bucket_location(Bucket=bucket_name)["LocationConstraint"]
        if s3_location is None:
            s3_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key_with_extension}"
        else:
            s3_url = f"https://{bucket_name}.s3-{s3_location}.amazonaws.com/{s3_key_with_extension}"

        print(f"Successfully uploaded video {video_id}. S3 URL: {s3_url}")
        return s3_url

    except requests.exceptions.RequestException as e:
        print(
            f"Error downloading video content for {video_id} from {final_download_url}: {e}"
        )
        return None
    except boto3.exceptions.S3UploadFailedError as e:
        print(f"Error uploading video {video_id} to S3: {e}")
        return None
    except Exception as e:
        print(
            f"An unexpected error occurred during video download/upload for {video_id}: {e}"
        )
        return None


def process_image_creative(
    db: Database,
    image_hash: str,
    asset_link: Optional[str] = None,
    creative_tags_data: Optional[List[str]] = None,
    account_id: int = None,
) -> Optional[str]:
    """
    Process image creative and store/update in dim_image_creative.
    Takes image_hash and the final asset_link (either initial or fetched/S3 URL).
    Handles insertion if new or updating asset_link if it was previously missing.
    """
    if not image_hash:
        print("No image hash provided for creative processing.")
        return None

    creative_tags = json.dumps(creative_tags_data) if creative_tags_data else "{}"
    creative_available = bool(asset_link)
    creative_tagged = bool(creative_tags_data)

    # Upsert approach
    upsert_query_simple = """
        INSERT INTO dim_image_creative
        (image_hash, account_id, asset_link, creative_tags, creative_available, creative_tagged)
        VALUES (%s, %s, %s, %s::jsonb, %s, %s)
        ON CONFLICT (image_hash) DO UPDATE SET
            account_id = EXCLUDED.account_id,
            asset_link = COALESCE(EXCLUDED.asset_link, dim_image_creative.asset_link),
            creative_available = COALESCE(EXCLUDED.creative_available, dim_image_creative.creative_available),
            creative_tags = COALESCE(EXCLUDED.creative_tags, dim_image_creative.creative_tags),
            creative_tagged = COALESCE(EXCLUDED.creative_tagged, dim_image_creative.creative_tagged)
    """

    try:
        db.execute(
            upsert_query_simple,
            (
                image_hash,
                int(account_id),
                asset_link,
                creative_tags,
                creative_available,
                creative_tagged,
            ),
        )
        # if creative_available and not creative_tagged:
        #     # Call lambda function to tag the image
        #     lambda_gateway_response = creative_tags_lambda_request(
        #         db, IMAGE_TABLE, IMAGE_ID_COLUMN, IMAGE_URL_COLUMN
        #     )
        #     print("lambda_gateway_response", lambda_gateway_response)
        return image_hash
    except Exception as e:
        print(f"Error processing image creative with hash {image_hash}: {e}")
        return None


async def process_video_creative(
    db,
    video_id: int,
    ad_ids: Set[int],
    account_id: int,
    creative_tags_data: Optional[List[str]] = None,
) -> Optional[int]:
    """
    Process video creative, fetch source URL (with fallback), download video,
    upload to S3, and store/update in dim_video_creative.
    """
    if not video_id:
        print("No video ID provided for creative processing.")
        return None

    try:
        video_id = int(video_id)
    except (ValueError, TypeError):
        print(f"Invalid video ID format: {video_id}")
        return None

    meta_asset_link = None
    s3_asset_link = None
    video_source_type = "Unknown"

    check_query = """SELECT asset_link FROM dim_video_creative WHERE video_id = %s"""
    existing_s3_link = db.execute(check_query, (video_id,)).fetchone()

    if existing_s3_link:
        print(
            f"Existing S3 asset link found for video ID {video_id}. Skipping Meta API fetch and S3 upload."
        )
        s3_asset_link = existing_s3_link["asset_link"]
        video_source_type = "Database"
        print(f"Video {video_id} asset link obtained from {video_source_type}.")

    else:
        # --- Attempt 1: Fetch source URL directly from video ID ---
        print(f"Attempting to fetch source URL for video ID: {video_id}")
        headers = {
            "Authorization": f"Bearer {META_API_KEY}",
            "Content-Type": "application/json",
        }
        video_url_endpoint = (
            f"https://graph.facebook.com/v22.0/{video_id}?fields=source"
        )

        max_retries = 3
        initial_delay = 10
        max_delay = 300
        delay = initial_delay
        response = None
        last_error = None

        for attempt in range(max_retries):
            try:
                print(
                    f"  Source fetch Attempt {attempt + 1}/{max_retries}: Fetching video source for {video_id}..."
                )
                response = requests.get(video_url_endpoint, headers=headers, timeout=30)
                response.raise_for_status()
                print(f"  Source fetch Attempt {attempt + 1}: Success.")
                meta_asset_link = response.json().get("source")
                if meta_asset_link:
                    print(
                        f"Successfully fetched source for video {video_id} from direct endpoint."
                    )
                    video_source_type = "Direct Source"
                    print(
                        f"Video {video_id} asset link obtained from {video_source_type}."
                    )
                else:
                    print(
                        f"Source field empty or null for video {video_id}. Trying fallback."
                    )
                last_error = None
                break
            except requests.exceptions.Timeout as e:
                print(f"  Source fetch Attempt {attempt + 1} timed out. Error: {e}")
                last_error = e
            except requests.exceptions.RequestException as e:
                print(
                    f"  Source fetch Attempt {attempt + 1} failed. Status: {e.response.status_code if e.response else 'N/A'}. Error: {e}"
                )
                last_error = e
                if e.response is not None and e.response.status_code == 429:
                    print("  Rate limit hit during video source fetch.")
            except Exception as e:
                print(
                    f"  Source fetch Attempt {attempt + 1} encountered unexpected error: {e}"
                )
                last_error = e

            if attempt < max_retries - 1:
                print(f"  Retrying source fetch in {min(delay, max_delay)} seconds...")
                time.sleep(min(delay, max_delay))
                delay *= 2
            else:
                print(
                    f"  Max retries reached for source fetch. Last error: {last_error}"
                )

        # --- Attempt 2: Fallback to Preview Endpoint if source is not available ---
        if not meta_asset_link and ad_ids:
            print(f"==>> ad_ids from iframe: {ad_ids}  ==> video_id: {video_id}")

            preview_formats = [
                "MOBILE_FEED_STANDARD",
                "INSTAGRAM_STANDARD",
                "DESKTOP_FEED_STANDARD",
            ]

            max_retries_preview = 5
            initial_delay_preview = 60
            max_delay_preview = 3600

            for ad_id in ad_ids:
                if meta_asset_link:
                    break

                print(f"  Trying preview fallback for ad ID: {ad_id}")
                preview_base_url = f"https://graph.facebook.com/v22.0/{ad_id}/previews"

                for format in preview_formats:
                    if meta_asset_link:
                        break

                    print(f"Trying preview format: {format} for ad ID {ad_id}")
                    preview_params = {"ad_format": format, "fields": "body"}

                    delay_preview = initial_delay_preview
                    response_preview = None
                    last_error_preview = None

                    for attempt_preview in range(max_retries_preview):
                        try:
                            print(
                                f"Preview fetch Attempt {attempt_preview + 1}/{max_retries_preview} for format {format}..."
                            )
                            response_preview = requests.get(
                                preview_base_url,
                                headers=headers,
                                params=preview_params,
                                timeout=60,
                            )
                            response_preview.raise_for_status()
                            print(
                                f"Preview fetch Attempt {attempt_preview + 1}: Success."
                            )
                            last_error_preview = None

                            preview_data = response_preview.json().get("data")
                            print(f"==>> preview_data: {preview_data}")
                            if (
                                preview_data
                                and isinstance(preview_data, list)
                                and len(preview_data) > 0
                            ):
                                body = preview_data[0].get("body", "")
                                match = re.search(r'src=["\'](.*?)["\']', body)
                                if match:
                                    extracted_url = match.group(1)
                                    if extracted_url:
                                        decoded_url = extracted_url.replace(
                                            "&amp;", "&"
                                        )

                                        meta_asset_link = decoded_url
                                        print(
                                            f"==>> meta_asset_link: {meta_asset_link}"
                                        )
                                        video_source_type = (
                                            f"Preview Fallback ({format})"
                                        )
                                        print(
                                            f"Video {video_id} asset link obtained from {video_source_type}."
                                        )
                                        break
                                    else:
                                        print(
                                            f"Extracted empty URL from preview body for format {format} using ad {ad_id}. Trying next format."
                                        )
                                else:
                                    print(
                                        f"No iframe src found in preview body for format {format} using ad {ad_id}. Trying next format."
                                    )
                            else:
                                print(
                                    f"No data or empty data array in preview response for format {format} using ad {ad_id}. Trying next format."
                                )

                            break

                        except requests.exceptions.Timeout as e:
                            print(
                                f"Preview fetch Attempt {attempt_preview + 1} timed out. Error: {e}"
                            )
                            last_error_preview = e
                        except requests.exceptions.RequestException as e:
                            print(
                                f"Preview fetch Attempt {attempt_preview + 1} failed. Status: {e.response_preview.status_code if e.response_preview else 'N/A'}. Error: {e}"
                            )
                            last_error_preview = e
                            if (
                                e.response_preview is not None
                                and e.response_preview.status_code == 429
                            ):
                                print("Rate limit hit during preview fetch.")
                        except Exception as e:
                            print(
                                f"Preview fetch Attempt {attempt_preview + 1} encountered unexpected error: {e}"
                            )
                            last_error_preview = e

                        if attempt_preview < max_retries_preview - 1:
                            print(
                                f"Retrying preview fetch in {min(delay_preview, max_delay_preview)} seconds..."
                            )
                            time.sleep(min(delay_preview, max_delay_preview))
                            delay_preview *= 2
                        else:
                            print(
                                f"Max retries reached for preview format {format} using ad {ad_id}. Last error: {last_error_preview}"
                            )

            if not meta_asset_link:
                print(
                    f"Failed to find asset link for video {video_id} after trying source and all preview formats/associated ads."
                )
                video_source_type = "Failed Fetch"

    # --- Download and Upload Video to S3 if a Meta asset link was found and no existing S3 link ---
    if meta_asset_link and not existing_s3_link:
        print(
            f"Meta asset link found for video {video_id}. Attempting download and upload to S3."
        )
        s3_asset_link = await download_and_upload_video(
            video_id, meta_asset_link, S3_BUCKET_NAME
        )
        if s3_asset_link:
            print(f"Video {video_id} successfully uploaded to S3.")
        else:
            print(f"Failed to download or upload video {video_id} to S3.")

    creative_tags = json.dumps(creative_tags_data) if creative_tags_data else "{}"

    # Upsert for dim_video_creative
    upsert_query = """
        INSERT INTO dim_video_creative
        (video_id, account_id, asset_link, creative_tags, creative_available, creative_tagged)
        VALUES (%s, %s, %s, %s::jsonb, %s, %s)
        ON CONFLICT (video_id) DO UPDATE SET
            account_id = EXCLUDED.account_id,
            asset_link = COALESCE(EXCLUDED.asset_link, dim_video_creative.asset_link),
            creative_available = COALESCE(EXCLUDED.creative_available, dim_video_creative.creative_available),
            creative_tags = COALESCE(EXCLUDED.creative_tags, dim_video_creative.creative_tags),
            creative_tagged = COALESCE(EXCLUDED.creative_tagged, dim_video_creative.creative_tagged)
        """

    try:
        db.execute(
            upsert_query,
            (
                video_id,
                int(account_id),
                s3_asset_link,
                creative_tags,
                bool(s3_asset_link),
                bool(creative_tags_data),
            ),
        )
        # if bool(s3_asset_link) and not bool(creative_tags_data):
        #     # Call lambda function to tag the image
        #     lambda_gateway_response = creative_tags_lambda_request(
        #         VIDEO_TABLE, VIDEO_ID_COLUMN, VIDEO_URL_COLUMN
        #     )
        #     print("lambda_gateway_response", lambda_gateway_response)
        print(
            f"Processed video creative with ID: {video_id}. Final S3 Asset Link Found: {bool(s3_asset_link)}. Source Type: {video_source_type}"
        )
        return video_id
    except Exception as e:
        print(f"Error processing video creative {video_id}: {e}")
        return None


def process_carousel_creative(
    db: Database, ad_id: int, creative_data: Dict[str, Any], account_id: int
) -> bool:
    """
    Process carousel creative and store cards in dim_creative_card.
    Returns True if processing completes without database errors during card insertion,
    False otherwise. Skips cards missing asset IDs without returning False.
    """

    object_story_spec = creative_data.get("object_story_spec", {})
    child_attachments = []

    if "link_data" in object_story_spec:
        link_data = object_story_spec["link_data"]
        if "child_attachments" in link_data and isinstance(
            link_data["child_attachments"], list
        ):
            child_attachments = link_data["child_attachments"]
    elif "template_data" in object_story_spec:
        template_data = object_story_spec["template_data"]
        if "child_attachments" in template_data and isinstance(
            template_data["child_attachments"], list
        ):
            child_attachments = template_data["child_attachments"]

    if not child_attachments:
        return True

    success = True

    for idx, card in enumerate(child_attachments, 1):
        card_type = None
        image_hash = None
        video_id = None
        card_asset_processed_id = None

        if card.get("image_hash"):
            card_type = "image"
            image_hash = card.get("image_hash")
            check_asset_query = (
                """SELECT 1 FROM dim_image_creative WHERE image_hash = %s"""
            )
            if db.execute(check_asset_query, (image_hash,)).fetchone():
                card_asset_processed_id = image_hash
            else:
                print(
                    f"Warning: Image asset {image_hash} for carousel card {idx} on ad {ad_id} not found in dim_image_creative. Skipping card."
                )
                continue

        elif card.get("video_id"):
            card_type = "video"
            try:
                video_id = int(card.get("video_id"))
            except (ValueError, TypeError):
                print(
                    f"Warning: Invalid video ID format for carousel card {idx} on ad {ad_id}: {card.get('video_id')}. Skipping card."
                )
                continue

            check_asset_query = (
                """SELECT 1 FROM dim_video_creative WHERE video_id = %s"""
            )
            if db.execute(check_asset_query, (video_id,)).fetchone():
                card_asset_processed_id = video_id
            else:
                print(
                    f"Warning: Video asset {video_id} for carousel card {idx} on ad {ad_id} not found in dim_video_creative. Skipping card."
                )
                continue
        else:
            print(
                f"Card {idx} for ad {ad_id} has neither image_hash nor video_id. Skipping card."
            )
            continue

        if card_asset_processed_id is None:
            print(
                f"Skipping card {idx} for ad {ad_id} due to missing or unprocessed asset."
            )
            continue

        card_id = str(uuid.uuid4())
        upsert_query = """
            INSERT INTO dim_creative_card
            (card_id, account_id, ad_id, creative_type, video_id, image_hash, card_order)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ad_id, card_order) DO UPDATE SET
                 account_id = EXCLUDED.account_id,
                 creative_type = EXCLUDED.creative_type,
                 video_id = EXCLUDED.video_id,
                 image_hash = EXCLUDED.image_hash;
        """

        try:
            db.execute(
                upsert_query,
                (
                    card_id,
                    int(account_id),
                    ad_id,
                    card_type,
                    card_asset_processed_id if card_type == "video" else None,
                    card_asset_processed_id if card_type == "image" else None,
                    idx,
                ),
            )
        except Exception as e:
            print(f"Error processing carousel card {idx} for ad {ad_id}: {e}")
            success = False
            continue

    return success


def update_carousel_ad_availability(db: Database, ad_id: int) -> bool:
    """
    Updates the is_available flag for a carousel ad based on whether any of its cards
    have available assets.
    """
    try:
        availability_query = """
            UPDATE dim_ad
            SET is_available = EXISTS (
                SELECT 1 FROM dim_creative_card cc
                LEFT JOIN dim_image_creative dic ON cc.image_hash = dic.image_hash AND cc.creative_type = 'image'
                LEFT JOIN dim_video_creative dvc ON cc.video_id = dvc.video_id AND cc.creative_type = 'video'
                WHERE cc.ad_id = %s AND (dic.creative_available = TRUE OR dvc.creative_available = TRUE)
            )
            WHERE ad_id = %s AND creative_type = 'carousel'
        """

        db.execute(availability_query, (ad_id, ad_id))
        return True
    except Exception as e:
        print(f"Error updating carousel ad {ad_id} availability: {e}")
        return False


def process_dpa_assets(
    db: Session,
    creative_data: Dict[str, Any],
    image_assets_to_process: Dict[str, Dict[str, Any]],
    video_assets_to_process: Dict[int, Set[int]],
    all_image_hashes: Set[str],
):
    """
    Collects unique DPA image hashes and video IDs from asset_feed_spec
     and adds them to the respective processing lists/sets.
    Only collects the first video ID and first image hash if multiple are present.
    Doesn't perform DB operations directly, just collects information.
    """
    asset_feed_spec = creative_data.get("asset_feed_spec")
    if not asset_feed_spec:
        return

    videos = asset_feed_spec.get("videos")
    if videos and isinstance(videos, list) and len(videos) > 0:
        first_video = videos[0]
        video_id = first_video.get("video_id")
        if video_id:
            try:
                video_id_int = int(video_id)
                if video_id_int not in video_assets_to_process:
                    video_assets_to_process[video_id_int] = set()
            except (ValueError, TypeError):
                print(
                    f"Invalid video ID format in DPA asset_feed_spec (first video): {video_id}. Skipping."
                )
        else:
            print("First video in DPA asset_feed_spec has no video_id. Skipping.")

    images = asset_feed_spec.get("images")
    if images and isinstance(images, list) and len(images) > 0:
        first_image = images[0]
        image_hash = first_image.get("hash")
        image_url = first_image.get("url")
        if image_hash:
            if image_hash not in image_assets_to_process:
                image_assets_to_process[image_hash] = {"url": image_url}
            all_image_hashes.add(image_hash)
        else:
            print("First image in DPA asset_feed_spec has no hash. Skipping.")


def process_ad(db: Database, ad_data: Dict[str, Any], account_id: int) -> Optional[int]:
    """
    Process ad data and store/update in dim_ad.
    Assumes creative dimension tables (dim_image_creative, dim_video_creative)
    are already populated with relevant assets.
    Returns the ad_id if successful, None otherwise.
    """
    ad_id_str = ad_data.get("id")
    adset_data = ad_data.get("adset", {})
    adset_id_str = adset_data.get("id")
    creative_data = ad_data.get("creative", {})

    if not ad_id_str or not adset_id_str or not creative_data:
        print(
            f"Missing essential data for ad (ID: {ad_id_str}): ad_id, adset_id, or creative_data."
        )
        return None

    try:
        ad_id = int(ad_id_str)
        adset_id = int(adset_id_str)
    except (ValueError, TypeError):
        print(f"Invalid ID format for ad {ad_id_str} or adset {adset_id_str}.")
        return None

    creative_type = determine_creative_type(creative_data)

    ad_image_hash = None
    ad_video_id = None
    is_available = False

    if creative_type == "image":
        ad_image_hash = creative_data.get("image_hash")
        if not ad_image_hash:
            ad_image_hash = (
                creative_data.get("object_story_spec", {})
                .get("link_data", {})
                .get("image_hash")
            )
        if not ad_image_hash:
            ad_image_hash = (
                creative_data.get("object_story_spec", {})
                .get("photo_data", {})
                .get("image", {})
                .get("hash")
            )

        if ad_image_hash:
            availability_query = """SELECT creative_available FROM dim_image_creative WHERE image_hash = %s"""
            result = db.execute(availability_query, (ad_image_hash,)).fetchone()
            is_available = bool(result)

    elif creative_type == "video":
        ad_video_id_str = (
            creative_data.get("object_story_spec", {})
            .get("video_data", {})
            .get("video_id")
        )
        if not ad_video_id_str:
            link_data = creative_data.get("object_story_spec", {}).get("link_data", {})
            if link_data and "child_attachments" in link_data:
                for attachment in link_data["child_attachments"]:
                    if attachment.get("video_id"):
                        ad_video_id_str = attachment.get("video_id")
                        break

        if not ad_video_id_str:
            template_data = creative_data.get("object_story_spec", {}).get(
                "template_data", {}
            )
            if template_data and "child_attachments" in template_data:
                for attachment in template_data["child_attachments"]:
                    if attachment.get("video_id"):
                        ad_video_id_str = attachment.get("video_id")
                        break

        try:
            ad_video_id = int(ad_video_id_str) if ad_video_id_str else None

            if ad_video_id:
                availability_query = """SELECT creative_available FROM dim_video_creative WHERE video_id = %s"""
                result = db.execute(availability_query, (ad_video_id,)).fetchone()
                is_available = bool(result)

        except (ValueError, TypeError):
            ad_video_id = None
            print(
                f"Invalid video ID format for video creative on ad {ad_id}: {ad_video_id_str}"
            )

    elif creative_type == "DPA":
        asset_feed_spec = creative_data.get("asset_feed_spec", {})

        images = asset_feed_spec.get("images", [])
        if images and isinstance(images, list) and len(images) > 0:
            ad_image_hash = images[0].get("hash")
            if ad_image_hash:
                creative_type = "image"
                availability_query = """SELECT creative_available FROM dim_image_creative WHERE image_hash = %s"""
                result = db.execute(availability_query, (ad_image_hash,)).fetchone()
                is_available = bool(result)

        if not ad_image_hash:
            videos = asset_feed_spec.get("videos", [])
            if videos and isinstance(videos, list) and len(videos) > 0:
                video_id_str = videos[0].get("video_id")
                try:
                    ad_video_id = int(video_id_str) if video_id_str else None
                    if ad_video_id:
                        creative_type = "video"

                        availability_query = """SELECT creative_available FROM dim_video_creative WHERE video_id = %s"""
                        result = db.execute(
                            availability_query, (ad_video_id,)
                        ).fetchone()
                        is_available = bool(result)

                except (ValueError, TypeError):
                    ad_video_id = None
                    print(
                        f"Invalid video ID format in DPA for ad {ad_id}: {video_id_str}"
                    )

    elif creative_type == "carousel":
        is_available = False

    # Upsert for dim_ad
    upsert_query = """
         INSERT INTO dim_ad
         (ad_id, account_id, adset_id, ad_name, is_active, creative_type, video_id, image_hash, is_available)
         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
         ON CONFLICT (ad_id)
         DO UPDATE SET
             account_id = EXCLUDED.account_id,
             adset_id = EXCLUDED.adset_id,
             ad_name = EXCLUDED.ad_name,
             is_active = EXCLUDED.is_active,
             creative_type = EXCLUDED.creative_type,
             video_id = EXCLUDED.video_id,
             image_hash = EXCLUDED.image_hash,
             is_available = EXCLUDED.is_available;
     """

    try:
        if creative_type == "unknown":
            print(
                "============= Creative type in unknow. Returning without saving in the db ============="
            )
            return ad_id

        db.execute(
            upsert_query,
            (
                ad_id,
                int(account_id),
                adset_id,
                ad_data.get("name", ""),
                ad_data.get("effective_status", "") == "ACTIVE",
                creative_type,
                ad_video_id,
                ad_image_hash,
                is_available,
            ),
        )

        return ad_id
    except Exception as e:
        print(f"Error upserting ad {ad_id}: {e}")
        return None
