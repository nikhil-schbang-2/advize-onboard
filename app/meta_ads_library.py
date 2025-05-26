from datetime import datetime
import hashlib
import io
import re
import requests
import time
import json
import os
import boto3
from urllib.parse import urlparse
from database.db import Database
import sys
from app.celery_app import celery
from botocore.exceptions import ClientError

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# API credentials
APIFY_TOKEN = "apify_api_eEAWIIBVlGdguTukdYxKPJZb878pI04c75e8"
TASK_ID = "nikhil.pandey~facebook-ads-scraper-task"

BUCKET_NAME = "competitormetacreativebucket"
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)


def start_facebook_ads_scraper(page_id, country="IN", limit=10):
    """Start the Facebook Ads Scraper task with custom input"""

    # Construct Facebook Ads Library URL
    fb_url = f"https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country={country}&is_targeted_country=false&media_type=all&search_type=page&view_all_page_id={page_id}"

    # Create input payload exactly matching the working format
    input_payload = {
        "activeStatus": "active",
        "isDetailsPerAd": False,
        "onlyTotal": False,
        "resultsLimit": limit,
        "startUrls": [{"url": fb_url, "method": "GET"}],
    }

    # Start the task
    start_run_url = (
        f"https://api.apify.com/v2/actor-tasks/{TASK_ID}/runs?token={APIFY_TOKEN}"
    )
    response = requests.post(start_run_url, json=input_payload)
    response.raise_for_status()

    run = response.json()
    run_id = run["data"]["id"]

    # print(f"▶️ Task started. Run ID: {run_id}")
    return run_id


def wait_for_completion(run_id):
    """Wait for the task run to complete"""
    status = "RUNNING"
    while status in ["RUNNING", "READY", "PENDING"]:
        time.sleep(5)
        run_status_url = (
            f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
        )
        run_status = requests.get(run_status_url).json()
        status = run_status["data"]["status"]

    return run_status


def download_dataset(run_id):
    """Download the results dataset"""

    run_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
    run_info = requests.get(run_url).json()

    dataset_id = run_info["data"]["defaultDatasetId"]
    dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_TOKEN}&clean=true"

    dataset = requests.get(dataset_url).json()
    return dataset


def extract_ad_info(ad_data):
    """Extract relevant ad information from the Facebook Ads API response"""
    extracted = []

    for ad in ad_data:
        # Basic ad info
        ad_archive_id = ad.get("adArchiveID", "") or ad.get("adArchiveId", "")
        page_name = ad.get("pageName")
        start_date = ad.get("startDate", "") or ad.get("startDateFormatted", "")
        end_date = ad.get("endDate", "") or ad.get("endDateFormatted", "")
        is_active = ad.get("isActive", "")
        page_id = ad.get("pageId", "")

        # Get ad text and display format
        snapshot = ad.get("snapshot", {})
        if isinstance(snapshot, str):
            # If snapshot is a string, try to parse it as JSON
            try:
                snapshot = json.loads(snapshot)
            except:
                snapshot = {}

        body = snapshot.get("body", {})
        ad_text = ""
        if isinstance(body, dict):
            ad_text = body.get("text", "")
        elif isinstance(body, str):
            ad_text = body

        display_format = snapshot.get("displayFormat", "").upper()

        # Default values
        creative_type = "Unknown"
        creative_hash = ""
        media_url = ""

        # Based on display format, extract media info
        if display_format == "VIDEO":
            videos = snapshot.get("videos", [])
            if videos and len(videos) > 0:
                creative_type = "Video"
                creative_hash = generate_hash(ad_archive_id)
                # Try different URL keys that might exist
                for url_key in ["videoHdUrl", "videoSdUrl", "url", "thumbnailUrl"]:
                    if url_key in videos[0] and videos[0][url_key]:
                        media_url = videos[0][url_key]
                        break

        elif display_format == "IMAGE":
            images = snapshot.get("images", [])
            if images and len(images) > 0:
                creative_type = "Image"
                creative_hash = generate_hash(ad_archive_id)
                # Try different URL keys that might exist
                for url_key in ["originalImageUrl", "resizedImageUrl", "url"]:
                    if url_key in images[0] and images[0][url_key]:
                        media_url = images[0][url_key]
                        break

        elif display_format in ["CAROUSEL", "DPA", "DCO"]:
            cards = snapshot.get("cards", [])
            if cards and len(cards) > 0:
                card = cards[0]
                # Check for video first
                if any(card.get(key) for key in ["videoHdUrl", "videoSdUrl"]):
                    creative_type = "Video"
                    creative_hash = generate_hash(ad_archive_id)
                    media_url = card.get("videoHdUrl", "") or card.get("videoSdUrl", "")
                else:
                    creative_type = "Image"
                    creative_hash = generate_hash(ad_archive_id)
                    media_url = card.get("originalImageUrl", "") or card.get(
                        "resizedImageUrl", ""
                    )

        # Add extracted info to results
        extracted.append(
            {
                "ad_archive_id": ad_archive_id,
                "page_id": page_id,
                "page_name": page_name,
                "start_date": datetime.fromtimestamp(start_date),
                "is_active": is_active,
                "end_date": datetime.fromtimestamp(end_date),
                "creative_type": creative_type,
                "video_hash"
                if creative_type == "Video"
                else "image_hash": creative_hash,
                # "displayFormat": display_format,
                # "text": ad_text.strip() if ad_text else "",
                "media_url": media_url,
            }
        )

    return extracted


def download_video(url, filename):
    """Download a video from the given URL and save it to filename."""
    if not url:
        return False

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "")

        extension = ""
        if "/" in content_type:
            mime_main, mime_sub = content_type.split("/", 1)
            if mime_sub.lower() != "octet-stream":
                extension = mime_sub.lower()
                print(f"==>> extension2: {extension}")

        # Step 2: Fallback to file path in URL
        if not extension:
            path = urlparse(url).path
            match = re.search(r"\.([a-zA-Z0-9]+)$", path)
            extension = match.group(1).lower() if match else ""
            print(f"==>> extension3: {extension}")

        if not extension:
            extension = "mp4"
        print(f"==>> extension: {extension}")
        s3_key_with_extension = f"meta-creatives/videos/{filename}.{extension}"
        print(f"==>> s3_key_with_extension: {s3_key_with_extension}")

        # print(f"Downloading video content for video ID: {video_id}")
        video_data = io.BytesIO()
        for chunk in response.iter_content(chunk_size=8192):
            video_data.write(chunk)
        video_data.seek(0)

        s3 = boto3.client("s3")
        already_exists_video = None
        # Upload to S3
        try:
            s3.head_object(Bucket=BUCKET_NAME, Key=s3_key_with_extension)
            already_exists_video = s3_key_with_extension
        except ClientError as e:
            print(
                f"An S3 error occurred while checking for video object existence with key {s3_key_with_extension}: {e}"
            )
        except Exception as e:
            print(
                f"An unexpected error occurred during S3 existence check for video key {s3_key_with_extension}: {e}"
            )

        if already_exists_video:
            s3_location = s3.get_bucket_location(Bucket=BUCKET_NAME)[
                "LocationConstraint"
            ]
            if s3_location is None:
                s3_url = (
                    f"https://{BUCKET_NAME}.s3.amazonaws.com/{already_exists_video}"
                )
                return s3_url
            else:
                s3_url = f"https://{BUCKET_NAME}.s3-{s3_location}.amazonaws.com/{already_exists_video}"
                return s3_url

        s3.upload_fileobj(video_data, BUCKET_NAME, s3_key_with_extension)

        s3_location = s3.get_bucket_location(Bucket=BUCKET_NAME)["LocationConstraint"]
        if s3_location is None:
            s3_url = f"https://{BUCKET_NAME}.s3.amazonaws.com/{s3_key_with_extension}"
        else:
            s3_url = f"https://{BUCKET_NAME}.s3-{s3_location}.amazonaws.com/{s3_key_with_extension}"

        print(f"Video uploaded to S3: {s3_url}")
        return s3_url
    except Exception as e:
        print(f"==>> e: {e}")
        return False


def download_image(url, filename):
    """Download an image from the given URL and save it to filename."""
    if not url:
        return False

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type")
        extension = "jpg"
        if content_type:
            extension = content_type.split("/")[-1].lower()
            if extension == "jpeg":
                extension = "jpg"

        image_data = io.BytesIO(response.content)
        print(f"==>> image_data: {image_data}")

        s3_key_with_extension = f"meta-creatives/images/{filename}.{extension}"
        print(f"==>> s3_key_with_extension: {s3_key_with_extension}")
        # s3 = boto3.client("s3")
        # s3.upload_fileobj(image_data, BUCKET_NAME, s3_key_with_extension)

        # s3_location = s3.get_bucket_location(Bucket=BUCKET_NAME)["LocationConstraint"]
        # if s3_location is None:
        #     s3_url = f"https://{BUCKET_NAME}.s3.amazonaws.com/{s3_key_with_extension}"
        # else:
        #     s3_url = f"https://{BUCKET_NAME}.s3-{s3_location}.amazonaws.com/{s3_key_with_extension}"

        return s3_key_with_extension
    except Exception as e:
        return False


def generate_hash(input_string):
    """Generate a hash from the input string."""
    # Encode the string to bytes, then create the hash
    hash_object = hashlib.sha256(input_string.encode())
    hex_dig = hash_object.hexdigest()
    return hex_dig


@celery.task
def meta_library_ads_sync():
    print("starting the Facebook Ads Scraper...", datetime.now())
    # Set your target page ID and other parameters
    page_id = "102396233156564"
    country = "IN"
    limit = 20  # Max number of results to fetch

    # Initialize the database
    db = Database()

    # Start the Facebook Ads Scraper
    run_id = start_facebook_ads_scraper(page_id, country, limit)

    # Wait for completion
    wait_for_completion(run_id)

    # Download results
    dataset = download_dataset(run_id)

    # Save raw data

    # with open(f"{DATA_DIR}/raw_fb_ads_data.json", "w", encoding="utf-8") as f:
    #     json.dump(dataset, f, indent=2, ensure_ascii=False)

    # Extract and clean data
    cleaned_ads = extract_ad_info(dataset)

    # Save cleaned data
    # with open(f"{DATA_DIR}/cleaned_ads_from_api.json", "w", encoding="utf-8") as f:
    #     json.dump(cleaned_ads, f, indent=2, ensure_ascii=False)

    # Print stats
    if cleaned_ads:
        print("Started processing in database ...", datetime.now())
        print(f"Total ads extracted: {len(cleaned_ads)}")

        query = """
            INSERT INTO dim_ad_scraped (
                ad_archive_id,
                page_id,
                creative_type,
                video_hash,
                image_hash,
                start_date,
                end_date,
                is_active
            ) VALUES %s
            ON CONFLICT (ad_archive_id)
            DO UPDATE SET
                page_id = EXCLUDED.page_id,
                creative_type = EXCLUDED.creative_type,
                video_hash = EXCLUDED.video_hash,
                image_hash = EXCLUDED.image_hash,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                is_active = EXCLUDED.is_active;
            """

        values = [
            (
                row["ad_archive_id"],
                row["page_id"],
                row.get("creative_type", "").lower(),
                row.get("video_hash"),
                row.get("image_hash"),
                row["start_date"],
                row.get("end_date"),
                row.get("is_active", True),
            )
            for row in cleaned_ads
        ]

        db_update_status = db.bulk_execute_values(query, values)
        if db_update_status:
            print("Data inserted/updated successfully in the database.", datetime.now())
            video_count = 0
            image_count = 0
            for i, ad in enumerate(cleaned_ads):
                if ad["creative_type"] == "Video" and ad["media_url"]:
                    filename_hash = ad["video_hash"]

                    # Check if the video already exists in the database
                    cursor = db.execute(
                        "SELECT asset_link FROM dim_video_creative_scraped WHERE video_hash = %s",
                        (filename_hash,),
                    )
                    is_exist = cursor.fetchone()
                    if is_exist:
                        print(f"Video already exists in the database: {filename_hash}")
                        continue

                    if image_s3_url := download_video(ad["media_url"], filename_hash):
                        video_count += 1
                        print(f"Video URL: {image_s3_url}")

                        query = """
                            INSERT INTO dim_video_creative_scraped (
                                video_hash,
                                page_id,
                                asset_link
                            ) VALUES (%s, %s, %s)
                            ON CONFLICT (video_hash) DO NOTHING
                            """
                        cursor = db.execute(
                            query=query,
                            params=(
                                filename_hash,
                                ad["page_id"],
                                image_s3_url,
                            ),
                            commit=True,
                        )
                        if cursor:
                            cursor.close()
                            print(
                                f"Video URL inserted into the database: {image_s3_url}"
                            )
                        print(f"Video URL inserted into the database: {image_s3_url}")

                    # Only download a few videos for testing
                    if video_count >= 3:
                        break

                # if ad["creative_type"] == "Image" and ad["media_url"]:
                #     filename = f"{ad['image_hash']}"
                #     print(f"==>> filename: {filename}")
                #     if image_s3_url := download_image(
                #         ad["media_url"], f"{DATA_DIR}/{filename}"
                #     ):
                #         image_count += 1
                #         print(f"Image URL: {image_s3_url}")

                #     # Only download a few images for testing
                #     if image_count >= 3:
                #         break
            print(
                "Data inserted/updated successfully in the database.", db_update_status
            )
        else:
            print("Failed to insert/update data in the database.", db_update_status)

        media_types = {}
        for ad in cleaned_ads:
            media_type = ad["creative_type"]
            media_types[media_type] = media_types.get(media_type, 0) + 1

        for media_type, count in media_types.items():
            print(f"- {media_type}: {count}")

    print("Scrapper completed ...", datetime.now())
