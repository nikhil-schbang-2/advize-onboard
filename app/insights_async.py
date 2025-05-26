import asyncio
import json
import random
import os
import traceback
from datetime import date, datetime, timedelta
import logging
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError
from sqlalchemy import bindparam, text
from logging.handlers import TimedRotatingFileHandler
from facebook_business.adobjects.ad import Ad
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
import time
from itertools import islice
from typing import List, Dict, Any, Set
import psycopg2 as psy

from app.config import META_API_KEY, S3_BUCKET_NAME
from app.fact_table import upsert_dimension
from app.tasks import (
    determine_creative_type,
    download_and_upload_image,
    fetch_image_urls_batch,
    process_carousel_creative,
    process_image_creative,
    process_video_creative,
    process_ad,
    update_carousel_ad_availability,
)
from app.celery_app import celery
from app.facebook_sdk import FacebookAPIClient
from facebook_business.exceptions import FacebookRequestError
from database.db import Database
from app.utils import log_message


def exponential_backoff_retry(func, max_retries=5):
    """Retry a function with exponential backoff on rate limit or concurrency errors."""
    for attempt in range(max_retries):
        try:
            return func()
        except FacebookRequestError as e:
            error_code = e.api_error_code()
            error_subcode = e.api_error_subcode()
            error_msg = e.api_error_message()

            log_message(f"[Attempt {attempt + 1}] Facebook API Error: {error_msg}")

            if error_code in [17, 613, 80004] or error_subcode in [1487390, 80004]:
                wait_time = (2**attempt) + random.uniform(0, 1)
                log_message(
                    f"Rate/concurrency limit hit. Retrying in {wait_time:.2f}s..."
                )
                time.sleep(wait_time + (60 * attempt))
            else:
                raise
    raise Exception("Maximum retries exceeded")


def start_async_insights_query(ad_account_id: str, params: Dict[str, Any]):
    """Start an asynchronous insights query for the given ad account."""

    def _start():
        ad_account = AdAccount(ad_account_id)
        async_job = ad_account.get_insights(params=params, is_async=True)
        report_run_id = async_job[AdReportRun.Field.id]
        # print(f"Started async query with report_run_id: {report_run_id}")
        return report_run_id

    return exponential_backoff_retry(_start)


def poll_async_job(report_run_id: str, poll_interval: int = 5, max_attempts: int = 60):
    """Poll the async job until completion or failure."""
    for attempt in range(max_attempts):
        try:
            async_job = AdReportRun(report_run_id).api_get()
            status = async_job[AdReportRun.Field.async_status]
            percent_complete = async_job[AdReportRun.Field.async_percent_completion]
            # print(f"Status: {status}, {percent_complete}% complete")

            if status == "Job Completed":
                # print("Async job completed successfully.")
                return async_job
            elif status in ["Job Failed", "Job Skipped"]:
                # print(f"Async job failed with status: {status}")
                raise Exception(f"Job failed with status: {status}")

            time.sleep(poll_interval)
        except FacebookRequestError as e:
            # print(f"Error polling async job: {e.api_error_message()}")
            time.sleep(5)

    raise TimeoutError("Async job did not complete within the maximum attempts.")


def retrieve_insights_results(async_job):
    """Retrieve and return the results of a completed async job."""
    try:
        results = async_job.get_result()
        insights = [dict(insight) for insight in results]
        # print(f"Retrieved {len(insights)} insights.")
        paging = getattr(results, "paging", None)
        return insights, paging
    except FacebookRequestError as e:
        # print(f"Failed to retrieve insights: {e.api_error_message()}")
        raise


def fetch_all_insights(ad_account_id, params, breakdowns=None, limit=100):
    """Fetch all insights with pagination, respecting the limit."""
    current_params = params.copy()
    if breakdowns:
        current_params["breakdowns"] = json.dumps(breakdowns)

    try:
        report_run_id = start_async_insights_query(ad_account_id, current_params)
        async_job = poll_async_job(report_run_id)
        insights, _ = retrieve_insights_results(async_job)
        # print(f"Total insights collected: {len(insights)}")
        return insights
    except Exception as e:
        # print(f"Error fetching insights: {str(e)}")
        return None


def fetch_ads_in_batches(
    ad_ids: List[str], fields: List[str] = [], batch_size: int = 50
):
    """Fetch ad data for a list of ad_ids in batches using the Ads API."""
    if not ad_ids:
        # print("No ad IDs provided.")
        return None

    all_ads = []
    api = FacebookAdsApi.get_default_api()

    def chunks(iterable, size):
        iterator = iter(iterable)
        for first in iterator:
            yield [first] + list(islice(iterator, size - 1))

    def process_batch(batch_ad_ids):
        batch = api.new_batch()
        responses = []

        for ad_id in batch_ad_ids:
            ad = Ad(ad_id)
            ad.api_get(fields=fields, batch=batch, success=responses.append)

        batch.execute()
        time.sleep(1)

        # Collect successful responses
        return [ad_data.json() for ad_data in responses if ad_data.is_success]

    batch_counter = 0
    for batch_ad_ids in chunks(ad_ids, batch_size):
        # print(f"Processing batch of {len(batch_ad_ids)} ads")
        try:
            ads = exponential_backoff_retry(lambda: process_batch(batch_ad_ids))
            all_ads.extend(ads)
            batch_counter += 1
            # print(f"Batch {batch_counter} executed.")
        except Exception as e:
            # print(f"Error processing batch {batch_counter}: {str(e)}")
            continue

    return all_ads


def process_and_insert_insights(
    insights_data, time_window_key, account_id, db: Database
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

    rows_to_insert = []

    for row in insights_data:
        try:
            ad_id = int(row.get("ad_id"))

            # This condition is only for account id: 841473654502812
            if ad_id in [120217380875130251, 120217444539950251]:
                continue

            adset_id = row.get("adset_id")
            campaign_id = row.get("campaign_id")

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

            amount_spent = float(row.get("spend", 0))
            impressions = int(row.get("impressions", 0))
            reach = int(row.get("reach", 0))

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

            region_id = None
            age_id = None
            gender_id = None
            platform_id = None
            placement_id = None
            impression_device_id = None

            upsert_dimension(
                db,
                "dim_account",
                "account_id",
                account_id,
                "account_id",
            )

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
                "account",
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
            log_message(
                f"Error processing insights row for Ad ID {row.get('ad_id')}",
                e,
            )
            traceback.print_exc()
            if isinstance(e, psy.Error):
                log_message(f"  PostgreSQL Error Code: {e.pgcode}")
                if e.diag:
                    log_message(f"  PostgreSQL Error Message: {e.diag.message_primary}")
                    try:
                        if e.diag.detail:
                            log_message(f"  PostgreSQL Error Detail: {e.diag.detail}")
                    except AttributeError:
                        pass
                    if e.diag.constraint_name:
                        log_message(
                            f"  PostgreSQL Constraint Name: {e.diag.constraint_name}"
                        )

    if rows_to_insert:
        try:
            upsert_partition_atomic(account_id, rows_to_insert, db)
            log_message(
                f"Successfully inserted/updated {len(rows_to_insert)} rows for breakdown account."
            )
        except Exception as e:
            log_message(
                "\n--- Database insertion error for breakdown account",
                e,
            )
            traceback.print_exc()
            if isinstance(e, psy.Error):
                log_message(f"  PostgreSQL Error Code: {e.pgcode}")
                if e.diag:
                    log_message(f"  PostgreSQL Error Message: {e.diag.message_primary}")
                    try:
                        if e.diag.detail:
                            log_message(f"  PostgreSQL Error Detail: {e.diag.detail}")
                    except AttributeError:
                        pass
                    if e.diag.constraint_name:
                        log_message(
                            f"  PostgreSQL Constraint Name: {e.diag.constraint_name}"
                        )

    else:
        log_message("No valid rows to insert for breakdown account.")

def upsert_partition_atomic(account_id, rows_to_insert, db: Database):
    timestamp = time.strftime("%Y%m%d%H%M%S")
    new_table = f"fact_ad_metrics_aggregated_acct_{account_id}_tmp_{timestamp}"
    final_table = f"fact_ad_metrics_aggregated_acct_{account_id}"

    # Create unlogged staging table
    db.execute(f"""
        CREATE UNLOGGED TABLE {new_table}
        (LIKE fact_ad_metrics_aggregated INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
    """)

    # Bulk insert into the new table
    insert_stmt = f"""
        INSERT INTO {new_table} (
            time_window, start_date, end_date, ad_id, account_id, adset_id, campaign_id,
            region_id, age_id, gender_id, platform_id, placement_id, impression_device_id,
            breakdown_type,
            amount_spent, video_plays_3s, impressions, reach, thruplays,
            link_click, landing_page_view, purchase, lead, post_reaction, post_shares, post_save,
            purchase_revenue, lead_revenue,
            custom_metrics, created_at
        ) VALUES %s
    """
    db.bulk_execute_values(insert_stmt, rows_to_insert)

    # Create indexes (optional but recommended)
    db.execute(f"CREATE INDEX ON {new_table} (time_window, breakdown_type);")
    db.execute(f"CREATE INDEX ON {new_table} (ad_id);")
    db.execute(f"ANALYZE {new_table};")

    # Begin atomic swap
    try:
        # Check and detach + drop existing partition
        db.execute(f"""
            DO $$
            DECLARE
                rel_exists BOOLEAN;
            BEGIN
                SELECT EXISTS (
                    SELECT 1 FROM pg_class WHERE relname = '{final_table}'
                ) INTO rel_exists;

                IF rel_exists THEN
                    BEGIN
                        EXECUTE format(
                            'ALTER TABLE fact_ad_metrics_aggregated DETACH PARTITION %I',
                            '{final_table}'
                        );
                    EXCEPTION WHEN others THEN
                        -- Ignore if already detached or not a partition
                        NULL;
                    END;

                    -- Drop the old table
                    EXECUTE format(
                        'DROP TABLE IF EXISTS %I',
                        '{final_table}'
                    );
                END IF;
            END$$;
        """)

        # Rename staging to final
        db.execute(f"ALTER TABLE {new_table} RENAME TO {final_table};")

        # Attach partition — inject account_id directly, must be integer
        db.execute(f"""
            ALTER TABLE fact_ad_metrics_aggregated
            ATTACH PARTITION {final_table} FOR VALUES IN ({account_id});
        """)
        
        db.conn.commit()
    except Exception as e:
        print(f"[DB ERROR] Atomic swap failed: {e}")
        raise e


def save_ad_creatives(all_ad_data, account_id, db: Database):
    if not all_ad_data:
        log_message("No ads found in the specified time range or account.")
        return {"message": "No ads found"}

    # --- Pass 1: Collect all unique asset IDs and hashes needing lookup ---
    image_assets_to_process: Dict[str, Dict[str, Any]] = {}
    video_assets_to_process: Dict[int, Set[int]] = {}
    all_image_hashes: Set[str] = set()
    campaigns_to_process: Dict[int, Dict[str, Any]] = {}
    adsets_to_process: Dict[int, Dict[str, Any]] = {}

    for ad_data in all_ad_data:
        ad_id_str = ad_data.get("id")
        ad_id = int(ad_id_str) if ad_id_str else None

        campaign = ad_data.get("campaign", {})
        campaign_id_str = campaign.get("id")

        if campaign_id_str:
            try:
                campaign_id = int(campaign_id_str)
                if campaign_id not in campaigns_to_process:
                    campaigns_to_process[campaign_id] = {
                        "account_id": int(account_id),
                        "campaign_name": campaign.get("name", ""),
                        "objective": campaign.get("objective", ""),
                        "is_active": campaign.get("status", "") == "ACTIVE",
                    }
            except (ValueError, TypeError):
                log_message(
                    f"Invalid campaign ID format for ad {ad_id_str}: {campaign_id_str}. Skipping campaign."
                )

        adset = ad_data.get("adset", {})
        adset_id_str = adset.get("id")
        if adset_id_str and campaign_id_str:
            try:
                adset_id = int(adset_id_str)
                campaign_id = int(campaign_id_str)
                if adset_id not in adsets_to_process:
                    adsets_to_process[adset_id] = {
                        "account_id": int(account_id),
                        "campaign_id": campaign_id,
                        "adset_name": adset.get("name", ""),
                        "result_type": adset.get("optimization_goal", ""),
                        "is_active": adset.get("effective_status", "") == "ACTIVE",
                    }
            except (ValueError, TypeError):
                log_message(
                    f"Invalid adset ID format for ad {ad_id_str}: {adset_id_str}. Skipping adset."
                )

        creative_data = ad_data.get("creative", {})
        if not creative_data:
            continue

        def process_image_asset_collect(img_hash, img_url):
            if not img_hash:
                return
            all_image_hashes.add(img_hash)
            if img_hash not in image_assets_to_process:
                image_assets_to_process[img_hash] = {"url": img_url}
            elif img_url and not image_assets_to_process[img_hash]["url"]:
                image_assets_to_process[img_hash]["url"] = img_url

        def process_video_asset_collect(vid_id, current_ad_id):
            if not vid_id or current_ad_id is None:
                return
            try:
                video_id_int = int(vid_id)
                if video_id_int not in video_assets_to_process:
                    video_assets_to_process[video_id_int] = set()
                video_assets_to_process[video_id_int].add(current_ad_id)
            except (ValueError, TypeError):
                log_message(
                    f"Invalid video ID format: {vid_id} for ad {current_ad_id}. Skipping."
                )

        process_image_asset_collect(
            creative_data.get("image_hash"), creative_data.get("image_url")
        )
        process_image_asset_collect(None, creative_data.get("url"))
        process_image_asset_collect(None, creative_data.get("link_og_image_url"))

        object_story_spec = creative_data.get("object_story_spec", {})
        video_data = object_story_spec.get("video_data", {})
        if video_data:
            process_video_asset_collect(video_data.get("video_id"), ad_id)

        if object_story_spec:
            link_data = object_story_spec.get("link_data", {})
            if link_data:
                process_image_asset_collect(
                    link_data.get("image_hash"), link_data.get("image_url")
                )
                child_attachments = link_data.get("child_attachments", [])
                for card in child_attachments:
                    process_image_asset_collect(
                        card.get("image_hash"), card.get("image_url")
                    )
                    process_video_asset_collect(card.get("video_id"), ad_id)

            template_data = object_story_spec.get("template_data")
            if template_data:
                process_image_asset_collect(
                    template_data.get("image_hash"), template_data.get("image_url")
                )
                child_attachments = template_data.get("child_attachments", [])
                for card in child_attachments:
                    process_image_asset_collect(
                        card.get("image_hash"), card.get("image_url")
                    )
                    process_video_asset_collect(card.get("video_id"), ad_id)

            photo_data = object_story_spec.get("photo_data", {})
            if photo_data:
                image_info = photo_data.get("image", {})
                process_image_asset_collect(
                    image_info.get("hash"), image_info.get("url")
                )

        asset_feed_spec = creative_data.get("asset_feed_spec")
        if asset_feed_spec:
            videos = asset_feed_spec.get("videos")
            if videos and isinstance(videos, list) and len(videos) > 0:
                first_video = videos[0]
                process_video_asset_collect(first_video.get("video_id"), ad_id)

            images = asset_feed_spec.get("images")
            if images and isinstance(images, list) and len(images) > 0:
                first_image = images[0]
                process_image_asset_collect(
                    first_image.get("hash"), first_image.get("url")
                )

    log_message(
        f"Collected {len(campaigns_to_process)} unique campaigns, "
        f"{len(adsets_to_process)} unique adsets, "
        f"{len(image_assets_to_process)} unique image assets (hashes) with initial data, "
        f"{len(all_image_hashes)} total unique image hashes, and "
        f"{len(video_assets_to_process)} unique video assets (IDs)."
    )

    print("Starting batch image URL lookup for all collected hashes from Meta...")
    hash_url_map = fetch_image_urls_batch(list(all_image_hashes), account_id)

    # --- Step 3: Download Images and Upload to S3 ---
    print("Downloading images from Meta URLs and uploading to S3...")
    s3_url_map = {}
    for image_hash, meta_url in hash_url_map.items():
        if meta_url:
            s3_url = download_and_upload_image(
                image_hash, meta_url, int(ad_id_str), S3_BUCKET_NAME
            )
            if s3_url:
                s3_url_map[image_hash] = s3_url
            else:
                log_message(
                    f"Failed to download or upload image for hash {image_hash} from {meta_url}. S3 URL will be None."
                )
        else:
            log_message(
                f"No Meta URL found for hash {image_hash}. S3 URL will be None."
            )

    log_message(f"Successfully uploaded {len(s3_url_map)} images to S3.")

    errors_occurred = False

    try:
        log_message("Started processing in database table dim_campaign")
        for campaign_id, data in campaigns_to_process.items():
            try:
                upsert_campaign_query = """
                    INSERT INTO dim_campaign
                    (campaign_id, account_id, campaign_name, objective, is_active)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (campaign_id)
                    DO UPDATE SET
                        account_id = EXCLUDED.account_id,
                        campaign_name = EXCLUDED.campaign_name,
                        objective = EXCLUDED.objective,
                        is_active = EXCLUDED.is_active;
                """
                upsert_campaign_value = (
                    campaign_id,
                    data["account_id"],
                    data["campaign_name"],
                    data["objective"],
                    data["is_active"],
                )
                print("upsert_campaign_value", upsert_campaign_value)
                db.execute(upsert_campaign_query, upsert_campaign_value)
            except Exception as e:
                log_message(f"Error processing campaign {campaign_id}: {e}")
                errors_occurred = True

        for adset_id, data in adsets_to_process.items():
            try:
                upsert_adset_query = """
                    INSERT INTO dim_adset
                    (adset_id, account_id, campaign_id, adset_name, result_type, is_active)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (adset_id)
                    DO UPDATE SET
                        account_id = EXCLUDED.account_id,
                        campaign_id = EXCLUDED.campaign_id,
                        adset_name = EXCLUDED.adset_name,
                        result_type = EXCLUDED.result_type,
                        is_active = EXCLUDED.is_active;
                """
                upsert_adset_value = (
                    adset_id,
                    data["account_id"],
                    data["campaign_id"],
                    data["adset_name"],
                    data["result_type"],
                    data["is_active"],
                )
                db.execute(upsert_adset_query, upsert_adset_value)
            except Exception as e:
                log_message(f"Error processing adset {adset_id}: {e}")
                errors_occurred = True

        print("Processing Video Creatives...")
        for video_id, associated_ad_ids in video_assets_to_process.items():
            try:
                asyncio.run(
                    process_video_creative(
                        db, video_id, associated_ad_ids, account_id, None
                    )
                )
            except Exception as e:
                log_message(
                    f"Error running async process_video_creative for video {video_id}: {e}"
                )
                errors_occurred = True

        for image_hash in all_image_hashes:
            data = image_assets_to_process.get(image_hash, {})
            final_asset_link = s3_url_map.get(image_hash)

            if (
                process_image_creative(
                    db, image_hash, final_asset_link, data.get("tags"), account_id
                )
                is None
            ):
                errors_occurred = True

        if errors_occurred:
            log_message(
                "Errors occurred during initial dimension processing (Campaigns, Adsets, Images, Videos). Rolling back dimension updates."
            )
            db.conn.rollback()

            return {
                "status": "Failure",
                "message": "Failed to process initial dimension tables.",
            }
        else:
            db.conn.commit()
            log_message(
                "Dimension tables (Campaigns, Adsets, Images, Videos) processed and committed."
            )

    except Exception as e:
        db.conn.rollback()

        log_message(f"Critical error during initial dimension processing phase: {e}")
        return {
            "status": "Failure",
            "message": f"Critical error during initial dimension processing: {e}",
        }

    # --- Step 5: Process Ads and Carousel Cards (linking to dimensions) ---
    print("Processing Ads and Carousel Cards...")
    errors_occurred_linking = False
    update_status_failed = False
    carousel_ads_to_update_availability = set()

    try:
        for ad_data in all_ad_data:
            processed_ad_id = process_ad(db, ad_data, account_id)
            if processed_ad_id is None:
                errors_occurred_linking = True
            else:
                creative_data = ad_data.get("creative", {})
                if (
                    creative_data
                    and determine_creative_type(creative_data) == "carousel"
                ):
                    if process_carousel_creative(
                        db, processed_ad_id, creative_data, account_id
                    ):
                        carousel_ads_to_update_availability.add(processed_ad_id)
                    else:
                        errors_occurred_linking = True

        if errors_occurred_linking:
            log_message(
                "Errors occurred during ad/card processing. Rolling back ad/card updates."
            )
            db.conn.rollback()
            status_message = "Partial Success (Errors processing ads/cards)"
        else:
            db.conn.commit()
            log_message("Ads and Carousel Cards processed and committed.")
            status_message = "Success"

        # --- Step 6: Update Carousel Ad Availability ---
        log_message(
            f"Updating availability for {len(carousel_ads_to_update_availability)} carousel ads..."
        )
        availability_update_errors = False
        if not availability_update_errors:
            for ad_id in carousel_ads_to_update_availability:
                if not update_carousel_ad_availability(db, ad_id):
                    availability_update_errors = True
                    errors_occurred_linking = True

            if availability_update_errors:
                log_message(
                    "Errors occurred during carousel ad availability updates. Rolling back availability updates."
                )
                db.conn.rollback()
            else:
                if not errors_occurred_linking:
                    db.conn.commit()
                    log_message("Carousel ad availability updated and committed.")

        if not update_status_failed:
            try:
                update_status = """
                    INSERT INTO cron_run_status (account_id, last_run_at)
                    VALUES (%s, NOW())
                    ON CONFLICT (account_id)
                    DO UPDATE SET last_run_at = NOW(), updated_at = NOW();
                """
                db.execute(update_status, (int(account_id),), True)
                log_message("Updated cron run status.")
            except Exception as e:
                log_message(f"Error updating cron run status: {e}")
                errors_occurred_linking = True
                status_message = "Partial Success (DB error updating status)"

    except Exception as e:
        db.conn.rollback()
        log_message(f"Critical error during ad/card/availability processing phase: {e}")
        status_message = "Failure"

    finally:
        if db and db.in_transaction():
            try:
                db.conn.rollback()
            except Exception as e:
                print(f"Error during final rollback before closing session: {e}")

    return {
        "message": f"Ads and creatives fetch and processing finished. Status: {status_message}",
        "status": status_message,
        "campaigns_processed": len(campaigns_to_process),
        "adsets_processed": len(adsets_to_process),
        "all_image_hashes_collected": len(all_image_hashes),
        "video_assets_collected": len(video_assets_to_process),
        "meta_image_urls_fetched_in_batch": len(hash_url_map),
        "images_uploaded_to_s3": len(s3_url_map),
        "total_ads_retrieved": len(all_ad_data),
    }


@celery.task
def update_ad_insights(active_accounts):
    ACCESS_TOKEN = META_API_KEY

    db = Database()
    try:
        for account_id, days in active_accounts.items():
            days = sorted(days, reverse=True)
            for index, day in enumerate(days):
                # for account_id in active_accounts:
                log_message(
                    f"Syncing started with account id: {account_id} for day {day}"
                )

                today_fact = date.today()
                duration = today_fact - timedelta(days=day)
                FACT_FETCH_TIME_RANGE = {
                    "since": duration.strftime("%Y-%m-%d"),
                    "until": today_fact.strftime("%Y-%m-%d"),
                }

                try:
                    # Initialize Facebook API client
                    FacebookAPIClient(access_token=ACCESS_TOKEN)

                    # Query parameters
                    params = {
                        "level": "ad",
                        "fields": [
                            "ad_id",
                            "adset_id",
                            "campaign_id",
                            "actions",
                            "action_values",
                            "spend",
                            "impressions",
                            "reach",
                            "video_thruplay_watched_actions",
                        ],
                        "time_range": json.dumps(FACT_FETCH_TIME_RANGE),
                        "action_breakdowns": json.dumps(["action_type"]),
                    }

                    # Fetch insights
                    all_insights_data = fetch_all_insights(
                        f"act_{account_id}", params, []
                    )

                    if not all_insights_data:
                        log_message(
                            f"Not get any Insights from the account_id: {account_id} of day {day}"
                        )
                        continue

                    if index < 1:
                        print(f"fetching ads creatives {index}")
                        ad_ids = [
                            row["ad_id"]
                            for row in all_insights_data
                            if row.get("ad_id")
                        ]
                        print("ad_ids", len(ad_ids))

                        ads_data = fetch_ads_in_batches(
                            ad_ids,
                            [
                                "id",
                                "name",
                                "effective_status",
                                "campaign{id,name,objective,status}",
                                "adset{id,name,optimization_goal,effective_status}",
                                "creative{id,name,object_story_id,object_story_spec,image_url,image_hash,asset_feed_spec,video_id}",
                            ],
                        )

                        save_ad_creatives(ads_data, account_id, db)

                    process_and_insert_insights(
                        all_insights_data,
                        f"{day}d",
                        account_id,
                        db,
                    )
                    log_message(f"Sync completed for account id: {account_id}")

                except Exception as acc_error:
                    log_message(
                        f"Error while processing account {account_id}: {acc_error}"
                    )

    except Exception as outer_error:
        db.conn.close()
        log_message(f"Fatal error during update_ad_insights: {outer_error}")
