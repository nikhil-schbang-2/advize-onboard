import asyncio
import os
import logging
import json
import re
from playwright.async_api import async_playwright, Route, Response
from urllib.parse import urlparse
from typing import Optional  # Keep Optional for type hinting return value

# Set up logging for this module
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def get_video_from_facebook_iframe(iframe_url: str) -> Optional[str]:
    """
    Uses Playwright to navigate a Facebook preview iframe URL,
    extract the actual video download URL by observing network requests
    and inspecting the DOM, based on the original logic provided.

    Args:
        iframe_url: The URL of the Facebook preview iframe (e.g., business.facebook.com/ads/api/preview_iframe.php?...)

    Returns:
        The extracted video download URL (e.g., an .mp4 link) if found, otherwise None.
    """
    logger.info(
        f"Attempting to fetch video URL from iframe using Playwright: {iframe_url}"
    )
    video_sources = []  # Collect potential video URLs
    # video_data = {} # Original variable, not used for return, removed

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )

            page = await context.new_page()
            await page.route("**/*", lambda route: route.continue_())
            page.on("console", lambda msg: logger.info(f"CONSOLE: {msg.text}"))

            async def capture_video_url(response):
                url = response.url

                try:
                    content_type = response.headers.get("content-type", "")
                    if "video/" in content_type or url.lower().endswith(".mp4"):
                        logger.info(
                            f" Found video response via Content-Type: {url} (Content-Type: {content_type})"
                        )
                        video_sources.append(url)
                    if (
                        "application/json" in content_type
                        or "javascript" in content_type
                        or "text/plain" in content_type
                    ):
                        try:
                            body_text = await response.text()
                            video_matches = re.findall(
                                r'(https?://[^"\']+\.mp4[^"\'\s]*)', body_text
                            )
                            for match in video_matches:
                                logger.info(
                                    f"Found video URL in response body ({content_type}): {match}"
                                )
                                video_sources.append(match)
                            playable_url_matches = re.search(
                                r'"playable_url":"([^"]+)"', body_text
                            )
                            if playable_url_matches:
                                url = playable_url_matches.group(1).replace(r"\/", "/")
                                logger.info(
                                    f"Found playable_url in response body: {url}"
                                )
                                video_sources.append(url)
                            if "videoData" in url or "video_data" in url:
                                try:
                                    json_data = json.loads(
                                        body_text
                                    )  # Use body_text from above
                                    logger.info(
                                        f"Found video data JSON response from URL match: {url}"
                                    )
                                except:
                                    pass
                        except Exception as e:
                            logger.debug(
                                f"Could not read or parse response body from {url}: {e}"
                            )

                except Exception as e:
                    logger.error(f"Error processing response URL {url}: {e}")

            page.on(
                "response",
                lambda response: asyncio.create_task(capture_video_url(response)),
            )
            logger.info(f"Navigating to iframe URL: {iframe_url}")
            await page.goto(iframe_url, timeout=90000, wait_until="load")
            logger.info("Waiting for network activity to settle...")
            await page.wait_for_load_state("networkidle", timeout=30000)
            logger.info("Searching for video elements...")
            video_elements = await page.query_selector_all("video")
            logger.info(f"Found {len(video_elements)} video elements")

            for i, video in enumerate(video_elements):
                try:
                    src = await video.get_attribute("src")
                    if src:
                        logger.info(f"Video element {i + 1} src: {src}")
                        if src.startswith("http"):
                            video_sources.append(src)
                    source_elements = await video.query_selector_all("source")
                    for source in source_elements:
                        src = await source.get_attribute("src")
                        if src:
                            logger.info(f"Source element src: {src}")
                            if src.startswith("http"):
                                video_sources.append(src)
                except Exception as e:
                    logger.error(f"Error extracting video source: {e}")

            logger.info("Checking for video data in page content...")
            try:
                content = await page.content()
                playable_url_matches = re.search(r'"playable_url":"([^"]+)"', content)
                if playable_url_matches:
                    url = playable_url_matches.group(1).replace(r"\/", "/")
                    logger.info(f"Found playable_url: {url}")
                    video_sources.append(url)

                fb_video_matches = re.findall(
                    r'(https?://[^"\']+\.mp4[^"\'\s]*)', content
                )
                for url in fb_video_matches:
                    logger.info(f"Found video URL in page: {url}")
                    video_sources.append(url)
            except Exception as e:
                logger.error(f"Error extracting video URLs from page content: {e}")

            await asyncio.sleep(5)

            try:
                js_video_sources = await page.evaluate("""() => {
                    const sources = [];
                    document.querySelectorAll('video').forEach(video => {
                        if (video.src) sources.push(video.src);
                        video.querySelectorAll('source').forEach(source => {
                            if (source.src) sources.push(source.src);
                        });
                    });
                    return sources;
                }""")
                logger.info(f"Video sources from JS: {js_video_sources}")
                video_sources.extend(js_video_sources)
            except Exception as e:
                logger.error(f"Error getting video sources via JS: {e}")

            await page.screenshot(path="facebook_iframe.png")
            logger.info("Saved screenshot to facebook_iframe.png")

            await browser.close()

            unique_sources = list(set(video_sources))
            logger.info(f"Found {len(unique_sources)} unique video sources")

            best_source = None
            for source in unique_sources:
                if source and source.lower().endswith(".mp4"):
                    best_source = source
                    break

            if not best_source and unique_sources:
                # Ensure the first source is not None before assigning
                best_source = unique_sources[0] if unique_sources[0] else None

            return best_source

    except Exception as e:
        logger.error(
            f"An error occurred during Playwright execution for iframe {iframe_url}: {e}"
        )
        return None


async def get_main_image_from_facebook_iframe(iframe_url: str) -> Optional[str]:
    """
    Extracts the main thumbnail image from a Facebook iframe URL, focusing on network
    responses with content-type: image/* (Network tab initiator 'image/').

    Args:
        iframe_url (str): The Facebook iframe preview URL.

    Returns:
        Optional[str]: URL of the main thumbnail image, or None if not found.
    """
    image_sources = []
    fb_ads_images = []

    async def capture_images(response: Response) -> None:
        """Capture images from network responses with content-type: image/*."""
        url = response.url
        content_type = response.headers.get("content-type", "").lower()

        try:
            if "image/" in content_type and re.search(
                r"\.(jpg|jpeg|png|webp|gif|avif)", url, re.I
            ):
                if "logo" not in url.lower() and "icon" not in url.lower():
                    logger.info(f"Captured image: {url} (content-type: {content_type})")
                    image_sources.append(
                        (url, response.headers.get("content-length", 0))
                    )
                    if "business.facebook.com/ads/image" in url or "fbcdn.net" in url:
                        fb_ads_images.append(
                            (url, response.headers.get("content-length", 0))
                        )
        except Exception as e:
            logger.debug(f"Error processing response: {url} - {e}")

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                )
            )
            page = await context.new_page()

            # Capture all network responses
            page.on(
                "response",
                lambda response: asyncio.create_task(capture_images(response)),
            )

            logger.info(f"Navigating to: {iframe_url}")
            await page.goto(iframe_url, timeout=90000, wait_until="load")

            # Wait for image-related network requests explicitly
            try:
                await page.wait_for_response(
                    lambda res: "image/" in res.headers.get("content-type", "").lower()
                    and (
                        "business.facebook.com/ads/image" in res.url
                        or "fbcdn.net" in res.url
                    ),
                    timeout=15000,
                )
                logger.info("Found image response matching criteria")
            except:
                logger.warning(
                    "No specific image response found within timeout, falling back to networkidle"
                )

            # Wait for additional network activity
            await page.wait_for_load_state("networkidle", timeout=30000)

            await browser.close()

            # Sort images by content-length (larger images are likely thumbnails)
            fb_ads_images.sort(key=lambda x: int(x[1] or 0), reverse=True)
            image_sources.sort(key=lambda x: int(x[1] or 0), reverse=True)

            # Select the best image
            if fb_ads_images:
                best_image = fb_ads_images[0][0]
            elif image_sources:
                best_image = image_sources[0][0]
            else:
                best_image = None

            logger.info(f"✅ Best image selected: {best_image}")
            logger.debug(
                f"Found {len(fb_ads_images)} ad images and {len(image_sources)} total images"
            )
            return best_image

    except Exception as e:
        logger.error(f"Error extracting image from iframe {iframe_url}: {e}")
        return None


async def get_carousel_images_from_facebook_iframe(
    iframe_url: str, max_images: Optional[int] = None
):
    """
    Extracts carousel image URLs from a Facebook iframe URL by scraping <img> tags
    within the carousel container (div[class*='_8f73']), with a fallback to network
    responses with content-type: image/*.

    Args:
        iframe_url (str): The Facebook iframe preview URL.
        max_images (Optional[int]): Maximum number of images to return (default: None, returns all).

    Returns:
        Optional[List[str]]: List of carousel image URLs, or None if none found.
    """
    image_urls = set()

    async def capture_network_images(response: Response) -> None:
        """Capture images from network responses with content-type: image/* as a fallback."""
        url = response.url
        content_type = response.headers.get("content-type", "").lower()

        try:
            if (
                "image/" in content_type
                and re.search(r"\.(jpg|jpeg|png|webp|gif|avif)", url, re.I)
                and "static.xx.fbcdn.net" not in url
                and "/images/vault/" not in url
                and "logo" not in url.lower()
                and "icon" not in url.lower()
                and not url.endswith("header-background-2x.png")
            ):
                logger.info(
                    f"Captured network image: {url} (content-type: {content_type})"
                )
                image_urls.add(url)
        except Exception as e:
            logger.debug(f"Error processing response: {url} - {e}")

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                )
            )
            page = await context.new_page()

            # Capture network responses as a fallback
            page.on(
                "response",
                lambda response: asyncio.create_task(capture_network_images(response)),
            )

            logger.info(f"Navigating to: {iframe_url}")
            await page.goto(iframe_url, timeout=90000, wait_until="load")

            # Wait for page to stabilize
            await page.wait_for_load_state("networkidle", timeout=30000)

            # Scrape <img> tags within the carousel container
            carousel_selector = "div[class*='_8f73'] img"
            images = await page.query_selector_all(carousel_selector)
            for img in images:
                src = await img.get_attribute("src")
                if (
                    src
                    and "logo" not in src.lower()
                    and "icon" not in src.lower()
                    and "static.xx.fbcdn.net" not in src
                    and "/images/vault/" not in src
                    and not src.endswith("header-background-2x.png")
                    and re.search(r"\.(jpg|jpeg|png|webp|gif|avif)", src, re.I)
                ):
                    logger.info(
                        f"Captured DOM image: {src} (selector: {carousel_selector})"
                    )
                    image_urls.add(src)

            # Fallback: Broader selector if carousel selector yields no results
            if not image_urls:
                logger.debug(
                    "No images found with carousel selector, trying all <img> tags"
                )
                images = await page.query_selector_all("img")
                for img in images:
                    src = await img.get_attribute("src")
                    if (
                        src
                        and "logo" not in src.lower()
                        and "icon" not in src.lower()
                        and "static.xx.fbcdn.net" not in src
                        and "/images/vault/" not in src
                        and not src.endswith("header-background-2x.png")
                        and re.search(r"\.(jpg|jpeg|png|webp|gif|avif)", src, re.I)
                    ):
                        logger.info(f"Captured DOM image (fallback): {src}")
                        image_urls.add(src)

            # Fallback: Network responses if DOM yields too few images
            if not image_urls or (max_images and len(image_urls) < max_images):
                logger.debug(
                    "DOM scraping yielded insufficient images, using network fallback"
                )
                for _ in range((max_images or 2) - len(image_urls)):
                    try:
                        await page.wait_for_response(
                            lambda res: "image/"
                            in res.headers.get("content-type", "").lower()
                            and "static.xx.fbcdn.net" not in res.url
                            and "/images/vault/" not in res.url
                            and "logo" not in res.url.lower()
                            and "icon" not in res.url.lower()
                            and not res.url.endswith("header-background-2x.png"),
                            timeout=10000,
                        )
                        logger.info("Found additional network image response")
                    except:
                        logger.debug(
                            "No more network image responses found within timeout"
                        )
                        break

            await browser.close()

            # Convert to list and limit to max_images if specified
            carousel_images = list(image_urls)
            if max_images:
                carousel_images = carousel_images[:max_images]

            logger.info(f"✅ Carousel images selected: {carousel_images}")
            logger.debug(
                f"Found {len(image_urls)} total images, selected {len(carousel_images)}"
            )
            return carousel_images if carousel_images else None

    except Exception as e:
        logger.error(f"Error extracting images from iframe {iframe_url}: {e}")
        return None
