import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Meta Configuration
META_API_KEY = os.getenv("META_API_KEY")
META_AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID")
RESPONSE = os.getenv("RESPONSE")

# Database configuration
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# AWS SQS Configuration
AWS_REGION = os.environ.get("AWS_REGION")  # e.g., 'us-east-1'
SQS_QUEUE_URL = os.environ.get(
    "SQS_QUEUE_URL", "https://sqs.ap-south-1.amazonaws.com/245681210311/image_queue"
)  # e.g., 'https://sqs.your-region.amazonaws.com/your-account-id/your-queue-name'
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

# AWS S3 BUCKET
S3_BUCKET_NAME = "metacreativebucket"
S3_KEY_PREFIX = "images/"


# Image Table and Columns
IMAGE_TABLE = "dim_image_creative"
IMAGE_ID_COLUMN = "image_hash"
IMAGE_URL_COLUMN = "asset_link"


# Video Table and Columns
VIDEO_TABLE = "dim_video_creative"
VIDEO_ID_COLUMN = "video_id"
VIDEO_URL_COLUMN = "asset_link"


# celery
CELERY_BROKER = os.getenv("CELERY_BROKER","redis://localhost:6379/0")
CELERY_BACKEND = os.getenv("CELERY_BACKEND","redis://localhost:6379/0")