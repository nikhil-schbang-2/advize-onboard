from facebook_business import FacebookAdsApi

class FacebookAPIClient:
    """Singleton-style Facebook API client initializer."""

    _initialized = False

    def __init__(self, access_token: str, app_id: str = None, app_secret: str = None):
        """Initialize the API client only once."""
        if not FacebookAPIClient._initialized:
            self._access_token = access_token
            self._app_id = app_id
            self._app_secret = app_secret
            self._init_api()
            FacebookAPIClient._initialized = True
        else:
            print("Facebook API already initialized.")

    def _init_api(self):
        try:
            FacebookAdsApi.init(
                access_token=self._access_token,
                app_id=self._app_id,
                app_secret=self._app_secret,
            )
            print("✅ Facebook API initialized.")
        except Exception as e:
            print(f"❌ Failed to initialize Facebook API: {e}")
            raise