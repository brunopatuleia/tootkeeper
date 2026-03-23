import os

VERSION = "1.4.0"
GITHUB_REPO = "brunopatuleia/mastoferr"

# These env vars are optional — OAuth flow via the web UI is the primary method.
# If set, they serve as fallback/override (useful for headless/automated setups).
MASTODON_INSTANCE = os.environ.get("MASTODON_INSTANCE", "")
MASTODON_ACCESS_TOKEN = os.environ.get("MASTODON_ACCESS_TOKEN", "")

POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "5"))
DB_PATH = os.environ.get("DB_PATH", "/app/data/mastoferr.db")
MEDIA_PATH = os.environ.get("MEDIA_PATH", "/app/data/media")

# The external URL where this app is reachable (for OAuth redirect)
# e.g. http://localhost:6886 or https://mastoferr.example.com
APP_URL = os.environ.get("APP_URL", "http://localhost:6886")

# Optional password to protect the web UI. Leave blank for open access.
APP_PASSWORD = os.environ.get("APP_PASSWORD", "")

# AI provider for roast generation (optional)
# Supported: anthropic, openai, gemini, openai-compatible
AI_PROVIDER = os.environ.get("AI_PROVIDER", "")
AI_API_KEY = os.environ.get("AI_API_KEY", "")
AI_MODEL = os.environ.get("AI_MODEL", "")
AI_BASE_URL = os.environ.get("AI_BASE_URL", "")  # For openai-compatible providers (e.g. Ollama)
