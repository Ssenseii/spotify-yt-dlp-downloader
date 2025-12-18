import base64
import hashlib
import json
import logging
import secrets
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

try:
    import httpx
except ImportError:  # pragma: no cover
    httpx = None  # type: ignore[assignment]

from .token_manager import TokenInfo, TokenManager

logger = logging.getLogger(__name__)

SPOTIFY_ACCOUNTS_BASE_URL = "https://accounts.spotify.com"

# Exportify's public Client ID (convenient fallback, but could change upstream).
# Source: exportify/exportify.js
EXPORTIFY_FALLBACK_CLIENT_ID = "d99b082b01d74d61a100c9a0e056380b"


def _base64url_no_pad(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def code_challenge_from_verifier(verifier: str) -> str:
    """Compute PKCE S256 code_challenge from code_verifier."""

    digest = hashlib.sha256((verifier or "").encode("utf-8")).digest()
    return _base64url_no_pad(digest)


def get_effective_spotify_client_id(config: dict) -> str:
    """Return the Spotify Client ID to use (config value or Exportify fallback)."""

    config = config or {}
    client_id = str(config.get("spotify_client_id", "")).strip()
    return client_id or EXPORTIFY_FALLBACK_CLIENT_ID


def check_spotify_credentials(config: Dict[str, Any]) -> Dict[str, Any]:
    """Validate Spotify OAuth config fields and return a structured status dict."""

    config = config or {}
    redirect_uri = str(config.get("spotify_redirect_uri", "")).strip()
    scopes = list(config.get("spotify_scopes", []) or [])

    client_id_raw = str(config.get("spotify_client_id", "")).strip()
    client_id = get_effective_spotify_client_id(config)
    client_id_source = "config" if client_id_raw else "exportify_fallback"

    if not redirect_uri:
        return {
            "ok": False,
            "client_id": client_id,
            "client_id_source": client_id_source,
            "redirect_uri": redirect_uri,
            "scopes": scopes,
            "message": (
                "Missing spotify_redirect_uri in config.json.\n"
                "Recommended default: http://127.0.0.1:8888/callback"
            ),
        }

    if client_id_source == "exportify_fallback":
        return {
            "ok": True,
            "client_id": client_id,
            "client_id_source": client_id_source,
            "redirect_uri": redirect_uri,
            "scopes": scopes,
            "message": (
                "spotify_client_id is not set. Falling back to Exportify's public client id.\n"
                "For reliability and to avoid breakage if Exportify rotates credentials, create your own Spotify app\n"
                "and set spotify_client_id in config.json (see spotify_app_setup_instructions())."
            ),
        }

    return {
        "ok": True,
        "client_id": client_id,
        "client_id_source": client_id_source,
        "redirect_uri": redirect_uri,
        "scopes": scopes,
        "message": "Spotify credentials look OK.",
    }


def spotify_app_setup_instructions(*, redirect_uri: str = "http://127.0.0.1:8888/callback") -> str:
    """Return user-facing setup instructions for creating a Spotify Developer app."""

    redirect_uri = str(redirect_uri or "").strip() or "http://127.0.0.1:8888/callback"
    return (
        "Spotify app setup (recommended):\n"
        "1) Go to https://developer.spotify.com/dashboard\n"
        "2) Create an app (or select an existing app)\n"
        f"3) Add this Redirect URI in the app settings: {redirect_uri}\n"
        "4) Copy the Client ID into spotify-yt-dlp-downloader/config.json as spotify_client_id\n"
        "5) Keep spotify_scopes as-is unless you know you need different permissions\n\n"
        "Notes:\n"
        "- This project uses Authorization Code + PKCE (no client secret required).\n"
        "- Redirect URI must match *exactly* what you configure in the Spotify dashboard.\n"
        "- Spotify no longer allows 'localhost' as a redirect URI; use a loopback IP (127.0.0.1 / ::1).\n"
        "- The port is part of the redirect URI; if you change it, update the Spotify app settings too.\n"
    )


def extract_code_from_redirect_url(redirect_url: str) -> Dict[str, str]:
    """Parse a redirect URL and return {"code": ..., "state": ...} (missing keys omitted)."""

    parsed = urllib.parse.urlparse(str(redirect_url or "").strip())
    qs = urllib.parse.parse_qs(parsed.query)
    out: Dict[str, str] = {}
    if qs.get("code"):
        out["code"] = str(qs["code"][0])
    if qs.get("state"):
        out["state"] = str(qs["state"][0])
    if qs.get("error"):
        out["error"] = str(qs["error"][0])
    return out


@dataclass(frozen=True)
class PKCEPair:
    code_verifier: str
    code_challenge: str


class SpotifyPKCEAuth:
    """Spotify OAuth (Authorization Code + PKCE) helper."""

    def __init__(self, config: Dict[str, Any], *, token_manager: Optional[TokenManager] = None):
        self.config = config or {}
        self.token_manager = token_manager or TokenManager()

    @staticmethod
    def generate_pkce_pair() -> PKCEPair:
        """Generate a PKCE verifier + challenge."""

        # RFC 7636: verifier length 43-128 chars, characters from ALPHA / DIGIT / "-" / "." / "_" / "~"
        verifier = secrets.token_urlsafe(64).rstrip("=")
        verifier = verifier[:128]
        if len(verifier) < 43:
            verifier = (verifier + secrets.token_urlsafe(64)).rstrip("=")[:43]

        challenge = code_challenge_from_verifier(verifier)
        return PKCEPair(code_verifier=verifier, code_challenge=challenge)

    def get_authorize_url(
        self,
        *,
        code_challenge: str,
        state: Optional[str] = None,
        scopes: Optional[Iterable[str]] = None,
        show_dialog: bool = False,
    ) -> str:
        client_id = get_effective_spotify_client_id(self.config)
        redirect_uri = str(self.config.get("spotify_redirect_uri", "")).strip()
        if not redirect_uri:
            raise ValueError("Missing config.spotify_redirect_uri")

        scope_list = list(scopes if scopes is not None else self.config.get("spotify_scopes", []))
        scope_str = " ".join([str(s).strip() for s in scope_list if str(s).strip()])

        params: Dict[str, str] = {
            "client_id": client_id,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "code_challenge_method": "S256",
            "code_challenge": str(code_challenge),
            "show_dialog": "true" if show_dialog else "false",
        }
        if scope_str:
            params["scope"] = scope_str
        if state:
            params["state"] = str(state)

        return f"{SPOTIFY_ACCOUNTS_BASE_URL}/authorize?{urllib.parse.urlencode(params)}"

    def exchange_code_for_token(self, *, code: str, code_verifier: str) -> TokenInfo:
        payload = self._post_form(
            f"{SPOTIFY_ACCOUNTS_BASE_URL}/api/token",
            {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": str(self.config.get("spotify_redirect_uri", "")).strip(),
                "client_id": get_effective_spotify_client_id(self.config),
                "code_verifier": code_verifier,
            },
        )
        token = TokenInfo.from_spotify_token_response(payload)
        if not token.access_token:
            raise RuntimeError(f"Spotify token exchange failed: {payload}")

        self.token_manager.save(self.config, token)
        return token

    def refresh_access_token(self, *, refresh_token: str) -> TokenInfo:
        payload = self._post_form(
            f"{SPOTIFY_ACCOUNTS_BASE_URL}/api/token",
            {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": get_effective_spotify_client_id(self.config),
            },
        )

        token = TokenInfo.from_spotify_token_response(payload)

        # Spotify may omit refresh_token on refresh; keep existing.
        if not token.refresh_token:
            token = TokenInfo(
                access_token=token.access_token,
                token_type=token.token_type,
                expires_at=token.expires_at,
                refresh_token=refresh_token,
                scope=token.scope,
            )

        if not token.access_token:
            raise RuntimeError(f"Spotify token refresh failed: {payload}")

        self.token_manager.save(self.config, token)
        return token

    def load_cached_token(self) -> Optional[TokenInfo]:
        return self.token_manager.load(self.config)

    def begin_oauth_flow(self, *, show_dialog: bool = True) -> Dict[str, Any]:
        """Return {auth_url, pkce_pair, state} for starting the PKCE browser flow."""

        pkce = self.generate_pkce_pair()
        state = secrets.token_urlsafe(16).rstrip("=")
        url = self.get_authorize_url(code_challenge=pkce.code_challenge, state=state, show_dialog=show_dialog)
        return {"auth_url": url, "pkce_pair": pkce, "state": state}

    def _post_form(self, url: str, form: Dict[str, Any]) -> Dict[str, Any]:
        data = {k: str(v) for k, v in (form or {}).items() if v is not None}

        if httpx is None:
            raise RuntimeError(
                "Missing dependency: httpx. Install requirements (pip install -r requirements.txt) to use Spotify OAuth."
            )

        try:
            with httpx.Client(timeout=30.0, follow_redirects=False) as client:
                resp = client.post(
                    url,
                    data=data,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
        except Exception as e:
            raise RuntimeError(f"Spotify token request failed: {e}") from e

        if resp.status_code >= 400:
            raise RuntimeError(f"Spotify token request failed (HTTP {resp.status_code}): {resp.text}")

        try:
            payload = resp.json()
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Spotify token response was not JSON: {resp.text}") from e

        if not isinstance(payload, dict):
            raise RuntimeError(f"Spotify token response was not an object: {payload}")

        return payload
