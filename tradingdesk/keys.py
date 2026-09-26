"""Saving provider API keys from the web app, the way the CLI's key prompt does.

``cli.prompts.ensure_api_key`` asks for a missing key, writes it to the
project's ``.env`` with owner-only permissions and exports it into the process.
The web app does the same from a form, so a key pasted in the browser is used
by the next run and is still there after a restart. The key itself is never
sent back to the browser.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import find_dotenv, set_key, unset_key


def env_file_path(configured: str | None = None) -> Path:
    """Where keys are kept: ``TRADINGDESK_ENV_FILE`` when set, else the ``.env`` the package loads."""
    if configured:
        return Path(configured).expanduser()
    return Path(find_dotenv(usecwd=True) or Path.cwd() / ".env")


def save_api_key(env_var: str, key: str, configured_path: str | None = None) -> Path:
    """Write ``env_var=key`` to the env file (created owner-only) and export it now."""
    path = env_file_path(configured_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        os.close(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))
    os.chmod(path, 0o600)
    set_key(str(path), env_var, key)
    os.environ[env_var] = key
    return path


def forget_api_key(env_var: str, configured_path: str | None = None) -> Path:
    """Remove ``env_var`` from the env file and from this process."""
    path = env_file_path(configured_path)
    if path.exists():
        unset_key(str(path), env_var)
    os.environ.pop(env_var, None)
    return path
