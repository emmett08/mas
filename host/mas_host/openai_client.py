"""Optional OpenAI client ownership, independent of any application contract."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Mapping

from .provider_contract import ProviderError


def create_client(api_key: str):
    """Construct the fixed-endpoint SDK client with bounded, non-retrying calls."""
    if not api_key:
        raise ProviderError("OpenAI mode requires OPENAI_API_KEY_FILE or OPENAI_API_KEY")
    try:
        from openai import OpenAI
    except ImportError:
        raise ProviderError("OpenAI mode requires the optional openai dependency") from None
    try:
        return OpenAI(api_key=api_key, base_url="https://api.openai.com/v1",
                      timeout=45.0, max_retries=0)
    except Exception:
        raise ProviderError("could not initialise the OpenAI client") from None


@contextmanager
def response_client(*, client=None, api_key: str | None = None,
                    environ: Mapping[str, str] | None = None):
    """Resolve secrets on execution; close owned clients and retain injected ones.

    Registration and descriptor inspection never enter this context. An explicit
    environment is a credential source for embedders/tests; the default is read
    from the executing process so secret rotation does not alter the binding.
    """
    if client is not None:
        yield client
        return
    if api_key is None:
        from .provider_credentials import read_api_key
        api_key = read_api_key("openai", environ=environ)
    owned = create_client(api_key)
    primary = None
    try:
        yield owned
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            owned.close()
        except BaseException:
            if primary is None:
                raise
