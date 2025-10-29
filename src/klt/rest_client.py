from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
from requests import Session
from requests_cache import CachedSession


def make_rest_client(
    kobo_token: str, kobo_server: str, session: Session | CachedSession | None = None
) -> RESTClient:
    return RESTClient(
        base_url=kobo_server,
        auth=APIKeyAuth(
            name="Authorization", api_key=f"Token {kobo_token}", location="header"
        ),
        paginator=JSONLinkPaginator(next_url_path="next"),
        data_selector="results",
        session=session,
    )
