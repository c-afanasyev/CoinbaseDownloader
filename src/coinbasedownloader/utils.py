from aiohttp.client_exceptions import ClientResponseError


async def is_not_429(exception):
    return not (isinstance(exception, ClientResponseError) and exception.status == 429)