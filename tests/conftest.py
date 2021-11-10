import asyncio

import pytest


@pytest.fixture(scope="session")
def event_loop():
    return asyncio.new_event_loop()
