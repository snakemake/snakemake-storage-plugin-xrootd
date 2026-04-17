import subprocess
import time
from pathlib import Path
from typing import Optional, Type

import pytest
from snakemake_interface_common.logging import get_logger
from snakemake_interface_storage_plugins.tests import TestStorageBase
from snakemake_interface_storage_plugins.storage_provider import StorageProviderBase
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase

from snakemake_storage_plugin_xrootd import StorageProvider, StorageProviderSettings
import socket

XROOTD_TEST_PORT = 32293


def make_provider(settings: StorageProviderSettings) -> StorageProvider:
    return StorageProvider(
        local_prefix=Path(".tests-local"),
        logger=get_logger(),
        settings=settings,
        keep_local=False,
        is_default=False,
    )


@pytest.fixture(scope="module", autouse=True)
def start_xrootd_server():
    """Starts an XRootD server for testing."""
    proc = subprocess.Popen(["xrootd", "-p", str(XROOTD_TEST_PORT)])
    start_time = time.time()

    while time.time() - start_time < 10:
        if proc.poll() is not None:
            pytest.fail("XRootD server terminated unexpectedly.")
        try:
            with socket.create_connection(("localhost", XROOTD_TEST_PORT), timeout=1):
                break
        except Exception:
            time.sleep(0.1)
    else:
        pytest.fail("XRootD server did not start within 10 seconds.")

    yield XROOTD_TEST_PORT

    proc.terminate()


class TestStorage(TestStorageBase):
    __test__ = True
    retrieve_only = False  # set to True if the storage is read-only
    store_only = False  # set to True if the storage is write-only
    delete = True  # set to False if the storage does not support deletion

    def get_query(self, tmp_path) -> str:
        # Return a query. If retrieve_only is True, this should be a query that
        # is present in the storage, as it will not be created.
        return "root://tmp/test.txt"

    def get_query_not_existing(self, tmp_path) -> str:
        return "root://tmp/test_not_existing.txt"

    def get_storage_provider_cls(self) -> Type[StorageProviderBase]:
        # Return the StorageProvider class of this plugin
        return StorageProvider

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        # instantiate StorageProviderSettings of this plugin as appropriate
        return StorageProviderSettings(
            host="localhost",
            port=XROOTD_TEST_PORT,
            url_decorator="test_decorators:add_decorator",
        )


class TestStorageEval(TestStorageBase):
    __test__ = True
    retrieve_only = False
    store_only = False
    delete = True

    def get_query(self, tmp_path) -> str:
        return "root://tmp/test_eval.txt"

    def get_query_not_existing(self, tmp_path) -> str:
        return "root://tmp/test_eval_not_existing.txt"

    def get_storage_provider_cls(self) -> Type[StorageProviderBase]:
        return StorageProvider

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        return StorageProviderSettings(
            host="localhost",
            port=XROOTD_TEST_PORT,
            url_decorator="url + '?authz=anonymous'",
        )


def test_postprocess_query_adds_protocol_preference():
    provider = make_provider(StorageProviderSettings(protocol="krb5"))

    query = provider.postprocess_query("root://host//test.txt")

    assert query == "root://host:1094//test.txt?xrd.wantprot=krb5"


def test_postprocess_query_supports_multiple_protocols():
    provider = make_provider(StorageProviderSettings(protocol="krb5,unix"))

    query = provider.postprocess_query("root://host//test.txt")

    assert query == "root://host:1094//test.txt?xrd.wantprot=krb5,unix"


def test_postprocess_query_preserves_existing_params():
    provider = make_provider(StorageProviderSettings(protocol="krb5,unix"))

    query = provider.postprocess_query("root://host//test.txt?authz=anonymous")

    assert query == "root://host:1094//test.txt?authz=anonymous&xrd.wantprot=krb5,unix"


def test_protocol_is_added_before_url_decorator():
    provider = make_provider(
        StorageProviderSettings(
            protocol="krb5,unix",
            url_decorator="test_decorators:add_query_aware_decorator",
        )
    )

    query = provider.postprocess_query("root://host//test.txt")

    assert "xrd.wantprot=krb5,unix" in query
    assert "saw_query=yes" in query


def test_safe_print_redacts_protocol_query_param():
    provider = make_provider(StorageProviderSettings(protocol="krb5,unix"))

    query = provider.postprocess_query("root://host//test.txt")

    assert provider.safe_print(query) == "root://host:1094//test.txt?****"
