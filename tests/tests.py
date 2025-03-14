import subprocess
import time
from typing import Optional, Type

import pytest
from snakemake_interface_storage_plugins.tests import TestStorageBase
from snakemake_interface_storage_plugins.storage_provider import StorageProviderBase
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase

from snakemake_storage_plugin_xrootd import StorageProvider, StorageProviderSettings
import socket

XROOTD_TEST_PORT = 32293


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
            url_decorator="url + '?authz=anonymous'",
        )
