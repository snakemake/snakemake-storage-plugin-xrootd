from dataclasses import dataclass, field
import os
import re
from typing import Any, Iterable, Optional, List, Type

from reretry import retry

from XRootD import client
from XRootD.client.flags import MkDirFlags, StatInfoFlags
from XRootD.client.responses import XRootDStatus
from XRootD.client import URL

from snakemake.exceptions import WorkflowError

from snakemake_interface_common.logging import get_logger
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_provider import (  # noqa: F401
    StorageProviderBase,
    StorageQueryValidationResult,
    ExampleQuery,
    Operation,
    QueryType,
)
from snakemake_interface_storage_plugins.storage_object import (
    StorageObjectRead,
    StorageObjectWrite,
    retry_decorator,
)
from snakemake_interface_storage_plugins.io import IOCacheStorageInterface


class XRootDFatalException(Exception):
    """
    Used to prevent retries for certain XRootD errors with the retry decorator
    """


def _raise_fatal_error(exception: Type[Exception]) -> None:
    if isinstance(exception, XRootDFatalException):
        get_logger().warning("Unrecoverable error, no more retries")
        raise exception


# TODO configurable would be nice
xrootd_retry = retry(
    tries=3, delay=3, backoff=2, logger=get_logger(), fail_callback=_raise_fatal_error
)


@dataclass
class StorageProviderSettings(StorageProviderSettingsBase):
    host: Optional[str] = field(
        default=None,
        metadata={
            "help": "The XrootD host to connect to",
            "env_var": False,
            "required": False,
        },
    )
    port: Optional[int] = field(
        default=None,
        metadata={
            "help": "The XrootD port to connect to",
            "env_var": False,
            "required": False,
        },
    )
    username: Optional[str] = field(
        default=None,
        metadata={
            "help": "The username to use for authentication",
            "env_var": True,
            "required": False,
        },
    )
    password: Optional[str] = field(
        default=None,
        metadata={
            "help": "The password to use for authentication. NOTE: Only use this "
            "setting in trusted environments! Snakemake will print the "
            "password in plaintext as part of the XRootD URLs used in the "
            "inputs/outputs of jobs.",
            "env_var": True,
            "required": False,
        },
    )
    url_decorator: Optional[str] = field(
        default=None,
        metadata={
            "help": (
                "A Python expression (given as str) to decorate the URL (which is "
                "available as variable `url` in the expression) e.g. to decorate it "
                "with an auth token."
            ),
            "env_var": False,
            "required": False,
        },
    )


class StorageProvider(StorageProviderBase):
    def __post_init__(self):
        self.username = self.settings.username
        self.password = self.settings.password
        if self.password is not None:
            get_logger().warning(
                "A password has been specified -- it will be printed "
                "in plaintext when Snakemake displays the "
                "inputs/outputs of jobs! Only use this option in "
                "trusted environments."
            )
        self.host = self.settings.host
        self.port = self.settings.port
        # List of error codes that there is no point in retrying
        self.no_retry_codes = [
            3000,
            3001,
            3002,
            3006,
            3008,
            3009,
            3010,
            3011,
            3013,
            3017,
            3021,
            3025,
            3030,
            3031,
            3032,
        ]

    def url_decorator(self, url: str) -> str:
        if self.settings.url_decorator is not None:
            return eval(self.settings.url_decorator, {"url": url})
        return url

    def _check_status(self, status: XRootDStatus, error_preamble: str):
        if not status.ok:
            if status.errno in self.no_retry_codes:
                raise XRootDFatalException(f"{error_preamble}: {status.message}")
            raise WorkflowError(f"{error_preamble}: {status.message}")

    @classmethod
    def example_queries(cls) -> List[ExampleQuery]:
        """Return an example queries with description for this storage provider (at
        least one)."""
        return [
            ExampleQuery(
                query="root://eosuser.cern.ch//eos/user/s/someuser/somefile.txt",
                description="A file on a XrootD instance not specifying any arguments.",
                type=QueryType.ANY,
            ),
            ExampleQuery(
                query="root://eos/user/s/someuser/somefile.txt",
                description="A file on an XrootD instance where the host has been"
                "specified in the storage object.",
                type=QueryType.ANY,
            ),
        ]

    # ?
    def rate_limiter_key(self, query: str, operation: Operation) -> Any:
        """Return a key for identifying a rate limiter given a query and an operation.

        This is used to identify a rate limiter for the query.
        E.g. for a storage provider like http that would be the host name.
        For s3 it might be just the endpoint URL.
        """
        return "xrootd"

    def default_max_requests_per_second(self) -> float:
        """Return the default maximum number of requests per second for this storage
        provider."""
        return 1.0

    def use_rate_limiter(self) -> bool:
        """Return False if no rate limiting is needed for this provider."""
        return True

    @staticmethod
    def _no_pass_url(url: str) -> str:
        new_url = URL(url)
        if new_url.password == "":
            return str(new_url)
        return str(new_url).replace(new_url.password, "****")

    @staticmethod
    def _no_params_url(url: str) -> str:
        new_url = URL(url)
        if new_url.path_with_params != new_url.path:
            return url.replace(new_url.path_with_params, f"{new_url.path}?****")
        return url

    @staticmethod
    def _safe_to_print_url(url: str) -> str:
        return StorageProvider._no_params_url(StorageProvider._no_pass_url(url))

    def _parse_url(self, query: str) -> List[str] | None:
        url = URL(query)
        user = self.username or url.username
        password = self.password or url.password
        host = self.host or url.hostname
        port = self.port or url.port
        match (user, password):
            case ("", ""):
                user_pass = ""
            case ("", _):
                raise WorkflowError(
                    "XRootD Error: Cannot specify a password without "
                    "specifying a user. You may need to unset the "
                    "`SNAKEMAKE_STORAGE_XROOTD_PASSWORD` environment "
                    "variable"
                )
            case (_, ""):
                user_pass = f"{user}@"
            case (_, _):
                user_pass = f"{user}:{password}@"

        # The XRootD parsing does not understand the host not being there
        if self.host is not None and self.host != url.hostname:
            full_path = f"/{url.hostname}/{url.path_with_params}"
        else:
            full_path = url.path_with_params
        new_url = f"{url.protocol}://{user_pass}{host}:{port}/{full_path}"
        dec_url = self.url_decorator(new_url)
        full_url = URL(dec_url)
        if not full_url.is_valid():
            if URL(new_url).is_valid():
                raise WorkflowError(
                    f"XRootD Error: URL {self._safe_to_print_url(dec_url)} was made"
                    "invalid when applying the url_decorator"
                )
            else:
                raise WorkflowError(
                    f"XRootD Error: URL {self._safe_to_print_url(new_url)} is invalid"
                )

        dirname, filename = os.path.split(full_url.path)
        # We need a trailing / to keep XRootD happy
        dirname += "/"

        # XRootD also needs absoulte paths
        if not dirname.startswith("/"):
            dirname = f"/{dirname}"

        return full_url, dirname, filename

    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """Return whether the given query is valid for this storage provider."""
        # Ensure that also queries containing wildcards (e.g. {sample}) are accepted
        # and considered valid. The wildcards will be resolved before the storage
        # object is actually used.
        url = URL(query)
        # XRootD URL.is_valid() is very permissive so regex to be safe
        if not url.is_valid() or re.findall(r"((?:[A-Za-z]+://))(.+)", query) == []:
            return StorageQueryValidationResult(
                valid=False,
                reason="Malformed XRootD url",
                query=query,
            )
        if url.protocol in ["http", "https"]:
            return StorageQueryValidationResult(
                valid=False, reason="Protocol cannot be http or https", query=query
            )
        return StorageQueryValidationResult(valid=True, query=query)

    def postprocess_query(self, query: str) -> str:
        """Postprocess the query by adding any global settings to the url."""
        url, _, _ = self._parse_url(query)
        return str(url)

    def safe_print(self, query: str) -> str:
        return StorageProvider._safe_to_print_url(query)


# Required:
# Implementation of storage object. If certain methods cannot be supported by your
# storage (e.g. because it is read-only see
# snakemake-storage-http for comparison), remove the corresponding base classes
# from the list of inherited items.
class StorageObject(StorageObjectRead, StorageObjectWrite):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        # Does is_valid_query happen before this or we need to verify here too?
        self.url, self.dirname, self.filename = self.provider._parse_url(self.query)
        self.path = self.url.path
        self.file_system = client.FileSystem(self.url.hostid)

    # TODO
    async def inventory(self, cache: IOCacheStorageInterface):
        """From this file, try to find as much existence and modification date
        information as possible. Only retrieve that information that comes for free
        given the current object.
        """
        # This is optional and can be left as is

        # If this is implemented in a storage object, results have to be stored in
        # the given IOCache object, using self.cache_key() as key.
        # Optionally, this can take a custom local suffix, needed e.g. when you want
        # to cache more items than the current query: self.cache_key(local_suffix=...)
        pass

    def get_inventory_parent(self) -> Optional[str]:
        """Return the parent directory of this object."""
        # this is optional and can be left as is
        return str(self.url).replace(self.filename, "")

    def local_suffix(self) -> str:
        """Return a unique suffix for the local path, determined from self.query."""
        # path always has a '/' at the end which we do not want here
        return str(self.path)[2:]

    # Check but should be nothing?
    def cleanup(self):
        """Perform local cleanup of any remainders of the storage object."""
        # self.local_path() should not be removed, as this is taken care of by
        # Snakemake.
        pass

    @xrootd_retry
    def exists(self) -> bool:
        return self._exists(self.query)

    def _exists(self, query) -> bool:
        # we split up the query again so that this can be re-used to check the
        # existence of other files e.g. the parent directory.
        url, dirname, filename = self.provider._parse_url(query)
        status, stat_info = self.file_system.stat(url.path_with_params)
        # a bit special, 3011 == file not found
        if not status.ok:
            if status.errno == 3011:
                return False
            self.provider._check_status(
                status,
                "Error checking existence of "
                f"{self.provider._safe_to_print_url(query)} on XRootD",
            )
        return True

    @xrootd_retry
    def mtime(self) -> float:
        # return the modification time
        status, stat = self.file_system.stat(self.url.path_with_params)
        self.provider._check_status(
            status,
            f"Error checking info of {self.provider._safe_to_print_url(self.query)}",
        )
        return stat.modtime

    @xrootd_retry
    def size(self) -> int:
        # return the size in bytes
        status, stat = self.file_system.stat(self.url.path_with_params)
        self.provider._check_status(
            status,
            f"Error checking info of {self.provider._safe_to_print_url(self.query)}",
        )
        return stat.size

    @xrootd_retry
    def retrieve_object(self):
        # Ensure that the object is accessible locally under self.local_path()
        # check if dir

        process = client.CopyProcess()

        # local path must be an absoulte path as well
        local_path = os.path.abspath(self.local_path())
        process.add_job(str(self.url), local_path, force=True)

        process.prepare()
        status, returns = process.run()
        self.provider._check_status(
            status,
            f"Error downloading from {self.provider._safe_to_print_url(self.query)}",
        )
        self.provider._check_status(
            returns[0]["status"],
            f"Error downloading from {self.provider._safe_to_print_url(self.query)}",
        )

    # The following to methods are only required if the class inherits from
    # StorageObjectReadWrite.

    @xrootd_retry
    def _makedirs(self):
        if not self._exists(self.get_inventory_parent()):
            status, _ = self.file_system.mkdir(self.dirname, MkDirFlags.MAKEPATH)
            self.provider._check_status(
                status,
                "Error creating directory "
                f"{self.provider._safe_to_print_url(self.query)}",
            )

    @xrootd_retry
    def store_object(self):
        # Ensure that the object is stored at the location specified by
        # self.local_path().
        process = client.CopyProcess()
        self._makedirs()
        local_path = os.path.abspath(self.local_path())
        process.add_job(local_path, str(self.url), force=True)
        process.prepare()
        status, returns = process.run()
        self.provider._check_status(
            status, f"Error uploading to {self.provider._safe_to_print_url(self.query)}"
        )
        self.provider._check_status(
            returns[0]["status"],
            f"Error uploading to {self.provider._safe_to_print_url(self.query)}",
        )

    @xrootd_retry
    def remove(self):
        # Remove the object from the storage.
        status, stat = self.file_system.stat(self.path)
        self.provider._check_status(status, f"Error could not stat {self.path}")
        if stat.flags & StatInfoFlags.IS_DIR:
            rm_func = self.file_system.rmdir
        else:
            rm_func = self.file_system.rm
        status, _ = rm_func(self.path)
        self.provider._check_status(status, f"Error removing {self.path}")

    # The following to methods are only required if the class inherits from
    # StorageObjectGlob.

    # TODO
    @retry_decorator
    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query."""
        # This is used by glob_wildcards() to find matches for wildcards in the query.
        # The method has to return concretized queries without any remaining wildcards.
        # Use snakemake_executor_plugins.io.get_constant_prefix(self.query) to get the
        # prefix of the query before the first wildcard.
        # TODO add support for listing files here
        raise NotImplementedError()
