from dataclasses import dataclass, field
import os
import re
from urllib.parse import quote
from typing import Any, Iterable, Optional, List, Type
import importlib

from reretry import retry

from XRootD import client
from XRootD.client.flags import DirListFlags, MkDirFlags, StatInfoFlags
from XRootD.client.responses import XRootDStatus, StatInfo, DirectoryList
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
    StorageObjectGlob,
)
from snakemake_interface_storage_plugins.io import (
    IOCacheStorageInterface,
    get_constant_prefix,
)


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
    protocol: Optional[str] = field(
        default=None,
        metadata={
            "help": (
                "Preferred XRootD authentication protocol(s), passed through "
                "to the client unchanged. Comma-separated values such as "
                "'krb5,unix' are allowed."
            ),
            "env_var": False,
            "required": False,
        },
    )
    url_decorator: Optional[str] = field(
        default=None,
        metadata={
            "help": (
                "Entry point to a function (e.g. 'module:func') or a Python "
                "expression (e.g. 'url + \"?foo=bar\"') that decorates the URL. "
                "Function expects a single string argument (URL) and returns "
                "the decorated URL. Expression has 'url' available."
            ),
            "env_var": False,
            "required": False,
        },
    )
    glob_wildcards_max_depth: int = field(
        default=100,
        metadata={
            "help": (
                "Maximum directory nesting depth when calling `glob_wildcards`. "
                "Guards against symlink loops or misbehaving servers causing "
                "unbounded recursion."
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
        self.dec_func = None
        if self.settings.url_decorator is not None:
            self.dec_func = self.load_decorator()

    def load_decorator(self):
        if (
            self.settings.url_decorator is not None
            and ":" in self.settings.url_decorator
        ):
            try:
                module_name, func_name = self.settings.url_decorator.split(":", 1)
                module = importlib.import_module(module_name)
                return getattr(module, func_name)
            except (ImportError, AttributeError) as e:
                get_logger().warning(
                    f"Failed to load url_decorator entry point '{self.settings.url_decorator}': {e}"
                )
                return None
        return None

    def url_decorator(self, url: str) -> str:
        if self.dec_func is not None:
            return self.dec_func(url)
        if (
            self.settings.url_decorator is not None
            and ":" not in self.settings.url_decorator
        ):
            # Fallback for backwards compatibility (eval)
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

    @staticmethod
    def _append_query_param(url: str, key: str, value: str) -> str:
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}{key}={quote(value, safe=',')}"

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
        if self.settings.protocol:
            new_url = self._append_query_param(
                new_url, "xrd.wantprot", self.settings.protocol
            )
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
class StorageObject(StorageObjectRead, StorageObjectWrite, StorageObjectGlob):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        # Does is_valid_query happen before this or we need to verify here too?
        self.url, self.dirname, self.filename = self.provider._parse_url(self.query)
        self.path = self.url.path
        self.file_system = client.FileSystem(str(self.url))

    @xrootd_retry
    def _stat(
        self, path_with_params: str, allow_missing: bool = False
    ) -> Optional[StatInfo]:
        status, stat_info = self.file_system.stat(path_with_params)
        # 3011==file not found is in the no_retry_codes list, so we need to handle it here
        if allow_missing and not status.ok and status.errno == 3011:
            return None
        self.provider._check_status(
            status,
            f"Error checking info of {self.provider._safe_to_print_url(self.query)}",
        )
        return stat_info

    def _exists(self, path_with_params: str) -> bool:
        return self._stat(path_with_params, allow_missing=True) is not None

    @xrootd_retry
    def _makedirs(self):
        if not self._exists(URL(self.get_inventory_parent()).path_with_params):
            status, _ = self.file_system.mkdir(self.dirname, MkDirFlags.MAKEPATH)
            self.provider._check_status(
                status,
                "Error creating directory "
                f"{self.provider._safe_to_print_url(self.query)}",
            )

    @staticmethod
    def _url_with_new_path(url: str, new_path: str) -> str:
        parsed = URL(url)
        parsed_params = parsed.path_with_params[len(parsed.path) :]

        new_url = (
            parsed.protocol + "://" + parsed.hostid + "/" + new_path + parsed_params
        )
        if not URL(new_url).is_valid():
            raise WorkflowError(
                f"XRootD Error: URL {StorageProvider._safe_to_print_url(new_url)} is invalid"
            )
        return new_url

    @xrootd_retry
    def _dirlist(self, path_with_params: str) -> DirectoryList:
        status, dirlist = self.file_system.dirlist(path_with_params, DirListFlags.STAT)
        self.provider._check_status(
            status,
            f"Error listing directory {self.provider._safe_to_print_url(self.query)}",
        )
        return dirlist

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
        return self._url_with_new_path(str(self.url), self.dirname)

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

    def exists(self) -> bool:
        return self._exists(self.url.path_with_params)

    def mtime(self) -> float:
        # return the modification time
        stat = self._stat(self.url.path_with_params)
        return stat.modtime

    def size(self) -> int:
        # return the size in bytes
        stat = self._stat(self.url.path_with_params)
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
        stat = self._stat(self.url.path_with_params)
        if stat.flags & StatInfoFlags.IS_DIR:
            rm_func = self.file_system.rmdir
        else:
            rm_func = self.file_system.rm
        status, _ = rm_func(self.url.path_with_params)
        self.provider._check_status(
            status, f"Error removing {self.provider._safe_to_print_url(self.query)}"
        )

    # The following to methods are only required if the class inherits from
    # StorageObjectGlob.

    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query."""
        # This is used by glob_wildcards() to find matches for wildcards in the query.
        # The method has to return concretized queries without any remaining wildcards.
        # Use snakemake_executor_plugins.io.get_constant_prefix(self.query) to get the
        # prefix of the query before the first wildcard.
        const_prefix = get_constant_prefix(self.url.path, strip_incomplete_parts=True)
        glob_query = self._url_with_new_path(str(self.url), const_prefix)
        yield from self._list_recursive(glob_query)

    def _list_recursive(self, query: str, _depth: int = 0) -> Iterable[str]:
        if _depth > self.provider.settings.glob_wildcards_max_depth:
            raise WorkflowError(
                "XRootD Error: directory nesting exceeds maximum depth of "
                f"{self.provider.settings.glob_wildcards_max_depth} while listing "
                f"{self.provider._safe_to_print_url(query)}"
            )

        url = URL(query)
        # First check if the path is a directory or a file. If it is a file, we can
        # return it directly. (Only need to do this for the first call, since we check
        # it before recursing into subdirectories.)
        if _depth == 0:
            stat_info = self._stat(url.path_with_params, allow_missing=True)
            # Prefix does not exist, return nothing.
            if stat_info is None:
                return
            if not stat_info.flags & StatInfoFlags.IS_DIR:
                yield query
                return

        dirlist = self._dirlist(url.path_with_params)

        for entry in dirlist.dirlist:
            # Dirlist should never return entries with empty names or "." or ".." or
            # names containing slashes, but we check for that anyway to be safe.
            if (
                not entry.name
                or entry.name in (".", "..")
                or "/" in entry.name
                or "\\" in entry.name
            ):
                get_logger().warning(
                    "Skipping suspicious directory entry name "
                    f"{entry.name!r} returned while listing "
                    f"{self.provider._safe_to_print_url(query)}"
                )
                continue

            child_query = self._url_with_new_path(
                str(url), url.path.rstrip("/") + "/" + entry.name
            )
            if (
                entry.statinfo is not None
                and entry.statinfo.flags & StatInfoFlags.IS_DIR
            ):
                yield from self._list_recursive(child_query, _depth + 1)
            else:
                yield child_query
