from dataclasses import dataclass, field
from pathlib import PosixPath
from typing import Any, Iterable, Mapping, Optional, List
from urllib.parse import urlparse

from XRootD import client

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
    retry_decorator,
)
from snakemake_interface_storage_plugins.io import IOCacheStorageInterface


def parse_query_params(query_params: List[str]):
    """Parse query params from command line."""
    return dict(param.split("=") for param in query_params)


def unparse_query_params(query_params: Mapping[str, str]):
    """Unparse query params to command line."""
    return [f"{key}={value}" for key, value in query_params.items()]


@dataclass
class StorageProviderSettings(StorageProviderSettingsBase):
    host: Optional[str] = field(
        default=None,
        metadata={
            "help": "The XrootD host to connect to",
            "env_var": False,
            "required": True,
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
            "help": "The password to use for authentication",
            "env_var": True,
            "required": False,
        },
    )
    query_params: Mapping[str, str] = field(
        default_factory=dict,
        metadata={
            "help": "Optional query parameters for XRootD (e.g. xrd.wantprot=unix, "
            "authz=XXXXXX)",
            "env_var": True,
            # Optionally specify a function that parses the value given by the user.
            # This is useful to create complex types from the user input.
            "parse_func": parse_query_params,
            # If a parse_func is specified, you also have to specify an unparse_func
            # that converts the parsed value back to a string.
            "unparse_func": unparse_query_params,
            "required": False,
            "nargs": "+",
        },
    )


# Required:
# Implementation of your storage provider
# This class can be empty as the one below.
# You can however use it to store global information or maintain e.g. a connection
# pool.
class StorageProvider(StorageProviderBase):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.

        userpass = ""
        if self.settings.username:
            userpass += self.provider.settings.username
        if self.settings.password:
            userpass += f":{self.settings.password}"
        if userpass:
            userpass += "@"
        port = f":{self.settings.port}" if self.settings.port else ""
        self.netloc = f"{userpass}{self.settings.host}{port}"

        self.filesystem_client = client.FileSystem(f"root://{self.netloc}")
        self.file_client = client.File()

    @classmethod
    def example_queries(cls) -> List[ExampleQuery]:
        """Return an example queries with description for this storage provider (at
        least one)."""
        return [
            ExampleQuery(
                query="root://eos/user/s/someuser/somefile.txt",
                description="A file on an XrootD instance. Note that we omit the host "
                "name from the query as that is given via the settings of the storage "
                "provider.",
                type=QueryType.ANY,
            )
        ]

    def rate_limiter_key(self, query: str, operation: Operation) -> Any:
        """Return a key for identifying a rate limiter given a query and an operation.

        This is used to identify a rate limiter for the query.
        E.g. for a storage provider like http that would be the host name.
        For s3 it might be just the endpoint URL.
        """
        return self.settings.host

    def default_max_requests_per_second(self) -> float:
        """Return the default maximum number of requests per second for this storage
        provider."""
        return 10.0

    def use_rate_limiter(self) -> bool:
        """Return False if no rate limiting is needed for this provider."""
        return True

    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """Return whether the given query is valid for this storage provider."""
        # Ensure that also queries containing wildcards (e.g. {sample}) are accepted
        # and considered valid. The wildcards will be resolved before the storage
        # object is actually used.
        parsed = urlparse(query)
        if parsed.scheme != "root":
            return StorageQueryValidationResult(
                valid=False,
                reason="Scheme must be root://",
                query=query,
            )
        else:
            return StorageQueryValidationResult(valid=True, query=query)

    @property
    def query_params(self):
        return "&".join(
            f"{key}={val}" for key, val in self.settings.query_params.items()
        )


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
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.
        self.parsed = urlparse(self.query)
        self.path = f"/{self.parsed.netloc}{self.parsed.path}"
        self.url = self._get_url(self.path)

    def _get_url(self, path):
        return f"root://{self.provider.netloc}/{path}{self.provider.query_params}"

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
        return None

    def local_suffix(self) -> str:
        """Return a unique suffix for the local path, determined from self.query."""
        return f"{self.parsed.netloc}/{self.parsed.path}"

    def cleanup(self):
        """Perform local cleanup of any remainders of the storage object."""
        # self.local_path() should not be removed, as this is taken care of by
        # Snakemake.
        pass

    # Fallible methods should implement some retry logic.
    # The easiest way to do this (but not the only one) is to use the retry_decorator
    # provided by snakemake-interface-storage-plugins.
    @retry_decorator
    def exists(self) -> bool:
        return self._exists(self.path)

    def _exists(self, path) -> bool:
        status, _ = self.provider.filesystem_client.stat(path)
        if status.errno == 3011 or status.errno == 3005:
            return False
        if not status.ok:
            raise IOError(
                f"Error checking existence of {path} on XRootD: {status.message}"
            )
        return True

    @retry_decorator
    def mtime(self) -> float:
        # return the modification time
        status, stat = self.provider.filesystem_client.stat(self.path)
        if not status.ok:
            raise IOError(f"Error checking existence of {self.url}: {status.message}")
        return stat.modtime

    @retry_decorator
    def size(self) -> int:
        # return the size in bytes
        status, stat = self.provider.filesystem_client.stat(self.path)
        if not status.ok:
            raise IOError(f"Error checking existence of {self.url}: {status.message}")
        return stat.size

    @retry_decorator
    def retrieve_object(self):
        # Ensure that the object is accessible locally under self.local_path()
        # check if dir

        process = client.CopyProcess()

        # TODO is special handling for directories needed?
        # if stat.flags & client.flags.StatInfoFlags.IS_DIR:
        #     self.local_path().mkdir(parents=True, exist_ok=True)
        #     _, listing = self.provider.filesystem_client.dirlist(
        #         self.path, flags=client.flags.DirListFlags.STAT)
        #     for item in listing:
        #         item_path = f"{self.path}/{item.name}"
        #         if item.statinfo.flags & client.flags.StatInfoFlags.IS_DIR:

        #         else:
        #             process.add_job(self._get_url(item_path),
        #                 str(self.local_path() / item_path), force=True)
        # else:
        process.add_job(self.url, str(self.local_path()), force=True)

        process.prepare()
        status, returns = process.run()
        if not status.ok or not returns[0]["status"].ok:
            raise IOError(f"Error donwloading from {self.url}: {status.message}")

    # The following to methods are only required if the class inherits from
    # StorageObjectReadWrite.

    @retry_decorator
    def store_object(self):
        # Ensure that the object is stored at the location specified by
        # self.local_path().
        def mkdir(path):
            if path != "." and path != "/" and not self._exists(path):
                status, _ = self.provider.filesystem_client.mkdir(
                    path + "/", flags=client.flags.MkDirFlags.MAKEPATH
                )
                if not status.ok:
                    raise IOError(f"Error creating directory {path}: {status.message}")

        process = client.CopyProcess()
        if self.local_path().is_dir():
            mkdir(self.path)

            for file in self.local_path().iterdir():
                process.add_job(str(file), f"{self.url}/{file.name}", force=True)
        else:
            parent = str(PosixPath(self.path).parent)
            mkdir(parent)
            process.add_job(str(self.local_path()), self.url, force=True)

        process.prepare()
        status, returns = process.run()
        if not status.ok or not returns[0]["status"].ok:
            raise IOError(f"Error uploading to {self.url}: {status.message}")

    @retry_decorator
    def remove(self):
        # Remove the object from the storage.
        _, stat = self.provider.filesystem_client.stat(self.path)
        if stat.flags & client.flags.StatInfoFlags.IS_DIR:
            self.provider.filesystem_client.rmdir(self.path)
        else:
            self.provider.filesystem_client.rm(self.path)

    # The following to methods are only required if the class inherits from
    # StorageObjectGlob.

    @retry_decorator
    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query."""
        # This is used by glob_wildcards() to find matches for wildcards in the query.
        # The method has to return concretized queries without any remaining wildcards.
        # Use snakemake_executor_plugins.io.get_constant_prefix(self.query) to get the
        # prefix of the query before the first wildcard.
        # TODO add support for listing files here
        raise NotImplementedError()
