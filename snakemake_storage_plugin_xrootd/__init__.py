from dataclasses import dataclass, field
import os
import re
from typing import Any, Iterable, Optional, List

from XRootD import client
from XRootD.client.flags import MkDirFlags, StatInfoFlags

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


@dataclass
class StorageProviderSettings(StorageProviderSettingsBase):
    keep_local: Optional[bool] = field(
        default=False,
        metadata={
            "help": "Keep local copy of storage object(s)",
            "env_var": False,
            "required": False,
            "type": bool,
        },
    )
    retrieve: Optional[bool] = field(
        default=False,
        metadata={
            "help": "Download remote files",
            "env_var": False,
            "required": False,
            "type": bool,
        },
    )


class StorageProvider(StorageProviderBase):

    @classmethod
    def example_queries(cls) -> List[ExampleQuery]:
        """Return an example queries with description for this storage provider (at
        least one)."""
        return [
            ExampleQuery(
                query="root://eosuser.cern.ch//eos/user/s/someuser/somefile.txt",
                description="A file on an XrootD instance.",
                type=QueryType.ANY,
            )
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
    def _parse_url(query: str) -> List[str] | None:
        match = re.search(
            r"(?P<domain>(?:[A-Za-z]+://)[A-Za-z0-9:@\_\-\.\{\}]+\:?/)(?P<path>.+)",
            query,
        )
        if match is None:
            return None

        domain = match.group("domain")
        dirname, filename = os.path.split(match.group("path"))
        # We need a trailing / to keep XRootD happy
        dirname += "/"

        # XRootD also needs absoulte paths
        if not dirname.startswith("/"):
            dirname = f"/{dirname}"

        return domain, dirname, filename

    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """Return whether the given query is valid for this storage provider."""
        # Ensure that also queries containing wildcards (e.g. {sample}) are accepted
        # and considered valid. The wildcards will be resolved before the storage
        # object is actually used.
        parsed_query = cls._parse_url(query)
        if parsed_query is None:
            return StorageQueryValidationResult(
                valid=False,
                reason="Malformed XRootD url",
                query=query,
            )
        else:
            return StorageQueryValidationResult(valid=True, query=query)


# Required:
# Implementation of storage object. If certain methods cannot be supported by your
# storage (e.g. because it is read-only see
# snakemake-storage-http for comparison), remove the corresponding base classes
# from the list of inherited items.
# class StorageObject(StorageObjectRead, StorageObjectWrite, StorageObjectGlob):
class StorageObject(StorageObjectRead, StorageObjectWrite):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        # Does is_valid_query happen before this or we need to verify here too?
        self.domain, self.dirname, self.filename = self.provider._parse_url(self.query)
        self.path = os.path.join(self.dirname, self.filename)
        self.file_system = client.FileSystem(self.domain)
        self.keep_local = self.provider.settings.keep_local
        self.retrieve = self.provider.settings.retrieve

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
        return self.domain + self.dirname

    def local_suffix(self) -> str:
        """Return a unique suffix for the local path, determined from self.query."""
        # path always has a '/' at the end which we do not want here
        return str(self.path)[1:]

    # Check but should be nothing?
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
        return self._exists(self.query)

    def _exists(self, query) -> bool:
        # we split up the query again so that this can be re-used to check the
        # existence of other files e.g. the parent directory.
        domain, dirname, filename = self.provider._parse_url(query)
        status, stat_info = self.file_system.stat(os.path.join(dirname, filename))
        if not status.ok:
            if status.errno == 3011:
                return False
            raise IOError(
                f"Error checking existence of {query} on XRootD: {status.message}"
            )
        return True

    def mtime(self) -> float:
        # return the modification time
        status, stat = self.file_system.stat(self.path)
        if not status.ok:
            raise IOError(f"Error checking existence of {self.query}: {status.message}")
        return stat.modtime

    def size(self) -> int:
        # return the size in bytes
        status, stat = self.file_system.stat(self.path)
        if not status.ok:
            raise IOError(f"Error checking existence of {self.query}: {status.message}")
        return stat.size

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

        # local path must be an absoulte path as well
        local_path = os.path.abspath(self.local_path())
        process.add_job(self.query, local_path, force=True)

        process.prepare()
        status, returns = process.run()
        if not status.ok or not returns[0]["status"].ok:
            raise IOError(f"Error donwloading from {self.query}: {status.message}")

    # The following to methods are only required if the class inherits from
    # StorageObjectReadWrite.

    def _makedirs(self):
        if not self._exists(self.get_inventory_parent()):
            status, _ = self.file_system.mkdir(self.dirname, MkDirFlags.MAKEPATH)
            if not status.ok:
                raise IOError(
                    f"Error creating directory {self.query}: {status.message}"
                )

    def store_object(self):
        # Ensure that the object is stored at the location specified by
        # self.local_path().
        process = client.CopyProcess()
        self._makedirs()
        local_path = os.path.abspath(self.local_path())
        process.add_job(local_path, self.query, force=True)
        process.prepare()
        status, returns = process.run()
        if not status.ok or not returns[0]["status"].ok:
            raise IOError(f"Error uploading to {self.query}: {status.message}")

    def remove(self):
        # Remove the object from the storage.
        status, stat = self.file_system.stat(self.path)
        if not status.ok:
            raise IOError(f"Error: could not stat {self.query}: {status.message}")
        if stat.flags & StatInfoFlags.IS_DIR:
            rm_func = self.file_system.rmdir
        else:
            rm_func = self.file_system.rm
        status, _ = rm_func(self.path)
        if not status.ok:
            raise IOError(f"Error removing {self.path}: {status.message}")

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
