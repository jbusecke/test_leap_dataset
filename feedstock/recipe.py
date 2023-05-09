
### needs to be here otherwise the import fails
"""Modified transforms from Pangeo Forge"""

import abc
import apache_beam as beam
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    # OpenURLWithFSSpec, 
    OpenWithXarray, 
    StoreToZarr
    )

import datetime
import typing as t
import numpy as np
import time
from dataclasses import dataclass

from typing import Optional, Union

from pangeo_forge_recipes.transforms import _add_keys, CacheFSSpecTarget, open_url

def _shard(elem, num_shards: int):
    return (np.random.randint(0, num_shards), elem)

class RateLimit(beam.PTransform, abc.ABC):
    """PTransform to extend to apply a global rate limit to an operation.

    The input PCollection and be of any type and the output will be whatever is
    returned by the `process` method.
    """

    def __init__(self,
                 global_rate_limit_qps: int,
                 latency_per_request: float,
                 max_concurrent_requests: int):
        """Creates a RateLimit object.

        global_rate_limit_qps and latency_per_request are used to determine how the
        data should be sharded via:
        global_rate_limit_qps * latency_per_request.total_seconds()

        For example, global_rate_limit_qps = 500 and latency_per_request=.5 seconds.
        Then the data will be sharded into 500*.5=250 groups.  Each group can be
        processed in parallel and will call the 'process' function at most once
        every latency_per_request.

        It is important to note that the max QPS may not be reach based on how many
        workers are scheduled.

        Args:
            global_rate_limit_qps: QPS to rate limit requests across all workers to.
            latency_per_request: The expected latency per request.
            max_concurrent_requests: Maximum allowed concurrent api requests to EE.
        """

        self._rate_limit = global_rate_limit_qps
        self._latency_per_request = datetime.timedelta(seconds=latency_per_request)
        self._num_shards = max(1, min(int(self._rate_limit * self._latency_per_request.total_seconds()),
                                      max_concurrent_requests))

    @abc.abstractmethod
    def process(self, elem: t.Any):
        """Process is the operation that will be rate limited.

        Results will be yielded each time time the process method is called.

        Args:
            elem: The individual element to process.

        Returns:
            Output can be anything, output will be the output of the RateLimit
            PTransform.
        """
        pass

    def expand(self, pcol: beam.PCollection):
        return (pcol
                | beam.Map(_shard, self._num_shards)
                | beam.GroupByKey()
                | beam.ParDo(
                    _RateLimitDoFn(self.process, self._latency_per_request)))


class _RateLimitDoFn(beam.DoFn):
    """DoFn that ratelimits calls to rate_limit_fn."""

    def __init__(self, rate_limit_fn: t.Callable, wait_time: datetime.timedelta):
        self._rate_limit_fn = rate_limit_fn
        self._wait_time = wait_time
        self._is_generator = inspect.isgeneratorfunction(self._rate_limit_fn)  # type: ignore

    def process(self, keyed_elem: t.Tuple[t.Any, t.Iterable[t.Any]]):
        shard, elems = keyed_elem

        start_time = datetime.datetime.now()
        end_time = None
        for elem in elems:
            if end_time is not None and (end_time - start_time) < self._wait_time:
                wait_time = (self._wait_time - (end_time - start_time))
                time.sleep(wait_time.total_seconds())
            start_time = datetime.datetime.now()
            if self._is_generator:
                yield from self._rate_limit_fn(elem)
            else:
                yield self._rate_limit_fn(elem)
            end_time = datetime.datetime.now()


# This has side effects if using a cache
@dataclass
class OpenURLWithFSSpec(RateLimit):
    """Open indexed string-based URLs with fsspec.

    :param cache: If provided, data will be cached at this url path before opening.
    :param secrets: If provided these secrets will be injected into the URL as a query string.
    :param open_kwargs: Extra arguments passed to fsspec.open.
    """

    cache: Optional[Union[str, CacheFSSpecTarget]] = None
    secrets: Optional[dict] = None
    open_kwargs: Optional[dict] = None

    # def expand(self, pcoll):
    def process(self, elem: t.Any):
        if isinstance(self.cache, str):
            cache = CacheFSSpecTarget.from_url(self.cache)
        else:
            cache = self.cache
        return elem | "Open with fsspec" >> beam.Map(
            _add_keys(open_url),
            cache=cache,
            secrets=self.secrets,
            open_kwargs=self.open_kwargs,
        )




transform_dict = {}

# for testing
years = range(2001, 2006)
# years = range(2001, 2022)

#trying to avoid time out errors
# see https://stackoverflow.com/a/73746020
# from aiohttp import ClientTimeout
# open_kwargs = {"client_kwargs":{"timeout": ClientTimeout(total=5000, connect=1000)}}

## Monthly version

input_urls = [
    f'https://zenodo.org/record/7761881/files/METAFLUX_GPP_RECO_monthly_{year}.nc?download=1' for year in years
]

pattern = pattern_from_file_sequence(input_urls, concat_dim='time')
transforms = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()#open_kwargs=open_kwargs
    | OpenWithXarray()
    | StoreToZarr(
        store_name="METAFLUX_GPP_RECO_monthly.zarr",
        combine_dims=pattern.combine_dim_keys,
    )
)
transform_dict['METAFLUX_GPP_RECO_monthly'] = transforms


## daily version
input_urls = [f"https://zenodo.org/record/7761881/files/METAFLUX_GPP_RECO_daily_{year}{month:02}.nc?download=1" for year in years for month in range(1,12) ]
pattern = pattern_from_file_sequence(input_urls, concat_dim='time')
transforms = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()#open_kwargs=open_kwargs
    | OpenWithXarray()
    | StoreToZarr(
        store_name="METAFLUX_GPP_RECO_daily.zarr",
        combine_dims=pattern.combine_dim_keys,
    )
)
transform_dict['METAFLUX_GPP_RECO_daily'] = transforms