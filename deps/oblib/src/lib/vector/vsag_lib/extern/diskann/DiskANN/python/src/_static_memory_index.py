# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT license.

import os
import warnings
from typing import Optional

import numpy as np

from . import (
    DistanceMetric,
    QueryResponse,
    QueryResponseBatch,
    VectorDType,
    VectorLike,
    VectorLikeBatch,
)
from . import _diskannpy as _native_dap
from ._common import (
    _assert,
    _assert_is_nonnegative_uint32,
    _assert_is_positive_uint32,
    _castable_dtype_or_raise,
    _ensure_index_metadata,
    _valid_index_prefix,
    _valid_metric,
)

__ALL__ = ["StaticMemoryIndex"]


class StaticMemoryIndex:
    """
    A StaticMemoryIndex is an immutable in-memory DiskANN index.
    """

    def __init__(
        self,
        index_directory: str,
        num_threads: int,
        initial_search_complexity: int,
        index_prefix: str = "ann",
        distance_metric: Optional[DistanceMetric] = None,
        vector_dtype: Optional[VectorDType] = None,
        dimensions: Optional[int] = None,
    ):
        """
        ### Parameters
        - **index_directory**: The directory containing the index files. This directory must contain the following
          files:
            - `{index_prefix}.data`
            - `{index_prefix}`


          It may also include the following optional files:
            - `{index_prefix}_vectors.bin`: Optional. `diskannpy` builder functions may create this file in the
              `index_directory` if the index was created from a numpy array
            - `{index_prefix}_metadata.bin`: Optional. `diskannpy` builder functions create this file to store metadata
            about the index, such as vector dtype, distance metric, number of vectors and vector dimensionality.
            If an index is built from the `diskann` cli tools, this file will not exist.
        - **num_threads**: Number of threads to use when searching this index. (>= 0), 0 = num_threads in system
        - **initial_search_complexity**: Should be set to the most common `complexity` expected to be used during the
          life of this `diskannpy.DynamicMemoryIndex` object. The working scratch memory allocated is based off of
          `initial_search_complexity` * `search_threads`. Note that it may be resized if a `search` or `batch_search`
          operation requests a space larger than can be accommodated by these values.
        - **index_prefix**: The prefix of the index files. Defaults to "ann".
        - **distance_metric**: A `str`, strictly one of {"l2", "mips", "cosine"}. `l2` and `cosine` are supported for all 3
          vector dtypes, but `mips` is only available for single precision floats. Default is `None`. **This
          value is only used if a `{index_prefix}_metadata.bin` file does not exist.** If it does not exist,
          you are required to provide it.
        - **vector_dtype**: The vector dtype this index has been built with. **This value is only used if a
          `{index_prefix}_metadata.bin` file does not exist.** If it does not exist, you are required to provide it.
        - **dimensions**: The vector dimensionality of this index. All new vectors inserted must be the same
          dimensionality. **This value is only used if a `{index_prefix}_metadata.bin` file does not exist.** If it
          does not exist, you are required to provide it.
        """
        index_prefix = _valid_index_prefix(index_directory, index_prefix)
        vector_dtype, metric, num_points, dims = _ensure_index_metadata(
            index_prefix,
            vector_dtype,
            distance_metric,
            1,  # it doesn't matter because we don't need it in this context anyway
            dimensions,
        )
        dap_metric = _valid_metric(metric)

        _assert_is_nonnegative_uint32(num_threads, "num_threads")
        _assert_is_positive_uint32(
            initial_search_complexity, "initial_search_complexity"
        )

        self._vector_dtype = vector_dtype
        self._dimensions = dims

        if vector_dtype == np.uint8:
            _index = _native_dap.StaticMemoryUInt8Index
        elif vector_dtype == np.int8:
            _index = _native_dap.StaticMemoryInt8Index
        else:
            _index = _native_dap.StaticMemoryFloatIndex

        self._index = _index(
            distance_metric=dap_metric,
            num_points=num_points,
            dimensions=dims,
            index_path=os.path.join(index_directory, index_prefix),
            num_threads=num_threads,
            initial_search_complexity=initial_search_complexity,
        )

    def search(
        self, query: VectorLike, k_neighbors: int, complexity: int
    ) -> QueryResponse:
        """
        Searches the index by a single query vector.

        ### Parameters
        - **query**: 1d numpy array of the same dimensionality and dtype of the index.
        - **k_neighbors**: Number of neighbors to be returned. If query vector exists in index, it almost definitely
          will be returned as well, so adjust your ``k_neighbors`` as appropriate. Must be > 0.
        - **complexity**: Size of distance ordered list of candidate neighbors to use while searching. List size
          increases accuracy at the cost of latency. Must be at least k_neighbors in size.
        """
        _query = _castable_dtype_or_raise(query, expected=self._vector_dtype)
        _assert(len(_query.shape) == 1, "query vector must be 1-d")
        _assert(
            _query.shape[0] == self._dimensions,
            f"query vector must have the same dimensionality as the index; index dimensionality: {self._dimensions}, "
            f"query dimensionality: {_query.shape[0]}",
        )
        _assert_is_positive_uint32(k_neighbors, "k_neighbors")
        _assert_is_nonnegative_uint32(complexity, "complexity")

        if k_neighbors > complexity:
            warnings.warn(
                f"k_neighbors={k_neighbors} asked for, but list_size={complexity} was smaller. Increasing {complexity} to {k_neighbors}"
            )
            complexity = k_neighbors
        return self._index.search(query=_query, knn=k_neighbors, complexity=complexity)

    def batch_search(
        self,
        queries: VectorLikeBatch,
        k_neighbors: int,
        complexity: int,
        num_threads: int,
    ) -> QueryResponseBatch:
        """
        Searches the index by a batch of query vectors.

        This search is parallelized and far more efficient than searching for each vector individually.

        ### Parameters
        - **queries**: 2d numpy array, with column dimensionality matching the index and row dimensionality being the
          number of queries intended to search for in parallel. Dtype must match dtype of the index.
        - **k_neighbors**: Number of neighbors to be returned. If query vector exists in index, it almost definitely
          will be returned as well, so adjust your ``k_neighbors`` as appropriate. Must be > 0.
        - **complexity**: Size of distance ordered list of candidate neighbors to use while searching. List size
          increases accuracy at the cost of latency. Must be at least k_neighbors in size.
        - **num_threads**: Number of threads to use when searching this index. (>= 0), 0 = num_threads in system
        """

        _queries = _castable_dtype_or_raise(queries, expected=self._vector_dtype)
        _assert(len(_queries.shape) == 2, "queries must must be 2-d np array")
        _assert(
            _queries.shape[1] == self._dimensions,
            f"query vectors must have the same dimensionality as the index; index dimensionality: {self._dimensions}, "
            f"query dimensionality: {_queries.shape[1]}",
        )
        _assert_is_positive_uint32(k_neighbors, "k_neighbors")
        _assert_is_positive_uint32(complexity, "complexity")
        _assert_is_nonnegative_uint32(num_threads, "num_threads")

        if k_neighbors > complexity:
            warnings.warn(
                f"k_neighbors={k_neighbors} asked for, but list_size={complexity} was smaller. Increasing {complexity} to {k_neighbors}"
            )
            complexity = k_neighbors

        num_queries, dim = _queries.shape
        return self._index.batch_search(
            queries=_queries,
            num_queries=num_queries,
            knn=k_neighbors,
            complexity=complexity,
            num_threads=num_threads,
        )
