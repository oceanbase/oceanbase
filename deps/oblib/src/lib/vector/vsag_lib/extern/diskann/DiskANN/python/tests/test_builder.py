# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT license.

import unittest

import diskannpy as dap
import numpy as np


class TestBuildDiskIndex(unittest.TestCase):
    def test_valid_shape(self):
        rng = np.random.default_rng(12345)
        rando = rng.random((1000, 100, 5), dtype=np.single)
        with self.assertRaises(ValueError):
            dap.build_disk_index(
                data=rando,
                distance_metric="l2",
                index_directory="test",
                complexity=5,
                graph_degree=5,
                search_memory_maximum=0.01,
                build_memory_maximum=0.01,
                num_threads=1,
                pq_disk_bytes=0,
            )

        rando = rng.random(1000, dtype=np.single)
        with self.assertRaises(ValueError):
            dap.build_disk_index(
                data=rando,
                distance_metric="l2",
                index_directory="test",
                complexity=5,
                graph_degree=5,
                search_memory_maximum=0.01,
                build_memory_maximum=0.01,
                num_threads=1,
                pq_disk_bytes=0,
            )

    def test_value_ranges_build(self):
        good_ranges = {
            "vector_dtype": np.single,
            "distance_metric": "l2",
            "graph_degree": 5,
            "complexity": 5,
            "search_memory_maximum": 0.01,
            "build_memory_maximum": 0.01,
            "num_threads": 1,
            "pq_disk_bytes": 0,
        }
        bad_ranges = {
            "vector_dtype": np.float64,
            "distance_metric": "soups this time",
            "graph_degree": -1,
            "complexity": -1,
            "search_memory_maximum": 0,
            "build_memory_maximum": 0,
            "num_threads": -1,
            "pq_disk_bytes": -1,
        }
        for bad_value_key in good_ranges.keys():
            kwargs = good_ranges.copy()
            kwargs[bad_value_key] = bad_ranges[bad_value_key]
            with self.subTest(
                f"testing bad value key: {bad_value_key} with bad value: {bad_ranges[bad_value_key]}"
            ):
                with self.assertRaises(ValueError):
                    dap.build_disk_index(data="test", index_directory="test", **kwargs)


class TestBuildMemoryIndex(unittest.TestCase):
    def test_valid_shape(self):
        rng = np.random.default_rng(12345)
        rando = rng.random((1000, 100, 5), dtype=np.single)
        with self.assertRaises(ValueError):
            dap.build_memory_index(
                data=rando,
                distance_metric="l2",
                index_directory="test",
                complexity=5,
                graph_degree=5,
                alpha=1.2,
                num_threads=1,
                use_pq_build=False,
                num_pq_bytes=0,
                use_opq=False,
            )

        rando = rng.random(1000, dtype=np.single)
        with self.assertRaises(ValueError):
            dap.build_memory_index(
                data=rando,
                distance_metric="l2",
                index_directory="test",
                complexity=5,
                graph_degree=5,
                alpha=1.2,
                num_threads=1,
                use_pq_build=False,
                num_pq_bytes=0,
                use_opq=False,
            )

    def test_value_ranges_build(self):
        good_ranges = {
            "vector_dtype": np.single,
            "distance_metric": "l2",
            "graph_degree": 5,
            "complexity": 5,
            "alpha": 1.2,
            "num_threads": 1,
            "num_pq_bytes": 0,
        }
        bad_ranges = {
            "vector_dtype": np.float64,
            "distance_metric": "soups this time",
            "graph_degree": -1,
            "complexity": -1,
            "alpha": -1.2,
            "num_threads": 1,
            "num_pq_bytes": -60,
        }
        for bad_value_key in good_ranges.keys():
            kwargs = good_ranges.copy()
            kwargs[bad_value_key] = bad_ranges[bad_value_key]
            with self.subTest(
                f"testing bad value key: {bad_value_key} with bad value: {bad_ranges[bad_value_key]}"
            ):
                with self.assertRaises(ValueError):
                    dap.build_memory_index(
                        data="test",
                        index_directory="test",
                        use_pq_build=True,
                        use_opq=False,
                        **kwargs,
                    )
