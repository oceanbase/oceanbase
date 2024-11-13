# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT license.

import shutil
import unittest
from tempfile import mkdtemp

import diskannpy as dap
import numpy as np
from fixtures import calculate_recall, random_vectors, vectors_as_temp_file
from sklearn.neighbors import NearestNeighbors


def _build_random_vectors_and_index(dtype, metric):
    query_vectors = random_vectors(1000, 10, dtype=dtype)
    index_vectors = random_vectors(10000, 10, dtype=dtype)
    with vectors_as_temp_file(index_vectors) as vector_temp:
        ann_dir = mkdtemp()
        dap.build_disk_index(
            data=vector_temp,
            distance_metric=metric,
            vector_dtype=dtype,
            index_directory=ann_dir,
            graph_degree=16,
            complexity=32,
            search_memory_maximum=0.00003,
            build_memory_maximum=1,
            num_threads=1,
            pq_disk_bytes=0,
        )
    return metric, dtype, query_vectors, index_vectors, ann_dir


class TestStaticDiskIndex(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._test_matrix = [
            _build_random_vectors_and_index(np.float32, "l2"),
            _build_random_vectors_and_index(np.uint8, "l2"),
            _build_random_vectors_and_index(np.int8, "l2"),
        ]
        cls._example_ann_dir = cls._test_matrix[0][4]

    @classmethod
    def tearDownClass(cls) -> None:
        for test in cls._test_matrix:
            try:
                _, _, _, _, ann_dir = test
                shutil.rmtree(ann_dir, ignore_errors=True)
            except:
                pass

    def test_recall_and_batch(self):
        for metric, dtype, query_vectors, index_vectors, ann_dir in self._test_matrix:
            with self.subTest(msg=f"Testing dtype {dtype}"):
                index = dap.StaticDiskIndex(
                    distance_metric="l2",
                    vector_dtype=dtype,
                    index_directory=ann_dir,
                    num_threads=16,
                    num_nodes_to_cache=10,
                )

                k = 5
                diskann_neighbors, diskann_distances = index.batch_search(
                    query_vectors,
                    k_neighbors=k,
                    complexity=5,
                    beam_width=2,
                    num_threads=16,
                )
                if metric == "l2":
                    knn = NearestNeighbors(
                        n_neighbors=100, algorithm="auto", metric="l2"
                    )
                    knn.fit(index_vectors)
                    knn_distances, knn_indices = knn.kneighbors(query_vectors)
                    recall = calculate_recall(diskann_neighbors, knn_indices, k)
                    self.assertTrue(
                        recall > 0.70,
                        f"Recall [{recall}] was not over 0.7",
                    )

    def test_single(self):
        for metric, dtype, query_vectors, index_vectors, ann_dir in self._test_matrix:
            with self.subTest(msg=f"Testing dtype {dtype}"):
                index = dap.StaticDiskIndex(
                    distance_metric="l2",
                    vector_dtype=dtype,
                    index_directory=ann_dir,
                    num_threads=16,
                    num_nodes_to_cache=10,
                )

                k = 5
                ids, dists = index.search(
                    query_vectors[0], k_neighbors=k, complexity=5, beam_width=2
                )
                self.assertEqual(ids.shape[0], k)
                self.assertEqual(dists.shape[0], k)

    def test_value_ranges_search(self):
        good_ranges = {"complexity": 5, "k_neighbors": 10, "beam_width": 2}
        bad_ranges = {"complexity": -1, "k_neighbors": 0, "beam_width": 0}
        for bad_value_key in good_ranges.keys():
            kwargs = good_ranges.copy()
            kwargs[bad_value_key] = bad_ranges[bad_value_key]
            with self.subTest():
                with self.assertRaises(ValueError):
                    index = dap.StaticDiskIndex(
                        distance_metric="l2",
                        vector_dtype=np.single,
                        index_directory=self._example_ann_dir,
                        num_threads=16,
                        num_nodes_to_cache=10,
                    )
                    index.search(query=np.array([], dtype=np.single), **kwargs)

    def test_value_ranges_batch_search(self):
        good_ranges = {
            "complexity": 5,
            "k_neighbors": 10,
            "beam_width": 2,
            "num_threads": 5,
        }
        bad_ranges = {
            "complexity": 0,
            "k_neighbors": 0,
            "beam_width": -1,
            "num_threads": -1,
        }
        for bad_value_key in good_ranges.keys():
            kwargs = good_ranges.copy()
            kwargs[bad_value_key] = bad_ranges[bad_value_key]
            with self.subTest():
                with self.assertRaises(ValueError):
                    index = dap.StaticDiskIndex(
                        distance_metric="l2",
                        vector_dtype=np.single,
                        index_directory=self._example_ann_dir,
                        num_threads=16,
                        num_nodes_to_cache=10,
                    )
                    index.batch_search(
                        queries=np.array([[]], dtype=np.single), **kwargs
                    )
