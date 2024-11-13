# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT license.

import shutil
import tempfile
import unittest
import warnings

import diskannpy as dap
import numpy as np
from fixtures import build_random_vectors_and_memory_index
from sklearn.neighbors import NearestNeighbors


def _calculate_recall(
    result_set_tags: np.ndarray,
    original_indices_to_tags: np.ndarray,
    truth_set_indices: np.ndarray,
    recall_at: int = 5,
) -> float:
    found = 0
    for i in range(0, result_set_tags.shape[0]):
        result_set_set = set(result_set_tags[i][0:recall_at])
        truth_set_set = set()
        for knn_index in truth_set_indices[i][0:recall_at]:
            truth_set_set.add(
                original_indices_to_tags[knn_index]
            )  # mapped into our tag number instead
        found += len(result_set_set.intersection(truth_set_set))
    return found / (result_set_tags.shape[0] * recall_at)


class TestDynamicMemoryIndex(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._test_matrix = [
            build_random_vectors_and_memory_index(np.float32, "l2", with_tags=True),
            build_random_vectors_and_memory_index(np.uint8, "l2", with_tags=True),
            build_random_vectors_and_memory_index(np.int8, "l2", with_tags=True),
            build_random_vectors_and_memory_index(np.float32, "cosine", with_tags=True),
            build_random_vectors_and_memory_index(np.uint8, "cosine", with_tags=True),
            build_random_vectors_and_memory_index(np.int8, "cosine", with_tags=True),
        ]
        cls._example_ann_dir = cls._test_matrix[0][4]

    @classmethod
    def tearDownClass(cls) -> None:
        for test in cls._test_matrix:
            try:
                ann_dir = test[4]
                shutil.rmtree(ann_dir, ignore_errors=True)
            except:
                pass

    def test_recall_and_batch(self):
        for (
            metric,
            dtype,
            query_vectors,
            index_vectors,
            ann_dir,
            vector_bin_file,
            generated_tags,
        ) in self._test_matrix:
            with self.subTest(msg=f"Testing dtype {dtype}"):
                index = dap.DynamicMemoryIndex.from_file(
                    index_directory=ann_dir,
                    max_vectors=11_000,
                    complexity=64,
                    graph_degree=32,
                    num_threads=16,
                )

                k = 5
                diskann_neighbors, diskann_distances = index.batch_search(
                    query_vectors,
                    k_neighbors=k,
                    complexity=5,
                    num_threads=16,
                )
                if metric == "l2" or metric == "cosine":
                    knn = NearestNeighbors(
                        n_neighbors=100, algorithm="auto", metric=metric
                    )
                    knn.fit(index_vectors)
                    knn_distances, knn_indices = knn.kneighbors(query_vectors)
                    recall = _calculate_recall(
                        diskann_neighbors, generated_tags, knn_indices, k
                    )
                    self.assertTrue(
                        recall > 0.70,
                        f"Recall [{recall}] was not over 0.7",
                    )

    def test_single(self):
        for (
            metric,
            dtype,
            query_vectors,
            index_vectors,
            ann_dir,
            vector_bin_file,
            generated_tags,
        ) in self._test_matrix:
            with self.subTest(msg=f"Testing dtype {dtype}"):
                index = dap.DynamicMemoryIndex(
                    distance_metric="l2",
                    vector_dtype=dtype,
                    dimensions=10,
                    max_vectors=11_000,
                    complexity=64,
                    graph_degree=32,
                    num_threads=16,
                )
                index.batch_insert(vectors=index_vectors, vector_ids=generated_tags)

                k = 5
                ids, dists = index.search(query_vectors[0], k_neighbors=k, complexity=5)
                self.assertEqual(ids.shape[0], k)
                self.assertEqual(dists.shape[0], k)

    def test_valid_metric(self):
        with self.assertRaises(ValueError):
            dap.DynamicMemoryIndex(
                distance_metric="sandwich",
                vector_dtype=np.single,
                dimensions=10,
                max_vectors=11_000,
                complexity=64,
                graph_degree=32,
                num_threads=16,
            )
        with self.assertRaises(ValueError):
            dap.DynamicMemoryIndex(
                distance_metric=None,
                vector_dtype=np.single,
                dimensions=10,
                max_vectors=11_000,
                complexity=64,
                graph_degree=32,
                num_threads=16,
            )
        dap.DynamicMemoryIndex(
            distance_metric="l2",
            vector_dtype=np.single,
            dimensions=10,
            max_vectors=11_000,
            complexity=64,
            graph_degree=32,
            num_threads=16,
        )
        dap.DynamicMemoryIndex(
            distance_metric="mips",
            vector_dtype=np.single,
            dimensions=10,
            max_vectors=11_000,
            complexity=64,
            graph_degree=32,
            num_threads=16,
        )
        dap.DynamicMemoryIndex(
            distance_metric="MiPs",
            vector_dtype=np.single,
            dimensions=10,
            max_vectors=11_000,
            complexity=64,
            graph_degree=32,
            num_threads=16,
        )

    def test_valid_vector_dtype(self):
        aliases = {np.single: np.float32, np.byte: np.int8, np.ubyte: np.uint8}
        for (
            metric,
            dtype,
            query_vectors,
            index_vectors,
            ann_dir,
            vector_bin_file,
            generated_tags,
        ) in self._test_matrix:
            with self.subTest():
                index = dap.DynamicMemoryIndex(
                    distance_metric="l2",
                    vector_dtype=aliases[dtype],
                    dimensions=10,
                    max_vectors=11_000,
                    complexity=64,
                    graph_degree=32,
                    num_threads=16,
                )

        invalid = [np.double, np.float64, np.ulonglong]
        for invalid_vector_dtype in invalid:
            with self.subTest():
                with self.assertRaises(ValueError, msg=invalid_vector_dtype):
                    dap.DynamicMemoryIndex(
                        distance_metric="l2",
                        vector_dtype=invalid_vector_dtype,
                        dimensions=10,
                        max_vectors=11_000,
                        complexity=64,
                        graph_degree=32,
                        num_threads=16,
                    )

    def test_value_ranges_ctor(self):
        (
            metric,
            dtype,
            query_vectors,
            index_vectors,
            ann_dir,
            vector_bin_file,
            generated_tags,
        ) = build_random_vectors_and_memory_index(
            np.single, "l2", with_tags=True, index_prefix="not_ann"
        )
        good_ranges = {
            "distance_metric": "l2",
            "vector_dtype": np.single,
            "dimensions": 10,
            "max_vectors": 11_000,
            "complexity": 64,
            "graph_degree": 32,
            "max_occlusion_size": 10,
            "alpha": 1.2,
            "num_threads": 16,
            "filter_complexity": 10,
            "num_frozen_points": 10,
            "initial_search_complexity": 32,
            "search_threads": 0,
        }

        bad_ranges = {
            "distance_metric": "l200000",
            "vector_dtype": np.double,
            "dimensions": -1,
            "max_vectors": -1,
            "complexity": 0,
            "graph_degree": 0,
            "max_occlusion_size": -1,
            "alpha": -1,
            "num_threads": -1,
            "filter_complexity": -1,
            "num_frozen_points": -1,
            "initial_search_complexity": -1,
            "search_threads": -1,
        }
        for bad_value_key in good_ranges.keys():
            kwargs = good_ranges.copy()
            kwargs[bad_value_key] = bad_ranges[bad_value_key]
            with self.subTest():
                with self.assertRaises(
                    ValueError,
                    msg=f"expected to fail with parameter {bad_value_key}={bad_ranges[bad_value_key]}",
                ):
                    index = dap.DynamicMemoryIndex(saturate_graph=False, **kwargs)

    def test_value_ranges_search(self):
        good_ranges = {"complexity": 5, "k_neighbors": 10}
        bad_ranges = {"complexity": -1, "k_neighbors": 0}
        for bad_value_key in good_ranges.keys():
            kwargs = good_ranges.copy()
            kwargs[bad_value_key] = bad_ranges[bad_value_key]
            with self.subTest(msg=f"Test value ranges search with {kwargs=}"):
                with self.assertRaises(ValueError):
                    index = dap.DynamicMemoryIndex.from_file(
                        index_directory=self._example_ann_dir,
                        num_threads=16,
                        initial_search_complexity=32,
                        max_vectors=10001,
                        complexity=64,
                        graph_degree=32,
                    )
                    index.search(query=np.array([], dtype=np.single), **kwargs)

    def test_value_ranges_batch_search(self):
        good_ranges = {
            "complexity": 5,
            "k_neighbors": 10,
            "num_threads": 5,
        }
        bad_ranges = {
            "complexity": 0,
            "k_neighbors": 0,
            "num_threads": -1,
        }
        for bad_value_key in good_ranges.keys():
            kwargs = good_ranges.copy()
            kwargs[bad_value_key] = bad_ranges[bad_value_key]
            with self.subTest(msg=f"Testing value ranges batch search with {kwargs=}"):
                with self.assertRaises(ValueError):
                    index = dap.DynamicMemoryIndex.from_file(
                        index_directory=self._example_ann_dir,
                        num_threads=16,
                        initial_search_complexity=32,
                        max_vectors=10001,
                        complexity=64,
                        graph_degree=32,
                    )
                    index.batch_search(
                        queries=np.array([[]], dtype=np.single), **kwargs
                    )

    # Issue #400
    def test_issue400(self):
        _, _, _, index_vectors, ann_dir, _, generated_tags = self._test_matrix[0]

        deletion_tag = generated_tags[10]  # arbitrary choice
        deletion_vector = index_vectors[10]

        index = dap.DynamicMemoryIndex.from_file(
            index_directory=ann_dir,
            num_threads=16,
            initial_search_complexity=32,
            max_vectors=10100,
            complexity=64,
            graph_degree=32,
        )
        index.insert(np.array([1.0] * 10, dtype=np.single), 10099)
        index.insert(np.array([2.0] * 10, dtype=np.single), 10050)
        index.insert(np.array([3.0] * 10, dtype=np.single), 10053)
        tags, distances = index.search(
            np.array([3.0] * 10, dtype=np.single), k_neighbors=5, complexity=64
        )
        self.assertIn(10053, tags)
        tags, distances = index.search(deletion_vector, k_neighbors=5, complexity=64)
        self.assertIn(
            deletion_tag, tags, "deletion_tag should exist, as we have not deleted yet"
        )
        index.mark_deleted(deletion_tag)
        tags, distances = index.search(deletion_vector, k_neighbors=5, complexity=64)
        self.assertNotIn(
            deletion_tag,
            tags,
            "deletion_tag should not exist, as we have marked it for deletion",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            index.save(tmpdir)

            index2 = dap.DynamicMemoryIndex.from_file(
                index_directory=tmpdir,
                num_threads=16,
                initial_search_complexity=32,
                max_vectors=10100,
                complexity=64,
                graph_degree=32,
            )
            tags, distances = index2.search(
                deletion_vector, k_neighbors=5, complexity=64
            )
            self.assertNotIn(
                deletion_tag,
                tags,
                "deletion_tag should not exist, as we saved and reloaded the index without it",
            )

    def test_inserts_past_max_vectors(self):
        def _tiny_index():
            return dap.DynamicMemoryIndex(
                distance_metric="l2",
                vector_dtype=np.float32,
                dimensions=10,
                max_vectors=2,
                complexity=64,
                graph_degree=32,
                num_threads=16,
            )


        rng = np.random.default_rng(12345)

        # insert 3 vectors and look for an exception
        index = _tiny_index()
        index.insert(rng.random(10, dtype=np.float32), 1)
        index.insert(rng.random(10, dtype=np.float32), 2)
        with self.assertRaises(RuntimeError):
            index.insert(rng.random(10, dtype=np.float32), 3)

        # insert 2 vectors, delete 1, and insert another and expect a warning
        index = _tiny_index()
        index.insert(rng.random(10, dtype=np.float32), 1)
        index.insert(rng.random(10, dtype=np.float32), 2)
        index.mark_deleted(2)
        with self.assertWarns(UserWarning):
            self.assertEqual(index._removed_num_vectors, 1)
            self.assertEqual(index._num_vectors, 2)
            index.insert(rng.random(10, dtype=np.float32), 3)
            self.assertEqual(index._removed_num_vectors, 0)
            self.assertEqual(index._num_vectors, 2)

        # insert 3 batch and look for an exception
        index = _tiny_index()
        with self.assertRaises(RuntimeError):
            index.batch_insert(
                rng.random((3, 10), dtype=np.float32),
                np.array([1,2,3], dtype=np.uint32)
            )


        # insert 2 batch, remove 1, add 1 and expect a warning, remove 1, insert 2 batch and look for an exception
        index = _tiny_index()
        index.batch_insert(
            rng.random((2, 10), dtype=np.float32),
            np.array([1,2], dtype=np.uint32)
        )
        index.mark_deleted(1)
        with self.assertWarns(UserWarning):
            index.insert(rng.random(10, dtype=np.float32), 3)
        index.mark_deleted(2)
        with self.assertRaises(RuntimeError):
            index.batch_insert(rng.random((2,10), dtype=np.float32), np.array([4, 5], dtype=np.uint32))

        # insert 1, remove it, add 2 batch, and expect a warning
        index = _tiny_index()
        index.insert(rng.random(10, dtype=np.float32), 1)
        index.mark_deleted(1)
        with self.assertWarns(UserWarning):
            index.batch_insert(rng.random((2, 10), dtype=np.float32), np.array([10, 20], dtype=np.uint32))

        # insert 2 batch, remove both, add 2 batch, and expect a warning
        index = _tiny_index()
        index.batch_insert(rng.random((2,10), dtype=np.float32), np.array([10, 20], dtype=np.uint32))
        index.mark_deleted(10)
        index.mark_deleted(20)
        with self.assertWarns(UserWarning):
            index.batch_insert(rng.random((2, 10), dtype=np.float32), np.array([15, 25], dtype=np.uint32))

        # insert 2 batch, remove both, consolidate_delete, add 2 batch and do not expect warning
        index = _tiny_index()
        index.batch_insert(rng.random((2,10), dtype=np.float32), np.array([10, 20], dtype=np.uint32))
        index.mark_deleted(10)
        index.mark_deleted(20)
        index.consolidate_delete()
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # turns warnings into raised exceptions
            index.batch_insert(rng.random((2, 10), dtype=np.float32), np.array([15, 25], dtype=np.uint32))


