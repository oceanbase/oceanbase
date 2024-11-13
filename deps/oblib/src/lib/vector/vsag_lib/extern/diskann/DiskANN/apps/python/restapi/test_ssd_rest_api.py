# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT license.

import atexit
import os
import subprocess
import sys
import time
import unittest

import numpy as np
import requests

from tempfile import TemporaryDirectory

from .disk_ann_util import build_ssd_index

_VECTOR_DIMS = 100
_RNG_SEED = 12345

def is_ascending(lst):
    prev = -float("inf")
    for x in lst:
        if x > prev:
            return False;
        prev = x
    return True

class TestSSDRestApi(unittest.TestCase):
    VECTOR_KEY = "query"
    K_KEY = "k"
    INDICES_KEY = "indices"
    DISTANCES_KEY = "distances"
    TAGS_KEY = "tags"
    QUERY_ID_KEY = "query_id"
    ERROR_MESSAGE_KEY = "error"
    L_KEY = "Ls"
    TIME_TAKEN_KEY = "time_taken_in_us"
    PARTITION_KEY = "partition"
    UNKNOWN_ERROR = "unknown_error"

    @classmethod
    def setUpClass(cls):
        if "DISKANN_REST_SERVER" in os.environ:
            cls._rest_address = os.environ["DISKANN_REST_SERVER"]
            cls._cleanup_lambda = lambda : None
        else:
            if "DISKANN_BUILD_DIR" not in os.environ:
                raise Exception("We require the environment variable DISKANN_BUILD_DIR be set to the diskann build directory on disk")
            diskann_build_dir = os.environ["DISKANN_BUILD_DIR"]

            if "DISKANN_REST_TEST_WORKING_DIR" not in os.environ:
                cls._temp_dir = TemporaryDirectory()
                cls._build_dir = cls._temp_dir.name
            else:
                cls._temp_dir = None
                cls._build_dir = os.environ["DISKANN_REST_TEST_WORKING_DIR"]

            rng = np.random.default_rng(_RNG_SEED)  # adjust seed for new random numbers
            cls._working_vectors = rng.random((1000, _VECTOR_DIMS), dtype=float)
            build_ssd_index(
                diskann_build_dir,
                cls._build_dir,
                cls._working_vectors
            )
            # now we have a built index, we should run the rest server
            rest_port = rng.integers(10000, 10100)
            cls._rest_address = f"http://127.0.0.1:{rest_port}/"

            ssd_server_path = os.path.join(diskann_build_dir, "apps", "restapi", "ssd_server")

            args = [
                ssd_server_path,
                "--address",
                cls._rest_address,
                "--data_type",
                "float",
                "--index_path_prefix",
                os.path.join(cls._build_dir, "smoke_test"),
                "--num_nodes_to_cache",
                str(_VECTOR_DIMS),
                "--num_threads",
                "1"
            ]

            command_run = " ".join(args)
            print(f"Executing REST server startup command: {command_run}", file=sys.stderr)

            cls._rest_process = subprocess.Popen(args)
            time.sleep(10)

            cls._cleanup_lambda = lambda: cls._rest_process.kill()

            # logically this shouldn't be necessary, but an open port is worse than some random gibberish in the
            # system tmp dir
            atexit.register(cls._cleanup_lambda)

    @classmethod
    def tearDownClass(cls):
        cls._cleanup_lambda()

    def _is_ready(self):
        return self._rest_process.poll() is None  # None means the process has no return status code yet

    def test_server_responds(self):
        rng = np.random.default_rng(_RNG_SEED)
        query = rng.random((_VECTOR_DIMS), dtype=float).tolist()
        json_payload = {
            "Ls": 32,
            "query_id": 1234,
            "query": query,
            "k": 10
        }
        try:
            response = requests.post(self._rest_address, json=json_payload)
            self.assertEqual(200, response.status_code, "Expected a successful request")
            jsonobj = response.json()
            self.assertAlmostEqual(10, len(jsonobj[self.DISTANCES_KEY]), "Expected 10 distances")
            self.assertAlmostEqual(10, len(jsonobj[self.INDICES_KEY]), "Expected 10 indexes")
        except Exception:
            if hasattr(self, "_rest_process"):
                raise Exception(f"Rest process status code is: {self._rest_process.poll()}")
            else:
                raise Exception(f"Client only mode, k: {k}")

    def test_server_responds_valid_k(self):
        rng = np.random.default_rng(_RNG_SEED)
        query = rng.random((_VECTOR_DIMS), dtype=float).tolist()
        k_list = [1, 5, 10, 20]
        for k in k_list:
            json_payload = {
                "Ls": 32,
                "query_id": 1234,
                "query": query,
                "k": k
            }
            try:
                response = requests.post(self._rest_address, json=json_payload)
                self.assertEqual(200, response.status_code, "Expected a successful request")
                jsonobj = response.json()
                self.assertAlmostEqual(k, len(jsonobj[self.DISTANCES_KEY]), "Expected 10 distances")
                self.assertAlmostEqual(k, len(jsonobj[self.INDICES_KEY]), "Expected 10 indexes")
                #self.assertTrue(is_ascending(jsonobj[self.DISTANCES_KEY]))
            except Exception:
                if hasattr(self, "_rest_process"):
                    raise Exception(f"Rest process status code is: {self._rest_process.poll()}")
                else:
                    raise Exception(f"Client only mode, k: {k}")

    def test_server_responds_invalid_k(self):
        rng = np.random.default_rng(_RNG_SEED)
        query = rng.random((_VECTOR_DIMS), dtype=float).tolist()
        k_list = [-1, 0]
        for k in k_list:
            json_payload = {
                "Ls": 32,
                "query_id": 1234,
                "query": query,
                "k": k
            }
            try:
                response = requests.post(self._rest_address, json=json_payload)
                self.assertEqual(500, response.status_code, "Expected a successful request")
            except Exception:
                if hasattr(self, "_rest_process"):
                    raise Exception(f"Rest process status code is: {self._rest_process.poll()}")
                else:
                    raise Exception(f"Client only mode, k: {k}")
