#  Copyright 2024-present the vsag project
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import h5py
import logging
import numpy as np
import json
import time
from datetime import datetime
import pyvsag
from .dataset import download_and_open_dataset


from itertools import product


def build_index(index_name, datas, ids, index_parameters):
    index = pyvsag.Index(index_name, json.dumps(index_parameters))
    index.build(datas, ids, datas.shape[0], datas.shape[1])
    return index

def run(config):

    logging.basicConfig(encoding='utf-8',
                        level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(message)s',
                        handlers=[logging.FileHandler('/tmp/bench-index.log'),
                                  logging.StreamHandler()])
    logging.info(f'{__file__} running at {datetime.now()}')
    logging.info(f'config: {config}')

    for dataset in config["index_test"]:
        dataset_name = dataset["dataset_name"]
        logging.info(f"dataset: {dataset_name}")
        with download_and_open_dataset(dataset_name, logging) as file:
            base = np.array(file["train"])
            data_len = base.shape[0]
            ids = np.arange(data_len)
            dim = base.shape[1]

            for index in dataset["index"]:
                build_time = time.time()
                index_name = index["index_name"]
                instance = build_index(index_name, base, ids, {
                    "dtype": index["params"]["dtype"],
                    "metric_type": index["params"]["metric_type"],
                    "dim": dim,
                    index_name: index["params"]["build"]
                })
                build_time = time.time() - build_time

                query_config = dataset["query"]
                query_size = query_config["query_size"]
                query = np.array(file["test"][:query_size])
                neighbors = np.array(file["neighbors"][:query_size])

                if "knn" in query_config:
                    k = query_config["knn"]["k"]
                    correct = 0
                    time_list = []
                    for gt, item in zip(neighbors, query):
                        search_time = time.time()
                        labels, distances = instance.knn_search(item, k, json.dumps({
                            index_name: index["params"]["search"]
                        }))
                        time_list.append(time.time() - search_time)
                        correct += len(set(labels) & set(gt[:k]))

                    logging.info(f"datasize:{data_len}")
                    logging.info(f"building time: {build_time * 1000 / data_len}, searching time: {(np.sum(time_list)) * 1000 / query_size}")
                    logging.info(f"recall: {correct/(query_size * k)}")
