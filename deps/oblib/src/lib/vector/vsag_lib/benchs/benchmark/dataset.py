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

import oss2
import os
import h5py
import numpy as np
import pandas as pd
from sklearn.neighbors import NearestNeighbors
import datetime
from oss2.credentials import EnvironmentVariableCredentialsProvider
import ast
from tqdm import tqdm

OSS_ACCESS_KEY_ID = os.environ.get('OSS_ACCESS_KEY_ID')
OSS_ACCESS_KEY_SECRET = os.environ.get('OSS_ACCESS_KEY_SECRET')
OSS_ENDPOINT = os.environ.get('OSS_ENDPOINT')
OSS_BUCKET = os.environ.get('OSS_BUCKET')
OSS_SOURCE_DIR = os.environ.get('OSS_SOURCE_DIR')

_auth = oss2.ProviderAuth(EnvironmentVariableCredentialsProvider())
_bucket = oss2.Bucket(_auth, OSS_ENDPOINT, OSS_BUCKET)
target_dir = '/tmp/dataset'
def download_and_open_dataset(dataset_name, logging=None):
    if None in [OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT, OSS_BUCKET, OSS_SOURCE_DIR]:
        if logging is not None:
            logging.error("missing oss env")
        exit(-1)
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    source_file = os.path.join(OSS_SOURCE_DIR, dataset_name)
    target_file = os.path.join(target_dir, dataset_name)

    if not os.path.exists(target_file):
        _bucket.get_object_to_file(source_file, target_file)
    return h5py.File(target_file, 'r')


def read_dataset(dataset_name, logging=None):
    with download_and_open_dataset(dataset_name, logging) as file:
        train = np.array(file["train"])
        test = np.array(file["test"])
        neighbors = np.array(file["neighbors"])
        distances = np.array(file["distances"])
    return train, test, neighbors, distances


def create_dataset(ids, base, query, topk, dataset_name, distance):
    if distance == "angular":
        metric = "cosine"
    elif distance == "euclidean":
        metric = "euclidean"
    print("data size:", len(ids), len(base))
    print("query size:", len(query))
    nbrs = NearestNeighbors(n_neighbors=topk, metric=metric, algorithm='brute').fit(base)
    batch_size = 50
    n_query = len(query)
    distances = []
    indices = []

    for i in tqdm(range(0, n_query, batch_size)):
        end = min(i + batch_size, n_query)
        batch_query = query[i:end]
        D_batch, I_batch = nbrs.kneighbors(batch_query)
        distances.append(D_batch)
        indices.append(I_batch)

    D = np.vstack(distances)
    I = np.vstack(indices)

    with h5py.File(os.path.join(target_dir, dataset_name), "w") as f:
        f.create_dataset("ids", data=ids)
        f.create_dataset("train", data=base)
        f.create_dataset("test", data=query)
        f.create_dataset("neighbors", data=I)
        f.create_dataset("distances", data=D)
        f.attrs["type"] = "dense"
        f.attrs["distance"] = distance
        f.attrs["dimension"] = len(base[0])
        f.attrs["point_type"] = "float"


def csv_to_data(filename, id_column, base_column, dim):
    parser = vector_parse_wrapper(dim)
    df = pd.read_csv(filename=10000)
    df[base_column] = df[base_column].apply(parser)
    df_cleaned = df.dropna(subset=[base_column])
    base = np.array(df_cleaned[base_column].tolist())
    ids = np.array(df_cleaned[id_column].tolist())
    ids_dtype = h5py.string_dtype(encoding='utf-8', length=max(len(s) for s in ids))
    ids_array = np.array(ids, dtype=ids_dtype)
    return ids_array, base



def csv_to_dataset(base_filename, base_size, query_size, id_column, base_column, dim, distance, dataset_name):
    data_ids, data = csv_to_data(base_filename, id_column, base_column, dim)
    unique_ids, index = np.unique(data_ids, return_index=True)
    unique_data = data[index]
    base_ids, base = unique_ids[:base_size], unique_data[:base_size]
    qeury_ids, query = unique_ids[-query_size:], unique_data[-query_size:]
    create_dataset(base_ids, base, query, 100, dataset_name, distance)


# You can customize the parsing function for the vector column and then run the main function.
fail_count = 0
def vector_parse_wrapper(dim):
    def split_vector(x):
        global fail_count
        try:
            data = np.array([float(i) for i in x.split(",")], dtype=np.float32)
            if data.shape[0] != dim:
                print(fail_count, x, dim)
                fail_count += 1
                return None
            return data
        except:
            print(fail_count, x)
            fail_count += 1
            return None
    return split_vector


"""
python script.py data/base.csv 1000000 --id_column=id --vector_column=base_vector --vector_size=2048 --output_file=test.hdf5
"""

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Convert CSV to dataset")
    parser.add_argument("csv_file", help="Path to the CSV file")
    parser.add_argument("size", type=int, help="Size of dataset")
    parser.add_argument("--query_size", type=int, default=10000, help="Index number")
    parser.add_argument("--id_column", help="Name of the ID column")
    parser.add_argument("--vector_column", help="Name of the vector column")
    parser.add_argument("--vector_dim", type=int, help="Dim of the vector")
    parser.add_argument("--index_type", choices=["angular", "euclidean"], default="angular", help="Type of index")
    parser.add_argument("--output_file", help="Path to the output file")

    args = parser.parse_args()
    csv_to_dataset(args.csv_file, args.size, args.column, args.query_size, args.id_column, args.vector_column, args.vector_dim, args.index_type, args.output_file)






