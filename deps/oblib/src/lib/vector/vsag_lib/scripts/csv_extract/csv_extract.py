#!/usr/bin/env python3

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
import csv
import h5py
import numpy as np
import subprocess
import argparse
import pickle
from pprint import pprint

def extract_from_csv(csvfile,
                     id_column_name,
                     vector_column_name,
                     output_hdf5,
                     dataset_name,
                     overwrite,
                     id_callback=lambda *args: None,
                     vector_callback=lambda *args: None):
    with h5py.File(output_hdf5, 'a') as hdf5file:
        data = list()
        csvfiles = [f for f in csvfile.split(":") if f != '']
        for i in range(len(csvfiles)):
            csvfile = csvfiles[i]
            line_count = int(subprocess.check_output(f'wc -l {csvfile}', shell=True).split()[0]) - 1
            with open(csvfile, 'r') as basefile:
                reader = csv.reader(basefile)
                header = next(reader)
                id_column_idx = header.index(id_column_name)
                vector_column_idx = header.index(vector_column_name)

                lineno = 0
                for row in reader:
                    id = row[id_column_idx]
                    id_callback(lineno, id)
                    # vector = [int(val) for val in row[vector_column_idx].split(',')]
                    vector = np.fromstring(row[vector_column_idx], dtype=np.int8, sep=',')
                    vector_callback(lineno, vector)
                    data.append(vector)

                    lineno += 1
                    if lineno % (line_count // 100) == 0:
                        print(f"\r{dataset_name} parsing ... {lineno / (line_count // 100)}% [{i+1}/{len(csvfiles)}]", end="")
                print()

        print("converting ...")
        data = np.array(data, dtype=np.int8)

        if dataset_name in hdf5file.keys():
            if overwrite:
                del hdf5file[dataset_name]
            else:
                print(f"error: {dataset_name} exists in {output_hdf5}, overwrite is NOT allow")
                exit(-1)
        hdf5file.create_dataset(dataset_name, data.shape, data=data)

def extract(base_csv, base_id_column_name, base_vector_column_name,
            query_csv, query_id_column_name, query_vector_column_name,
            output_hdf5, overwrite):
    print("Step #1: extract base data")
    idmap = dict()
    def proc1(lineno, id):
        idmap[id] = lineno
    extract_from_csv(base_csv,
                     base_id_column_name,
                     base_vector_column_name,
                     output_hdf5,
                     "base",
                     overwrite,
                     id_callback=proc1)
    # with h5py.File(output_hdf5, 'r') as hdf5file:
    #     base = np.array(hdf5file["base"])
    #     bf_index = hnswlib.BFIndex(space='l2', dim=base.shape[1])
    #     bf_index.init_index(max_elements=base.shape[0])
    #     bf_index.add_items(base)

    print("Step #2: extract query data")
    groundtruth = list()
    failed_list = list()
    def proc2(lineno, id):
        if id not in idmap:
            failed_list.append(id)
            groundtruth.append([-1])
        else:
            groundtruth.append([idmap[id]])
    extract_from_csv(query_csv,
                     query_id_column_name,
                     query_vector_column_name,
                     output_hdf5,
                     "query",
                     overwrite,
                     id_callback=proc2)
    if len(failed_list):
        pprint({"failed list": failed_list})
    groundtruth = np.array(groundtruth)
    print(f"groundtruth.shape: {groundtruth.shape}")

    # print("Step #3: calculate groundtruth")
    # with h5py.File(output_hdf5, 'r') as hdf5file:
    #     query = np.array(hdf5file["query"])
    #     gt_ids, _ = bf_index.knn_query(query, 20)
    # groundtruth = np.array(gt_ids)
    # print(groundtruth.shape)

    # Save groundtruth and idmap
    with h5py.File(output_hdf5, 'a') as hdf5file:
        if 'groundtruth' in hdf5file.keys():
            if overwrite:
                del hdf5file['groundtruth']
            else:
                print(f"error: groundtruth exists in {output_hdf5}, overwrite is NOT allow")
                exit(-1)
        hdf5file.create_dataset('groundtruth', groundtruth.shape, data=groundtruth)
    idmap_filename = os.path.splitext(output_hdf5)[0] + ".idmap"
    with open(idmap_filename, 'wb') as f:
        pickle.dump(idmap, f)

def interactive():
    # input of base dataset
    base_csv = ""
    while not os.path.isfile(base_csv):
        base_csv = input("1. base csv filename: ")
    base_line_count = int(subprocess.check_output(f'wc -l {base_csv}', shell=True).split()[0]) - 1
    with open(base_csv, 'r') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)
        print(header, "\n")

        base_id_column_name = None
        while base_id_column_name not in header:
            base_id_column_name = input("1.1 select id column: ")
        base_id_column_idx = header.index(base_id_column_name)

        base_vector_column_name = None
        while base_vector_column_name not in header:
            base_vector_column_name = input("1.2 select vector column: ")
        base_vector_column_idx = header.index(base_vector_column_name)

    # input of query dataset
    query_csv = ""
    while not os.path.isfile(query_csv):
        query_csv = input("2. query csv filename: ")
    query_line_count = int(subprocess.check_output(f'wc -l {query_csv}', shell=True).split()[0]) - 1
    with open(query_csv, 'r') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)
        print(header, "\n")

        query_id_column_name = None
        while query_id_column_name not in header:
            query_id_column_name = input("2.1 select id column: ")
        query_id_column_idx = header.index(query_id_column_name)

        query_vector_column_name = None
        while query_vector_column_name not in header:
            query_vector_column_name = input("2.2 select vector column: ")
        query_vector_column_idx = header.index(query_vector_column_name)

    # input of output
    output_hdf5 = ""
    overwrite = True
    while output_hdf5 == "" or overwrite is False:
        output_hdf5 = input("3. output filename: ")
        output_exist = os.path.isfile(output_hdf5)
        if output_exist:
            overwrite = input(f"{output_hdf5} file existed, overwrite? [y/N]")
            overwrite = overwrite.upper() == "Y"
            if overwrite == "Y":
                print(f"output data would overwrite the base and query dataset in the output hdf5 file")

    plan_message = f"""
Extract Plan:
    base:
        row count: {base_line_count}
        id field name, colidx: {base_id_column_name}, {base_id_column_idx}
        vector field name, colidx: {base_vector_column_name}, {base_vector_column_idx}
    query:
        row count: {query_line_count}
        id field name, colidx: {query_id_column_name}, {query_id_column_idx}
        vector field name, colidx: {query_vector_column_name}, {query_vector_column_idx}
    output:
        filename: {output_hdf5}{", OVERWRITE" if output_exist else ""}
"""
    print(plan_message)

    extract(base_csv,
            base_id_column_name,
            base_vector_column_name,
            query_csv,
            query_id_column_name,
            query_vector_column_name,
            output_hdf5,
            overwrite)


call_from_cmd_example = '''
python3 csv_extract.py --base-csv /path/to/base.csv \\
	--base-id-name id --base-vector-name feature \\
	--query-csv /path/to/query.csv \\
	--query-id-name id --query-vector-name feature \\
	--output /path/to/output.h5 --overwrite
'''
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract vector dataset from csv')
    parser.add_argument('--example', action='store_true', help='print example commands')
    parser.add_argument('--base-csv', type=str, help='base csv filename')
    parser.add_argument('--base-id-name', type=str, help='id column name in base csv')
    parser.add_argument('--base-vector-name', type=str, help='vector column name in base csv')
    parser.add_argument('--query-csv', type=str, help='query csv filename')
    parser.add_argument('--query-id-name', type=str, help='id column name in base csv')
    parser.add_argument('--query-vector-name', type=str, help='vector column name in base csv')
    parser.add_argument('--output', type=str, help='output hdf5 filename')
    parser.add_argument('--overwrite', action='store_true', help='overwrite if output file exist')
    args = parser.parse_args()

    if args.example:
        print(call_from_cmd_example)
        exit(0)

    if None in [args.base_csv, args.base_id_name, args.base_vector_name,
                args.query_csv, args.query_id_name, args.query_vector_name,
                args.output]:
        interactive()
    else:
        extract(args.base_csv, args.base_id_name, args.base_vector_name,
                args.query_csv, args.query_id_name, args.query_vector_name,
                args.output, args.overwrite)
    
