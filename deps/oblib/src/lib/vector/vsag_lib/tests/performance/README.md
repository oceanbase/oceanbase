# test performance tool

usage:
```
Usage: ./build/tests/test_performance <dataset_file_path> <process> <index_name> <build_param> <search_param>
```

example running commands for building index:
```
./build/tests/test_performance \
    '/data/random-100k-128-euclidean.hdf5' \
    'build' \
	'hnsw' \
	'{"dim": 128, "dtype": "float32", "metric_type": "l2", "hnsw": {"max_degree": 12, "ef_construction": 100}, "diskann": {"max_degree": 12, "ef_construction": 100, "pq_dims": 64, "pq_sample_rate": 0.1}}' \
	'{"hnsw":{"ef_search":100},"diskann":{"ef_search":100,"beam_search":4,"io_limit":200,"use_reorder":true}}'
```

example output:
```
{
    "build_parameters": "{\"dim\": 128, \"dtype\": \"float32\", \"metric_type\": \"l2\", \"hnsw\": {\"max_degree\": 12, \"ef_construction\": 100}, \"diskann\": {\"max_degree\": 12, \"ef_construction\": 100, \"pq_dims\": 64, \"pq_sample_rate\": 0.1}}",
    "build_time_in_second": 51.173068142,
    "dataset": "/data/random-100k-128-euclidean.hdf5",
    "num_base": 100000,
    "tps": 1954.1529095443386
}
```


example running commands for searching:
```
./build/tests/test_performance \
    '/data/random-100k-128-euclidean.hdf5' \
    'search' \
	'hnsw' \
	'{"dim": 128, "dtype": "float32", "metric_type": "l2", "hnsw": {"max_degree": 12, "ef_construction": 100}, "diskann": {"max_degree": 12, "ef_construction": 100, "pq_dims": 64, "pq_sample_rate": 0.1}}' \
	'{"hnsw":{"ef_search":100},"diskann":{"ef_search":100,"beam_search":4,"io_limit":200,"use_reorder":true}}'
```

example output:
```
{
    "correct": 3984,
    "dataset": "/data/random-100k-128-euclidean.hdf5",
    "index_name": "hnsw",
    "memory": 62863936,
    "num_query": 10000,
    "qps": 1987.0230208986188,
    "recall": 0.3984000086784363,
    "search_parameters": "{\"hnsw\":{\"ef_search\":100},\"diskann\":{\"ef_search\":100,\"beam_search\":4,\"io_limit\":200,\"use_reorder\":true}}",
    "search_time_in_second": 5.032654325
}
```
