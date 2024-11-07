// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "utils.h"

#define NUM_PQ_BITS 8
#define NUM_PQ_CENTROIDS (1 << NUM_PQ_BITS)
#define MAX_OPQ_ITERS 20
#define NUM_KMEANS_REPS_PQ 12
#define MAX_PQ_TRAINING_SET_SIZE 256000
#define MAX_PQ_CHUNKS 512
#define NUM_CENTROID 256
#define ALIGNED_SIZE 256

namespace vsag {

typedef void (*PQDistanceFunc)(const void* single_dim_centers, float single_dim_val, void* result);

}

namespace diskann
{
class FixedChunkPQTable
{
    float *tables = nullptr; // pq_tables = float array of size [NUM_CENTROID * ndims]
    uint64_t ndims = 0;      // ndims = true dimension of vectors
    uint64_t n_chunks = 0;
    bool use_rotation = false;
    uint32_t *chunk_offsets = nullptr;
    float *centroid = nullptr;
    float *tables_tr = nullptr; // same as pq_tables, but col-major
    float *rotmat_tr = nullptr;
    vsag::PQDistanceFunc func;

  public:
    FixedChunkPQTable();

    virtual ~FixedChunkPQTable();

#ifdef EXEC_ENV_OLS
    void load_pq_centroid_bin(MemoryMappedFiles &files, const char *pq_table_file, size_t num_chunks);
#else
    void load_pq_centroid_bin(const char *pq_table_file, size_t num_chunks);
#endif

    uint32_t get_num_chunks();

    void preprocess_query(float *query_vec);

    // assumes pre-processed query
    void populate_chunk_distances(const float *query_vec, float *dist_vec);

    float l2_distance(const float *query_vec, uint8_t *base_vec);

    float inner_product(const float *query_vec, uint8_t *base_vec);

    // assumes no rotation is involved
    void inflate_vector(uint8_t *base_vec, float *out_vec);

    void populate_chunk_inner_products(const float *query_vec, float *dist_vec);

    void load_pq_centroid_bin(std::stringstream &pq_table, size_t num_chunks);

    int64_t get_memory_usage();

};

template <typename T> struct PQScratch
{
    float *aligned_pqtable_dist_scratch = nullptr; // MUST BE AT LEAST [NUM_CENTROID * NCHUNKS]
    float *aligned_dist_scratch = nullptr;         // MUST BE AT LEAST diskann MAX_DEGREE
    uint8_t *aligned_pq_coord_scratch = nullptr;   // MUST BE AT LEAST  [N_CHUNKS * MAX_DEGREE]
    float *rotated_query = nullptr;
    float *aligned_query_float = nullptr;

    PQScratch(size_t graph_degree, size_t aligned_dim, size_t pq_chunks)
    {

        diskann::alloc_aligned((void **)&aligned_pq_coord_scratch,
                               ROUND_UP(((size_t)graph_degree * (size_t)pq_chunks * sizeof(uint8_t)), ALIGNED_SIZE), ALIGNED_SIZE);
        diskann::alloc_aligned((void **)&aligned_pqtable_dist_scratch, NUM_CENTROID * (size_t)pq_chunks * sizeof(float),
                               ALIGNED_SIZE);
        diskann::alloc_aligned((void **)&aligned_dist_scratch, ROUND_UP((size_t)graph_degree * sizeof(float), ALIGNED_SIZE), ALIGNED_SIZE);
        diskann::alloc_aligned((void **)&aligned_query_float, aligned_dim * sizeof(float), 8 * sizeof(float));
        diskann::alloc_aligned((void **)&rotated_query, aligned_dim * sizeof(float), 8 * sizeof(float));

        memset(aligned_query_float, 0, aligned_dim * sizeof(float));
        memset(rotated_query, 0, aligned_dim * sizeof(float));
    }

    ~PQScratch() {
        diskann::aligned_free(aligned_pq_coord_scratch);
        diskann::aligned_free(aligned_pqtable_dist_scratch);
        diskann::aligned_free(aligned_dist_scratch);
        diskann::aligned_free(aligned_query_float);
        diskann::aligned_free(rotated_query);
    }

    void set(size_t dim, T *query, const float norm = 1.0f)
    {
        for (size_t d = 0; d < dim; ++d)
        {
            if (norm != 1.0f)
                rotated_query[d] = aligned_query_float[d] = static_cast<float>(query[d]) / norm;
            else
                rotated_query[d] = aligned_query_float[d] = static_cast<float>(query[d]);
        }
    }
};

void aggregate_coords(const std::vector<unsigned> &ids, const uint8_t *all_coords, const uint64_t ndims, uint8_t *out);

void pq_dist_lookup(const uint8_t *pq_ids, const size_t n_pts, const size_t pq_nchunks, const float *pq_dists,
                    std::vector<float> &dists_out);

// Need to replace calls to these with calls to vector& based functions above
void aggregate_coords(const unsigned *ids, const uint64_t n_ids, const uint8_t *all_coords, const uint64_t ndims,
                      uint8_t *out);

void pq_dist_lookup(const uint8_t *pq_ids, const size_t n_pts, const size_t pq_nchunks, const float *pq_dists,
                    float *dists_out);

DISKANN_DLLEXPORT int generate_pq_pivots(const float *const train_data, size_t num_train, unsigned dim,
                                         unsigned num_centers, unsigned num_pq_chunks, unsigned max_k_means_reps,
                                         std::string pq_pivots_path, bool make_zero_mean = false);

DISKANN_DLLEXPORT int generate_pq_pivots(const float *const train_data, size_t num_train, unsigned dim,
                                             unsigned num_centers, unsigned num_pq_chunks, unsigned max_k_means_reps,
                                             std::stringstream &pq_pivots_stream, bool make_zero_mean = false);

DISKANN_DLLEXPORT int generate_pq_pivots(const float *const train_data, size_t num_train, unsigned dim,
                                         unsigned num_centers, unsigned num_pq_chunks, unsigned max_k_means_reps,
                                         std::vector<std::vector<std::vector<float>>> &codebook, bool make_zero_mean = false);


DISKANN_DLLEXPORT int generate_opq_pivots(const float *train_data, size_t num_train, unsigned dim, unsigned num_centers,
                                          unsigned num_pq_chunks, std::string opq_pivots_path, std::shared_ptr<float[]> &rotmat_tr,
                                          bool make_zero_mean = false);

DISKANN_DLLEXPORT std::unique_ptr<float[]> generate_opq_pivots(const float *train_data, size_t num_train, unsigned dim, unsigned num_centers,
                                          unsigned num_pq_chunks, std::stringstream &pq_pivots_stream,
                                          bool make_zero_mean = false);
template <typename T>
int generate_pq_data_from_pivots(const std::string &data_file, uint32_t num_centers, uint32_t num_pq_chunks,
                                 const std::string &pq_pivots_path, const std::string &pq_compressed_vectors_path,
                                 bool use_opq = false);

template <typename T>
int generate_pq_data_from_pivots(std::stringstream &base_reader, uint32_t num_centers, uint32_t num_pq_chunks,
                                 std::stringstream &pq_pivots_stream, std::stringstream &compressed_file_writer,
                                 bool use_opq = false);

template <typename T>
int generate_pq_data_from_pivots(const T* data, size_t num_points, size_t dim, const std::vector<size_t>& skip_locs, uint32_t num_centers, uint32_t num_pq_chunks,
                                 std::stringstream &pq_pivots_stream, std::stringstream &compressed_file_writer,
                                 bool use_opq = false, std::shared_ptr<float[]> rotmat_tr = nullptr, bool use_bsa = false);

template <typename T>
void generate_disk_quantized_data(const std::string &data_file_to_use, const std::string &disk_pq_pivots_path,
                                  const std::string &disk_pq_compressed_vectors_path,
                                  const diskann::Metric compare_metric, const double p_val, size_t &disk_pq_dims);
template <typename T>
void generate_disk_quantized_data(std::stringstream &data_stream, std::stringstream &disk_pq_pivots,
                                  std::stringstream &disk_pq_compressed_vectors, diskann::Metric compare_metric,
                                  const double p_val, size_t &disk_pq_dims);

template <typename T>
void generate_disk_quantized_data(const T* train_data, size_t train_size, size_t train_dim, const std::vector<size_t>& skip_locs,
                                  std::stringstream &disk_pq_pivots, std::stringstream &disk_pq_compressed_vectors,
                                  diskann::Metric compare_metric, const double p_val, size_t &disk_pq_dims, bool use_opq = false,
                                  bool use_bsa = false);

template <typename T>
void generate_quantized_data(const std::string &data_file_to_use, const std::string &pq_pivots_path,
                             const std::string &pq_compressed_vectors_path, const diskann::Metric compare_metric,
                             const double p_val, const uint64_t num_pq_chunks, const bool use_opq,
                             const std::string &codebook_prefix = "");
} // namespace diskann
