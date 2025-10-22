/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_VSAG_ADAPTOR_H
#define OB_VSAG_ADAPTOR_H

#include <stdint.h>
#include <float.h>
#include <string>

namespace oceanbase {
namespace common {
namespace obvsag {

typedef void* VectorIndexPtr;
extern bool is_init_;
enum IndexType {
  INVALID_INDEX_TYPE = -1,
  HNSW_TYPE = 0,
  HNSW_SQ_TYPE = 1,
  // Keep it the same as ObVectorIndexAlgorithmType
  // IVF_FLAT_TYPE,
  // IVF_SQ8_TYPE,
  // IVF_PQ_TYPE,
  HNSW_BQ_TYPE = 5,
  HGRAPH_TYPE = 6,
  MAX_INDEX_TYPE
};

enum QuantizationType {
  FP32 = 0,
  SQ8 = 1,
  MAX_TYPE
};

class FilterInterface {
public:
  virtual bool test(int64_t id) = 0;
  virtual bool test(const char* data) = 0;
};
/**
 *   * Get the version based on git revision
 *   * 
 *   * @return the version text
 *   */
extern std::string version();

/**
 *   * Init the vsag library
 *   * 
 *   * @return true always
 *   */
extern bool is_init();

/*
 * *trace = 0
 * *debug = 1
 * *info = 2
 * *warn = 3
 * *err = 4
 * *critical = 5
 * *off = 6
 * */
void set_log_level(int32_t ob_level_num);
void set_logger(void *logger_ptr);
void set_block_size_limit(uint64_t size);
bool is_hgraph_type(uint8_t create_type);
const char* get_index_type_str(uint8_t create_type);
int construct_vsag_create_param(
    uint8_t create_type, const char *dtype, const char *metric, int dim,
    int max_degree, int ef_construction, int ef_search, void *allocator,
    int extra_info_size, int16_t refine_type, int16_t bq_bits_query,
    bool bq_use_fht, char *result_param_str);
int construct_vsag_search_param(uint8_t create_type, 
                                             int64_t ef_search, 
                                             bool use_extra_info_filter, 
                                             char *result_param_str);
int create_index(VectorIndexPtr& index_handler, IndexType index_type,
                 const char* dtype,
                 const char* metric,int dim,
                 int max_degree, int ef_construction, int ef_search, void* allocator = nullptr,
                 int extra_info_size = 0, int16_t refine_type = 0,
                 int16_t bq_bits_query = 32, bool bq_use_fht = false);
int build_index(VectorIndexPtr& index_handler, float* vector_list, int64_t* ids, int dim, int size, char *extra_infos = nullptr);
int add_index(VectorIndexPtr& index_handler, float* vector, int64_t* ids, int dim, int size, char *extra_info = nullptr);
int get_index_number(VectorIndexPtr& index_handler, int64_t &size);
int get_index_type(VectorIndexPtr& index_handler);
int cal_distance_by_id(VectorIndexPtr& index_handler, const float* vector, const int64_t* ids, int64_t count, const float *&distances);
int get_vid_bound(VectorIndexPtr& index_handler, int64_t &min_vid, int64_t &max_vid);
int knn_search(VectorIndexPtr& index_handler,float* query_vector, int dim, int64_t topk,
               const float*& dist, const int64_t*& ids, int64_t &result_size, int ef_search,
               bool need_extra_info, const char*& extra_infos,
               void* invalid, bool reverse_filter, bool use_extra_info_filter,
               float valid_ratio, void *&iter_ctx, bool is_last_search = false, void *allocator = nullptr);
int knn_search(VectorIndexPtr& index_handler,float* query_vector, int dim, int64_t topk,
               const float*& dist, const int64_t*& ids, int64_t &result_size, int ef_search,
               bool need_extra_info, const char*& extra_infos,
               void* invalid = nullptr, bool reverse_filter = false,
               bool use_extra_info_filter = false, void *allocator = nullptr, float valid_ratio = 1, float distance_threshold = FLT_MAX);
int serialize(VectorIndexPtr& index_handler, const std::string dir);
int deserialize_bin(VectorIndexPtr& index_handler, const std::string dir);
int fserialize(VectorIndexPtr& index_handler, std::ostream& out_stream);
int fdeserialize(VectorIndexPtr& index_handler, std::istream& in_stream);
int delete_index(VectorIndexPtr& index_handler);
void delete_iter_ctx(void *iter_ctx);
uint64_t estimate_memory(VectorIndexPtr& index_handler, const uint64_t row_count, const bool is_build);
int get_extra_info_by_ids(VectorIndexPtr& index_handler, 
                          const int64_t* ids, 
                          int64_t count, 
                          char *extra_infos);
int immutable_optimize(VectorIndexPtr& index_handler);

} // namesapce obvsag
} // namespace common
} // namespace oceanbase

#endif  /* OB_VECTOR_UTIL_H */