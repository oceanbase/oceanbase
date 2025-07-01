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

#ifndef OB_VECTOR_UTIL_H
#define OB_VECTOR_UTIL_H
#include <stdint.h>
#include <vsag/allocator.h>
#include <vsag/logger.h>
#include <vsag/iterator_context.h>
#include <fstream>
#include "lib/allocator/page_arena.h"
#include "lib/vector/ob_vsag_adaptor.h"

namespace oceanbase {
namespace common {

namespace obvectorutil {

class ObVsagLogger : public vsag::Logger {
    public:
    void SetLevel(Level Log_level) override;
    void
    Trace(const std::string& msg) override;

    void
    Debug(const std::string& msg) override;

    void
    Info(const std::string& msg) override;

    void
    Warn(const std::string& msg) override;

    void
    Error(const std::string& msg) override;

    void
    Critical(const std::string& msg) override;
};

int init_vasg_logger(void* logger);

bool check_vsag_init();

int create_index(obvsag::VectorIndexPtr& index_handler, int index_type,
                 const char* dtype, const char* metric, int dim,
                 int max_degree, int ef_construction, int ef_search,
                 void* allocator = NULL, int extra_info_size = 0,
                 int16_t refine_type = 0, int16_t bq_bits_query = 32,
                 bool bq_use_fht = false);

int build_index(obvsag::VectorIndexPtr index_handler, float* vector_list, int64_t* ids, int dim, int size, char *extra_info = nullptr);

int add_index(obvsag::VectorIndexPtr index_handler,float* vector_list, int64_t* ids, int dim, char *extra_info, int size);

int get_index_number(obvsag::VectorIndexPtr index_handler, int64_t &size);

int get_index_type(obvsag::VectorIndexPtr index_handler);
int cal_distance_by_id(obvsag::VectorIndexPtr index_handler,
                       const float *vector,
                       const int64_t *ids,
                       int64_t count,
                       const float *&distances);
int get_extra_info_by_ids(obvsag::VectorIndexPtr& index_handler,
                          const int64_t* ids,
                          int64_t count,
                          char *extra_infos);
int get_vid_bound(obvsag::VectorIndexPtr index_handler, int64_t &min_vid, int64_t &max_vid);
uint64_t estimate_memory(obvsag::VectorIndexPtr& index_handler, const uint64_t row_count, const bool is_build);
int knn_search(obvsag::VectorIndexPtr index_handler,
               float *query_vector,
               int dim,
               int64_t topk,
               const float *&result_dist,
               const int64_t *&result_ids,
               const char *&extra_info,
               int64_t &result_size,
               int ef_search,
               void *invalid = NULL,
               bool reverse_filter = false,
               bool is_extra_info_filter = false,
               float valid_ratio = 1.0,
               void *allocator = nullptr,
               bool need_extra_info = false);

int knn_search(obvsag::VectorIndexPtr index_handler,
               float *query_vector,
               int dim,
               int64_t topk,
               const float *&result_dist,
               const int64_t *&result_ids,
               const char *&extra_info,
               int64_t &result_size,
               int ef_search,
               void *invalid,
               bool reverse_filter,
               bool is_extra_info_filter,
               float valid_ratio,
               void *allocator,
               bool need_extra_info,
               void *&iter_filter,
               bool is_last_search = false);

int fserialize(obvsag::VectorIndexPtr index_handler, std::ostream& out_stream);

int fdeserialize(obvsag::VectorIndexPtr& index_handler, std::istream& in_stream);
int delete_index(obvsag::VectorIndexPtr& index_handler);
void delete_iter_ctx(void *iter_ctx);
} // namesapce obvectorutil
} // namespace common
} // namespace oceanbase
#endif  /* OB_VECTOR_UTIL_H */
