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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_vector_util.h"
#include "lib/string/ob_string.h"


namespace oceanbase {
namespace common {
namespace obvectorutil {

void ObVsagLogger::SetLevel(Level Log_level)
{
    //obvectorlib::set_log_level(Log_level);
}
void ObVsagLogger::Trace(const std::string& msg)
{
    ObString Log = ObString(msg.size(), msg.c_str());
    LOG_TRACE("[Vsag]",K(Log));
}

void ObVsagLogger::Debug(const std::string& msg)
{
    ObString Log = ObString(msg.size(), msg.c_str());
    LOG_TRACE("[Vsag]",K(Log));
}

void ObVsagLogger::Info(const std::string& msg)
{
    ObString Log = ObString(msg.size(), msg.c_str());
    LOG_TRACE("[Vsag]",K(Log));
}

void ObVsagLogger::Warn(const std::string& msg)
{
    int ret=0;
    ObString Log = ObString(msg.size(), msg.c_str());
    LOG_WARN("[Vsag]",K(Log));
}

void ObVsagLogger::Error(const std::string& msg)
{
    int ret=0;
    ObString Log = ObString(msg.size(), msg.c_str());
    LOG_ERROR("[Vsag]",K(Log));
}

void ObVsagLogger::Critical(const std::string& msg)
{
    ObString Log = ObString(msg.size(), msg.c_str());
    LOG_TRACE("[Vsag]",K(Log));
}

int init_vasg_logger(void* logger)
{
    INIT_SUCC(ret);
    if (!check_vsag_init()) {
        return -4016;
    } else {
#ifdef OB_BUILD_CDC_DISABLE_VSAG
#else
        obvectorlib::set_logger(logger);
        obvectorlib::set_log_level(static_cast<vsag::Logger::Level>(OB_LOGGER.get_log_level()));
#endif
    }
    return 0;
}

bool check_vsag_init()
{
    INIT_SUCC(ret);
#ifdef OB_BUILD_CDC_DISABLE_VSAG
    return true;
#else
    return obvectorlib::is_init();
#endif
}

int create_index(obvectorlib::VectorIndexPtr& index_handler, int index_type,
                 const char* dtype, const char* metric, int dim,
                 int max_degree, int ef_construction, int ef_search,
                 void* allocator, int extra_info_size /*= 0*/)
{
  INIT_SUCC(ret);
#ifdef OB_BUILD_CDC_DISABLE_VSAG
  return ret;
#else
  obvectorlib::set_block_size_limit(2*1024*1024);
  return obvectorlib::create_index(index_handler,
                                   static_cast<obvectorlib::IndexType>(index_type),
                                   dtype, metric,
                                   dim,
                                   max_degree,
                                   ef_construction,
                                   ef_search,
                                   allocator,
                                   extra_info_size);
#endif
}

int build_index(obvectorlib::VectorIndexPtr index_handler, float* vector_list, int64_t* ids, int dim, int size, char* extra_info /*= nullptr*/)
{
  INIT_SUCC(ret);
#ifdef OB_BUILD_CDC_DISABLE_VSAG
    return ret;
#else
  return obvectorlib::build_index(index_handler, vector_list, ids, dim, size, extra_info);
#endif

}

int add_index(obvectorlib::VectorIndexPtr index_handler, float* vector_list, int64_t* ids, int dim, char *extra_info, int size)
{
  INIT_SUCC(ret);
#ifdef OB_BUILD_CDC_DISABLE_VSAG
  return ret;
#else
  return obvectorlib::add_index(index_handler, vector_list, ids, dim, size, extra_info);
#endif
}

int get_index_number(obvectorlib::VectorIndexPtr index_handler, int64_t &size)
{
    INIT_SUCC(ret);
#ifdef OB_BUILD_CDC_DISABLE_VSAG
    return ret;
#else
    return obvectorlib::get_index_number(index_handler, size);
#endif
}

int get_index_type(obvectorlib::VectorIndexPtr index_handler)
{
    INIT_SUCC(ret);
#ifdef OB_BUILD_CDC_DISABLE_VSAG
    return ret;
#else
    return obvectorlib::get_index_type(index_handler);
#endif
}

int cal_distance_by_id(obvectorlib::VectorIndexPtr index_handler,
                       const float *vector,
                       const int64_t *ids,
                       int64_t count,
                       const float *&distances)
{
  INIT_SUCC(ret);
#ifdef OB_BUILD_CDC_DISABLE_VSAG
    return ret;
#else
    return obvectorlib::cal_distance_by_id(index_handler, vector, ids, count, distances);
#endif
}

int get_vid_bound(obvectorlib::VectorIndexPtr index_handler, int64_t &min_vid, int64_t &max_vid)
{
    INIT_SUCC(ret);
#ifdef OB_BUILD_CDC_DISABLE_VSAG
    return ret;
#else
    return obvectorlib::get_vid_bound(index_handler, min_vid, max_vid);
#endif
}

int get_extra_info_by_ids(obvectorlib::VectorIndexPtr& index_handler,
                          const int64_t* ids,
                          int64_t count,
                          char *extra_infos) {
INIT_SUCC(ret);
#ifdef OB_BUILD_CDC_DISABLE_VSAG
    return ret;
#else
    return obvectorlib::get_extra_info_by_ids(index_handler, ids, count, extra_infos);
#endif
return ret;
}

int knn_search(obvectorlib::VectorIndexPtr index_handler, float* query_vector,int dim, int64_t topk,
               const float*& result_dist, const int64_t*& result_ids, const char *&extra_info, int64_t &result_size, int ef_search,
               void* invalid, bool reverse_filter, bool is_extra_info_filter, float valid_ratio, bool need_extra_info)
{
  INIT_SUCC(ret);
#ifdef OB_BUILD_CDC_DISABLE_VSAG
  return ret;
#else
  return obvectorlib::knn_search(index_handler, query_vector, dim, topk,
                                  result_dist, result_ids, result_size,
                                  ef_search, need_extra_info, extra_info,
                                  invalid, reverse_filter, is_extra_info_filter,
                                  valid_ratio);
#endif
}

int knn_search(obvectorlib::VectorIndexPtr index_handler, float* query_vector,int dim, int64_t topk,
               const float*& result_dist, const int64_t*& result_ids, const char *&extra_info, int64_t &result_size, int ef_search,
               void* invalid, bool reverse_filter, bool is_extra_info_filter, float valid_ratio, bool need_extra_info, void *&iter_ctx, bool is_last_search)
{
  INIT_SUCC(ret);
#ifdef OB_BUILD_CDC_DISABLE_VSAG
  return ret;
#else
  return obvectorlib::knn_search(index_handler, query_vector, dim, topk,
                                result_dist, result_ids, result_size,
                                ef_search, need_extra_info, extra_info,
                                invalid, reverse_filter, is_extra_info_filter,
                                valid_ratio, iter_ctx, is_last_search);
#endif
}

int fserialize(obvectorlib::VectorIndexPtr index_handler, std::ostream& out_stream)
{
    INIT_SUCC(ret);
#ifdef OB_BUILD_CDC_DISABLE_VSAG
    return ret;
#else
    return obvectorlib::fserialize(index_handler, out_stream);
#endif
}

int fdeserialize(obvectorlib::VectorIndexPtr& index_handler, std::istream& in_stream)
{
    INIT_SUCC(ret);
#ifdef OB_BUILD_CDC_DISABLE_VSAG
    return ret;
#else
    return obvectorlib::fdeserialize(index_handler,in_stream);
#endif
}

int delete_index(obvectorlib::VectorIndexPtr& index_handler)
{
    INIT_SUCC(ret);
#ifdef OB_BUILD_CDC_DISABLE_VSAG
    return ret;
#else
    return obvectorlib::delete_index(index_handler);
#endif
}

void delete_iter_ctx(void *iter_ctx)
{
#ifdef OB_BUILD_CDC_DISABLE_VSAG
#else
    obvectorlib::delete_iter_ctx(iter_ctx);
#endif
}

// return byte
uint64_t estimate_memory(obvectorlib::VectorIndexPtr& index_handler, const uint64_t row_count, const uint64_t dim, const bool is_hnsw_bq_build)
{
  INIT_SUCC(ret);
#ifdef OB_BUILD_CDC_DISABLE_VSAG
    return ret;
#else
  return obvectorlib::estimate_memory(index_handler, row_count) + (is_hnsw_bq_build ? (row_count * dim * sizeof(float)): 0);
#endif

}

} //namespace obvectorlib
} //namespace common
} //namespace oceanbase
