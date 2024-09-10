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
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include <random>


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
        obvectorlib::set_logger(logger);
        obvectorlib::set_log_level(static_cast<vsag::Logger::Level>(1));
    }
    return 0;
}

bool check_vsag_init()
{
    INIT_SUCC(ret);
    return obvectorlib::is_init();
}

int create_index(obvectorlib::VectorIndexPtr& index_handler, int index_type,
                 const char* dtype, const char* metric, int dim,
                 int max_degree, int ef_construction, int ef_search,
                 void* allocator)
{
  INIT_SUCC(ret);
  obvectorlib::set_block_size_limit(2*1024*1024);
  return obvectorlib::create_index(index_handler,
                                   static_cast<obvectorlib::IndexType>(index_type),
                                   dtype, metric,
                                   dim,
                                   max_degree,
                                   ef_construction,
                                   ef_search,
                                   allocator);
}

int build_index(obvectorlib::VectorIndexPtr index_handler, float* vector_list, int64_t* ids, int dim, int size)
{
  INIT_SUCC(ret);
  return obvectorlib::build_index(index_handler, vector_list, ids, dim, size);
}

int add_index(obvectorlib::VectorIndexPtr index_handler, float* vector_list, int64_t* ids, int dim, int size)
{
  INIT_SUCC(ret);
  return obvectorlib::add_index(index_handler, vector_list, ids, dim, size);
}

int get_index_number(obvectorlib::VectorIndexPtr index_handler, int64_t &size)
{
    INIT_SUCC(ret);
    return obvectorlib::get_index_number(index_handler, size);
}

int knn_search(obvectorlib::VectorIndexPtr index_handler, float* query_vector,int dim, int64_t topk,
               const float*& result_dist, const int64_t*& result_ids, int64_t &result_size, int ef_search,
               void* invalid)
{
  INIT_SUCC(ret);
  return obvectorlib::knn_search(index_handler, query_vector, dim, topk,
                                 result_dist, result_ids, result_size,
                                 ef_search, invalid);
}

int fserialize(obvectorlib::VectorIndexPtr index_handler, std::ostream& out_stream)
{
    INIT_SUCC(ret);
    return obvectorlib::fserialize(index_handler, out_stream);
}

int fdeserialize(obvectorlib::VectorIndexPtr& index_handler, std::istream& in_stream)
{
    INIT_SUCC(ret);
    return obvectorlib::fdeserialize(index_handler,in_stream);
}

int delete_index(obvectorlib::VectorIndexPtr& index_handler)
{
    INIT_SUCC(ret);
    return obvectorlib::delete_index(index_handler);
}

} //namespace obvectorlib
} //namespace common
} //namespace oceanbase