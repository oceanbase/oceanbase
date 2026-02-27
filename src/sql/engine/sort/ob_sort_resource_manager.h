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

#ifndef OCEANBASE_SQL_ENGINE_SORT_OB_SORT_RESOURCE_MANAGER_H_
#define OCEANBASE_SQL_ENGINE_SORT_OB_SORT_RESOURCE_MANAGER_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_array.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/rc/context.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"
#include "lib/oblog/ob_log.h"
#include "sql/engine/sort/ob_sort_row_store_mgr.h"
#include "sql/engine/sort/ob_sort_vec_strategy.h"
#include "sql/engine/sort/ob_external_merge_sorter.h"

namespace oceanbase
{
namespace sql
{

template<typename Compare, typename Store_Row, bool has_addon>
class ObSortResourceManager
{
public:
  static const int64_t EXTEND_MULTIPLE = 2;
  static const int64_t MAX_ROW_CNT = 268435456; // (2G / 8)
  static constexpr int64_t MIN_MERGE_WAYS = 8;

  explicit ObSortResourceManager(ObMonitorNode &op_monitor_info, lib::MemoryContext *mem_context, ObSqlWorkAreaType profile_type)
    : op_monitor_info_(op_monitor_info),
      mem_context_(mem_context),
      profile_(profile_type),
      sql_mem_processor_(profile_, op_monitor_info_)
  {}

  virtual ~ObSortResourceManager()
  {
    sql_mem_processor_.unregister_profile();
  }

  void unregister_profile()
  {
    sql_mem_processor_.unregister_profile();
  }

  int64_t get_memory_bound() const
  {
    return sql_mem_processor_.get_mem_bound();
  }

  int64_t get_memory_used() const
  {
    return this->mem_context_->ref_context()->used();
  }

  virtual int64_t get_data_size() const
  {
    return sql_mem_processor_.get_data_size();
  }

  void set_number_pass(int32_t num_pass)
  {
    sql_mem_processor_.set_number_pass(num_pass);
  }

  virtual int64_t get_total_used_size() const = 0;

  virtual bool need_dump() const = 0;

  int preprocess_dump(bool &dumped)
  {
    int ret = OB_SUCCESS;
    dumped = false;
    if (OB_FAIL(sql_mem_processor_.get_max_available_mem_size(&this->mem_context_->ref_context()->get_malloc_allocator()))) {
      SQL_ENG_LOG(WARN, "failed to get max available memory size", K(ret));
    } else if (OB_FALSE_IT(sql_mem_processor_.update_used_mem_size(get_total_used_size()))) {
      SQL_ENG_LOG(WARN, "failed to update used memory size", K(ret));
    } else {
      dumped = need_dump();
      if (dumped) {
        if (!sql_mem_processor_.is_auto_mgr()) {
          // 如果dump在非auto管理模式也需要注册到workarea
          if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(&mem_context_->ref_context()->get_malloc_allocator(),
                                                                [&](int64_t max_memory_size) {
                                                                  UNUSED(max_memory_size);
                                                                  return need_dump();
                                                                },
                                                                dumped, get_total_used_size()))) {
            SQL_ENG_LOG(WARN, "failed to extend memory size", K(ret));
          }
        } else if (profile_.get_cache_size() < profile_.get_global_bound_size()) {
          // in-memory：所有数据都可以缓存，即global bound
          // size比较大，则继续看是否有更多内存可用
          if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(&mem_context_->ref_context()->get_malloc_allocator(),
                                                                [&](int64_t max_memory_size) {
                                                                  UNUSED(max_memory_size);
                                                                  return need_dump();
                                                                },
                                                                dumped, get_total_used_size()))) {
            SQL_ENG_LOG(WARN, "failed to extend memory size", K(ret));
          }
          SQL_ENG_LOG(TRACE, "trace sort need dump", K(dumped), K(get_memory_used()),
                                            K(profile_.get_global_bound_size()), K(get_memory_limit()),
                                            K(profile_.get_cache_size()), K(profile_.get_expect_size()));
        } else {
          // one-pass
          if (profile_.get_cache_size() <= get_data_size()) {
            // 总体数据量超过cache
            // size，说明估算的cache不准确，需要重新估算one-pass
            // size，按照2*cache_size处理
            if (OB_FAIL(sql_mem_processor_.update_cache_size(&mem_context_->ref_context()->get_malloc_allocator(),
                                                             profile_.get_cache_size() * EXTEND_MULTIPLE))) {
              SQL_ENG_LOG(WARN, "failed to update cache size", K(ret), K(profile_.get_cache_size()));
            } else {
              dumped = need_dump();
            }
          }
        }
        SQL_ENG_LOG(INFO, "trace sort need dump", K(dumped), K(get_memory_used()),
                    K(get_memory_limit()), K(profile_.get_cache_size()), K(profile_.get_global_bound_size()),
                    K(profile_.get_expect_size()), K(sql_mem_processor_.get_data_size()),
                    K(sql_mem_processor_.is_auto_mgr()));
      }
    }
    return ret;
  }

  // 内存管理辅助接口
  int update_max_available_mem_size_periodically(const int64_t row_count, bool &updated)
  {
    int ret = OB_SUCCESS;
    updated = false;
    if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
          &this->mem_context_->ref_context()->get_malloc_allocator(),
          [&](int64_t cur_cnt) {
            // 让子类决定是否需要更新，基于当前的行数等
            return row_count > cur_cnt;
          }, updated))) {
      SQL_ENG_LOG(WARN, "failed to get max available memory size", K(ret));
    }

    return ret;
  }

  int update_used_memory_size()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(sql_mem_processor_.update_used_mem_size(get_total_used_size()))) {
      SQL_ENG_LOG(WARN, "failed to update used memory size", K(ret));
    }

    return ret;
  }

  int update_used_mem_size(int64_t used_size)
  {
    int ret = OB_SUCCESS;
    sql_mem_processor_.update_used_mem_size(used_size);
    return ret;
  }

  int64_t get_dir_id() const
  {
    return sql_mem_processor_.get_dir_id();
  }

  void alloc(int64_t size)
  {
    sql_mem_processor_.alloc(size);
  }

  void free(int64_t size)
  {
    sql_mem_processor_.free(size);
  }

  ObSqlMemMgrProcessor &get_sql_mem_processor()
  {
    return this->sql_mem_processor_;
  }

  ObSqlWorkAreaProfile &get_profile()
  {
    return this->sql_mem_processor_.get_profile();
  }

  const ObSqlWorkAreaProfile &get_profile() const
  {
    return this->sql_mem_processor_.get_profile();
  }

  int get_merge_ways_by_memory(ObIAllocator *allocator, const int64_t tempstore_read_alignment_size, const int64_t max_merge_ways, int64_t &merge_ways)
  {
    int ret = OB_SUCCESS;
    merge_ways = 0;
    if (OB_UNLIKELY(nullptr == allocator || tempstore_read_alignment_size < 0 || max_merge_ways < 2)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid argument", K(ret), KP(allocator), K(tempstore_read_alignment_size), K(max_merge_ways));
    } else {
      int64_t tempstore_block_size = max(tempstore_read_alignment_size, ObTempBlockStore::MIN_READ_BUFFER_SIZE);
      merge_ways = (get_memory_bound() - mem_context_->ref_context()->used()) / tempstore_block_size;
      merge_ways = std::max(MIN_MERGE_WAYS, merge_ways);
      if (merge_ways < max_merge_ways) {
        bool dumped = false;
        // extra extend one tempstore_block_size
        // to prevent situations where sufficient memory exists but merge_ways not equal to max_ways.
        int64_t need_size = (max_merge_ways + 1) * tempstore_block_size + mem_context_->ref_context()->used();
        if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(
              allocator,
              [&](int64_t max_memory_size) { return max_memory_size < need_size; }, dumped,
              get_total_used_size()))) {
          SQL_ENG_LOG(WARN, "failed to extend memory size", K(ret));
        }
        merge_ways = std::max(merge_ways, (get_memory_bound() - mem_context_->ref_context()->used()) / tempstore_block_size);
      }
      merge_ways = std::min(merge_ways, max_merge_ways);
    }
    return ret;
  }

  typedef ObSortVecOpChunk<Store_Row, has_addon> ChunkType;
  int calc_merge_ways(common::ObDList<ChunkType> &chunks, ObIAllocator *allocator, const int64_t tempstore_read_alignment_size, int64_t &merge_ways)
  {
    int ret = OB_SUCCESS;
    merge_ways = 0;
    const int64_t MAX_MERGE_WAYS = ObExternalMergeSorter<Compare, Store_Row, has_addon>::MAX_MERGE_WAYS;
    if (OB_UNLIKELY(nullptr == allocator || chunks.get_size() < 2 || tempstore_read_alignment_size < 0)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid argument", K(ret), KP(allocator), K(chunks.get_size()), K(tempstore_read_alignment_size));
    } else if (OB_FAIL(sql_mem_processor_.get_max_available_mem_size(allocator))) {
      SQL_ENG_LOG(WARN, "failed to get max available memory size", K(ret));
    } else {
      // 检查chunk level，如果只有一个chunk在当前level，提升到下一level
      ChunkType *first = chunks.get_first();
      if (first->level_ != first->get_next()->level_) {
        LOG_TRACE("only one chunk in current level, move to next level directly", K(first->level_));
        first->level_ = first->get_next()->level_;
      }

      // 1. 计算最大归并路数（基于chunk level）
      int64_t max_ways = 1;
      ChunkType *c = first->get_next();
      // get max merge ways in same level
      for (int64_t i = 0; first->level_ == c->level_
           && i < MIN(chunks.get_size(), MAX_MERGE_WAYS) - 1; i++) {
        max_ways += 1;
        c = c->get_next();
      }

      // 2. 计算实际归并路数（基于内存限制）
      if (OB_SUCC(ret)) {
        if (OB_FAIL(get_merge_ways_by_memory(allocator, tempstore_read_alignment_size, max_ways, merge_ways))) {
          SQL_ENG_LOG(WARN, "failed to get merge ways by memory", K(ret));
        }
      }
      LOG_TRACE("do merge sort ", K(ret), K(first->level_), K(merge_ways), K(chunks.get_size()),
                                  K(mem_context_->ref_context()->used()), K(get_memory_bound()), K(get_profile()));
    }
    return ret;
  }

protected:
  // 内存上下文和监控
  ObMonitorNode &op_monitor_info_;
  lib::MemoryContext *mem_context_;

  // 内存管理和性能分析
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
};


} // namespace sql
} // namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_OB_SORT_RESOURCE_MANAGER_H_ */
