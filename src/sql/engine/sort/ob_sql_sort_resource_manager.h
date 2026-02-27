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

#ifndef OCEANBASE_SQL_ENGINE_SORT_OB_SQL_SORT_RESOURCE_MANAGER_H_
#define OCEANBASE_SQL_ENGINE_SORT_OB_SQL_SORT_RESOURCE_MANAGER_H_

#include "sql/engine/sort/ob_sort_resource_manager.h"

namespace oceanbase
{
namespace sql
{

template<typename Compare, typename Store_Row, bool has_addon>
class ObSQLSortResourceManager : public ObSortResourceManager<Compare, Store_Row, has_addon>
{
public:
  typedef std::function<bool(int64_t)> PredFunc;
  ObSQLSortResourceManager(ObMonitorNode &op_monitor_info,
      lib::MemoryContext *mem_context,
      ObSqlWorkAreaType profile_type,
      ObSortRowStoreMgr<Store_Row, has_addon> &store_mgr)
  :  ObSortResourceManager<Compare, Store_Row, has_addon>(op_monitor_info, mem_context, profile_type),
     store_mgr_(store_mgr)
  {}
  virtual ~ObSQLSortResourceManager() {}
  int init(int64_t part_cnt, bool use_partition_topn_sort, bool is_topn_filter_enabled, bool is_topn_sort,
      ObPartitionTopNSortStrategy<Compare, Store_Row, has_addon> *part_topn_sort_strategy,
      const uint64_t tenant_id, const int64_t cache_size, ObExecContext *exec_ctx)
  {
    int ret = OB_SUCCESS;
    part_cnt_ = part_cnt;
    use_partition_topn_sort_ = use_partition_topn_sort;
    is_topn_filter_enabled_ = is_topn_filter_enabled;
    is_topn_sort_ = is_topn_sort;
    part_topn_sort_strategy_ = part_topn_sort_strategy;
    if (OB_FAIL(this->sql_mem_processor_.init(&this->mem_context_->ref_context()->get_malloc_allocator(),
        tenant_id, cache_size,
        this->op_monitor_info_.op_type_,
        this->op_monitor_info_.op_id_,
        exec_ctx))) {
      SQL_ENG_LOG(WARN, "init sql memory processor failed", K(ret));
    }
    return ret;
  }

  int64_t get_cache_size() const
  {
    return this->get_profile().get_cache_size();
  }

  int64_t get_expect_size() const
  {
    return this->get_profile().get_expect_size();
  }

  void reset_sql_mem_processor()
  {
    this->sql_mem_processor_.reset();
  }

  int get_max_available_mem_size(ObIAllocator *allocator)
  {
    return this->sql_mem_processor_.get_max_available_mem_size(allocator);
  }

  int extend_max_memory_size(ObIAllocator *allocator, PredFunc dump_fun, bool &need_dump,
                            int64_t mem_used, int64_t max_times = 1024)
  {
    return this->sql_mem_processor_.extend_max_memory_size(allocator, dump_fun, need_dump, mem_used, max_times);
  }

  void unregister_profile_if_necessary()
  {
    this->sql_mem_processor_.unregister_profile_if_necessary();
  }

  virtual int64_t get_total_used_size() const override {
    return this->mem_context_->ref_context()->used() + get_need_extra_mem_size();
  }

  virtual bool need_dump() const override {
    return (this->mem_context_->ref_context()->used() + get_need_extra_mem_size() > get_tmp_buffer_mem_bound())
            || (get_total_used_size() >= this->profile_.get_global_bound_size());
  }

  virtual int64_t get_data_size() const override {
    return store_mgr_.get_mem_hold() + store_mgr_.get_file_size();
  }

private:
  int64_t get_partition_topn_ht_bucket_size() const {
    OB_ASSERT(nullptr != part_topn_sort_strategy_);
    return part_topn_sort_strategy_->get_ht_bucket_size();
  }

  int64_t get_partition_sort_ht_bucket_size() const {
    int64_t row_cnt = store_mgr_.get_row_cnt();
    return ((part_cnt_ == 0) ? 0 :
          (row_cnt * ObPartitionSortStrategy<Compare, Store_Row, has_addon>::FIXED_PART_NODE_SIZE * 2) +                          // size of(part_hash_nodes_)
          (next_pow2(std::max(16L, row_cnt)) * ObPartitionSortStrategy<Compare, Store_Row, has_addon>::FIXED_PART_BKT_SIZE * 2)); // size of(buckets_)
  }

  int64_t get_need_extra_mem_size() const {
    int64_t ret = 0;
    if (use_partition_topn_sort_) {
      ret = part_topn_sort_strategy_->get_need_extra_mem_size();
    } else {
      ret = get_partition_sort_ht_bucket_size();
    }
    return ret;
  }

  int64_t get_ht_bucket_size() const {
    int64_t ret = 0;
    if (part_cnt_ != 0) {
      if (use_partition_topn_sort_) {
        ret = get_partition_topn_ht_bucket_size();
      } else {
        ret = get_partition_sort_ht_bucket_size();
      }
    }
    return ret;
  }

  inline int64_t get_tmp_buffer_mem_bound() const {
    // The memory reserved for ObSortVecOpEagerFilter should be deducted when topn filter is enabled.
    return (is_topn_filter_enabled_ && is_topn_sort_)
               ? this->get_memory_bound() *
                     (1.0 - ObSortVecOpEagerFilter<Compare, Store_Row,
                                            has_addon>::FILTER_RATIO)
               : this->get_memory_bound();
  }
private:
  int64_t part_cnt_;
  bool use_partition_topn_sort_;
  bool is_topn_filter_enabled_;
  bool is_topn_sort_;
  ObPartitionTopNSortStrategy<Compare, Store_Row, has_addon> *part_topn_sort_strategy_;
  ObSortRowStoreMgr<Store_Row, has_addon> &store_mgr_;
};

} // namespace sql
} // namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_OB_SQL_SORT_RESOURCE_MANAGER_H_ */
