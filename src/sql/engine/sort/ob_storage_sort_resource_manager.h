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

#ifndef OCEANBASE_SQL_ENGINE_SORT_OB_STORAGE_SORT_RESOURCE_MANAGER_H_
#define OCEANBASE_SQL_ENGINE_SORT_OB_STORAGE_SORT_RESOURCE_MANAGER_H_

#include "sql/engine/sort/ob_sort_resource_manager.h"

namespace oceanbase
{
namespace sql
{

template<typename Compare, typename Store_Row, bool has_addon>
class ObStorageSortResourceManager : public ObSortResourceManager<Compare, Store_Row, has_addon>
{
public:
  ObStorageSortResourceManager(ObMonitorNode &op_monitor_info,
      lib::MemoryContext *mem_context,
      ObSortRowStoreMgr<Store_Row, has_addon> &store_mgr)
  : ObSortResourceManager<Compare, Store_Row, has_addon>(op_monitor_info, mem_context, ObSqlWorkAreaType::SORT_WORK_AREA),
    store_mgr_(store_mgr)
  {}
  virtual ~ObStorageSortResourceManager() {}


  int init(const uint64_t tenant_id, const int64_t cache_size, ObSqlProfileExecInfo exec_info)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(this->sql_mem_processor_.init(&this->mem_context_->ref_context()->get_malloc_allocator(),
        tenant_id, cache_size,
        this->op_monitor_info_.op_type_,
        this->op_monitor_info_.op_id_,
        exec_info))) {
      SQL_ENG_LOG(WARN, "init sql memory processor failed", K(ret));
    }
    return ret;
  }

  int64_t get_total_used_size() const override {
    return this->mem_context_->ref_context()->used();
  }

  bool need_dump() const override {
    return this->get_data_size() > this->get_memory_bound()
        || this->get_total_used_size() >= this->get_profile().get_global_bound_size();
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

private:
  ObSortRowStoreMgr<Store_Row, has_addon> &store_mgr_;
};

} // namespace sql
} // namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_OB_STORAGE_SORT_RESOURCE_MANAGER_H_ */
