/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_FREEZE_TASK_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_FREEZE_TASK_H_

#include "share/vector_index/ob_vector_index_async_task.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "share/vector_index/ob_vector_index_i_task_executor.h"

namespace oceanbase
{
namespace share
{

class ObVecIdxFreezeTaskExecutor : public ObVecAsyncTaskExector
{
public:
  ObVecIdxFreezeTaskExecutor()
    : ObVecAsyncTaskExector(), max_active_segment_size_(0)
  {}
  virtual ~ObVecIdxFreezeTaskExecutor() {}
  int load_task(uint64_t &task_trace_base_num) override;
private:
  bool check_operation_allow() override;
  int check_need_freeze(ObPluginVectorIndexAdaptor *adapter, bool &need_freeze);
  int calc_freeze_threshold(ObTenantVectorAllocator& vector_allocator,
                      const int64_t inc_mem_size,
                      int64_t &total_pre_alloc_size,
                      int64_t &freeze_threshold);

  int64_t max_active_segment_size_;
};


class ObVecIdxFreezeTask : public ObVecIndexAsyncTask
{
public:
  ObVecIdxFreezeTask() : ObVecIndexAsyncTask(ObMemAttr(MTL_ID(), "VecFrezTask")) {}
  virtual ~ObVecIdxFreezeTask() {}
  int do_work() override;

private:
  int process_freeze();
  int check_and_freeze(ObPluginVectorIndexAdapterGuard &adpt_guard, const ObLSID &ls_id);
  int check_and_wait_write(const ObVecIdxFrozenDataHandle &frozen_data);
  int refresh_adaptor(ObPluginVectorIndexAdapterGuard &adpt_guard, ObIAllocator &allocator, const ObLSID &ls_id);

  DISALLOW_COPY_AND_ASSIGN(ObVecIdxFreezeTask);
};


} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_FREEZE_TASK_H_
