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

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_MEMSYNC_TRIGGER_TASK_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_MEMSYNC_TRIGGER_TASK_H_

#include "share/vector_index/ob_vector_index_i_task_executor.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"

namespace oceanbase
{
namespace share
{

class ObPluginVectorIndexMgr;
class ObPluginVectorIndexLoadScheduler;

class ObVecIdxMemSyncTriggerExecutor : public ObVecITaskExecutor
{
public:
  ObVecIdxMemSyncTriggerExecutor()
    : ObVecITaskExecutor(),
      scheduler_(nullptr)
  {}
  virtual ~ObVecIdxMemSyncTriggerExecutor() {}
  int load_task(uint64_t &task_trace_base_num) override;
  int check_and_set_thread_pool() override;
  void set_scheduler(ObPluginVectorIndexLoadScheduler *scheduler) { scheduler_ = scheduler; }
private:
  bool check_operation_allow() override;
  ObPluginVectorIndexLoadScheduler *scheduler_;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_MEMSYNC_TRIGGER_TASK_H_