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

#ifndef OCEANBASE_OBSERVER_OB_VECTOR_INDEX_ASYNC_TASK_DEFINE_H_
#define OCEANBASE_OBSERVER_OB_VECTOR_INDEX_ASYNC_TASK_DEFINE_H_

#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "share/vector_index/ob_vector_index_i_task_executor.h"

namespace oceanbase
{
namespace share
{
// schedule vector tasks for a ls
class ObPluginVectorIndexMgr;
class ObVecAsyncTaskExector : public ObVecITaskExecutor
{
public:
  ObVecAsyncTaskExector()
    : ObVecITaskExecutor()
  {}
  virtual ~ObVecAsyncTaskExector() {}
  int load_task(uint64_t &task_trace_base_num) override;
  int check_and_set_thread_pool() override;
private:
  bool check_operation_allow() override;
};

class ObVecTaskManager
{
public:
  ObVecTaskManager(uint64_t tenant_id, int64_t index_table_id, ObVecIndexAsyncTaskType task_type)
      : tenant_id_(tenant_id),
        index_table_id_(index_table_id),
        task_type_(task_type),
        task_ids_()
  {}
  ~ObVecTaskManager() {}
  int process_task();
  int create_task();
  int check_task_status();
  TO_STRING_KV(K_(tenant_id), K_(index_table_id), K_(task_type), K_(task_ids));
private:
  uint64_t tenant_id_;
  int64_t index_table_id_;
  ObVecIndexAsyncTaskType task_type_;
  ObSEArray<int64_t, 4> task_ids_;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_VECTOR_INDEX_ASYNC_TASK_DEFINE_H_
