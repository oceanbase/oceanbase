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
class ObVecAsyncTaskExector final : public ObVecITaskExecutor
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

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_VECTOR_INDEX_ASYNC_TASK_DEFINE_H_
