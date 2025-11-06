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

#ifndef OCEANBASE_SHARE_OB_VECTOR_INDEX_I_TASK_EXECUTOR_H_
#define OCEANBASE_SHARE_OB_VECTOR_INDEX_I_TASK_EXECUTOR_H_

#include "share/vector_index/ob_vector_index_async_task_util.h"

namespace oceanbase
{
namespace share
{

class ObVecITaskExecutor
{
public:
  ObVecITaskExecutor()
    : is_inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      vector_index_service_(nullptr),
      ls_(nullptr),
      async_task_ref_cnt_(0)
  {}
  virtual ~ObVecITaskExecutor() {}
  virtual int init(const uint64_t tenant_id, storage::ObLS *ls);
  int resume_task();
  int load_task_from_inner_table();
  int start_task();
  int clear_task_ctxs(ObVecIndexAsyncTaskOption &task_opt, const ObVecIndexTaskCtxArray &task_ctx_array);
  int clear_old_task_ctx_if_need();
  virtual int load_task(uint64_t &task_trace_base_num) = 0;
  virtual int check_and_set_thread_pool() = 0;

protected:
  static const int64_t VEC_INDEX_TASK_MAX_RETRY_TIME = 3; // 200
  static const int64_t INVALID_TG_ID = -1;
  static const int64_t MAX_ASYNC_TASK_PROCESSING_COUNT = 128; // the thread pool max paralell processing cnt is 8

  int get_index_ls_mgr(ObPluginVectorIndexMgr *&index_ls_mgr);
  virtual bool check_operation_allow() = 0;
  int clear_task_ctx(ObVecIndexAsyncTaskOption &task_opt, ObVecIndexAsyncTaskCtx *task_ctx);
  int check_task_result(ObVecIndexAsyncTaskCtx *task_ctx);
  int insert_new_task(ObVecIndexTaskCtxArray &task_ctx_array);

  bool is_inited_;
  uint64_t tenant_id_;
  ObPluginVectorIndexService *vector_index_service_;
  storage::ObLS *ls_;
  volatile int64_t async_task_ref_cnt_;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_VECTOR_INDEX_I_TASK_EXECUTOR_H_
