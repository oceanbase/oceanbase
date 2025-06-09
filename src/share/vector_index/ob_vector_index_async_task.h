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

#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "share/rc/ob_tenant_base.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"

namespace oceanbase
{
namespace share
{

// schedule vector tasks for a ls
class ObPluginVectorIndexMgr;
class ObVecAsyncTaskExector final
{
public:
  ObVecAsyncTaskExector()
    : is_inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      vector_index_service_(nullptr),
      ls_(nullptr)
  {}
  virtual ~ObVecAsyncTaskExector() {}
  int init(const uint64_t tenant_id, ObLS *ls);
  int resume_task();
  int start_task();
  int load_task();
  int check_and_set_thread_pool();
  int clear_old_task_ctx_if_need();

private:
  int get_index_ls_mgr(ObPluginVectorIndexMgr *&index_ls_mgr);
  int check_task_result(ObVecIndexAsyncTaskCtx *task_ctx);
  int insert_new_task(ObVecIndexTaskCtxArray &task_status_array);
  int update_status_and_ret_code(ObVecIndexAsyncTaskCtx *task_ctx);
  int clear_task_ctx(ObVecIndexAsyncTaskOption &task_opt, ObVecIndexAsyncTaskCtx *task_ctx);
  int clear_task_ctxs(ObVecIndexAsyncTaskOption &task_opt, const ObVecIndexTaskCtxArray &task_ctx_array);

  bool check_operation_allow();

private:
  static const int64_t VEC_INDEX_TASK_MAX_RETRY_TIME = 3; // 200
  static const int64_t INVALID_TG_ID = -1;

  bool is_inited_;
  uint64_t tenant_id_;
  ObPluginVectorIndexService *vector_index_service_;
  ObLS *ls_;
  volatile int64_t async_task_ref_cnt_;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_VECTOR_INDEX_ASYNC_TASK_DEFINE_H_