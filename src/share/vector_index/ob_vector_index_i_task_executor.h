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
#include "lib/hash/ob_hashset.h"

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
  int start_task();
  int clear_old_task_ctx_if_need();
  virtual int load_task(uint64_t &task_trace_base_num) = 0;
  virtual int check_and_set_thread_pool() = 0;

protected:
  static const int64_t VEC_INDEX_TASK_MAX_RETRY_TIME = 3; // 200
  static const int64_t INVALID_TG_ID = -1;
  static const int64_t MAX_ASYNC_TASK_PROCESSING_COUNT = 128; // the thread pool max paralell processing cnt is 8

  int get_index_ls_mgr(ObPluginVectorIndexMgr *&index_ls_mgr);
  virtual bool check_operation_allow() = 0;
  int update_status_and_ret_code(ObVecIndexAsyncTaskCtx *task_ctx);
  int clear_task_ctx(ObVecIndexAsyncTaskOption &task_opt, ObVecIndexAsyncTaskCtx *task_ctx);
  int clear_task_ctxs(ObVecIndexAsyncTaskOption &task_opt, const ObVecIndexTaskCtxArray &task_ctx_array);
  int check_task_result(ObVecIndexAsyncTaskCtx *task_ctx);
  int insert_new_task(ObVecIndexTaskCtxArray &task_ctx_array);

  // Query __all_ddl_task_status for HNSW-related DDL tasks (type 14/15/17),
  // populate two conflict sets for coarse-grained (table-level) and fine-grained (index-level) matching.
  int check_has_hnsw_ddl(common::hash::ObHashSet<uint64_t> &conflict_table_id_set,
                          common::hash::ObHashSet<uint64_t> &conflict_index_task_set);
  // Check whether a given task ctx conflicts with active DDL, considering trigger type.
  // Sets is_conflict=true when a conflict is detected; on error, is_conflict is left unchanged.
  int check_task_ddl_conflict(ObVecIndexAsyncTaskCtx *task_ctx,
                              ObPluginVectorIndexMgr *index_ls_mgr,
                              const common::hash::ObHashSet<uint64_t> &conflict_table_id_set,
                              const common::hash::ObHashSet<uint64_t> &conflict_index_task_set,
                              bool &is_conflict);

  bool is_inited_;
  uint64_t tenant_id_;
  ObPluginVectorIndexService *vector_index_service_;
  storage::ObLS *ls_;
  volatile int64_t async_task_ref_cnt_;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_VECTOR_INDEX_I_TASK_EXECUTOR_H_
