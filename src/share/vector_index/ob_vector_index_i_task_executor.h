/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_VECTOR_INDEX_I_TASK_EXECUTOR_H_
#define OCEANBASE_SHARE_OB_VECTOR_INDEX_I_TASK_EXECUTOR_H_

#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase
{
namespace share
{

class ObVecIdxAsyncTaskScheduler;

class ObVecITaskExecutor
{
public:
  ObVecITaskExecutor()
    : is_inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      vector_index_service_(nullptr),
      scheduler_(nullptr),
      ls_handle_(),
      async_task_ref_cnt_(0)
  {}
  virtual ~ObVecITaskExecutor() {}
  virtual int init(const uint64_t tenant_id, storage::ObLSHandle &ls_handle);
  storage::ObLS *get_ls() const { return ls_handle_.get_ls(); }
  // R1 sweep: push all non-FINISH rows belonging to this LS that were last executed by
  // self_addr to FINISH+CANCELED. Used on both leader and follower load paths after a
  // role switch so the new owner of the LS clears the previous incarnation's residue.
  int resume_task();
  // Load one manually-triggered inner-table row. Each executor subclass implements its own load logic.
  virtual int load_triggered_task(const ObVecIndexTaskStatus &task_row) = 0;
  int clear_old_task_ctx_if_need();
  virtual int load_task(uint64_t &task_trace_base_num) = 0;
  virtual int check_and_set_thread_pool() = 0;

  static int check_has_hnsw_ddl(
      const uint64_t tenant_id,
      common::hash::ObHashSet<uint64_t> &conflict_table_id_set,
      common::hash::ObHashSet<uint64_t> &conflict_index_task_set);
  static int check_task_ddl_conflict(
      ObVecIndexAsyncTaskCtx *task_ctx,
      ObPluginVectorIndexMgr *index_ls_mgr,
      const common::hash::ObHashSet<uint64_t> &conflict_table_id_set,
      const common::hash::ObHashSet<uint64_t> &conflict_index_task_set,
      bool &is_conflict);

protected:
  static const int64_t INVALID_TG_ID = -1;
  static const int64_t MAX_ASYNC_TASK_PROCESSING_COUNT = 128; // the thread pool max paralell processing cnt is 8

  int get_index_ls_mgr(ObPluginVectorIndexMgr *&index_ls_mgr);
  virtual bool check_operation_allow() = 0;
  int insert_new_task(ObVecIndexTaskCtxArray &task_ctx_array);
  int start_task();
  int clear_task_ctx(ObVecIndexAsyncTaskOption &task_opt, ObVecIndexAsyncTaskCtx *task_ctx);
  int clear_task_ctxs(ObVecIndexAsyncTaskOption &task_opt, const ObVecIndexTaskCtxArray &task_ctx_array);
  int check_task_result(ObVecIndexAsyncTaskCtx *task_ctx);

public:
  TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(ls_handle), K_(async_task_ref_cnt));

protected:
  bool is_inited_;
  uint64_t tenant_id_;
  ObPluginVectorIndexService *vector_index_service_;
  // Owned by ObPluginVectorIndexService; guaranteed alive while this executor is alive.
  ObVecIdxAsyncTaskScheduler *scheduler_;
  storage::ObLSHandle ls_handle_;
  volatile int64_t async_task_ref_cnt_;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_VECTOR_INDEX_I_TASK_EXECUTOR_H_
