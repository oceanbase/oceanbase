/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/dag/ob_table_load_dag_parallel_merger.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/plan/ob_table_load_table_op.h"

namespace oceanbase
{
namespace observer
{
using namespace storage;

ObTableLoadDagParallelMerger::ObTableLoadDagParallelMerger()
  : store_ctx_(nullptr), op_ctx_(nullptr), clear_table_prepared_(false), is_inited_(false)
{
}

ObTableLoadDagParallelMerger::~ObTableLoadDagParallelMerger() {}

int ObTableLoadDagParallelMerger::init_merge_ctx()
{
  int ret = OB_SUCCESS;
  ObTableLoadStoreTableCtx *store_table_ctx = op_ctx_->store_table_ctx_;
  ObDirectLoadMergeParam param;
  param.table_id_ = store_table_ctx->table_id_;
  param.rowkey_column_num_ = store_table_ctx->schema_->rowkey_column_count_;
  param.column_count_ = store_table_ctx->schema_->store_column_count_;
  param.col_descs_ = &store_table_ctx->schema_->column_descs_;
  param.datum_utils_ = &store_table_ctx->schema_->datum_utils_;
  param.lob_column_idxs_ = &store_table_ctx->schema_->lob_column_idxs_;
  param.merge_mode_ = op_ctx_->merge_mode_;
  param.dml_row_handler_ = op_ctx_->dml_row_handler_;
  param.insert_table_ctx_ = op_ctx_->insert_table_ctx_;
  param.trans_param_ = op_ctx_->insert_table_ctx_->get_param().trans_param_;
  param.file_mgr_ = store_ctx_->tmp_file_mgr_;
  param.ctx_ = store_ctx_->ctx_;
  if (OB_FAIL(merge_ctx_.init(param, store_table_ctx->ls_partition_ids_))) {
    LOG_WARN("fail to init merge ctx", KR(ret), K(param));
  }
  return ret;
}

int ObTableLoadDagParallelMerger::init_merge_task(ObTableLoadStoreCtx *store_ctx, ObTableLoadTableOpCtx *op_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadDagParallelMerger init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == store_ctx || nullptr == op_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(store_ctx), KP(op_ctx));
  } else {
    store_ctx_ = store_ctx;
    op_ctx_ = op_ctx;
    if (OB_FAIL(init_merge_ctx())) {
      LOG_WARN("fail to init merge ctx", KR(ret));
    } else if (ObDirectLoadMergeMode::MERGE_WITH_ORIGIN_QUERY_FOR_LOB == op_ctx_->merge_mode_) {
      if (OB_FAIL(merge_ctx_.build_del_lob_task(op_ctx_->table_store_, store_ctx_->thread_cnt_, true/*for_dag*/))) {
        LOG_WARN("fail to build del lob task", KR(ret));
      }
    } else {
      if (OB_FAIL(merge_ctx_.build_merge_task(op_ctx_->table_store_, store_ctx_->thread_cnt_))) {
        LOG_WARN("fail to build merge task", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(task_iter_.init(&merge_ctx_))) {
      LOG_WARN("fail to init task iter", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadDagParallelMerger::get_next_merge_task(ObDirectLoadIMergeTask *&merge_task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(task_iter_.get_next_task(merge_task))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next merger", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadDagParallelMerger::prepare_clear_table()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (clear_table_prepared_) {
  } else {
    ObMutexGuard guard(mutex_);
    if (clear_table_prepared_) {
    } else {
      merge_ctx_.reset();
      FOREACH_X(it, op_ctx_->table_store_, OB_SUCC(ret))
      {
        ObDirectLoadTableHandleArray *table_handle_array = it->second;
        if (OB_FAIL(all_table_handles_.add(*table_handle_array))) {
          LOG_WARN("fail to add table handles", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        op_ctx_->table_store_.clear();
        clear_table_prepared_ = true;
      }
    }
  }
  return ret;
}

int ObTableLoadDagParallelMerger::clear_table(const int64_t thread_cnt, const int64_t thread_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!clear_table_prepared_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not prepared", KR(ret));
  } else if (all_table_handles_.empty()) {
    // do nothing
  } else {
    const int64_t table_cnt_pre_thread = all_table_handles_.count() / thread_cnt;
    const int64_t remain_table_cnt = all_table_handles_.count() % thread_cnt;
    const int64_t start_idx = table_cnt_pre_thread * thread_idx + MIN(thread_idx, remain_table_cnt);
    const int64_t table_cnt = table_cnt_pre_thread + (thread_idx < remain_table_cnt ? 1 : 0);
    for (int64_t i = 0; i < table_cnt; ++i) {
      all_table_handles_.at(start_idx + i).reset();
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
