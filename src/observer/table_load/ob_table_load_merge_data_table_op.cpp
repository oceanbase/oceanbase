/**
 * Copyright (c) 2024 OceanBase
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

#include "observer/table_load/ob_table_load_merge_data_table_op.h"
#include "observer/table_load/ob_table_load_data_row_ack_handler.h"
#include "observer/table_load/ob_table_load_data_row_insert_handler.h"
#include "observer/table_load/ob_table_load_data_row_delete_handler.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
/**
 * ObTableLoadMergeDataTableOp
 */

ObTableLoadMergeDataTableOp::ObTableLoadMergeDataTableOp(ObTableLoadMergePhaseBaseOp *parent)
  : ObTableLoadMergeTableOp(parent)
{
}

int ObTableLoadMergeDataTableOp::inner_init()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("ObTableLoadMergeDataTableOp START");
  ObTableLoadStoreDataTableCtx *store_table_ctx = store_ctx_->data_store_table_ctx_;
  need_del_lob_ = ObDirectLoadMethod::is_incremental(ctx_->param_.method_) &&
                  nullptr != store_ctx_->data_store_table_ctx_->lob_table_ctx_ &&
                  !store_table_ctx->schema_->is_table_without_pk_;
  need_rescan_ =
    ObDirectLoadMethod::is_full(ctx_->param_.method_) && store_table_ctx->schema_->is_column_store_;
  if (!store_ctx_->write_ctx_.is_fast_heap_table_) {
    if (OB_FAIL(store_table_ctx->init_insert_table_ctx(merge_phase_ctx_->trans_param_,
                                                       true /*online_opt_stat_gather*/,
                                                       true /*is_insert_lob*/))) {
      LOG_WARN("fail to init insert table ctx", KR(ret));
    } else if (ObDirectLoadMethod::is_incremental(ctx_->param_.method_)) {
      // init table_build
      if (need_del_lob_ &&
          OB_FAIL(store_ctx_->data_store_table_ctx_->lob_table_ctx_->init_build_delete_table())) {
        LOG_WARN("fail to init build lob table", KR(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < store_ctx_->index_store_table_ctxs_.count(); ++i) {
        ObTableLoadStoreIndexTableCtx *index_table_ctx = store_ctx_->index_store_table_ctxs_.at(i);
        if (OB_FAIL(index_table_ctx->init_build_insert_table())) {
          LOG_WARN("fail to init build index table", KR(ret));
        }
      }
    }
  }

  // store_table_ctx_
  inner_ctx_.store_table_ctx_ = store_table_ctx;
  inner_ctx_.insert_table_ctx_ = store_table_ctx->insert_table_ctx_;
  // table_store_
  inner_ctx_.table_store_ = &(store_table_ctx->insert_table_store_);
  // dml_row_handler_
  if (OB_SUCC(ret)) {
    ObTableLoadDataRowInsertHandler *data_row_handler = nullptr;
    if (OB_ISNULL(inner_ctx_.dml_row_handler_ = data_row_handler =
                    OB_NEWx(ObTableLoadDataRowInsertHandler, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadDataRowInsertHandler", KR(ret));
    } else if (OB_FAIL(data_row_handler->init(store_ctx_))) {
      LOG_WARN("fail to init data row handler", KR(ret));
    }
  }
  // merge_mode_
  if (OB_SUCC(ret)) {
    switch (ctx_->param_.method_) {
      // 全量
      case ObDirectLoadMethod::FULL:
        switch (ctx_->param_.insert_mode_) {
          case ObDirectLoadInsertMode::NORMAL:
            inner_ctx_.merge_mode_ = ObDirectLoadMergeMode::MERGE_WITH_ORIGIN_DATA;
            break;
          case ObDirectLoadInsertMode::OVERWRITE:
            inner_ctx_.merge_mode_ = ObDirectLoadMergeMode::NORMAL;
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected insert mode in full method", KR(ret),
                     K(ctx_->param_.insert_mode_));
            break;
        }
        break;
      // 增量
      case ObDirectLoadMethod::INCREMENTAL:
        if (store_table_ctx->schema_->is_table_without_pk_) {
          inner_ctx_.merge_mode_ = ObDirectLoadMergeMode::NORMAL;
        } else {
          switch (ctx_->param_.insert_mode_) {
            case ObDirectLoadInsertMode::NORMAL:
              inner_ctx_.merge_mode_ = ObDirectLoadMergeMode::MERGE_WITH_CONFLICT_CHECK;
              break;
            case ObDirectLoadInsertMode::INC_REPLACE:
              // 有lob或索引的时候还是需要进行冲突检测
              if (!store_table_ctx->schema_->lob_column_idxs_.empty() ||
                  !store_table_ctx->schema_->index_table_ids_.empty()) {
                inner_ctx_.merge_mode_ = ObDirectLoadMergeMode::MERGE_WITH_CONFLICT_CHECK;
              } else {
                inner_ctx_.merge_mode_ = ObDirectLoadMergeMode::NORMAL;
              }
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected insert mode in incremental method", KR(ret),
                       K(ctx_->param_.insert_mode_));
              break;
          }
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected method", KR(ret), K(ctx_->param_.method_));
        break;
    }
  }
  // use_batch_mode_
  inner_ctx_.use_batch_mode_ = (ObDirectLoadMethod::is_full(ctx_->param_.method_) &&
                                store_table_ctx->schema_->is_column_store_);
  // need_calc_range_
  inner_ctx_.need_calc_range_ = need_rescan_;
  // need_close_insert_tablet_ctx_
  inner_ctx_.need_close_insert_tablet_ctx_ = (!need_rescan_ && !need_del_lob_);
  // is_del_lob_
  inner_ctx_.is_del_lob_ = false;
  merge_table_ctx_ = &inner_ctx_;
  return ret;
}

int ObTableLoadMergeDataTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("ObTableLoadMergeDataTableOp FINISH");
  // close table_build
  for (int64_t i = 0; OB_SUCC(ret) && i < store_ctx_->index_store_table_ctxs_.count(); ++i) {
    ObTableLoadStoreIndexTableCtx *index_table_ctx = store_ctx_->index_store_table_ctxs_.at(i);
    if (OB_FAIL(index_table_ctx->close_build_insert_table())) {
      LOG_WARN("fail to close build index table", KR(ret));
    }
  }
  // 关闭insert_table_ctx
  if (OB_SUCC(ret)) {
    if (OB_FAIL(merge_table_ctx_->insert_table_ctx_->collect_sql_stats(store_ctx_->dml_stats_,
                                                                       store_ctx_->sql_stats_))) {
      LOG_WARN("fail to collect sql stats", KR(ret));
    } else if (OB_FAIL(store_ctx_->data_store_table_ctx_->close_insert_table_ctx())) {
      LOG_WARN("fail to close insert table ctx", KR(ret));
    }
  }
  // reset table_store
  inner_ctx_.table_store_->clear();
  return ret;
}

/**
 * ObTableLoadMergeDeletePhaseDataTableOp
 */

ObTableLoadMergeDeletePhaseDataTableOp::ObTableLoadMergeDeletePhaseDataTableOp(
  ObTableLoadMergePhaseBaseOp *parent)
  : ObTableLoadMergeTableOp(parent)
{
}

int ObTableLoadMergeDeletePhaseDataTableOp::inner_init()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("ObTableLoadMergeDeletePhaseDataTableOp START");
  ObTableLoadStoreDataTableCtx *store_table_ctx = store_ctx_->data_store_table_ctx_;
  need_del_lob_ = (nullptr != store_ctx_->data_store_table_ctx_->lob_table_ctx_);
  need_rescan_ = false;
  // init table_build
  if (need_del_lob_ &&
      OB_FAIL(store_ctx_->data_store_table_ctx_->lob_table_ctx_->init_build_delete_table())) {
    LOG_WARN("fail to init build lob table", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < store_ctx_->index_store_table_ctxs_.count(); ++i) {
    ObTableLoadStoreIndexTableCtx *index_table_ctx = store_ctx_->index_store_table_ctxs_.at(i);
    if (OB_FAIL(index_table_ctx
                  ->init_build_delete_table())) { // unique index table open also, but is empty
      LOG_WARN("fail to init build index table", KR(ret));
    }
  }

  // store_table_ctx_
  inner_ctx_.store_table_ctx_ = store_table_ctx;
  // insert table ctx
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(store_table_ctx->init_insert_table_ctx(merge_phase_ctx_->trans_param_,
                                                            false /*online_opt_stat_gather*/,
                                                            false /*is_insert_lob*/))) {
    LOG_WARN("fail to init insert table ctx", KR(ret));
  }
  inner_ctx_.insert_table_ctx_ = store_table_ctx->insert_table_ctx_;
  // table_store_
  inner_ctx_.table_store_ = &(store_table_ctx->delete_table_store_);
  // dml_row_handler_

  if (OB_SUCC(ret)) {
    ObTableLoadDataRowDeleteHandler *data_row_handler = nullptr;
    if (OB_ISNULL(inner_ctx_.dml_row_handler_ = data_row_handler =
                    OB_NEWx(ObTableLoadDataRowDeleteHandler, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadDataRowDeleteHandler", KR(ret));
    } else if (OB_FAIL(data_row_handler->init(store_ctx_))) {
      LOG_WARN("fail to init data row handler", KR(ret));
    }
  }
  inner_ctx_.merge_mode_ = ObDirectLoadMergeMode::MERGE_WITH_ORIGIN_QUERY_FOR_DATA;
  // use_batch_mode_
  inner_ctx_.use_batch_mode_ = false;
  // need_calc_range_
  inner_ctx_.need_calc_range_ = false;
  // need_close_insert_tablet_ctx_
  inner_ctx_.need_close_insert_tablet_ctx_ = !need_del_lob_;
  // is_del_lob_
  inner_ctx_.is_del_lob_ = false;
  merge_table_ctx_ = &inner_ctx_;
  return ret;
}

int ObTableLoadMergeDeletePhaseDataTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("ObTableLoadMergeDeletePhaseDataTableOp FINISH");
  // close table_build
  for (int64_t i = 0; OB_SUCC(ret) && i < store_ctx_->index_store_table_ctxs_.count(); ++i) {
    ObTableLoadStoreIndexTableCtx *index_table_ctx = store_ctx_->index_store_table_ctxs_.at(i);
    if (OB_FAIL(index_table_ctx
                  ->close_build_delete_table())) { // unique index table close also, but is empty
      LOG_WARN("fail to close build index table", KR(ret));
    }
  }
  // 关闭insert_table_ctx
  if (OB_SUCC(ret)) {
    if (OB_FAIL(store_ctx_->data_store_table_ctx_->close_insert_table_ctx())) {
      LOG_WARN("fail to close insert table ctx", KR(ret));
    }
  }
  // reset table_store
  inner_ctx_.table_store_->clear();
  return ret;
}

ObTableLoadMergeAckPhaseDataTableOp::ObTableLoadMergeAckPhaseDataTableOp(
  ObTableLoadMergePhaseBaseOp *parent)
  : ObTableLoadMergeTableOp(parent)
{
}

int ObTableLoadMergeAckPhaseDataTableOp::inner_init()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("ObTableLoadMergeAckPhaseDataTableOp START");
  ObTableLoadStoreDataTableCtx *store_table_ctx = store_ctx_->data_store_table_ctx_;
  need_rescan_ = false;
  need_del_lob_ = false;

  // store_table_ctx_
  inner_ctx_.store_table_ctx_ = store_table_ctx;
  // insert table ctx
  if (OB_FAIL(store_table_ctx->init_insert_table_ctx(merge_phase_ctx_->trans_param_,
                                                     false /*online_opt_stat_gather*/,
                                                     false /*is_insert_lob*/))) {
    LOG_WARN("fail to init insert table ctx", KR(ret));
  }
  inner_ctx_.insert_table_ctx_ = store_table_ctx->insert_table_ctx_;
  // table_store_
  inner_ctx_.table_store_ = &(store_table_ctx->ack_table_store_);
  // dml_row_handler_

  if (OB_SUCC(ret)) {
    ObTableLoadDataRowAckHandler *data_row_handler = nullptr;
    if (OB_ISNULL(inner_ctx_.dml_row_handler_ = data_row_handler =
                    OB_NEWx(ObTableLoadDataRowAckHandler, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadDataRowDeleteHandler", KR(ret));
    }
  }
  inner_ctx_.merge_mode_ = ObDirectLoadMergeMode::MERGE_WITH_ORIGIN_QUERY_FOR_DATA;
  // use_batch_mode_
  inner_ctx_.use_batch_mode_ = (ObDirectLoadMethod::is_full(ctx_->param_.method_) &&
                                store_table_ctx->schema_->is_column_store_);
  // need_calc_range_
  inner_ctx_.need_calc_range_ = need_rescan_;
  // need_close_insert_tablet_ctx_
  inner_ctx_.need_close_insert_tablet_ctx_ = !need_del_lob_;
  // is_del_lob_
  inner_ctx_.is_del_lob_ = false;
  merge_table_ctx_ = &inner_ctx_;
  return ret;
}

int ObTableLoadMergeAckPhaseDataTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("ObTableLoadMergeAckPhaseDataTableOp FINISH");
  // 关闭insert_table_ctx
  if (OB_FAIL(store_ctx_->data_store_table_ctx_->close_insert_table_ctx())) {
    LOG_WARN("fail to close insert table ctx", KR(ret));
  }
  // reset table_store
  inner_ctx_.table_store_->clear();
  return ret;
}

} // namespace observer
} // namespace oceanbase