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

#include "observer/table_load/ob_table_load_merge_table_op.h"
#include "observer/table_load/ob_table_load_index_row_handler.h"
#include "observer/table_load/ob_table_load_unique_index_row_handler.h"
#include "observer/table_load/ob_table_load_merge_data_op.h"
#include "observer/table_load/ob_table_load_merge_del_lob_op.h"
#include "observer/table_load/ob_table_load_merge_rescan_op.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace observer
{
using namespace common;

/**
 * ObTableLoadMergeTableCtx
 */

ObTableLoadMergeTableCtx::ObTableLoadMergeTableCtx()
  : store_table_ctx_(nullptr),
    insert_table_ctx_(nullptr),
    table_store_(nullptr),
    dml_row_handler_(nullptr),
    merge_mode_(ObDirectLoadMergeMode::INVALID_MERGE_MODE),
    use_batch_mode_(false),
    need_calc_range_(false),
    need_close_insert_tablet_ctx_(false),
    is_del_lob_(false)
{
}

/**
 * ObTableLoadMergeTableBaseOp
 */

ObTableLoadMergeTableBaseOp::ObTableLoadMergeTableBaseOp(ObTableLoadMergePhaseBaseOp *parent)
  : ObTableLoadMergePhaseBaseOp(parent), merge_table_ctx_(nullptr)
{
}

ObTableLoadMergeTableBaseOp::ObTableLoadMergeTableBaseOp(ObTableLoadMergeTableBaseOp *parent)
  : ObTableLoadMergePhaseBaseOp(parent), merge_table_ctx_(parent->merge_table_ctx_)
{
}

/**
 * ObTableLoadMergeTableOp
 */

ObTableLoadMergeTableOp::ObTableLoadMergeTableOp(ObTableLoadMergeTableBaseOp *parent)
  : ObTableLoadMergeTableBaseOp(parent),
    status_(Status::NONE),
    need_rescan_(false),
    need_del_lob_(false)
{
}

ObTableLoadMergeTableOp::ObTableLoadMergeTableOp(ObTableLoadMergePhaseBaseOp *parent)
  : ObTableLoadMergeTableBaseOp(parent),
    status_(Status::NONE),
    need_rescan_(false),
    need_del_lob_(false)
{
}

ObTableLoadMergeTableOp::~ObTableLoadMergeTableOp()
{
  if (nullptr != inner_ctx_.dml_row_handler_) {
    inner_ctx_.dml_row_handler_->~ObDirectLoadDMLRowHandler();
    allocator_->free(inner_ctx_.dml_row_handler_);
    inner_ctx_.dml_row_handler_ = nullptr;
  }
}

int ObTableLoadMergeTableOp::switch_next_op(bool is_parent_called)
{
  int ret = OB_SUCCESS;
  if (is_parent_called && OB_FAIL(inner_init())) {
    LOG_WARN("fail to init", KR(ret));
  } else if (OB_ISNULL(merge_table_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected merge table ctx is null", KR(ret));
  } else {
    ObTableLoadMergeOpType::Type child_op_type = ObTableLoadMergeOpType::INVALID_OP_TYPE;
    switch (status_) {
      case Status::NONE:
        status_ = Status::MERGE_DATA;
        child_op_type = ObTableLoadMergeOpType::MERGE_DATA;
        break;
      case Status::MERGE_DATA:
        if (need_rescan_) {
          status_ = Status::RESCAN;
          child_op_type = ObTableLoadMergeOpType::RESCAN;
          break;
        } else if (need_del_lob_) {
          // del_lob需要用到lob数据, 不能在inner_close的时候才close
          if (ObTableLoadMergerPhaseType::INSERT == merge_phase_ctx_->phase_) {
            if (OB_FAIL(
                  store_ctx_->data_store_table_ctx_->lob_table_ctx_->close_build_delete_table())) {
              LOG_WARN("fail to close build insert table", KR(ret));
            }
          } else if (ObTableLoadMergerPhaseType::DELETE == merge_phase_ctx_->phase_) {
            if (OB_FAIL(
                  store_ctx_->data_store_table_ctx_->lob_table_ctx_->close_build_delete_table())) {
              LOG_WARN("fail to close build delete table", KR(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected phase", KR(ret), K(merge_phase_ctx_->phase_));
          }
          status_ = Status::DEL_LOB;
          child_op_type = ObTableLoadMergeOpType::DEL_LOB;
          break;
        } else {
          status_ = Status::COMPLETED;
        }
        break;
      case Status::RESCAN:
        status_ = Status::COMPLETED;
        break;
      case Status::DEL_LOB:
        status_ = Status::COMPLETED;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status", KR(ret), K(status_));
        break;
    }
    if (OB_SUCC(ret)) {
      if (Status::COMPLETED == status_) {
        if (OB_FAIL(inner_close())) {
          LOG_WARN("fail to inner close", KR(ret));
        } else if (OB_FAIL(switch_parent_op())) {
          LOG_WARN("fail to switch parent op", KR(ret));
        }
      } else if (OB_FAIL(switch_child_op(child_op_type))) {
        LOG_WARN("fail to switch child op", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadMergeTableOp::acquire_child_op(ObTableLoadMergeOpType::Type child_op_type,
                                              ObIAllocator &allocator, ObTableLoadMergeOp *&child)
{
  int ret = OB_SUCCESS;
  child = nullptr;
  switch (child_op_type) {
    OB_TABLE_LOAD_MERGE_ACQUIRE_CHILD_OP(ObTableLoadMergeOpType::MERGE_DATA,
                                         ObTableLoadMergeDataOp);
    OB_TABLE_LOAD_MERGE_ACQUIRE_CHILD_OP(ObTableLoadMergeOpType::RESCAN, ObTableLoadMergeRescanOp);
    OB_TABLE_LOAD_MERGE_ACQUIRE_CHILD_OP(ObTableLoadMergeOpType::DEL_LOB, ObTableLoadMergeDelLobOp);
    OB_TABLE_LOAD_MERGE_UNEXPECTED_CHILD_OP_TYPE(child_op_type);
  }
  return ret;
}

/**
 * ObTableLoadMergeIndexTableOp
 */

ObTableLoadMergeIndexTableOp::ObTableLoadMergeIndexTableOp(
  ObTableLoadMergeTableBaseOp *parent, ObTableLoadStoreTableCtx *store_table_ctx)
  : ObTableLoadMergeTableOp(parent), store_table_ctx_(store_table_ctx)
{
}

int ObTableLoadMergeIndexTableOp::inner_init()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("ObTableLoadMergeIndexTableOp START");
  if (OB_ISNULL(store_table_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected store table ctx is null", KR(ret), KP(store_table_ctx_));
  } else if (OB_FAIL(store_table_ctx_->init_insert_table_ctx(merge_phase_ctx_->trans_param_,
                                                             false /*online_opt_stat_gather*/,
                                                             false /*is_insert_lob*/))) {
    LOG_WARN("fail to init insert table ctx", KR(ret));
  } else {
    // store_table_ctx_
    inner_ctx_.store_table_ctx_ = store_table_ctx_;
    // insert_table_ctx_
    inner_ctx_.insert_table_ctx_ = store_table_ctx_->insert_table_ctx_;
    // table_store_, table_store_ of unique index in delete phase is empty
    if (ObTableLoadMergerPhaseType::INSERT == merge_phase_ctx_->phase_) {
      inner_ctx_.table_store_ =
        &(static_cast<ObTableLoadStoreIndexTableCtx *>(store_table_ctx_)->insert_table_store_);
    } else if (ObTableLoadMergerPhaseType::DELETE == merge_phase_ctx_->phase_) {
      inner_ctx_.table_store_ =
        &(static_cast<ObTableLoadStoreIndexTableCtx *>(store_table_ctx_)->delete_table_store_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected phase", KR(ret), K(merge_phase_ctx_->phase_));
    }
    // dml_row_handler_
    if (OB_SUCC(ret)) {
      if (store_table_ctx_->schema_->is_local_unique_index() &&
          ObTableLoadMergerPhaseType::INSERT == merge_phase_ctx_->phase_) {
        ObTableLoadUniqueIndexRowHandler *index_row_handler = nullptr;
        if (OB_ISNULL(inner_ctx_.dml_row_handler_ = index_row_handler =
                        OB_NEWx(ObTableLoadUniqueIndexRowHandler, allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new ObTableLoadUniqueIndexRowHandler", KR(ret));
        } else if (OB_FAIL(index_row_handler->init(store_ctx_))) {
          LOG_WARN("fail to init ObTableLoadUniqueIndexRowHandler", KR(ret));
        }
      } else {
        ObTableLoadIndexRowHandler *index_row_handler = nullptr;
        if (OB_ISNULL(inner_ctx_.dml_row_handler_ = index_row_handler =
                        OB_NEWx(ObTableLoadIndexRowHandler, allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new ObTableLoadIndexRowHandler", KR(ret));
        }
      }
    }
    // merge_mode_
    if (store_table_ctx_->schema_->is_local_unique_index() &&
        ObTableLoadMergerPhaseType::INSERT == merge_phase_ctx_->phase_) {
      inner_ctx_.merge_mode_ = ObDirectLoadMergeMode::MERGE_WITH_CONFLICT_CHECK;
    } else {
      inner_ctx_.merge_mode_ = ObDirectLoadMergeMode::NORMAL;
    }
    // use_batch_mode_
    inner_ctx_.use_batch_mode_ = false;
    // need_calc_range_
    inner_ctx_.need_calc_range_ = false;
    // need_close_insert_tablet_ctx_
    inner_ctx_.need_close_insert_tablet_ctx_ = true;
    // is_del_lob_
    inner_ctx_.is_del_lob_ = false;
    merge_table_ctx_ = &inner_ctx_;
    if (OB_SUCC(ret)) {
      if (store_table_ctx_->schema_->is_local_unique_index() &&
          ObTableLoadMergerPhaseType::INSERT == merge_phase_ctx_->phase_) {
        if (OB_FAIL(static_cast<ObTableLoadStoreDataTableCtx *>(store_ctx_->data_store_table_ctx_)
                      ->init_build_delete_table())) {
          LOG_WARN("fail to init build delete table", KR(ret));
        } else if (OB_FAIL(static_cast<ObTableLoadStoreDataTableCtx *>(store_ctx_->data_store_table_ctx_)
                      ->init_build_ack_table())) {
          LOG_WARN("fail to init build ack table", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableLoadMergeIndexTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("ObTableLoadMergeIndexTableOp FINISH");
  if (store_table_ctx_->schema_->is_local_unique_index() &&
      ObTableLoadMergerPhaseType::INSERT == merge_phase_ctx_->phase_) {
    if (OB_FAIL(static_cast<ObTableLoadStoreDataTableCtx *>(store_ctx_->data_store_table_ctx_)
                  ->close_build_delete_table())) {
      LOG_WARN("fail to init build delete table", KR(ret));
    } else if (OB_FAIL(
                 static_cast<ObTableLoadStoreDataTableCtx *>(store_ctx_->data_store_table_ctx_)
                   ->close_build_ack_table())) {
      LOG_WARN("fail to init build ack table", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(store_table_ctx_->close_insert_table_ctx())) {
    LOG_WARN("fail to close insert table ctx", KR(ret));
  }
  inner_ctx_.table_store_->clear();
  return ret;
}

/**
 * ObTableLoadMergeIndexesTableOp
 */

ObTableLoadMergeIndexesTableOp::ObTableLoadMergeIndexesTableOp(ObTableLoadMergePhaseBaseOp *parent)
  : ObTableLoadMergeTableOp(parent), pos_(-1), is_inited_(false)
{
}

int ObTableLoadMergeIndexesTableOp::inner_init()
{
  int ret = OB_SUCCESS;
  LOG_INFO("INDEXS TABLE OP START");
  return ret;
}

int ObTableLoadMergeIndexesTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  LOG_INFO("INDEXS TABLE OP FINISH");
  return ret;
}

int ObTableLoadMergeIndexesTableOp::switch_next_op(bool is_parent_called)
{
  int ret = OB_SUCCESS;
  ++pos_;
  if (pos_ >= store_ctx_->index_store_table_ctxs_.count()) {
    if (OB_FAIL(switch_parent_op())) {
      LOG_WARN("fail to switch parent op", KR(ret));
    }
  } else {
    if (OB_FAIL(switch_child_op(ObTableLoadMergeOpType::INDEX_TABLE))) {
      LOG_WARN("fail to switch child op", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadMergeIndexesTableOp::acquire_child_op(ObTableLoadMergeOpType::Type child_op_type,
                                                     ObIAllocator &allocator,
                                                     ObTableLoadMergeOp *&child)
{
  int ret = OB_SUCCESS;
  child = nullptr;
  ObTableLoadStoreTableCtx *store_table_ctx = store_ctx_->index_store_table_ctxs_.at(pos_);
  switch (child_op_type) {
    OB_TABLE_LOAD_MERGE_ACQUIRE_CHILD_OP(ObTableLoadMergeOpType::INDEX_TABLE,
                                         ObTableLoadMergeIndexTableOp, store_table_ctx);
    OB_TABLE_LOAD_MERGE_UNEXPECTED_CHILD_OP_TYPE(child_op_type);
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase