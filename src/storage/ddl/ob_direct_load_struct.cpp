/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_direct_load_struct.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "sql/engine/pdml/static/ob_px_sstable_insert_op.h"
#include "storage/ob_storage_schema_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/das/ob_das_utils.h"
#include "storage/ddl/ob_direct_load_mgr_agent.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "sql/engine/expr/ob_array_expr_utils.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;
using namespace oceanbase::transaction;

int ObTabletDirectLoadInsertParam::assign(const ObTabletDirectLoadInsertParam &other_param)
{
  int ret = OB_SUCCESS;
  if (other_param.common_param_.is_valid()) {
    common_param_ = other_param.common_param_;
  }
  if (other_param.runtime_only_param_.is_valid()) {
    runtime_only_param_ = other_param.runtime_only_param_;
  }
  is_replay_ = other_param.is_replay_;
  return ret;
}

ObDDLSliceRowIterator::ObDDLSliceRowIterator(
    sql::ObPxMultiPartSSTableInsertOp *op,
    const common::ObTabletID &tablet_id,
    const bool is_slice_empty,
    const bool is_index_table,
    const int64_t rowkey_cnt,
    const int64_t snapshot_version,
    const ObTabletSliceParam &ddl_slice_param,
    const bool need_idempotent_autoinc_val,
    const int64_t table_all_slice_count,
    const int64_t table_level_slice_idx,
    const int64_t autoinc_range_interval)
  : op_(op),
    tablet_id_(tablet_id),
    current_row_(),
    rowkey_col_cnt_(rowkey_cnt),
    snapshot_version_(snapshot_version),
    ddl_slice_param_(ddl_slice_param),
    table_all_slice_count_(table_all_slice_count),
    table_level_slice_idx_(table_level_slice_idx),
    cur_row_idx_(0),
    autoinc_range_interval_(autoinc_range_interval),
    is_slice_empty_(is_slice_empty),
    is_next_row_cached_(true),
    need_idempotent_autoinc_val_(need_idempotent_autoinc_val),
    is_index_table_(is_index_table),
    has_lob_rowkey_(true)
{
}

ObDDLSliceRowIterator::~ObDDLSliceRowIterator()
{
}

int ObDDLSliceRowIterator::get_next_row(
    const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == op_ || snapshot_version_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is null", K(ret), KP(op_), K(snapshot_version_));
  } else {
    if (is_slice_empty_) {
      // without any data in the current slice.
      ret = OB_ITER_END;
    } else if (OB_UNLIKELY(is_next_row_cached_)) {
      is_next_row_cached_ = false;
    } else if (OB_FAIL(op_->get_next_row_with_cache())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from child failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      op_->clear_evaluated_flag();
      common::ObTabletID row_tablet_id;
      ObTabletSliceParam row_tablet_slice_param;
      if (OB_FAIL(op_->get_tablet_info_from_row(op_->get_child()->get_spec().output_, row_tablet_id, &row_tablet_slice_param))) {
        LOG_WARN("get tablet info failed", K(ret));
      } // DAISI MARK, for inc_direct_load in ss mode, could use this optimization
      else if (GCTX.is_shared_storage_mode() && row_tablet_id == tablet_id_ && row_tablet_slice_param.slice_id_ != ddl_slice_param_.slice_id_) {
        // only in shared storage mode, one thread may process multiple slice
        ret = OB_ITER_END;
      } else if (row_tablet_id != tablet_id_) {
        // iter the partition end, and switch to next part.
        ret = OB_ITER_END;
      } else {
        const ObExprPtrIArray &exprs = op_->get_spec().ins_ctdef_.new_row_;
        const int64_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
        ObEvalCtx &eval_ctx = op_->get_eval_ctx();
        if (need_idempotent_autoinc_val_) {
          eval_ctx.exec_ctx_.set_ddl_idempotent_autoinc_params(
              table_all_slice_count_, table_level_slice_idx_, cur_row_idx_++,
              autoinc_range_interval_);
        }
        const int64_t request_cnt = exprs.count() + extra_rowkey_cnt;
        if (OB_UNLIKELY((rowkey_col_cnt_ > exprs.count()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected rowkey count", K(ret), K(rowkey_col_cnt_), K(exprs.count()));
        } else if (current_row_.get_column_count() <= 0
          && OB_FAIL(current_row_.init(op_->get_exec_ctx().get_allocator(), request_cnt))) {
          LOG_WARN("init datum row failed", K(ret), K(request_cnt));
        } else if (OB_UNLIKELY(current_row_.get_column_count() != request_cnt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected err", K(ret), K(request_cnt), "datum_row_cnt", current_row_.get_column_count());
        } else {
          int64_t rowkey_length = 0;
          int64_t byte_len = 0;
          bool has_lob = false;
          for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
            ObDatum *datum = nullptr;
            const ObExpr *e = exprs.at(i);
            if (OB_ISNULL(e)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("expr is NULL", K(ret), K(i));
            } else if (OB_FAIL(e->eval(eval_ctx, datum))) {
              LOG_WARN("evaluate expression failed", K(ret), K(i), KPC(e));
            } else if (is_index_table_ && has_lob_rowkey_ && i < rowkey_col_cnt_) {
              if (e->obj_meta_.is_lob_storage()) {
                has_lob = true;
                if (!datum->is_null() && !datum->is_nop()) {
                  const ObString &data = datum->get_string();
                  const bool has_lob_header = e->obj_meta_.has_lob_header();
                  ObLobLocatorV2 locator(data, has_lob_header);
                  if (!locator.is_inrow_disk_lob_locator()) {
                    ret = OB_ERR_TOO_LONG_KEY_LENGTH;
                    LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, OB_MAX_USER_ROW_KEY_LENGTH);
                    STORAGE_LOG(WARN, "invalid lob", K(ret), K(locator), KPC(datum), K(has_lob_header), K(data));
                  }
                }
              }
            }
            if (OB_SUCC(ret)) {
              if (i < rowkey_col_cnt_) {
                current_row_.storage_datums_[i].shallow_copy_from_datum(*datum);
              } else {
                current_row_.storage_datums_[i + extra_rowkey_cnt].shallow_copy_from_datum(*datum);
              }
            }
          }
          if (OB_SUCC(ret)) {
            has_lob_rowkey_ = has_lob;
            // add extra rowkey
            current_row_.storage_datums_[rowkey_col_cnt_].set_int(-snapshot_version_);
            current_row_.storage_datums_[rowkey_col_cnt_ + 1].set_int(0);
            LOG_DEBUG("ddl row iter get next row", K(row_tablet_id), K(tablet_id_), K(row_tablet_slice_param), K(ddl_slice_param_), K(current_row_), K(has_lob), K_(has_lob_rowkey));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    current_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    row = &current_row_;
  }
  return ret;
}


ObDDLInsertRowIterator::ObDDLInsertRowIterator()
  : is_inited_(false),
    source_tenant_id_(MTL_ID()),
    ddl_agent_(nullptr),
    slice_row_iter_(nullptr),
    ls_id_(),
    current_tablet_id_(),
    context_id_(-1),
    macro_seq_(),
    lob_allocator_(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    lob_slice_id_(0),
    lob_cols_cnt_(0),
    is_skip_lob_(false),
    total_slice_cnt_(-1)
{
  lob_id_cache_.set(1/*start*/, 0/*end*/);
}

ObDDLInsertRowIterator::~ObDDLInsertRowIterator()
{

}
int ObDDLInsertRowIterator::init(
    const uint64_t source_tenant_id,
    ObDirectLoadMgrAgent &agent,
    ObIStoreRowIterator *slice_row_iter,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const int64_t context_id,
    const ObTabletSliceParam &tablet_slice_param,
    const int64_t lob_cols_cnt,
    const int64_t total_slice_cnt,
    const bool is_skip_lob)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == source_tenant_id
        || nullptr == slice_row_iter
        || !ls_id.is_valid()
        || !tablet_id.is_valid()
        || context_id < 0
        // no need check tablet slice param, invalid when slice empty
        || lob_cols_cnt < 0
        || (is_shared_storage_dempotent_mode(agent.get_direct_load_type()) && total_slice_cnt < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(source_tenant_id), KP(slice_row_iter), K(ls_id), K(tablet_id), K(context_id), K(tablet_slice_param), K(lob_cols_cnt), K(total_slice_cnt));
  } else if (lob_cols_cnt > 0 && tablet_slice_param.is_valid()
      && OB_FAIL(lob_id_generator_.init(tablet_slice_param.slice_idx_ * ObTabletSliceParam::LOB_ID_SEQ_INTERVAL, // start
                                        ObTabletSliceParam::LOB_ID_SEQ_INTERVAL, // interval
                                        tablet_slice_param.slice_count_ * ObTabletSliceParam::LOB_ID_SEQ_INTERVAL))) { // step
  } else {
    source_tenant_id_ = source_tenant_id;
    ddl_agent_ = &agent;
    slice_row_iter_ = slice_row_iter;
    ls_id_ = ls_id;
    current_tablet_id_ = tablet_id;
    context_id_ = context_id;
    lob_cols_cnt_ = lob_cols_cnt;
    const int64_t parallel_idx = tablet_slice_param.slice_idx_ >= 0 ? tablet_slice_param.slice_idx_ : 0;
    is_skip_lob_ = is_skip_lob;
    is_inited_ = true;
    if ((is_shared_storage_dempotent_mode(ddl_agent_->get_direct_load_type()))) {
      total_slice_cnt_ = total_slice_cnt;
    }
    if (OB_FAIL(macro_seq_.set_parallel_degree(parallel_idx))) {
      LOG_WARN("set failed", K(ret), K(parallel_idx));
  #ifdef OB_BUILD_SHARED_STORAGE
    // Regardless of whether it contains data row or not,
    // the shared-storage ddl requires at least one slice to generate the major sstable in the end.
    // And the shared-nothing ddl allocates the lobid cache when processing each row.
    } else if (is_shared_storage_dempotent_mode(ddl_agent_->get_direct_load_type())) {
      if (!is_skip_lob && lob_cols_cnt_ > 0 && lob_id_cache_.remain_count() < lob_cols_cnt_) { // lob id cache not enough.
        if (OB_FAIL(switch_to_new_lob_slice())) { // close the old slice, and open the new one.
          LOG_WARN("switch to new lob slice failed", K(ret));
        }
      }
  #endif
    }
  }
  return ret;
}

int ObDDLInsertRowIterator::close_lob_sstable_slice()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(slice_row_iter_), KP(ddl_agent_));
  } else if (lob_slice_id_ > 0) {
    blocksstable::ObMacroDataSeq unused_seq;
    ObDirectLoadSliceInfo slice_info;
    slice_info.is_full_direct_load_ = true;
    slice_info.is_lob_slice_ = true;
    slice_info.ls_id_ = ls_id_;
    slice_info.data_tablet_id_ = current_tablet_id_;
    slice_info.slice_id_ = lob_slice_id_;
    slice_info.context_id_ = context_id_;
    if (OB_FAIL(ddl_agent_->close_sstable_slice(slice_info, nullptr/*insert_monitor*/, unused_seq))) {
      LOG_WARN("close sstable slice failed", K(ret), K(slice_info));
#ifdef OB_BUILD_SHARED_STORAGE
    } else if (OB_FAIL(ddl_agent_->update_max_lob_id(lob_id_generator_.get_current()))) {
      LOG_WARN("update max lob id failed", K(ret), "last_lob_id", lob_id_generator_.get_current());
#endif
    } else {
      lob_slice_id_ = 0;
    }
  }
  return ret;
}

int ObDDLInsertRowIterator::get_next_row(
    const bool skip_lob,
    const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  const blocksstable::ObDatumRow *current_row = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(slice_row_iter_), KP(ddl_agent_));
  } else if (OB_FAIL(slice_row_iter_->get_next_row(current_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed", K(ret));
    }
  } else if (OB_ISNULL(current_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret));
  } else if (!skip_lob && lob_cols_cnt_ > 0) { // has lob.
    if (lob_id_cache_.remain_count() < lob_cols_cnt_) { // lob id cache not enough.
      if (OB_FAIL(switch_to_new_lob_slice())) { // close the old slice, and open the new one.
        LOG_WARN("switch to new lob slice failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObDirectLoadSliceInfo slice_info;
      slice_info.is_full_direct_load_ = true;
      slice_info.is_lob_slice_ = true;
      slice_info.ls_id_ = ls_id_;
      slice_info.data_tablet_id_ = current_tablet_id_;
      slice_info.slice_id_ = lob_slice_id_;
      slice_info.context_id_ = context_id_;
      slice_info.src_tenant_id_ = source_tenant_id_;
      lob_allocator_.reuse();
      if (OB_FAIL(ddl_agent_->fill_lob_sstable_slice(lob_allocator_, slice_info,
          lob_id_cache_, *const_cast<blocksstable::ObDatumRow *>(current_row)))) { // const_cast or new assign.
        LOG_WARN("fill batch lob sstable slice failed", K(ret), K(slice_info), KPC(current_row));
      }
    }
  }
  if (OB_ITER_END == ret) {
    // slice no row, or iter slice's row end.
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(close_lob_sstable_slice())) {
      LOG_WARN("fail to close lob sstable slice", K(tmp_ret));
      ret = tmp_ret;
    }
  }
  if (OB_SUCC(ret)) {
    row = current_row;
  }
  return ret;
}


// close old lob sstable slice, request new lob id cache interval,
// and construct new sstable slice writer.
int ObDDLInsertRowIterator::switch_to_new_lob_slice()
{
  int ret = OB_SUCCESS;
  // slice info to close.
  ObDirectLoadSliceInfo slice_info;
  uint64_t lob_id = 0;
  slice_info.is_full_direct_load_ = true;
  slice_info.is_lob_slice_ = true;
  slice_info.ls_id_ = ls_id_;
  slice_info.data_tablet_id_ = current_tablet_id_;
  slice_info.slice_id_ = lob_slice_id_;
  slice_info.context_id_ = context_id_;
  if (is_shared_storage_dempotent_mode(ddl_agent_->get_direct_load_type())) {
    slice_info.total_slice_cnt_ = total_slice_cnt_;
  }
  ObTabletAutoincrementService &auto_inc = ObTabletAutoincrementService::get_instance();
  ObTabletID lob_meta_tablet_id;
  int64_t CACHE_SIZE_REQUESTED = AUTO_INC_CACHE_SIZE;
  blocksstable::ObMacroDataSeq next_block_start_seq;
#ifdef ERRSIM
  if (-10000 == (OB_E(EventTable::EN_DDL_LOBID_CACHE_SIZE_INJECTED) OB_SUCCESS)) {
    CACHE_SIZE_REQUESTED = 10000;
    FLOG_INFO("ddl inject test, set lob cache size 1w", K(CACHE_SIZE_REQUESTED));
  }
#endif
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(slice_row_iter_), KP(ddl_agent_));
  } else if (OB_FAIL(ddl_agent_->get_lob_meta_tablet_id(lob_meta_tablet_id))) {
    LOG_WARN("get tablet direct load mgr failed", K(ret), K(current_tablet_id_));
  } else if (OB_FALSE_IT(lob_id_cache_.tablet_id_ = lob_meta_tablet_id)) {
    // fetch cache via lob meta tablet id.
  } else if (lob_slice_id_ > 0 &&
    OB_FAIL(ddl_agent_->close_sstable_slice(slice_info, nullptr/*insert_monitor*/, next_block_start_seq))) {
    LOG_WARN("close old lob slice failed", K(ret), K(slice_info));
  } else if (lob_slice_id_ > 0) {
    if (OB_UNLIKELY(next_block_start_seq.get_data_seq() < macro_seq_.get_data_seq())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected start sequence", K(ret), K(next_block_start_seq), K(macro_seq_));
    } else {
      macro_seq_ = next_block_start_seq;
    }
  }

  if (OB_FAIL(ret)) {
#ifdef OB_BUILD_SHARED_STORAGE
    // max lob id need manual sync in ss mode
  } else if (is_shared_storage_dempotent_mode(ddl_agent_->get_direct_load_type())) {
    int64_t lob_id_start = -1;
    int64_t lob_id_end = -1;
    if (!lob_id_generator_.is_inited()) {
      // TDDO(cangdi): check this task can not retry
      lob_id_cache_.cache_size_ = CACHE_SIZE_REQUESTED;
      if (OB_FAIL(auto_inc.get_tablet_cache_interval(MTL_ID(), lob_id_cache_))) {
        LOG_WARN("get_autoinc_seq fail", K(ret), K(MTL_ID()), K(slice_info));
      } else if (OB_UNLIKELY(CACHE_SIZE_REQUESTED > lob_id_cache_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected autoincrement value count", K(ret), K(lob_id_cache_));
      }
    } else if (OB_FAIL(lob_id_generator_.get_next_interval(lob_id_start, lob_id_end))) {
      LOG_WARN("get lob id cache from ddl sequence generator fail", K(ret), K(lob_id_generator_));
    } else {
      lob_id_cache_.cache_size_ = lob_id_generator_.get_interval_size();
      lob_id_cache_.set(max(lob_id_start, 1), lob_id_end);
    }
#endif
  } else {
    if (OB_FALSE_IT(lob_id_cache_.cache_size_ = CACHE_SIZE_REQUESTED)) {
    } else if (OB_FAIL(auto_inc.get_tablet_cache_interval(MTL_ID(), lob_id_cache_))) {
      LOG_WARN("get_autoinc_seq fail", K(ret), K(MTL_ID()), K(slice_info));
    } else if (OB_UNLIKELY(CACHE_SIZE_REQUESTED > lob_id_cache_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected autoincrement value count", K(ret), K(lob_id_cache_));
    }
  }
  if (OB_SUCC(ret)) {
    // new slice info to open.
    slice_info.slice_id_ = 0;
    if (OB_FAIL(ddl_agent_->open_sstable_slice(macro_seq_, slice_info))) {
      LOG_WARN("open lob sstable slice failed", KR(ret), K(macro_seq_), K(slice_info));
    } else {
      lob_slice_id_ = slice_info.slice_id_;
    }
  }
  return ret;
}

ObLobMetaRowIterator::ObLobMetaRowIterator()
  : is_inited_(false), iter_(nullptr), trans_id_(0), trans_version_(0), sql_no_(0),
    tmp_row_(), lob_meta_write_result_(), direct_load_type_(DIRECT_LOAD_INVALID)
{
}

ObLobMetaRowIterator::~ObLobMetaRowIterator()
{
  reset();
}

int ObLobMetaRowIterator::init(ObLobMetaWriteIter *iter,
                                const transaction::ObTransID &trans_id,
                                const int64_t trans_version,
                                const int64_t sql_no,
                                const ObDirectLoadType direct_load_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(iter) || OB_UNLIKELY(trans_id < 0 || sql_no < 0 || trans_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("iter is nullptr", K(ret), K(trans_id), K(sql_no), K(trans_version));
  } else if (!tmp_row_.is_valid() && OB_FAIL(tmp_row_.init(ObLobMetaUtil::LOB_META_COLUMN_CNT + ObLobMetaUtil::SKIP_INVALID_COLUMN))) {
    LOG_WARN("Failed to init datum row", K(ret));
  } else {
    iter_ = iter;
    trans_id_ = trans_id;
    trans_version_ = trans_version;
    sql_no_ = sql_no;
    direct_load_type_ = direct_load_type;
    is_inited_ = true;
  }
  return ret;
}

void ObLobMetaRowIterator::reset()
{
  is_inited_ = false;
  iter_ = nullptr;
  trans_id_.reset();
  trans_version_ = 0;
  sql_no_ = 0;
  direct_load_type_ = DIRECT_LOAD_INVALID;
  tmp_row_.reset();
}

void ObLobMetaRowIterator::reuse()
{
  is_inited_ = false;
  iter_ = nullptr;
  trans_id_.reset();
  trans_version_ = 0;
  sql_no_ = 0;
  tmp_row_.reuse();
}

int ObLobMetaRowIterator::get_next_row(const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObLobMetaWriteIter is nullptr", K(ret));
  } else if (OB_FAIL(iter_->get_next_row(lob_meta_write_result_))) {
    if (OB_UNLIKELY(ret != OB_ITER_END)) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else {
    if (OB_FAIL(ObLobMetaUtil::transform_from_info_to_row(lob_meta_write_result_.info_, &tmp_row_, true))) {
      LOG_WARN("transform failed", K(ret), K(lob_meta_write_result_.info_));
    } else {
      tmp_row_.storage_datums_[ObLobMetaUtil::SEQ_ID_COL_ID + 1].set_int(-trans_version_);
      tmp_row_.storage_datums_[ObLobMetaUtil::SEQ_ID_COL_ID + 2].set_int(-get_seq_no());
      tmp_row_.set_trans_id(trans_id_);
      tmp_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      tmp_row_.mvcc_row_flag_.set_compacted_multi_version_row(true);
      tmp_row_.mvcc_row_flag_.set_first_multi_version_row(true);
      tmp_row_.mvcc_row_flag_.set_last_multi_version_row(true);
      tmp_row_.mvcc_row_flag_.set_uncommitted_row(trans_id_.is_valid());
      row = &tmp_row_;
    }
  }
  return ret;
}

int64_t ObLobMetaRowIterator::get_seq_no() const
{
  return is_incremental_direct_load(direct_load_type_) ? lob_meta_write_result_.seq_no_ : sql_no_;
}

ObTabletDDLParam::ObTabletDDLParam()
  : direct_load_type_(ObDirectLoadType::DIRECT_LOAD_INVALID),
    ls_id_(),
    start_scn_(SCN::min_scn()),
    commit_scn_(SCN::min_scn()),
    data_format_version_(0),
    table_key_(),
    snapshot_version_(0),
    trans_id_()
{

}

ObTabletDDLParam::~ObTabletDDLParam()
{

}

/**
 * ObChunkSliceStore
 */
int ObChunkSliceStore::init(const int64_t rowkey_column_count, const ObStorageSchema *storage_schema,
    ObArenaAllocator &allocator, const ObIArray<ObColumnSchemaItem> &col_array, const int64_t dir_id,
    const int64_t parallelism)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null schema", K(ret), K(*this));
  } else if (OB_UNLIKELY(rowkey_column_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalida argument", K(ret), K(rowkey_column_count));
  } else if (OB_FAIL(prepare_datum_stores(MTL_ID(), storage_schema, allocator, col_array, dir_id, parallelism))) {
    LOG_WARN("fail to prepare datum stores");
  } else {
    arena_allocator_ = &allocator;
    rowkey_column_count_ = rowkey_column_count;
    is_inited_ = true;
    LOG_DEBUG("init chunk slice store", K(ret), KPC(this));
  }
  return ret;
}

void ObChunkSliceStore::reset()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(arena_allocator_)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_stores_.count(); ++i) {
      sql::ObCompactStore *cur_store = datum_stores_.at(i);
      cur_store->~ObCompactStore();
      arena_allocator_->free(cur_store);
      cur_store = nullptr;
    }
  }
  datum_stores_.reset();
  cg_schemas_.reset();
  endkey_.reset();
  target_store_idx_ = -1;
  row_cnt_ = 0;
  arena_allocator_ = nullptr;
  is_canceled_ = false;
  is_inited_ = false;
}

int64_t ObChunkSliceStore::calc_chunk_limit(const ObStorageColumnGroupSchema &cg_schema)
{
  const int64_t basic_column_cnt = 10;
  const int64_t basic_chunk_memory_limit = 512L * 1024L; // 512KB
  return ((cg_schema.column_cnt_ / basic_column_cnt) + 1) * basic_chunk_memory_limit;
}

int ObChunkSliceStore::prepare_datum_stores(const uint64_t tenant_id, const ObStorageSchema *storage_schema, ObIAllocator &allocator,
                                            const ObIArray<ObColumnSchemaItem> &col_array, const int64_t dir_id, const int64_t parallelism)
{
  int ret = OB_SUCCESS;
  const int64_t chunk_mem_limit = 64 * 1024L; // 64K
  ObCompactStore *datum_store = nullptr;
  void *buf = nullptr;
  if (OB_UNLIKELY(tenant_id <= 0 || nullptr == storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), KP(storage_schema));
  } else {
    const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema->get_column_groups();
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_schemas.count(); ++i) {
      const ObStorageColumnGroupSchema &cur_cg_schema = cg_schemas.at(i);
      ObCompressorType compressor_type = cur_cg_schema.compressor_type_;
      compressor_type = NONE_COMPRESSOR == compressor_type ? (CS_ENCODING_ROW_STORE == cur_cg_schema.row_store_type_ ? ZSTD_1_3_8_COMPRESSOR : NONE_COMPRESSOR) : compressor_type;
      if (OB_FAIL(ObDDLUtil::get_temp_store_compress_type(compressor_type,
                                                          parallelism,
                                                          compressor_type))) {
        LOG_WARN("fail to get temp store compress type", K(ret));
      }
      if (cur_cg_schema.is_rowkey_column_group() || cur_cg_schema.is_all_column_group()) {
        target_store_idx_ = i;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObCompactStore)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        datum_store = new (buf) ObCompactStore();
        ObArray<ObColumnSchemaItem> cur_column_items;
        cur_column_items.set_attr(ObMemAttr(tenant_id, "tmp_cg_item"));
        for (int64_t j = 0; OB_SUCC(ret) && j < cur_cg_schema.column_cnt_; ++j) {
          int64_t column_idx = cur_cg_schema.get_column_idx(j); // all_cg column_idxs_ = null
          if (column_idx >= col_array.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid column idex", K(ret), K(column_idx), K(col_array.count()), K(i), K(cur_cg_schema));
          } else if (OB_FAIL(cur_column_items.push_back(col_array.at(column_idx)))) {
            LOG_WARN("fail to push_back col_item", K(ret));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(datum_store->init(chunk_mem_limit, cur_column_items, tenant_id, ObCtxIds::DEFAULT_CTX_ID,
                                            "DL_SLICE_STORE", true/*enable_dump*/, 0, false/*disable truncate*/,
                                            compressor_type))) {
          LOG_WARN("failed to init chunk datum store", K(ret));
        } else {
          datum_store->set_dir_id(dir_id);
          datum_store->get_inner_allocator().set_tenant_id(tenant_id);
          LOG_INFO("set dir id", K(dir_id));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(datum_stores_.push_back(datum_store))) {
            LOG_WARN("fail to push back datum_store", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
          if (OB_NOT_NULL(datum_store)) {
            datum_store->~ObCompactStore();
            allocator.free(datum_store);
            datum_store = nullptr;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(cg_schemas_.assign(cg_schemas))) {
        LOG_WARN("fail to copy cg schemas", K(ret));
      }
    }
  }
  LOG_INFO("init ObChunkSliceStore", K(*this));
  return ret;
}

int ObChunkSliceStore::append_row(const blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!datum_row.is_valid() || datum_row.get_column_count() < rowkey_column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(datum_row), K(rowkey_column_count_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_schemas_.count(); ++i) {
      ObStorageColumnGroupSchema &cur_cg_schema = cg_schemas_.at(i);
      sql::ObCompactStore *cur_store = datum_stores_.at(i);
      if (OB_FAIL(cur_store->add_row(datum_row, cur_cg_schema, 0/*extra_size*/))) {
        LOG_WARN("chunk datum store add row failed", K(ret), K(i), K(datum_row.get_column_count()), K(cur_cg_schema), K(cg_schemas_));
      }
    }
    if (OB_SUCC(ret)) {
      ++row_cnt_;
    }
  }
  return ret;
}

int ObChunkSliceStore::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (datum_stores_.count() > 0 && OB_NOT_NULL(datum_stores_.at(target_store_idx_)) && datum_stores_.at(target_store_idx_)->get_row_cnt() > 0) { // save endkey
    const ObChunkDatumStore::StoredRow *stored_row = nullptr;
    ObCompactStore *target_store = datum_stores_.at(target_store_idx_);
    if (OB_FAIL(target_store->get_last_stored_row(stored_row))) {
      LOG_WARN("fail to get last stored row", K(ret));
    } else if (OB_UNLIKELY(nullptr == stored_row || stored_row->cnt_ < rowkey_column_count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("last stored row is null", K(ret), KPC(stored_row));
    } else {
      void *buf = arena_allocator_->alloc(sizeof(ObStorageDatum) * rowkey_column_count_);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for endkey datums failed", K(ret), KPC(stored_row));
      } else {
        endkey_.datums_ = new (buf) ObStorageDatum[rowkey_column_count_];
        endkey_.datum_cnt_ = rowkey_column_count_;
        ObStorageDatum tmp_datum;
        for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_count_; ++i) {
          tmp_datum.shallow_copy_from_datum(stored_row->cells()[i]);
          if (OB_FAIL(endkey_.datums_[i].deep_copy(tmp_datum, *arena_allocator_))) {
            LOG_WARN("deep copy storage datum failed", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_stores_.count(); ++i) {
      if (OB_FAIL(datum_stores_.at(i)->dump(true/*all_dump*/))) {
        LOG_WARN("dump failed", K(ret));
      } else if (OB_FAIL(datum_stores_.at(i)->finish_add_row(true/*need_dump*/))) {
        LOG_WARN("finish add row failed", K(ret));
      }
    }
  }
  LOG_DEBUG("chunk slice store closed", K(ret), K(endkey_));
  return ret;
}

int ObChunkSliceStore::fill_column_group(const int64_t cg_idx,
                                         ObCOSliceWriter *cur_writer,
                                         ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObChunkSliceStore not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(cg_idx < 0 || cg_idx >= datum_stores_.count() || nullptr == cur_writer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(datum_stores_.count()), K(cg_idx), KP(cur_writer));
  } else {
    sql::ObCompactStore *cur_datum_store = datum_stores_.at(cg_idx);
    const ObChunkDatumStore::StoredRow *stored_row = nullptr;
    bool has_next = false;
    int64_t cg_row_inserted_cnt = 0;
    while (OB_SUCC(ret) && OB_SUCC(cur_datum_store->has_next(has_next)) && has_next) {
      if (OB_FAIL(cur_datum_store->get_next_row(stored_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("get next row failed", K(ret));
        }
      } else {
        if (OB_FAIL(cur_writer->append_row(stored_row))) {
          LOG_WARN("append row failed", K(ret), KPC(stored_row));
        } else {
          ++cg_row_inserted_cnt;
          if (0 == cg_row_inserted_cnt % 100) {
            if (OB_NOT_NULL(insert_monitor)) {
              (void) ATOMIC_AAF(&insert_monitor->inserted_cg_row_cnt_, 100);
            }
            if (OB_UNLIKELY(is_canceled_)) {
              ret = OB_CANCELED;
              LOG_WARN("fill column group is canceled", KR(ret));
            } else if (OB_FAIL(share::dag_yield())) {
              LOG_WARN("dag yield failed", K(ret)); // exit for dag task as soon as possible after canceled.
            } else if (OB_FAIL(THIS_WORKER.check_status())) {
              LOG_WARN("check status failed", K(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(insert_monitor)) {
        (void) ATOMIC_AAF(&insert_monitor->inserted_cg_row_cnt_, cg_row_inserted_cnt % 100);
      }
      cur_datum_store->reset();
    }
  }
  return ret;
}

/**
 * ObChunkBatchSliceStore
 */

void ObChunkBatchSliceStore::reset()
{
  is_inited_ = false;
  if (OB_NOT_NULL(arena_allocator_)) {
    for (int64_t i = 0; i < cg_ctxs_.count(); ++i) {
      ColumnGroupCtx *cg_ctx = cg_ctxs_.at(i);
      cg_ctx->~ColumnGroupCtx();
      arena_allocator_->free(cg_ctx);
      cg_ctx = nullptr;
    }
  }
  cg_ctxs_.reset();
  arena_allocator_ = nullptr;
  column_count_ = 0;
  rowkey_column_count_ = 0;
  row_cnt_ = 0;
  start_key_.reset();
  is_canceled_ = false;
}

int ObChunkBatchSliceStore::init(const int64_t rowkey_column_count,
                                 const ObStorageSchema *storage_schema,
                                 ObArenaAllocator &allocator,
                                 const ObIArray<ObColumnSchemaItem> &col_array,
                                 const int64_t dir_id,
                                 const int64_t parallelism,
                                 const int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObChunkBatchSliceStore init twice", KR(ret), KP(this));
  } else if (OB_ISNULL(storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null schema", KR(ret), K(*this));
  } else if (OB_UNLIKELY(rowkey_column_count <= 0 || max_batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(rowkey_column_count), K(max_batch_size));
  } else if (OB_FAIL(prepare_column_group_ctxs(MTL_ID(), storage_schema, allocator, col_array, dir_id, parallelism, max_batch_size))) {
    LOG_WARN("fail to prepare datum stores");
  } else {
    arena_allocator_ = &allocator;
    column_count_ = col_array.count();
    rowkey_column_count_ = rowkey_column_count;
    is_inited_ = true;
  }
  LOG_DEBUG("init chunk batch slice store", KR(ret), KPC(this));
  return ret;
}

int ObChunkBatchSliceStore::prepare_column_group_ctxs(
    const uint64_t tenant_id,
    const ObStorageSchema *storage_schema,
    ObIAllocator &allocator,
    const ObIArray<ObColumnSchemaItem> &col_array,
    const int64_t dir_id,
    const int64_t parallelism,
    const int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  const int64_t chunk_mem_limit = 64 * 1024L; // 64K
  if (OB_UNLIKELY(tenant_id <= 0 || nullptr == storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), KP(storage_schema));
  } else {
    const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema->get_column_groups();
    ObArray<ObColumnSchemaItem> column_items;
    column_items.set_attr(ObMemAttr(tenant_id, "tmp_cg_item"));
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_schemas.count(); ++i) {
      const ObStorageColumnGroupSchema &cg_schema = cg_schemas.at(i);
      ObCompressorType compressor_type = cg_schema.compressor_type_;
      compressor_type = NONE_COMPRESSOR == compressor_type ? (CS_ENCODING_ROW_STORE == cg_schema.row_store_type_ ? ZSTD_1_3_8_COMPRESSOR : NONE_COMPRESSOR) : compressor_type;
      column_items.reuse();
      if (OB_FAIL(ObDDLUtil::get_temp_store_compress_type(compressor_type,
                                                          parallelism,
                                                          compressor_type))) {
        LOG_WARN("fail to get temp store compress type", KR(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < cg_schema.column_cnt_; ++j) {
        const int64_t column_idx = cg_schema.get_column_idx(j); // all_cg column_idxs_ = null
        if (OB_UNLIKELY(column_idx >= col_array.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column idex", KR(ret), K(i), K(j), K(column_idx), K(cg_schema), K(col_array.count()));
        } else if (OB_FAIL(column_items.push_back(col_array.at(column_idx)))) {
          LOG_WARN("fail to push_back col_item", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        const int64_t skip_size = ObBitVector::memory_size(max_batch_size);
        void *skip_mem = nullptr;
        ColumnGroupCtx *cg_ctx = nullptr;
        if (OB_ISNULL(cg_ctx = OB_NEWx(ColumnGroupCtx, &allocator))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new ColumnGroupCtx", KR(ret));
        } else if (OB_FAIL(ObTempColumnStore::init_vectors(column_items, cg_ctx->allocator_, cg_ctx->vectors_))) {
          LOG_WARN("fail to init vectors", KR(ret), K(i), K(column_items));
        } else if (OB_FAIL(cg_ctx->store_.init(cg_ctx->vectors_,
                                               max_batch_size,
                                               ObMemAttr(tenant_id, "DL_CK_VEC_STORE"),
                                               chunk_mem_limit,
                                               true/*enable_dump*/,
                                               compressor_type))) {
          LOG_WARN("failed to init temp column store", KR(ret), K(i), K(column_items));
        } else if (OB_FAIL(cg_ctx->append_vectors_.prepare_allocate(cg_schema.column_cnt_))) {
          LOG_WARN("fail to prepare allocate", KR(ret), K(cg_schema.column_cnt_));
        } else if (OB_ISNULL(skip_mem = allocator.alloc(skip_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc skip buf", KR(ret));
        } else {
          cg_ctx->cg_schema_ = cg_schema;
          cg_ctx->store_.set_dir_id(dir_id);
          cg_ctx->store_.get_inner_allocator().set_tenant_id(tenant_id);
          cg_ctx->brs_.skip_ = to_bit_vector(skip_mem);
          cg_ctx->brs_.skip_->reset(max_batch_size);
          cg_ctx->brs_.size_ = 0;
          cg_ctx->brs_.set_all_rows_active(true);
          LOG_INFO("set dir id", K(dir_id));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(cg_ctxs_.push_back(cg_ctx))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
        if (OB_FAIL(ret)) {
          if (nullptr != cg_ctx) {
            cg_ctx->~ColumnGroupCtx();
            allocator.free(cg_ctx);
            cg_ctx = nullptr;
          }
        }
      }
    }
  }
  return ret;
}

int ObChunkBatchSliceStore::init_start_key()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = arena_allocator_->alloc(sizeof(ObStorageDatum) * rowkey_column_count_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc datums", KR(ret), K(rowkey_column_count_));
  } else {
    start_key_.datums_ = new (buf) ObStorageDatum[rowkey_column_count_];
    start_key_.datum_cnt_ = rowkey_column_count_;
  }
  return ret;
}

int ObChunkBatchSliceStore::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObChunkBatchSliceStore not init", KR(ret), KP(this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_ctxs_.count(); ++i) {
      ColumnGroupCtx *cg_ctx = cg_ctxs_.at(i);
      if (OB_FAIL(cg_ctx->store_.dump(true/*all_dump*/))) {
        LOG_WARN("fail to dump", KR(ret));
      } else if (OB_FAIL(cg_ctx->store_.finish_add_row(true/*need_dump*/))) {
        LOG_WARN("fail to finish add row", KR(ret));
      } else {
        cg_ctx->store_.reset_batch_ctx();
        cg_ctx->append_vectors_.reset();
      }
    }
  }
  LOG_DEBUG("chunk batch slice store closed", KR(ret), K(start_key_));
  return ret;
}

int ObChunkBatchSliceStore::append_batch(const ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObChunkBatchSliceStore not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(datum_rows.get_column_count() < column_count_ || datum_rows.row_count_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(column_count_), K(datum_rows.get_column_count()), K(datum_rows.row_count_));
  } else {
    int64_t stored_rows_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_ctxs_.count(); ++i) {
      ColumnGroupCtx *cg_ctx = cg_ctxs_.at(i);
      const ObStorageColumnGroupSchema &cg_schema = cg_ctx->cg_schema_;
      for (int64_t j = 0; j < cg_schema.column_cnt_; ++j) {
        const int64_t column_idx = cg_schema.get_column_idx(j);
        cg_ctx->append_vectors_.at(j) = datum_rows.vectors_.at(column_idx);
      }
      cg_ctx->brs_.size_ = datum_rows.row_count_;
      if (OB_FAIL(cg_ctx->store_.add_batch(cg_ctx->append_vectors_, cg_ctx->brs_, stored_rows_count))) {
        LOG_WARN("fail to add batch", KR(ret), K(i), K(cg_schema));
      } else if (OB_UNLIKELY(stored_rows_count != datum_rows.row_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected rows count", KR(ret), K(stored_rows_count), K(datum_rows.row_count_));
      }
    }
    // save start_key_
    if (OB_SUCC(ret) && 0 == row_cnt_) {
      bool is_null = false;
      const char *payload = nullptr;
      ObLength length = 0;
      ObStorageDatum tmp_datum;
      if (OB_FAIL(init_start_key())) {
        LOG_WARN("fail to init start key", KR(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_count_; ++i) {
        ObIVector *vec = datum_rows.vectors_.at(i);
        vec->get_payload(0, is_null, payload, length);
        tmp_datum.shallow_copy_from_datum(ObDatum(payload, length, is_null));
        if (OB_FAIL(start_key_.datums_[i].deep_copy(tmp_datum, *arena_allocator_))) {
          LOG_WARN("fail to deep copy storage datum", KR(ret), K(tmp_datum));
        }
      }
    }
    if (OB_SUCC(ret)) {
      row_cnt_ += datum_rows.row_count_;
    }
  }
  return ret;
}

int ObChunkBatchSliceStore::fill_column_group(const int64_t cg_idx,
                                              ObCOSliceWriter *writer,
                                              ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObChunkBatchSliceStore not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(cg_idx < 0 || cg_idx >= cg_ctxs_.count() || nullptr == writer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(cg_ctxs_.count()), K(cg_idx), KP(writer));
  } else {
    ColumnGroupCtx *cg_ctx = cg_ctxs_.at(cg_idx);
    ObTempColumnStore::Iterator iterator;
    int64_t read_rows = 0;
    if (OB_FAIL(cg_ctx->store_.begin(iterator, false/*async*/))) {
      LOG_WARN("fail to begin iterator", KR(ret));
    }
    while (OB_SUCC(ret)) {
      if (OB_UNLIKELY(is_canceled_)) {
        ret = OB_CANCELED;
        LOG_WARN("fill column group is canceled", KR(ret));
      } else if (OB_FAIL(share::dag_yield())) {
        LOG_WARN("dag yield failed", K(ret)); // exit for dag task as soon as possible after canceled.
      } else if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (OB_FAIL(iterator.get_next_batch(cg_ctx->vectors_, read_rows))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next batch", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(writer->append_batch(cg_ctx->vectors_, read_rows))) {
        LOG_WARN("fail to append batch", KR(ret));
      } else {
        if (OB_NOT_NULL(insert_monitor)) {
          (void) ATOMIC_AAF(&insert_monitor->inserted_cg_row_cnt_, read_rows);
        }
      }
    }
    iterator.reset();
    if (OB_SUCC(ret)) {
      cg_ctx->store_.reset();
      cg_ctx->vectors_.reset();
      cg_ctx->allocator_.reset();
    }
  }
  return ret;
}

/**
 * ObColumnSliceStore
 */
ObColumnSliceStore::ObColumnSliceStore()
  : is_inited_(false), allocator_(nullptr), storage_schema_(nullptr), row_count_(0), dumped_row_count_(0), direct_load_type_(DIRECT_LOAD_MAX),
    tenant_data_version_(0), snapshot_version_(0), ddl_task_id_(0), parallel_task_count_(0),
    is_micro_index_clustered_(false), tablet_transfer_seq_(share::OB_INVALID_TRANSFER_SEQ), slice_idx_(0), is_cs_replica_(false)
{

}

ObColumnSliceStore::~ObColumnSliceStore()
{
  destroy();
}

int ObColumnSliceStore::init(ObIAllocator &allocator,
                             const ObStorageSchema *storage_schema,
                             ObTabletDirectLoadMgr *tablet_direct_load_mgr,
                             const blocksstable::ObMacroDataSeq &data_seq,
                             const int64_t slice_idx,
                             const share::SCN &start_scn,
                             const int64_t dir_id,
                             const bool is_cs_replica)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == storage_schema || nullptr == tablet_direct_load_mgr || !data_seq.is_valid() || slice_idx < 0 || !start_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(storage_schema), KP(tablet_direct_load_mgr), K(data_seq), K(slice_idx), K(start_scn));
  } else {
    const int64_t chunk_mem_limit = 64 * 1024L; // 64K
    const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema->get_column_groups();
    const ObIArray<ObColumnSchemaItem> &col_array = tablet_direct_load_mgr->get_column_info();
    allocator_ = &allocator;
    storage_schema_ = storage_schema;
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_schemas.count(); ++i) {
      ObCompactStore *datum_store = nullptr;//TODO@wenqu: use temp column store later
      const ObStorageColumnGroupSchema &cur_cg_schema = cg_schemas.at(i);
      ObArray<ObColumnSchemaItem> cur_column_items;
      cur_column_items.set_attr(ObMemAttr(MTL_ID(), "tmp_cg_item"));
      for (int64_t j = 0; OB_SUCC(ret) && j < cur_cg_schema.column_cnt_; ++j) {
        int64_t column_idx = cur_cg_schema.get_column_idx(j);
        if (column_idx >= col_array.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column idex", K(ret), K(column_idx), K(col_array.count()), K(i), K(cur_cg_schema));
        } else if (OB_FAIL(cur_column_items.push_back(col_array.at(column_idx)))) {
          LOG_WARN("fail to push_back col_item", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        ObCompressorType compressor_type = cur_cg_schema.compressor_type_;
        if (OB_FAIL(ObDDLUtil::get_temp_store_compress_type(compressor_type, 0/*parallelism, unused*/, compressor_type))) {
          LOG_WARN("fail to get temp store compress type", K(ret));
        } else if (OB_FAIL(start_seqs_.push_back(data_seq))) {
          LOG_WARN("push back start seq failed", K(ret));
        } else if (OB_ISNULL(datum_store = OB_NEWx(ObCompactStore, allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else if (OB_FAIL(datum_store->init(chunk_mem_limit, cur_column_items, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID,
                "DL_SLICE_STORE", true/*enable_dump*/, 0/*row_extra_size*/, false/*disable truncate*/, compressor_type))) {
          LOG_WARN("failed to init chunk datum store", K(ret));
        } else {
          datum_store->get_inner_allocator().set_tenant_id(MTL_ID());
          datum_store->set_dir_id(dir_id);
          if (OB_FAIL(datum_stores_.push_back(datum_store))) {
            LOG_WARN("fail to push back datum_store", K(ret));
          }
        }
      }
      if (OB_FAIL(ret) && nullptr != datum_store) {
        datum_store->~ObCompactStore();
        allocator_->free(datum_store);
        datum_store = nullptr;
      }
    }

    if (OB_SUCC(ret)) {
      ls_id_ = tablet_direct_load_mgr->get_ls_id();
      tablet_id_ = tablet_direct_load_mgr->get_table_key().get_tablet_id();
      direct_load_type_ = tablet_direct_load_mgr->get_direct_load_type();
      tenant_data_version_ = tablet_direct_load_mgr->get_data_format_version();
      snapshot_version_ = tablet_direct_load_mgr->get_table_key().get_snapshot_version();
      ddl_task_id_ = tablet_direct_load_mgr->get_ddl_task_id();
      parallel_task_count_ = tablet_direct_load_mgr->get_task_cnt();
      slice_idx_ = slice_idx;
      start_scn_ = start_scn;
      is_cs_replica_ = is_cs_replica;

      is_inited_ = true;
    }
  }
  LOG_INFO("init column slice store finished", K(ret), K(*this));
  return ret;
}

int ObColumnSliceStore::append_row(const blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    const ObIArray<ObStorageColumnGroupSchema > &cg_schemas = storage_schema_->get_column_groups();
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_schemas.count(); ++i) {
      const ObStorageColumnGroupSchema &cur_cg_schema = cg_schemas.at(i);
      sql::ObCompactStore *cur_store = datum_stores_.at(i);
      if (OB_FAIL(cur_store->add_row(datum_row, cur_cg_schema, 0/*extra_size*/))) {
        LOG_WARN("chunk datum store add row failed", K(ret), K(i), K(datum_row.get_column_count()), K(cur_cg_schema));
      }
    }
    if (OB_SUCC(ret)) {
      ++row_count_;
      bool need_dump = false;
      if (OB_FAIL(check_need_dump(need_dump))) {
        LOG_WARN("check need dump failed", K(ret));
      } else if (OB_UNLIKELY(need_dump) && OB_FAIL(dump_macro_block())) {
        LOG_WARN("dump macro block failed", K(ret));
      }
    }
  }
  return ret;
}

int ObColumnSliceStore::dump_macro_block()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (storage_schema_->get_column_group_count() != datum_stores_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datum store not match storage schema", K(ret), K(datum_stores_.count()), K(storage_schema_->get_column_group_count()));
  } else if (row_count_ > 0) {
    const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema_->get_column_groups();
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_schemas.count(); ++i) {
      const ObStorageColumnGroupSchema &cur_cg_schema = cg_schemas.at(i);
      ObCompactStore *cur_datum_store = datum_stores_.at(i);
      ObITable::TableKey table_key;
      table_key.version_range_.snapshot_version_ = snapshot_version_;
      table_key.tablet_id_ = tablet_id_;
      table_key.column_group_idx_ = i;
      table_key.slice_range_.start_slice_idx_ = slice_idx_;
      table_key.slice_range_.end_slice_idx_ = slice_idx_;
      table_key.table_type_ = (cur_cg_schema.is_all_column_group() || cur_cg_schema.is_rowkey_column_group()) ?
      ObITable::TableType::COLUMN_ORIENTED_SSTABLE : ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE;

      ObMacroDataSeq &cur_start_seq = start_seqs_.at(i);
      ObMacroSeqParam macro_seq_param;
      macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
      macro_seq_param.start_ = cur_start_seq.macro_data_seq_;

      ObPreWarmerParam pre_warm_param;
      ObSSTablePrivateObjectCleaner *object_cleaner = nullptr;
      ObWholeDataStoreDesc data_desc;
      ObDDLRedoLogWriterCallback ddl_redo_callback;
      HEAP_VAR(ObMacroBlockWriter, macro_block_writer, true/*is_need_macro_buffer*/) {
      HEAP_VAR(ObSSTableIndexBuilder, index_builder, true /*use buffer*/) {
      const bool need_submit_io = !is_cs_replica_; // if need to process cs replica, only write clog, not submit macro block to disk
      const int64_t row_id_offset = dumped_row_count_; // TODO@wenqu: save dumped_row_count for each cg
      if (OB_FAIL(data_desc.init(true/*is ddl*/,
                                        *storage_schema_,
                                        ls_id_,
                                        tablet_id_,
                                        compaction::ObMergeType::MAJOR_MERGE,
                                        snapshot_version_,
                                        tenant_data_version_,
                                        is_micro_index_clustered_,
                                        tablet_transfer_seq_,
                                        SCN::min_scn()/*end_scn, unused for major*/,
                                        &cur_cg_schema,
                                        i/*cg_idx*/,
                                        compaction::ObExecMode::EXEC_MODE_LOCAL,
                                        need_submit_io))) {
        LOG_WARN("init data store desc failed", K(ret));
      } else if (OB_FAIL(index_builder.init(data_desc.get_desc(), ObSSTableIndexBuilder::ENABLE))) { // data_desc is deep copied
        LOG_WARN("init sstable index builder failed", K(ret), K(ls_id_), K(table_key), K(data_desc));
      } else if (FALSE_IT(data_desc.get_desc().sstable_index_builder_ = &index_builder)) { // for build the tail index block in macro block
      } else if (OB_FAIL(ddl_redo_callback.init(
          ls_id_, tablet_id_, DDL_MB_DATA_TYPE, table_key, ddl_task_id_, start_scn_,
          tenant_data_version_, parallel_task_count_, storage_schema_->get_column_group_count(),
	        direct_load_type_, row_id_offset/*row_id_offset*/, false/*need_delay*/, is_cs_replica_, need_submit_io))) {
        LOG_WARN("fail to init full ddl_redo_callback", K(ret));
      } else if (OB_FAIL(pre_warm_param.init(ls_id_, tablet_id_))) {
        LOG_WARN("failed to init pre warm param", K(ret), K(ls_id_), K(tablet_id_));
      } else if (OB_FAIL(ObSSTablePrivateObjectCleaner::get_cleaner_from_data_store_desc(data_desc.get_desc(), object_cleaner))) {
        LOG_WARN("failed to get cleaner from data store desc", K(ret));
      } else if (OB_FAIL(macro_block_writer.open(data_desc.get_desc(), cur_start_seq.get_parallel_idx(),
              macro_seq_param, pre_warm_param, *object_cleaner, &ddl_redo_callback))) {
        LOG_WARN("open macro bock writer failed", K(ret), K(data_desc), K(macro_seq_param), KPC(object_cleaner));
      } else {
        ObDatumRow cg_row;
        const ObChunkDatumStore::StoredRow *stored_row = nullptr;
        bool has_next = false;
        int64_t cg_row_inserted_cnt = 0;
        if (OB_FAIL(cg_row.init(cur_cg_schema.get_column_count()))) {
          LOG_WARN("init column group row failed", K(ret));
        }
        while (OB_SUCC(ret) && OB_SUCC(cur_datum_store->has_next(has_next)) && has_next) {
          if (OB_FAIL(cur_datum_store->get_next_row(stored_row))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("get next row failed", K(ret));
            }
          } else if (OB_FAIL(ObCOSliceWriter::project_cg_row(cur_cg_schema, stored_row, cg_row))) {
            LOG_WARN("project cg row failed", K(ret), KPC(stored_row));
          } else if (OB_FAIL(macro_block_writer.append_row(cg_row))) {
            LOG_WARN("append row failed", K(ret), K(cg_row));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(macro_block_writer.close())) {
            LOG_WARN("close macro block writer failed", K(ret));
          } else {
            cur_start_seq = macro_block_writer.get_last_macro_seq();
            // cur_datum_store->reuse(); //TODO@wenqu: wait temp column store to support reuse
          }
        }
      }
      } // HEAP_VAR for index_builder
      } // HEAP_VAR for macro_block_writer
    }
  }
  return ret;
}

int ObColumnSliceStore::check_need_dump(bool &need_dump)
{
  int ret = OB_SUCCESS;
  need_dump = false; // TODO@wenqu: never dump until temp store support reuse
//  need_dump = row_count_ > 0 && row_count_ % (500L * 10000L) == 0;
  return ret;
}

void ObColumnSliceStore::destroy()
{
  is_inited_ = false;
  for (int64_t i = 0; OB_NOT_NULL(allocator_) && i < datum_stores_.count(); ++i) {
    ObCompactStore *datum_store = datum_stores_.at(i);
    if (OB_NOT_NULL(datum_store)) {
      datum_store->~ObCompactStore();
      allocator_->free(datum_store);
      datum_store = nullptr;
    }
  }
  datum_stores_.reset();
  storage_schema_ = nullptr;
  row_count_ = 0;
  dumped_row_count_ = 0;
  ls_id_.reset();
  tablet_id_.reset();
  direct_load_type_ = ObDirectLoadType::DIRECT_LOAD_MAX;
  tenant_data_version_ = 0;
  snapshot_version_ = 0;
  ddl_task_id_ = 0;
  parallel_task_count_ = 0;
  is_micro_index_clustered_ = false;
  tablet_transfer_seq_ = share::OB_INVALID_TRANSFER_SEQ;
  start_seqs_.reset();
  start_scn_.reset();
  slice_idx_ = 0;
  is_cs_replica_ = false;
  allocator_ = nullptr;
}

int ObColumnSliceStore::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(dump_macro_block())) {
    LOG_WARN("dump macro block failed", K(ret));
  }
  return ret;
}

/**
 * ObColumnBatchSliceStore
 */

void ObColumnBatchSliceStore::reset()
{
  is_inited_ = false;
  if (OB_NOT_NULL(arena_allocator_)) {
    for (int64_t i = 0; i < cg_ctxs_.count(); ++i) {
      ColumnGroupCtx *cg_ctx = cg_ctxs_.at(i);
      cg_ctx->~ColumnGroupCtx();
      arena_allocator_->free(cg_ctx);
      cg_ctx = nullptr;
    }
  }
  cg_ctxs_.reset();
  arena_allocator_ = nullptr;
  column_count_ = 0;
  rowkey_column_count_ = 0;
  row_cnt_ = 0;
  slice_idx_ = 0;
  merge_slice_idx_ = 0;
  storage_schema_ = nullptr;
  direct_load_type_ = DIRECT_LOAD_MAX;
  tenant_data_version_ = 0;
  snapshot_version_ = 0;
  ddl_task_id_ = 0;
  parallel_task_count_ = 0;
  is_micro_index_clustered_ = false;
  tablet_transfer_seq_ = OB_INVALID_TRANSFER_SEQ;
  is_cs_replica_ = false;
}

int ObColumnBatchSliceStore::init(
    const int64_t rowkey_column_count,
    const ObStorageSchema *storage_schema,
    ObArenaAllocator &allocator,
    const int64_t slice_idx,
    const int64_t merge_slice_idx,
    ObTabletDirectLoadMgr *tablet_direct_load_mgr,
    const blocksstable::ObMacroDataSeq &data_seq,
    const share::SCN &start_scn,
    const int64_t dir_id,
    const bool is_cs_replica,
    const int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObColumnBatchSliceStore init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(rowkey_column_count <= 0 ||
                         slice_idx < 0 ||
                         nullptr == storage_schema ||
                         nullptr == tablet_direct_load_mgr ||
                         !data_seq.is_valid() ||
                         !start_scn.is_valid() ||
                         max_batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(rowkey_column_count), K(slice_idx), KP(storage_schema),
        KP(tablet_direct_load_mgr), K(data_seq), K(start_scn), K(max_batch_size));
  } else if (OB_FAIL(prepare_column_group_ctxs(MTL_ID(),
                                               storage_schema,
                                               allocator,
                                               tablet_direct_load_mgr->get_column_info(),
                                               data_seq,
                                               dir_id,
                                               max_batch_size))) {
    LOG_WARN("fail to prepare datum stores");
  } else {
    arena_allocator_ = &allocator;
    rowkey_column_count_ = rowkey_column_count;
    column_count_ = tablet_direct_load_mgr->get_column_info().count();
    slice_idx_ = slice_idx;
    merge_slice_idx_ = merge_slice_idx;
    storage_schema_ = storage_schema;
    ls_id_ = tablet_direct_load_mgr->get_ls_id();
    tablet_id_ = tablet_direct_load_mgr->get_table_key().get_tablet_id();
    direct_load_type_ = tablet_direct_load_mgr->get_direct_load_type();
    tenant_data_version_ = tablet_direct_load_mgr->get_data_format_version();
    snapshot_version_ = tablet_direct_load_mgr->get_table_key().get_snapshot_version();
    ddl_task_id_ = tablet_direct_load_mgr->get_ddl_task_id();
    parallel_task_count_ = tablet_direct_load_mgr->get_task_cnt();
    start_scn_ = start_scn;
    is_cs_replica_ = is_cs_replica;
    is_inited_ = true;
  }
  LOG_DEBUG("init column batch slice store", KR(ret), KPC(this));
  return ret;
}

int ObColumnBatchSliceStore::prepare_column_group_ctxs(
    const uint64_t tenant_id,
    const ObStorageSchema *storage_schema,
    ObIAllocator &allocator,
    const ObIArray<ObColumnSchemaItem> &col_array,
    const blocksstable::ObMacroDataSeq &data_seq,
    const int64_t dir_id,
    const int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  const int64_t chunk_mem_limit = 64 * 1024L; // 64K
  if (OB_UNLIKELY(tenant_id <= 0 || nullptr == storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), KP(storage_schema));
  } else {
    const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema->get_column_groups();
    ObArray<ObColumnSchemaItem> column_items;
    column_items.set_attr(ObMemAttr(tenant_id, "tmp_cg_item"));
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_schemas.count(); ++i) {
      const ObStorageColumnGroupSchema &cg_schema = cg_schemas.at(i);
      ObCompressorType compressor_type = cg_schema.compressor_type_;
      compressor_type = NONE_COMPRESSOR == compressor_type ? (CS_ENCODING_ROW_STORE == cg_schema.row_store_type_ ? ZSTD_1_3_8_COMPRESSOR : NONE_COMPRESSOR) : compressor_type;
      column_items.reuse();
      if (OB_FAIL(ObDDLUtil::get_temp_store_compress_type(compressor_type,
                                                          0/*parallelism, unused*/,
                                                          compressor_type))) {
        LOG_WARN("fail to get temp store compress type", KR(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < cg_schema.column_cnt_; ++j) {
        const int64_t column_idx = cg_schema.get_column_idx(j); // all_cg column_idxs_ = null
        if (OB_UNLIKELY(column_idx >= col_array.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column idex", KR(ret), K(i), K(j), K(column_idx), K(cg_schema), K(col_array.count()));
        } else if (OB_FAIL(column_items.push_back(col_array.at(column_idx)))) {
          LOG_WARN("fail to push_back col_item", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        const int64_t skip_size = ObBitVector::memory_size(max_batch_size);
        void *skip_mem = nullptr;
        ColumnGroupCtx *cg_ctx = nullptr;
        if (OB_ISNULL(cg_ctx = OB_NEWx(ColumnGroupCtx, &allocator))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new ColumnGroupCtx", KR(ret));
        } else if (OB_FAIL(cg_ctx->append_vectors_.prepare_allocate(cg_schema.column_cnt_))) {
          LOG_WARN("fail to prepare allocate", KR(ret), K(cg_schema.column_cnt_));
        } else if (OB_FAIL(ObTempColumnStore::init_vectors(column_items, cg_ctx->allocator_, cg_ctx->datum_rows_.vectors_))) {
          LOG_WARN("fail to init vectors", KR(ret), K(i), K(column_items));
        } else if (OB_FAIL(cg_ctx->store_.init(cg_ctx->datum_rows_.vectors_,
                                               max_batch_size,
                                               ObMemAttr(tenant_id, "DL_CL_VEC_STORE"),
                                               chunk_mem_limit,
                                               true/*enable_dump*/,
                                               compressor_type))) {
          LOG_WARN("failed to init temp column store", KR(ret), K(i), K(column_items));
        } else if (OB_ISNULL(skip_mem = allocator.alloc(skip_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc skip buf", KR(ret));
        } else {
          cg_ctx->cg_schema_ = cg_schema;
          cg_ctx->store_.set_enable_truncate(true);
          cg_ctx->store_.set_sequential_read(true);
          cg_ctx->store_.set_dir_id(dir_id);
          cg_ctx->store_.get_inner_allocator().set_tenant_id(tenant_id);
          cg_ctx->brs_.skip_ = to_bit_vector(skip_mem);
          cg_ctx->brs_.skip_->reset(max_batch_size);
          cg_ctx->brs_.size_ = 0;
          cg_ctx->brs_.set_all_rows_active(true);
          cg_ctx->datum_rows_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
          cg_ctx->data_seq_ = data_seq;
          LOG_INFO("set dir id", K(dir_id));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(cg_ctxs_.push_back(cg_ctx))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
        if (OB_FAIL(ret)) {
          if (nullptr != cg_ctx) {
            cg_ctx->~ColumnGroupCtx();
            allocator.free(cg_ctx);
            cg_ctx = nullptr;
          }
        }
      }
    }
  }
  return ret;
}

int ObColumnBatchSliceStore::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObColumnBatchSliceStore not init", KR(ret), KP(this));
  } else if (OB_FAIL(dump_macro_block())) {
    LOG_WARN("dump macro block failed", K(ret));
  } else {
    reset();
  }
  LOG_DEBUG("column batch slice store closed", KR(ret));
  return ret;
}

int ObColumnBatchSliceStore::append_batch(const ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObColumnBatchSliceStore not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(datum_rows.get_column_count() < column_count_ || datum_rows.row_count_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(column_count_), K(datum_rows.get_column_count()), K(datum_rows.row_count_));
  } else {
    int64_t stored_rows_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_ctxs_.count(); ++i) {
      ColumnGroupCtx *cg_ctx = cg_ctxs_.at(i);
      const ObStorageColumnGroupSchema &cg_schema = cg_ctx->cg_schema_;
      for (int64_t j = 0; j < cg_schema.column_cnt_; ++j) {
        const int64_t column_idx = cg_schema.get_column_idx(j);
        cg_ctx->append_vectors_.at(j) = datum_rows.vectors_.at(column_idx);
      }
      cg_ctx->brs_.size_ = datum_rows.row_count_;
      if (OB_FAIL(cg_ctx->store_.add_batch(cg_ctx->append_vectors_, cg_ctx->brs_, stored_rows_count))) {
        LOG_WARN("fail to add batch", KR(ret), K(i), K(cg_schema));
      } else if (OB_UNLIKELY(stored_rows_count != datum_rows.row_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected rows count", KR(ret), K(stored_rows_count), K(datum_rows.row_count_));
      }
    }
    if (OB_SUCC(ret)) {
      row_cnt_ += datum_rows.row_count_;
      bool need_dump = false;
      if (OB_FAIL(check_need_dump(need_dump))) {
        LOG_WARN("check need dump failed", K(ret));
      } else if (OB_UNLIKELY(need_dump)) {
        if (OB_FAIL(dump_macro_block())) {
          LOG_WARN("dump macro block failed", K(ret));
        } else {
          row_cnt_ = 0;
        }
      }
    }
  }
  return ret;
}

int ObColumnBatchSliceStore::check_need_dump(bool &need_dump)
{
  int ret = OB_SUCCESS;
  need_dump = row_cnt_ > (500L * 10000L);
  return ret;
}

int ObColumnBatchSliceStore::dump_macro_block()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (row_cnt_ > 0) {
    const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema_->get_column_groups();
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_schemas.count(); ++i) {
      const ObStorageColumnGroupSchema &cur_cg_schema = cg_schemas.at(i);
      ColumnGroupCtx *cg_ctx = cg_ctxs_.at(i);
      ObITable::TableKey table_key;
      table_key.version_range_.snapshot_version_ = snapshot_version_;
      table_key.tablet_id_ = tablet_id_;
      table_key.column_group_idx_ = i;
      table_key.slice_range_.start_slice_idx_ = slice_idx_;
      table_key.slice_range_.end_slice_idx_ = slice_idx_;
      table_key.table_type_ = (cur_cg_schema.is_all_column_group() || cur_cg_schema.is_rowkey_column_group()) ?
                              ObITable::TableType::COLUMN_ORIENTED_SSTABLE : ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE;

      ObMacroDataSeq &cur_start_seq = cg_ctx->data_seq_;
      ObMacroSeqParam macro_seq_param;
      macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
      macro_seq_param.start_ = cur_start_seq.macro_data_seq_;

      ObPreWarmerParam pre_warm_param;
      ObSSTablePrivateObjectCleaner *object_cleaner = nullptr;
      ObWholeDataStoreDesc data_desc;
      ObDDLRedoLogWriterCallback ddl_redo_callback;
      HEAP_VAR(ObMacroBlockWriter, macro_block_writer, true/*is_need_macro_buffer*/) {
        HEAP_VAR(ObSSTableIndexBuilder, index_builder, true /*use buffer*/) {
          const bool need_submit_io = !is_cs_replica_; // if need to process cs replica, only write clog, not submit macro block to disk
          const int64_t row_id_offset = cg_ctx->dumped_row_count_;
          if (OB_FAIL(data_desc.init(true/*is ddl*/,
                                    *storage_schema_,
                                    ls_id_,
                                    tablet_id_,
                                    compaction::ObMergeType::MAJOR_MERGE,
                                    snapshot_version_,
                                    tenant_data_version_,
                                    is_micro_index_clustered_,
                                    tablet_transfer_seq_,
                                    SCN::min_scn()/*end_scn, unused for major*/,
                                    &cur_cg_schema,
                                    i/*cg_idx*/,
                                    compaction::ObExecMode::EXEC_MODE_LOCAL,
                                    need_submit_io))) {
            LOG_WARN("init data store desc failed", K(ret));
          } else if (OB_FAIL(index_builder.init(data_desc.get_desc(), ObSSTableIndexBuilder::ENABLE))) { // data_desc is deep copied
            LOG_WARN("init sstable index builder failed", K(ret), K(ls_id_), K(table_key), K(data_desc));
          } else if (FALSE_IT(data_desc.get_desc().sstable_index_builder_ = &index_builder)) { // for build the tail index block in macro block
          } else if (OB_FAIL(ddl_redo_callback.init(ls_id_,
                                                    tablet_id_,
                                                    DDL_MB_DATA_TYPE,
                                                    table_key,
                                                    ddl_task_id_,
                                                    start_scn_,
                                                    tenant_data_version_,
                                                    parallel_task_count_,
                                                    storage_schema_->get_column_group_count(),
                                                    direct_load_type_,
                                                    row_id_offset/*row_id_offset*/,
                                                    false/*need_delay*/,
                                                    is_cs_replica_,
                                                    need_submit_io))) {
            LOG_WARN("fail to init full ddl_redo_callback", K(ret));
          } else if (FALSE_IT(ddl_redo_callback.set_merge_slice_idx(merge_slice_idx_))) {
          } else if (OB_FAIL(pre_warm_param.init(ls_id_, tablet_id_))) {
            LOG_WARN("failed to init pre warm param", K(ret), K(ls_id_), K(tablet_id_));
          } else if (OB_FAIL(ObSSTablePrivateObjectCleaner::get_cleaner_from_data_store_desc(data_desc.get_desc(), object_cleaner))) {
            LOG_WARN("failed to get cleaner from data store desc", K(ret));
          } else if (OB_FAIL(macro_block_writer.open(data_desc.get_desc(),
                                                    cur_start_seq.get_parallel_idx(),
                                                    macro_seq_param,
                                                    pre_warm_param,
                                                    *object_cleaner,
                                                    &ddl_redo_callback))) {
            LOG_WARN("open macro bock writer failed", K(ret), K(data_desc), K(macro_seq_param), KPC(object_cleaner));
          } else {
            ObTempColumnStore::Iterator iterator;
            int64_t read_rows = 0;
            if (OB_FAIL(cg_ctx->store_.begin(iterator, false/*async*/))) {
              LOG_WARN("fail to begin iterator", KR(ret));
            }
            while (OB_SUCC(ret)) {
              if (OB_FAIL(share::dag_yield())) {
                LOG_WARN("dag yield failed", K(ret)); // exit for dag task as soon as possible after canceled.
              } else if (OB_FAIL(THIS_WORKER.check_status())) {
                LOG_WARN("check status failed", K(ret));
              } else if (OB_FAIL(iterator.get_next_batch(cg_ctx->datum_rows_.vectors_, read_rows))) {
                if (OB_UNLIKELY(OB_ITER_END != ret)) {
                  LOG_WARN("fail to get next batch", KR(ret));
                } else {
                  ret = OB_SUCCESS;
                  break;
                }
              } else if (FALSE_IT(cg_ctx->datum_rows_.row_count_ = read_rows)) {
              } else if (OB_FAIL(macro_block_writer.append_batch(cg_ctx->datum_rows_))) {
                LOG_WARN("write column group row failed", K(ret));
              } else {
                cg_ctx->dumped_row_count_ += read_rows;
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(macro_block_writer.close())) {
                LOG_WARN("close macro block writer failed", K(ret));
              } else {
                cur_start_seq = macro_block_writer.get_last_macro_seq();
                iterator.reset();
                cg_ctx->store_.reuse();
              }
            }
          }
        } // HEAP_VAR for index_builder
      } // HEAP_VAR for macro_block_writer
    }
  }
  return ret;
}

/**
 * ObMacroBlockSliceStore
 */
int ObMacroBlockSliceStore::init(
    ObTabletDirectLoadMgr *tablet_direct_load_mgr,
    const blocksstable::ObMacroDataSeq &data_seq,
    const SCN &start_scn,
    const bool need_process_cs_replica /*= false*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(nullptr == tablet_direct_load_mgr || !data_seq.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(tablet_direct_load_mgr), K(data_seq));
  } else {
    const ObLSID &ls_id = tablet_direct_load_mgr->get_ls_id();
    const ObITable::TableKey &table_key = tablet_direct_load_mgr->get_table_key(); // TODO(cangdi): fix it with right table key
    const int64_t ddl_task_id = tablet_direct_load_mgr->get_ddl_task_id();
    const uint64_t data_format_version = tablet_direct_load_mgr->get_data_format_version();
    const ObDirectLoadType direct_load_type = tablet_direct_load_mgr->get_direct_load_type();
    const ObWholeDataStoreDesc &data_desc = tablet_direct_load_mgr->get_sqc_build_ctx().data_block_desc_;
    ObTxDesc *tx_desc = tablet_direct_load_mgr->get_sqc_build_ctx().build_param_.runtime_only_param_.tx_desc_;
    const ObTransID &trans_id = tablet_direct_load_mgr->get_sqc_build_ctx().build_param_.runtime_only_param_.trans_id_;
    if (is_incremental_direct_load(direct_load_type)) {
      if (OB_ISNULL(ddl_redo_callback_ = OB_NEW(ObDDLIncRedoLogWriterCallback, ObMemAttr(MTL_ID(), "DDL_MBSS")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret));
      } else if (OB_FAIL(static_cast<ObDDLIncRedoLogWriterCallback *>(ddl_redo_callback_)->init(
          ls_id, table_key.tablet_id_, DDL_MB_DATA_TYPE, table_key, ddl_task_id, start_scn, data_format_version, direct_load_type, tx_desc, trans_id,
          tablet_direct_load_mgr->get_task_cnt(), tablet_direct_load_mgr->get_cg_cnt()))) {
        LOG_WARN("fail to init inc ddl_redo_callback_", K(ret));
      }
    } else {
      if (OB_ISNULL(ddl_redo_callback_ = OB_NEW(ObDDLRedoLogWriterCallback, ObMemAttr(MTL_ID(), "DDL_MBSS")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret));
      } else if (OB_FAIL(static_cast<ObDDLRedoLogWriterCallback *>(ddl_redo_callback_)->init(
          ls_id, table_key.tablet_id_,
          tablet_direct_load_mgr->get_is_no_logging() ? DDL_MB_SS_EMPTY_DATA_TYPE : DDL_MB_DATA_TYPE,
          table_key, ddl_task_id, start_scn,
          data_format_version,
          tablet_direct_load_mgr->get_task_cnt(),
          tablet_direct_load_mgr->get_cg_cnt(),
	        direct_load_type,
          -1/*row_id_offset*/,
          false /*need_delay*/,
          need_process_cs_replica))) {
        LOG_WARN("fail to init full ddl_redo_callback_", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObMacroSeqParam macro_seq_param;
      macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
      macro_seq_param.start_ = data_seq.macro_data_seq_;
      ObPreWarmerParam pre_warm_param;
      ObSSTablePrivateObjectCleaner *object_cleaner = nullptr;
      if (OB_FAIL(pre_warm_param.init(ls_id, table_key.tablet_id_))) {
        LOG_WARN("failed to init pre warm param", K(ret), K(ls_id), "tablet_id", table_key.tablet_id_);
      } else if (OB_FAIL(ObSSTablePrivateObjectCleaner::get_cleaner_from_data_store_desc(
                                 tablet_direct_load_mgr->get_sqc_build_ctx().data_block_desc_.get_desc(),
                                 object_cleaner))) {
        LOG_WARN("failed to get cleaner from data store desc", K(ret));
      } else if (OB_FAIL(macro_block_writer_.open(
                     data_desc.get_desc(), data_seq.get_parallel_idx(),
                     macro_seq_param, pre_warm_param, *object_cleaner,
                     ddl_redo_callback_))) {
        LOG_WARN("open macro bock writer failed", K(ret), K(macro_seq_param), KPC(object_cleaner));
      } else {
        need_process_cs_replica_ = need_process_cs_replica;
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObMacroBlockSliceStore::append_row(const blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(macro_block_writer_.append_row(datum_row))) {
    LOG_WARN("macro block writer append row failed", K(ret), K(datum_row));
  }
  return ret;
}

int ObMacroBlockSliceStore::append_batch(const blocksstable::ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMacroBlockSliceStore not init", KR(ret), KP(this));
  } else if (OB_FAIL(macro_block_writer_.append_batch(datum_rows))) {
    LOG_WARN("macro block writer append batch failed", K(ret));
  }
  return ret;
}

int ObMacroBlockSliceStore::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(macro_block_writer_.close())) {
    LOG_WARN("close macro block writer failed", K(ret));
  }
  return ret;
}

ObMultiSliceStore::ObMultiSliceStore()
: is_inited_(false),
  arena_allocator_(nullptr),
  cs_replica_schema_(nullptr),
  row_slice_store_(nullptr),
  column_slice_store_(nullptr)
{}

ObMultiSliceStore::~ObMultiSliceStore()
{
  reset();
}

int ObMultiSliceStore::init(
    ObArenaAllocator &allocator,
    ObTabletDirectLoadMgr *tablet_direct_load_mgr,
    const blocksstable::ObMacroDataSeq &data_seq,
    const int64_t slice_idx,
    const int64_t merge_slice_idx,
    const share::SCN &start_scn,
    const int64_t rowkey_column_count,
    const ObStorageSchema *storage_schema,
    const ObIArray<ObColumnSchemaItem> &col_schema,
    const int64_t dir_id,
    const int64_t parallelism,
    const bool use_batch_store,
    const int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("multi slice store init twice", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(nullptr == tablet_direct_load_mgr
                      || !data_seq.is_valid()
                      || slice_idx < 0
                      || rowkey_column_count <= 0
                      || nullptr == storage_schema
                      || !storage_schema->is_row_store()
                      || !storage_schema->is_user_data_table())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(tablet_direct_load_mgr), K(data_seq), K(slice_idx), K(rowkey_column_count), KPC(storage_schema));
  } else if (OB_FAIL(ObStorageSchemaUtil::alloc_storage_schema(allocator, cs_replica_schema_))) {
    LOG_WARN("fail to alloc cs_replica_schema", K(ret));
  } else if (OB_FAIL(cs_replica_schema_->init(allocator, *storage_schema, false /*skip_column_info*/, nullptr /*column_group_schema*/, true /*generate_default_cg_array*/))) {
    LOG_WARN("fail to init cs_replica_schema for multi slice store", K(ret), KPC(storage_schema));
  } else if (OB_ISNULL(row_slice_store_ = OB_NEWx(ObMacroBlockSliceStore, &allocator))){
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for row slice store", K(ret));
  } else if (OB_FAIL(row_slice_store_->init(tablet_direct_load_mgr, data_seq, start_scn, true /*need_process_cs_replica*/))) {
    LOG_WARN("fail to init row slice store", K(ret), KPC(tablet_direct_load_mgr), K(data_seq), K(start_scn));
  } else {
    if (!ObDDLUtil::need_rescan_column_store(tablet_direct_load_mgr->get_data_format_version())) {
      if (use_batch_store) {
        ObColumnBatchSliceStore *column_slice_store = nullptr;
        if (OB_ISNULL(column_slice_store_ = column_slice_store = OB_NEWx(ObColumnBatchSliceStore, &allocator))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new ObColumnBatchSliceStore", KR(ret));
        } else if (OB_FAIL(column_slice_store->init(rowkey_column_count, cs_replica_schema_, allocator,
                slice_idx, merge_slice_idx, tablet_direct_load_mgr, data_seq, start_scn, dir_id, true/*is_cs_replica*/, max_batch_size))) {
          LOG_WARN("init column batch slice store failed", K(ret), KPC(storage_schema), KPC(tablet_direct_load_mgr), K(data_seq), K(start_scn), K(dir_id));
        }
      } else {
        ObColumnSliceStore *column_slice_store = nullptr;
        if (OB_ISNULL(column_slice_store_ = column_slice_store = OB_NEWx(ObColumnSliceStore, &allocator))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory for chunk slice store failed", K(ret));
        } else if (OB_FAIL(column_slice_store->init(allocator, cs_replica_schema_, tablet_direct_load_mgr, data_seq, slice_idx, start_scn, dir_id, true/*is_cs_replica*/))) {
          LOG_WARN("init column slice store failed", K(ret), KPC(storage_schema), KPC(tablet_direct_load_mgr), K(data_seq), K(start_scn), K(dir_id));
        }
      }
    } else {
      if (use_batch_store) {
        ObChunkBatchSliceStore *column_slice_store = nullptr;
        if (OB_ISNULL(column_slice_store_ = column_slice_store = OB_NEWx(ObChunkBatchSliceStore, &allocator))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new ObChunkBatchSliceStore", KR(ret));
        } else if (OB_FAIL(column_slice_store->init(rowkey_column_count, cs_replica_schema_, allocator, col_schema, dir_id, parallelism, max_batch_size))) {
          LOG_WARN("fail to init chunk batch slice store", K(ret), K(dir_id), K(parallelism), KPC(storage_schema), K(col_schema), K(rowkey_column_count), K(max_batch_size));
        }
      } else {
        ObChunkSliceStore *column_slice_store = nullptr;
        if (OB_ISNULL(column_slice_store_ = column_slice_store = OB_NEWx(ObChunkSliceStore, &allocator))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new ObChunkSliceStore", KR(ret));
        } else if (OB_FAIL(column_slice_store->init(rowkey_column_count, cs_replica_schema_, allocator, col_schema, dir_id, parallelism))) {
          LOG_WARN("fail to init chunk slice store", K(ret), K(dir_id), K(parallelism), KPC(storage_schema), K(col_schema), K(rowkey_column_count));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
    arena_allocator_ = &allocator;
    LOG_DEBUG("[CS-Replica] Successfully init multi slice store", K(ret), KPC(this));
  }

  if (OB_FAIL(ret)) {
    (void) free_memory(allocator);
  }
  return ret;
}

int ObMultiSliceStore::append_row(const blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("multi slice store not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == row_slice_store_ ||  nullptr == column_slice_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected slice store", K(ret), KPC_(row_slice_store), KPC_(column_slice_store));
  } else if (OB_FAIL(row_slice_store_->append_row(datum_row))) {
    LOG_WARN("fail to append row to row slice store", K(ret));
  } else if (OB_FAIL(column_slice_store_->append_row(datum_row))) {
    LOG_WARN("fail to append row to column slice store", K(ret));
  }
  return ret;
}

int ObMultiSliceStore::append_batch(const blocksstable::ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMultiSliceStore not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == row_slice_store_ ||  nullptr == column_slice_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected slice store", KR(ret), KP(row_slice_store_), KP(column_slice_store_));
  } else if (OB_FAIL(row_slice_store_->append_batch(datum_rows))) {
    LOG_WARN("fail to append batch to row slice store", KR(ret));
  } else if (OB_FAIL(column_slice_store_->append_batch(datum_rows))) {
    LOG_WARN("fail to append batch to column slice store", KR(ret));
  }
  return ret;
}

int ObMultiSliceStore::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("multi slice store not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == row_slice_store_ ||  nullptr == column_slice_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected slice store", K(ret), KPC_(row_slice_store), KPC_(column_slice_store));
  } else if (OB_FAIL(row_slice_store_->close())) {
    LOG_WARN("fail to close row slice store", K(ret));
  } else if (OB_FAIL(column_slice_store_->close())) {
    LOG_WARN("fail to close column slice store", K(ret));
  } else {
    LOG_DEBUG("[CS-Replica] Finish close multi slice store", K(ret), KPC(this));
  }
  return ret;
}

int ObMultiSliceStore::fill_column_group(const int64_t cg_idx,
                                         ObCOSliceWriter *writer,
                                         ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMultiSliceStore not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == column_slice_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected column slice store is null", KR(ret), KP(column_slice_store_));
  } else if (OB_FAIL(column_slice_store_->fill_column_group(cg_idx, writer, insert_monitor))) {
    LOG_WARN("fail to fill column group", KR(ret), K(cg_idx), KP(writer));
  }
  return ret;
}

void ObMultiSliceStore::cancel()
{
  if (OB_NOT_NULL(row_slice_store_)) {
    row_slice_store_->cancel();
  }
  if (OB_NOT_NULL(column_slice_store_)) {
    column_slice_store_->cancel();
  }
}

int64_t ObMultiSliceStore::get_row_count() const
{
  return column_slice_store_->get_row_count();
}

int64_t ObMultiSliceStore::get_next_block_start_seq() const
{
  return row_slice_store_->get_next_block_start_seq();
}

ObDatumRowkey ObMultiSliceStore::get_compare_key() const
{
  ObDatumRowkey rowkey;
  if (OB_NOT_NULL(column_slice_store_)) {
    rowkey = column_slice_store_->get_compare_key();
  }
  return rowkey;
}

void ObMultiSliceStore::reset()
{
  if (OB_NOT_NULL(arena_allocator_)) {
    (void) free_memory(*arena_allocator_);
  } else if (nullptr == column_slice_store_ || nullptr == row_slice_store_ || nullptr == cs_replica_schema_) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected status", KPC_(column_slice_store), KPC_(row_slice_store), KPC_(cs_replica_schema));
  }
  column_slice_store_ = nullptr;
  row_slice_store_ = nullptr;
  cs_replica_schema_ = nullptr;
  arena_allocator_ = nullptr;
  is_inited_ = false;
}

void ObMultiSliceStore::free_memory(ObArenaAllocator &allocator)
{
  if (OB_NOT_NULL(column_slice_store_)) {
    column_slice_store_->~ObTabletSliceStore();
    allocator.free(column_slice_store_);
    column_slice_store_ = nullptr;
  }
  if (OB_NOT_NULL(row_slice_store_)) {
    row_slice_store_->~ObMacroBlockSliceStore();
    allocator.free(row_slice_store_);
    row_slice_store_ = nullptr;
  }
  if (OB_NOT_NULL(cs_replica_schema_)) {
    cs_replica_schema_->~ObStorageSchema();
    allocator.free(cs_replica_schema_);
    cs_replica_schema_ = nullptr;
  }
}

bool ObTabletDDLParam::is_valid() const
{
  return is_valid_direct_load(direct_load_type_)
    && ls_id_.is_valid()
    && table_key_.is_valid()
    && start_scn_.is_valid_and_not_min()
    && commit_scn_.is_valid() && commit_scn_ != SCN::max_scn()
    && snapshot_version_ > 0
    && data_format_version_ > 0
    && (is_incremental_direct_load(direct_load_type_) ? trans_id_.is_valid() : !trans_id_.is_valid());
}

ObDirectLoadSliceWriter::ObDirectLoadSliceWriter()
  : is_inited_(false), writer_type_(ObDirectLoadSliceWriterType::WRITER_TYPE_MAX), is_canceled_(false), start_seq_(), slice_idx_(0), merge_slice_idx_(0), tablet_direct_load_mgr_(nullptr),
    slice_store_(nullptr), meta_write_iter_(nullptr), row_iterator_(nullptr),
    allocator_(lib::ObLabel("SliceWriter"), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    lob_allocator_(nullptr), rowkey_lengths_(), row_offset_(-1)
{
}

ObDirectLoadSliceWriter::~ObDirectLoadSliceWriter()
{
  if (nullptr != slice_store_) {
    slice_store_->~ObTabletSliceStore();
    allocator_.free(slice_store_);
    slice_store_ = nullptr;
  }
  if (nullptr != meta_write_iter_) {
    meta_write_iter_->~ObLobMetaWriteIter();
    allocator_.free(meta_write_iter_);
    meta_write_iter_ = nullptr;
  }
  if (nullptr != row_iterator_) {
    row_iterator_->~ObLobMetaRowIterator();
    allocator_.free(row_iterator_);
    row_iterator_ = nullptr;
  }
  if (nullptr != lob_allocator_) {
    lob_allocator_->reset();
    allocator_.free(lob_allocator_);
    lob_allocator_= nullptr;
  }
  allocator_.reset();
  rowkey_lengths_.destroy();
  row_offset_ = -1;
  writer_type_ = ObDirectLoadSliceWriterType::WRITER_TYPE_MAX;
}

//for test
int ObDirectLoadSliceWriter::mock_chunk_store(const int64_t row_cnt)
{
  int ret = OB_SUCCESS;
  if (row_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row cnt", K(ret), K(row_cnt));
  } else {
    ObChunkSliceStore *chunk_slice_store = nullptr;
    if (OB_ISNULL(chunk_slice_store = OB_NEWx(ObChunkSliceStore, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for chunk slice store failed", K(ret));
    } else {
      chunk_slice_store->row_cnt_ = row_cnt;
      slice_store_ = chunk_slice_store;

    }
    if (OB_FAIL(ret) && nullptr != chunk_slice_store) {
      chunk_slice_store->~ObChunkSliceStore();
      allocator_.free(chunk_slice_store);
    }
  }
  return ret;
}

int ObDirectLoadSliceWriter::prepare_vector_slice_store(
    const ObStorageSchema *storage_schema,
    const ObString vec_idx_param,
    const int64_t vec_dim)
{
  int ret = OB_SUCCESS;
  ObVectorIndexBaseSliceStore *vec_idx_slice_store = nullptr;
  if (OB_ISNULL(storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null schema", K(ret), K(*this));
  } else if (schema::is_vec_index_snapshot_data_type(storage_schema->get_index_type()) &&
              OB_ISNULL(vec_idx_slice_store = OB_NEWx(ObVectorIndexSliceStore, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for chunk slice store failed", K(ret));
  } else if (schema::is_local_vec_ivf_centroid_index(storage_schema->get_index_type())
            && OB_ISNULL(vec_idx_slice_store = OB_NEWx(ObIvfCenterSliceStore, &allocator_))) {
    // NOTE(liyao): pq/sq8/flat use same centroid index
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for chunk slice store failed", K(ret));
  } else if (schema::is_vec_ivfsq8_meta_index(storage_schema->get_index_type())
          && OB_ISNULL(vec_idx_slice_store = OB_NEWx(ObIvfSq8MetaSliceStore, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for chunk slice store failed", K(ret));
  } else if (schema::is_vec_ivfpq_pq_centroid_index(storage_schema->get_index_type())
          && OB_ISNULL(vec_idx_slice_store = OB_NEWx(ObIvfPqSliceStore, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for chunk slice store failed", K(ret));
  } else if (OB_ISNULL(vec_idx_slice_store)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index type with sclice store", K(ret), K(storage_schema->get_index_type()));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(vec_idx_slice_store->init(tablet_direct_load_mgr_, vec_idx_param, vec_dim,
                                                tablet_direct_load_mgr_->get_column_info()))) {
    LOG_WARN("init vector index slice store failed", K(ret), KPC(storage_schema));
  } else {
    slice_store_ = vec_idx_slice_store;
  }
  if (OB_FAIL(ret) && nullptr != vec_idx_slice_store) {
    if (schema::is_vec_index_snapshot_data_type(storage_schema->get_index_type())) {
      ObVectorIndexSliceStore *slice_store = nullptr;
      if (OB_NOT_NULL(slice_store = dynamic_cast<ObVectorIndexSliceStore*>(vec_idx_slice_store))) {
        slice_store->~ObVectorIndexSliceStore();
      }
    } else if (schema::is_local_vec_ivf_centroid_index(storage_schema->get_index_type())) {
      ObIvfCenterSliceStore *slice_store = nullptr;
      if (OB_NOT_NULL(slice_store = dynamic_cast<ObIvfCenterSliceStore*>(vec_idx_slice_store))) {
        slice_store->~ObIvfCenterSliceStore();
      }
    } else if (schema::is_vec_ivfsq8_meta_index(storage_schema->get_index_type())) {
      ObIvfSq8MetaSliceStore *slice_store = nullptr;
      if (OB_NOT_NULL(slice_store = dynamic_cast<ObIvfSq8MetaSliceStore*>(vec_idx_slice_store))) {
        slice_store->~ObIvfSq8MetaSliceStore();
      }
    } else if (schema::is_vec_ivfpq_pq_centroid_index(storage_schema->get_index_type())) {
      ObIvfPqSliceStore *slice_store = nullptr;
      if (OB_NOT_NULL(slice_store = dynamic_cast<ObIvfPqSliceStore*>(vec_idx_slice_store))) {
        slice_store->~ObIvfPqSliceStore();
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid index type with sclice store", K(ret), K(storage_schema->get_index_type()));
    }
    allocator_.free(vec_idx_slice_store);
  }
  return ret;
}

int ObDirectLoadSliceWriter::prepare_slice_store_if_need(
    const int64_t schema_rowkey_column_num,
    const bool is_column_store,
    const int64_t dir_id,
    const int64_t parallelism,
    const ObStorageSchema *storage_schema,
    const SCN &start_scn,
    const ObString vec_idx_param,
    const int64_t vec_dim,
    const bool use_batch_store,
    const int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (nullptr != slice_store_) {
    // do nothing
  } else if (is_full_direct_load(tablet_direct_load_mgr_->get_direct_load_type()) &&
             OB_NOT_NULL(storage_schema) &&
             (schema::is_vec_index_snapshot_data_type(storage_schema->get_index_type()) ||
              schema::is_local_vec_ivf_centroid_index(storage_schema->get_index_type()) ||
              schema::is_vec_ivfsq8_meta_index(storage_schema->get_index_type()) ||
              schema::is_vec_ivfpq_pq_centroid_index(storage_schema->get_index_type()))) {
    if (OB_FAIL(prepare_vector_slice_store(storage_schema, vec_idx_param, vec_dim))) {
      LOG_WARN("failed to prepare vector slice_store", K(ret));
    }
  } else if (tablet_direct_load_mgr_->need_process_cs_replica()) {
    writer_type_ = ObDirectLoadSliceWriterType::COL_REPLICA_WRITER;
    ObMultiSliceStore *multi_slice_store = nullptr;
    if (OB_ISNULL(storage_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("null schema", K(ret), K(*this));
    } else if (OB_ISNULL(multi_slice_store = OB_NEWx(ObMultiSliceStore, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for multi slice store failed", K(ret));
    } else if (OB_FAIL(multi_slice_store->init(allocator_,
                                               tablet_direct_load_mgr_,
                                               start_seq_,
                                               slice_idx_,
                                               merge_slice_idx_,
                                               start_scn,
                                               schema_rowkey_column_num + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt(),
                                               storage_schema,
                                               tablet_direct_load_mgr_->get_column_info(),
                                               dir_id,
                                               parallelism,
                                               use_batch_store,
                                               max_batch_size))) {
      LOG_WARN("init multi slice store failed", K(ret), KPC_(tablet_direct_load_mgr), KPC(storage_schema));
    } else {
      slice_store_ = multi_slice_store;
    }
    if (OB_FAIL(ret) && nullptr != multi_slice_store) {
      multi_slice_store->~ObMultiSliceStore();
      allocator_.free(multi_slice_store);
    }
  } else if (is_full_direct_load(tablet_direct_load_mgr_->get_direct_load_type()) && is_column_store) {
    writer_type_ = ObDirectLoadSliceWriterType::COL_STORE_WRITER;
    if (OB_ISNULL(storage_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("null schema", K(ret), K(*this));
    } else {
      if (!ObDDLUtil::need_rescan_column_store(tablet_direct_load_mgr_->get_data_format_version())) {
        if (use_batch_store) {
          ObColumnBatchSliceStore *slice_store = nullptr;
          if (OB_ISNULL(slice_store_ = slice_store = OB_NEWx(ObColumnBatchSliceStore, &allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to new ObColumnBatchSliceStore", KR(ret));
          } else if (OB_FAIL(slice_store->init(
                schema_rowkey_column_num + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt(),
                storage_schema,
                allocator_,
                slice_idx_,
                merge_slice_idx_,
                tablet_direct_load_mgr_,
                start_seq_,
                start_scn,
                dir_id,
                false/*is_cs_replica*/,
                max_batch_size))) {
            LOG_WARN("fail to init column batch slice store", K(ret), KPC(storage_schema), KPC(tablet_direct_load_mgr_), K(start_seq_), K(start_scn));
          }
        } else {
          ObColumnSliceStore *slice_store = nullptr;
          if (OB_ISNULL(slice_store_ = slice_store = OB_NEWx(ObColumnSliceStore, &allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory for chunk slice store failed", K(ret));
          } else if (OB_FAIL(slice_store->init(
                allocator_,
                storage_schema,
                tablet_direct_load_mgr_,
                start_seq_,
                slice_idx_,
                start_scn,
                dir_id,
                false/*is_cs_replica*/))) {
            LOG_WARN("init column slice store failed", K(ret), KPC(storage_schema), KPC(tablet_direct_load_mgr_), K(start_seq_), K(start_scn));
          }
        }
      } else {
        if (use_batch_store) {
          ObChunkBatchSliceStore *slice_store = nullptr;
          if (OB_ISNULL(slice_store_ = slice_store = OB_NEWx(ObChunkBatchSliceStore, &allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to new ObChunkBatchSliceStore", KR(ret));
          } else if (OB_FAIL(slice_store->init(
                schema_rowkey_column_num + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt(),
                storage_schema,
                allocator_,
                tablet_direct_load_mgr_->get_column_info(),
                dir_id,
                parallelism,
                max_batch_size))) {
            LOG_WARN("fail to init chunk batch slice store", K(ret), KPC(storage_schema));
          }
        } else {
          ObChunkSliceStore *slice_store = nullptr;
          if (OB_ISNULL(slice_store_ = slice_store = OB_NEWx(ObChunkSliceStore, &allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to new ObChunkSliceStore", KR(ret));
          } else if (OB_FAIL(slice_store->init(
                schema_rowkey_column_num + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt(),
                storage_schema,
                allocator_,
                tablet_direct_load_mgr_->get_column_info(),
                dir_id,
                parallelism))) {
            LOG_WARN("fail to init chunk slice store", K(ret), KPC(storage_schema));
          }
        }
      }
    }
  } else {
    writer_type_ = ObDirectLoadSliceWriterType::ROW_STORE_WRITER;
    ObMacroBlockSliceStore *macro_block_slice_store = nullptr;
    if (OB_ISNULL(macro_block_slice_store = OB_NEWx(ObMacroBlockSliceStore, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for macro block slice store failed", K(ret));
    } else if (OB_FAIL(macro_block_slice_store->init(tablet_direct_load_mgr_, start_seq_, start_scn))) {
      LOG_WARN("init macro block slice store failed", K(ret), KPC(tablet_direct_load_mgr_), K(start_seq_));
    } else {
      slice_store_ = macro_block_slice_store;
    }
    if (OB_FAIL(ret) && nullptr != macro_block_slice_store) {
      macro_block_slice_store->~ObMacroBlockSliceStore();
      allocator_.free(macro_block_slice_store);
    }
  }
  return ret;
}

int ObDirectLoadSliceWriter::init(
    ObTabletDirectLoadMgr *tablet_direct_load_mgr,
    const blocksstable::ObMacroDataSeq &start_seq,
    const int64_t slice_idx,
    const int64_t merge_slice_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(nullptr == tablet_direct_load_mgr || !start_seq.is_valid() || slice_idx < 0 || merge_slice_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(tablet_direct_load_mgr), K(start_seq), K(slice_idx), K(merge_slice_idx));
  } else {
    tablet_direct_load_mgr_ = tablet_direct_load_mgr;
    start_seq_ = start_seq;
    slice_idx_ = slice_idx;
    merge_slice_idx_ = merge_slice_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadSliceWriter::prepare_iters(
    ObIAllocator &allocator,
    ObIAllocator &iter_allocator,
    blocksstable::ObStorageDatum &datum,
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t trans_version,
    const ObObjType &obj_type,
    const ObCollationType &cs_type,
    const transaction::ObTransID trans_id,
    const int64_t seq_no,
    const int64_t timeout_ts,
    const ObLobStorageParam &lob_storage_param,
    const uint64_t src_tenant_id,
    const ObDirectLoadType direct_load_type,
    transaction::ObTxDesc* tx_desc,
    share::ObTabletCacheInterval &pk_interval,
    ObLobMetaRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  row_iter = nullptr;

  if (OB_ISNULL(lob_allocator_)) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(common::ObArenaAllocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc lob allocator failed", K(ret));
    } else {
      lob_allocator_ = new (buf) common::ObArenaAllocator("LobWriter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    }
  }

  if (OB_SUCC(ret) && OB_ISNULL(meta_write_iter_)) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObLobMetaWriteIter)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc lob meta write iter failed", K(ret));
    } else {
      // keep allocator is same as insert_lob_column
      meta_write_iter_ = new (buf) ObLobMetaWriteIter(lob_allocator_, ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(row_iterator_)) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObLobMetaRowIterator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc lob meta row iter failed", K(ret));
      } else {
        row_iterator_ = new (buf) ObLobMetaRowIterator();
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t unused_affected_rows = 0;
    if (is_incremental_direct_load(direct_load_type) && OB_ISNULL(tx_desc)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tx_desc should not be null if is incremental_direct_load", K(ret), K(direct_load_type),
          K(ls_id), K(tablet_id), K(trans_version), K(seq_no), K(obj_type), K(cs_type), K(trans_id));
    } else if (OB_FAIL(ObInsertLobColumnHelper::insert_lob_column(
        allocator, *lob_allocator_, tx_desc, pk_interval, ls_id, tablet_id/* tablet_id of main table */, tablet_direct_load_mgr_->get_tablet_id()/*tablet id of lob meta table*/,
        obj_type, cs_type, lob_storage_param, datum, timeout_ts, true/*has_lob_header*/, src_tenant_id, *meta_write_iter_))) {
      LOG_WARN("fail to insert_lob_col", K(ret), K(ls_id), K(tablet_id), K(src_tenant_id));
    } else if (OB_FAIL(row_iterator_->init(meta_write_iter_, trans_id,
        trans_version, seq_no, direct_load_type))) {
      LOG_WARN("fail to lob meta row iterator", K(ret), K(trans_id), K(trans_version), K(seq_no), K(direct_load_type));
    } else {
      row_iter = row_iterator_;
    }
  }
  return ret;
}

int ObDirectLoadSliceWriter::fill_lob_sstable_slice(
    const uint64_t table_id,
    ObIAllocator &allocator,
    ObIAllocator &iter_allocator,
    const SCN &start_scn,
    const ObBatchSliceWriteInfo &info,
    share::ObTabletCacheInterval &pk_interval,
    const ObArray<int64_t> &lob_column_idxs,
    const ObArray<common::ObObjMeta> &col_types,
    const ObTableSchemaItem &schema_item,
    blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  const uint64_t data_format_version = tablet_direct_load_mgr_->get_data_format_version();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSliceWriter not init", KR(ret), KP(this));
  } else {
    ObLobStorageParam lob_storage_param;
    lob_storage_param.inrow_threshold_ = schema_item.lob_inrow_threshold_;
    lob_storage_param.is_index_table_ = schema_item.is_index_table_;
    for (int64_t i = 0; OB_SUCC(ret) && i < lob_column_idxs.count(); i++) {
      const int64_t idx = lob_column_idxs.at(i);
      const ObObjMeta &col_type = col_types.at(i);
      ObStorageDatum &datum = datum_row.storage_datums_[idx];
      lob_storage_param.is_rowkey_col_ = idx < schema_item.rowkey_column_num_;
      if (DATA_VERSION_4_3_0_0 > data_format_version) {
        if (OB_FAIL(fill_lob_into_memtable(allocator, info, col_type, lob_storage_param, datum))) {
          LOG_WARN("fill lob into memtable failed", K(ret), K(data_format_version));
        }
      } else {
        if (OB_FAIL(fill_lob_into_macro_block(allocator, iter_allocator, start_scn, info,
            pk_interval, col_type, lob_storage_param, datum))) {
          LOG_WARN("fill lob into macro block failed", K(ret), K(data_format_version));
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadSliceWriter::fill_lob_into_memtable(
    ObIAllocator &allocator,
    const ObBatchSliceWriteInfo &info,
    const common::ObObjMeta &col_type,
    const ObLobStorageParam &lob_storage_param,
    blocksstable::ObStorageDatum &datum)
{
  // to insert lob data into memtable.
  int ret = OB_SUCCESS;
  const int64_t timeout_ts = ObTimeUtility::fast_current_time() + ObInsertLobColumnHelper::LOB_ACCESS_TX_TIMEOUT;
  if (OB_FAIL(ObInsertLobColumnHelper::insert_lob_column(
    allocator, info.ls_id_, info.data_tablet_id_, col_type.get_type(), col_type.get_collation_type(),
    lob_storage_param, datum, timeout_ts, true/*has_lob_header*/, info.src_tenant_id_))) {
    LOG_WARN("fail to insert_lob_col", K(ret), K(datum));
  }
  return ret;
}

int ObDirectLoadSliceWriter::fill_lob_into_macro_block(
    ObIAllocator &allocator,
    ObIAllocator &iter_allocator,
    const SCN &start_scn,
    const ObBatchSliceWriteInfo &info,
    share::ObTabletCacheInterval &pk_interval,
    const common::ObObjMeta &col_type,
    const ObLobStorageParam &lob_storage_param,
    blocksstable::ObStorageDatum &datum)
{
  // to insert lob data into macro block.
  int ret = OB_SUCCESS;
  int64_t unused_affected_rows = 0;
  const int64_t timeout_ts = ObTimeUtility::fast_current_time() + ObInsertLobColumnHelper::LOB_ACCESS_TX_TIMEOUT;
  if (!datum.is_nop() && !datum.is_null()) {
    {
      ObLobMetaRowIterator *row_iter = nullptr;
      if (OB_FAIL(prepare_iters(allocator, iter_allocator, datum, info.ls_id_,
          info.data_tablet_id_, info.trans_version_, col_type.get_type(), col_type.get_collation_type(),
          info.trans_id_, info.seq_no_, timeout_ts, lob_storage_param, info.src_tenant_id_, info.direct_load_type_,
          info.tx_desc_, pk_interval, row_iter))) {
        LOG_WARN("fail to prepare iters", K(ret), KP(row_iter), K(datum));
      } else {
        while (OB_SUCC(ret)) {
          const blocksstable::ObDatumRow *cur_row = nullptr;
          if (OB_FAIL(THIS_WORKER.check_status())) {
            LOG_WARN("check status failed", K(ret));
          } else if (ATOMIC_LOAD(&is_canceled_)) {
            ret = OB_CANCELED;
            LOG_WARN("fil lob task canceled", K(ret), K(is_canceled_));
          } else if (OB_FAIL(row_iter->get_next_row(cur_row))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("get next row failed", K(ret));
            }
          } else if (OB_ISNULL(cur_row) || !cur_row->is_valid()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid args", KR(ret), KPC(cur_row));
          } else if (OB_FAIL(check_null_and_length(false/*is_index_table*/, false/*has_lob_rowkey*/,
                                                   ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT, *cur_row))) {
            LOG_WARN("fail to check rowkey null value and length in row", KR(ret), KPC(cur_row));
          } else if (OB_FAIL(prepare_slice_store_if_need(ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT,
              false/*is_column_store*/, 1L/*unsued*/, 1L/*unused*/, nullptr /*storage_schema*/, start_scn,
              ObString()/*unsued*/, 0/*unsued*/))) {
            LOG_WARN("prepare macro block writer failed", K(ret));
          } else if (OB_FAIL(slice_store_->append_row(*cur_row))) {
            LOG_WARN("macro block writer append row failed", K(ret), KPC(cur_row));
          }
          if (OB_SUCC(ret)) {
            ++unused_affected_rows;
            LOG_DEBUG("sstable insert op append row", K(unused_affected_rows), KPC(cur_row));
          }
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(meta_write_iter_) && OB_FAIL(meta_write_iter_->check_write_length())) {
          LOG_WARN("check_write_length fail", K(ret), KPC(meta_write_iter_));
        }
        if (OB_SUCC(ret)) {
          if (OB_NOT_NULL(meta_write_iter_)) {
            meta_write_iter_->reuse();
          }
          if (OB_NOT_NULL(row_iterator_)) {
            row_iterator_->reuse();
          }
          if (OB_NOT_NULL(lob_allocator_)) {
            lob_allocator_->reuse();
          }
        }
      }
    }
  }
  return ret;
}

static bool fast_check_vector_is_all_null(ObIVector *vector, const int64_t batch_size)
{
  bool is_all_null = false;
  VectorFormat format = vector->get_format();
  switch (format) {
    case VEC_FIXED:
    case VEC_DISCRETE:
    case VEC_CONTINUOUS:
      is_all_null = static_cast<ObBitmapNullVectorBase *>(vector)->get_nulls()->is_all_true(batch_size);
      break;
    default:
      break;
  }
  return is_all_null;
}

static int new_discrete_vector(VecValueTypeClass value_tc,
                               const int64_t max_batch_size,
                               ObIAllocator &allocator,
                               ObDiscreteBase *&result_vec)
{
  int ret = OB_SUCCESS;
  result_vec = nullptr;
  ObIVector *vector = nullptr;
  switch (value_tc) {
#define DISCRETE_VECTOR_INIT_SWITCH(value_tc)                           \
  case value_tc: {                                                      \
    using VecType = RTVectorType<VEC_DISCRETE, value_tc>;               \
    static_assert(sizeof(VecType) <= ObIVector::MAX_VECTOR_STRUCT_SIZE, \
                  "vector size exceeds MAX_VECTOR_STRUCT_SIZE");        \
    vector = OB_NEWx(VecType, &allocator, nullptr, nullptr, nullptr);   \
    break;                                                              \
  }
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_NUMBER);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_EXTEND);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_STRING);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET_INNER);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_RAW);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_ROWID);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_LOB);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_JSON);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_GEO);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_UDT);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_COLLECTION);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_ROARINGBITMAP);
#undef DISCRETE_VECTOR_INIT_SWITCH
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected discrete vector value type class", KR(ret), K(value_tc));
      break;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(vector)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc vecttor", KR(ret));
  } else {
    ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vector);
    const int64_t nulls_size = ObBitVector::memory_size(max_batch_size);
    const int64_t lens_size = sizeof(int32_t) * max_batch_size;
    const int64_t ptrs_size = sizeof(char *) * max_batch_size;
    ObBitVector *nulls = nullptr;
    int32_t *lens = nullptr;
    char **ptrs = nullptr;
    if (OB_ISNULL(nulls = to_bit_vector(allocator.alloc(nulls_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", KR(ret), K(nulls_size));
    } else if (OB_ISNULL(lens = static_cast<int32_t *>(allocator.alloc(lens_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", KR(ret), K(lens_size));
    } else if (OB_ISNULL(ptrs = static_cast<char **>(allocator.alloc(ptrs_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", KR(ret), K(ptrs_size));
    } else {
      nulls->reset(max_batch_size);
      discrete_vec->set_nulls(nulls);
      discrete_vec->set_lens(lens);
      discrete_vec->set_ptrs(ptrs);
      result_vec = discrete_vec;
    }
  }
  return ret;
}

int ObDirectLoadSliceWriter::fill_lob_sstable_slice(
    const uint64_t table_id,
    ObIAllocator &allocator,
    ObIAllocator &iter_allocator,
    const SCN &start_scn,
    const ObBatchSliceWriteInfo &info,
    share::ObTabletCacheInterval &pk_interval,
    const ObArray<int64_t> &lob_column_idxs,
    const ObArray<common::ObObjMeta> &col_types,
    const ObTableSchemaItem &schema_item,
    blocksstable::ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  const uint64_t data_format_version = tablet_direct_load_mgr_->get_data_format_version();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSliceWriter not init", KR(ret), KP(this));
  } else {
    ObStorageDatum temp_datum;
    ObLobStorageParam lob_storage_param;
    lob_storage_param.inrow_threshold_ = schema_item.lob_inrow_threshold_;
    lob_storage_param.is_index_table_ = schema_item.is_index_table_;
    for (int64_t i = 0; OB_SUCC(ret) && i < lob_column_idxs.count(); i++) {
      const int64_t idx = lob_column_idxs.at(i);
      const ObObjMeta &col_type = col_types.at(i);
      ObIVector *vector = datum_rows.vectors_.at(idx);
      const VectorFormat format = vector->get_format();
      lob_storage_param.is_rowkey_col_ = idx < schema_item.rowkey_column_num_;
      if (fast_check_vector_is_all_null(vector, datum_rows.row_count_)) {
        // do nothing
        continue;
      }
      switch (format) {
        case VEC_CONTINUOUS:
        {
          ObContinuousBase *continuous_vec = static_cast<ObContinuousBase *>(vector);
          ObDiscreteBase *discrete_vec = nullptr;
          char *data = continuous_vec->get_data();
          uint32_t *offsets = continuous_vec->get_offsets();
          char **ptrs = nullptr;
          ObLength *lens = nullptr;
          VecValueTypeClass value_tc = get_vec_value_tc(col_type.get_type(),
                                                        col_type.get_scale(),
                                                        PRECISION_UNKNOWN_YET);
          if (OB_FAIL(new_discrete_vector(value_tc, datum_rows.row_count_, allocator, discrete_vec))) {
            LOG_WARN("fail to new discrete vector", KR(ret));
          } else {
            ptrs = discrete_vec->get_ptrs();
            lens = discrete_vec->get_lens();
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < datum_rows.row_count_; ++j) {
            if (continuous_vec->is_null(j)) {
              discrete_vec->set_null(j);
            } else {
              temp_datum.ptr_ = data + offsets[j];
              temp_datum.len_ = offsets[j + 1] - offsets[j];
              if (DATA_VERSION_4_3_0_0 > data_format_version) {
                if (OB_FAIL(fill_lob_into_memtable(allocator, info, col_type, lob_storage_param, temp_datum))) {
                  LOG_WARN("fill lob into memtable failed", K(ret), K(data_format_version));
                }
              } else {
                if (OB_FAIL(fill_lob_into_macro_block(allocator, iter_allocator, start_scn, info,
                    pk_interval, col_type, lob_storage_param, temp_datum))) {
                  LOG_WARN("fill lob into macro block failed", K(ret), K(data_format_version));
                }
              }
              if (OB_SUCC(ret)) {
                ptrs[j] = const_cast<char *>(temp_datum.ptr_);
                lens[j] = temp_datum.len_;
              }
            }
          }
          if (OB_SUCC(ret)) {
            datum_rows.vectors_.at(idx) = discrete_vec;
          }
          break;
        }
        case VEC_DISCRETE:
        {
          ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vector);
          char **ptrs = discrete_vec->get_ptrs();
          ObLength *lens =discrete_vec->get_lens();
          for (int64_t j = 0; OB_SUCC(ret) && j < datum_rows.row_count_; ++j) {
            if (!discrete_vec->is_null(j)) {
              temp_datum.ptr_ = ptrs[j];
              temp_datum.len_ = lens[j];
              if (DATA_VERSION_4_3_0_0 > data_format_version) {
                if (OB_FAIL(fill_lob_into_memtable(allocator, info, col_type, lob_storage_param, temp_datum))) {
                  LOG_WARN("fill lob into memtable failed", K(ret), K(data_format_version));
                }
              } else {
                if (OB_FAIL(fill_lob_into_macro_block(allocator, iter_allocator, start_scn, info,
                    pk_interval, col_type, lob_storage_param, temp_datum))) {
                  LOG_WARN("fill lob into macro block failed", K(ret), K(data_format_version));
                }
              }
              if (OB_SUCC(ret)) {
                ptrs[j] = const_cast<char *>(temp_datum.ptr_);
                lens[j] = temp_datum.len_;
              }
            }
          }
          break;
        }
        case VEC_UNIFORM:
        {
          ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
          ObDatum *datums = uniform_vec->get_datums();
          for (int64_t j = 0; OB_SUCC(ret) && j < datum_rows.row_count_; ++j) {
            ObDatum &datum = datums[j];
            if (!datum.is_null()) {
              temp_datum.ptr_ = datum.ptr_;
              temp_datum.len_ = datum.len_;
              if (DATA_VERSION_4_3_0_0 > data_format_version) {
                if (OB_FAIL(fill_lob_into_memtable(allocator, info, col_type, lob_storage_param, temp_datum))) {
                  LOG_WARN("fill lob into memtable failed", K(ret), K(data_format_version));
                }
              } else {
                if (OB_FAIL(fill_lob_into_macro_block(allocator, iter_allocator, start_scn, info,
                    pk_interval, col_type, lob_storage_param, temp_datum))) {
                  LOG_WARN("fill lob into macro block failed", K(ret), K(data_format_version));
                }
              }
              if (OB_SUCC(ret)) {
                datum.ptr_ = temp_datum.ptr_;
                datum.len_ = temp_datum.len_;
              }
            }
          }
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected lob vector format", KR(ret), K(i), K(format));
          break;
      }
    }
  }
  return ret;
}

int ObDirectLoadSliceWriter::fill_lob_meta_sstable_slice(
    const share::SCN &start_scn,
    const uint64_t table_id,
    const ObTabletID &curr_tablet_id,
    ObIStoreRowIterator *row_iter,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSliceWriter not init", KR(ret), KP(this));
  } else {
    const int64_t rowkey_column_count = ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT;
    const int64_t column_count = ObLobMetaUtil::LOB_META_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    while (OB_SUCC(ret)) {
      const blocksstable::ObDatumRow *cur_row = nullptr;
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (ATOMIC_LOAD(&is_canceled_)) {
        ret = OB_CANCELED;
        LOG_WARN("fil sstable task canceled", K(ret), K(is_canceled_));
      } else if (OB_FAIL(row_iter->get_next_row(cur_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("get next row failed", K(ret));
        }
      } else if (OB_ISNULL(cur_row) || !cur_row->is_valid() || cur_row->get_column_count() != column_count) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid args", KR(ret), KPC(cur_row), K(column_count));
      } else if (OB_FAIL(check_null_and_length(false/*is_index_table*/, false/*has_lob_rowkey*/,
                                               rowkey_column_count, *cur_row))) {
        LOG_WARN("fail to check rowkey null value and length in row", KR(ret), KPC(cur_row));
      } else if (OB_FAIL(prepare_slice_store_if_need(rowkey_column_count,
                                                     false/*is_column_store*/,
                                                     1L/*unsued*/,
                                                     1L/*unused*/,
                                                     nullptr /*storage_schema*/,
                                                     start_scn,
                                                     ObString()/*unsued*/,
                                                     0/*unsued*/))) {
        LOG_WARN("prepare macro block writer failed", K(ret));
      } else if (OB_FAIL(slice_store_->append_row(*cur_row))) {
        LOG_WARN("macro block writer append row failed", K(ret), KPC(cur_row));
      }
      if (OB_SUCC(ret)) {
        ++affected_rows;
        LOG_DEBUG("sstable insert op append row", K(affected_rows), KPC(cur_row));
      }
    }
  }
  return ret;
}

int ObDirectLoadSliceWriter::fill_sstable_slice(
    const SCN &start_scn,
    const uint64_t table_id,
    const ObTabletID &tablet_id,
    const ObStorageSchema *storage_schema,
    ObIStoreRowIterator *row_iter,
    const ObTableSchemaItem &schema_item,
    const ObDirectLoadType &direct_load_type,
    const ObArray<ObColumnSchemaItem> &column_items,
    const int64_t dir_id,
    const int64_t parallelism,
    int64_t &affected_rows,
    ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  const bool is_full_direct_load_task = is_full_direct_load(direct_load_type);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSliceWriter not init", KR(ret), KP(this));
  } else if (OB_ISNULL(storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null schema", K(ret), K(*this));
  } else {
    ObArenaAllocator arena("SliceW_sst", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const ObDataStoreDesc &data_desc = tablet_direct_load_mgr_->get_sqc_build_ctx().data_block_desc_.get_desc();

    while (OB_SUCC(ret)) {
      arena.reuse();
      const blocksstable::ObDatumRow *cur_row = nullptr;
      if (OB_FAIL(share::dag_yield())) {
        LOG_WARN("dag yield failed", K(ret), K(affected_rows)); // exit for dag task as soon as possible after canceled.
      } else if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (ATOMIC_LOAD(&is_canceled_)) {
        ret = OB_CANCELED;
        LOG_WARN("fil sstable task canceled", K(ret), K(is_canceled_));
      } else if (OB_FAIL(row_iter->get_next_row(cur_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("get next row failed", K(ret));
        }
      } else if (OB_ISNULL(cur_row) || !cur_row->is_valid() || cur_row->get_column_count() != data_desc.get_col_desc_array().count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid args", KR(ret), KPC(cur_row), K(data_desc.get_col_desc_array()));
      } else { // row reshape
        for (int64_t i = 0; OB_SUCC(ret) && i < cur_row->get_column_count(); ++i) {
          const ObColDesc &col_desc = data_desc.get_col_desc_array().at(i);
          ObStorageDatum &datum_cell = cur_row->storage_datums_[i];
          if (i >= schema_item.rowkey_column_num_ && i < schema_item.rowkey_column_num_ + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()) {
            // skip multi version column
          } else if (datum_cell.is_null()) {
            //ignore null
          } else if (OB_UNLIKELY(i >= column_items.count()) || OB_UNLIKELY(!column_items.at(i).is_valid_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column schema is wrong", K(ret), K(i), K(column_items));
          } else if (OB_FAIL(ObDASUtils::reshape_datum_value(column_items.at(i).col_type_, column_items.at(i).col_accuracy_, true/*enable_oracle_empty_char_reshape_to_null*/, arena, datum_cell))) {
            LOG_WARN("reshape storage datum failed", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(check_null_and_length(schema_item.is_index_table_, schema_item.has_lob_rowkey_,
                                               schema_item.rowkey_column_num_, *cur_row))) {
        LOG_WARN("fail to check rowkey null value and length in row", KR(ret), KPC(cur_row));
      } else if (OB_FAIL(prepare_slice_store_if_need(schema_item.rowkey_column_num_,
                                                     schema_item.is_column_store_,
                                                     dir_id,
                                                     parallelism,
                                                     storage_schema,
                                                     start_scn,
                                                     schema_item.vec_idx_param_,
                                                     schema_item.vec_dim_))) {
        LOG_WARN("prepare macro block writer failed", K(ret));
      } else if (OB_FAIL(slice_store_->append_row(*cur_row))) {
        if (is_full_direct_load_task && OB_ERR_PRIMARY_KEY_DUPLICATE == ret && schema_item.is_unique_index_) {
          int report_ret_code = OB_SUCCESS;
          LOG_USER_ERROR(OB_ERR_PRIMARY_KEY_DUPLICATE, "", static_cast<int>(sizeof("UNIQUE IDX") - 1), "UNIQUE IDX");
          (void) report_unique_key_dumplicated(ret, table_id, *cur_row, tablet_direct_load_mgr_->get_tablet_id(), report_ret_code); // ignore ret
          if (OB_ERR_DUPLICATED_UNIQUE_KEY == report_ret_code) {
            //error message of OB_ERR_PRIMARY_KEY_DUPLICATE is not compatiable with oracle, so use a new error code
            ret = OB_ERR_DUPLICATED_UNIQUE_KEY;
          }
        } else {
          LOG_WARN("macro block writer append row failed", K(ret), KPC(cur_row));
        }
      }
      if (OB_SUCC(ret)) {
        ++affected_rows;
        LOG_DEBUG("sstable insert op append row", KPC(cur_row));
        if ((affected_rows % 100 == 0) && OB_NOT_NULL(insert_monitor)) {
          (void) ATOMIC_AAF(&insert_monitor->scanned_row_cnt_, 100);
          (void) ATOMIC_AAF(&insert_monitor->inserted_row_cnt_, 100);
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(insert_monitor)) {
      (void) ATOMIC_AAF(&insert_monitor->scanned_row_cnt_, affected_rows % 100);
      (void) ATOMIC_AAF(&insert_monitor->inserted_row_cnt_, affected_rows % 100);
    }
  }
  return ret;
}

int ObDirectLoadSliceWriter::fill_sstable_slice(
    const SCN &start_scn,
    const uint64_t table_id,
    const ObTabletID &tablet_id,
    const ObStorageSchema *storage_schema,
    const blocksstable::ObBatchDatumRows &datum_rows,
    const ObTableSchemaItem &schema_item,
    const ObDirectLoadType &direct_load_type,
    const ObArray<ObColumnSchemaItem> &column_items,
    const int64_t dir_id,
    const int64_t parallelism,
    ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  const bool is_full_direct_load_task = is_full_direct_load(direct_load_type);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSliceWriter not init", KR(ret), KP(this));
  } else if (OB_ISNULL(storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null schema", K(ret), K(*this));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_canceled_))) {
    ret = OB_CANCELED;
    LOG_WARN("fil sstable task canceled", K(ret), K(is_canceled_));
  } else {
    ObArenaAllocator arena("SliceW_sst", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const ObDataStoreDesc &data_desc = tablet_direct_load_mgr_->get_sqc_build_ctx().data_block_desc_.get_desc();
    const int64_t max_batch_size = tablet_direct_load_mgr_->get_sqc_build_ctx().build_param_.runtime_only_param_.max_batch_size_;
    if (OB_UNLIKELY(datum_rows.get_column_count() != data_desc.get_col_desc_array().count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", KR(ret), K(datum_rows.get_column_count()), K(data_desc.get_col_desc_array()));
    } else { // row reshape
      for (int64_t i = 0; OB_SUCC(ret) && i < datum_rows.get_column_count(); ++i) {
        const ObColDesc &col_desc = data_desc.get_col_desc_array().at(i);
        ObIVector *vector = datum_rows.vectors_.at(i);
        if (i >= schema_item.rowkey_column_num_ && i < schema_item.rowkey_column_num_ + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()) {
          // skip multi version column
        } else if (OB_UNLIKELY(i >= column_items.count()) || OB_UNLIKELY(!column_items.at(i).is_valid_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column schema is wrong", K(ret), K(i), K(column_items));
        } else if (OB_FAIL(ObDASUtils::reshape_vector_value(column_items.at(i).col_type_,
                                                            column_items.at(i).col_accuracy_,
                                                            arena,
                                                            vector,
                                                            datum_rows.row_count_))) {
          LOG_WARN("fail to reshape vector value", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_null_and_length(schema_item.is_index_table_, schema_item.has_lob_rowkey_,
                                             schema_item.rowkey_column_num_, datum_rows))) {
      LOG_WARN("fail to check rowkey null value and length in row", KR(ret));
    } else if (OB_FAIL(prepare_slice_store_if_need(schema_item.rowkey_column_num_,
                                                   schema_item.is_column_store_,
                                                   dir_id,
                                                   parallelism,
                                                   storage_schema,
                                                   start_scn,
                                                   schema_item.vec_idx_param_,
                                                   schema_item.vec_dim_,
                                                   true/*use_batch_store*/,
                                                   max_batch_size))) {
      LOG_WARN("prepare macro block writer failed", K(ret));
    } else if (OB_FAIL(slice_store_->append_batch(datum_rows))) {
      if (is_full_direct_load_task && OB_ERR_PRIMARY_KEY_DUPLICATE == ret && schema_item.is_unique_index_) {
        int report_ret_code = OB_SUCCESS;
        LOG_USER_ERROR(OB_ERR_PRIMARY_KEY_DUPLICATE, "", static_cast<int>(sizeof("UNIQUE IDX") - 1), "UNIQUE IDX");
        (void) report_unique_key_dumplicated(ret, table_id, datum_rows, tablet_direct_load_mgr_->get_tablet_id(), report_ret_code); // ignore ret
        if (OB_ERR_DUPLICATED_UNIQUE_KEY == report_ret_code) {
          //error message of OB_ERR_PRIMARY_KEY_DUPLICATE is not compatiable with oracle, so use a new error code
          ret = OB_ERR_DUPLICATED_UNIQUE_KEY;
        }
      } else {
        LOG_WARN("macro block writer append batch failed", K(ret));
      }
    } else {
      LOG_DEBUG("sstable insert op append batch", K(datum_rows.row_count_));
      if (OB_NOT_NULL(insert_monitor)) {
        (void) ATOMIC_AAF(&insert_monitor->scanned_row_cnt_, datum_rows.row_count_);
        (void) ATOMIC_AAF(&insert_monitor->inserted_row_cnt_, datum_rows.row_count_);
      }
    }
  }
  return ret;
}

int ObDirectLoadSliceWriter::report_unique_key_dumplicated(
    const int ret_code, const uint64_t table_id, const ObDatumRow &datum_row,
    const ObTabletID &tablet_id, int &report_ret_code)
{
  int ret = OB_SUCCESS;
  report_ret_code = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
          MTL_ID(), schema_guard))) {
    LOG_WARN("get tenant schema failed", K(ret), K(table_id), K(MTL_ID()), K(table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(),
          table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(MTL_ID()), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(MTL_ID()), K(table_id));
  } else {
    const int64_t rowkey_column_num = table_schema->get_rowkey_column_num();
    char index_key_buffer[OB_TMP_BUF_SIZE_256] = { 0 };
    int64_t task_id = 0;
    ObDatumRowkey index_key;
    ObDDLErrorMessageTableOperator::ObDDLErrorInfo error_info;
    index_key.assign(datum_row.storage_datums_, rowkey_column_num);
    if (OB_FAIL(ObDDLErrorMessageTableOperator::extract_index_key(*table_schema, index_key, index_key_buffer, OB_TMP_BUF_SIZE_256))) {   // read the unique key that violates the unique constraint
      LOG_WARN("extract unique index key failed", K(ret), K(index_key), K(index_key_buffer));
    } else if (OB_FAIL(ObDDLErrorMessageTableOperator::get_index_task_info(*GCTX.sql_proxy_, *table_schema, error_info))) {
      LOG_WARN("get task id of index table failed", K(ret), K(task_id), K(table_schema));
    } else if (OB_FAIL(ObDDLErrorMessageTableOperator::generate_index_ddl_error_message(ret_code, *table_schema, ObCurTraceId::get_trace_id_str(),
            error_info.task_id_, error_info.parent_task_id_, tablet_id.id(), GCTX.self_addr(), *GCTX.sql_proxy_, index_key_buffer, report_ret_code))) {
      LOG_WARN("generate index ddl error message", K(ret), K(ret), K(report_ret_code));
    }
  }
  return ret;
}

int ObDirectLoadSliceWriter::report_unique_key_dumplicated(
    const int ret_code,
    const uint64_t table_id,
    const ObBatchDatumRows &datum_rows,
    const ObTabletID &tablet_id,
    int &report_ret_code)
{
  int ret = OB_SUCCESS;
  report_ret_code = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
          MTL_ID(), schema_guard))) {
    LOG_WARN("get tenant schema failed", K(ret), K(table_id), K(MTL_ID()), K(table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(),
          table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(MTL_ID()), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(MTL_ID()), K(table_id));
  } else {
    const int64_t rowkey_column_num = table_schema->get_rowkey_column_num();
    ObMemAttr mem_attr(MTL_ID(), "DL_Temp");
    ObArenaAllocator allocator(mem_attr);
    ObArray<ObColDesc> col_descs;
    blocksstable::ObStorageDatumUtils datum_utils;
    char index_key_buffer[OB_TMP_BUF_SIZE_256] = { 0 };
    int64_t task_id = 0;
    ObDatumRowkey key1, key2;
    ObDatumRowkey *index_key = nullptr, *prev_key = nullptr, *next_key = nullptr;
    ObDDLErrorMessageTableOperator::ObDDLErrorInfo error_info;
    col_descs.set_block_allocator(ModulePageAllocator(allocator));

    if (OB_FAIL(table_schema->get_rowkey_column_ids(col_descs))) {
      LOG_WARN("fail to get rowkey column ids", KR(ret));
    } else if (OB_FAIL(datum_utils.init(col_descs,
                                        rowkey_column_num,
                                        lib::is_oracle_mode(),
                                        allocator,
                                        true/*no need compare multiple version cols*/))) {
      LOG_WARN("fail to init datum utils", KR(ret), K(col_descs), K(rowkey_column_num));
    }

    // init keys for compare
    if (OB_SUCC(ret)) {
      const int64_t datum_cnt = rowkey_column_num * 2;
      ObStorageDatum *datums = nullptr;
      if (OB_ISNULL(datums = static_cast<ObStorageDatum*>(allocator.alloc(sizeof(ObStorageDatum) * datum_cnt)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc datums", KR(ret), K(datum_cnt));
      } else {
        key1.assign(datums, rowkey_column_num);
        key2.assign(datums + rowkey_column_num, rowkey_column_num);
        prev_key = &key1;
        next_key = &key2;
      }
    }
    // find dumplicated key
    if (OB_SUCC(ret)) {
      bool is_null = false;
      const char *payload = nullptr;
      ObLength length = 0;
      ObStorageDatum tmp_datum;
      int cmp_ret = 0;
      bool find_dumplicated_key = false;
      // init prev key
      for (int64_t j = 1; OB_SUCC(ret) && j < rowkey_column_num; ++j) {
        ObIVector *vector = datum_rows.vectors_.at(j);
        vector->get_payload(0, is_null, payload, length);
        prev_key->datums_[j].shallow_copy_from_datum(ObDatum(payload, length, is_null));
      }
      for (int64_t i = 1; OB_SUCC(ret) && !find_dumplicated_key && i < datum_rows.row_count_; ++i) {
        // set next key
        for (int64_t j = 1; OB_SUCC(ret) && j < rowkey_column_num; ++j) {
          ObIVector *vector = datum_rows.vectors_.at(j);
          vector->get_payload(i, is_null, payload, length);
          next_key->datums_[j].shallow_copy_from_datum(ObDatum(payload, length, is_null));
        }
        // compare key
        if (OB_FAIL(next_key->compare(*prev_key, datum_utils, cmp_ret))) {
          LOG_WARN("fail to compare rowkey", KR(ret), KPC(prev_key), KPC(next_key));
        } else if (0 == cmp_ret) {
          find_dumplicated_key = true;
        } else {
          std::swap(prev_key, next_key);
        }
      }
      if (OB_SUCC(ret)) {
        index_key = prev_key;
        if (!find_dumplicated_key) {
          // first key is dumplicated key
          for (int64_t j = 1; OB_SUCC(ret) && j < rowkey_column_num; ++j) {
            ObIVector *vector = datum_rows.vectors_.at(j);
            vector->get_payload(0, is_null, payload, length);
            index_key->datums_[j].shallow_copy_from_datum(ObDatum(payload, length, is_null));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDDLErrorMessageTableOperator::extract_index_key(*table_schema, *index_key, index_key_buffer, OB_TMP_BUF_SIZE_256))) {   // read the unique key that violates the unique constraint
      LOG_WARN("extract unique index key failed", K(ret), KPC(index_key), K(index_key_buffer));
    } else if (OB_FAIL(ObDDLErrorMessageTableOperator::get_index_task_info(*GCTX.sql_proxy_, *table_schema, error_info))) {
      LOG_WARN("get task id of index table failed", K(ret), K(task_id), K(table_schema));
    } else if (OB_FAIL(ObDDLErrorMessageTableOperator::generate_index_ddl_error_message(ret_code, *table_schema, ObCurTraceId::get_trace_id_str(),
            error_info.task_id_, error_info.parent_task_id_, tablet_id.id(), GCTX.self_addr(), *GCTX.sql_proxy_, index_key_buffer, report_ret_code))) {
      LOG_WARN("generate index ddl error message", K(ret), K(ret), K(report_ret_code));
    }
  }
  return ret;
}

int ObDirectLoadSliceWriter::check_null_and_length(
    const bool is_index_table,
    const bool has_lob_rowkey,
    const int64_t rowkey_column_num,
    const ObDatumRow &row_val) const
{
  int ret = OB_SUCCESS;
  if (is_index_table && !has_lob_rowkey) {
    // index table is index-organized but can have null values in index column
  } else if (OB_UNLIKELY(rowkey_column_num > row_val.get_column_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey column number", KR(ret), K(rowkey_column_num), K(row_val));
  } else {
    int64_t rowkey_length = 0;
    bool has_null = false;
    for (int64_t i = 0; i < rowkey_column_num; i++) {
      const ObStorageDatum &cell = row_val.storage_datums_[i];
      rowkey_length += cell.len_;
      has_null |= cell.is_null();
    }
    if (!is_index_table && has_null) {
      ret = OB_ER_INVALID_USE_OF_NULL;
      LOG_WARN("invalid null cell for row key column", KR(ret), K(row_val));
    }
    if (OB_SUCC(ret) && has_lob_rowkey && rowkey_length > OB_MAX_VARCHAR_LENGTH_KEY) {
      ret = OB_ERR_TOO_LONG_KEY_LENGTH;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, OB_MAX_VARCHAR_LENGTH_KEY);
      STORAGE_LOG(WARN, "rowkey is too long", K(ret), K(rowkey_length), K(rowkey_column_num), K(row_val));
    }
  }
  return ret;
}

int ObDirectLoadSliceWriter::check_null_and_length(
    const bool is_index_table,
    const bool has_lob_rowkey,
    const int64_t rowkey_column_num,
    const ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (is_index_table && !has_lob_rowkey) {
    // index table is index-organized but can have null values in index column
  } else if (OB_UNLIKELY(rowkey_column_num > datum_rows.get_column_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey column number", KR(ret), K(rowkey_column_num), K(datum_rows.get_column_count()));
  } else {
    rowkey_lengths_.reuse();
    bool has_null = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_num; ++i) {
      ObIVector *vector = datum_rows.vectors_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < datum_rows.row_count_; ++j) {
        const int64_t col_length = vector->get_length(j);
        has_null |= vector->is_null(j);
        if (i == 0) {
          if (OB_FAIL(rowkey_lengths_.push_back(col_length))) {
            LOG_WARN("fail to push back column length", K(ret), K(col_length));
          }
        } else {
          rowkey_lengths_[j] += col_length;
        }
      }
    }
    if (OB_SUCC(ret) && !is_index_table && has_null) {
      ret = OB_ER_INVALID_USE_OF_NULL;
      LOG_WARN("invalid null cell for row key column", KR(ret), K(datum_rows));
    }
    for (int64_t i = 0; OB_SUCC(ret) && has_lob_rowkey && i < datum_rows.row_count_; ++i) {
      if (rowkey_lengths_.at(i) > OB_MAX_VARCHAR_LENGTH_KEY) {
        ret = OB_ERR_TOO_LONG_KEY_LENGTH;
        LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, OB_MAX_VARCHAR_LENGTH_KEY);
        STORAGE_LOG(WARN, "rowkey is too long", K(ret), K(i), K(rowkey_lengths_.at(i)));
      }
    }
  }
  return ret;
}

int ObDirectLoadSliceWriter::fill_aggregated_column_group(
    const int64_t cg_idx,
    ObCOSliceWriter *cur_writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (nullptr == slice_store_ || is_empty()) {
    // do nothing
    LOG_INFO("chunk slice store is null or empty", K(ret),
        KPC(slice_store_), KPC(tablet_direct_load_mgr_));
  } else if (ATOMIC_LOAD(&is_canceled_)) {
    ret = OB_CANCELED;
    LOG_WARN("fil cg task canceled", K(ret), K(is_canceled_));
  } else if (OB_FAIL(slice_store_->fill_column_group(cg_idx, cur_writer, nullptr/*insert_monitor*/))) {
    LOG_WARN("fail to fill column group", KR(ret), KPC(slice_store_), K(cg_idx));
  }
  return ret;
}

int ObDirectLoadSliceWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSliceWriter not init", KR(ret), KP(this));
  } else if (nullptr != slice_store_) {
    if (OB_FAIL(slice_store_->close())) {
      LOG_WARN("close slice store failed", K(ret));
    }
  }
  return ret;
}

int ObDirectLoadSliceWriter::inner_fill_vector_index_data(
    ObMacroBlockSliceStore *&macro_block_slice_store,
    ObVectorIndexBaseSliceStore *vec_idx_slice_store,
    const int64_t snapshot_version,
    const ObStorageSchema *storage_schema,
    const SCN &start_scn,
    ObVectorIndexAlgorithmType index_type,
    ObInsertMonitor* insert_monitor)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vec_idx_slice_store) || OB_ISNULL(storage_schema) || snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(vec_idx_slice_store), KP(storage_schema), K(snapshot_version));
  } else {
    // build macro slice
    if (OB_ISNULL(macro_block_slice_store = OB_NEWx(ObMacroBlockSliceStore, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for macro block slice store failed", K(ret));
    } else if (OB_FAIL(macro_block_slice_store->init(tablet_direct_load_mgr_, start_seq_, start_scn))) {
      LOG_WARN("init macro block slice store failed", K(ret), KPC(tablet_direct_load_mgr_), K(start_seq_));
    } else {
      const int64_t rk_cnt = storage_schema->get_rowkey_column_num();
      const int64_t col_cnt = storage_schema->get_column_count();
      blocksstable::ObDatumRow *datum_row = nullptr;
      // do write
      while (OB_SUCC(ret)) {
        // build row
        if (OB_FAIL(vec_idx_slice_store->get_next_vector_data_row(rk_cnt, col_cnt, snapshot_version, index_type, datum_row))) {
          if (ret != OB_ITER_END) {
            LOG_WARN("fail to get next vector data row", K(ret), KPC(vec_idx_slice_store));
          }
        } else if (OB_FAIL(macro_block_slice_store->append_row(*datum_row))) {
          LOG_WARN("fail to append row to macro block slice store", K(ret), KPC(macro_block_slice_store));
        } else {
          LOG_INFO("[vec index debug] append one row into vec data tablet", K(tablet_direct_load_mgr_->get_tablet_id()), KPC(datum_row));
          if (OB_NOT_NULL(insert_monitor)) {
            insert_monitor->inserted_row_cnt_ =  insert_monitor->inserted_row_cnt_ + 1;
          }
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(macro_block_slice_store->close())) {
          LOG_WARN("fail to close macro_block_slice_store", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadSliceWriter::inner_fill_hnsw_vector_index_data(
    ObVectorIndexSliceStore &vec_idx_slice_store,
    const int64_t snapshot_version,
    const ObStorageSchema *storage_schema,
    const SCN &start_scn,
    const int64_t lob_inrow_threshold,
    ObInsertMonitor* insert_monitor)
{
  int ret = OB_SUCCESS;
  int end_trans_ret = OB_SUCCESS;
  ObTxDesc *tx_desc = nullptr;
  ObMacroBlockSliceStore *macro_block_slice_store = nullptr;
  ObVectorIndexAlgorithmType index_type = VIAT_MAX;
  const uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
  if (OB_ISNULL(storage_schema) || snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(vec_idx_slice_store), KP(storage_schema), K(snapshot_version));
  } else if (OB_FAIL(ObInsertLobColumnHelper::start_trans(tablet_direct_load_mgr_->get_ls_id(), false/*is_for_read*/, timeout_us, tx_desc))) {
    LOG_WARN("fail to get tx_desc", K(ret));
  } else if (OB_FAIL(vec_idx_slice_store.serialize_vector_index(&allocator_, tx_desc, lob_inrow_threshold, index_type))) {
    LOG_WARN("fail to do vector index snapshot data serialize", K(ret));
  } else if (OB_FAIL(inner_fill_vector_index_data(macro_block_slice_store, &vec_idx_slice_store, snapshot_version, storage_schema, start_scn, index_type, insert_monitor))) {
    LOG_WARN("fail to inner fill vector index data", K(ret));
  }
  if (OB_NOT_NULL(tx_desc)) {
    if (OB_SUCCESS != (end_trans_ret = ObInsertLobColumnHelper::end_trans(tx_desc, OB_SUCCESS != ret, INT64_MAX))) {
      LOG_WARN("fail to end read trans", K(ret), K(end_trans_ret));
      ret = end_trans_ret;
    }
  }
  if (nullptr != macro_block_slice_store) {
    macro_block_slice_store->~ObMacroBlockSliceStore();
    allocator_.free(macro_block_slice_store);
  }
  return ret;
}

int ObDirectLoadSliceWriter::fill_vector_index_data(
    const int64_t snapshot_version,
    const ObStorageSchema *storage_schema,
    const SCN &start_scn,
    const ObTableSchemaItem &schema_item,
    ObInsertMonitor* insert_monitor)
{
#define FILL_VECTOR_INDEX_DATA(type_str, slice_store_type) \
  slice_store_type *vec_idx_slice_store = static_cast<slice_store_type *>(slice_store_); \
  if (OB_ISNULL(vec_idx_slice_store)) { \
    slice_store_type tmp_slice_store; \
    if (OB_FAIL(tmp_slice_store.init(tablet_direct_load_mgr_, schema_item.vec_idx_param_, schema_item.vec_dim_, \
                                                tablet_direct_load_mgr_->get_column_info()))) { \
      LOG_WARN("init vector index slice store failed", K(ret), KPC(storage_schema)); \
    } else if (OB_FAIL(inner_fill_##type_str##_vector_index_data( \
        tmp_slice_store, snapshot_version, storage_schema, start_scn, schema_item.lob_inrow_threshold_, insert_monitor))) { \
      LOG_WARN("failed to fill vector index data", K(ret), K(tmp_slice_store)); \
    } \
  } else if (OB_FAIL(inner_fill_##type_str##_vector_index_data( \
      *vec_idx_slice_store, snapshot_version, storage_schema, start_scn, schema_item.lob_inrow_threshold_, insert_monitor))) { \
    LOG_WARN("failed to fill vector index data", K(ret), K(*vec_idx_slice_store)); \
  }

  int ret = OB_SUCCESS;
  ObIvfSliceStore *vec_idx_slice_store = static_cast<ObIvfSliceStore *>(slice_store_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(storage_schema) || snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(storage_schema), KP(slice_store_), K(snapshot_version));
  } else if (schema::is_vec_index_snapshot_data_type(storage_schema->get_index_type())) {
    FILL_VECTOR_INDEX_DATA(hnsw, ObVectorIndexSliceStore);
  } else if (schema::is_local_vec_ivf_centroid_index(storage_schema->get_index_type())) {
    FILL_VECTOR_INDEX_DATA(ivf, ObIvfCenterSliceStore);
  } else if (schema::is_vec_ivfsq8_meta_index(storage_schema->get_index_type())) {
    FILL_VECTOR_INDEX_DATA(ivf, ObIvfSq8MetaSliceStore);
  } else if (schema::is_vec_ivfpq_pq_centroid_index(storage_schema->get_index_type())) {
    FILL_VECTOR_INDEX_DATA(ivf, ObIvfPqSliceStore);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index type", K(ret), K(storage_schema->get_index_type()));
  }
#undef FILL_VECTOR_INDEX_DATA

  return ret;
}

int ObDirectLoadSliceWriter::inner_fill_ivf_vector_index_data(
    ObIvfSliceStore &vec_idx_slice_store,
    const int64_t snapshot_version,
    const ObStorageSchema *storage_schema,
    const SCN &start_scn,
    const int64_t lob_inrow_threshold,
    ObInsertMonitor* insert_monitor)
{
  UNUSED(lob_inrow_threshold);
  int ret = OB_SUCCESS;
  bool is_empty = false;
  ObMacroBlockSliceStore *macro_block_slice_store = nullptr;
  if (OB_ISNULL(storage_schema) || snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(vec_idx_slice_store), KP(storage_schema), K(snapshot_version));
  } else if (OB_FAIL(vec_idx_slice_store.is_empty(is_empty))) {
    LOG_WARN("failed to check vec_idx_slice_store", K(ret));
  } else if (is_empty) {
    // do nothing
    LOG_INFO("[vec index debug] maybe no data for this tablet", K(tablet_direct_load_mgr_->get_tablet_id()));
  } else if (OB_FAIL(vec_idx_slice_store.build_clusters())) {
    LOG_WARN("fail to build clusters", K(ret));
  } else if (OB_FAIL(inner_fill_vector_index_data(macro_block_slice_store, &vec_idx_slice_store, snapshot_version, storage_schema, start_scn, VIAT_MAX/*index_type*/, insert_monitor))) {
    LOG_WARN("fail to inner fill vector index data", K(ret));
  } else {
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    if (OB_ISNULL(vec_index_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null ObPluginVectorIndexService ptr", K(ret), K(MTL_ID()));
    } else if (OB_FAIL(vec_index_service->erase_ivf_build_helper(tablet_direct_load_mgr_->get_ls_id(), vec_idx_slice_store.tablet_id_))) {
      LOG_WARN("failed to erase ivf build helper", K(ret), K(tablet_direct_load_mgr_->get_ls_id()), K(vec_idx_slice_store.tablet_id_));
    }
  }
  if (nullptr != macro_block_slice_store) {
    macro_block_slice_store->~ObMacroBlockSliceStore();
    allocator_.free(macro_block_slice_store);
  }
  return ret;
}

int ObDirectLoadSliceWriter::fill_column_group(const ObStorageSchema *storage_schema, const SCN &start_scn, ObInsertMonitor* insert_monitor)
{
  int ret = OB_SUCCESS;
  const bool need_process_cs_replica = tablet_direct_load_mgr_->need_process_cs_replica();
  ObStorageSchema *cs_replica_storage_schema = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(storage_schema));
  } else if (OB_UNLIKELY(row_offset_ < 0)) {
    ret = OB_ERR_SYS;
    LOG_WARN("row offset not set", K(ret), K(row_offset_));
  } else if (OB_ISNULL(slice_store_) || is_empty()) {
    // do nothing
    LOG_INFO("slice_store_ is null or empty", K(ret), KPC_(slice_store), KPC(tablet_direct_load_mgr_));
  } else if (ATOMIC_LOAD(&is_canceled_)) {
    ret = OB_CANCELED;
    LOG_WARN("fil cg task canceled", K(ret), K(is_canceled_));
  } else if (need_process_cs_replica && OB_FAIL(ObStorageSchemaUtil::alloc_storage_schema(allocator_, cs_replica_storage_schema))) {
    LOG_WARN("failed to alloc storage schema", K(ret));
  } else if (need_process_cs_replica && OB_FAIL(cs_replica_storage_schema->init(allocator_, *storage_schema,
                false /*skip_column_info*/, nullptr /*column_group_schema*/, true /*generate_cs_replica_cg_array*/))) {
    LOG_WARN("failed to init storage schema for cs replica", K(ret), KPC(storage_schema));
  } else if (OB_FAIL(inner_fill_column_group(slice_store_, need_process_cs_replica ? cs_replica_storage_schema : storage_schema, start_scn, insert_monitor))) {
    LOG_WARN("failed to fill column group", K(ret));
  }

  if (OB_NOT_NULL(cs_replica_storage_schema)) {
    ObStorageSchemaUtil::free_storage_schema(allocator_, cs_replica_storage_schema);
    cs_replica_storage_schema = nullptr;
  }

  return ret;
}


int ObDirectLoadSliceWriter::inner_fill_column_group(
    ObTabletSliceStore *slice_store,
    const ObStorageSchema *storage_schema,
    const SCN &start_scn,
    ObInsertMonitor* insert_monitor)
{
  int ret = OB_SUCCESS;
  { // remain this {} pair to make git diff more readable
    const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema->get_column_groups();
    FLOG_INFO("[DDL_FILL_CG] fill column group start",
        "tablet_id", tablet_direct_load_mgr_->get_tablet_id(),
        "row_count", slice_store->get_row_count(),
        "column_group_count", cg_schemas.count());

    // 1. reserve writers
    ObCOSliceWriter *cur_writer = nullptr;
    if (OB_ISNULL(cur_writer = OB_NEWx(ObCOSliceWriter, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for co writer failed", K(ret));
    } else {
      // 2. rescan and write
      for (int64_t cg_idx = 0; OB_SUCC(ret) && cg_idx < cg_schemas.count(); ++cg_idx) {
        cur_writer->reset();
        if (OB_FAIL(cur_writer->init(storage_schema, cg_idx, tablet_direct_load_mgr_, start_seq_, row_offset_, start_scn, tablet_direct_load_mgr_->need_process_cs_replica()))) {
          LOG_WARN("init co ddl writer failed", K(ret), KPC(cur_writer), K(cg_idx), KPC(this));
        } else if (OB_FAIL(slice_store->fill_column_group(cg_idx, cur_writer, insert_monitor))) {
          LOG_WARN("fail to fill column group", KR(ret), K(cg_idx));
        } else if (OB_FAIL(cur_writer->close())) {
          LOG_WARN("close co ddl writer failed", K(ret));
        }
      }
    }
    if (OB_NOT_NULL(cur_writer)) {
      cur_writer->~ObCOSliceWriter();
      allocator_.free(cur_writer);
    }
    FLOG_INFO("[DDL_FILL_CG] fill column group finished",
        "tablet_id", tablet_direct_load_mgr_->get_tablet_id(),
        "row_count", slice_store->get_row_count(),
        "column_group_count", cg_schemas.count());
  }
  return ret;
}

void ObDirectLoadSliceWriter::cancel()
{
  ATOMIC_SET(&is_canceled_, true);
  if (OB_NOT_NULL(slice_store_)) {
    slice_store_->cancel();
  }
}

void ObCOSliceWriter::reset()
{
  is_inited_ = false;
  cg_row_.reset();
  datum_rows_.reset();
  macro_block_writer_.reset();
  flush_callback_.reset();
  index_builder_.reset();
  data_desc_.reset();
  cg_schema_ = nullptr;
  cg_idx_ = -1;
}

int ObCOSliceWriter::init(const ObStorageSchema *storage_schema, const int64_t cg_idx,
    ObTabletDirectLoadMgr *tablet_direct_load_mgr, const ObMacroDataSeq &start_seq, const int64_t row_id_offset,
    const SCN &start_scn, const bool with_cs_replica)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == storage_schema || cg_idx < 0 || cg_idx >= storage_schema->get_column_group_count()
        || nullptr == tablet_direct_load_mgr || !start_seq.is_valid() || row_id_offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cg_idx), K(row_id_offset), K(start_seq), KPC(tablet_direct_load_mgr), KPC(storage_schema));
  } else {
    const ObStorageColumnGroupSchema &cg_schema = storage_schema->get_column_groups().at(cg_idx);
    ObITable::TableKey table_key = tablet_direct_load_mgr->get_table_key(); // TODO(cangdi): fix it
    table_key.column_group_idx_ = cg_idx;
    table_key.table_type_ = (cg_schema.is_all_column_group() || cg_schema.is_rowkey_column_group()) ?
      ObITable::TableType::COLUMN_ORIENTED_SSTABLE : ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE;
    const int64_t ddl_task_id = tablet_direct_load_mgr->get_ddl_task_id();
    const uint64_t data_format_version = tablet_direct_load_mgr->get_data_format_version();
    ObLSID ls_id = tablet_direct_load_mgr->get_ls_id();
    const bool need_submit_io = !with_cs_replica; // if need to process cs replica, only write clog, not submit macro block to disk

    ObMacroSeqParam macro_seq_param;
    macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    macro_seq_param.start_ = start_seq.macro_data_seq_;
    ObPreWarmerParam pre_warm_param;
    ObSSTablePrivateObjectCleaner *object_cleaner = nullptr;
    if (OB_FAIL(pre_warm_param.init(ls_id, table_key.tablet_id_))) {
      LOG_WARN("failed to init pre warm param", KR(ret), K(ls_id), "tablet_id", table_key.tablet_id_);
    } else if (GCTX.is_shared_storage_mode()) {
      if (cg_idx >= tablet_direct_load_mgr->get_sqc_build_ctx().cg_index_builders_.count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid cg idx", K(ret), K(cg_idx), K(tablet_direct_load_mgr->get_sqc_build_ctx().cg_index_builders_.count()));
      } else {
        ObSSTableIndexItem &cur_item = tablet_direct_load_mgr->get_sqc_build_ctx().cg_index_builders_.at(cg_idx);
        if (OB_FAIL(flush_callback_.init(ls_id, table_key.tablet_id_,
                tablet_direct_load_mgr->get_is_no_logging() ? DDL_MB_SS_EMPTY_DATA_TYPE : DDL_MB_DATA_TYPE,
                table_key, ddl_task_id,
                start_scn, data_format_version, tablet_direct_load_mgr->get_task_cnt(),
                tablet_direct_load_mgr->get_cg_cnt(), tablet_direct_load_mgr->get_direct_load_type(), row_id_offset))) {
          LOG_WARN("fail to init redo log writer callback", KR(ret));
        } else if (OB_UNLIKELY(!cur_item.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid index item", K(ret), K(cur_item));
        } else if (OB_FAIL(ObSSTablePrivateObjectCleaner::
                               get_cleaner_from_data_store_desc(
                                   cur_item.data_desc_->get_desc(),
                                   object_cleaner))) {
          LOG_WARN("fail to get cleaner from data store desc", K(ret),
                   K(cur_item.data_desc_->get_desc()), KPC(object_cleaner));
        } else if (OB_FAIL(macro_block_writer_.open(
                       cur_item.data_desc_->get_desc(),
                       start_seq.get_parallel_idx(), macro_seq_param,
                       pre_warm_param, *object_cleaner, &flush_callback_))) {
          LOG_WARN("fail to open macro block writer", K(ret), K(ls_id), K(table_key), K(cur_item), K(macro_seq_param));
        }
      }
    } else if (OB_FAIL(data_desc_.init(true/*is ddl*/, *storage_schema,
                                ls_id,
                                table_key.get_tablet_id(),
                                compaction::ObMergeType::MAJOR_MERGE,
                                table_key.get_snapshot_version(),
                                data_format_version,
                                tablet_direct_load_mgr->get_micro_index_clustered(),
                                tablet_direct_load_mgr->get_tablet_transfer_seq(),
                                SCN::min_scn(),
                                &cg_schema,
                                cg_idx,
                                compaction::ObExecMode::EXEC_MODE_LOCAL,
                                need_submit_io))) {
      LOG_WARN("init data store desc failed", K(ret));
    } else if (OB_FAIL(index_builder_.init(data_desc_.get_desc(), ObSSTableIndexBuilder::ENABLE))) { // data_desc is deep copied
      LOG_WARN("init sstable index builder failed", K(ret), K(ls_id), K(table_key), K(data_desc_));
    } else if (FALSE_IT(data_desc_.get_desc().sstable_index_builder_ = &index_builder_)) { // for build the tail index block in macro block
    } else if (OB_FAIL(flush_callback_.init(ls_id, table_key.tablet_id_, DDL_MB_DATA_TYPE, table_key, ddl_task_id,
            start_scn, data_format_version, tablet_direct_load_mgr->get_task_cnt(),
            tablet_direct_load_mgr->get_cg_cnt(), tablet_direct_load_mgr->get_direct_load_type(), row_id_offset,
            false /*need_replay*/, with_cs_replica, need_submit_io))) {
      LOG_WARN("fail to init redo log writer callback", KR(ret));
    } else if (OB_FAIL(ObSSTablePrivateObjectCleaner::get_cleaner_from_data_store_desc(data_desc_.get_desc(), object_cleaner))) {
      LOG_WARN("fail to get cleaner from data store desc", K(ret), K(data_desc_.get_desc()));
    } else if (OB_FAIL(macro_block_writer_.open(data_desc_.get_desc(), start_seq.get_parallel_idx(),
        macro_seq_param, pre_warm_param, *object_cleaner, &flush_callback_))) {
      LOG_WARN("fail to open macro block writer", K(ret), K(ls_id), K(table_key), K(data_desc_), K(start_seq), KPC(object_cleaner));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(cg_row_.init(cg_schema.column_cnt_))) {
        LOG_WARN("init column group row failed", K(ret));
      } else if (OB_FAIL(datum_rows_.vectors_.prepare_allocate(cg_schema.column_cnt_))) {
        LOG_WARN("fail to prepare allocate", K(ret));
      } else {
        datum_rows_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
        cg_idx_ = cg_idx;
        cg_schema_ = &cg_schema;
        is_inited_ = true;
      }
    }
  }
  LOG_DEBUG("co ddl writer init", K(ret), K(cg_idx), K(row_id_offset), KPC(this));
  return ret;
}

int ObCOSliceWriter::append_row(const sql::ObChunkDatumStore::StoredRow *stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(project_cg_row(*cg_schema_, stored_row, cg_row_))) {
    LOG_WARN("project column group row failed", K(ret));
  } else if (OB_FAIL(macro_block_writer_.append_row(cg_row_))) {
    LOG_WARN("write column group row failed", K(ret));
  }
  return ret;
}

int ObCOSliceWriter::append_batch(const ObIArray<ObIVector *> &vectors, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(vectors.count() != datum_rows_.get_column_count() || batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(vectors.count()), K(batch_size), K(datum_rows_.get_column_count()));
  } else {
    for (int64_t i = 0; i < datum_rows_.get_column_count(); ++i) {
      datum_rows_.vectors_.at(i) = vectors.at(i);
    }
    datum_rows_.row_count_ = batch_size;
    if (OB_FAIL(macro_block_writer_.append_batch(datum_rows_))) {
      LOG_WARN("write column group row failed", K(ret));
    }
  }
  return ret;
}

int ObCOSliceWriter::project_cg_row(const ObStorageColumnGroupSchema &cg_schema,
                                const ObChunkDatumStore::StoredRow *stored_row,
                                ObDatumRow &cg_row)
{
  int ret = OB_SUCCESS;
  cg_row.reuse();
  cg_row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
  if (OB_UNLIKELY(!cg_schema.is_valid() || nullptr == stored_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cg_schema), KP(stored_row));
  } else if (cg_schema.column_cnt_ != stored_row->cnt_ || cg_row.get_column_count() != cg_schema.column_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column count not match", K(ret), K(stored_row->cnt_), K(cg_row), K(cg_schema));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_schema.column_cnt_; ++i) {
      const ObDatum &cur_datum = stored_row->cells()[i];
      cg_row.storage_datums_[i].set_datum(cur_datum);
    }
  }
  return ret;
}

int ObCOSliceWriter::close()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(macro_block_writer_.close())) {
    LOG_WARN("close macro block writer failed", K(ret));
  }
  LOG_DEBUG("co ddl writer close", K(ret), KPC(this));
  return ret;
}

ObInsertMonitor::~ObInsertMonitor()
{
}

void ObTabletDirectLoadSliceGroup::reset()
{
  int ret = OB_SUCCESS;
  {
    ObBucketWLockAllGuard all_lock(bucket_lock_);
    for (auto iter = batch_slice_map_.begin(); OB_SUCC(ret) && iter != batch_slice_map_.end(); ++iter) {
      ObArray<int64_t> *cur_array = iter->second;
      cur_array->~ObArray<int64_t>();
      allocator_.free(cur_array);
      cur_array = nullptr;
    }
  }
  bucket_lock_.destroy();
  allocator_.reset();
  is_inited_ = false;
}

int ObTabletDirectLoadSliceGroup::init(const int64_t task_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t memory_limit = 1024L * 1024L * 1024L * 10L; // 10GB
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(lbt()));
  } else if (OB_UNLIKELY(task_cnt < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task cnt", K(ret), K(task_cnt));
  } else {
    ObMemAttr attr(MTL_ID(), "batch_slice_map");
    if (OB_FAIL(allocator_.init(OB_MALLOC_MIDDLE_BLOCK_SIZE, "SLICE_GRP", MTL_ID(), memory_limit))) {
      LOG_WARN("init io allocator failed", K(ret));
    } else if (OB_FAIL(batch_slice_map_.create(task_cnt, attr, attr))) {
      LOG_WARN("fail to create map", K(ret), K(task_cnt));
    } else if (OB_FAIL(bucket_lock_.init(task_cnt))) {
      LOG_WARN("failed to init bucket lock", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObVectorIndexSliceStore::init(
    ObTabletDirectLoadMgr *tablet_direct_load_mgr,
    const ObString vec_idx_param,
    const int64_t vec_dim,
    const ObIArray<ObColumnSchemaItem> &col_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(nullptr == tablet_direct_load_mgr || vec_idx_param.empty() || 0 >= vec_dim || col_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(tablet_direct_load_mgr));
  } else {
    is_inited_ = true;
    ctx_.ls_id_ = tablet_direct_load_mgr->get_ls_id();
    tablet_id_ = tablet_direct_load_mgr->get_tablet_id();
    vec_idx_param_ = vec_idx_param;
    vec_dim_ = vec_dim;
    // get data tablet id and lob tablet id
    ObLSHandle ls_handle;
    ObTabletHandle five_tablet_handle;
    ObTabletHandle data_tablet_handle;
    ObTabletBindingMdsUserData ddl_data;
    if (OB_FAIL(MTL(ObLSService *)->get_ls(ctx_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get log stream", K(ret), K(ctx_.ls_id_));
    } else if (OB_ISNULL(ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls should not be null", K(ret));
    } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id_, five_tablet_handle))) {
      LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id_));
    } else if (FALSE_IT(ctx_.data_tablet_id_ = five_tablet_handle.get_obj()->get_data_tablet_id())) {
    } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(ctx_.data_tablet_id_, data_tablet_handle))) {
      LOG_WARN("fail to get tablet handle", K(ret), K(ctx_.data_tablet_id_));
    } else if (OB_FAIL(data_tablet_handle.get_obj()->get_ddl_data(ddl_data))) {
      LOG_WARN("failed to get ddl data from tablet", K(ret), K(data_tablet_handle));
    } else {
      ctx_.lob_meta_tablet_id_ = ddl_data.lob_meta_tablet_id_;
      ctx_.lob_piece_tablet_id_ = ddl_data.lob_piece_tablet_id_;
    }
    // get vid col and vector col
    for (int64_t i = 0; OB_SUCC(ret) && i < col_array.count(); i++) {
      if (ObSchemaUtils::is_vec_hnsw_vid_column(col_array.at(i).column_flags_)) {
        vector_vid_col_idx_ = i;
      } else if (ObSchemaUtils::is_vec_hnsw_vector_column(col_array.at(i).column_flags_)) {
        vector_col_idx_ = i;
      } else if (ObSchemaUtils::is_vec_hnsw_key_column(col_array.at(i).column_flags_)) {
        vector_key_col_idx_ = i;
      } else if (ObSchemaUtils::is_vec_hnsw_data_column(col_array.at(i).column_flags_)) {
        vector_data_col_idx_ = i;
      }
    }
    if (OB_SUCC(ret)) {
      if (vector_vid_col_idx_ == -1 || vector_col_idx_ == -1 || vector_key_col_idx_ == -1 || vector_data_col_idx_ == -1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get valid vector index col idx", K(ret), K(vector_col_idx_), K(vector_vid_col_idx_),
                 K(vector_key_col_idx_), K(vector_data_col_idx_), K(col_array));
      }
    }
  }
  return ret;
}

int ObTabletDirectLoadSliceGroup::record_slice_id(const ObTabletDirectLoadBatchSliceKey &key, const int64_t slice_id)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> *slice_array = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(lbt()));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_FAIL(batch_slice_map_.get_refactored(key, slice_array))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to set key into map", K(ret), K(key), KP(slice_array));
      } else {
        ObArray<int64_t> *new_array = nullptr;
        void *buf = nullptr;
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObArray<int64_t>)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObArray<int64_t>)));
        } else {
          new_array = new (buf) ObArray<int64_t>;
          new_array->set_attr(ObMemAttr(MTL_ID(), "slice_array"));
          if (OB_FAIL(batch_slice_map_.set_refactored(key, new_array))) {
            LOG_WARN("fail to set key into map", K(ret), K(key), KP(new_array));
          } else if (OB_FAIL(new_array->push_back(slice_id))) {
            LOG_WARN("fail to push slice_writer", K(ret), K(key), K(slice_id));
          }
        }
        if (OB_FAIL(ret)) {
          if (OB_NOT_NULL(new_array)) {
            new_array->~ObArray<int64_t>();
            allocator_.free(new_array);
            new_array = nullptr;
          }
        }
      }
    } else if (OB_ISNULL(slice_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null slice", K(ret), K(key), K(slice_id));
    } else if (OB_FAIL(slice_array->push_back(slice_id))) {
      LOG_WARN("fail to push slice_writer", K(ret), K(key), K(slice_id));
    }
  }
  return ret;
}

int ObTabletDirectLoadSliceGroup::get_slice_array(const ObTabletDirectLoadBatchSliceKey &key, ObArray<int64_t> &slice_array)
{
  int ret = OB_SUCCESS;
  slice_array.reset();
  ObArray<int64_t> *cur_slice_array = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(lbt()));
  } else {
    ObBucketHashRLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_FAIL(batch_slice_map_.get_refactored(key, cur_slice_array))) {
      LOG_WARN("fail to get slice array", K(ret), K(key), KP(cur_slice_array));
    } else if (OB_ISNULL(cur_slice_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid slice array", K(ret), KP(cur_slice_array));
    } else if (OB_FAIL(slice_array.assign(*cur_slice_array))) {
      LOG_WARN("fail to copy array", K(ret));
    }
  }
  return ret;
}

int ObTabletDirectLoadSliceGroup::remove_slice_array(const ObTabletDirectLoadBatchSliceKey &key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(lbt()));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
    ObArray<int64_t> *slice_array = nullptr;
    if (OB_FAIL(batch_slice_map_.erase_refactored(key, &slice_array))) {
      LOG_WARN("erase failed", K(ret), K(key));
    } else {
      slice_array->~ObArray<int64_t>();
      allocator_.free(slice_array);
      slice_array = nullptr;
    }
  }
  return ret;
}

int ObVectorIndexBaseSliceStore::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

void ObVectorIndexBaseSliceStore::reset()
{
  is_inited_ = false;
  row_cnt_ = 0;
  tablet_id_.reset();
  vec_idx_param_.reset();
  vec_dim_ = 0;
  cur_row_pos_ = 0;
  current_row_.reset();
}

int ObVectorIndexSliceStore::append_row(const blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // append to vector inedx adaptor
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    ObPluginVectorIndexAdapterGuard adaptor_guard;
    if (OB_ISNULL(vec_index_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null ObPluginVectorIndexService ptr", K(ret), K(MTL_ID()));
    } else if (OB_FAIL(vec_index_service->acquire_adapter_guard(ctx_.ls_id_,
                                                                tablet_id_,
                                                                ObIndexType::INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                                                adaptor_guard,
                                                                &vec_idx_param_,
                                                                vec_dim_))) {
      LOG_WARN("fail to get ObMockPluginVectorIndexAdapter", K(ret), K(ctx_.ls_id_), K(tablet_id_));
    } else {
      // get vid and vector
      ObString vec_str;
      int64_t vec_vid;
      if (datum_row.get_column_count() <= vector_vid_col_idx_ || datum_row.get_column_count() <= vector_col_idx_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get valid vector index col idx", K(ret), K(vector_col_idx_), K(vector_vid_col_idx_), K(datum_row));
      } else if (datum_row.storage_datums_[vector_col_idx_].is_null() || datum_row.storage_datums_[vector_col_idx_].is_nop()) {
        // do nothing
      } else if (FALSE_IT(vec_vid = datum_row.storage_datums_[vector_vid_col_idx_].get_int())) {
      } else if (FALSE_IT(vec_str = datum_row.storage_datums_[vector_col_idx_].get_string())) {
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator_,
                                                                    ObLongTextType,
                                                                    CS_TYPE_BINARY,
                                                                    true,
                                                                    vec_str))) {
        LOG_WARN("fail to get real data.", K(ret), K(vec_str));
      } else if (vec_str.length() == 0) {
        // do nothing
      } else if (OB_FAIL(adaptor_guard.get_adatper()->add_snap_index(reinterpret_cast<float*>(vec_str.ptr()), &vec_vid, 1))) {
        LOG_WARN("fail to build index to adaptor", K(ret), KPC(this));
      } else {
        LOG_INFO("[vec index debug] add into snap index success", K(tablet_id_), K(vec_vid), K(vec_str));
      }
    }
  }
  tmp_allocator_.reuse();
  return ret;
}

void ObVectorIndexSliceStore::reset()
{
  ObVectorIndexBaseSliceStore::reset();
  ctx_.reset();
  vector_vid_col_idx_ = -1;
  vector_col_idx_ = -1;
  vector_key_col_idx_ = -1;
  vector_data_col_idx_ = -1;
  vec_allocator_.reset();
  tmp_allocator_.reset();
}

int ObVectorIndexSliceStore::serialize_vector_index(
    ObIAllocator *allocator,
    ObTxDesc *tx_desc,
    int64_t lob_inrow_threshold,
    ObVectorIndexAlgorithmType &type)
{
  int ret = OB_SUCCESS;
  tmp_allocator_.reuse();
  // first we do vsag serialize
  ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexAdapterGuard adaptor_guard;
  if (OB_ISNULL(vec_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null ObPluginVectorIndexService ptr", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(vec_index_service->acquire_adapter_guard(ctx_.ls_id_,
                                                              tablet_id_,
                                                              ObIndexType::INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                                              adaptor_guard,
                                                              &vec_idx_param_,
                                                              vec_dim_))) {
    LOG_WARN("fail to get ObMockPluginVectorIndexAdapter", K(ret), K(ctx_.ls_id_), K(tablet_id_));
  } else {
    ObHNSWSerializeCallback callback;
    ObOStreamBuf::Callback cb = callback;

    ObHNSWSerializeCallback::CbParam param;
    param.vctx_ = &ctx_;
    param.allocator_ = allocator;
    param.tmp_allocator_ = &tmp_allocator_;
    param.lob_inrow_threshold_ = lob_inrow_threshold;
    // build tx
    oceanbase::transaction::ObTransService *txs = MTL(transaction::ObTransService*);
    oceanbase::transaction::ObTxReadSnapshot snapshot;
    int64_t timeout = ObTimeUtility::fast_current_time() + ObInsertLobColumnHelper::LOB_ACCESS_TX_TIMEOUT;
    if (OB_ISNULL(tx_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get tx desc, get nullptr", K(ret));
    } else if (OB_FAIL(txs->get_ls_read_snapshot(*tx_desc, transaction::ObTxIsolationLevel::RC, ctx_.ls_id_, timeout, snapshot))) {
      LOG_WARN("fail to get snapshot", K(ret));
    } else {
      param.timeout_ = timeout;
      param.snapshot_ = &snapshot;
      param.tx_desc_ = tx_desc;
      ObPluginVectorIndexAdaptor *adp = adaptor_guard.get_adatper();
      if (OB_FAIL(adp->check_snap_hnswsq_index())) {
        LOG_WARN("failed to check snap hnswsq index", K(ret));
      } else if (OB_FAIL(adp->serialize(allocator, param, cb))) {
        if (OB_NOT_INIT == ret) {
          // ignore // no data in slice store
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to do vsag serialize", K(ret));
        }
      } else {
        type = adp->get_snap_index_type();
        LOG_INFO("HgraphIndex finish vsag serialize for tablet", K(tablet_id_), K(ctx_.get_vals().count()), K(type));
      }
    }
  }
  tmp_allocator_.reuse();
  return ret;
}

bool ObVectorIndexSliceStore::is_vec_idx_col_invalid(const int64_t column_cnt) const
{
  return vector_key_col_idx_ < 0 || vector_key_col_idx_ >= column_cnt ||
         vector_data_col_idx_ < 0 || vector_data_col_idx_ >= column_cnt ||
         vector_vid_col_idx_ < 0 || vector_vid_col_idx_ >= column_cnt ||
         vector_col_idx_ < 0 || vector_col_idx_ >= column_cnt;
}

int ObVectorIndexSliceStore::get_next_vector_data_row(
    const int64_t rowkey_cnt,
    const int64_t column_cnt,
    const int64_t snapshot_version,
    ObVectorIndexAlgorithmType index_type,
    blocksstable::ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  const int64_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int64_t request_cnt = column_cnt + extra_rowkey_cnt;
  if (current_row_.get_column_count() <= 0
    && OB_FAIL(current_row_.init(vec_allocator_, request_cnt))) {
    LOG_WARN("init datum row failed", K(ret), K(request_cnt));
  } else if (OB_UNLIKELY(current_row_.get_column_count() != request_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(request_cnt), "datum_row_cnt", current_row_.get_column_count());
  } else if (cur_row_pos_ >= ctx_.vals_.count()) {
    ret = OB_ITER_END;
  } else if (index_type >= VIAT_MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get index type invalid.", K(ret), K(index_type));
  } else if (is_vec_idx_col_invalid(current_row_.get_column_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, vec col idx error", K(ret), K(vector_key_col_idx_), K(vector_data_col_idx_),
             K(vector_vid_col_idx_), K(vector_col_idx_));
  } else {
    // set vec key
    int64_t key_pos = 0;
    char *key_str = static_cast<char*>(vec_allocator_.alloc(OB_VEC_IDX_SNAPSHOT_KEY_LENGTH));
    if (OB_ISNULL(key_str)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc vec key", K(ret));
    } else if (index_type == VIAT_HNSW && OB_FAIL(databuff_printf(key_str, OB_VEC_IDX_SNAPSHOT_KEY_LENGTH, key_pos, "%lu_hnsw_data_part%05ld", tablet_id_.id(), cur_row_pos_))) {
      LOG_WARN("fail to build vec snapshot key str", K(ret), K(index_type));
    } else if (index_type == VIAT_HNSW_SQ && OB_FAIL(databuff_printf(key_str, OB_VEC_IDX_SNAPSHOT_KEY_LENGTH, key_pos, "%lu_hnsw_sq_data_part%05ld", tablet_id_.id(), cur_row_pos_))) {
      LOG_WARN("fail to build sq vec snapshot key str", K(ret), K(index_type));
    } else {
      current_row_.storage_datums_[vector_key_col_idx_].set_string(key_str, key_pos);
    }
    // set vec data
    if (OB_FAIL(ret)) {
    } else {
      // TODO @lhd maybe we should do deep copy
      current_row_.storage_datums_[vector_data_col_idx_].set_string(ctx_.vals_.at(cur_row_pos_));
    }
    // set vid and vec to null
    if (OB_SUCC(ret)) {
      current_row_.storage_datums_[vector_vid_col_idx_].set_null();
      current_row_.storage_datums_[vector_col_idx_].set_null();
    }
    if (OB_SUCC(ret)) {
      // add extra rowkey
      // TODO how to get snapshot
      current_row_.storage_datums_[rowkey_cnt].set_int(-snapshot_version);
      current_row_.storage_datums_[rowkey_cnt + 1].set_int(0);
      current_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      datum_row = &current_row_;
      cur_row_pos_++;
    }
  }
  return ret;
}

/////////////////////
// ObIvfSliceStore //
/////////////////////

void ObIvfSliceStore::reset()
{
  ObVectorIndexBaseSliceStore::reset();
  vec_allocator_.reset();
  tmp_allocator_.reset();
}

///////////////////////////
// ObIvfCenterSliceStore //
///////////////////////////

void ObIvfCenterSliceStore::reset()
{
  ObIvfSliceStore::reset();
  center_id_col_idx_ = -1;
  center_vector_col_idx_ = -1;
}

int ObIvfCenterSliceStore::init(
    ObTabletDirectLoadMgr *tablet_direct_load_mgr,
    const ObString vec_idx_param,
    const int64_t vec_dim,
    const ObIArray<ObColumnSchemaItem> &col_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(nullptr == tablet_direct_load_mgr || vec_idx_param.empty() || 0 >= vec_dim || col_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(tablet_direct_load_mgr));
  } else {
    ObLSID ls_id = tablet_direct_load_mgr->get_ls_id();
    tablet_id_ = tablet_direct_load_mgr->get_tablet_id();
    vec_idx_param_ = vec_idx_param;
    vec_dim_ = vec_dim;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_array.count(); i++) {
      if (ObSchemaUtils::is_vec_ivf_center_id_column(col_array.at(i).column_flags_)) {
        center_id_col_idx_ = i;
      } else if (ObSchemaUtils::is_vec_ivf_center_vector_column(col_array.at(i).column_flags_)) {
        center_vector_col_idx_ = i;
      }
    }
    if (OB_SUCC(ret)) {
      ObIvfFlatBuildHelper *helper = nullptr;
      if (center_id_col_idx_ == -1 || center_vector_col_idx_ == -1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get valid vector index col idx", K(ret), K(center_id_col_idx_), K(center_vector_col_idx_), K(col_array));
      } else {
        ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
        if (OB_ISNULL(vec_index_service)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null ObPluginVectorIndexService ptr", K(ret), K(MTL_ID()));
        } else if (OB_FAIL(vec_index_service->acquire_ivf_build_helper_guard(ls_id,
                                                                             tablet_id_,
                                                                             ObIndexType::INDEX_TYPE_VEC_IVFFLAT_CENTROID_LOCAL,
                                                                             helper_guard_,
                                                                             vec_idx_param_))) {
          LOG_WARN("failed to acquire ivf build helper guard", K(ret), K(ls_id), K(tablet_id_));
        } else if (OB_FAIL(get_spec_ivf_helper(helper))) {
          LOG_WARN("fail to get ivf flat helper", K(ret));
        } else if (OB_FAIL(helper->init_kmeans_ctx(vec_dim_))) {
          LOG_WARN("failed ot init kmeans ctx", K(ret), K_(vec_dim));
        } else {
          is_inited_ = true;
        }
      }
    }
  }
  return ret;
}

int ObIvfCenterSliceStore::append_row(const blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // get vid and vector
    ObString vec_str;
    ObSingleKmeansExecutor *executor = nullptr;
    ObIvfFlatBuildHelper *helper = nullptr;
    if (datum_row.get_column_count() <= center_vector_col_idx_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get valid vector index col idx", K(ret), K(center_vector_col_idx_), K(datum_row));
    } else if (datum_row.storage_datums_[center_vector_col_idx_].is_null()) {
      // do nothing // ignore
    } else if (FALSE_IT(vec_str = datum_row.storage_datums_[center_vector_col_idx_].get_string())) {
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator_,
                                                                  ObLongTextType,
                                                                  CS_TYPE_BINARY,
                                                                  true,
                                                                  vec_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(vec_str));
    } else if (OB_FAIL(get_spec_ivf_helper(helper))) {
      LOG_WARN("fail to get ivf flat helper", K(ret));
    } else if (OB_ISNULL(executor = helper->get_kmeans_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr ctx", K(ret));
    } else if (OB_FAIL(executor->append_sample_vector(reinterpret_cast<float*>(vec_str.ptr())))) {
      LOG_WARN("failed to append sample vector", K(ret));
    } else {
      LOG_DEBUG("[vec index debug] append sample vector", K(tablet_id_), K(vec_str));
    }
  }
  tmp_allocator_.reuse();
  return ret;
}

int ObIvfCenterSliceStore::build_clusters()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSingleKmeansExecutor *executor = nullptr;
    ObIvfFlatBuildHelper *helper = nullptr;
    if (OB_FAIL(get_spec_ivf_helper(helper))) {
      LOG_WARN("fail to get ivf flat helper", K(ret));
    } else if (OB_ISNULL(executor = helper->get_kmeans_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr ctx", K(ret));
    } else if (OB_FAIL(executor->build())) {
      LOG_WARN("failed to build clusters", K(ret));
    }
  }
  return ret;
}

int ObIvfCenterSliceStore::is_empty(bool &empty)
{
  int ret = OB_SUCCESS;
  empty = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObKmeansExecutor *executor = nullptr;
    ObIvfFlatBuildHelper *helper = nullptr;
    if (OB_FAIL(get_spec_ivf_helper(helper))) {
      LOG_WARN("fail to get ivf flat helper", K(ret));
    } else if (OB_ISNULL(executor = helper->get_kmeans_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr ctx", K(ret));
    } else {
      empty = executor->is_empty();
    }
  }
  return ret;
}

int ObIvfCenterSliceStore::get_next_vector_data_row(
    const int64_t rowkey_cnt,
    const int64_t column_cnt,
    const int64_t snapshot_version,
    ObVectorIndexAlgorithmType index_type,
    blocksstable::ObDatumRow *&datum_row)
{
  UNUSED(index_type);
  int ret = OB_SUCCESS;
  tmp_allocator_.reuse();
  ObSingleKmeansExecutor *executor = nullptr;
  const int64_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int64_t request_cnt = column_cnt + extra_rowkey_cnt;
  ObIvfFlatBuildHelper *helper = nullptr;
  if (OB_FAIL(get_spec_ivf_helper(helper))) {
    LOG_WARN("fail to get ivf flat helper", K(ret));
  } else if (OB_ISNULL(executor = helper->get_kmeans_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctx", K(ret));
  } else if (current_row_.get_column_count() <= 0
    && OB_FAIL(current_row_.init(tmp_allocator_, request_cnt))) {
    LOG_WARN("init datum row failed", K(ret), K(request_cnt));
  } else if (OB_UNLIKELY(current_row_.get_column_count() != request_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(request_cnt), "datum_row_cnt", current_row_.get_column_count());
  } else if (cur_row_pos_ >= executor->get_centers_count()) {
    ret = OB_ITER_END;
  } else if (center_id_col_idx_ < 0 || center_id_col_idx_ >= current_row_.get_column_count() ||
             center_vector_col_idx_ < 0 || center_vector_col_idx_ >= current_row_.get_column_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, center col idx error", K(ret), K(center_id_col_idx_), K(center_vector_col_idx_));
  } else {
    ObString data_str;
    ObString vec_res;
    float *center_vector = nullptr;
    int64_t dim = executor->get_centers_dim();
    int64_t buf_len = OB_DOC_ID_COLUMN_BYTE_LENGTH;
    char *buf = nullptr;
    if (OB_ISNULL(center_vector = executor->get_center(cur_row_pos_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("upexpected nullptr center_vector", K(ret));
    } else {
      data_str.assign(reinterpret_cast<char *>(center_vector), static_cast<int64_t>(sizeof(float) * dim));
      if (OB_FAIL(ObArrayExprUtils::set_array_res(nullptr, data_str.length(), tmp_allocator_, vec_res, data_str.ptr()))) {
        LOG_WARN("failed to set array res", K(ret));
      } else if (OB_ISNULL(buf = static_cast<char*>(tmp_allocator_.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc cid", K(ret));
      } else {
        ObString cid_str(buf_len, 0, buf);
        ObCenterId center_id(tablet_id_.id(), cur_row_pos_ + 1);
        if (OB_FAIL(ObVectorClusterHelper::set_center_id_to_string(center_id, cid_str))) {
          LOG_WARN("failed to set center_id to string", K(ret), K(center_id), K(cid_str));
        } else {
          for (int64_t idx = rowkey_cnt + extra_rowkey_cnt; idx < request_cnt; ++idx) {
            if (idx != center_id_col_idx_ && idx != center_vector_col_idx_) {
              current_row_.storage_datums_[idx].set_null(); // set null part key
            }
          }
          current_row_.storage_datums_[center_vector_col_idx_].set_string(vec_res);
          current_row_.storage_datums_[center_id_col_idx_].set_string(cid_str);
          current_row_.storage_datums_[rowkey_cnt].set_int(-snapshot_version);
          current_row_.storage_datums_[rowkey_cnt + 1].set_int(0);
          current_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
          datum_row = &current_row_;
          cur_row_pos_++;
        }
      }
    }
  }
  return ret;
}

////////////////////////////
// ObIvfSq8MetaSliceStore //
////////////////////////////

int ObIvfSq8MetaSliceStore::is_empty(bool &empty)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // NOTE(liyao): empty = false if is_inited_
    empty = false;
  }
  return ret;
}

void ObIvfSq8MetaSliceStore::reset()
{
  ObIvfSliceStore::reset();
  meta_id_col_idx_ = -1;
  meta_vector_col_idx_ = -1;
}

int ObIvfSq8MetaSliceStore::init(
    ObTabletDirectLoadMgr *tablet_direct_load_mgr,
    const ObString vec_idx_param,
    const int64_t vec_dim,
    const ObIArray<ObColumnSchemaItem> &col_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(nullptr == tablet_direct_load_mgr)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null tablet_direct_load_mgr", K(ret));
  } else {
    ObLSID ls_id = tablet_direct_load_mgr->get_ls_id();
    tablet_id_ = tablet_direct_load_mgr->get_tablet_id();
    vec_idx_param_ = vec_idx_param;
    vec_dim_ = vec_dim;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_array.count(); i++) {
      if (ObSchemaUtils::is_vec_ivf_meta_id_column(col_array.at(i).column_flags_)) {
        meta_id_col_idx_ = i;
      } else if (ObSchemaUtils::is_vec_ivf_meta_vector_column(col_array.at(i).column_flags_)) {
        meta_vector_col_idx_ = i;
      }
    }
    if (OB_SUCC(ret)) {
      ObIvfSq8BuildHelper *helper = nullptr;
      if (meta_id_col_idx_ == -1 || meta_vector_col_idx_ == -1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get valid vector index col idx", K(ret), K(meta_id_col_idx_), K(meta_vector_col_idx_), K(col_array));
      } else {
        ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
        if (OB_ISNULL(vec_index_service)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null ObPluginVectorIndexService ptr", K(ret), K(MTL_ID()));
        } else if (OB_FAIL(vec_index_service->acquire_ivf_build_helper_guard(ls_id,
                                                                             tablet_id_,
                                                                             ObIndexType::INDEX_TYPE_VEC_IVFSQ8_META_LOCAL,
                                                                             helper_guard_,
                                                                             vec_idx_param_))) {
          LOG_WARN("failed to acquire ivf build helper guard", K(ret), K(ls_id), K(tablet_id_));
        } else if (OB_FAIL(get_spec_ivf_helper(helper))) {
          LOG_WARN("fail to get ivf flat helper", K(ret));
        } else if (OB_FAIL(helper->init_result_vectors(vec_dim_))) {
          LOG_WARN("failed ot init kmeans ctx", K(ret), K(vec_dim_));
        } else {
          is_inited_ = true;
        }
      }
    }
  }
  return ret;
}

int ObIvfSq8MetaSliceStore::append_row(const blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // get vid and vector
    ObString vec_str;
    ObSingleKmeansExecutor *ctx = nullptr;
    ObIvfSq8BuildHelper *helper = nullptr;
    int64_t vec_dim = 0;
    if (datum_row.get_column_count() <= meta_vector_col_idx_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get valid vector index col idx", K(ret), K(meta_vector_col_idx_), K(datum_row));
    } else if (datum_row.storage_datums_[meta_vector_col_idx_].is_null()) {
      // do nothing // ignore
    } else if (FALSE_IT(vec_str = datum_row.storage_datums_[meta_vector_col_idx_].get_string())) {
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator_,
                                                                  ObLongTextType,
                                                                  CS_TYPE_BINARY,
                                                                  true,
                                                                  vec_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(vec_str));
    } else if (OB_FAIL(get_spec_ivf_helper(helper))) {
      LOG_WARN("fail to get ivf flat helper", K(ret));
    } else if (FALSE_IT(vec_dim = vec_str.length() / sizeof(float))) {
    } else if (OB_FAIL(helper->update(reinterpret_cast<float*>(vec_str.ptr()), vec_dim))) {
      LOG_WARN("failed to update helper", K(ret));
    } else {
      LOG_DEBUG("[vec index debug] append sample vector", K(tablet_id_), K(vec_str));
    }
  }
  tmp_allocator_.reuse();
  return ret;
}

int ObIvfSq8MetaSliceStore::build_clusters()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObIvfSq8BuildHelper *helper = nullptr;
    if (OB_FAIL(get_spec_ivf_helper(helper))) {
      LOG_WARN("fail to get ivf flat helper", K(ret));
    } else if (OB_FAIL(helper->build())) {
      LOG_WARN("fail to do helper build", K(ret), KPC(helper));
    }
  }
  return ret;
}

int ObIvfSq8MetaSliceStore::get_next_vector_data_row(
    const int64_t rowkey_cnt,
    const int64_t column_cnt,
    const int64_t snapshot_version,
    ObVectorIndexAlgorithmType index_type,
    blocksstable::ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  UNUSED(index_type);
  tmp_allocator_.reuse();
  const int64_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int64_t request_cnt = column_cnt + extra_rowkey_cnt;
  ObIvfSq8BuildHelper *helper = nullptr;
  if (current_row_.get_column_count() <= 0
    && OB_FAIL(current_row_.init(vec_allocator_, request_cnt))) {
    LOG_WARN("init datum row failed", K(ret), K(request_cnt));
  } else if (OB_UNLIKELY(current_row_.get_column_count() != request_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(request_cnt), "datum_row_cnt", current_row_.get_column_count());
  } else if (cur_row_pos_ >= ObIvfConstant::SQ8_META_ROW_COUNT) {
    ret = OB_ITER_END;
  } else if (meta_id_col_idx_ < 0 || meta_id_col_idx_ >= current_row_.get_column_count() ||
             meta_vector_col_idx_ < 0 || meta_vector_col_idx_ >= current_row_.get_column_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, center col idx error", K(ret), K(meta_id_col_idx_), K(meta_vector_col_idx_));
  } else if (OB_FAIL(get_spec_ivf_helper(helper))) {
    LOG_WARN("fail to get ivf flat helper", K(ret));
  } else {
    ObString data_str;
    ObString vec_res;
    float *cur_vector = nullptr;
    int64_t buf_len = OB_DOC_ID_COLUMN_BYTE_LENGTH;
    char *buf = nullptr;
    if (OB_FAIL(helper->get_result(cur_row_pos_, cur_vector))) {
      LOG_WARN("fail to get result", K(ret));
    } else {
      data_str.assign(reinterpret_cast<char *>(cur_vector), static_cast<int64_t>(sizeof(float) * vec_dim_));
      if (OB_FAIL(ObArrayExprUtils::set_array_res(nullptr, data_str.length(), vec_allocator_, vec_res, data_str.ptr()))) {
        LOG_WARN("failed to set array res", K(ret));
      } else if (OB_ISNULL(buf = static_cast<char*>(vec_allocator_.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc cid", K(ret));
      } else {
        ObString cid_str(buf_len, 0, buf);
        // reuse center_id encode, min: 1, max: 2, step: 3
        ObCenterId center_id(tablet_id_.id(), cur_row_pos_ + 1);
        if (OB_FAIL(ObVectorClusterHelper::set_center_id_to_string(center_id, cid_str))) {
          LOG_WARN("failed to set center_id to string", K(ret), K(center_id), K(cid_str));
        } else {
          for (int64_t i = 0; i < current_row_.get_column_count(); ++i) {
            if (meta_vector_col_idx_ == i) {
              current_row_.storage_datums_[meta_vector_col_idx_].set_string(vec_res);
            } else if (meta_id_col_idx_ == i) {
              current_row_.storage_datums_[meta_id_col_idx_].set_string(cid_str);
            } else if (rowkey_cnt == i) {
              current_row_.storage_datums_[i].set_int(-snapshot_version);
            } else if (rowkey_cnt + 1 == i) {
              current_row_.storage_datums_[i].set_int(0);
            } else {
              current_row_.storage_datums_[i].set_null(); // set part key null
            }
          }
          current_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
          datum_row = &current_row_;
          cur_row_pos_++;
        }
      }
    }
  }
  return ret;
}

///////////////////////////
// ObIvfPqSliceStore //
///////////////////////////

void ObIvfPqSliceStore::reset()
{
  ObIvfSliceStore::reset();
  pq_center_id_col_idx_ = -1;
  pq_center_vector_col_idx_ = -1;
}

int ObIvfPqSliceStore::init(
    ObTabletDirectLoadMgr *tablet_direct_load_mgr,
    const ObString vec_idx_param,
    const int64_t vec_dim,
    const ObIArray<ObColumnSchemaItem> &col_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(nullptr == tablet_direct_load_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(tablet_direct_load_mgr));
  } else {
    ObLSID ls_id = tablet_direct_load_mgr->get_ls_id();
    tablet_id_ = tablet_direct_load_mgr->get_tablet_id();
    vec_idx_param_ = vec_idx_param;
    vec_dim_ = vec_dim;
    // prepare in prepare_schema_item_on_demand -> prepare_schema_item_for_vec_idx_data
    for (int64_t i = 0; OB_SUCC(ret) && i < col_array.count(); i++) {
      if (ObSchemaUtils::is_vec_ivf_pq_center_id_column(col_array.at(i).column_flags_)) {
        pq_center_id_col_idx_ = i;
      } else if (ObSchemaUtils::is_vec_ivf_center_vector_column(col_array.at(i).column_flags_)) {
        pq_center_vector_col_idx_ = i;
      }
    }
    if (OB_SUCC(ret)) {
      ObIvfPqBuildHelper *helper = nullptr;
      if (pq_center_id_col_idx_ == -1 || pq_center_vector_col_idx_ == -1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get valid vector index col idx", K(ret), K(pq_center_id_col_idx_), K(pq_center_vector_col_idx_), K(col_array));
      } else {
        ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
        if (OB_ISNULL(vec_index_service) || OB_ISNULL(GCTX.ddl_sql_proxy_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null ObPluginVectorIndexService or GCTX.ddl_sql_proxy_ ptr", K(ret), K(MTL_ID()), KP(vec_index_service));
        } else if (OB_FAIL(vec_index_service->acquire_ivf_build_helper_guard(ls_id,
                                                                             tablet_id_,
                                                                             ObIndexType::INDEX_TYPE_VEC_IVFPQ_PQ_CENTROID_LOCAL,
                                                                             helper_guard_,
                                                                             vec_idx_param_))) {
          LOG_WARN("failed to acquire ivf build helper guard", K(ret), K(ls_id), K(tablet_id_));
        } else if (OB_FAIL(get_spec_ivf_helper(helper))) {
          LOG_WARN("fail to get ivf flat helper", K(ret));
        } else if (OB_FAIL(helper->init_ctx(vec_dim_))) {
          LOG_WARN("failed ot init kmeans ctx", K(ret), K_(vec_dim));
        } else {
          is_inited_ = true;
        }
      }
    }
  }
  return ret;
}

int ObIvfPqSliceStore::append_row(const blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObString residual_str;
    ObMultiKmeansExecutor *executor = nullptr;
    ObIvfPqBuildHelper *helper = nullptr;
    if (datum_row.get_column_count() <= pq_center_vector_col_idx_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get valid vector index col idx", K(ret), K(pq_center_vector_col_idx_), K(datum_row));
    } else if (datum_row.storage_datums_[pq_center_vector_col_idx_].is_null()) {
      // do nothing // ignore
    } else if (FALSE_IT(residual_str = datum_row.storage_datums_[pq_center_vector_col_idx_].get_string())) {
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator_,
                                                                  ObLongTextType,
                                                                  CS_TYPE_BINARY,
                                                                  true,
                                                                  residual_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(residual_str));
    } else if (OB_FAIL(get_spec_ivf_helper(helper))) {
      LOG_WARN("fail to get ivf flat helper", K(ret));
    } else if (OB_ISNULL(executor = helper->get_kmeans_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr ctx", K(ret));
    } else if (OB_FAIL(executor->append_sample_vector(reinterpret_cast<float*>(residual_str.ptr())))) {
      LOG_WARN("failed to append sample vector", K(ret));
    } else {
      LOG_DEBUG("[vec index debug] append sample vector", K(tablet_id_), K(residual_str));
    }
  }
  tmp_allocator_.reuse();
  return ret;
}

int ObIvfPqSliceStore::build_clusters()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObMultiKmeansExecutor *executor = nullptr;
    ObIvfPqBuildHelper *helper = nullptr;
    if (OB_FAIL(get_spec_ivf_helper(helper))) {
      LOG_WARN("fail to get ivf flat helper", K(ret));
    } else if (OB_ISNULL(executor = helper->get_kmeans_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr ctx", K(ret));
    } else if (OB_FAIL(executor->build())) {
      LOG_WARN("failed to build clusters", K(ret));
    }
  }
  return ret;
}

int ObIvfPqSliceStore::get_next_vector_data_row(
    const int64_t rowkey_cnt,
    const int64_t column_cnt,
    const int64_t snapshot_version,
    ObVectorIndexAlgorithmType index_type,
    blocksstable::ObDatumRow *&datum_row)
{
  UNUSED(index_type);
  int ret = OB_SUCCESS;
  tmp_allocator_.reuse();
  ObMultiKmeansExecutor *executor = nullptr;
  const int64_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int64_t request_cnt = column_cnt + extra_rowkey_cnt;
  ObIvfPqBuildHelper *helper = nullptr;
  if (OB_FAIL(get_spec_ivf_helper(helper))) {
    LOG_WARN("fail to get ivf flat helper", K(ret));
  } else if (OB_ISNULL(executor = helper->get_kmeans_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctx", K(ret));
  } else if (current_row_.get_column_count() <= 0
    && OB_FAIL(current_row_.init(vec_allocator_, request_cnt))) {
    LOG_WARN("init datum row failed", K(ret), K(request_cnt));
  } else if (OB_UNLIKELY(current_row_.get_column_count() != request_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(request_cnt), "datum_row_cnt", current_row_.get_column_count());
  } else if (cur_row_pos_ >= executor->get_total_centers_count()) {
    ret = OB_ITER_END;
  } else if (pq_center_id_col_idx_ < 0 || pq_center_id_col_idx_ >= current_row_.get_column_count() ||
             pq_center_vector_col_idx_ < 0 || pq_center_vector_col_idx_ >= current_row_.get_column_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, center col idx error", K(ret), K(pq_center_id_col_idx_), K(pq_center_vector_col_idx_));
  } else {
    ObString data_str;
    ObString vec_res;
    float *center_vector = nullptr;
    int64_t dim = executor->get_centers_dim();
    int64_t buf_len = OB_DOC_ID_COLUMN_BYTE_LENGTH;
    char *buf = nullptr;
    int64_t center_count_per_kmeans = executor->get_centers_count_per_kmeans();
    if (center_count_per_kmeans == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("upexpected zero center count", K(ret), K(center_count_per_kmeans));
    } else if (OB_ISNULL(center_vector = executor->get_center(cur_row_pos_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("upexpected nullptr center_vector", K(ret), K(cur_row_pos_), K(center_count_per_kmeans));
    } else {
      data_str.assign(reinterpret_cast<char *>(center_vector), static_cast<int64_t>(sizeof(float) * dim));
      if (OB_FAIL(ObArrayExprUtils::set_array_res(nullptr, data_str.length(), vec_allocator_, vec_res, data_str.ptr()))) {
        LOG_WARN("failed to set array res", K(ret));
      } else if (OB_ISNULL(buf = static_cast<char*>(vec_allocator_.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc cid", K(ret));
      } else {
        ObString pq_cid_str(buf_len, 0, buf);
        // row_i = pq_centers[m_id - 1][center_id - 1] since m_id and center_id start from 1
        ObPqCenterId pq_center_id(tablet_id_.id(), cur_row_pos_ / center_count_per_kmeans + 1, cur_row_pos_ % center_count_per_kmeans + 1);
        if (OB_FAIL(ObVectorClusterHelper::set_pq_center_id_to_string(pq_center_id, pq_cid_str))) {
          LOG_WARN("failed to set center_id to string", K(ret), K(pq_center_id), K(pq_cid_str));
        } else {
          for (int64_t i = 0; i < current_row_.get_column_count(); ++i) {
            if (pq_center_vector_col_idx_ == i) {
              current_row_.storage_datums_[i].set_string(vec_res);
            } else if (pq_center_id_col_idx_ == i) {
              current_row_.storage_datums_[i].set_string(pq_cid_str);
            } else if (rowkey_cnt == i) {
              current_row_.storage_datums_[i].set_int(-snapshot_version);
            } else if (rowkey_cnt + 1 == i) {
              current_row_.storage_datums_[i].set_int(0);
            } else {
              current_row_.storage_datums_[i].set_null(); // set part key null
            }
          }
          current_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
          datum_row = &current_row_;
          cur_row_pos_++;
        }
      }
    }
  }
  return ret;
}

int ObIvfPqSliceStore::is_empty(bool &empty)
{
  int ret = OB_SUCCESS;
  empty = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObKmeansExecutor *executor = nullptr;
    ObIvfFlatBuildHelper *helper = nullptr;
    if (OB_FAIL(get_spec_ivf_helper(helper))) {
      LOG_WARN("fail to get ivf flat helper", K(ret));
    } else if (OB_ISNULL(executor = helper->get_kmeans_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr ctx", K(ret));
    } else {
      empty = executor->is_empty();
    }
  }
  return ret;
}
