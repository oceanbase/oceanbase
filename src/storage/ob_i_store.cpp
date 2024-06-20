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

#include "ob_i_store.h"
#include "blocksstable/ob_storage_cache_suite.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/tablet/ob_tablet.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_dml_param.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_operator.h"
#include "storage/access/ob_dml_param.h"
#include "storage/ob_relative_table.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_table/ob_tx_table.h"
#include "blocksstable/ob_datum_row.h"
#include "storage/tx_storage/ob_ls_handle.h"  //ObLSHandle

namespace oceanbase
{
using namespace transaction;
using namespace share;
namespace common
{
OB_SERIALIZE_MEMBER(ObQueryFlag, flag_);
}

namespace storage
{
using namespace common;

int ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(ObColDescIArray &store_out_cols)
{
  int ret = OB_SUCCESS;
  share::schema::ObColDesc desc;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_extra_rowkey_col_cnt(); ++i) {
    const ObMultiVersionExtraRowkey &mv_ext_rowkey = OB_MULTI_VERSION_EXTRA_ROWKEY[i];
    desc.col_id_ = mv_ext_rowkey.column_index_;
    (desc.col_type_.*mv_ext_rowkey.set_meta_type_func_)();
    // by default the trans_version column value would be multiplied by -1
    // so in effect we store the latest version first
    desc.col_order_ = ObOrderType::ASC;
    if (OB_FAIL(store_out_cols.push_back(desc))) {
      STORAGE_LOG(WARN, "add store utput columns failed", K(ret));
    }
  }
  return ret;
}

void ObStoreCtx::reset()
{
  ls_id_.reset();
  ls_ = nullptr;
  branch_ = 0;
  tablet_id_.reset();
  table_iter_ = nullptr;
  table_version_ = INT64_MAX;
  timeout_ = 0;
  mvcc_acc_ctx_.reset();
  tablet_stat_.reset();
  is_read_store_ctx_ = false;
}

int ObStoreCtx::init_for_read(const ObLSID &ls_id,
                              const common::ObTabletID tablet_id,
                              const int64_t timeout,
                              const int64_t tx_lock_timeout,
                              const SCN &snapshot_version)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLSHandle ls_handle;
  if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    STORAGE_LOG(WARN, "get_ls from ls service fail.", K(ret), K(*ls_svr));
  } else {
    tablet_id_ = tablet_id;
    ret = init_for_read(ls_handle, timeout, tx_lock_timeout, snapshot_version);
  }
  return ret;
}

int ObStoreCtx::init_for_read(const ObLSHandle &ls_handle,
                              const int64_t timeout,
                              const int64_t tx_lock_timeout,
                              const SCN &snapshot_version)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObTxTableGuard tx_table_guard;
  if (!ls_handle.is_valid() || timeout < 0 || !snapshot_version.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid arguments", K(ret), K(ls_handle), K(timeout), K(tx_lock_timeout), K(snapshot_version));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls->get_tx_table_guard(tx_table_guard))) {
    STORAGE_LOG(WARN, "get_tx_table_guard from log stream fail.", K(ret), K(*ls));
  } else {
    ls_id_ = ls->get_ls_id();
    timeout_ = timeout;
    mvcc_acc_ctx_.init_read(tx_table_guard, snapshot_version, timeout, tx_lock_timeout);
  }
  return ret;
}

void ObStoreCtx::force_print_trace_log()
{
  if (NULL != mvcc_acc_ctx_.tx_desc_) {
    mvcc_acc_ctx_.tx_desc_->print_trace();
  }
  if (NULL != mvcc_acc_ctx_.tx_ctx_) {
    mvcc_acc_ctx_.tx_ctx_->print_trace_log();
  }
}

bool ObStoreCtx::is_uncommitted_data_rollbacked() const
{
  bool bret = false;
  if (NULL != mvcc_acc_ctx_.tx_ctx_) {
    bret = mvcc_acc_ctx_.tx_ctx_->is_data_rollbacked();
  }
  return bret;
}

void ObStoreRowLockState::reset()
{
  is_locked_ = false;
  trans_version_ = SCN::min_scn();
  lock_trans_id_.reset();
  lock_data_sequence_.reset();
  lock_dml_flag_ = blocksstable::ObDmlFlag::DF_NOT_EXIST;
  is_delayed_cleanout_ = false;
  exist_flag_ = ObExistFlag::UNKNOWN;
  mvcc_row_ = NULL;
  trans_scn_ = SCN::max_scn();
}

OB_DEF_SERIALIZE(ObStoreRow)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              row_val_,
              flag_,
              is_get_,
              scan_index_,
              from_base_,
              row_type_flag_.flag_,
              is_sparse_row_);
  if (OB_SUCC(ret) && is_sparse_row_) {
    if (OB_ISNULL(column_ids_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "sparse row's column id is null", K(ret));
    } else {
      OB_UNIS_ENCODE_ARRAY(column_ids_, row_val_.count_);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObStoreRow)
{
  int ret = OB_SUCCESS;
  int col_id_count = 0;
  LST_DO_CODE(OB_UNIS_DECODE,
              row_val_,
              flag_,
              is_get_,
              scan_index_,
              from_base_,
              row_type_flag_.flag_,
              is_sparse_row_);
  if (OB_SUCC(ret) && is_sparse_row_) {
    OB_UNIS_DECODE(col_id_count);
    if (OB_SUCC(ret)) {
      if (row_val_.count_ != col_id_count) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "column id count is not equal", K(ret), K(col_id_count),
            K(row_val_.count_));
      } else {
        OB_UNIS_DECODE_ARRAY(column_ids_, row_val_.count_);
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObStoreRow)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              row_val_,
              flag_,
              is_get_,
              scan_index_,
              from_base_,
              row_type_flag_.flag_,
              is_sparse_row_);
  if (is_sparse_row_) {
    OB_UNIS_ADD_LEN_ARRAY(column_ids_, row_val_.count_);
  }
  return len;
}

void ObStoreRow::reset()
{
  row_val_.reset();
  flag_.reset();
  capacity_ = 0;
  is_get_ = false;
  from_base_ = false;
  scan_index_ = 0;
  row_type_flag_.reset();
  is_sparse_row_ = false;
  column_ids_ = NULL;
  snapshot_version_ = 0;
  group_idx_ = 0;
  trans_id_.reset();
  fast_filter_skipped_ = false;
  last_purge_ts_ = 0;
}

int ObStoreRow::deep_copy(const ObStoreRow &src, char *buf, const int64_t len, int64_t &pos)
{
  int ret = common::OB_SUCCESS;

  if (NULL == buf || len <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(len), KP(buf));
  } else if (!src.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "src is not valid", K(ret));
  } else {
    flag_ = src.flag_;
    capacity_ = src.capacity_;
    is_get_ = src.is_get_;
    from_base_ = src.from_base_;
    scan_index_ = src.scan_index_;
    row_type_flag_ = src.row_type_flag_;
    is_sparse_row_ = src.is_sparse_row_;
    trans_id_ = src.trans_id_;
    fast_filter_skipped_ = src.fast_filter_skipped_;
    if (OB_FAIL(row_val_.deep_copy(src.row_val_, buf, len, pos))) {
      STORAGE_LOG(WARN, "failed to deep copy row_val", K(ret));
    }
  }
  return ret;
}

int64_t ObStoreRow::to_string(char *buffer, const int64_t length) const
{
  int64_t pos = 0;

  if (NULL != buffer && length >= 0) {

    pos += flag_.to_string(buffer + pos, length - pos);
    common::databuff_printf(buffer, length, pos, " capacity_=%ld ", capacity_);
    common::databuff_printf(buffer, length, pos, "is_get=%d ", is_get_);
    common::databuff_printf(buffer, length, pos, "from_base=%d ", from_base_);
    common::databuff_printf(buffer, length, pos, "trans_id=%ld ", trans_id_.hash());
    common::databuff_printf(buffer, length, pos, "scan_index=%ld ", scan_index_);
    common::databuff_printf(buffer, length, pos, "multi_version_row_flag=%d ", row_type_flag_.flag_);
    pos += row_type_flag_.to_string(buffer + pos, length - pos);
    common::databuff_printf(buffer, length, pos, " row_val={count=%ld,", row_val_.count_);
    common::databuff_printf(buffer, length, pos, "cells=[");
    if (NULL != row_val_.cells_) {
      for (int64_t i = 0; i < row_val_.count_; ++i) {
        common::databuff_print_obj(buffer, length, pos, row_val_.cells_[i]);
        common::databuff_printf(buffer, length, pos, ",");
      }
    }
    common::databuff_printf(buffer, length, pos, "] ");
    common::databuff_printf(buffer, length, pos, "is_sparse_row=%d ", is_sparse_row_);
    common::databuff_printf(buffer, length, pos, "snapshot_version=%ld ", snapshot_version_);
    common::databuff_printf(buffer, length, pos, "fast_filtered=%d ", fast_filter_skipped_);
    common::databuff_printf(buffer, length, pos, "group_idx_=%ld ", group_idx_);
    common::databuff_printf(buffer, length, pos, "last_purge_ts=%ld ", last_purge_ts_);
    if (is_sparse_row_ && NULL != column_ids_) {
      common::databuff_printf(buffer, length, pos, "column_id=[");
      for (int64_t i = 0; i < row_val_.count_; ++i) {
        common::databuff_printf(buffer, length, pos, "%u,", column_ids_[i]);
      }
      common::databuff_printf(buffer, length, pos, "] ");
    }
  }
  return pos;
}

int ObLockRowChecker::check_lock_row_valid(
    const ObDatumRow &row,
    const int64_t rowkey_cnt,
    bool is_memtable_iter_row_check)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row.get_column_count() < rowkey_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "row count is less than rowkey count", K(row), K(rowkey_cnt));
  } else if (row.is_uncommitted_row() || is_memtable_iter_row_check) {
    bool pure_empty_row = true;
    for (int i = rowkey_cnt; pure_empty_row && i < row.get_column_count(); ++i) {
      if (!row.storage_datums_[i].is_nop()) { // not nop value
        pure_empty_row = false;
      }
    }
    if (row.is_uncommitted_row()) {
      if (!pure_empty_row) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "uncommitted lock row have normal cells", K(ret), K(row), K(rowkey_cnt));
      }
    } else if (is_memtable_iter_row_check && pure_empty_row) { // a pure lock committed row from memtable
      // a pure empty lock row from upgrade sstable need to be compatible
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "a committed lock row only have rowkey", K(ret), K(row), K(rowkey_cnt));
    }
  }
  return ret;
}

int ObLockRowChecker::check_lock_row_valid(const blocksstable::ObDatumRow &row, const ObITableReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_read_cnt = MIN(read_info.get_seq_read_column_count(), read_info.get_rowkey_count());
  if (OB_UNLIKELY(!read_info.is_valid() || row.get_column_count() < rowkey_read_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_info), K(row));
  } else if (row.is_uncommitted_row()) {
    const ObColumnIndexArray &col_index = read_info.get_columns_index();
    for (int i = rowkey_read_cnt; i < row.get_column_count(); ++i) {
      if (col_index.at(i) < read_info.get_rowkey_count()) {
        // not checking rowkey col
      } else if (!row.storage_datums_[i].is_nop()) { // not nop value
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "uncommitted lock row have normal cells", K(ret), K(row),
          K(rowkey_read_cnt), K(read_info));
        break;
      }
    }
  }
  return ret;
}

}
}
