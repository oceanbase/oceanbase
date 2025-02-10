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

#include "ob_lob_tablet_dml.h"
#include "share/schema/ob_table_dml_param.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/ob_lob_locator_struct.h"
#include "storage/ob_dml_running_ctx.h"
#include "storage/memtable/ob_memtable_context.h"
#include "share/schema/ob_table_dml_param.h"

namespace oceanbase
{
using namespace common;
namespace storage
{

ObLobTabletDmlCtx::~ObLobTabletDmlCtx()
{
}

int ObLobTabletDmlHelper::build_common_lob_param_for_dml(
    ObDMLRunningCtx &run_ctx,
    const blocksstable::ObDatumRow &data_row,
    const int16_t col_idx,
    ObString &disk_lob_locator,
    ObLobAccessParam &lob_param)
{
  int ret = OB_SUCCESS;
  const ObColDesc &column = run_ctx.col_descs_->at(col_idx);
  ObLobCommon *lob_common = reinterpret_cast<ObLobCommon*>(disk_lob_locator.ptr());
  if (OB_NOT_NULL(lob_common)) {
    lob_param.lob_common_ = lob_common;
    lob_param.handle_size_ = disk_lob_locator.length();
    lob_param.byte_size_ = lob_common->get_byte_size(disk_lob_locator.length());
  }

  lob_param.tx_desc_ = run_ctx.store_ctx_.mvcc_acc_ctx_.tx_desc_;
  lob_param.parent_seq_no_ = run_ctx.store_ctx_.mvcc_acc_ctx_.tx_scn_;
  lob_param.tx_id_ = lob_param.tx_desc_->get_tx_id();
  lob_param.is_mlog_ = run_ctx.dml_param_.table_param_->get_data_table().is_mlog_table();

  lob_param.sql_mode_ = run_ctx.dml_param_.sql_mode_;
  lob_param.is_total_quantity_log_ = run_ctx.dml_param_.is_total_quantity_log_;
  lob_param.ls_id_ = run_ctx.store_ctx_.ls_id_;
  lob_param.tablet_id_ = run_ctx.relative_table_.get_tablet_id();
  lob_param.lob_meta_tablet_id_ = run_ctx.lob_dml_ctx_.lob_meta_tablet_id_;
  lob_param.lob_piece_tablet_id_ = run_ctx.lob_dml_ctx_.lob_piece_tablet_id_;
  lob_param.coll_type_ = ObLobCharsetUtil::get_collation_type(column.col_type_.get_type(), column.col_type_.get_collation_type());
  lob_param.allocator_ = &run_ctx.dml_param_.lob_allocator_;
  lob_param.timeout_ = run_ctx.dml_param_.timeout_;
  lob_param.scan_backward_ = false;
  lob_param.offset_ = 0;
  lob_param.data_row_ = &data_row;
  lob_param.is_index_table_ = run_ctx.relative_table_.is_index_table();
  lob_param.main_table_rowkey_col_ = run_ctx.is_main_table_rowkey_col(col_idx) ||
    (!run_ctx.relative_table_.is_index_table() && col_idx < run_ctx.relative_table_.get_rowkey_column_num());
  if (OB_FAIL(set_lob_storage_params(run_ctx, column, lob_param))) {
    LOG_WARN("set_lob_storage_params fail", K(ret), K(column));
  } else if (OB_FAIL(lob_param.snapshot_.assign(run_ctx.dml_param_.snapshot_))) {
    LOG_WARN("assign snapshot fail", K(ret), K(run_ctx.dml_param_.snapshot_));
  } else if (lob_param.snapshot_.is_none_read()) {
    // NOTE:
    // lob_insert need table_scan, the snapshot already generated in
    // run_ctx.store_ctx, use it as an LS ReadSnapshot
    lob_param.snapshot_.init_ls_read(run_ctx.store_ctx_.ls_id_,
                                      run_ctx.store_ctx_.mvcc_acc_ctx_.snapshot_);
  }
  return ret;
}

int ObLobTabletDmlHelper::process_lob_column_before_insert(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow &row,
    const int16_t row_idx,
    const int16_t col_idx,
    blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  ObString old_disk_locator;
  bool need_do_write = false;
  if (OB_FAIL(prepare_lob_write(run_ctx, row, row_idx, col_idx, old_disk_locator, datum, need_do_write))) {
    LOG_WARN("prepare_lob_write fail", K(ret), K(row_idx), K(col_idx), K(datum), K(row));
  } else if (! need_do_write) {
  } else if(OB_FAIL(insert_lob_col(run_ctx, row, col_idx, datum, nullptr, old_disk_locator/*empty data*/))) {
    LOG_WARN("insert_lob_col fail", K(ret), K(row_idx), K(col_idx), K(datum), K(row));
  }
  return ret;
}

int ObLobTabletDmlHelper::process_lob_column_after_insert(
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow &row,
    ObLobDataInsertTask &info)
{
  int ret = OB_SUCCESS;
  blocksstable::ObStorageDatum datum;
  const ObColDesc &column = run_ctx.col_descs_->at(info.col_idx_);
  datum.set_string(info.src_data_locator_.ptr_, info.src_data_locator_.size_);
  if (info.src_data_locator_.has_lob_header_)datum.set_has_lob_header();

  ObString locator_data(info.cur_data_locator_.size_, info.cur_data_locator_.ptr_);
  // dup locator
  char buf[ObLobManager::LOB_OUTROW_FULL_SIZE];
  MEMCPY(static_cast<char*>(buf), info.cur_data_locator_.ptr_, ObLobManager::LOB_OUTROW_FULL_SIZE);
  ObString dup_locator_data(ObLobManager::LOB_OUTROW_FULL_SIZE, buf);

  ObLobDiskLocatorWrapper lob_disk_locator;
  ObLobAccessParam del_param;
  if (OB_FAIL(lob_disk_locator.init(info.cur_data_locator_.ptr_, info.cur_data_locator_.size_))) {
    LOG_WARN("init disk locator fail", K(ret), K(info));
  } else if (! lob_disk_locator.is_ext_info_log()) {
    del_param.seq_no_st_ = lob_disk_locator.get_seq_no_st();
    del_param.total_seq_cnt_ = lob_disk_locator.get_seq_no_cnt();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(lob_disk_locator.reset_for_dml())) {
    LOG_WARN("reset_for_dml fail", K(ret), K(lob_disk_locator));
  } else if(OB_FAIL(insert_lob_col(run_ctx, row, info.col_idx_, datum, &del_param, locator_data, &info.lob_meta_list_, true/*try_flush_redo*/))) {
    LOG_WARN("insert_lob_col fail", K(ret), K(column), K(datum), K(info), K(row));
  } else if (datum.get_string().ptr() != locator_data.ptr() || datum.get_string().length() != locator_data.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob locator memory is changed", K(ret), KP(datum.get_string().ptr()), K(datum.get_string().length()), KP(locator_data.ptr()), K(locator_data.length()));
  } else if (OB_FAIL(register_ext_info_commit_cb(run_ctx, column, dup_locator_data, locator_data))) {
    LOG_WARN("register_ext_info_commit_cb fail", K(ret), K(column), K(datum), K(info));
  }
  return ret;
}

int ObLobTabletDmlHelper::process_lob_column_before_update(
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow &old_row,
    blocksstable::ObDatumRow &new_row,
    const bool data_tbl_rowkey_change,
    const int16_t row_idx,
    const int16_t col_idx,
    blocksstable::ObStorageDatum &old_datum,
    blocksstable::ObStorageDatum &new_datum)
{
  int ret = OB_SUCCESS;
  bool need_do_write = false;
  ObString old_disk_locator = (old_datum.is_null() || old_datum.is_nop_value()) ? ObString() :  old_datum.get_string();
  if (OB_FAIL(ObLobTabletDmlHelper::prepare_lob_write(run_ctx, new_row, row_idx, col_idx, old_disk_locator, new_datum, need_do_write))) {
    LOG_WARN("prepare_lob_write fail", K(ret), K(row_idx), K(col_idx), K(new_datum));
  } else if (! need_do_write) {
  } else if (OB_FAIL(ObLobTabletDmlHelper::update_lob_col(run_ctx, old_row, new_row, data_tbl_rowkey_change, col_idx, old_datum, new_datum))) {
    LOG_WARN("[STORAGE_LOB]failed to to update lob col", K(ret), K(row_idx), K(col_idx), K(new_datum));
  }
  return ret;
}

int ObLobTabletDmlHelper::process_lob_column_after_update(
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow &old_row,
    blocksstable::ObDatumRow &new_row,
    const bool data_tbl_rowkey_change,
    ObLobDataInsertTask &info)
{
  int ret = OB_SUCCESS;
  blocksstable::ObStorageDatum new_datum;
  const ObColDesc &column = run_ctx.col_descs_->at(info.col_idx_);
  new_datum.set_string(info.src_data_locator_.ptr_, info.src_data_locator_.size_);
  if (info.src_data_locator_.has_lob_header_)new_datum.set_has_lob_header();

  blocksstable::ObStorageDatum &old_datum = old_row.storage_datums_[info.col_idx_];

  ObString locator_data(info.cur_data_locator_.size_, info.cur_data_locator_.ptr_);
  // dup locator
  char buf[ObLobManager::LOB_OUTROW_FULL_SIZE];
  MEMCPY(static_cast<char*>(buf), info.cur_data_locator_.ptr_, ObLobManager::LOB_OUTROW_FULL_SIZE);
  ObString dup_locator_data(ObLobManager::LOB_OUTROW_FULL_SIZE, buf);

  ObLobAccessParam lob_param;
  ObLobCommon *old_lob_common = nullptr;
  ObLobCommon *new_lob_common = nullptr;

  ObLobDiskLocatorWrapper lob_disk_locator;
  bool use_seq_pre_alloc = false;
  if (OB_FAIL(info.src_data_locator_.get_lob_data_byte_len(lob_param.update_len_))) {
    LOG_WARN("fail to get new lob byte len", K(ret), K(info));
  } else if (OB_FAIL(delete_lob_col(run_ctx, old_row, info.col_idx_, old_datum, old_lob_common, lob_param, true/*try_flush_redo*/))) {
    LOG_WARN("[STORAGE_LOB]failed to erase old lob col", K(ret), K(old_row));
  } else if (OB_FAIL(lob_disk_locator.init(info.cur_data_locator_.ptr_, info.cur_data_locator_.size_))) {
    LOG_WARN("init disk locator fail", K(ret), K(info));
  } else if (! lob_disk_locator.is_ext_info_log() && lob_param.seq_no_st_.is_valid() && lob_param.used_seq_cnt_ > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should be ext info log", K(ret), K(info), K(lob_disk_locator), K(lob_param));
  } else if (! lob_disk_locator.is_ext_info_log()) {
    lob_param.seq_no_st_ = lob_disk_locator.get_seq_no_st();
    lob_param.total_seq_cnt_ = lob_disk_locator.get_seq_no_cnt();
    use_seq_pre_alloc = true;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(lob_disk_locator.reset_for_dml())) {
    LOG_WARN("reset_for_dml fail", K(ret), K(lob_disk_locator));
  } else if (OB_ISNULL(new_lob_common = lob_disk_locator.get_lob_common())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new lob common is null", K(ret), K(lob_disk_locator));
  } else if (data_tbl_rowkey_change) {
    // need lob_param when use pre alloc seq no
    if (OB_FAIL(insert_lob_col(run_ctx, new_row, info.col_idx_, new_datum,
        use_seq_pre_alloc ? &lob_param : nullptr, locator_data, &info.lob_meta_list_, true/*try_flush_redo*/))) {
      LOG_WARN("[STORAGE_LOB]failed to insert new lob col.", K(ret), K(new_row));
    }
  } else if (OB_FAIL(insert_lob_col(run_ctx, new_row, info.col_idx_, new_datum, &lob_param, locator_data, &info.lob_meta_list_, true/*try_flush_redo*/))) {
    LOG_WARN("[STORAGE_LOB]failed to insert new lob col.", K(ret), K(new_row));
  }

  if (OB_FAIL(ret)) {
  } else if (new_datum.get_string().ptr() != locator_data.ptr() || new_datum.get_string().length() != locator_data.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob locator memory is changed", K(ret), KP(new_datum.get_string().ptr()), K(new_datum.get_string().length()), KP(locator_data.ptr()), K(locator_data.length()));
  } else if (OB_FAIL(register_ext_info_commit_cb(run_ctx, column, dup_locator_data, locator_data))) {
    LOG_WARN("register_ext_info_commit_cb fail", K(ret), K(column), K(new_datum), K(info));
  }
  return ret;
}

int ObLobTabletDmlHelper::insert_lob_col(
    ObDMLRunningCtx &run_ctx,
    const blocksstable::ObDatumRow &data_row,
    const int16_t col_idx,
    blocksstable::ObStorageDatum &datum,
    ObLobAccessParam *del_param,
    ObString &disk_locator_data,
    ObArray<ObLobMetaInfo> *lob_meta_list,
    const bool try_flush_redo)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  ObLobAccessParam lob_param;
  const ObColDesc &column = run_ctx.col_descs_->at(col_idx);
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB]failed to get lob manager handle.", K(ret));
  } else if (!column.col_type_.is_lob_storage() || datum.is_nop_value() || datum.is_null()) {
    // do nothing
  } else if (OB_FAIL(build_common_lob_param_for_dml(run_ctx, data_row, col_idx, disk_locator_data, lob_param))) {
    LOG_WARN("build_common_lob_param_for_dml fail", K(ret), K(col_idx), K(column));
  } else {
    if (OB_NOT_NULL(del_param)) { // for obcdc lob
      lob_param.total_seq_cnt_ = del_param->total_seq_cnt_;
      lob_param.used_seq_cnt_ = del_param->used_seq_cnt_;
      lob_param.seq_no_st_ = del_param->seq_no_st_;
    }
    lob_param.try_flush_redo_ = try_flush_redo;
    ObString raw_data = datum.get_string();
    // for not strict sql mode, will insert empty string without lob header
    bool has_lob_header = datum.has_lob_header() && raw_data.length() > 0;
    ObLobLocatorV2 loc(raw_data, has_lob_header);
    if (nullptr != lob_meta_list && lob_meta_list->count() > 0 ) {
      if (OB_FAIL(lob_mngr->insert(lob_param, loc, *lob_meta_list))) {
        LOG_WARN("[STORAGE_LOB]lob insert failed.", K(ret));
      } else {
        datum.set_lob_data(*lob_param.lob_common_, lob_param.handle_size_);
        LOG_DEBUG("[STORAGE_LOB]write ob lob data.", K(lob_param), KPC(lob_param.lob_common_),
                  K(lob_param.handle_size_), K(column.col_type_.get_collation_type()));
      }
    } else if (OB_FAIL(lob_mngr->append(lob_param, loc))) {
      LOG_WARN("[STORAGE_LOB]lob append failed.", K(ret));
    } else {
      datum.set_lob_data(*lob_param.lob_common_, lob_param.handle_size_);
      LOG_DEBUG("[STORAGE_LOB]write ob lob data.", K(lob_param), KPC(lob_param.lob_common_),
                K(lob_param.handle_size_), K(column.col_type_.get_collation_type()));
    }
    if (OB_SUCC(ret) && has_lob_header) {
      datum.set_has_lob_header();
    }
  }
  return ret;
}

int ObLobTabletDmlHelper::delete_lob_col(
    ObDMLRunningCtx &run_ctx,
    const blocksstable::ObDatumRow &data_row,
    const int16_t col_idx,
    blocksstable::ObStorageDatum &datum,
    ObLobCommon *&lob_common,
    ObLobAccessParam &lob_param,
    const bool try_flush_redo)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  const ObColDesc &column = run_ctx.col_descs_->at(col_idx);
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB]get lob manager instance failed.", K(ret));
  } else if (!column.col_type_.is_lob_storage() || datum.is_nop_value() || datum.is_null() ||
             !run_ctx.is_old_row_valid_for_lob_) {
    // do nothing
  } else {
    ObString data = datum.get_string();
    ObLobLocatorV2 locator(data, datum.has_lob_header());
    char *buf = nullptr;
    if (data.length() < sizeof(ObLobCommon)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[STORAGE_LOB]Invalid Lob data.", K(ret), K(datum), K(data));
    } else if (locator.is_inrow()) {
      // delete inrow lob no need to use the lob manager
    } else if (OB_ISNULL(buf = static_cast<char*>(run_ctx.dml_param_.lob_allocator_.alloc(data.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to deep copy lob data.", K(ret), K(data));
    } else {
      MEMCPY(buf, data.ptr(), data.length());
      lob_common = reinterpret_cast<ObLobCommon*>(buf);
      ObString dup_data = ObString(data.length(), buf);
      if (OB_FAIL(build_common_lob_param_for_dml(run_ctx, data_row, col_idx, dup_data, lob_param))) {
        LOG_WARN("build_common_lob_param_for_dml fail", K(ret), K(col_idx), K(datum), K(data));
      } else if (lob_param.byte_size_ < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("calc byte size is negative.", K(ret), K(data), K(lob_param));
      // use byte size to delete all
      } else if (OB_FALSE_IT(lob_param.len_ = lob_param.byte_size_)) {
      } else if (OB_FALSE_IT(lob_param.need_read_latest_ = true)) {
      } else if (OB_FALSE_IT(lob_param.try_flush_redo_ = try_flush_redo)) {
      } else if (OB_FAIL(lob_mngr->erase(lob_param))) {
        LOG_WARN("[STORAGE_LOB]lob erase failed.", K(ret), K(lob_param));
      }
    }
  }
  return ret;
}

int ObLobTabletDmlHelper::update_lob_col(
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow &old_row,
    blocksstable::ObDatumRow &new_row,
    const bool data_tbl_rowkey_change,
    const int16_t col_idx,
    blocksstable::ObStorageDatum &old_datum,
    blocksstable::ObStorageDatum &new_datum)
{
  int ret = OB_SUCCESS;
  ObLobAccessParam lob_param;
  ObLobCommon *lob_common = nullptr;
  ObString new_lob_str = (new_datum.is_null() || new_datum.is_nop_value())
                          ? ObString(0, nullptr) : new_datum.get_string();
  ObLobLocatorV2 new_lob(new_lob_str, new_datum.has_lob_header());
  ObString disk_locator_data;
  if (OB_FAIL(new_lob.get_lob_data_byte_len(lob_param.update_len_))) {
    LOG_WARN("fail to get new lob byte len", K(ret), K(new_lob));
  } else if (OB_FAIL(delete_lob_col(run_ctx, old_row, col_idx, old_datum, lob_common, lob_param))) {
    LOG_WARN("[STORAGE_LOB]failed to erase old lob col", K(ret), K(old_row));
  } else if (data_tbl_rowkey_change) {
    if (OB_FAIL(insert_lob_col(run_ctx, new_row, col_idx, new_datum, nullptr, disk_locator_data/*empty data*/))) { // no need del_param
      LOG_WARN("[STORAGE_LOB]failed to insert new lob col.", K(ret), K(new_row));
    }
  } else if (OB_FALSE_IT(disk_locator_data.assign_ptr(reinterpret_cast<char*>(lob_common), lob_param.handle_size_))) {
  } else if (OB_FAIL(insert_lob_col(run_ctx, new_row, col_idx, new_datum, &lob_param, disk_locator_data))) {
    LOG_WARN("[STORAGE_LOB]failed to insert new lob col.", K(ret), K(new_row));
  }
  return ret;
}

int ObLobTabletDmlHelper::process_delta_lob(
    ObDMLRunningCtx &run_ctx,
    const blocksstable::ObDatumRow &data_row,
    const int16_t col_idx,
    blocksstable::ObStorageDatum &old_datum,
    ObLobLocatorV2 &delta_lob,
    blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  const ObColDesc &column = run_ctx.col_descs_->at(col_idx);
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB]failed to get lob manager handle.", K(ret));
  } else if (!delta_lob.is_delta_temp_lob()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB] invalid lob type", K(ret), K(delta_lob));
  } else {
    ObLobAccessParam lob_param;
    // should use old datum
    ObLobLocatorV2 old_lob;
    ObString old_disk_lob;
    old_datum.get_mem_lob(old_lob);
    if (!old_lob.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("old lob locator is invalid.", K(ret));
    } else if (OB_FAIL(old_lob.get_disk_locator(old_disk_lob))) {
      LOG_WARN("fail to get old lob disk locator.", K(ret));
    // TODO copy old
    } else if (OB_FAIL(build_common_lob_param_for_dml(run_ctx, data_row, col_idx, old_disk_lob, lob_param))) {
      LOG_WARN("build_common_lob_param_for_dml fail", K(ret), K(col_idx), K(datum), K(old_disk_lob));
    } else if (OB_FAIL(lob_mngr->process_delta(lob_param, delta_lob))) {
      LOG_WARN("failed to process delta lob.", K(ret), K(lob_param), K(delta_lob));
    } else {
      // update datum with new disk locator
      datum.set_lob_data(*lob_param.lob_common_, lob_param.handle_size_);
      if (! lob_param.ext_info_log_.is_null()
        && OB_FAIL(register_ext_info_commit_cb(run_ctx, column, datum, lob_param.ext_info_log_))) {
        LOG_WARN("register_ext_info_commit_cb fail", K(ret), K(lob_param));
      }
    }
  }
  return ret;
}

int ObLobTabletDmlHelper::prepare_lob_write(
    ObDMLRunningCtx &run_ctx,
    const blocksstable::ObDatumRow &data_row,
    const int16_t row_idx,
    const int16_t col_idx,
    ObString &old_disk_locator,
    blocksstable::ObStorageDatum &new_datum,
    bool &need_do_write)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (is_sys_table(run_ctx.relative_table_.get_table_id())) {
    // sys table just write
    need_do_write = true;
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (data_version < DATA_VERSION_4_3_4_0) {
    need_do_write = true;
    LOG_DEBUG("before 4.3.4", K(data_version), K(row_idx), K(col_idx));
  } else {
    ObString raw_data = (new_datum.is_null() || new_datum.is_nop_value())
                            ? ObString(0, nullptr) : new_datum.get_string();
    bool has_lob_header = new_datum.has_lob_header() && raw_data.length() > 0;
    ObLobLocatorV2 src_data_locator(raw_data, has_lob_header);
    bool is_outrow = false;
    ObLobAccessParam lob_param;
    ObLobDataInsertTask info;
    ObLobManager *lob_mngr = MTL(ObLobManager*);
    info.src_data_locator_ = src_data_locator;
    bool skip_task = run_ctx.relative_table_.is_index_table() || col_idx < run_ctx.relative_table_.get_rowkey_column_num();
    if (OB_FAIL(build_common_lob_param_for_dml(run_ctx, data_row, col_idx, old_disk_locator, lob_param))) {
      LOG_WARN("build_common_lob_param_for_dml fail", K(ret), K(col_idx), K(src_data_locator));
    } else if (!skip_task && OB_FAIL(lob_mngr->prepare_insert_task(lob_param, is_outrow, info))) {
      LOG_WARN("prepare_insert_task fail", K(ret), K(src_data_locator));
    } else if (is_outrow) {
      if (lob_param.lob_meta_tablet_id_.is_valid() && lob_param.lob_piece_tablet_id_.is_valid()) {
        run_ctx.lob_dml_ctx_.lob_meta_tablet_id_ = lob_param.lob_meta_tablet_id_;
        run_ctx.lob_dml_ctx_.lob_piece_tablet_id_ = lob_param.lob_piece_tablet_id_;
      }
      // outrow sholud insert after row
      // here only construct outrow lob locator
      info.col_idx_ = col_idx;
      info.row_idx_ = row_idx;
      if (OB_FAIL(run_ctx.lob_dml_ctx_.insert_data_info_.push_back(info))) {
        LOG_WARN("push back fail", K(ret));
      } else {
        new_datum.set_string(info.cur_data_locator_.ptr_, info.cur_data_locator_.size_);
        need_do_write = false;
      }
    } else {
      need_do_write = true;
    }
  }
  return ret;
}

int ObLobTabletDmlHelper::register_ext_info_commit_cb(
    ObDMLRunningCtx &run_ctx,
    const ObColDesc &column,
    ObDatum &col_data,
    ObObj &ext_info_data)
{
  int ret = OB_SUCCESS;
  ObLobDiskLocatorWrapper lob_disk_locator;
  ObLobDataOutRowCtx *lob_data_outrow_ctx = nullptr;
  transaction::ObTxSEQ seq_no_st;
  int64_t seq_no_cnt = 0;
  ObExtInfoLogHeader header(OB_JSON_DIFF_EXT_INFO_LOG);
  const int64_t data_size = ext_info_data.get_string_len() + header.get_serialize_size();
  memtable::ObMvccWriteGuard guard(false);
  if (ext_info_data.is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ext_info_log is null", K(ret), K(ext_info_data));
  } else if (OB_FAIL(lob_disk_locator.init(const_cast<char*>(col_data.get_string().ptr()), col_data.get_string().length()))) {
    LOG_WARN("init disk locator fail", K(ret), K(col_data));
  } else if (OB_ISNULL(lob_data_outrow_ctx = lob_disk_locator.get_outrow_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob_data_outrow_ctx is null", K(ret), K(lob_disk_locator));
  } else if (OB_FAIL(ObExtInfoCbRegister::alloc_seq_no(
      run_ctx.store_ctx_.mvcc_acc_ctx_.tx_desc_, run_ctx.store_ctx_.mvcc_acc_ctx_.tx_scn_,
      data_size, seq_no_st, seq_no_cnt))) {
    LOG_WARN("alloc_seq_no fail", K(ret));
  } else if (OB_FAIL(guard.write_auth(run_ctx.store_ctx_))) {
    LOG_WARN("write_auth fail", K(ret), K(run_ctx.store_ctx_));
  } else if (OB_FAIL(run_ctx.store_ctx_.mvcc_acc_ctx_.mem_ctx_->register_ext_info_commit_cb(
      run_ctx.dml_param_.timeout_, run_ctx.dml_flag_,
      seq_no_st, seq_no_cnt, col_data.get_string(), column.col_type_.get_type(), header, ext_info_data))) {
    LOG_WARN("register_ext_info_commit_cb fail", K(ret), K(run_ctx.store_ctx_), K(col_data), K(ext_info_data));
  } else {
    lob_data_outrow_ctx->seq_no_st_ = seq_no_st.cast_to_int();
    lob_data_outrow_ctx->seq_no_cnt_ = seq_no_cnt;
    lob_data_outrow_ctx->modified_len_ = data_size;
  }
  return ret;
}

int ObLobTabletDmlHelper::register_ext_info_commit_cb(
    ObDMLRunningCtx &run_ctx,
    const ObColDesc &column,
    ObString &index_data,
    ObString &data)
{
  int ret = OB_SUCCESS;
  ObDatum index_datum;
  index_datum.set_string(index_data);
  ObObj ext_info_data;
  ext_info_data.set_raw(data);
  ObLobDiskLocatorWrapper index_lob_disk_locator;
  ObLobDiskLocatorWrapper data_lob_disk_locator;
  transaction::ObTxSEQ seq_no_st;
  int64_t seq_no_cnt = 0;
  ObExtInfoLogHeader header(OB_OUTROW_DISK_LOB_LOCATOR_EXT_INFO_LOG);
  memtable::ObMvccWriteGuard guard(false);
  if (OB_FAIL(index_lob_disk_locator.init(index_data.ptr(), index_data.length()))) {
    LOG_WARN("init disk locator fail", K(ret), K(index_data), K(column));
  } else if (OB_FAIL(data_lob_disk_locator.init(data.ptr(), data.length()))) {
    LOG_WARN("init disk locator fail", K(ret), K(data), K(column));
  } else if (OB_FAIL(data_lob_disk_locator.check_for_dml(index_lob_disk_locator))) {
    LOG_WARN("check_for_dml fail", K(ret), K(data_lob_disk_locator), K(index_lob_disk_locator));
  } else if (! index_lob_disk_locator.is_ext_info_log()) {
    LOG_DEBUG("not ext info log", K(index_lob_disk_locator));
  } else if (OB_FALSE_IT(seq_no_st = index_lob_disk_locator.get_seq_no_st())) {
  } else if (OB_FALSE_IT(seq_no_cnt = index_lob_disk_locator.get_seq_no_cnt())) {
  } else if (OB_FAIL(guard.write_auth(run_ctx.store_ctx_))) {
    LOG_WARN("write_auth fail", K(ret), K(run_ctx.store_ctx_));
  } else if (OB_FAIL(run_ctx.store_ctx_.mvcc_acc_ctx_.mem_ctx_->register_ext_info_commit_cb(
      run_ctx.dml_param_.timeout_,
      run_ctx.dml_flag_,
      seq_no_st,
      seq_no_cnt,
      index_data,
      column.col_type_.get_type(),
      header,
      ext_info_data))) {
    LOG_WARN("register_ext_info_commit_cb fail", K(ret), K(run_ctx.store_ctx_), K(seq_no_st), K(seq_no_cnt), K(index_datum), K(ext_info_data));
  }
  return ret;
}

int ObLobTabletDmlHelper::set_lob_storage_params(
    ObDMLRunningCtx &run_ctx,
    const ObColDesc &column,
    ObLobAccessParam &lob_param)
{
  int ret = OB_SUCCESS;
  const ObTableDMLParam *table_param = run_ctx.dml_param_.table_param_;
  const ObColumnParam *column_param = nullptr;
  if (OB_ISNULL(table_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_param is null", K(ret));
  } else if (OB_ISNULL(column_param = table_param->get_data_table().get_column(column.col_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_param is null", K(ret), K(table_param));
  } else {
    lob_param.inrow_threshold_ = table_param->get_data_table().get_lob_inrow_threshold();
    lob_param.schema_chunk_size_ = column_param->get_lob_chunk_size();
  }
  return ret;
}


}  // end namespace storage
}  // end namespace oceanbase
