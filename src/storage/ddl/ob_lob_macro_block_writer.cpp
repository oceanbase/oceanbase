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

#include "storage/ddl/ob_lob_macro_block_writer.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/lob/ob_lob_access_param.h"
#include "storage/ddl/ob_cg_macro_block_writer.h"
#include "share/ob_ddl_common.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::sql;

ObLobMacroBlockWriter::ObLobMacroBlockWriter()
  : is_inited_(false), lob_column_count_(0),
    lob_arena_("lob_meta_iter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    meta_write_iter_(&lob_arena_, ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE),
    macro_block_writer_(nullptr), total_lob_cell_count_(0), inrow_lob_cell_count_(0)
{
  lob_id_cache_.set(1/*start*/, 0/*end*/); // to calculate remain count correctly
}

ObLobMacroBlockWriter::~ObLobMacroBlockWriter()
{
  if (nullptr != macro_block_writer_) {
    macro_block_writer_->~ObCgMacroBlockWriter();
    ob_free(macro_block_writer_);
    macro_block_writer_ = nullptr;
  }
}

int ObLobMacroBlockWriter::init(const ObWriteMacroParam &param,
                                const ObTabletID &data_tablet_id,
                                const blocksstable::ObMacroDataSeq &start_sequence)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(
        !param.is_valid()
        || !data_tablet_id.is_valid()
        || !start_sequence.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param), K(data_tablet_id), K(start_sequence));
  } else {
    lob_meta_tablet_id_ = param.lob_meta_tablet_id_;
    slice_idx_ = param.slice_idx_;
    macro_seq_ = start_sequence;
    lob_column_count_ = param.ddl_table_schema_.lob_column_idxs_.count();
    lob_id_cache_.tablet_id_ = lob_meta_tablet_id_;
    lob_inrow_threshold_ = param.ddl_table_schema_.table_item_.lob_inrow_threshold_;
    ls_id_ = param.ls_id_;
    tablet_id_ = param.tablet_id_;
    param_ = param;
    // init lob id generator if need
    if (OB_SUCC(ret) && param.slice_count_ > 0) {
      if (slice_idx_ >= param.slice_count_) { // idem mode, for table with primary key
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid slice idx", K(ret), K(slice_idx_), K(param));
      } else if (OB_FAIL(lob_id_generator_.init(slice_idx_ * ObTabletSliceParam::LOB_ID_SEQ_INTERVAL, // start
                                                ObTabletSliceParam::LOB_ID_SEQ_INTERVAL, // interval
                                                param.slice_count_ * ObTabletSliceParam::LOB_ID_SEQ_INTERVAL))) { // step
        LOG_WARN("init lob id generator failed", K(param));
      }
    }

    // init lob meta row
    if (OB_SUCC(ret)) {
      if (OB_FAIL(lob_meta_row_.init(ObLobMetaUtil::LOB_META_COLUMN_CNT + ObLobMetaUtil::SKIP_INVALID_COLUMN))) {
        LOG_WARN("init lob meta row failed", K(ret));
      } else {
        lob_meta_row_.storage_datums_[ObLobMetaUtil::SEQ_ID_COL_ID + 1].set_int(-param.snapshot_version_);
        lob_meta_row_.storage_datums_[ObLobMetaUtil::SEQ_ID_COL_ID + 2].set_int(-param_.tx_info_.seq_no_);
        lob_meta_row_.set_trans_id(param_.tx_info_.trans_id_);
        lob_meta_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
        lob_meta_row_.mvcc_row_flag_.set_compacted_multi_version_row(true);
        lob_meta_row_.mvcc_row_flag_.set_first_multi_version_row(true);
        lob_meta_row_.mvcc_row_flag_.set_last_multi_version_row(true);
        lob_meta_row_.mvcc_row_flag_.set_uncommitted_row(
            param_.tx_info_.trans_id_.is_valid() && is_incremental_minor_direct_load(param_.direct_load_type_));
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::write(const ObColumnSchemaItem &column_schema, ObIAllocator &row_allocator, blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    meta_write_iter_.reuse();
    lob_arena_.reuse();
    const int64_t timeout_ts = ObTimeUtility::fast_current_time() + ObInsertLobColumnHelper::LOB_ACCESS_TX_TIMEOUT;
    ObLobStorageParam lob_storage_param;
    lob_storage_param.inrow_threshold_ = lob_inrow_threshold_;
    lob_storage_param.is_index_table_ = param_.ddl_table_schema_.table_item_.is_index_table_;
    lob_storage_param.is_rowkey_col_ = column_schema.is_rowkey_column_;
    if (lob_id_cache_.remain_count() < lob_column_count_ && OB_FAIL(switch_lob_id_cache())) {
      LOG_WARN("switch lob id cache failed", K(ret), K(lob_id_cache_), K(lob_column_count_), K(lob_id_generator_));
    } else if (OB_FAIL(ObInsertLobColumnHelper::insert_lob_column(row_allocator,
                                                                  lob_arena_/*lob_allocator*/,
                                                                  param_.tx_info_.tx_desc_,
                                                                  lob_id_cache_,
                                                                  ls_id_,
                                                                  tablet_id_,
                                                                  lob_meta_tablet_id_,
                                                                  column_schema.col_type_.get_type(),
                                                                  column_schema.col_type_.get_collation_type(),
                                                                  lob_storage_param,
                                                                  datum,
                                                                  timeout_ts,
                                                                  true/*has_lob_header*/,
                                                                  param_.ddl_table_schema_.src_tenant_id_,
                                                                  meta_write_iter_))) {
      LOG_WARN("insert lob column failed", K(ret), K(param_));
    }
    ObLobMetaWriteResult lob_meta_write_result;
    bool first_get_next = true;
    ++total_lob_cell_count_;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (OB_FAIL(meta_write_iter_.get_next_row(lob_meta_write_result))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next lob meta write resutl failed", K(ret));
        } else {
          ret = OB_SUCCESS;
          if (first_get_next) {
            ++inrow_lob_cell_count_;
          }
          break;
        }
      } else if (FALSE_IT(first_get_next = false)) {
      } else if (OB_FAIL(transform_lob_meta_row(lob_meta_write_result))) {
        LOG_WARN("transform lob meta row failed", K(ret), K(lob_meta_write_result));
      } else if (OB_FAIL(prepare_macro_block_writer())) {
        LOG_WARN("prepare macro bock writer failed", K(ret));
      } else if (OB_FAIL(macro_block_writer_->append_row(lob_meta_row_))) {
        LOG_WARN("macro block writer append row failed", K(ret), K(lob_meta_row_));
      } else {
        LOG_DEBUG("lob writer append row", K(lob_meta_row_));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(meta_write_iter_.check_write_length())) {
        LOG_WARN("check_write_length fail", K(ret), K(meta_write_iter_));
      }
    }
  }

  return ret;
}

int ObLobMacroBlockWriter::transform_lob_meta_row(ObLobMetaWriteResult &lob_meta_write_result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObLobMetaUtil::transform_from_info_to_row(lob_meta_write_result.info_, &lob_meta_row_, true/*with_extra_rowkey*/))) {
    LOG_WARN("transform lob meta row failed", K(ret), K(lob_meta_write_result.info_));
  } else if (is_incremental_direct_load(param_.direct_load_type_)) {
    lob_meta_row_.storage_datums_[ObLobMetaUtil::SEQ_ID_COL_ID + 2].set_int(-lob_meta_write_result.seq_no_);
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!lob_meta_row_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob meta row", KR(ret), K(lob_meta_row_));
    } else if (OB_FAIL(ObDDLUtil::check_null_and_length(false/*is_index_table*/,
                                                        false/*has_lob_rowkey*/,
                                                        ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT,
                                                        lob_meta_row_))) {
      LOG_WARN("fail to check rowkey null value and length in row", KR(ret), K(lob_meta_row_));
    }
  }

  return ret;
}

int ObLobMacroBlockWriter::prepare_macro_block_writer()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(macro_block_writer_)) {
    if (OB_ISNULL(macro_block_writer_ = OB_NEW(ObCgMacroBlockWriter, ObMemAttr(MTL_ID(), "lob_mb_writer")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alocate memory for cg macro block writer failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!macro_block_writer_->is_inited())) {
    ObITable::TableKey lob_table_key;
    lob_table_key.tablet_id_ = lob_meta_tablet_id_;
    if (is_incremental_minor_direct_load(param_.direct_load_type_)) { // 增量
      lob_table_key.table_type_ = ObITable::MINI_SSTABLE;
      lob_table_key.scn_range_.start_scn_.convert_for_tx(1);
      lob_table_key.scn_range_.end_scn_.convert_for_tx(param_.snapshot_version_); // for logic version
    } else if (is_incremental_major_direct_load(param_.direct_load_type_)) { // 增量
      lob_table_key.table_type_ = ObITable::INC_MAJOR_SSTABLE;
      lob_table_key.column_group_idx_ = 0;
      // slice idx is not the same order with the rowkey of lob. set slice idx 0 here to compare rowkey when merge major sstable
      lob_table_key.slice_range_.start_slice_idx_ = 0;
      lob_table_key.slice_range_.end_slice_idx_ = 0;
      lob_table_key.version_range_.snapshot_version_ = param_.snapshot_version_;
    } else { // 全量
      lob_table_key.table_type_ = ObITable::MAJOR_SSTABLE;
      lob_table_key.column_group_idx_ = 0;
      // slice idx is not the same order with the rowkey of lob. set slice idx 0 here to compare rowkey when merge major sstable
      lob_table_key.slice_range_.start_slice_idx_ = 0;
      lob_table_key.slice_range_.end_slice_idx_ = 0;
      lob_table_key.version_range_.snapshot_version_ = param_.snapshot_version_;
    }
    uint64_t lob_start_seq = 0;
    if (OB_FAIL(lob_id_cache_.get_value(lob_start_seq))) {
      LOG_WARN("get lob start seq failed", K(ret), K(lob_id_cache_));
    } else if (OB_FAIL(macro_block_writer_->init(param_, lob_table_key, macro_seq_, 0 /*row_offset*/, lob_start_seq))) {
      LOG_WARN("init macro block writer failed", K(ret), K(lob_table_key));
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::switch_lob_id_cache()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(close_macro_block_writer())) {
    LOG_WARN("close macro block writer failed", K(ret));
  } else {
    uint64_t old_value = 0;
    (void) lob_id_cache_.get_value(old_value);
    if (lob_id_generator_.is_inited()) {
      int64_t lob_id_start = -1;
      int64_t lob_id_end = -1;
      if (OB_FAIL(lob_id_generator_.get_next_interval(lob_id_start, lob_id_end))) {
        LOG_WARN("get lob id cache from ddl sequence generator fail", K(ret), K(lob_id_generator_));
      } else {
        lob_id_cache_.cache_size_ = lob_id_generator_.get_interval_size();
        lob_id_cache_.set(max(lob_id_start, 1), lob_id_end);
      }
    } else {
      static const int64_t AUTO_INC_CACHE_INTERVAL = 5000000L; // 500w.
      ObTabletAutoincrementService &auto_inc = ObTabletAutoincrementService::get_instance();
      lob_id_cache_.cache_size_ = AUTO_INC_CACHE_INTERVAL;
      if (OB_FAIL(auto_inc.get_tablet_cache_interval(MTL_ID(), lob_id_cache_))) {
        LOG_WARN("autoinc service get tablet cache failed", K(ret), K(MTL_ID()));
      }
    }
    FLOG_INFO("switch lob id cache", K(ret), K(tablet_id_), K(slice_idx_), "is_idem", lob_id_generator_.is_inited(), K(old_value), K(total_lob_cell_count_), K(inrow_lob_cell_count_), "new_cache", lob_id_cache_);
    total_lob_cell_count_ = 0;
    inrow_lob_cell_count_ = 0;
  }
  return ret;
}

int ObLobMacroBlockWriter::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(close_macro_block_writer())) {
    LOG_WARN("close macro block writer failed", K(ret));
  } else {
    uint64_t last_lob_id = 0;
    if (OB_FAIL(lob_id_cache_.get_value(last_lob_id))) {
      LOG_WARN("get last lob id failed", K(ret));
    } else if (OB_FAIL(ObDDLUtil::set_tablet_autoinc_seq(ls_id_, lob_meta_tablet_id_, last_lob_id))) {
      LOG_WARN("update max lob id failed", K(ret), K(last_lob_id));
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::close_macro_block_writer()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_NOT_NULL(macro_block_writer_)) {
    if (OB_FAIL(macro_block_writer_->close())) {
      LOG_WARN("macro block writer close failed", K(ret));
    } else {
      macro_seq_ = macro_block_writer_->get_last_macro_seq();
      macro_block_writer_->reset(); // TODO@wenqu: just flush macro block for better performance
    }
  }
  return ret;
}
