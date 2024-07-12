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
#include "share/ob_ddl_checksum.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "share/ob_ddl_common.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/compaction/ob_column_checksum_calculator.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "sql/engine/pdml/static/ob_px_sstable_insert_op.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"
#include "storage/lob/ob_lob_util.h"
#include "storage/tablet/ob_tablet.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/das/ob_das_utils.h"
#include "sql/engine/basic/chunk_store/ob_compact_store.h"

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

ObDDLInsertRowIterator::ObDDLInsertRowIterator()
  : is_inited_(false),
    lob_allocator_(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    op_(nullptr),
    ls_id_(),
    current_tablet_id_(),
    current_row_(),
    is_next_row_cached_(true),
    is_slice_empty_(false),
    rowkey_count_(-1),
    snapshot_version_(-1),
    lob_slice_id_(-1),
    lob_id_cache_(),
    context_id_(-1),
    macro_seq_()
{
}

ObDDLInsertRowIterator::~ObDDLInsertRowIterator()
{

}

int ObDDLInsertRowIterator::init(
    sql::ObPxMultiPartSSTableInsertOp *op,
    const bool is_slice_empty,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const int64_t rowkey_cnt,
    const int64_t snapshot_version,
    const int64_t context_id,
    const int64_t parallel_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || rowkey_cnt < 0
      || snapshot_version < 0 || context_id < 0 || parallel_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_id), K(tablet_id), K(rowkey_cnt), K(snapshot_version), K(context_id), K(parallel_idx));
  } else if (OB_FAIL(macro_seq_.set_parallel_degree(parallel_idx))) {
    LOG_WARN("set parallel failed", K(ret), K(parallel_idx));
  } else {
    op_ = op;
    is_slice_empty_ = is_slice_empty;
    ls_id_ = ls_id;
    current_tablet_id_ = tablet_id;
    rowkey_count_ = rowkey_cnt;
    snapshot_version_ = snapshot_version;
    context_id_ = context_id;
    is_next_row_cached_ = true;
    lob_id_cache_.set(1/*start*/, 0/*end*/);
    is_inited_ = true;
  }
  return ret;
}

int ObDDLInsertRowIterator::close_lob_sstable_slice()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (lob_slice_id_ > 0) {
    ObDirectLoadSliceInfo slice_info;
    slice_info.is_full_direct_load_ = true;
    slice_info.is_lob_slice_ = true;
    slice_info.ls_id_ = ls_id_;
    slice_info.data_tablet_id_ = current_tablet_id_;
    slice_info.slice_id_ = lob_slice_id_;
    slice_info.context_id_ = context_id_;
    ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
    ObMacroDataSeq unused_seq;
    if (OB_FAIL(tenant_direct_load_mgr->close_sstable_slice(slice_info, nullptr/*insert_monitor*/, unused_seq))) {
      LOG_WARN("close sstable slice failed", K(ret), K(slice_info));
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
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == op_ || snapshot_version_ <= 0 || nullptr == tenant_direct_load_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is null", K(ret), KP(op_), K(snapshot_version_), KP(tenant_direct_load_mgr), K(MTL_ID()));
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
      lob_allocator_.reuse();
      if (OB_FAIL(op_->get_tablet_id_from_row(op_->get_child()->get_spec().output_,
                                              op_->get_spec().row_desc_.get_part_id_index(),
                                              row_tablet_id))) {
        LOG_WARN("get part id failed", K(ret));
      } else if (row_tablet_id != current_tablet_id_) {
        // iter the partition end, and switch to next part.
        ret = OB_ITER_END;
      } else {
        const ObExprPtrIArray &exprs = op_->get_spec().ins_ctdef_.new_row_;
        const int64_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
        ObEvalCtx &eval_ctx = op_->get_eval_ctx();
        const int64_t request_cnt = exprs.count() + extra_rowkey_cnt;
        if (OB_UNLIKELY((rowkey_count_ > exprs.count()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected rowkey count", K(ret), K(rowkey_count_), K(exprs.count()));
        } else if (current_row_.get_column_count() <= 0
          && OB_FAIL(current_row_.init(op_->get_exec_ctx().get_allocator(), request_cnt))) {
          LOG_WARN("init datum row failed", K(ret), K(request_cnt));
        } else if (OB_UNLIKELY(current_row_.get_column_count() != request_cnt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected err", K(ret), K(request_cnt), "datum_row_cnt", current_row_.get_column_count());
        } else {
          ObArray<int64_t> lob_column_idxs;
          ObArray<common::ObObjMeta> col_types;
          lob_column_idxs.set_attr(ObMemAttr(MTL_ID(), "DL_lob_idxs"));
          col_types.set_attr(ObMemAttr(MTL_ID(), "DL_col_types"));
          for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
            int64_t storage_index = 0; // real storage index of the column.
            ObDatum *datum = NULL;
            const ObExpr *e = exprs.at(i);
            if (OB_ISNULL(e)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("expr is NULL", K(ret), K(i));
            } else if (OB_FAIL(e->eval(eval_ctx, datum))) {
              LOG_WARN("evaluate expression failed", K(ret), K(i), KPC(e));
            } else if (i < rowkey_count_) {
              storage_index = i;
              current_row_.storage_datums_[storage_index].shallow_copy_from_datum(*datum);
            } else {
              storage_index = i + extra_rowkey_cnt;
              current_row_.storage_datums_[storage_index].shallow_copy_from_datum(*datum);
            }

            if (OB_FAIL(ret)) {
            } else if (exprs.at(i)->obj_meta_.is_lob_storage()
              && !current_row_.storage_datums_[storage_index].is_nop()
              && !current_row_.storage_datums_[storage_index].is_null()) {
              if (OB_FAIL(lob_column_idxs.push_back(storage_index))) {
                LOG_WARN("fail to push back storage_index", K(ret), K(storage_index));
              } else if (OB_FAIL(col_types.push_back(exprs.at(i)->obj_meta_))) {
                LOG_WARN("fail to push back col_type", K(ret), K(exprs.at(i)->obj_meta_));
              }
            }
          }

          if (OB_SUCC(ret) && !skip_lob && lob_column_idxs.count() > 0) {
            //handle lob
            if (lob_id_cache_.remain_count() < lob_column_idxs.count()) {
              if (OB_FAIL(switch_to_new_lob_slice())) {
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
              if (OB_FAIL(tenant_direct_load_mgr->fill_lob_sstable_slice(lob_allocator_, slice_info,
                  lob_id_cache_, lob_column_idxs, col_types, current_row_))) {
                LOG_WARN("fill batch lob sstable slice failed", K(ret), K(slice_info), K(current_row_));
              }
            }
          }

          if (OB_SUCC(ret)) {
            // add extra rowkey
            current_row_.storage_datums_[rowkey_count_].set_int(-snapshot_version_);
            current_row_.storage_datums_[rowkey_count_ + 1].set_int(0);
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(close_lob_sstable_slice())) {
        LOG_WARN("fail to close lob sstable slice", K(tmp_ret));
      } else {
        ret = tmp_ret == OB_SUCCESS ? OB_ITER_END : tmp_ret;
      }
    }
  }
  if (OB_SUCC(ret)) {
    current_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    row = &current_row_;
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
  ObTabletAutoincrementService &auto_inc = ObTabletAutoincrementService::get_instance();
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  ObTabletDirectLoadMgrHandle direct_load_mgr_handle;
  direct_load_mgr_handle.reset();
  int64_t CACHE_SIZE_REQUESTED = AUTO_INC_CACHE_SIZE;
#ifdef ERRSIM
  int64_t negative_inject_num = OB_E(EventTable::EN_DDL_LOBID_CACHE_SIZE_INJECTED) OB_SUCCESS;
  CACHE_SIZE_REQUESTED = negative_inject_num * -1;
#endif
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(tenant_direct_load_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(tenant_direct_load_mgr->get_tablet_mgr(current_tablet_id_,
      true /*is_full_direct_load*/, direct_load_mgr_handle))) {
    LOG_WARN("get tablet direct load mgr failed", K(ret), K(current_tablet_id_));
  } else if (OB_FALSE_IT(lob_id_cache_.tablet_id_ =
    direct_load_mgr_handle.get_obj()->get_lob_meta_tablet_id())) {
    // fetch cache via lob meta tablet id.
  } else if (OB_FALSE_IT(lob_id_cache_.cache_size_ = CACHE_SIZE_REQUESTED)) {
  } else if (OB_FAIL(auto_inc.get_tablet_cache_interval(MTL_ID(), lob_id_cache_))) {
    LOG_WARN("get_autoinc_seq fail", K(ret), K(MTL_ID()), K(slice_info));
  } else if (OB_UNLIKELY(CACHE_SIZE_REQUESTED > lob_id_cache_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected autoincrement value count", K(ret), K(lob_id_cache_));
  } else if (lob_slice_id_ > 0) {
    blocksstable::ObMacroDataSeq next_block_start_seq;
    if (OB_FAIL(tenant_direct_load_mgr->close_sstable_slice(slice_info, nullptr/*insert_monitor*/, next_block_start_seq))) {
      LOG_WARN("close old lob slice failed", K(ret), K(slice_info));
    } else if (OB_UNLIKELY(next_block_start_seq.get_data_seq() < macro_seq_.get_data_seq())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected start sequence", K(ret), K(next_block_start_seq), K(macro_seq_));
    } else {
      macro_seq_ = next_block_start_seq;
    }
  }
  if (OB_SUCC(ret)) {
    // new slice info to open.
    slice_info.slice_id_ = 0;
    if (OB_FAIL(tenant_direct_load_mgr->open_sstable_slice(macro_seq_, slice_info))) {
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
          int64_t column_idx = cur_cg_schema.column_idxs_ ? cur_cg_schema.column_idxs_[j] : j; // all_cg column_idxs_ = null
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
      if (OB_FAIL(datum_stores_.at(i)->finish_add_row(true/*need_dump*/))) {
        LOG_WARN("finish add row failed", K(ret));
      }
    }
  }
  LOG_DEBUG("chunk slice store closed", K(ret), K(endkey_));
  return ret;
}


int ObMacroBlockSliceStore::init(
    ObTabletDirectLoadMgr *tablet_direct_load_mgr,
    const blocksstable::ObMacroDataSeq &data_seq,
    const SCN &start_scn)
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
          ls_id, table_key.tablet_id_, DDL_MB_DATA_TYPE, table_key, ddl_task_id, start_scn, data_format_version, direct_load_type, tx_desc, trans_id))) {
        LOG_WARN("fail to init inc ddl_redo_callback_", K(ret));
      }
    } else {
      if (OB_ISNULL(ddl_redo_callback_ = OB_NEW(ObDDLRedoLogWriterCallback, ObMemAttr(MTL_ID(), "DDL_MBSS")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret));
      } else if (OB_FAIL(static_cast<ObDDLRedoLogWriterCallback *>(ddl_redo_callback_)->init(
          ls_id, table_key.tablet_id_, DDL_MB_DATA_TYPE, table_key, ddl_task_id, start_scn, data_format_version, direct_load_type, -1/*row_id_offset*/))) {
        LOG_WARN("fail to init full ddl_redo_callback_", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(macro_block_writer_.open(data_desc.get_desc(), data_seq, ddl_redo_callback_))) {
        LOG_WARN("open macro bock writer failed", K(ret));
      } else {
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
  : is_inited_(false), need_column_store_(false), is_canceled_(false), start_seq_(), tablet_direct_load_mgr_(nullptr),
    slice_store_(nullptr), meta_write_iter_(nullptr), row_iterator_(nullptr),
    allocator_(lib::ObLabel("SliceWriter"), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()), row_offset_(-1)
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
  allocator_.reset();
  row_offset_ = -1;
  need_column_store_ = false;
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

int ObDirectLoadSliceWriter::prepare_slice_store_if_need(
    const int64_t schema_rowkey_column_num,
    const bool is_column_store,
    const int64_t dir_id,
    const int64_t parallelism,
    const ObStorageSchema *storage_schema,
    const SCN &start_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (nullptr != slice_store_) {
    // do nothing
  } else if (is_full_direct_load(tablet_direct_load_mgr_->get_direct_load_type()) && is_column_store) {
    need_column_store_ = true;
    ObChunkSliceStore *chunk_slice_store = nullptr;
    if (OB_ISNULL(storage_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("null schema", K(ret), K(*this));
    } else if (OB_ISNULL(chunk_slice_store = OB_NEWx(ObChunkSliceStore, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for chunk slice store failed", K(ret));
    } else if (OB_FAIL(chunk_slice_store->init(schema_rowkey_column_num + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt(),
                                    storage_schema, allocator_, tablet_direct_load_mgr_->get_column_info(), dir_id, parallelism))) {
      LOG_WARN("init chunk slice store failed", K(ret), KPC(storage_schema));
    } else {
      slice_store_ = chunk_slice_store;
    }
    if (OB_FAIL(ret) && nullptr != chunk_slice_store) {
      chunk_slice_store->~ObChunkSliceStore();
      allocator_.free(chunk_slice_store);
    }
  } else {
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
    const blocksstable::ObMacroDataSeq &start_seq)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(nullptr == tablet_direct_load_mgr || !start_seq.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(tablet_direct_load_mgr), K(start_seq));
  } else {
    tablet_direct_load_mgr_ = tablet_direct_load_mgr;
    start_seq_ = start_seq;
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
    const ObLobId &lob_id,
    const transaction::ObTransID trans_id,
    const int64_t seq_no,
    const int64_t timeout_ts,
    const int64_t lob_inrow_threshold,
    const uint64_t src_tenant_id,
    const ObDirectLoadType direct_load_type,
    transaction::ObTxDesc* tx_desc,
    ObLobMetaRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  row_iter = nullptr;

  if (OB_ISNULL(meta_write_iter_)) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObLobMetaWriteIter)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc lob meta write iter failed", K(ret));
    } else {
      // keep allocator is same as insert_lob_column
      meta_write_iter_ = new (buf) ObLobMetaWriteIter(&allocator, ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE);
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
    ObLobStorageParam lob_storage_param;
    lob_storage_param.inrow_threshold_ = lob_inrow_threshold;
    int64_t unused_affected_rows = 0;
    if (is_incremental_direct_load(direct_load_type) && OB_ISNULL(tx_desc)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tx_desc should not be null if is incremental_direct_load", K(ret), K(direct_load_type),
          K(ls_id), K(tablet_id), K(lob_id), K(trans_version), K(seq_no), K(obj_type), K(cs_type), K(trans_id));
    } else if (OB_FAIL(ObInsertLobColumnHelper::insert_lob_column(
      allocator, tx_desc, ls_id, tablet_id, lob_id, obj_type, cs_type, lob_storage_param, datum,
      timeout_ts, true/*has_lob_header*/, src_tenant_id, *meta_write_iter_))) {
      LOG_WARN("fail to insert_lob_col", K(ret), K(ls_id), K(tablet_id), K(lob_id), K(src_tenant_id));
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
    const int64_t lob_inrow_threshold,
    blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  const uint64_t data_format_version = tablet_direct_load_mgr_->get_data_format_version();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSliceWriter not init", KR(ret), KP(this));
  } else if (DATA_VERSION_4_3_0_0 > data_format_version) {
    if (OB_FAIL(fill_lob_into_memtable(allocator, info, lob_column_idxs, col_types, lob_inrow_threshold, datum_row))) {
      LOG_WARN("fill lob into memtable failed", K(ret), K(data_format_version));
    }
  } else if (OB_FAIL(fill_lob_into_macro_block(allocator, iter_allocator, start_scn, info,
      pk_interval, lob_column_idxs, col_types, lob_inrow_threshold, datum_row))) {
    LOG_WARN("fill lob into macro block failed", K(ret), K(data_format_version));
  }
  return ret;
}

int ObDirectLoadSliceWriter::fill_lob_into_memtable(
    ObIAllocator &allocator,
    const ObBatchSliceWriteInfo &info,
    const ObArray<int64_t> &lob_column_idxs,
    const ObArray<common::ObObjMeta> &col_types,
    const int64_t lob_inrow_threshold,
    blocksstable::ObDatumRow &datum_row)
{
  // to insert lob data into memtable.
  int ret = OB_SUCCESS;
  const int64_t timeout_ts =
      ObTimeUtility::fast_current_time() + (ObInsertLobColumnHelper::LOB_ACCESS_TX_TIMEOUT * lob_column_idxs.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < lob_column_idxs.count(); i++) {
    const int64_t idx = lob_column_idxs.at(i);
    ObStorageDatum &datum = datum_row.storage_datums_[idx];
    ObLobStorageParam lob_storage_param;
    lob_storage_param.inrow_threshold_ = lob_inrow_threshold;
    if (OB_FAIL(ObInsertLobColumnHelper::insert_lob_column(
      allocator, info.ls_id_, info.data_tablet_id_, col_types.at(i).get_type(), col_types.at(i).get_collation_type(),
      lob_storage_param, datum, timeout_ts, true/*has_lob_header*/, info.src_tenant_id_))) {
      LOG_WARN("fail to insert_lob_col", K(ret), K(datum));
    }
  }
  return ret;
}

int ObDirectLoadSliceWriter::fill_lob_into_macro_block(
    ObIAllocator &allocator,
    ObIAllocator &iter_allocator,
    const SCN &start_scn,
    const ObBatchSliceWriteInfo &info,
    share::ObTabletCacheInterval &pk_interval,
    const ObArray<int64_t> &lob_column_idxs,
    const ObArray<common::ObObjMeta> &col_types,
    const int64_t lob_inrow_threshold,
    blocksstable::ObDatumRow &datum_row)
{
  // to insert lob data into macro block.
  int ret = OB_SUCCESS;
  int64_t unused_affected_rows = 0;
  const int64_t timeout_ts =
      ObTimeUtility::fast_current_time() + (ObInsertLobColumnHelper::LOB_ACCESS_TX_TIMEOUT * lob_column_idxs.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < lob_column_idxs.count(); i++) {
    int64_t idx = lob_column_idxs.at(i);
    ObStorageDatum &datum = datum_row.storage_datums_[idx];
    if (!datum.is_nop() && !datum.is_null()) {
      uint64_t pk_seq = OB_INVALID_ID;
      if (OB_FAIL(pk_interval.next_value(pk_seq))) {
        LOG_WARN("fail to get next lob_id", K(ret), K(pk_seq));
      } else {
        ObLobId lob_id;
        lob_id.lob_id_ = pk_seq;
        lob_id.tablet_id_ = tablet_direct_load_mgr_->get_tablet_id().id(); // lob meta tablet id.
        ObLobMetaRowIterator *row_iter = nullptr;
        if (OB_FAIL(prepare_iters(allocator, iter_allocator, datum, info.ls_id_,
            info.data_tablet_id_, info.trans_version_, col_types.at(i).get_type(), col_types.at(i).get_collation_type(), lob_id,
            info.trans_id_, info.seq_no_, timeout_ts, lob_inrow_threshold, info.src_tenant_id_, info.direct_load_type_, info.tx_desc_, row_iter))) {
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
            } else if (OB_FAIL(check_null(false/*is_index_table*/, ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT, *cur_row))) {
              LOG_WARN("fail to check null value in row", KR(ret), KPC(cur_row));
            } else if (OB_FAIL(prepare_slice_store_if_need(ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT,
                false/*is_column_store*/, 1L/*unsued*/, 1L/*unused*/, nullptr /*storage_schema*/, start_scn))) {
              LOG_WARN("prepare macro block writer failed", K(ret));
            } else if (OB_FAIL(slice_store_->append_row(*cur_row))) {
              LOG_WARN("macro block writer append row failed", K(ret), KPC(cur_row));
            }
            if (OB_SUCC(ret)) {
              ++unused_affected_rows;
              LOG_DEBUG("sstable insert op append row", K(unused_affected_rows), KPC(cur_row));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_NOT_NULL(meta_write_iter_)) {
              meta_write_iter_->reuse();
            }
            if (OB_NOT_NULL(row_iterator_)) {
              row_iterator_->reuse();
            }
          }
        }
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
      } else if (OB_FAIL(check_null(false/*is_index_table*/, rowkey_column_count, *cur_row))) {
        LOG_WARN("fail to check null value in row", KR(ret), KPC(cur_row));
      } else if (OB_FAIL(prepare_slice_store_if_need(rowkey_column_count,
                                                     false/*is_column_store*/,
                                                     1L/*unsued*/,
                                                     1L/*unused*/,
                                                     nullptr /*storage_schema*/,
                                                     start_scn))) {
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
      } else if (OB_FAIL(check_null(schema_item.is_index_table_, schema_item.rowkey_column_num_, *cur_row))) {
        LOG_WARN("fail to check null value in row", KR(ret), KPC(cur_row));
      } else if (OB_FAIL(prepare_slice_store_if_need(schema_item.rowkey_column_num_, schema_item.is_column_store_, dir_id, parallelism, storage_schema, start_scn))) {
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
          LOG_WARN("macro block writer append row failed", K(ret), KPC(cur_row), KPC(cur_row));
        }
      }
      if (OB_SUCC(ret)) {
        LOG_DEBUG("sstable insert op append row", KPC(cur_row), KPC(cur_row));
        ++affected_rows;
        if (OB_NOT_NULL(insert_monitor)) {
          insert_monitor->inserted_row_cnt_ =  insert_monitor->inserted_row_cnt_ + 1;
        }
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

int ObDirectLoadSliceWriter::check_null(
    const bool is_index_table,
    const int64_t rowkey_column_num,
    const ObDatumRow &row_val) const
{
  int ret = OB_SUCCESS;
  if (is_index_table) {
    // index table is index-organized but can have null values in index column
  } else if (OB_UNLIKELY(rowkey_column_num > row_val.get_column_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey column number", KR(ret), K(rowkey_column_num), K(row_val));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_num; i++) {
      const ObStorageDatum &cell = row_val.storage_datums_[i];
      if (cell.is_null()) {
        ret = OB_ER_INVALID_USE_OF_NULL;
        LOG_WARN("invalid null cell for row key column", KR(ret), K(cell));
      }
    }
  }
  return ret;
}

int ObDirectLoadSliceWriter::fill_aggregated_column_group(
    const int64_t cg_idx,
    ObCOSliceWriter *cur_writer,
    ObIArray<sql::ObCompactStore *> &datum_stores)
{
  int ret = OB_SUCCESS;
  datum_stores.reset();
  ObChunkSliceStore *chunk_slice_store = static_cast<ObChunkSliceStore *>(slice_store_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (nullptr == chunk_slice_store || is_empty()) {
    // do nothing
    LOG_INFO("chunk slice store is null or empty", K(ret),
        KPC(chunk_slice_store), KPC(tablet_direct_load_mgr_));
  } else if (ATOMIC_LOAD(&is_canceled_)) {
    ret = OB_CANCELED;
    LOG_WARN("fil cg task canceled", K(ret), K(is_canceled_));
  } else if (cg_idx < 0 || cg_idx > chunk_slice_store->datum_stores_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cg idx", K(ret), K(cg_idx), K(chunk_slice_store->datum_stores_));
  } else {
    sql::ObCompactStore *cur_datum_store = chunk_slice_store->datum_stores_.at(cg_idx);
    const ObChunkDatumStore::StoredRow *stored_row = nullptr;
    bool has_next = false;
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
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(datum_stores.push_back(cur_datum_store))) {
        LOG_WARN("fail to push datum store", K(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadSliceWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSliceWriter not init", KR(ret), KP(this));
  } else if (nullptr != slice_store_ && OB_FAIL(slice_store_->close())) {
    LOG_WARN("close slice store failed", K(ret));
  }
  return ret;
}

int ObDirectLoadSliceWriter::fill_column_group(const ObStorageSchema *storage_schema, const SCN &start_scn, ObInsertMonitor* insert_monitor)
{
  int ret = OB_SUCCESS;
  ObChunkSliceStore *chunk_slice_store = static_cast<ObChunkSliceStore *>(slice_store_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(storage_schema));
  } else if (OB_UNLIKELY(row_offset_ < 0)) {
    ret = OB_ERR_SYS;
    LOG_WARN("row offset not set", K(ret), K(row_offset_));
  } else if (nullptr == chunk_slice_store || is_empty()) {
    // do nothing
    LOG_INFO("chunk slice store is null or empty", K(ret),
        KPC(chunk_slice_store), KPC(tablet_direct_load_mgr_));
  } else if (ATOMIC_LOAD(&is_canceled_)) {
    ret = OB_CANCELED;
    LOG_WARN("fil cg task canceled", K(ret), K(is_canceled_));
  } else {
    const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema->get_column_groups();
    FLOG_INFO("[DDL_FILL_CG] fill column group start",
        "tablet_id", tablet_direct_load_mgr_->get_tablet_id(),
        "row_count", chunk_slice_store->get_row_count(),
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
        if (OB_FAIL(cur_writer->init(storage_schema, cg_idx, tablet_direct_load_mgr_, start_seq_, row_offset_, start_scn))) {
          LOG_WARN("init co ddl writer failed", K(ret), KPC(cur_writer), K(cg_idx), KPC(this));
        } else {
          sql::ObCompactStore *cur_datum_store = chunk_slice_store->datum_stores_.at(cg_idx);
          const ObChunkDatumStore::StoredRow *stored_row = nullptr;
          bool has_next = false;
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
              } else if (OB_NOT_NULL(insert_monitor)) {
                insert_monitor->inserted_cg_row_cnt_ += 1;
              }
            }
          }
          if (OB_SUCC(ret)) {
            // 3. close writers
            if (OB_FAIL(cur_writer->close())) {
              LOG_WARN("close co ddl writer failed", K(ret));
            } else {
              // 4. reset datum store (if fail, datum store will free when ~ObDirectLoadSliceWriter())
              cur_datum_store->reset();
            }
          }
        }
      }
    }
    if (OB_NOT_NULL(cur_writer)) {
      cur_writer->~ObCOSliceWriter();
      allocator_.free(cur_writer);
    }
    FLOG_INFO("[DDL_FILL_CG] fill column group finished",
        "tablet_id", tablet_direct_load_mgr_->get_tablet_id(),
        "row_count", chunk_slice_store->get_row_count(),
        "column_group_count", cg_schemas.count());
  }
  return ret;
}


void ObCOSliceWriter::reset()
{
  is_inited_ = false;
  cg_row_.reset();
  macro_block_writer_.reset();
  flush_callback_.reset();
  index_builder_.reset();
  data_desc_.reset();
  cg_schema_ = nullptr;
  cg_idx_ = -1;
}

int ObCOSliceWriter::init(const ObStorageSchema *storage_schema, const int64_t cg_idx,
    ObTabletDirectLoadMgr *tablet_direct_load_mgr, const ObMacroDataSeq &start_seq, const int64_t row_id_offset,
    const SCN &start_scn)
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

    if (OB_FAIL(data_desc_.init(*storage_schema,
                                ls_id,
                                table_key.get_tablet_id(),
                                compaction::ObMergeType::MAJOR_MERGE,
                                table_key.get_snapshot_version(),
                                data_format_version,
                                SCN::min_scn(),
                                &cg_schema,
                                cg_idx))) {
      LOG_WARN("init data store desc failed", K(ret));
    } else if (OB_FAIL(index_builder_.init(data_desc_.get_desc(), nullptr/*macro block flush callback*/, ObSSTableIndexBuilder::ENABLE))) { // data_desc is deep copied
      LOG_WARN("init sstable index builder failed", K(ret), K(ls_id), K(table_key), K(data_desc_));
    } else if (FALSE_IT(data_desc_.get_desc().sstable_index_builder_ = &index_builder_)) { // for build the tail index block in macro block
    } else if (OB_FAIL(flush_callback_.init(ls_id, table_key.tablet_id_, DDL_MB_DATA_TYPE, table_key, ddl_task_id,
        start_scn, data_format_version, tablet_direct_load_mgr->get_direct_load_type(), row_id_offset))) {
      LOG_WARN("fail to init redo log writer callback", KR(ret));
    } else if (OB_FAIL(macro_block_writer_.open(data_desc_.get_desc(), start_seq, &flush_callback_))) {
      LOG_WARN("fail to open macro block writer", K(ret), K(ls_id), K(table_key), K(data_desc_), K(start_seq));
    } else if (OB_FAIL(cg_row_.init(cg_schema.column_cnt_))) {
      LOG_WARN("init column group row failed", K(ret));
    } else {
      cg_idx_ = cg_idx;
      cg_schema_ = &cg_schema;
      is_inited_ = true;
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
