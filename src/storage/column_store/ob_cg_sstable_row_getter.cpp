/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX STORAGE
#include "lib/stat/ob_diagnose_info.h"
#include "ob_cg_sstable_row_getter.h"

namespace oceanbase
{
namespace storage
{
///////////////////////////////////////// ObCGGetter ///////////////////////////////////////////////
ObCGGetter::~ObCGGetter()
{
  FREE_PTR_FROM_CONTEXT(access_ctx_, micro_getter_, ObMicroBlockCGRowGetter);
}

void ObCGGetter::reset()
{
  FREE_PTR_FROM_CONTEXT(access_ctx_, micro_getter_, ObMicroBlockCGRowGetter);
  is_inited_ = false;
  is_same_data_block_ = false;
  sstable_ = nullptr;
  iter_param_ = nullptr;
  access_ctx_ = nullptr;
  table_wrapper_.reset();
  prefetcher_.reset();
  read_handle_.reset();
}

void ObCGGetter::reuse()
{
  is_inited_ = false;
  sstable_ = nullptr;
  table_wrapper_.reset();
  is_same_data_block_ = false;
  prefetcher_.reuse();
}

int ObCGGetter::init(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObSSTableWrapper &wrapper,
    const blocksstable::ObDatumRowkey &idx_key)
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObCGGetter has been inited", K(ret));
  } else if (OB_UNLIKELY(!wrapper.is_valid() ||
                         !wrapper.get_sstable()->is_normal_cg_sstable() ||
                         !iter_param.is_valid() ||
                         1 != idx_key.get_datum_cnt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init ObCGGetter", K(ret), K(wrapper), K(iter_param));
  } else if (OB_FAIL(wrapper.get_loaded_column_store_sstable(sstable))) {
    LOG_WARN("fail to get sstable", K(ret), K(wrapper));
  } else {
    is_same_data_block_ = false;
    table_wrapper_ = wrapper;
    if (!prefetcher_.is_valid()) {
      if (OB_FAIL(prefetcher_.init(
                  ObStoreRowIterator::IteratorCOSingleGet,
                  *sstable,
                  iter_param,
                  access_ctx,
                  nullptr))) {
        LOG_WARN("Fail to init prefetcher", K(ret));
      }
    } else {
      if (OB_FAIL(prefetcher_.switch_context(
                  ObStoreRowIterator::IteratorCOSingleGet,
                  *sstable,
                  iter_param,
                  access_ctx,
                  nullptr))) {
        LOG_WARN("Fail to switch context for prefetcher", K(ret));
      } else {
        ObMicroBlockDataHandle &micro_handle = prefetcher_.get_last_data_handle();
        is_same_data_block_ =
            nullptr != sstable_ &&
            sstable->get_key() == sstable_->get_key() &&
            micro_handle.in_block_state() &&
            0 == read_handle_.index_block_info_.get_row_range().compare(idx_key.datums_[0].get_int()) &&
            micro_handle.match(
                read_handle_.index_block_info_.get_macro_id(),
                read_handle_.index_block_info_.get_block_offset(),
                read_handle_.index_block_info_.get_block_size());
      }
    }

    if (OB_SUCC(ret)) {
      if (is_same_data_block_) {
        read_handle_.rowkey_ = &idx_key;
        read_handle_.micro_handle_ = &prefetcher_.get_last_data_handle();
      } else {
        read_handle_.reset();
        read_handle_.rowkey_ = &idx_key;
        read_handle_.range_idx_ = 0;
        read_handle_.is_get_ = true;
        if (OB_FAIL(prefetcher_.single_prefetch(read_handle_))) {
          LOG_WARN("ObCGGetter prefetch failed ", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    sstable_ = sstable;
    iter_param_ = &iter_param;
    access_ctx_ = &access_ctx;
    is_inited_ = true;
  } else {
    reset();
  }
  return ret;
}

int ObCGGetter::get_next_row(ObMacroBlockReader &block_reader, const blocksstable::ObDatumRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObCGGetter is not inited");
  } else if (!is_same_data_block_ && OB_FAIL(prefetcher_.lookup_in_index_tree(read_handle_, true))) {
    LOG_WARN("Fail to lookup in index tree", K(ret), K_(read_handle));
  } else if (nullptr == micro_getter_) {
    if (nullptr == (micro_getter_ = OB_NEWx(ObMicroBlockCGRowGetter, access_ctx_->stmt_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate micro block row getter", K(ret));
    } else if (OB_FAIL(micro_getter_->init(*iter_param_, *access_ctx_, sstable_))) {
      LOG_WARN("Fail to init micro block row getter", K(ret));
    }
  } else if (OB_FAIL(micro_getter_->switch_context(*iter_param_, *access_ctx_, sstable_))) {
    LOG_WARN("Fail to switch context", K(ret));
  }
  LOG_DEBUG("start to fetch row", KPC_(read_handle_.rowkey), K_(read_handle));

  if (OB_SUCC(ret)) {
    int64_t row_idx = read_handle_.rowkey_->datums_[0].get_int();
    if (OB_UNLIKELY(row_idx < read_handle_.index_block_info_.get_row_range().start_row_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected row idx", K(ret), K(row_idx), K(read_handle_.index_block_info_.get_row_range()));
    } else if (OB_FAIL(micro_getter_->get_row(
                read_handle_,
                block_reader,
                row_idx - read_handle_.index_block_info_.get_row_range().start_row_id_,
                store_row))) {
      LOG_WARN("Fail to get row", K(ret), K(row_idx), "macro_id", read_handle_.micro_handle_->macro_block_id_);
    }
  }
  return ret;
}

///////////////////////////////////////// ObCGSSTableRowGetter ///////////////////////////////////////////////
ObCGSSTableRowGetter::~ObCGSSTableRowGetter()
{
  FREE_PTR_FROM_CONTEXT(access_ctx_, cg_param_pool_, ObCGIterParamPool);
  FREE_PTR_FROM_CONTEXT(access_ctx_, flat_reader_, ObMicroBlockGetReader);
  FREE_PTR_FROM_CONTEXT(access_ctx_, encode_reader_, ObEncodeBlockGetReader);
  FREE_PTR_FROM_CONTEXT(access_ctx_, cs_encode_reader_, ObCSEncodeBlockGetReader);
  FREE_PTR_FROM_CONTEXT(access_ctx_, micro_getter_, ObMicroBlockRowGetter);
}

void ObCGSSTableRowGetter::reset()
{
  FREE_PTR_FROM_CONTEXT(access_ctx_, cg_param_pool_, ObCGIterParamPool);
  FREE_PTR_FROM_CONTEXT(access_ctx_, flat_reader_, ObMicroBlockGetReader);
  FREE_PTR_FROM_CONTEXT(access_ctx_, encode_reader_, ObEncodeBlockGetReader);
  FREE_PTR_FROM_CONTEXT(access_ctx_, cs_encode_reader_, ObCSEncodeBlockGetReader);
  FREE_PTR_FROM_CONTEXT(access_ctx_, micro_getter_, ObMicroBlockRowGetter);
  is_inited_ = false;
  co_sstable_ = nullptr;
  access_ctx_ = nullptr;
  iter_param_ = nullptr;
  row_.reset();
  row_idx_datum_.reset();
  row_idx_key_.reset();
  row_getters_.reset();
  ObStoreRowIterator::reset();
}

void ObCGSSTableRowGetter::reuse()
{
  is_inited_ = false;
  row_.reuse();
  co_sstable_ = nullptr;
  ObStoreRowIterator::reuse();
}

int ObCGSSTableRowGetter::init(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObIndexTreePrefetcher &prefetcher,
    ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObCGSSTableRowGetter has been inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == query_range ||
                         nullptr == table ||
                         !table->is_co_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init ObCOSSTableRowGetter", K(ret), KP(query_range), KPC(table));
  } else {
    co_sstable_ = static_cast<ObCOSSTableV2 *>(table);
    iter_param_ = &iter_param;
    access_ctx_ = &access_ctx;
    row_getters_.set_allocator(access_ctx_->stmt_allocator_);
    ObSSTable *sstable = static_cast<ObSSTable *>(table);
    int64_t access_col_cnt = iter_param.get_out_col_cnt();
    if (OB_FAIL(row_getters_.prepare_reallocate(access_col_cnt))) {
      LOG_WARN("Failed to init row getters", K(ret), K(access_col_cnt));
    } else if (!row_.is_valid()) {
      if (OB_FAIL(row_.init(*access_ctx.stmt_allocator_, access_col_cnt))) {
        LOG_WARN("Failed to init storage datum row", K(ret), K(access_col_cnt));
      }
    } else if (OB_FAIL(row_.reserve(access_col_cnt))) {
      LOG_WARN("Failed to reserve datum row", K(ret), K(access_col_cnt));
    }

    if (OB_FAIL(ret)) {
    } else if (!prefetcher.is_valid()) {
      if (OB_FAIL(prefetcher.init(
                  type_, *sstable, iter_param, access_ctx, query_range))) {
        LOG_WARN("Fail to init prefetcher", K(ret));
      }
    } else if (OB_FAIL(prefetcher.switch_context(
                type_, *sstable, iter_param, access_ctx, query_range))) {
      LOG_WARN("Fail to switch context for prefetcher", K(ret));
    }
  }
  return ret;
}

int ObCGSSTableRowGetter::init_cg_param_pool(ObTableAccessContext &context)
{
  int ret = OB_SUCCESS;
  if (nullptr == cg_param_pool_) {
    if (OB_ISNULL(cg_param_pool_ = OB_NEWx(ObCGIterParamPool,
                                           context.stmt_allocator_,
                                           *context.stmt_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to alloc cg param map", K(ret));
    }
  }
  context.cg_param_pool_ = cg_param_pool_;
  return ret;
}

int ObCGSSTableRowGetter::prepare_reader(const ObRowStoreType store_type)
{
  int ret = OB_SUCCESS;
  reader_ = nullptr;
  if (FLAT_ROW_STORE == store_type) {
    if (nullptr == flat_reader_) {
      flat_reader_ = OB_NEWx(ObMicroBlockGetReader, access_ctx_->stmt_allocator_);
    }
    reader_ = flat_reader_;
  } else if (ENCODING_ROW_STORE == store_type || SELECTIVE_ENCODING_ROW_STORE == store_type) {
    if (nullptr == encode_reader_) {
      encode_reader_ = OB_NEWx(ObEncodeBlockGetReader, access_ctx_->stmt_allocator_);
    }
    reader_ = encode_reader_;
  } else if (CS_ENCODING_ROW_STORE == store_type) {
    if (nullptr == cs_encode_reader_) {
      cs_encode_reader_ = OB_NEWx(ObCSEncodeBlockGetReader, access_ctx_->stmt_allocator_);
    }
    reader_ = cs_encode_reader_;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported row store type", K(ret), K(store_type));
  }
  LOG_DEBUG("row store type", K(ret), K(store_type));
  if (OB_SUCC(ret) && OB_ISNULL(reader_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate reader", K(ret), K(store_type));
  }
  return ret;
}

int ObCGSSTableRowGetter::get_row_id(ObSSTableReadHandle &read_handle, ObCSRowId &row_id)
{
  int ret = OB_SUCCESS;
  if (ObSSTableRowState::NOT_EXIST == read_handle.row_state_) {
    row_id = OB_INVALID_CS_ROW_ID;
  } else if (ObSSTableRowState::IN_BLOCK == read_handle.row_state_) {
    int64_t cursor = -1;
    ObMicroBlockData block_data;
    if (OB_FAIL(read_handle.get_block_data(macro_block_reader_, block_data))) {
      LOG_WARN("Fail to get block data", K(ret), K(read_handle));
    } else if (OB_FAIL(prepare_reader(block_data.get_store_type()))) {
      LOG_WARN("Fail to prepare reader", K(ret), K(read_handle.micro_handle_->macro_block_id_));
    } else if (OB_FAIL(reader_->get_row_id(block_data, *read_handle.rowkey_, *iter_param_->get_read_info(), cursor))) {
      if (OB_BEYOND_THE_RANGE == ret) {
        row_id = OB_INVALID_CS_ROW_ID;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to get row id", K(ret), KPC(read_handle.rowkey_), K(read_handle.micro_handle_->macro_block_id_));
      }
    } else {
      row_id = read_handle.index_block_info_.get_row_range().start_row_id_ + cursor;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected read state", K(ret), K(read_handle.row_state_));
  }
  return ret;
}

int ObCGSSTableRowGetter::prepare_cg_row_getter(const ObCSRowId row_id, const ObNopPos *nop_pos, ObIArray<int32_t> &project_idxs)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<int32_t> *access_cgs = iter_param_->get_read_info()->get_cg_idxs();
  if (OB_UNLIKELY(0 > row_id || nullptr == access_cgs || nullptr == iter_param_->out_cols_project_ ||
                  (nullptr != iter_param_->output_exprs_ && iter_param_->output_exprs_->count() != iter_param_->out_cols_project_->count()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null param", K(ret), K(row_id), KPC_(iter_param));
  } else {
    const common::ObIArray<int32_t> &out_cols_project = *iter_param_->out_cols_project_;
    const ObITableReadInfo *read_info = iter_param_->get_read_info();
    int64_t rowkey_cnt = read_info->get_rowkey_count();
    const ObColumnIndexArray &cols_index = read_info->get_columns_index();
    int64_t column_cnt = (nullptr == nop_pos) ? iter_param_->get_out_col_cnt() : nop_pos->count();
    int64_t column_group_cnt = co_sstable_->get_cs_meta().get_column_group_count();
    ObSSTableWrapper table_wrapper;
    ObTableIterParam* cg_param = nullptr;
    int32_t cg_idx = INT32_MAX;
    row_idx_datum_.reuse();
    row_idx_datum_.set_int(row_id);
    row_idx_key_.assign(&row_idx_datum_, 1);
    for (int64_t i = 0;  OB_SUCC(ret) && i < column_cnt; i++) {
      int32_t col_pos = (nullptr == nop_pos) ? i : nop_pos->nops_[i];
      sql::ObExpr *expr = nullptr;
      if (-1 == cols_index.at(col_pos)) {
        row_.storage_datums_[col_pos].set_nop();
      } else if (cols_index.at(col_pos) < rowkey_cnt) {
        // no need to get rowkey column from cg sstable
      } else if (FALSE_IT(cg_idx = access_cgs->at(col_pos))) {
      } else if (cg_idx >= column_group_cnt) {
        // added column
        row_.storage_datums_[col_pos].set_nop();
      } else {
        int32_t access_idx = -1;
        for (int32_t out_col_idx = 0; out_col_idx < out_cols_project.count(); out_col_idx++) {
          if (out_cols_project.at(out_col_idx) == col_pos) {
            access_idx = out_col_idx;
            break;
          }
        }
        if (access_idx >= 0 && nullptr != iter_param_->output_exprs_) {
          expr = iter_param_->output_exprs_->at(access_idx);
        }
        row_getters_[col_pos].reuse();
        if (OB_FAIL(cg_param_pool_->get_iter_param(cg_idx, *iter_param_, expr, cg_param))) {
          LOG_WARN("Fail to get cg iter param", K(ret), K(cg_idx), K(access_idx), KPC_(iter_param));
        } else if (OB_FAIL(co_sstable_->fetch_cg_sstable(cg_idx, table_wrapper))) {
          LOG_WARN("Fail to get cg sstable", K(ret), K(cg_idx), KPC_(co_sstable));
        } else if (OB_FAIL(row_getters_[col_pos].init(*cg_param, *access_ctx_, table_wrapper, row_idx_key_))) {
          LOG_WARN("Fail to init cg row getter", K(ret));
        } else if (OB_FAIL(project_idxs.push_back(col_pos))) {
          LOG_WARN("Fail to push back expr idx", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObCGSSTableRowGetter::fetch_row(ObSSTableReadHandle &read_handle, const ObNopPos *nop_pos, const blocksstable::ObDatumRow *&store_row)
{
  int ret = OB_SUCCESS;
  ObCSRowId row_id = OB_INVALID_CS_ROW_ID;
  ObSEArray<int32_t, 16> project_idxs;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_row_id(read_handle, row_id))) {
    LOG_WARN("Fail to get row id", K(ret));
  } else if (OB_INVALID_CS_ROW_ID == row_id) {
    // not found
    if (OB_FAIL(get_not_exist_row(*read_handle.rowkey_, row_))){
      LOG_WARN("Fail to get not exist row", K(ret));
    }
  } else if (OB_FAIL(row_.reserve(iter_param_->get_out_col_cnt()))) {
    LOG_WARN("Failed to reserve row", K(ret));
  } else if (OB_FAIL(init_cg_param_pool(*access_ctx_))) {
    LOG_WARN("Fail to init cg param pool", K(ret));
  } else if (OB_FAIL(prepare_cg_row_getter(row_id, nop_pos, project_idxs))) {
    LOG_WARN("Fail to prepare cg row getter", K(ret), K(row_id));
  } else {
    const blocksstable::ObDatumRow *cg_row = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < project_idxs.count(); i++) {
      int32_t project_idx = project_idxs.at(i);
      if (OB_FAIL(row_getters_[project_idx].get_next_row(macro_block_reader_, cg_row))) {
        LOG_WARN("Fail to get next cg row", K(ret), K(project_idx));
      } else if (OB_UNLIKELY(nullptr == cg_row || cg_row->row_flag_.is_not_exist())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected cg row", K(ret), KPC(cg_row), K(project_idx), K(row_id));
      } else {
        // TODO: refactor later when more than one column get from cg sstable
        row_.storage_datums_[project_idx] = cg_row->storage_datums_[0];
      }
    }
    if (OB_SUCC(ret) && iter_param_->need_scn_) {
      if (OB_FAIL(fetch_rowkey_row(read_handle, store_row))) {
        LOG_WARN("Fail to fetch row", K(ret));
      } else if (OB_FAIL(set_row_scn(*iter_param_, store_row))) {
        LOG_WARN("failed to set row scn", K(ret));
      } else {
        int64_t trans_idx = iter_param_->get_read_info()->get_trans_col_index();
        row_.storage_datums_[trans_idx] = store_row->storage_datums_[trans_idx];
      }
    }

    if (OB_SUCC(ret)) {
      row_.mvcc_row_flag_.reset();
      row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      row_.count_ = iter_param_->get_out_col_cnt();
      for (int64_t i = 0; i < read_handle.rowkey_->get_datum_cnt(); i++) {
        row_.storage_datums_[i] = read_handle.rowkey_->datums_[i];
      }
    }
  }

  if (OB_SUCC(ret)) {
    store_row = &row_;
    EVENT_INC(ObStatEventIds::SSSTORE_READ_ROW_COUNT);
    LOG_DEBUG("inner get next row", KPC(store_row), KPC(read_handle.rowkey_));
  }
  return ret;
}

int ObCGSSTableRowGetter::fetch_rowkey_row(ObSSTableReadHandle &read_handle, const ObDatumRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (nullptr == micro_getter_) {
    if (nullptr == (micro_getter_ = OB_NEWx(ObMicroBlockRowGetter, access_ctx_->stmt_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate micro block row getter", K(ret));
    } else if (OB_FAIL(micro_getter_->init(*iter_param_, *access_ctx_, static_cast<ObSSTable *>(co_sstable_)))) {
      LOG_WARN("Fail to init micro block row getter", K(ret));
    }
  } else if (OB_FAIL(micro_getter_->switch_context(*iter_param_, *access_ctx_, static_cast<ObSSTable *>(co_sstable_)))) {
    LOG_WARN("Fail to switch context", K(ret));
  }
  LOG_DEBUG("start to fetch row", KPC_(read_handle.rowkey), K(read_handle));

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(micro_getter_->get_row(
              read_handle,
              store_row,
              &macro_block_reader_))) {
    LOG_WARN("Fail to get row", K(ret));
  }
  return ret;
}

int ObCGSSTableRowGetter::get_not_exist_row(const ObDatumRowkey &rowkey, ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row.reserve(rowkey.get_datum_cnt()))) {
    LOG_WARN("fail to reserve datum row", K(ret), K(rowkey.get_datum_cnt()));
  } else {
    row.row_flag_.reset();
    row_.mvcc_row_flag_.reset();
    row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
    row.count_ = rowkey.get_datum_cnt();
    //TODO maybe we do not need to copy the rowkey datum
    for (int64_t i = 0; i < rowkey.get_datum_cnt(); i++) {
      row.storage_datums_[i] = rowkey.datums_[i];
    }
  }
  return ret;
}

}
}
