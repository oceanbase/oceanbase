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
#include "ob_micro_block_row_getter.h"
#include "ob_macro_block_reader.h"
#include "index_block/ob_index_block_row_scanner.h"
#include "storage/access/ob_sstable_row_getter.h"
#include "storage/access/ob_index_tree_prefetcher.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/blocksstable/cs_encoding/ob_micro_block_cs_decoder.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"

namespace oceanbase
{
namespace blocksstable
{
/**
 * --------------------------------------------ObMicroBlockRowFetcher-------------------------------------------------
 */
ObIMicroBlockRowFetcher::ObIMicroBlockRowFetcher()
  : param_(nullptr),
    context_(nullptr),
    sstable_(nullptr),
    reader_(nullptr),
    flat_reader_(nullptr),
    encode_reader_(nullptr),
    cs_encode_reader_(nullptr),
    read_info_(nullptr),
    long_life_allocator_(nullptr),
    is_inited_(false)
{}

ObIMicroBlockRowFetcher::~ObIMicroBlockRowFetcher()
{
  FREE_ITER_FROM_ALLOCATOR(long_life_allocator_, flat_reader_, ObMicroBlockGetReader);
  FREE_ITER_FROM_ALLOCATOR(long_life_allocator_, encode_reader_, ObEncodeBlockGetReader);
  FREE_ITER_FROM_ALLOCATOR(long_life_allocator_, cs_encode_reader_, ObCSEncodeBlockGetReader);
}

int ObIMicroBlockRowFetcher::init(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const blocksstable::ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid() || !context.is_valid() || nullptr == sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param), K(context), KP(sstable));
  } else if (OB_ISNULL(long_life_allocator_ = context.get_long_life_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpected null long life allocator", K(ret));
  } else if (OB_ISNULL(read_info_ = param.get_read_info(
              !sstable->is_normal_cg_sstable() &&
              (context.enable_put_row_cache() || context.use_fuse_row_cache_) &&
              param.read_with_same_schema()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null read info", K(ret), K(sstable->is_normal_cg_sstable()),
             K(context.use_fuse_row_cache_), K(context.enable_put_row_cache()),
             K(param.read_with_same_schema()), K(param));
  } else {
    param_ = &param;
    context_ = &context;
    sstable_ = sstable;
    reader_ = nullptr;
    flat_reader_ = nullptr;
    encode_reader_ = nullptr;
    is_inited_ = true;
  }
  return ret;
}

int ObIMicroBlockRowFetcher::switch_context(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const blocksstable::ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid() || !context.is_valid() || nullptr == sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param), K(context), KP(sstable));
  } else if (OB_ISNULL(long_life_allocator_ = context.get_long_life_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpected null long life allocator", K(ret));
  } else if (OB_ISNULL(read_info_ = param.get_read_info(
              !sstable->is_normal_cg_sstable() &&
              (context.enable_put_row_cache() || context.use_fuse_row_cache_) &&
              param.read_with_same_schema()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null read info", K(ret), K(sstable->is_normal_cg_sstable()),
             K(context.use_fuse_row_cache_), K(context.enable_put_row_cache()),
             K(param.read_with_same_schema()), K(param));
  } else {
    param_ = &param;
    context_ = &context;
    sstable_ = sstable;
  }
  return ret;
}

int ObIMicroBlockRowFetcher::prepare_reader(const ObRowStoreType store_type)
{
  int ret = OB_SUCCESS;
  reader_ = nullptr;
  if (FLAT_ROW_STORE == store_type) {
    if (nullptr == flat_reader_) {
      flat_reader_ = OB_NEWx(ObMicroBlockGetReader, long_life_allocator_);
    }
    reader_ = flat_reader_;
  } else if (ENCODING_ROW_STORE == store_type || SELECTIVE_ENCODING_ROW_STORE == store_type) {
    if (OB_LIKELY(!sstable_->is_multi_version_minor_sstable())) {
      if (nullptr == encode_reader_) {
        encode_reader_ = OB_NEWx(ObEncodeBlockGetReader, long_life_allocator_);
      }
      reader_ = encode_reader_;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported multi version encode store type", K(ret), K(store_type));
    }
  } else if (CS_ENCODING_ROW_STORE == store_type) {
    if (OB_LIKELY(!sstable_->is_multi_version_minor_sstable())) {
      if (nullptr == cs_encode_reader_) {
        cs_encode_reader_ = OB_NEWx(ObCSEncodeBlockGetReader, long_life_allocator_);
      }
      reader_ = cs_encode_reader_;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported multi version encode store type", K(ret), K(store_type));
    }
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

/**
 * --------------------------------------------ObMicroBlockRowGetter-------------------------------------------------
 */
int ObMicroBlockRowGetter::init(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() ||
                         !context.is_valid() ||
                         nullptr == sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param), K(context), KP(sstable));
  } else if (sstable->is_multi_version_minor_sstable() &&
             sstable->get_upper_trans_version() > context.trans_version_range_.snapshot_version_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sstable", K(ret), KPC(sstable), K(context.trans_version_range_));
  } else if (OB_FAIL(ObIMicroBlockRowFetcher::init(param, context, sstable))) {
    LOG_WARN("fail to init micro block row fecher", K(ret));
  } else if (OB_FAIL(row_.init(*long_life_allocator_, param.get_buffered_request_cnt(read_info_)))) {
    LOG_WARN("Failed to init datum row", K(ret));
  } else if (context.enable_put_row_cache() && param.read_with_same_schema() &&
             OB_FAIL(cache_project_row_.init(*long_life_allocator_, param.get_buffered_request_cnt(read_info_)))) {
    STORAGE_LOG(WARN, "Failed to init cache project row", K(ret));
  } else {
    LOG_DEBUG("success to init micro block row getter", K(param));
  }

  return ret;
}

int ObMicroBlockRowGetter::switch_context(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const blocksstable::ObSSTable *sstable)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockRowGetter is not inited", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())
      || OB_UNLIKELY(!context.is_valid())
      || OB_ISNULL(sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param), K(context), KP(sstable));
  } else if (sstable->is_multi_version_minor_sstable()
             && sstable->get_upper_trans_version() > context.trans_version_range_.snapshot_version_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sstable", K(ret), KPC(sstable), K(context.trans_version_range_));
  } else if (OB_FAIL(ObIMicroBlockRowFetcher::switch_context(param, context, sstable))) {
    LOG_WARN("fail to switch context micro block row fecher, ", K(ret));
  } else {
    if (context.enable_put_row_cache() && param.read_with_same_schema()) {
      if (cache_project_row_.is_valid()) {
      } else if (OB_FAIL(cache_project_row_.init(*long_life_allocator_, param.get_buffered_request_cnt(read_info_)))) {
        STORAGE_LOG(WARN, "Failed to init cache project row", K(ret));
      }
    }
  }
  return ret;
}

int ObMicroBlockRowGetter::get_row(
    ObSSTableReadHandle &read_handle,
    const ObDatumRow *&store_row,
    ObMacroBlockReader *block_reader)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init micro block row getter", K(ret));
  } else if (OB_UNLIKELY(!read_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(read_handle));
  } else {
    switch (read_handle.row_state_) {
      case ObSSTableRowState::NOT_EXIST:
        if (OB_FAIL(get_not_exist_row(read_handle.get_rowkey(), store_row))) {
          LOG_WARN("fail to get not exist row", K(ret), K(read_handle.get_rowkey()));
        }
        break;
      case ObSSTableRowState::IN_ROW_CACHE:
        if (OB_FAIL(get_cached_row(
                    read_handle.get_rowkey(),
                    *read_handle.row_handle_.row_value_,
                    store_row))) {
          LOG_WARN("fail to get cache row", K(ret), K(read_handle.get_rowkey()));
        }
        break;
      case ObSSTableRowState::IN_BLOCK:
        if (OB_ISNULL(block_reader)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null block reader", K(ret), KP(block_reader));
        } else if (OB_FAIL(get_block_row(read_handle, *block_reader, store_row))) {
          LOG_WARN("Fail to get block row", K(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid row state", K(ret), K(read_handle.row_state_));
    }
  }

  if (OB_SUCC(ret)) {
    (const_cast<ObDatumRow*> (store_row))->scan_index_ = read_handle.range_idx_;
    LOG_DEBUG("get row", K(*store_row), K(read_handle.row_state_), K(read_handle.get_rowkey()));
  }
  return ret;
}

int ObMicroBlockRowGetter::get_block_row(
    ObSSTableReadHandle &read_handle,
    ObMacroBlockReader &block_reader,
    const ObDatumRow *&store_row
)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData block_data;
  if (OB_FAIL(read_handle.get_block_data(block_reader, block_data))) {
    LOG_WARN("Fail to get block data", K(ret), K(read_handle));
  } else if (OB_FAIL(inner_get_row(
              read_handle.micro_handle_->macro_block_id_,
              read_handle.get_rowkey(),
              block_data,
              store_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get block row", K(ret), K(read_handle.get_rowkey()));
    }
  } else {
    if (store_row->row_flag_.is_not_exist()) {
      ++context_->table_store_stat_.empty_read_cnt_;
      EVENT_INC(ObStatEventIds::GET_ROW_EMPTY_READ);
      if (!context_->query_flag_.is_index_back()
          && context_->query_flag_.is_use_bloomfilter_cache()
          && !sstable_->is_small_sstable()) {
        (void) OB_STORE_CACHE.get_bf_cache().inc_empty_read(
            MTL_ID(),
            param_->table_id_,
            read_handle.micro_handle_->macro_block_id_,
            read_handle.get_rowkey().get_datum_cnt());
      }
    } else {
      EVENT_INC(ObStatEventIds::GET_ROW_EFFECT_READ);
    }
  }


  return ret;
}

int ObMicroBlockRowGetter::get_cached_row(
    const ObDatumRowkey &rowkey,
    const ObRowCacheValue &value,
    const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (value.is_row_not_exist()) {
    //not exist row
    if (OB_FAIL(get_not_exist_row(rowkey, row))) {
      LOG_WARN("Fail to get not exist row", K(ret), K(rowkey));
    }
  } else if (OB_FAIL(project_cache_row(value, row_))) {
    LOG_WARN("fail to project cache row", K(ret));
  } else {
    row = &row_;
    LOG_DEBUG("success to get cache row", K(ret), K(*row));
  }
  return ret;
}

int ObMicroBlockRowGetter::project_cache_row(const ObRowCacheValue &value, ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const ObITableReadInfo *read_info = nullptr;
  if (OB_ISNULL(read_info = param_->get_read_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null read_info", K(ret), K_(param));
  } else if (OB_FAIL(row.reserve(read_info->get_request_count()))) {
    LOG_WARN("fail to reserve memory for datum row", K(ret), K(read_info->get_request_count()));
  } else {
    const int64_t request_cnt = read_info->get_request_count();
    const ObColumnIndexArray &cols_index = read_info->get_columns_index();
    row.row_flag_ = value.get_flag();
    row.count_ = read_info->get_request_count();
    ObStorageDatum *const datums = value.get_datums();
    const int64_t column_cnt = value.get_column_cnt();
    for (int64_t i = 0; OB_SUCC(ret) && i < request_cnt; i++) {
      if (cols_index.at(i) < column_cnt && cols_index.at(i) >= 0) {
        row.storage_datums_[i] = datums[cols_index.at(i)];
      } else {
        // new added col
        row.storage_datums_[i].set_nop();
      }
    }
  }
  return ret;
}

int ObMicroBlockRowGetter::inner_get_row(
    const MacroBlockId &macro_id,
    const ObDatumRowkey &rowkey,
    const ObMicroBlockData &block_data,
    const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == read_info_ || !read_info_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid read_info", K(ret), KPC_(read_info));
  } else if (OB_FAIL(prepare_reader(block_data.get_store_type()))) {
    LOG_WARN("failed to prepare reader", K(ret), K(macro_id));
  } else {
    if (OB_FAIL(row_.reserve(read_info_->get_request_count()))) {
      LOG_WARN("fail to reserve memory for datum row", K(ret), K(read_info_->get_request_count()));
    } else if (OB_FAIL(reader_->get_row(block_data, rowkey, *read_info_, row_))) {
      if (OB_BEYOND_THE_RANGE == ret) {
        if (OB_FAIL(get_not_exist_row(rowkey, row))) {
          LOG_WARN("Fail to get not exist row", K(ret), K(rowkey), K(macro_id));
        }
        STORAGE_LOG(DEBUG, "get not exist row", K(rowkey), K(macro_id));
      } else {
        LOG_WARN("Fail to get row", K(ret), K(rowkey), K(block_data), KPC_(read_info),
                 KPC_(param), KPC_(context), K(macro_id));
      }
    } else {
      row = &row_;
      LOG_DEBUG("Success to get row", K(ret), K(rowkey), K(row_), KPC_(read_info),
                K(context_->enable_put_row_cache()), K(context_->use_fuse_row_cache_), K(macro_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (context_->use_fuse_row_cache_) {
    // fuse row cache bypass the row cache
  } else if (context_->enable_put_row_cache() && param_->read_with_same_schema()) {
    ObRowCacheValue row_cache_value;
    if (OB_FAIL(row_cache_value.init(sstable_->get_key().get_start_scn().get_val_for_tx(), row_))) {
      LOG_WARN("fail to init row cache value", K(ret), K(row_));
    } else {
      //put row cache, ignore fail
      ObRowCacheKey row_cache_key(
          MTL_ID(),
          param_->tablet_id_,
          rowkey,
          read_info_->get_datum_utils(),
          sstable_->get_data_version(),
          sstable_->get_key().table_type_);
      if (OB_SUCCESS == OB_STORE_CACHE.get_row_cache().put_row(row_cache_key, row_cache_value)) {
        context_->table_store_stat_.row_cache_put_cnt_++;
      }

      if (OB_FAIL(project_cache_row(row_cache_value, cache_project_row_))) {
        LOG_WARN("fail to project cache row", K(ret), K(row_cache_value));
      } else {
        row = &cache_project_row_;
        LOG_DEBUG("Success to get row", K(ret), K(rowkey), K(row_), K(row_cache_value), K(macro_id));
      }
    }
  }
  return ret;
}

int ObMicroBlockRowGetter::get_not_exist_row(const ObDatumRowkey &rowkey, const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_FAIL(row_.reserve(rowkey.get_datum_cnt()))) {
      LOG_WARN("fail to reserve datum row", K(ret), K(rowkey.get_datum_cnt()));
    } else {
      row_.count_ = rowkey.get_datum_cnt();
      row_.row_flag_.reset();
      row_.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
      //TODO maybe we do not need to copy the rowkey datum
      for (int64_t i = 0; i < rowkey.get_datum_cnt(); i++) {
        row_.storage_datums_[i] = rowkey.datums_[i];
      }
      row = &row_;
    }
  }
  return ret;
}

/**
 * --------------------------------------------ObMicroBlockCGRowGetter-------------------------------------------------
 */
int ObMicroBlockCGRowGetter::init(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const blocksstable::ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(ObIMicroBlockRowFetcher::init(param, context, sstable))) {
    LOG_WARN("fail to init micro block row fecher", K(ret));
  } else if (OB_FAIL(row_.init(*context.stmt_allocator_, read_info_->get_request_count()))) {
    LOG_WARN("Failed to init datum row", K(ret));
  }
  return ret;
}

int ObMicroBlockCGRowGetter::switch_context(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const blocksstable::ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMicroBlockRowGetter is not inited", K(ret));
  } else if (OB_FAIL(ObIMicroBlockRowFetcher::switch_context(param, context, sstable))) {
    LOG_WARN("fail to switch context micro block row fecher, ", K(ret));
  }
  return ret;
}

int ObMicroBlockCGRowGetter::get_row(
    ObSSTableReadHandle &read_handle,
    ObMacroBlockReader &block_reader,
    const uint32_t row_idx,
    const ObDatumRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init micro block row getter", K(ret));
  } else if (OB_UNLIKELY(!read_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(read_handle));
  } else {
    switch (read_handle.row_state_) {
      case ObSSTableRowState::NOT_EXIST:
        if (OB_FAIL(get_not_exist_row(store_row))) {
          LOG_WARN("Fail to get not exist row", K(ret));
        }
        break;
      case ObSSTableRowState::IN_BLOCK:
        if (OB_FAIL(get_block_row(read_handle, block_reader, row_idx, store_row))) {
          LOG_WARN("Fail to get block row", K(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid row state", K(ret), K(read_handle.row_state_));
    }
  }

  if (OB_SUCC(ret)) {
    (const_cast<ObDatumRow*> (store_row))->scan_index_ = read_handle.range_idx_;
    LOG_DEBUG("get row", KPC(store_row), K(row_idx), K(read_handle.row_state_), "macro_id", read_handle.micro_handle_->macro_block_id_);
  }
  return ret;
}

int ObMicroBlockCGRowGetter::get_block_row(
    ObSSTableReadHandle &read_handle,
    ObMacroBlockReader &block_reader,
    const uint32_t row_idx,
    const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData block_data;
  const MacroBlockId &macro_id = read_handle.micro_handle_->macro_block_id_;
  if (OB_FAIL(read_handle.get_block_data(block_reader, block_data))) {
    LOG_WARN("Fail to get block data", K(ret), K(read_handle));
  } else if (OB_FAIL(prepare_reader(block_data.get_store_type()))) {
    LOG_WARN("Failed to prepare reader", K(ret), K(macro_id));
  } else if (OB_FAIL(row_.reserve(read_info_->get_request_count()))) {
    LOG_WARN("Fail to reserve memory for datum row", K(ret), K(read_info_->get_request_count()));
  } else if (OB_FAIL(reader_->get_row(block_data, *read_info_, row_idx, row_))) {
    LOG_WARN("Fail to get cs row", K(ret), K(row_idx), K(block_data), KPC_(read_info), K(macro_id));
  } else {
    row = &row_;
    LOG_DEBUG("Success to get row", K(ret), K(row_idx), K(row_), KPC_(read_info), K(macro_id));
  }
  return ret;
}

int ObMicroBlockCGRowGetter::get_not_exist_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row_.count_ = 0;
  row_.mvcc_row_flag_.reset();
  row_.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
  row = &row_;
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
