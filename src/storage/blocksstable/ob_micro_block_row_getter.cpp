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

namespace oceanbase {
using namespace common;
using namespace storage;
using share::schema::ColumnMap;
namespace blocksstable {
/**
 * ----------------------------------------------------------ObMicroBlockRowFetcher---------------------------------------------------------------
 */
ObIMicroBlockRowFetcher::ObIMicroBlockRowFetcher()
    : param_(NULL),
      context_(NULL),
      sstable_(NULL),
      reader_(NULL),
      flat_reader_(NULL),
      multi_version_reader_(NULL),
      sparse_reader_(NULL),
      is_multi_version_(false),
      is_inited_(false)
{}

ObIMicroBlockRowFetcher::~ObIMicroBlockRowFetcher()
{
  if (NULL != flat_reader_) {
    flat_reader_->~ObMicroBlockGetReader();
    flat_reader_ = NULL;
  }
  if (NULL != multi_version_reader_) {
    multi_version_reader_->~ObMultiVersionBlockGetReader();
    multi_version_reader_ = NULL;
  }
  if (NULL != sparse_reader_) {
    sparse_reader_->~ObSparseMicroBlockGetReader();
    sparse_reader_ = NULL;
  }
}

int ObIMicroBlockRowFetcher::init(
    const storage::ObTableIterParam& param, storage::ObTableAccessContext& context, const storage::ObSSTable* sstable)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(ret), KP(sstable));
  } else {
    param_ = &param;
    context_ = &context;
    sstable_ = sstable;
    reader_ = NULL;
    flat_reader_ = NULL;
    multi_version_reader_ = NULL;
    sparse_reader_ = NULL;
    is_multi_version_ = sstable->is_multi_version_minor_sstable();
    is_inited_ = true;
  }
  return ret;
}

int ObIMicroBlockRowFetcher::prepare_reader(const ObFullMacroBlockMeta& macro_meta)
{
  int ret = OB_SUCCESS;
  const int16_t store_type = macro_meta.meta_->row_store_type_;
  reader_ = NULL;
  if (FLAT_ROW_STORE == store_type) {
    if (!is_multi_version_) {
      if (NULL == flat_reader_) {
        flat_reader_ = OB_NEWx(ObMicroBlockGetReader, context_->allocator_);
      }
      reader_ = flat_reader_;
    } else {
      if (NULL == multi_version_reader_) {
        multi_version_reader_ = OB_NEWx(ObMultiVersionBlockGetReader, context_->allocator_);
      }
      reader_ = multi_version_reader_;
    }
  } else if (SPARSE_ROW_STORE == store_type) {  // sparse storage
    if (NULL == sparse_reader_) {
      sparse_reader_ = OB_NEWx(ObSparseMicroBlockGetReader, context_->allocator_);
    }
    reader_ = sparse_reader_;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported row store type", K(ret), K(store_type));
  }
  LOG_DEBUG("row store type", K(ret), K(store_type));
  if (OB_SUCC(ret) && OB_ISNULL(reader_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate reader, ", K(ret), K(store_type));
  }
  return ret;
}

/**
 * ----------------------------------------------------------ObMicroBlockRowGetter---------------------------------------------------------------
 */
ObMicroBlockRowGetter::ObMicroBlockRowGetter() : allocator_(ObModIds::OB_STORE_ROW_GETTER)
{}

ObMicroBlockRowGetter::~ObMicroBlockRowGetter()
{}

int ObMicroBlockRowGetter::init(const ObTableIterParam& param, ObTableAccessContext& context, const ObSSTable* sstable)
{
  int ret = OB_SUCCESS;
  const ObColDescIArray* out_cols = nullptr;
  const ObIArray<int32_t>* projector = nullptr;
  const share::schema::ColumnMap* column_id_map = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid()) || OB_UNLIKELY(!context.is_valid()) || OB_ISNULL(sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param), K(context), KP(sstable));
  } else if (sstable->is_multi_version_minor_sstable() &&
             sstable->get_snapshot_version() > context.trans_version_range_.snapshot_version_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sstable", K(ret), K(sstable->get_snapshot_version()), K(context.trans_version_range_));
  } else if (OB_FAIL(param.get_out_cols(context.use_fuse_row_cache_, out_cols))) {
    LOG_WARN("fail to get out cols", K(ret));
  } else if (OB_FAIL(param.get_column_map(context.use_fuse_row_cache_, column_id_map))) {
    LOG_WARN("fail to get column id map", K(ret));
  } else if (OB_FAIL(param.get_projector(context.use_fuse_row_cache_, projector))) {
    LOG_WARN("fail to get projector", K(ret));
  } else if (OB_FAIL(column_map_.init(*context.allocator_,
                 param.schema_version_,
                 param.rowkey_cnt_,
                 0, /*store_column_count*/
                 *out_cols,
                 column_id_map,
                 projector,
                 sstable->get_multi_version_rowkey_type()))) {
    LOG_WARN("fail to init column map", K(ret));
  } else if (OB_FAIL(ObIMicroBlockRowFetcher::init(param, context, sstable))) {
    LOG_WARN("fail to init micro block row fecher, ", K(ret));
  } else {
    LOG_DEBUG("success to init micro block row getter, ", K(param));
  }
  return ret;
}

int ObMicroBlockRowGetter::get_row(const ObStoreRowkey& rowkey, const MacroBlockId macro_id, const int64_t file_id,
    const ObFullMacroBlockMeta& macro_meta, const ObMicroBlockData& block_data,
    const storage::ObSSTableRowkeyHelper* rowkey_helper, const storage::ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(prepare_reader(macro_meta))) {
    LOG_WARN("failed to prepare reader, ", K(ret), K(macro_meta.meta_->row_store_type_));
  } else if (OB_FAIL(
                 column_map_.rebuild(macro_meta, param_->need_build_column_map(column_map_.get_schema_version())))) {
    LOG_WARN("fail to rebuild column map", K(ret), K(macro_meta));
  } else {
    row_.row_val_.cells_ = reinterpret_cast<ObObj*>(obj_buf_);
    row_.row_val_.count_ = OB_ROW_MAX_COLUMNS_COUNT;
    row_.capacity_ = OB_ROW_MAX_COLUMNS_COUNT;
    if (!context_->enable_put_row_cache()) {
      if (OB_FAIL(reader_->get_row(
              context_->pkey_.get_tenant_id(), block_data, rowkey, column_map_, macro_meta, rowkey_helper, row_))) {
        if (OB_BEYOND_THE_RANGE == ret) {
          if (OB_FAIL(get_not_exist_row(rowkey, row))) {
            LOG_WARN("Fail to get not exist row, ", K(ret), K(rowkey));
          }
        } else {
          LOG_WARN("Fail to get row, ", K(ret), K(rowkey));
        }
      } else {
        // cast different type
        if (OB_UNLIKELY(!column_map_.is_all_column_matched())) {
          allocator_.reuse();
          const ObColumnIndexItem* column_indexs = column_map_.get_column_indexs();
          const int64_t request_count = column_map_.get_request_count();
          for (int64_t i = 0; OB_SUCC(ret) && i < request_count; ++i) {
            if (!column_indexs[i].is_column_type_matched_) {
              if (OB_FAIL(ObIRowReader::cast_obj(
                      column_indexs[i].request_column_type_, allocator_, row_.row_val_.cells_[i]))) {
                LOG_WARN("Fail to cast obj, ", K(ret), K(i), K(column_indexs[i]));
              }
            }
          }
        }

        if (OB_SUCC(ret)) {
          row = &row_;
          LOG_DEBUG("Success to get row, ", K(*row));
        }
      }
    } else {
      // need read full row to put row cache
      full_row_.row_val_.cells_ = reinterpret_cast<ObObj*>(full_row_obj_buf_);
      full_row_.row_val_.count_ = OB_ROW_MAX_COLUMNS_COUNT;
      full_row_.capacity_ = OB_ROW_MAX_COLUMNS_COUNT;
      ObRowCacheValue row_cache_value;
      if (OB_FAIL(reader_->get_row(
              context_->pkey_.get_tenant_id(), block_data, rowkey, macro_meta, rowkey_helper, full_row_))) {
        if (OB_BEYOND_THE_RANGE == ret) {
          if (OB_FAIL(get_not_exist_row(rowkey, row))) {
            LOG_WARN("Fail to get not exist row, ", K(ret), K(rowkey));
          }
        } else {
          LOG_WARN("Fail to get row, ", K(ret), K(rowkey));
        }
      } else {
        LOG_DEBUG("Success to get row, ", K(ret), K(rowkey), K(full_row_));
        if (OB_FAIL(row_cache_value.init(macro_meta, &full_row_, macro_id))) {
          STORAGE_LOG(WARN, "fail to init row cache value", K(ret), K(macro_meta));
        } else if (OB_FAIL(project_row(rowkey, row_cache_value, column_map_, row))) {
          STORAGE_LOG(WARN, "fail to project cache row", K(ret), K(rowkey), K(row_cache_value));
        } else {
          // put row cache, ignore fail
          ObRowCacheKey row_cache_key(param_->table_id_,
              file_id,
              rowkey,
              sstable_->is_major_sstable() ? 0 : sstable_->get_snapshot_version(),
              sstable_->get_key().table_type_);
          if (OB_SUCCESS == OB_STORE_CACHE.get_row_cache().put_row(row_cache_key, row_cache_value)) {
            context_->access_stat_.row_cache_put_cnt_++;
          }
          LOG_DEBUG("Success to get row, ", K(*row), K_(full_row), K(row_cache_value));
        }
      }
    }
  }
  return ret;
}

int ObMicroBlockRowGetter::get_cached_row(const ObStoreRowkey& rowkey, const ObFullMacroBlockMeta& macro_meta,
    const ObRowCacheValue& value, const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(
                 column_map_.rebuild(macro_meta, param_->need_build_column_map(column_map_.get_schema_version())))) {
    LOG_WARN("fail to rebuild column map", K(ret));
  } else {
    if (value.is_row_not_exist()) {
      // not exist row
      if (OB_FAIL(get_not_exist_row(rowkey, row))) {
        LOG_WARN("Fail to get not exist row, ", K(ret), K(rowkey));
      }
    } else {
      if (OB_FAIL(project_row(rowkey, value, column_map_, row))) {
        LOG_WARN("fail to project cache row, ", K(ret));
      } else {
        LOG_DEBUG("success to get cache row, ", K(ret), K(*row));
      }
    }
  }
  return ret;
}

int ObMicroBlockRowGetter::get_not_exist_row(const common::ObStoreRowkey& rowkey, const storage::ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t request_cnt = column_map_.get_request_count();
    const int64_t rowkey_cnt = rowkey.get_obj_cnt();
    row_.row_val_.cells_ = reinterpret_cast<ObObj*>(obj_buf_);
    row_.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
    row_.row_val_.count_ = request_cnt;

    for (int64_t i = 0; i < rowkey_cnt; i++) {
      row_.row_val_.cells_[i] = rowkey.get_obj_ptr()[i];
    }
    for (int64_t i = rowkey_cnt; i < request_cnt; i++) {
      row_.row_val_.cells_[i].set_nop_value();
    }

    row = &row_;
  }
  return ret;
}

int ObMicroBlockRowGetter::project_row(const ObStoreRowkey& rowkey, const ObRowCacheValue& value,
    const ObColumnMap& column_map, const storage::ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value.get_column_ids())) {  // flat row
    if (OB_FAIL(project_cache_row(rowkey, value, column_map, row))) {
      LOG_WARN("project dense cache row failed", K(ret), K(row), K(rowkey));
    }
  } else {  // sparse row
    if (OB_FAIL(project_cache_sparse_row(rowkey, value, column_map, row))) {
      LOG_WARN("project sparse cache row failed", K(ret), K(row), K(rowkey));
    }
  }
  return ret;
}

int ObMicroBlockRowGetter::project_cache_row(const ObStoreRowkey& rowkey, const ObRowCacheValue& value,
    const ObColumnMap& column_map, const storage::ObStoreRow*& row)
{
  UNUSED(rowkey);
  int ret = OB_SUCCESS;
  int64_t store_index = 0;
  common::ObObj* const obj_array = value.get_obj_array();
  const int64_t column_cnt = value.get_column_cnt();
  const ObColumnIndexItem* column_indexs = column_map.get_column_indexs();
  const int64_t request_count = column_map.get_request_count();
  row_.row_val_.cells_ = reinterpret_cast<ObObj*>(obj_buf_);
  row_.row_val_.count_ = OB_ROW_MAX_COLUMNS_COUNT;

  for (int64_t i = 0; OB_SUCC(ret) && i < request_count; ++i) {
    store_index = column_indexs[i].store_index_;
    if (OB_INVALID_INDEX != store_index) {
      if (store_index < column_cnt) {
        row_.row_val_.cells_[i] = obj_array[store_index];
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected store index, ", K(ret), K(store_index), K(column_cnt));
      }
    } else {
      row_.row_val_.cells_[i].set_nop_value();
    }
  }

  // cast different type
  if (OB_UNLIKELY(!column_map.is_all_column_matched())) {
    allocator_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < request_count; ++i) {
      if (!column_indexs[i].is_column_type_matched_) {
        if (OB_FAIL(
                ObIRowReader::cast_obj(column_indexs[i].request_column_type_, allocator_, row_.row_val_.cells_[i]))) {
          LOG_WARN("Fail to cast obj, ", K(ret), K(i), K(column_indexs[i]));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    row_.row_val_.count_ = request_count;
    row_.flag_ = value.get_flag();
    row_.set_row_dml(value.get_row_dml());
    row = &row_;
  }
  return ret;
}

int ObMicroBlockRowGetter::project_cache_sparse_row(const ObStoreRowkey& rowkey, const ObRowCacheValue& value,
    const ObColumnMap& column_map, const storage::ObStoreRow*& row)
{
  UNUSED(rowkey);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_map.get_column_indexs()) || OB_ISNULL(column_map.get_cols_map()) ||
      column_map.get_request_count() <= 0 || OB_ISNULL(value.get_obj_array()) || OB_ISNULL(value.get_column_ids())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(column_map), K(value));
  } else {
    common::ObObj* const obj_array = value.get_obj_array();
    const int64_t column_cnt = value.get_column_cnt();
    const uint16_t* col_id_array = value.get_column_ids();
    const ObColumnIndexItem* column_indexs = column_map.get_column_indexs();
    const ColumnMap* col_id_map = column_map.get_cols_map();
    const int64_t request_count = column_map.get_request_count();
    row_.row_val_.cells_ = reinterpret_cast<ObObj*>(obj_buf_);
    row_.capacity_ = OB_ROW_MAX_COLUMNS_COUNT;

    for (int64_t i = 0; OB_SUCC(ret) && i < request_count; ++i) {  // set nop
      row_.row_val_.cells_[i].set_nop_value();
    }
    int32_t col_pos = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      if (OB_FAIL(col_id_map->get(col_id_array[i], col_pos))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get column pos failed", K(ret), "column_id", col_id_array[i]);
        }
      } else if (OB_INVALID_INDEX != col_pos) {
        if (col_pos > request_count) {
          LOG_WARN("invalid column pos", K(ret), "column_id", col_id_array[i], K(col_pos));
        } else {
          row_.row_val_.cells_[col_pos] = obj_array[i];
        }
      }
    }

    // cast different type
    if (OB_SUCC(ret)) {
      allocator_.reuse();
      for (int64_t i = 0; OB_SUCC(ret) && i < request_count; ++i) {
        if (!row_.row_val_.cells_[i].is_nop_value() && !row_.row_val_.cells_[i].is_null() &&
            column_indexs[i].request_column_type_ != row_.row_val_.cells_[i].get_meta()) {
          if (OB_FAIL(
                  ObIRowReader::cast_obj(column_indexs[i].request_column_type_, allocator_, row_.row_val_.cells_[i]))) {
            LOG_WARN("Fail to cast obj, ", K(ret), K(i), K(column_indexs[i]));
          }
        }
      }  // end for
    }

    if (OB_SUCC(ret)) {
      row_.row_val_.count_ = request_count;
      row_.flag_ = value.get_flag();
      row_.set_row_dml(value.get_row_dml());
      row_.is_sparse_row_ = false;
      row = &row_;
    }
  }
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
