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

#include "ob_column_map.h"
#include "lib/utility/ob_preload.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
namespace oceanbase {
using namespace common;
namespace blocksstable {
ObColumnMap::ObColumnMap()
    : request_count_(0),
      store_count_(0),
      rowkey_store_count_(0),
      schema_version_(0),
      cols_id_map_(nullptr),
      seq_read_column_count_(0),
      cur_idx_(0),
      column_indexs_(NULL),
      is_inited_(false),
      is_all_column_matched_(false),
      multi_version_rowkey_cnt_(0),
      create_col_id_map_(false),
      allocator_(nullptr)
{}

// make sure all parameters are valid
int ObColumnMap::init_column_index(const uint64_t col_id, const int64_t schema_rowkey_cnt, const int64_t store_index,
    const int64_t store_cnt, const bool is_multi_version, ObColumnIndexItem* column_index)
{
  int ret = OB_SUCCESS;
  if (store_index >= OB_ROW_MAX_COLUMNS_COUNT || (store_cnt > 0 && store_index >= store_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid store index", K(store_index), K(store_cnt));
  } else if (OB_HIDDEN_TRANS_VERSION_COLUMN_ID == col_id) {
    if (is_multi_version) {
      column_index->store_index_ = storage::ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(
          schema_rowkey_cnt, multi_version_rowkey_cnt_);
    } else {
      column_index->store_index_ = -1;
    }
  } else if (OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID == col_id) {
    if (is_multi_version) {
      column_index->store_index_ = storage::ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(
          schema_rowkey_cnt, multi_version_rowkey_cnt_);
    } else {
      column_index->store_index_ = -1;
    }
  } else if (store_index >= schema_rowkey_cnt) {
    column_index->store_index_ = static_cast<int16_t>(store_index + multi_version_rowkey_cnt_);
  } else {
    column_index->store_index_ = static_cast<int16_t>(store_index);
  }
  return ret;
}

int ObColumnMap::init(common::ObIAllocator& allocator, const int64_t schema_version, const int64_t schema_rowkey_cnt,
    const int64_t store_cnt, const common::ObIArray<share::schema::ObColDesc>& out_cols,
    const share::schema::ColumnMap* cols_id_map, const common::ObIArray<int32_t>* projector,
    const int multi_version_rowkey_type /*  = storage::ObMultiVersionRowkeyHelpper::MVRC_NONE*/)
{
  int ret = OB_SUCCESS;
  const int64_t out_cols_cnt = out_cols.count();

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObColumnMap has been inited, ", K(ret));
  } else if (OB_UNLIKELY(schema_version < 0 || schema_rowkey_cnt <= 0 || store_cnt < 0 ||
                         (store_cnt > 0 && store_cnt < schema_rowkey_cnt) || store_cnt > OB_ROW_MAX_COLUMNS_COUNT ||
                         out_cols_cnt <= 0 || out_cols_cnt > OB_ROW_MAX_COLUMNS_COUNT ||
                         multi_version_rowkey_type >= storage::ObMultiVersionRowkeyHelpper::MVRC_VERSION_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "Invalid argument, ", K(ret), K(schema_version), K(schema_rowkey_cnt), K(store_cnt), K(out_cols_cnt));
  } else if (OB_ISNULL(column_indexs_ = reinterpret_cast<ObColumnIndexItem*>(
                           allocator.alloc(sizeof(ObColumnIndexItem) * out_cols_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate column index, ", K(ret));
  } else {
    bool is_multi_version = multi_version_rowkey_type != storage::ObMultiVersionRowkeyHelpper::MVRC_NONE;
    multi_version_rowkey_cnt_ =
        storage::ObMultiVersionRowkeyHelpper::get_multi_version_rowkey_cnt(multi_version_rowkey_type);
    if (nullptr == projector) {
      for (int64_t i = 0; OB_SUCC(ret) && i < out_cols_cnt; ++i) {
        column_indexs_[i].column_id_ = out_cols.at(i).col_id_;
        column_indexs_[i].request_column_type_ = out_cols.at(i).col_type_;
        column_indexs_[i].macro_column_type_ = out_cols.at(i).col_type_;
        if (OB_FAIL(init_column_index(
                out_cols.at(i).col_id_, schema_rowkey_cnt, i, store_cnt, is_multi_version, &column_indexs_[i]))) {
          STORAGE_LOG(WARN, "init column index failed ", K(ret), K(out_cols.at(i).col_id_), K(i));
        } else {
          column_indexs_[i].is_column_type_matched_ = true;
        }
      }
      if (is_multi_version) {
        // bloom filter will probably need the prefix of rowkey
        seq_read_column_count_ = std::min(schema_rowkey_cnt, out_cols_cnt);
      } else {
        seq_read_column_count_ = out_cols_cnt;
      }
    } else {
      if (OB_UNLIKELY(projector->count() != out_cols_cnt)) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(out_cols_cnt), K(projector->count()));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < out_cols_cnt; ++i) {
          column_indexs_[i].column_id_ = out_cols.at(i).col_id_;
          column_indexs_[i].request_column_type_ = out_cols.at(i).col_type_;
          column_indexs_[i].macro_column_type_ = out_cols.at(i).col_type_;
          if (OB_FAIL(init_column_index(out_cols.at(i).col_id_,
                  schema_rowkey_cnt,
                  projector->at(i),
                  store_cnt,
                  is_multi_version,
                  &column_indexs_[i]))) {
            STORAGE_LOG(WARN, "init column index failed ", K(ret), K(out_cols.at(i).col_id_), K(projector->at(i)));
          } else {
            column_indexs_[i].is_column_type_matched_ = (column_indexs_[i].store_index_ >= 0);
            if (seq_read_column_count_ == i && column_indexs_[i].store_index_ == seq_read_column_count_ &&
                column_indexs_[i].is_column_type_matched_) {
              ++seq_read_column_count_;
            }
          }
        }
      }
    }
    // create col_id_map
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(cols_id_map)) {
        void* buf = static_cast<share::schema::ColumnMap*>(allocator.alloc(sizeof(share::schema::ColumnMap)));
        if (OB_ISNULL(buf)) {  // alloc failed
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "Fail to allocate col id map", K(ret));
        } else {
          share::schema::ColumnMap* cols_id_map_ptr = new (buf) share::schema::ColumnMap(allocator);
          if (OB_FAIL(cols_id_map_ptr->init(out_cols))) {
            allocator.free(cols_id_map_ptr);
            STORAGE_LOG(WARN, "Fail to init col id map", K(ret));
          } else {
            create_col_id_map_ = true;  // set create flag
            cols_id_map_ = cols_id_map_ptr;
          }
        }
      } else {
        cols_id_map_ = cols_id_map;
      }
    }
  }

  if (OB_SUCC(ret)) {
    request_count_ = out_cols_cnt;
    store_count_ = store_cnt;
    rowkey_store_count_ = schema_rowkey_cnt;
    schema_version_ = schema_version;
    is_all_column_matched_ = true;
    is_inited_ = true;
    allocator_ = &allocator;
  }
  return ret;
}

int ObColumnMap::rebuild(const ObFullMacroBlockMeta& full_meta, bool force_rebuild)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  const ObMacroBlockMetaV2* macro_meta = full_meta.meta_;
  const ObMacroBlockSchemaInfo* macro_schema = full_meta.schema_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObColumnMap has not been inited, ", K(ret));
  } else if (!full_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(full_meta));
  } else if (SPARSE_ROW_STORE == macro_meta->row_store_type_) {  // if is sparse row skip it
    // do nothing
    STORAGE_LOG(DEBUG, "Column Map of Sparse row store type", K(ret));
  } else if (macro_meta->schema_version_ > 0 && schema_version_ == macro_meta->schema_version_ && !force_rebuild) {
    // same schema
    store_count_ = macro_meta->column_number_;
  } else if (OB_FAIL(column_hash_.init(macro_meta->column_number_, macro_schema->column_id_array_, *allocator_))) {
    STORAGE_LOG(WARN, "failed to init column hash", K(ret), K(macro_meta));
  } else {
    is_all_column_matched_ = true;
    seq_read_column_count_ = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < request_count_; ++i) {
      column_hash_.get_index(static_cast<uint16_t>(column_indexs_[i].column_id_), idx);
      if (OB_SUCC(ret)) {
        column_indexs_[i].store_index_ = static_cast<int16_t>(idx);
        if (idx >= 0) {
          column_indexs_[i].macro_column_type_ = full_meta.schema_->column_type_array_[idx];
          if (column_indexs_[i].request_column_type_ == column_indexs_[i].macro_column_type_) {
            column_indexs_[i].is_column_type_matched_ = true;
          } else {
            // different column type
            column_indexs_[i].is_column_type_matched_ = false;
            is_all_column_matched_ = false;
          }
        } else {
          // not exist column
          column_indexs_[i].is_column_type_matched_ = false;
        }
        if (seq_read_column_count_ == i && column_indexs_[i].store_index_ == seq_read_column_count_ &&
            column_indexs_[i].is_column_type_matched_) {
          ++seq_read_column_count_;
        }
      }
    }

    if (OB_SUCC(ret)) {
      schema_version_ = macro_meta->schema_version_;
      store_count_ = macro_meta->column_number_;
    }
  }
  return ret;
}

void ObColumnMap::reset()
{
  reuse();
  column_hash_.reset();
  schema_version_ = 0;
  column_indexs_ = NULL;
  request_count_ = 0;
}

void ObColumnMap::reuse()
{
  // here we have an assumption that the request columns won't change during
  // the life cycle of a StoreRowIterator, so we don't reset request_count_ and
  // column_indexs_ in reuse
  store_count_ = 0;
  cur_idx_ = 0;
  rowkey_store_count_ = 0;
  seq_read_column_count_ = 0;
  is_inited_ = false;
  is_all_column_matched_ = false;
  multi_version_rowkey_cnt_ = 0;
  if (create_col_id_map_ && OB_NOT_NULL(cols_id_map_)) {
    allocator_->free((void*)cols_id_map_);
    create_col_id_map_ = false;
    cols_id_map_ = NULL;
  }
}

}  // end namespace blocksstable
}  // end namespace oceanbase
