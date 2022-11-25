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
#include "ob_table_read_info.h"
#include "storage/ob_i_store.h"
#include "storage/ob_storage_schema.h"
#include "share/schema/ob_table_param.h"

namespace oceanbase
{
using namespace common;
namespace storage
{

ObTableReadInfo::~ObTableReadInfo()
{
  reset();
}

void ObTableReadInfo::reset()
{
  if (nullptr != allocator_ && nullptr != index_read_info_) {
    index_read_info_->~ObTableReadInfo();
    allocator_->free(index_read_info_);
  }
  allocator_ = nullptr;
  schema_column_count_ = 0;
  trans_col_index_ = OB_INVALID_INDEX;
  group_idx_col_index_ = OB_INVALID_INDEX;
  schema_rowkey_cnt_ = 0;
  seq_read_column_count_ = 0;
  max_col_index_ = -1;
  is_oracle_mode_ = false;
  cols_param_.reset();
  cols_desc_.reset();
  cols_index_.reset();
  memtable_cols_index_.reset();
  datum_utils_.reset();
  index_read_info_ = nullptr;
}

int ObTableReadInfo::init(
    common::ObIAllocator &allocator,
    const int64_t schema_column_count,
    const int64_t schema_rowkey_cnt,
    const bool is_oracle_mode,
    const common::ObIArray<ObColDesc> &cols_desc,
    const bool is_multi_version_full,
    const common::ObIArray<int32_t> *storage_cols_index,
    const common::ObIArray<ObColumnParam *> *cols_param,
    const bool is_index_read_info)
{
  int ret = OB_SUCCESS;
  const int64_t out_cols_cnt = cols_desc.count();

  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), KPC(this));
  } else if (OB_UNLIKELY(0 > schema_rowkey_cnt ||
                         out_cols_cnt < schema_rowkey_cnt ||
                         out_cols_cnt > OB_ROW_MAX_COLUMNS_COUNT ||
                         (nullptr != storage_cols_index && storage_cols_index->count() != cols_desc.count()) ||
                         (nullptr != cols_param && cols_param->count() != cols_desc.count()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(schema_rowkey_cnt), K(cols_desc.count()), KPC(storage_cols_index), KPC(cols_param));
  } else {
    int16_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    cols_param_.set_allocator(&allocator);
    cols_desc_.set_allocator(&allocator);
    cols_index_.set_allocator(&allocator);
    allocator_ = &allocator;
    memtable_cols_index_.set_allocator(&allocator);
    schema_column_count_ = schema_column_count;
    schema_rowkey_cnt_ = schema_rowkey_cnt;
    rowkey_cnt_ = schema_rowkey_cnt + extra_rowkey_cnt;
    is_oracle_mode_ = is_oracle_mode;
    int64_t trans_version_col_idx = ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(
        schema_rowkey_cnt, true);
    int64_t sql_sequence_col_idx = ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(
        schema_rowkey_cnt, true);
    if (OB_FAIL(cols_desc_.assign(cols_desc))) {
      LOG_WARN("Fail to assign cols_desc", K(ret));
    } else if (nullptr != cols_param &&
               OB_FAIL(cols_param_.assign(*cols_param))) {
      LOG_WARN("Fail to assign cols_param", K(ret));
    } else if (OB_FAIL(cols_index_.prepare_allocate(out_cols_cnt))) {
      LOG_WARN("Fail to init cols_index", K(ret), K(out_cols_cnt));
    } else if (OB_FAIL(memtable_cols_index_.prepare_allocate(out_cols_cnt))) {
      LOG_WARN("Fail to init memtable_cols_index", K(ret), K(out_cols_cnt));
    } else if (OB_LIKELY(!is_multi_version_full)) {
      int32_t col_index = OB_INVALID_INDEX;
      for (int64_t i = 0; i < out_cols_cnt; i++) {
        col_index = (nullptr == storage_cols_index) ? i : storage_cols_index->at(i);
        //memtable do not involve the multi version column
        memtable_cols_index_[i] = col_index;
        if (i < schema_rowkey_cnt) {
          // continue
        } else if (OB_INVALID_INDEX == col_index) {
          if (common::OB_HIDDEN_TRANS_VERSION_COLUMN_ID == cols_desc.at(i).col_id_) {
            trans_col_index_ = i;
            col_index = trans_version_col_idx;
          } else if (common::OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID == cols_desc.at(i).col_id_) {
            col_index = sql_sequence_col_idx;
          } else if (common::OB_HIDDEN_GROUP_IDX_COLUMN_ID == cols_desc.at(i).col_id_) {
            group_idx_col_index_ = i;
            col_index = -1;
          } else {
            col_index = -1;
          }
        } else {
          col_index = col_index + extra_rowkey_cnt;
        }
        cols_index_[i] = col_index;
      }
    } else {
      trans_col_index_ = trans_version_col_idx;
      for (int64_t i = 0; i < out_cols_cnt; i++) {
        if (common::OB_HIDDEN_GROUP_IDX_COLUMN_ID == cols_desc.at(i).col_id_) {
          group_idx_col_index_ = i;
          cols_index_[i] = -1;
          memtable_cols_index_[i] = -1;
        } else {
          cols_index_[i] = i;
          if (i < schema_rowkey_cnt) {
            memtable_cols_index_[i] = i;
          } else if (i < rowkey_cnt_) {
            memtable_cols_index_[i] = OB_INVALID_INDEX;
          } else {
            memtable_cols_index_[i] = i - extra_rowkey_cnt;
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    seq_read_column_count_ = 0;
    while (seq_read_column_count_ < cols_index_.count() &&
           cols_index_.at(seq_read_column_count_) == seq_read_column_count_) {
      seq_read_column_count_++;
    }
    max_col_index_ = -1;
    for (int64_t i = 0; i < cols_index_.count(); i++) {
      if (cols_index_.at(i) > max_col_index_) {
        max_col_index_ = cols_index_.at(i);
      } else if (-1 == cols_index_.at(i)) {
        max_col_index_ = INT64_MAX;
      }
    }
    if (OB_FAIL(datum_utils_.init(cols_desc_, schema_rowkey_cnt, is_oracle_mode, allocator))) {
      STORAGE_LOG(WARN, "Failed to init datum utils", K(ret), K(schema_rowkey_cnt), K(is_oracle_mode));
    } else if (is_index_read_info) {
      // no need to build index_read_info_
      index_read_info_ = nullptr;
    } else if (OB_FAIL(build_index_read_info(allocator, schema_rowkey_cnt, is_oracle_mode, cols_desc))) {
      STORAGE_LOG(WARN, "Failed to build index read info", K(ret));
    }
  }
  return ret;
}

int ObTableReadInfo::build_index_read_info(
    common::ObIAllocator &allocator,
    const int64_t schema_rowkey_cnt,
    const bool is_oracle_mode,
    const common::ObIArray<ObColDesc> &cols_desc)
{
  int ret = OB_SUCCESS;
  ObSEArray<share::schema::ObColDesc, 16> idx_col_descs;
  ObColDesc idx_col_desc;
  idx_col_desc.col_type_.set_varbinary();
  idx_col_desc.col_id_ = OB_APP_MIN_COLUMN_ID + rowkey_cnt_; // todo: remove col id in col desc
  idx_col_desc.col_order_ = common::ObOrderType::ASC;
  if (OB_UNLIKELY(schema_rowkey_cnt > cols_desc.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid cols desc", K(ret), K(schema_rowkey_cnt), K(cols_desc));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < schema_rowkey_cnt; ++i) {
    if (OB_FAIL(idx_col_descs.push_back(cols_desc.at(i)))) {
      STORAGE_LOG(WARN, "Failed to append rowkey col descs", K(ret), K(i), K(cols_desc));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(storage::ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(idx_col_descs))) {
    STORAGE_LOG(WARN, "Failed to add extra rowkey cols", K(ret));
  } else if (OB_FAIL(idx_col_descs.push_back(idx_col_desc))) {
    STORAGE_LOG(WARN, "Failed to append index col desc", K(ret), K(idx_col_desc));
  } else if (OB_ISNULL(index_read_info_
      = static_cast<ObTableReadInfo *>(allocator.alloc(sizeof(ObTableReadInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc memory for index read info", K(ret), K(sizeof(ObTableReadInfo)));
  } else if (OB_ISNULL(index_read_info_ = new (index_read_info_) ObTableReadInfo())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null index read info", K(ret));
  } else if (OB_FAIL(index_read_info_->init(
      allocator,
      schema_rowkey_cnt + 1,
      schema_rowkey_cnt,
      is_oracle_mode,
      idx_col_descs,
      true, /* is_multi_version_full */
      nullptr, /* column index */
      nullptr, /* column param */
      true /* is_index_read_info */))) {
    STORAGE_LOG(WARN, "Failed to build index read info in table read info", K(ret));
  }
  return ret;
}

int ObTableReadInfo::serialize(
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_ENCODE,
              schema_column_count_,
              schema_rowkey_cnt_,
              rowkey_cnt_,
              trans_col_index_,
              group_idx_col_index_,
              seq_read_column_count_,
              is_oracle_mode_,
              cols_desc_,
              cols_index_,
              memtable_cols_index_);
  if (OB_SUCC(ret)) {

    if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, cols_param_.count()))) {
      LOG_WARN("Fail to encode column count", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < cols_param_.count(); ++i) {
      if (OB_ISNULL(cols_param_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(i));
      } else if (OB_FAIL(cols_param_.at(i)->serialize(buf, buf_len, pos))) {
        LOG_WARN("Fail to serialize column", K(ret));
      }
    }
  }

  return ret;
}

int ObTableReadInfo::deserialize(
    common::ObIAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;

  reset();
  cols_desc_.set_allocator(&allocator);
  cols_index_.set_allocator(&allocator);
  memtable_cols_index_.set_allocator(&allocator);
  cols_param_.set_allocator(&allocator);
  LST_DO_CODE(OB_UNIS_DECODE,
              schema_column_count_,
              schema_rowkey_cnt_,
              rowkey_cnt_,
              trans_col_index_,
              group_idx_col_index_,
              seq_read_column_count_,
              is_oracle_mode_,
              cols_desc_,
              cols_index_,
              memtable_cols_index_);
  if (OB_SUCC(ret)) {
    allocator_ = &allocator;
    int64_t column_param_cnt = 0;
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &column_param_cnt))) {
      LOG_WARN("Fail to decode column count", K(ret));
    } else if (column_param_cnt > 0) {
      ObColumnParam **column = NULL;
      void *tmp_ptr  = NULL;
      if (OB_ISNULL(tmp_ptr = allocator.alloc(column_param_cnt * sizeof(ObColumnParam *)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to alloc", K(ret), K(column_param_cnt));
      } else if (FALSE_IT(column = static_cast<ObColumnParam **>(tmp_ptr))) {
        // not reach
      } else {
        ObArray<ObColumnParam *> tmp_columns;
        for (int64_t i = 0; OB_SUCC(ret) && i < column_param_cnt; ++i) {
          ObColumnParam *&cur_column = column[i];
          cur_column = nullptr;
          if (OB_ISNULL(tmp_ptr = allocator.alloc(sizeof(ObColumnParam)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc failed", K(ret));
          } else if (FALSE_IT(cur_column = new (tmp_ptr) ObColumnParam(allocator))) {
          } else if (OB_FAIL(cur_column->deserialize(buf, data_len, pos))) {
            LOG_WARN("Fail to deserialize column", K(ret));
          } else if (OB_FAIL(tmp_columns.push_back(cur_column))) {
            LOG_WARN("Fail to add column", K(ret));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(cols_param_.assign(tmp_columns))) {
          LOG_WARN("Fail to add columns", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && cols_desc_.count() > 0) {
    max_col_index_ = -1;
    for (int64_t i = 0; i < cols_index_.count(); i++) {
      if (cols_index_.at(i) > max_col_index_) {
        max_col_index_ = cols_index_.at(i);
      } else if (-1 == cols_index_.at(i)) {
        max_col_index_ = INT64_MAX;
      }
    }
    if (OB_FAIL(datum_utils_.init(cols_desc_, schema_rowkey_cnt_, is_oracle_mode_, allocator))) {
      STORAGE_LOG(WARN, "Failed to init datum utils", K(ret), K_(schema_rowkey_cnt));
    } else if (OB_FAIL(build_index_read_info(
        allocator, schema_rowkey_cnt_, is_oracle_mode_, cols_desc_))) {
      STORAGE_LOG(WARN, "Failed to build index read info", K(ret));
    }
  }

  return ret;
}


int64_t ObTableReadInfo::get_serialize_size() const
{
  int ret = OB_SUCCESS;
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              schema_column_count_,
              schema_rowkey_cnt_,
              rowkey_cnt_,
              trans_col_index_,
              group_idx_col_index_,
              seq_read_column_count_,
              is_oracle_mode_,
              cols_desc_,
              cols_index_,
              memtable_cols_index_);

  if (OB_SUCC(ret)) {
    len += serialization::encoded_length_vi64(cols_param_.count());
    for (int64_t i = 0; OB_SUCC(ret) && i < cols_param_.count(); ++i) {
      if (OB_ISNULL(cols_param_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(i));
      } else {
        len += cols_param_.at(i)->get_serialize_size();
      }
    }
  }

  return len;
}

int ObTableReadInfo::assign(
    common::ObIAllocator &allocator,
    const ObTableReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(read_info));
  } else {
    reset();
    cols_desc_.set_allocator(&allocator);
    cols_index_.set_allocator(&allocator);
    memtable_cols_index_.set_allocator(&allocator);
    cols_param_.set_allocator(&allocator);
    allocator_ = &allocator;
    schema_column_count_ = read_info.schema_column_count_;
    schema_rowkey_cnt_ = read_info.schema_rowkey_cnt_;
    rowkey_cnt_ = read_info.rowkey_cnt_;
    trans_col_index_ = read_info.trans_col_index_;
    group_idx_col_index_ = read_info.group_idx_col_index_;
    seq_read_column_count_ = read_info.seq_read_column_count_;
    max_col_index_ = read_info.max_col_index_;
    is_oracle_mode_ = read_info.is_oracle_mode_;
    if (OB_FAIL(cols_desc_.assign(read_info.cols_desc_))) {
      LOG_WARN("Fail to assign cols_desc", K(ret), K(read_info));
    } else if (OB_FAIL(cols_index_.assign(read_info.cols_index_))) {
      LOG_WARN("Fail to assign cols_index", K(ret), K(read_info));
    } else if (OB_FAIL(memtable_cols_index_.assign(read_info.memtable_cols_index_))) {
      LOG_WARN("Fail to assign memtable_cols_index", K(ret), K(read_info));
    } else if (!read_info.cols_param_.empty() &&
               OB_FAIL(cols_param_.assign(read_info.cols_param_))) {
      LOG_WARN("Fail to assign cols_param", K(ret), K(read_info));
    } else if (OB_FAIL(datum_utils_.init(cols_desc_, schema_rowkey_cnt_, is_oracle_mode_, allocator))) {
      STORAGE_LOG(WARN, "Failed to init datum utils", K(ret), K_(schema_rowkey_cnt));
    } else if (OB_FAIL(build_index_read_info(
        allocator, schema_rowkey_cnt_, is_oracle_mode_, cols_desc_))) {
      STORAGE_LOG(WARN, "Failed to build index read info", K(ret));
    }
  }
  return ret;
}

int64_t ObTableReadInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(schema_column_count),
       K_(schema_rowkey_cnt),
       K_(rowkey_cnt),
       K_(trans_col_index),
       K_(seq_read_column_count),
       K_(max_col_index),
       K_(is_oracle_mode),
       K_(cols_index),
       K_(memtable_cols_index),
       K_(cols_desc),
       K_(datum_utils),
       "cols_param",
       ObArrayWrap<ObColumnParam *>(0 == cols_param_.count() ? NULL : &cols_param_.at(0),
                                    cols_param_.count()),
       KPC_(index_read_info));
  J_OBJ_END();

  return pos;
}

}
}
