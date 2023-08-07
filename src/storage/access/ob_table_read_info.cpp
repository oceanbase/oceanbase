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
#include "storage/blocksstable/ob_macro_block.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
/*
 * ------------------------------- ObColumnIndexArray -------------------------------
 */
int64_t ObColumnIndexArray::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(rowkey_mode), K_(for_memtable));
  if (rowkey_mode_) {
    J_COMMA();
    J_KV(K_(schema_rowkey_cnt), K_(column_cnt));
  } else {
    J_COMMA();
    J_KV(K_(array));
  }
  J_OBJ_END();
  return pos;
}

int ObColumnIndexArray::init(const int64_t count, const int64_t schema_rowkey_cnt, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (rowkey_mode_) {
    column_cnt_ = count;
    schema_rowkey_cnt_ = schema_rowkey_cnt;
  } else {
    if (OB_FAIL(array_.init(count, allocator))) {
      LOG_WARN("failed to init array", K(ret), K(count));
    } else if (OB_FAIL(array_.prepare_allocate(count))) {
      LOG_WARN("failed to pre alloc array", K(ret), K(count));
    }
  }
  return ret;
}

int32_t ObColumnIndexArray::at(int64_t idx) const
{
  int32_t ret_val = 0;
  const int32_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  if (rowkey_mode_) {
    OB_ASSERT(idx >= 0 && idx < column_cnt_);
    if (for_memtable_) {
      if (idx < schema_rowkey_cnt_) {
        ret_val = idx;
      } else if (idx < schema_rowkey_cnt_ + extra_rowkey_cnt) {
        ret_val = OB_INVALID_INDEX;
      } else {
        ret_val = idx - extra_rowkey_cnt;
      }
    } else {
      ret_val = idx;
    }
  } else {
    OB_ASSERT(idx >= 0 && idx < array_.count());
    ret_val = array_.at(idx);
  }
  return ret_val;
}

int64_t ObColumnIndexArray::get_deep_copy_size() const
{
  return rowkey_mode_ ? 0 : array_.get_deep_copy_size();
}

int ObColumnIndexArray::deep_copy(
    char *dst_buf,
    const int64_t buf_size,
    int64_t &pos,
    ObColumnIndexArray &dst_array) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur column index array is invalid", K(ret), KPC(this));
  } else {
    dst_array.version_ = version_;
    dst_array.rowkey_mode_ = rowkey_mode_;
    dst_array.for_memtable_ = for_memtable_;
    dst_array.schema_rowkey_cnt_ = schema_rowkey_cnt_;
    dst_array.column_cnt_ = column_cnt_;
    if (!rowkey_mode_ && OB_FAIL(array_.deep_copy(dst_buf, buf_size, pos, dst_array.array_))) {
      LOG_WARN("failed to deep copy", K(ret));
    }
  }
  return ret;
}

int ObColumnIndexArray::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
    version_,
    rowkey_mode_,
    for_memtable_);
  if (OB_FAIL(ret)) {
  } else if (rowkey_mode_) {
    LST_DO_CODE(OB_UNIS_ENCODE, schema_rowkey_cnt_, column_cnt_);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("for non-rowkey-mode, should not use serialize func", K(ret), K(rowkey_mode_));
  }
  return ret;
}

int ObColumnIndexArray::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  reset();
  LST_DO_CODE(OB_UNIS_DECODE,
    version_,
    rowkey_mode_,
    for_memtable_);
  if (OB_FAIL(ret)) {
  } else if (rowkey_mode_) {
    LST_DO_CODE(OB_UNIS_DECODE, schema_rowkey_cnt_, column_cnt_);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("for non-rowkey-mode, should not use deserialize func", K(ret), K(rowkey_mode_));
  }
  return ret;
}

int64_t ObColumnIndexArray::get_serialize_size() const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
    version_,
    rowkey_mode_,
    for_memtable_);
  if (rowkey_mode_) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, schema_rowkey_cnt_, column_cnt_);
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "for non-rowkey-mode, should not use serialize func", K(rowkey_mode_));
  }
  return len;
}

/*
 * ------------------------------- ObReadInfoStruct -------------------------------
 */
void ObReadInfoStruct::reset()
{
  is_inited_ = false;
  is_oracle_mode_ = false;
  allocator_ = nullptr;
  schema_column_count_ = 0;
  compat_version_ = READ_INFO_VERSION_V1;
  reserved_ = 0;
  schema_rowkey_cnt_ = 0;
  rowkey_cnt_ = 0;
  cols_desc_.reset();
  cols_index_.reset();
  memtable_cols_index_.reset();
  datum_utils_.reset();
}

void ObReadInfoStruct::init_basic_info(const int64_t schema_column_count,
                     const int64_t schema_rowkey_cnt,
                     const bool is_oracle_mode) {
  schema_column_count_ = schema_column_count;
  schema_rowkey_cnt_ = schema_rowkey_cnt;
  rowkey_cnt_ = schema_rowkey_cnt + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  is_oracle_mode_ = is_oracle_mode;
}

int64_t ObReadInfoStruct::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inited), K_(compat_version), K_(is_oracle_mode),
       K_(schema_column_count),
       K_(schema_rowkey_cnt),
       K_(rowkey_cnt),
       K_(cols_index),
       K_(cols_desc),
       K_(datum_utils));
  J_OBJ_END();

  return pos;
}

int ObReadInfoStruct::init_compat_version()
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), compat_version))) {
    LOG_WARN("fail to get data version", K(ret));
  } else if (compat_version < DATA_VERSION_4_2_0_0) {
    compat_version_ = READ_INFO_VERSION_V0;
  } else {
    compat_version_ = READ_INFO_VERSION_V1;
  }
  return ret;
}

/*
 * ------------------------------- ObTableReadInfo -------------------------------
 */
ObTableReadInfo::ObTableReadInfo()
  : ObReadInfoStruct(false/*rowkey_mode*/),
    trans_col_index_(OB_INVALID_INDEX),
    group_idx_col_index_(OB_INVALID_INDEX),
    seq_read_column_count_(0),
    max_col_index_(-1)
{
}

ObTableReadInfo::~ObTableReadInfo()
{
  reset();
}

int ObTableReadInfo::init(
    common::ObIAllocator &allocator,
    const int64_t schema_column_count,
    const int64_t schema_rowkey_cnt,
    const bool is_oracle_mode,
    const common::ObIArray<ObColDesc> &cols_desc,
    const common::ObIArray<int32_t> *storage_cols_index,
    const common::ObIArray<ObColumnParam *> *cols_param)
{
  int ret = OB_SUCCESS;
  const int64_t out_cols_cnt = cols_desc.count();

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), KPC(this));
  } else if (OB_UNLIKELY(0 > schema_rowkey_cnt ||
                         out_cols_cnt < schema_rowkey_cnt ||
                         out_cols_cnt > OB_ROW_MAX_COLUMNS_COUNT ||
                         (nullptr != storage_cols_index && storage_cols_index->count() != cols_desc.count()) ||
                         (nullptr != cols_param && cols_param->count() != cols_desc.count()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(schema_rowkey_cnt), K(cols_desc.count()), KPC(storage_cols_index), KPC(cols_param));
  } else if (OB_FAIL(init_compat_version())) { // init compat verion
    LOG_WARN("failed to init compat version", KR(ret));
  } else {
    const int64_t trans_version_col_idx = ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(
        schema_rowkey_cnt, true);
    const int64_t sql_sequence_col_idx = ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(
        schema_rowkey_cnt, true);
    init_basic_info(schema_column_count, schema_rowkey_cnt, is_oracle_mode); // init basic info
    if (OB_FAIL(ObReadInfoStruct::prepare_arrays(allocator, cols_desc, out_cols_cnt))) {
      LOG_WARN("failed to prepare arrays", K(ret), K(out_cols_cnt));
    } else if (nullptr != cols_param && OB_FAIL(cols_param_.init_and_assign(*cols_param, allocator))) {
      LOG_WARN("Fail to assign cols_param", K(ret));
    } else {
      int32_t col_index = OB_INVALID_INDEX;
      for (int64_t i = 0; i < out_cols_cnt; i++) {
        col_index = (nullptr == storage_cols_index) ? i : storage_cols_index->at(i);
        //memtable do not involve the multi version column
        memtable_cols_index_.array_[i] = col_index;
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
          col_index = col_index + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
        }
        cols_index_.array_[i] = col_index;
      }
    }
  }

  if (FAILEDx(init_datum_utils(allocator))) {
    LOG_WARN("failed to init sequence read info & datum utils", K(ret));
  }
  if (OB_FAIL(ret)) {
    reset();
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObReadInfoStruct::prepare_arrays(
  common::ObIAllocator &allocator,
  const common::ObIArray<ObColDesc> &cols_desc,
  const int64_t out_cols_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cols_desc_.init_and_assign(cols_desc, allocator))) {
    LOG_WARN("Fail to assign cols_desc", K(ret));
  } else if (OB_FAIL(cols_index_.init(out_cols_cnt, schema_rowkey_cnt_, allocator))) {
    LOG_WARN("Fail to init cols_index", K(ret), K(out_cols_cnt));
  } else if (OB_FAIL(memtable_cols_index_.init(out_cols_cnt, schema_rowkey_cnt_, allocator))) {
    LOG_WARN("Fail to init memtable_cols_index", K(ret), K(out_cols_cnt));
  }
  return ret;
}

int ObTableReadInfo::init_datum_utils(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
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
  if (OB_FAIL(datum_utils_.init(cols_desc_, schema_rowkey_cnt_, is_oracle_mode_, allocator))) {
    STORAGE_LOG(WARN, "Failed to init datum utils", K(ret), K_(schema_rowkey_cnt), K_(is_oracle_mode));
  }
  return ret;
}

void ObTableReadInfo::reset()
{
  ObReadInfoStruct::reset();
  trans_col_index_ = OB_INVALID_INDEX;
  group_idx_col_index_ = OB_INVALID_INDEX;
  seq_read_column_count_ = 0;
  max_col_index_ = -1;
  cols_param_.reset();
  memtable_cols_index_.reset();
}

/*
  be careful to deal with cols_index_/memtable_cols_index_ when (de)serialize
  for compat, only serialize arrays
*/
int ObTableReadInfo::serialize(
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_ENCODE,
              info_,
              schema_rowkey_cnt_,
              rowkey_cnt_,
              trans_col_index_,
              group_idx_col_index_,
              seq_read_column_count_,
              is_oracle_mode_,
              cols_desc_,
              cols_index_.array_,
              memtable_cols_index_.array_);
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
  LST_DO_CODE(OB_UNIS_DECODE,
              info_,
              schema_rowkey_cnt_,
              rowkey_cnt_,
              trans_col_index_,
              group_idx_col_index_,
              seq_read_column_count_,
              is_oracle_mode_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(cols_desc_.deserialize(buf, data_len, pos, allocator))) {
    LOG_WARN("Fail to deserialize cols_desc", K(ret), K(data_len), K(pos));
  } else if (FALSE_IT(cols_index_.rowkey_mode_ = false)) {
  } else if (OB_FAIL(cols_index_.array_.deserialize(buf, data_len, pos, allocator))) {
    LOG_WARN("Fail to deserialize cols_index", K(ret), K(data_len), K(pos));
  } else if (FALSE_IT(memtable_cols_index_.rowkey_mode_ = false)) {
  } else if (OB_FAIL(memtable_cols_index_.array_.deserialize(buf, data_len, pos, allocator))) {
    LOG_WARN("Fail to deserialize memtable_cols_index_", K(ret), K(data_len), K(pos));
  }
  if (OB_SUCC(ret)) {
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
        if (OB_SUCC(ret) && OB_FAIL(cols_param_.init_and_assign(tmp_columns, allocator))) {
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
    } else {
      is_inited_ = true;
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  }

  return ret;
}


int64_t ObTableReadInfo::get_serialize_size() const
{
  int ret = OB_SUCCESS;
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              info_,
              schema_rowkey_cnt_,
              rowkey_cnt_,
              trans_col_index_,
              group_idx_col_index_,
              seq_read_column_count_,
              is_oracle_mode_,
              cols_desc_,
              cols_index_.array_,
              memtable_cols_index_.array_);

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
                                    cols_param_.count()));
  J_OBJ_END();

  return pos;
}

/*
 * ------------------------------- ObRowkeyReadInfo -------------------------------
 */
ObRowkeyReadInfo::ObRowkeyReadInfo()
  : ObReadInfoStruct(true/*rowkey_mode*/)
{
#if defined(__x86_64__)
  static_assert(sizeof(ObRowkeyReadInfo) == 368, "The size of ObRowkeyReadInfo will affect the meta memory manager, and the necessity of adding new fields needs to be considered.");
#endif
}

int ObRowkeyReadInfo::init(
    common::ObIAllocator &allocator,
    const int64_t schema_column_count,
    const int64_t schema_rowkey_cnt,
    const bool is_oracle_mode,
    const common::ObIArray<ObColDesc> &rowkey_col_descs)
{
  int ret = OB_SUCCESS;
  const int64_t extra_rowkey_col_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int64_t out_cols_cnt = schema_column_count + extra_rowkey_col_cnt;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), KPC(this));
  } else if (OB_UNLIKELY(0 > schema_rowkey_cnt
    || out_cols_cnt < schema_rowkey_cnt
    || schema_column_count > OB_ROW_MAX_COLUMNS_COUNT
    || schema_rowkey_cnt > schema_column_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(schema_rowkey_cnt), K(rowkey_col_descs.count()), K(out_cols_cnt), K(schema_column_count));
  } else if (OB_FAIL(init_compat_version())) { // init compat verion
    LOG_WARN("failed to init compat version", KR(ret));
  } else {
    init_basic_info(schema_column_count, schema_rowkey_cnt, is_oracle_mode); // init basic info
    if (OB_FAIL(prepare_arrays(allocator, rowkey_col_descs, out_cols_cnt))) {
      LOG_WARN("failed to prepare arrays", K(ret), K(out_cols_cnt));
    } else if (OB_FAIL(datum_utils_.init(cols_desc_, schema_rowkey_cnt_, is_oracle_mode_, allocator))) {
      STORAGE_LOG(WARN, "Failed to init datum utils", K(ret), K_(schema_rowkey_cnt), K_(is_oracle_mode));
    } else {
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObRowkeyReadInfo::init(
  common::ObIAllocator &allocator,
  const blocksstable::ObDataStoreDesc &data_store_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!data_store_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data store desc is invalid", K(ret), K(data_store_desc));
  } else if (OB_FAIL(ObRowkeyReadInfo::init(
                 allocator,
                 data_store_desc.row_column_count_ - ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt(),
                 data_store_desc.schema_rowkey_col_cnt_,
                 lib::is_oracle_mode(),
                 data_store_desc.is_major_merge() ? data_store_desc.get_full_stored_col_descs() : data_store_desc.get_rowkey_col_descs()))) {
    LOG_WARN("failed to init full read info", K(ret), K(data_store_desc));
  } else {
    LOG_TRACE("success to init full read info", K(ret), K(data_store_desc));
  }
  return ret;
}

int64_t ObRowkeyReadInfo::get_request_count() const
{
  return schema_column_count_ + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
}

int64_t ObRowkeyReadInfo::get_deep_copy_size() const
{
  return sizeof(ObRowkeyReadInfo)
      + cols_desc_.get_deep_copy_size()
      + cols_index_.get_deep_copy_size()
      + memtable_cols_index_.get_deep_copy_size()
      + datum_utils_.get_deep_copy_size();
}

int ObRowkeyReadInfo::deep_copy(char *buf, const int64_t buf_len, ObRowkeyReadInfo *&value) const
{
  int ret = OB_SUCCESS;
  const int64_t memory_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < memory_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalue argument", K(ret), KP(buf), K(buf_len), K(memory_size));
  } else {
    ObRowkeyReadInfo *dst_value = new (buf) ObRowkeyReadInfo();
    int64_t pos = sizeof(ObRowkeyReadInfo);
    dst_value->info_ = info_;
    dst_value->schema_rowkey_cnt_ = schema_rowkey_cnt_;
    dst_value->rowkey_cnt_ = rowkey_cnt_;
    dst_value->is_oracle_mode_ = is_oracle_mode_;
    // can not deep copy cols param cuz ObColumnParam need an allocator on constructor for default value
    if (OB_FAIL(cols_desc_.deep_copy(buf, buf_len, pos, dst_value->cols_desc_))) {
      LOG_WARN("fail to deep copy cols_desc array", K(ret));
    } else if (OB_FAIL(cols_index_.deep_copy(buf, buf_len, pos, dst_value->cols_index_))) {
      LOG_WARN("fail to deep copy cols_index array", K(ret));
    } else if (OB_FAIL(memtable_cols_index_.deep_copy(buf, buf_len, pos, dst_value->memtable_cols_index_))) {
      LOG_WARN("fail to deep copy memtable_cols_index array", K(ret));
    } else if (OB_FAIL(dst_value->datum_utils_.init(
        cols_desc_, schema_rowkey_cnt_, is_oracle_mode_, buf_len - pos, buf + pos))) {
      LOG_WARN("fail to init datum utilities on fixed memory buf", K(ret));
    } else {
      pos += datum_utils_.get_deep_copy_size();
      dst_value->is_inited_ = is_inited_;
      value = dst_value;
    }
  }
  return ret;
}


int ObRowkeyReadInfo::serialize(
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_ENCODE,
              info_,
              schema_rowkey_cnt_,
              rowkey_cnt_,
              is_oracle_mode_,
              cols_desc_,
              cols_index_,
              memtable_cols_index_);
  return ret;
}

int ObRowkeyReadInfo::deserialize(
    common::ObIAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  reset();
  LST_DO_CODE(OB_UNIS_DECODE,
              info_,
              schema_rowkey_cnt_,
              rowkey_cnt_,
              is_oracle_mode_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(cols_desc_.deserialize(buf, data_len, pos, allocator))) {
    LOG_WARN("Fail to deserialize cols_desc", K(ret), K(data_len), K(pos));
  } else if (OB_FAIL(cols_index_.deserialize(buf, data_len, pos, allocator))) {
    LOG_WARN("Fail to deserialize cols_index", K(ret), K(data_len), K(pos));
  } else if (OB_FAIL(memtable_cols_index_.deserialize(buf, data_len, pos, allocator))) {
    LOG_WARN("Fail to deserialize memtable_cols_index", K(ret), K(data_len), K(pos));
  }

  if (OB_SUCC(ret) && cols_desc_.count() > 0) {
    if (OB_FAIL(datum_utils_.init(cols_desc_, schema_rowkey_cnt_, is_oracle_mode_, allocator))) {
      STORAGE_LOG(WARN, "Failed to init datum utils", K(ret), K_(schema_rowkey_cnt));
    } else {
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int64_t ObRowkeyReadInfo::get_serialize_size() const
{
  int ret = OB_SUCCESS;
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              info_,
              schema_rowkey_cnt_,
              rowkey_cnt_,
              is_oracle_mode_,
              cols_desc_,
              cols_index_,
              memtable_cols_index_);
  return len;
}

}
}
