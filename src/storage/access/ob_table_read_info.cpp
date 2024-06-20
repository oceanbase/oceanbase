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
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
/*
 * ------------------------------- ObColumnIndexArray -------------------------------
 */
int64_t return_array_cnt(uint32_t schema_rowkey_cnt, const ObFixedMetaObjArray<int32_t> & array)
{
  return array.count();
}
int64_t return_schema_rowkey_cnt(uint32_t schema_rowkey_cnt, const ObFixedMetaObjArray<int32_t> & array)
{
  return schema_rowkey_cnt;
}
int32_t return_array_idx_for_memtable(uint32_t schema_rowkey_cnt, uint32_t column_cnt,
                         int64_t idx,
                         const ObFixedMetaObjArray<int32_t> &array)
{
  int32_t ret_val = 0;
  const int32_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  OB_ASSERT(idx >= 0 && idx < column_cnt);
  if (idx < schema_rowkey_cnt) {
    ret_val = idx;
  } else if (idx < schema_rowkey_cnt + extra_rowkey_cnt) {
    ret_val = OB_INVALID_INDEX;
  } else {
    ret_val = idx - extra_rowkey_cnt;
  }
  return ret_val;
}
int32_t return_idx(uint32_t schema_rowkey_cnt, uint32_t column_cnt,
                         int64_t idx,
                         const ObFixedMetaObjArray<int32_t> &array)
{
  OB_ASSERT(idx >= 0 && idx < column_cnt);
  return (int32_t)idx;
}
int32_t return_array_idx(uint32_t schema_rowkey_cnt, uint32_t column_cnt,
                         int64_t idx,
                         const ObFixedMetaObjArray<int32_t> &array)
{
  OB_ASSERT(idx >= 0 && idx < array.count());
  return array[idx];
}
ObColumnIndexArray::ObColumnIndexArray(const bool rowkey_mode /* = false*/,
                                       const bool for_memtable /* = false*/)
    : version_(COLUMN_INDEX_ARRAY_VERSION),
      rowkey_mode_(rowkey_mode),
      for_memtable_(for_memtable),
      reserved_(0),
      schema_rowkey_cnt_(0),
      column_cnt_(0),
      array_()
{
  if (rowkey_mode) {
    if (for_memtable) { // no multi_version rowkey in memtable
      at_func_ = return_array_idx_for_memtable;
    } else {
      at_func_ = return_idx;
    }
    count_func_ = return_schema_rowkey_cnt;
  } else {
    at_func_ = return_array_idx;
    count_func_ = return_array_cnt;
  }
}

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
  bool tmp_rowkey_mode = false;
  bool tmp_for_memtable = false;
  LST_DO_CODE(OB_UNIS_DECODE,
    version_,
    tmp_rowkey_mode,
    tmp_for_memtable);

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(rowkey_mode_ != tmp_rowkey_mode || for_memtable_ != tmp_for_memtable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialize info is different from cur array", KR(ret), K(rowkey_mode_), K(tmp_rowkey_mode),
      K(for_memtable_), K(tmp_for_memtable));
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
  compat_version_ = READ_INFO_VERSION_V2;
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
                     const bool is_oracle_mode,
                     const bool is_cg_sstable) {
  const int64_t extra_rowkey_cnt = is_cg_sstable ? 0 : storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  schema_column_count_ = schema_column_count;
  schema_rowkey_cnt_ = schema_rowkey_cnt;
  rowkey_cnt_ = schema_rowkey_cnt + extra_rowkey_cnt;
  is_oracle_mode_ = is_oracle_mode;
}

int ObReadInfoStruct::generate_for_column_store(ObIAllocator &allocator,
                                               const ObColDesc &desc,
                                               const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObColDesc, 1> tmp_cols_desc;
  if (OB_FAIL(tmp_cols_desc.push_back(desc))) {
    STORAGE_LOG(WARN, "Failed to push back col desc", K(ret), K(desc));
  } else if (OB_FAIL(prepare_arrays(allocator, tmp_cols_desc, 1/*col_cnt*/))) {
    STORAGE_LOG(WARN, "failed to prepare arrays", K(ret));
  } else if (OB_UNLIKELY(cols_index_.rowkey_mode_ || memtable_cols_index_.rowkey_mode_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cols index is unexpected rowkey_mode", K(ret), K(cols_index_), K(memtable_cols_index_));
  } else if (FALSE_IT(cols_index_.array_.at(0) = 0)) {
  } else if (FALSE_IT(memtable_cols_index_.array_.at(0) = 0)) {
  } else if (OB_FAIL(datum_utils_.init(cols_desc_, ObCGReadInfo::CG_ROWKEY_COL_CNT, is_oracle_mode, allocator, true/*is_column_store*/))) {
    STORAGE_LOG(WARN, "Fail to init datum utils", K(ret));
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
    schema_column_count_ = ObCGReadInfo::CG_COL_CNT;
    schema_rowkey_cnt_ = ObCGReadInfo::CG_ROWKEY_COL_CNT;
    rowkey_cnt_ = 0;
    is_oracle_mode_ = is_oracle_mode;
  }
  return ret;
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
  } else if (compat_version < DATA_VERSION_4_3_0_0) {
    compat_version_ = READ_INFO_VERSION_V1;
  } else {
    compat_version_ = READ_INFO_VERSION_V2;
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
    max_col_index_(-1),
    cols_param_(),
    cols_extend_(),
    has_all_column_group_(true)
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
    const common::ObIArray<ObColumnParam *> *cols_param,
    const common::ObIArray<int32_t> *cg_idxs,
    const common::ObIArray<ObColExtend> *cols_extend,
    const bool has_all_column_group,
    const bool is_cg_sstable)
{
  int ret = OB_SUCCESS;
  const int64_t out_cols_cnt = cols_desc.count();

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), KPC(this));
  } else if (OB_UNLIKELY(schema_rowkey_cnt < 0
      || schema_column_count < 0
      || out_cols_cnt < schema_rowkey_cnt
      || out_cols_cnt > OB_ROW_MAX_COLUMNS_COUNT
      || (nullptr != storage_cols_index && storage_cols_index->count() != cols_desc.count())
      || (nullptr != cols_param && cols_param->count() != cols_desc.count())
      || (nullptr != cg_idxs && cg_idxs->count() != cols_desc.count())
      || (nullptr != cols_extend && cols_extend->count() != cols_desc.count()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(schema_rowkey_cnt), K(schema_column_count),
             K(cols_desc.count()), KPC(storage_cols_index), KPC(cols_param),
             KPC(cg_idxs), KPC(cols_extend));
  } else if (OB_FAIL(init_compat_version())) { // init compat verion
    LOG_WARN("failed to init compat version", KR(ret));
  } else {
    has_all_column_group_ = has_all_column_group;
    const int64_t trans_version_col_idx = ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(
        schema_rowkey_cnt, true);
    const int64_t sql_sequence_col_idx = ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(
        schema_rowkey_cnt, true);
    const int64_t extra_rowkey_cnt = is_cg_sstable ? 0: storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    init_basic_info(schema_column_count, schema_rowkey_cnt, is_oracle_mode, is_cg_sstable); // init basic info
    if (OB_FAIL(ObReadInfoStruct::prepare_arrays(allocator, cols_desc, out_cols_cnt))) {
      LOG_WARN("failed to prepare arrays", K(ret), K(out_cols_cnt));
    } else if (nullptr != cols_param && OB_FAIL(cols_param_.init_and_assign(*cols_param, allocator))) {
      LOG_WARN("Fail to assign cols_param", K(ret));
    } else if (nullptr != cg_idxs && OB_FAIL(cg_idxs_.init_and_assign(*cg_idxs, allocator))) {
      LOG_WARN("Fail to init cg idxs", K(ret));
    } else if (nullptr != cols_extend && OB_FAIL(cols_extend_.init_and_assign(*cols_extend, allocator))) {
      LOG_WARN("Fail to assign cols_extend", K(ret));
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
          col_index = col_index + extra_rowkey_cnt;
        }
        cols_index_.array_[i] = col_index;
      }
    }
  }

  if (FAILEDx(init_datum_utils(allocator, is_cg_sstable))) {
    LOG_WARN("failed to init sequence read info & datum utils", K(ret));
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else if (OB_INIT_TWICE != ret) {
    reset();
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

int ObTableReadInfo::init_datum_utils(common::ObIAllocator &allocator, const bool is_cg_sstable)
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
  for (int64_t i = 0; i < cols_param_.count(); i++) {
    if (cols_param_.at(i)->get_meta_type().is_decimal_int()) {
      cols_desc_.at(i).col_type_.set_stored_precision(cols_param_.at(i)->get_accuracy().get_precision());
      cols_desc_.at(i).col_type_.set_scale(cols_param_.at(i)->get_accuracy().get_scale());
    }
  }
  if (OB_FAIL(datum_utils_.init(cols_desc_, schema_rowkey_cnt_, is_oracle_mode_, allocator, is_cg_sstable))) {
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
  cg_idxs_.reset();
  cols_extend_.reset();
  memtable_cols_index_.reset();
  has_all_column_group_ = true;
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

  if (OB_SUCC(ret) && compat_version_ >= READ_INFO_VERSION_V2) {
    LST_DO_CODE(OB_UNIS_ENCODE, cg_idxs_, cols_extend_, has_all_column_group_);
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
  if (OB_SUCC(ret)) {
    if (compat_version_ >= READ_INFO_VERSION_V2) {
      if (OB_FAIL(cg_idxs_.deserialize(buf, data_len, pos, allocator))) {
        LOG_WARN("Fail to deserialize cg_idxs", K(ret), K(data_len), K(pos));
      } else if (OB_FAIL(cols_extend_.deserialize(buf, data_len, pos, allocator))) {
        LOG_WARN("Fail to deserialize columns extend", K(ret), K(data_len), K(pos));
      } else {
        LST_DO_CODE(OB_UNIS_DECODE, has_all_column_group_);
      }
    } else {
      cg_idxs_.reset();
      cols_extend_.reset();
      has_all_column_group_ = true;
    }
  }

  if (OB_SUCC(ret) && cols_desc_.count() > 0) {
    const bool is_cg_sstable = ObCGReadInfo::is_cg_sstable(schema_rowkey_cnt_, schema_column_count_);
    max_col_index_ = -1;
    for (int64_t i = 0; i < cols_index_.count(); i++) {
      if (cols_index_.at(i) > max_col_index_) {
        max_col_index_ = cols_index_.at(i);
      } else if (-1 == cols_index_.at(i)) {
        max_col_index_ = INT64_MAX;
      }
    }
    if (OB_FAIL(datum_utils_.init(cols_desc_, schema_rowkey_cnt_, is_oracle_mode_, allocator, is_cg_sstable))) {
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
  if (OB_SUCC(ret) && compat_version_ >= READ_INFO_VERSION_V2) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, cg_idxs_, cols_extend_, has_all_column_group_);
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
       K_(cg_idxs),
       K_(cols_extend),
       K_(has_all_column_group),
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
  static_assert(sizeof(ObRowkeyReadInfo) <= 480, "The size of ObRowkeyReadInfo will affect the meta memory manager, and the necessity of adding new fields needs to be considered.");
#endif
}

int ObRowkeyReadInfo::init(
    common::ObIAllocator &allocator,
    const int64_t schema_column_count,
    const int64_t schema_rowkey_cnt,
    const bool is_oracle_mode,
    const common::ObIArray<ObColDesc> &rowkey_col_descs,
    const bool is_cg_sstable,
    const bool use_default_compat_version)
{
  int ret = OB_SUCCESS;
  const int64_t extra_rowkey_cnt = is_cg_sstable ? 0: storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int64_t out_cols_cnt = schema_column_count + extra_rowkey_cnt;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), KPC(this));
  } else if (OB_UNLIKELY(0 > schema_rowkey_cnt
    || schema_column_count > OB_ROW_MAX_COLUMNS_COUNT
    || schema_rowkey_cnt > schema_column_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(schema_rowkey_cnt), K(rowkey_col_descs.count()), K(out_cols_cnt), K(schema_column_count));
  } else if (use_default_compat_version) {
    compat_version_ = READ_INFO_VERSION_V2;
  } else if (OB_FAIL(init_compat_version())) { // init compat verion
    LOG_WARN("failed to init compat version", KR(ret));
  }
  if (OB_SUCC(ret)) {
    init_basic_info(schema_column_count, schema_rowkey_cnt, is_oracle_mode, is_cg_sstable); // init basic info
    if (OB_FAIL(prepare_arrays(allocator, rowkey_col_descs, out_cols_cnt))) {
      LOG_WARN("failed to prepare arrays", K(ret), K(out_cols_cnt));
    } else if (OB_FAIL(datum_utils_.init(cols_desc_, schema_rowkey_cnt_, is_oracle_mode_, allocator, is_cg_sstable))) {
      STORAGE_LOG(WARN, "Failed to init datum utils", K(ret), K_(schema_rowkey_cnt), K_(is_oracle_mode));
    } else {
      is_inited_ = true;
    }
    if (OB_FAIL(ret)) {
      reset();
    }
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
    const bool is_cg_sstable = ObCGReadInfo::is_cg_sstable(schema_rowkey_cnt_, schema_column_count_);
    if (OB_FAIL(datum_utils_.init(cols_desc_, schema_rowkey_cnt_, is_oracle_mode_, allocator, is_cg_sstable))) {
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

/*
 * ------------------------------- ObCGReadInfo -------------------------------
 */
ObCGReadInfo::ObCGReadInfo()
  : need_release_(false),
    cg_basic_info_(nullptr),
    cols_param_(nullptr),
    cols_extend_()
{}

ObCGReadInfo::~ObCGReadInfo()
{
  reset();
}

/*
 * ------------------------------- ObCGReadInfoHandle -------------------------------
 */
ObCGReadInfoHandle::~ObCGReadInfoHandle()
{
  reset();
}

void ObCGReadInfoHandle::reset()
{
  if (OB_NOT_NULL(cg_read_info_)) {
    MTL(ObTenantCGReadInfoMgr *)->release_cg_read_info(cg_read_info_);
  }
  cg_read_info_ = nullptr;
}

/*
 * ------------------------------- ObTenantCGReadInfoMgr -------------------------------
*/
ObTenantCGReadInfoMgr::ObTenantCGReadInfoMgr()
  : allocator_(MTL_ID()),
    index_read_info_(),
    normal_cg_read_infos_(),
    lock_(),
    hold_cg_read_info_cnt_(0),
    in_progress_cnt_(0),
    alloc_buf_(nullptr),
    is_inited_(false)
{
}

ObTenantCGReadInfoMgr::~ObTenantCGReadInfoMgr()
{
  destroy();
}

int ObTenantCGReadInfoMgr::init()
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", K(ret), K(is_inited_));
  } else if (OB_FAIL(allocator_.init(nullptr,
      common::OB_MALLOC_NORMAL_BLOCK_SIZE,
      lib::ObMemAttr(MTL_ID(), "CGReadInfoMgr")))) {
    COMMON_LOG(WARN, "failed to init allocator", K(ret));
  } else if (OB_FAIL(construct_index_read_info(allocator_, index_read_info_))) {
    STORAGE_LOG(WARN, "Fail to construct index read info", K(ret));
  } else if (OB_FAIL(construct_normal_cg_read_infos())) {
    STORAGE_LOG(WARN, "Fail to constuct normal cg read infos", K(ret));
  } else {
    release_cg_read_info_array_.set_attr(ObMemAttr(MTL_ID(), "RSCompCkmPair"));
    is_inited_ = true;
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else if (OB_INIT_TWICE != ret) {
    destroy();
  }

  return ret;
}

#define WAIT_CNT(cnt_str, cnt) \
    while (ATOMIC_LOAD(&cnt) > 0) { \
      usleep(1000L); \
      if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) { \
        LOG_INFO("ObTenantCGReadInfoMgr wait release", cnt_str, ATOMIC_LOAD(&cnt)); \
      } \
    } // end of while

void ObTenantCGReadInfoMgr::destroy()
{
  int ret = OB_SUCCESS;
  LOG_INFO("ObTenantCGReadInfoMgr start to destroy");
  if (IS_INIT) {
    is_inited_ = false;
    WEAK_BARRIER();
    // Any cg read info should not be used during or after destroy
    WAIT_CNT("in_progress_cnt", in_progress_cnt_);
    WAIT_CNT("hold_cg_read_info_cnt", hold_cg_read_info_cnt_);
    if (OB_FAIL(gc_cg_info_array())) {
      LOG_WARN("failed to gc cg info", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ObObjType::ObMaxType; ++i) {
      ObObjType type = static_cast<ObObjType>(i);
      if (not_in_normal_cg_array(type)) {
      } else if (OB_NOT_NULL(normal_cg_read_infos_.at(i))) {
        free_cg_info(normal_cg_read_infos_.at(i), false/*free_ptr_flag*/);
      }
    } // end of for
    normal_cg_read_infos_.reset();
    if (OB_NOT_NULL(alloc_buf_)) {
      allocator_.free(alloc_buf_);
      alloc_buf_ = nullptr;
    }
    index_read_info_.reset();
    allocator_.reset();
    LOG_INFO("ObTenantCGReadInfoMgr destroyed");
  }
}

int ObTenantCGReadInfoMgr::get_index_read_info(const ObITableReadInfo *&index_read_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTenantCGReadInfoMgr not inited", K(ret));
  } else {
    index_read_info = &index_read_info_;
  }
  return ret;
}

int ObTenantCGReadInfoMgr::get_cg_read_info(const ObColDesc &col_desc,
                                            ObColumnParam *col_param,
                                            const ObTabletID &tablet_id,
                                            ObCGReadInfoHandle &cg_read_info_handle)
{
  int ret = OB_SUCCESS;

  cg_read_info_handle.reset();
  const ObObjType col_type = col_desc.col_type_.get_type();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTenantCGReadInfoMgr not inited", K(ret));
  } else if (tablet_id.is_inner_tablet()) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "Inner tablet is not supported", K(ret), K(tablet_id));
  } else if (OB_UNLIKELY(col_desc.col_order_ != ObOrderType::ASC  // TODO : @lvling support desc order
                         || col_type >= ObObjType::ObMaxType || skip_type(col_type))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), K(col_desc));
  } else {
    ATOMIC_INC(&in_progress_cnt_);
    if (not_in_normal_cg_array(col_type)) {
      // construct specific cg read info
      if (OB_FAIL(alloc_spec_cg_read_info(col_desc, col_param, cg_read_info_handle))) {
        LOG_WARN("failed to construct cg read info", K(ret));
      }
    } else {
      // normal cg read info
      if (OB_ISNULL(normal_cg_read_infos_.at(col_type))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null read info", K(ret), K(col_desc), K(normal_cg_read_infos_.at(col_type)));
      } else {
        cg_read_info_handle.set_read_info(normal_cg_read_infos_.at(col_type));
      }
    }
    if (OB_SUCC(ret)) {
      ATOMIC_INC(&hold_cg_read_info_cnt_);
    }
    ATOMIC_DEC(&in_progress_cnt_);
  }

  return ret;
}

// will release cg_read_info in destroy stage, not check inited_ here
int ObTenantCGReadInfoMgr::release_cg_read_info(ObCGReadInfo *&cg_read_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == cg_read_info)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KPC(cg_read_info));
  } else if (cg_read_info->need_release_) {
    lib::ObMutexGuard guard(lock_);
    if (OB_FAIL(release_cg_read_info_array_.push_back(cg_read_info))) {
      LOG_WARN("failed to push_back cg_read_info", K(ret), KP(cg_read_info));
      // failed to push, need to destroy now
      free_cg_info(cg_read_info, true/*free_ptr_flag*/);
    }
    // cg cg info cnt
    cg_read_info = nullptr;
    ATOMIC_DEC(&hold_cg_read_info_cnt_);
  } else {
    cg_read_info = nullptr;
    ATOMIC_DEC(&hold_cg_read_info_cnt_);
  }
  return ret;
}

int ObTenantCGReadInfoMgr::mtl_init(ObTenantCGReadInfoMgr *&read_info_mgr)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(read_info_mgr->init())) {
    STORAGE_LOG(WARN, "Fail to init tenant read info mgr", K(ret));
  }

  return ret;
}

// index read info for cg sstable (ROWID + VARCHAR)
int ObTenantCGReadInfoMgr::construct_index_read_info(ObIAllocator &allocator, ObRowkeyReadInfo &index_read_info)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObColDesc, 4> idx_cols_desc;
  ObColDesc row_id_col_desc;
  row_id_col_desc.col_type_.set_int();
  row_id_col_desc.col_id_ = OB_APP_MIN_COLUMN_ID;
  row_id_col_desc.col_order_ = common::ObOrderType::ASC;
  ObColDesc var_col_desc;
  var_col_desc.col_type_.set_varbinary();
  var_col_desc.col_id_ = OB_APP_MIN_COLUMN_ID + 1;
  var_col_desc.col_order_ = common::ObOrderType::ASC;
  if (OB_FAIL(idx_cols_desc.push_back(row_id_col_desc))) {
    STORAGE_LOG(WARN, "Fail to push back row id col desc", K(ret));
  } else if (OB_FAIL(idx_cols_desc.push_back(var_col_desc))) {
    STORAGE_LOG(WARN, "Fail to push back var col desc", K(ret));
  } else if (OB_FAIL(index_read_info.init(allocator,
                                          2, /* schema_column_count */
                                          1, /* schema_rowkey_count */
                                          lib::is_oracle_mode(),
                                          idx_cols_desc,
                                          true, /* is_cg_sstable */
                                          true /* use_default_compat_version */))) {
    STORAGE_LOG(WARN, "Fail to init mtl index read info", K(ret));
  }

  return ret;
}

int ObTenantCGReadInfoMgr::construct_normal_cg_read_infos()
{
  int ret = OB_SUCCESS;

  normal_cg_read_infos_.set_allocator(&allocator_);
  if (OB_FAIL(normal_cg_read_infos_.prepare_allocate(ObObjType::ObMaxType))) {
    STORAGE_LOG(WARN, "Fail to prepare allocate normal cg read info", K(ret));
  } else {
    int64_t array_cnt = 0;
    for (int64_t i = 0 ; i < ObObjType::ObMaxType ; ++i) {
      normal_cg_read_infos_.at(i) = nullptr;
      if (not_in_normal_cg_array((ObObjType)i)) {
      } else {
        ++array_cnt;
      }
    }
    // allocate buf for cg_array
    void *buf = nullptr;
    ObCGReadInfo *read_info_array = nullptr;
    ObReadInfoStruct *basic_info_array = nullptr;
    const int64_t alloc_size = (sizeof(ObCGReadInfo) + sizeof(ObReadInfoStruct)) * array_cnt;
    if (OB_ISNULL(buf = allocator_.alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc read info array", K(ret), K(alloc_size));
    } else {
      read_info_array = new(buf) ObCGReadInfo[array_cnt];
      basic_info_array = new((char *)buf + (sizeof(ObCGReadInfo) * array_cnt)) ObReadInfoStruct[array_cnt];
      alloc_buf_ = buf;
      ObColExtend tmp_col_extend;
      tmp_col_extend.skip_index_attr_.set_min_max();
      int64_t idx = 0;
      for (int64_t i = 0 ; OB_SUCC(ret) && i < ObObjType::ObMaxType ; ++i) {
        ObObjType type = static_cast<ObObjType>(i);
        ObColDesc tmp_desc;
        ObCGReadInfo &tmp_read_info = read_info_array[idx];
        normal_cg_read_infos_.at(i) = &tmp_read_info;
        if (not_in_normal_cg_array(type)) {
        } else if (FALSE_IT(set_col_desc(type, tmp_desc))) {
        } else if (FALSE_IT(tmp_read_info.cg_basic_info_ = &basic_info_array[idx])) {
        } else if (OB_FAIL(tmp_read_info.cg_basic_info_->generate_for_column_store(allocator_, tmp_desc, index_read_info_.is_oracle_mode()))) {
          STORAGE_LOG(WARN, "Fail to generate column group read info", K(ret));
        } else if (OB_FAIL(tmp_read_info.cols_extend_.init(1, allocator_))) {
          STORAGE_LOG(WARN, "Fail to init columns extend", K(ret));
        } else if (OB_FAIL(tmp_read_info.cols_extend_.push_back(tmp_col_extend))) {
          STORAGE_LOG(WARN, "Fail to push col extend", K(ret));
        } else {
          tmp_read_info.need_release_ = false;
          idx++;
        }
      } // end of for
      // if failed, will call destroy outside
    }
  }

  return ret;
}

int ObTenantCGReadInfoMgr::construct_cg_read_info(
    common::ObIAllocator &allocator,
    const bool is_oracle_mode,
    const ObColDesc &col_desc,
    ObColumnParam *col_param,
    ObTableReadInfo &cg_read_info)
{
  int ret = OB_SUCCESS;
  share::schema::ObColExtend tmp_col_extend;
  tmp_col_extend.skip_index_attr_.set_min_max();
  ObSEArray<ObColDesc, 1> tmp_access_cols_desc;
  ObSEArray<ObColumnParam *, 1> tmp_access_cols_param;
  ObSEArray<int32_t, 1> tmp_access_cols_index;
  ObSEArray<ObColExtend, 1> tmp_access_cols_extend;
  if (OB_FAIL(tmp_access_cols_desc.push_back(col_desc))) {
    LOG_WARN("Fail to push back col desc", K(ret));
  } else if (nullptr != col_param && OB_FAIL(tmp_access_cols_param.push_back(col_param))) {
    LOG_WARN("Fail to push back col param", K(ret));
  } else if (OB_FAIL(tmp_access_cols_index.push_back(0))) {
    LOG_WARN("Fail to push back col index", K(ret));
  } else if (OB_FAIL(tmp_access_cols_extend.push_back(tmp_col_extend))) {
    LOG_WARN("Fail to push_back tmp_col_extend", K(ret));
  } else if (OB_FAIL(cg_read_info.init(allocator,
                                       ObCGReadInfo::CG_COL_CNT,
                                       ObCGReadInfo::CG_ROWKEY_COL_CNT,
                                       is_oracle_mode,
                                       tmp_access_cols_desc,
                                       &tmp_access_cols_index,
                                       tmp_access_cols_param.empty() ? nullptr : &tmp_access_cols_param,
                                       nullptr,
                                       &tmp_access_cols_extend,
                                       false,
                                       true))) {
    LOG_WARN("Fail to init cg read info", K(ret));
  }
  return ret;
}

void ObTenantCGReadInfoMgr::inner_print_log()
{
  int ret = OB_SUCCESS;
  if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
    LOG_INFO("ObTenantCGReadInfoMgr current status", "hold_cg_read_info_cnt", ATOMIC_LOAD(&hold_cg_read_info_cnt_));
  }
}

int ObTenantCGReadInfoMgr::alloc_spec_cg_read_info(
    const ObColDesc &col_desc,
    const ObColumnParam *col_param,
    ObCGReadInfoHandle &cg_read_info_handle)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObCGReadInfo *cg_info = nullptr;
  const int64_t basic_info_alloc_size = sizeof(ObCGReadInfo) + sizeof(ObReadInfoStruct);
  const int64_t alloc_size = basic_info_alloc_size + (nullptr != col_param ? (sizeof(Columns) + sizeof(ObColumnParam)) : 0);
  if (OB_ISNULL(buf = allocator_.alloc(alloc_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory for cg read info", K(ret), K(alloc_size));
  } else {
    cg_info = new (buf) ObCGReadInfo();
    cg_info->need_release_ = true;
    cg_info->cg_basic_info_ = new ((char *)buf + sizeof(ObCGReadInfo)) ObReadInfoStruct();
    ObColExtend tmp_col_extend;
    tmp_col_extend.skip_index_attr_.set_min_max();
    if (OB_FAIL(cg_info->cg_basic_info_->generate_for_column_store(allocator_, col_desc, index_read_info_.is_oracle_mode()))) {
      STORAGE_LOG(WARN, "Fail to generate column group read info", K(ret));
    } else if (OB_FAIL(cg_info->cols_extend_.init(1, allocator_))) {
      STORAGE_LOG(WARN, "Fail to init columns extend", K(ret));
    } else if (OB_FAIL(cg_info->cols_extend_.push_back(tmp_col_extend))) {
      STORAGE_LOG(WARN, "Fail to push col extend", K(ret));
    } else if (nullptr != col_param) { // assign col param
      Columns *cols_param_array = new((char *)buf + basic_info_alloc_size) Columns();
      ObColumnParam *tmp_col_param = new ((char *)buf + basic_info_alloc_size + sizeof(Columns)) ObColumnParam(allocator_);

      if (OB_FAIL(tmp_col_param->assign(*col_param))) {
        LOG_WARN("Failed to assign col param", K(ret), KPC(col_param));
      } else if (OB_FAIL(cols_param_array->init(ObCGReadInfo::CG_COL_CNT, allocator_))) {
        LOG_WARN("Fail to reserve cols param", K(ret));
      } else if (OB_FAIL(cols_param_array->push_back(tmp_col_param))) {
        LOG_WARN("failed to push back col param", K(ret), KPC(tmp_col_param));
      } else {
        cg_info->cols_param_ = cols_param_array;
      }

      if (OB_FAIL(ret)) {
        cols_param_array->destroy();
        tmp_col_param->destroy();
      }
    }

    if (OB_SUCC(ret)) {
      cg_read_info_handle.set_read_info(cg_info);
    } else {
      cg_info->cg_basic_info_->reset();
      cg_info->reset();
      cg_info = nullptr;
      allocator_.free(buf);
    }
  }

  return ret;
}

int ObTenantCGReadInfoMgr::gc_cg_info_array()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(lock_);
  for (int64_t idx = 0; idx < release_cg_read_info_array_.count(); ++idx) {
    free_cg_info(release_cg_read_info_array_.at(idx), true/*free_ptr_flag*/);
    release_cg_read_info_array_.at(idx) = nullptr; // clear ptr in array
  } // end of for
  if (release_cg_read_info_array_.count() > 0) {
    LOG_INFO("success to release cg read info array", K(ret),
        "release_cnt", release_cg_read_info_array_.count(),
        "hold_cg_read_info_cnt", ATOMIC_LOAD(&hold_cg_read_info_cnt_));
    release_cg_read_info_array_.reuse();
  }
  return ret;
}

void ObTenantCGReadInfoMgr::free_cg_info(ObCGReadInfo *&cg_read_info, const bool free_ptr_flag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == cg_read_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cg read info to release", K(ret), KP(cg_read_info));
  } else {
    if (nullptr != cg_read_info->cols_param_) { // need release cols param
      if (OB_UNLIKELY(cg_read_info->cols_param_->is_empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cg read info have cols_param array, but array is unexpected empty",
            K(ret), KPC(cg_read_info->cols_param_));
      } else {
        cg_read_info->cols_param_->at(0)->destroy();
        cg_read_info->cols_param_->destroy();
        cg_read_info->cols_param_ = nullptr;
      }
    }
    if (OB_ISNULL(cg_read_info->cg_basic_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cg basic info is unexpected null", KR(ret), KPC(cg_read_info));
    } else {
      cg_read_info->cg_basic_info_->reset();
    }
    cg_read_info->~ObCGReadInfo();
    if (free_ptr_flag) {
      allocator_.free(cg_read_info);
    }
    cg_read_info = nullptr;
  }
}

void ObTenantCGReadInfoMgr::set_col_desc(const ObObjType type, ObColDesc &col_desc)
{
  col_desc.col_id_ = UINT32_MAX;
  col_desc.col_order_ = ObOrderType::ASC;
  if (ObObjType::ObRawType == type) {
    col_desc.col_type_.set_raw();
  } else {
    col_desc.col_type_.set_type(type);
  }
}

bool ObTenantCGReadInfoMgr::skip_type(const ObObjType type)
{
  return ObObjType::ObUnknownType == type || ObObjType::ObEnumInnerType == type
         || ObObjType::ObSetInnerType == type || ObObjType::ObLobType == type;
}

}
}
