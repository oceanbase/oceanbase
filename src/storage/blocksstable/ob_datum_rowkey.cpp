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

#include "ob_datum_rowkey.h"
#include "ob_datum_range.h"
#include "share/schema/ob_table_param.h"

namespace oceanbase
{
namespace blocksstable
{

static ObStorageDatum make_ext_datum(const int64_t ext_value) { ObStorageDatum datum; datum.set_ext_value(ext_value); return datum; }

ObStorageDatum ObDatumRowkey::MIN_DATUM = make_ext_datum(ObObj::MIN_OBJECT_VALUE);
ObStorageDatum ObDatumRowkey::MAX_DATUM = make_ext_datum(ObObj::MAX_OBJECT_VALUE);
ObDatumRowkey ObDatumRowkey::MIN_ROWKEY(&ObDatumRowkey::MIN_DATUM, 1);
ObDatumRowkey ObDatumRowkey::MAX_ROWKEY(&ObDatumRowkey::MAX_DATUM, 1);

ObDatumRowkey::ObDatumRowkey(ObStorageDatum *datums, const int64_t datum_cnt)
  : datum_cnt_(datum_cnt),
    group_idx_(0),
    hash_(0),
    datums_(datums),
    store_rowkey_()
{}

ObDatumRowkey::ObDatumRowkey(ObStorageDatumBuffer &datum_buffer)
  : datum_cnt_(datum_buffer.get_capacity()),
    group_idx_(0),
    hash_(0),
    datums_(datum_buffer.get_datums()),
    store_rowkey_()
{
}

int ObDatumRowkey::murmurhash(const uint64_t seed, const ObStorageDatumUtils &datum_utils, uint64_t &hash) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || !datum_utils.is_valid() || datum_utils.get_rowkey_count() < datum_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to calc hash", K(ret), K(datum_utils), K(*this));
  } else {
    hash = seed;
    if (is_ext_rowkey()) {
      if (OB_FAIL(datum_utils.get_ext_hash_funcs().hash_func_(datums_[0], hash, hash))) {
        STORAGE_LOG(WARN, "fail to calc hash", K(ret));
      }
    } else {
      for (int64_t i = 0; i < datum_cnt_ && OB_SUCC(ret); i++) {
        if (OB_FAIL(datum_utils.get_hash_funcs().at(i).hash_func_(datums_[i], hash, hash))) {
          STORAGE_LOG(WARN, "fail to calc hash", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDatumRowkey::equal(const ObDatumRowkey &rhs, const ObStorageDatumUtils &datum_utils, bool &is_equal) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid() || !rhs.is_valid() || !datum_utils.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to compare datum rowkey", K(ret), K(*this), K(rhs), K(datum_utils));
  } else if (FALSE_IT(is_equal = datum_cnt_ == rhs.datum_cnt_)) {
  } else if (is_equal && datums_ != rhs.datums_) {
    if (datum_utils.get_rowkey_count() < datum_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error for datum utils without enough cols", K(ret), K(datum_cnt_), K(datum_utils));
    } else {
      const ObStoreCmpFuncs &cmp_funcs = datum_utils.get_cmp_funcs();
      int cmp_ret = 0;
      for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < datum_cnt_; i++) {
        if (OB_FAIL(cmp_funcs.at(i).compare(datums_[i], rhs.datums_[i], cmp_ret))) {
          STORAGE_LOG(WARN, "Failed to compare datum rowkey", K(ret), K(i), K(*this), K(rhs));
        } else {
          is_equal = 0 == cmp_ret;
        }
      }
    }
  }

  return ret;
}

int ObDatumRowkey::compare(const ObDatumRowkey &rhs, const ObStorageDatumUtils &datum_utils, int &cmp_ret,
                           const bool compare_datum_cnt) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid() || !rhs.is_valid() || !datum_utils.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to compare datum rowkey", K(ret), K(*this), K(rhs), K(datum_utils));
  } else {
    int64_t cmp_cnt = MIN(datum_cnt_, rhs.datum_cnt_);
    if (datum_utils.get_rowkey_count() < cmp_cnt) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error for datum utils without enough cols", K(ret), K(cmp_cnt), K(datum_utils));
    } else {
      const ObStoreCmpFuncs &cmp_funcs = datum_utils.get_cmp_funcs();
      cmp_ret = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < cmp_cnt && 0 == cmp_ret; ++i) {
        if (OB_FAIL(cmp_funcs.at(i).compare(datums_[i], rhs.datums_[i], cmp_ret))) {
          STORAGE_LOG(WARN, "Failed to compare datum rowkey", K(ret), K(i), K(*this), K(rhs));
        }
      }
      if (0 == cmp_ret && compare_datum_cnt) {
        cmp_ret = datum_cnt_ - rhs.datum_cnt_;
      }
    }
  }

  return ret;
}

OB_DEF_SERIALIZE(ObDatumRowkey)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "datum row key is invalid", KPC(this));
  } else {
    OB_UNIS_ENCODE_ARRAY(datums_, datum_cnt_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDatumRowkey)
{
  int ret = OB_SUCCESS;
  reuse();
  if (OB_ISNULL(datums_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "datum row key is not init", K(ret), KP(datums_));
  } else {
    OB_UNIS_DECODE(datum_cnt_);
    if (datum_cnt_ > OB_INNER_MAX_ROWKEY_COLUMN_NUMBER) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "table store inner max rowkey column number exceed the limit, too large", K(ret), K(datum_cnt_));
    }
    OB_UNIS_DECODE_ARRAY(datums_, datum_cnt_);
    hash_ = 0;
    group_idx_ = 0;
    store_rowkey_.reset();
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDatumRowkey)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(datums_, datum_cnt_);
  return len;
}


DEF_TO_STRING(ObDatumRowkey)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(datum_cnt), K_(group_idx), K_(hash));
  J_COMMA();
  J_ARRAY_START();
  if (nullptr != buf && buf_len >= 0) {
    if (nullptr != datums_) {
      for (int64_t i = 0; i < datum_cnt_; ++i) {
        databuff_printf(buf, buf_len, pos, "idx=%ld:", i);
        pos += datums_[i].storage_to_string(buf + pos, buf_len - pos);
        databuff_printf(buf, buf_len, pos, ",");
      }
    } else {
      J_EMPTY_OBJ();
    }
  }
  J_ARRAY_END();
  J_KV(K_(store_rowkey));
  J_OBJ_END();
  return pos;
}

void ObDatumRowkey::destroy(ObIAllocator &allocator)
{
  if (OB_NOT_NULL(datums_)) {
    allocator.free(datums_);
    datums_ = nullptr;
  }
  reset();
}

// shallow copy the obj value
int ObDatumRowkey::from_rowkey(const ObRowkey &rowkey, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObStorageDatum *datums = nullptr;

  if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to transfer from rowkey to datum rowkey", K(ret), K(rowkey));
  } else if (rowkey.is_max_row()) {
    set_max_rowkey();
  } else if (rowkey.is_min_row()) {
    set_min_rowkey();
  } else {
    datum_cnt_ = rowkey.get_obj_cnt();
    if (OB_ISNULL(datums = reinterpret_cast<ObStorageDatum *>(allocator.alloc(sizeof(ObStorageDatum) * datum_cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc memory", K(ret), K(datum_cnt_));
    } else  {
      // maybe we do not need the constructor
      datums = new (datums) ObStorageDatum[datum_cnt_];
      datums_ = datums;
      for (int64_t i = 0; OB_SUCC(ret) && i < datum_cnt_; i++) {
        if (OB_FAIL(datums[i].from_obj_enhance(rowkey.get_obj_ptr()[i]))) {
          STORAGE_LOG(WARN, "Failed to from obj to datum", K(ret), K(i));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    group_idx_ = 0;
    hash_ = 0;
    store_rowkey_.reset();
    store_rowkey_.get_rowkey() = rowkey;
  } else if (nullptr != datums) {
    allocator.free(datums);
  }

  return ret;
}

int ObDatumRowkey::from_rowkey(const ObRowkey &rowkey, ObStorageDatumBuffer &datum_buffer)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to transfer from rowkey to datum rowkey", K(ret), K(rowkey));
  } else if (rowkey.is_max_row()) {
    set_max_rowkey();
  } else if (rowkey.is_min_row()) {
    set_min_rowkey();
  } else if (OB_FAIL(datum_buffer.reserve(rowkey.get_obj_cnt()))) {
    STORAGE_LOG(WARN, "Failed to reserver datum buffer", K(ret));
  } else {
    ObStorageDatum *datums = datum_buffer.get_datums();
    datum_cnt_ = rowkey.get_obj_cnt();
    datums_ = datums;
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_cnt_; i++) {
      if (OB_FAIL(datums[i].from_obj_enhance(rowkey.get_obj_ptr()[i]))) {
        STORAGE_LOG(WARN, "Failed to from obj to datum", K(ret), K(i), K(rowkey));
      }
    }
  }
  if (OB_SUCC(ret)) {
    group_idx_ = 0;
    hash_ = 0;
    store_rowkey_.reset();
    store_rowkey_.get_rowkey() = rowkey;
  }

  return ret;
}


int ObDatumRowkey::to_store_rowkey(const common::ObIArray<share::schema::ObColDesc> &col_descs,
                                   common::ObIAllocator &allocator,
                                   common::ObStoreRowkey &store_rowkey) const
{
  int ret = OB_SUCCESS;
  common::ObObj *objs = nullptr;
  if (is_max_rowkey()) {
    store_rowkey.set_max();
  } else if (is_min_rowkey()) {
    store_rowkey.set_min();
  } else if (OB_UNLIKELY(!is_valid() || col_descs.count() < datum_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to transfer to store rowkey", K(ret), K(*this), K(col_descs.count()));
  } else if (OB_ISNULL(objs = reinterpret_cast<common::ObObj*>(allocator.alloc(sizeof(common::ObObj) * datum_cnt_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc memory for obj buffer", K(ret), K(datum_cnt_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_cnt_; i++) {
      if (OB_FAIL(datums_[i].to_obj_enhance(objs[i], col_descs.at(i).col_type_))) {
        STORAGE_LOG(WARN, "Failed to transfer datum to obj", K(ret), K(i), K(datums_[i]));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(store_rowkey.assign(objs, datum_cnt_))) {
        STORAGE_LOG(WARN, "Failed to assign rowkey", K(ret), K(*this), K(objs));
      }
    }
  }

  return ret;
}

int ObDatumRowkey::to_multi_version_rowkey(const bool min_value,
                                           common::ObIAllocator &allocator,
                                           ObDatumRowkey &dest) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to transfer multi version rowkey", K(ret), K(*this));
  } else if (is_max_rowkey()) {
    dest.set_max_rowkey();
  } else if (is_min_rowkey()) {
    dest.set_min_rowkey();
  } else {
    ObStorageDatum *datums = nullptr;
    // FIXME: hard coding
    const int64_t datum_cnt = datum_cnt_  + 1;
    if (OB_ISNULL(datums = (ObStorageDatum*) allocator.alloc(sizeof(ObStorageDatum) * datum_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "Failed to alloc memory for multi version rowkey", K(ret), K(datum_cnt));
    } else {
      datums = new (datums) ObStorageDatum[datum_cnt];
      for (int64_t i = 0; i < datum_cnt_; ++ i) {
        datums[i] = datums_[i];
      }
      if (min_value) {
        datums[datum_cnt_].set_min();
      } else {
        datums[datum_cnt_].set_max();
      }
      if (OB_FAIL(dest.assign(datums, datum_cnt))) {
        STORAGE_LOG(WARN, "Failed to assign datum rowkey", K(ret), KP(datums), K(datum_cnt));
        dest.reset();
        allocator.free(datums);
        datums = nullptr;
      }
    }
  }

  return ret;
}


int ObDatumRowkey::to_multi_version_range(common::ObIAllocator &allocator, ObDatumRange &dest) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to transfer multi version datum range", K(ret), K(*this));
  } else if (OB_FAIL(to_multi_version_rowkey(true/*min*/, allocator, dest.start_key_))) {
    STORAGE_LOG(WARN, "Failed to transfer start key", K(ret), K(*this));
  } else if (OB_FAIL(to_multi_version_rowkey(false/*max*/, allocator, dest.end_key_))) {
    STORAGE_LOG(WARN, "Failed to transfer end key", K(ret), K(*this));
  } else {
    dest.border_flag_.unset_inclusive_end();
    dest.border_flag_.unset_inclusive_start();
    dest.group_idx_ = group_idx_;
  }

  return ret;
}

void ObDatumRowkey::reuse()
{
  group_idx_ = 0;
  store_rowkey_.reset();
  hash_ = 0;
  for (int64_t i = 0; i < datum_cnt_; ++i) {
    datums_[i].reuse();
  }
}

int ObDatumRowkeyHelper::convert_datum_rowkey(const common::ObRowkey &rowkey, ObDatumRowkey &datum_rowkey)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to transfer datum rowkey", K(ret), K(rowkey));
  } else if (OB_FAIL(datum_rowkey.from_rowkey(rowkey, datum_buffer_))) {
    STORAGE_LOG(WARN, "Failed to transfer datum rowkey", K(ret), K(rowkey));
  }

  return ret;
}

int ObDatumRowkeyHelper::convert_store_rowkey(const ObDatumRowkey &datum_rowkey,
                                              const ObIArray<share::schema::ObColDesc> &col_descs,
                                              common::ObStoreRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *objs = nullptr;

  if (OB_UNLIKELY(!datum_rowkey.is_valid() || col_descs.count() < datum_rowkey.get_datum_cnt())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to transfer datum rowkey", K(ret), K(rowkey), K(col_descs));
  } else if (!obj_buffer_.is_inited() && OB_FAIL(obj_buffer_.init(allocator_))) {
    STORAGE_LOG(WARN, "Failed to init obj_buf array", K(ret));
  } else if (OB_FAIL(obj_buffer_.reserve(datum_rowkey.get_datum_cnt()))) {
    STORAGE_LOG(WARN, "Failed to reserve obj buffer", K(ret), K(datum_rowkey));
  } else if (OB_ISNULL(objs = obj_buffer_.get_data())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null obj buffer", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_rowkey.get_datum_cnt(); i++) {
      if (OB_FAIL(datum_rowkey.datums_[i].to_obj_enhance(objs[i], col_descs.at(i).col_type_))) {
        STORAGE_LOG(WARN, "Failed to transfer datum to obj", K(ret), K(i), K(datum_rowkey));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rowkey.assign(objs, datum_rowkey.get_datum_cnt()))) {
        STORAGE_LOG(WARN, "Failed to assign rowkey", K(ret), K(datum_rowkey), K(objs));
      }
    }
  }

  return ret;
}


int ObDatumRowkeyHelper::reserve(const int64_t rowkey_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(rowkey_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to reverve datum roweky", K(ret), K(rowkey_cnt));
  } else if (datum_buffer_.get_capacity() >= rowkey_cnt) {
  } else if (OB_FAIL(datum_buffer_.reserve(rowkey_cnt))) {
    STORAGE_LOG(WARN, "Failed to reserve datum buffer", K(ret), K(rowkey_cnt));
  }

  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
