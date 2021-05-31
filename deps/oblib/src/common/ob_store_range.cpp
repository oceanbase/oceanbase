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

#include "common/ob_store_range.h"

namespace oceanbase {
namespace common {

// ObObjs in start_key/end_key are only shallow-copied
int ObStoreRange::to_new_range(const ObIArrayWrap<ObOrderType>& column_orders, const int64_t rowkey_cnt,
    ObNewRange& range, ObIAllocator& allocator) const
{
  int ret = OB_SUCCESS;

  if (rowkey_cnt <= 0 || column_orders.count() < rowkey_cnt) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "invalid argument", K(rowkey_cnt), K(column_orders.count()));
  } else if (!start_key_.is_valid() || !end_key_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "invalid start or end key", K(start_key_), K(end_key_), K(ret));
  } else if (start_key_.get_obj_cnt() != rowkey_cnt || end_key_.get_obj_cnt() != rowkey_cnt) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR,
        "range start and end key obj_cnt does not match!",
        K(start_key_.get_obj_cnt()),
        K(end_key_.get_obj_cnt()),
        K(ret));
  } else {
    ObObj* start_key_array = NULL;
    ObObj* end_key_array = NULL;

    if (OB_ISNULL(start_key_array = reinterpret_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * rowkey_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "allocate memory for start_key_array failed", K(rowkey_cnt), K(ret));
    } else if (OB_ISNULL(end_key_array = reinterpret_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * rowkey_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "allocate memory for end_key_array failed", K(rowkey_cnt), K(ret));
    } else {
      bool swapped = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; i++) {
        ObOrderType order = column_orders.at(i);
        if (ObOrderType::ASC == order) {
          start_key_array[i] = start_key_.get_obj_ptr()[i];
          end_key_array[i] = end_key_.get_obj_ptr()[i];
        } else if (ObOrderType::DESC == order) {
          swapped = true;
          start_key_array[i] = end_key_.get_obj_ptr()[i];
          end_key_array[i] = start_key_.get_obj_ptr()[i];
        } else {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(ERROR, "illegal column order value!", K(ret), K(order));
        }
      }
      if (OB_SUCC(ret)) {
        range.table_id_ = table_id_;
        range.start_key_ = ObRowkey(start_key_array, rowkey_cnt);
        range.end_key_ = ObRowkey(end_key_array, rowkey_cnt);
        range.border_flag_ = border_flag_;
        // if swap happened, we need to swap border as well
        if (swapped && (border_flag_.inclusive_start() != border_flag_.inclusive_end())) {
          if (border_flag_.inclusive_start()) {
            range.border_flag_.unset_inclusive_start();
            range.border_flag_.set_inclusive_end();
          } else {
            range.border_flag_.set_inclusive_start();
            range.border_flag_.unset_inclusive_end();
          }
        }
      }
    }
  }

  return ret;
}

int ObStoreRange::set_whole_range(const ObIArray<ObOrderType>* column_orders, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(column_orders)) {
    start_key_.set_min();
    end_key_.set_max();
  } else if (OB_FAIL(start_key_.set_min(*column_orders, allocator))) {
    COMMON_LOG(WARN, "set start_key_ to min failed", K(ret));
  } else if (OB_FAIL(end_key_.set_max(*column_orders, allocator))) {
    COMMON_LOG(WARN, "set end_key_ to max failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    border_flag_.unset_inclusive_start();
    border_flag_.unset_inclusive_end();
  }

  return ret;
}

int ObStoreRange::is_whole_range(
    const ObIArrayWrap<ObOrderType>& column_orders, const int64_t rowkey_cnt, bool& is_whole) const
{
  int ret = OB_SUCCESS;
  bool is_start_key_min = false;
  bool is_end_key_max = false;

  if (OB_FAIL(start_key_.is_min(column_orders, rowkey_cnt, is_start_key_min))) {
    COMMON_LOG(WARN, "check if start_key_ is min failed", K(start_key_), K(ret));
  } else if (OB_FAIL(end_key_.is_max(column_orders, rowkey_cnt, is_end_key_max))) {
    COMMON_LOG(WARN, "check if end_key_ is max failed", K(end_key_), K(ret));
  } else {
    is_whole = is_start_key_min && is_end_key_max;
  }

  return ret;
}

int64_t ObStoreRange::to_string(char* buffer, const int64_t length) const
{
  int64_t pos = 0;
  if (NULL == buffer || 0 >= length) {
  } else {
    databuff_printf(buffer, length, pos, "{\"range\":\"");

    if (OB_INVALID_ID != table_id_) {
      databuff_printf(buffer, length, pos, "table_id:%ld,", table_id_);
    } else {
      databuff_printf(buffer, length, pos, "table_id:null,");
    }

    if (border_flag_.inclusive_start()) {
      databuff_printf(buffer, length, pos, "[");
    } else {
      databuff_printf(buffer, length, pos, "(");
    }

    pos += start_key_.to_string(buffer + pos, length - pos);
    databuff_printf(buffer, length, pos, ";");
    pos += end_key_.to_string(buffer + pos, length - pos);

    if (border_flag_.inclusive_end()) {
      databuff_printf(buffer, length, pos, "]");
    } else {
      databuff_printf(buffer, length, pos, ")");
    }

    databuff_printf(buffer, length, pos, "\"}");
  }

  return pos;
}

int64_t ObStoreRange::to_plain_string(char* buffer, const int64_t length) const
{
  int64_t pos = 0;
  if (NULL == buffer || 0 >= length) {
  } else {
    if (border_flag_.inclusive_start()) {
      databuff_printf(buffer, length, pos, "[");
    } else {
      databuff_printf(buffer, length, pos, "(");
    }
    pos += start_key_.to_plain_string(buffer + pos, length - pos);
    databuff_printf(buffer, length, pos, " ; ");
    pos += end_key_.to_plain_string(buffer + pos, length - pos);
    if (border_flag_.inclusive_end()) {
      databuff_printf(buffer, length, pos, "]");
    } else {
      databuff_printf(buffer, length, pos, ")");
    }
  }
  return pos;
}

int ObStoreRange::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, static_cast<int64_t>(table_id_)))) {
    COMMON_LOG(WARN, "serialize table_id failed", KP(buf), K(buf_len), K(table_id_), K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, border_flag_.get_data()))) {
    COMMON_LOG(WARN, "serialize border_falg failed", KP(buf), K(buf_len), K(pos), K(border_flag_), K(ret));
  } else if (OB_FAIL(start_key_.serialize(buf, buf_len, pos))) {
    COMMON_LOG(WARN, "serialize start_key failed", KP(buf), K(buf_len), K(pos), K(start_key_), K(ret));
  } else if (OB_FAIL(end_key_.serialize(buf, buf_len, pos))) {
    COMMON_LOG(WARN, "serialize end_key failed", KP(buf), K(buf_len), K(pos), K(end_key_), K(ret));
  }
  return ret;
}

int64_t ObStoreRange::get_serialize_size(void) const
{
  int64_t total_size = 0;

  total_size += serialization::encoded_length_vi64(table_id_);
  total_size += serialization::encoded_length_i8(border_flag_.get_data());

  total_size += start_key_.get_serialize_size();
  total_size += end_key_.get_serialize_size();

  return total_size;
}

int ObStoreRange::deserialize(ObIAllocator& allocator, const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  ObObj array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
  ObStoreRange copy_range;
  copy_range.start_key_.assign(array, OB_MAX_ROWKEY_COLUMN_NUMBER);
  copy_range.end_key_.assign(array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
  if (OB_FAIL(copy_range.deserialize(buf, data_len, pos))) {
    COMMON_LOG(WARN, "deserialize range to shallow copy range failed.", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(copy_range.deep_copy(allocator, *this))) {
    COMMON_LOG(WARN, "deep_copy range failed.", KP(buf), K(data_len), K(pos), K(copy_range), K(ret));
  }

  return ret;
}

int ObStoreRange::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int8_t flag = 0;

  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, reinterpret_cast<int64_t*>(&table_id_)))) {
    COMMON_LOG(WARN, "deserialize table_id failed.", KP(buf), K(data_len), K(pos), K(table_id_), K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, &flag))) {
    COMMON_LOG(WARN, "deserialize flag failed.", KP(buf), K(data_len), K(pos), K(flag), K(ret));
  } else if (OB_FAIL(start_key_.deserialize(buf, data_len, pos))) {
    COMMON_LOG(WARN, "deserialize start_key failed.", KP(buf), K(data_len), K(pos), K(start_key_), K(ret));
  } else if (OB_FAIL(end_key_.deserialize(buf, data_len, pos))) {
    COMMON_LOG(WARN, "deserialize end_key failed.", KP(buf), K(data_len), K(pos), K(end_key_), K(ret));
  } else {
    border_flag_.set_data(flag);
  }
  return ret;
}

int ObStoreRange::deep_copy(ObIAllocator& allocator, ObStoreRange& dst) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(start_key_.deep_copy(dst.start_key_, allocator))) {
    COMMON_LOG(WARN, "deep copy start key failed.", K(start_key_), K(ret));
  } else if (OB_FAIL(end_key_.deep_copy(dst.end_key_, allocator))) {
    COMMON_LOG(WARN, "deep copy end key failed.", K(end_key_), K(ret));
  } else {
    dst.table_id_ = table_id_;
    dst.border_flag_ = border_flag_;
  }

  return ret;
}

ObExtStoreRange::ObExtStoreRange() : range_(), ext_start_key_(), ext_end_key_()
{}

ObExtStoreRange::ObExtStoreRange(const ObStoreRange& range) : range_(range), ext_start_key_(), ext_end_key_()
{}

void ObExtStoreRange::reset()
{
  range_.reset();
  ext_start_key_.reset();
  ext_end_key_.reset();
}

int ObExtStoreRange::deep_copy(ObExtStoreRange& ext_range, ObIAllocator& allocator) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(range_.deep_copy(allocator, ext_range.range_))) {
    COMMON_LOG(WARN, "Fail to deep copy range, ", K(ret));
  } else if (OB_FAIL(ext_start_key_.deep_copy(ext_range.ext_start_key_, allocator))) {
    COMMON_LOG(WARN, "Fail to deep copy collation free start key", K(ret));
  } else if (OB_FAIL(ext_end_key_.deep_copy(ext_range.ext_end_key_, allocator))) {
    COMMON_LOG(WARN, "Fail to deep copy collation free end key", K(ret));
  }
  return ret;
}

void ObExtStoreRange::change_boundary(const ObStoreRowkey& gap_key, bool is_reverse, bool exclusive)
{
  if (is_reverse) {
    range_.set_end_key(gap_key);
    if (exclusive) {
      range_.set_right_open();
    } else {
      range_.set_right_closed();
    }
  } else {
    range_.set_start_key(gap_key);
    if (exclusive) {
      range_.set_left_open();
    } else {
      range_.set_left_closed();
    }
  }
}

// Current deep deserialize implementation has redundant code with the shallow deserialize
// implementation.
// We do not use the standard shallow_deserialization + deep_copy implementation for deep
// deserialization, because deep_copy requires the ObObj array of range_.start_key_ and
// range_.end_key_ to be prepared, which in turn requires us to know the implementation details of
// range_, which we should have no knowledge of.
// This inadequacy stems from the implementation of ObRowkey::deserialize(the shallow version),
// which requires its caller to know the implementation details ObRowkey to prepare the ObObj
// array, and should be fixed later.
int ObExtStoreRange::deserialize(common::ObIAllocator& allocator, const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t length = 0;
  int64_t cur_pos = pos;
  if (OB_FAIL(serialization::decode_i64(buf, data_len, cur_pos, &version))) {
    COMMON_LOG(WARN, "Fail to deserialize version, ", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, cur_pos, &length))) {
    COMMON_LOG(WARN, "Fail to deserialize length, ", K(ret));
  } else if (OB_FAIL(range_.deserialize(allocator, buf, data_len, cur_pos))) {
    COMMON_LOG(WARN, "Fail to deserialize range, ", K(ret));
  } else if (OB_FAIL(to_collation_free_range_on_demand_and_cutoff_range(allocator))) {
    COMMON_LOG(WARN, "fail to get collation free rowkey and range cutoff", K(ret));
  } else {
    pos += length;
  }
  return ret;
}

// Shallow deserialization DONT USE
int ObExtStoreRange::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t length = 0;
  int64_t cur_pos = pos;
  if (OB_FAIL(serialization::decode_i64(buf, data_len, cur_pos, &version))) {
    COMMON_LOG(WARN, "Fail to deserialize version, ", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, cur_pos, &length))) {
    COMMON_LOG(WARN, "Fail to deserialize length, ", K(ret));
  } else if (OB_FAIL(range_.deserialize(buf, data_len, cur_pos))) {
    COMMON_LOG(WARN, "Fail to deserialize range, ", K(ret));
  } else {
    pos += length;
  }
  return ret;
}

int ObExtStoreRange::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t length = get_serialize_size();
  if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, version))) {
    COMMON_LOG(WARN, "Fail to serialize version, ", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, length))) {
    COMMON_LOG(WARN, "Fail to serialize length, ", K(ret));
  } else if (OB_FAIL(range_.serialize(buf, buf_len, pos))) {
    COMMON_LOG(WARN, "Fail to serialize range, ", K(ret));
  }
  // no need to deserialize ext_start_key and ext_end_key
  return ret;
}

int64_t ObExtStoreRange::get_serialize_size(void) const
{
  int64_t size = 0;
  // version
  size += serialization::encoded_length_i64(1L);
  // length
  size += serialization::encoded_length_i64(1L);
  size += range_.get_serialize_size();
  // no need to deserialize ext_start_key and ext_end_key
  return size;
}

int ObExtStoreRange::to_collation_free_range_on_demand_and_cutoff_range(common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  ext_start_key_.get_store_rowkey().assign(range_.get_start_key().get_obj_ptr(), range_.get_start_key().get_obj_cnt());
  ext_end_key_.get_store_rowkey().assign(range_.get_end_key().get_obj_ptr(), range_.get_end_key().get_obj_cnt());
  if (OB_FAIL(ext_start_key_.to_collation_free_on_demand_and_cutoff_range(allocator))) {
    STORAGE_LOG(WARN, "fail to get collation free store rowkey of start key and cufoff range", K(ret));
  } else if (OB_FAIL(ext_end_key_.to_collation_free_on_demand_and_cutoff_range(allocator))) {
    STORAGE_LOG(WARN, "fail to get collation free store rowkey of end key and cufoff range", K(ret));
  }

  return ret;
}

void ObExtStoreRange::set_range_array_idx(const int64_t range_array_idx)
{
  ext_start_key_.set_range_array_idx(range_array_idx);
  ext_end_key_.set_range_array_idx(range_array_idx);
}

// for multi version get, the rowkey would be converted into a range (with trans version),
// e.g. rowkey1 -> [(rowkey1, -read_snapshot), (rowkey1, MIN_VERSION))
int ObVersionStoreRangeConversionHelper::store_rowkey_to_multi_version_range(const ObExtStoreRowkey& src_rowkey,
    const ObVersionRange& version_range, ObIAllocator& allocator, ObExtStoreRange& multi_version_range)
{
  int ret = OB_SUCCESS;
  // FIXME: hard coding
  if (OB_UNLIKELY(!version_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "version_range is not valid", K(ret), K(version_range));
  } else if (OB_FAIL(build_multi_version_store_rowkey(src_rowkey.get_store_rowkey(),
                 //-version_range.snapshot_version_,
                 -INT64_MAX,
                 allocator,
                 multi_version_range.get_range().get_start_key()))) {
    COMMON_LOG(WARN, "build multi version store rowkey failed", K(ret), K(src_rowkey), K(version_range));
  } else if (OB_FAIL(build_multi_version_store_rowkey(src_rowkey.get_store_rowkey(),
                 ObVersionRange::MIN_VERSION,
                 allocator,
                 multi_version_range.get_range().get_end_key()))) {
    COMMON_LOG(WARN, "build multi version store rowkey failed", K(ret), K(src_rowkey), K(version_range));
  } else if (OB_FAIL(multi_version_range.to_collation_free_range_on_demand_and_cutoff_range(allocator))) {
    COMMON_LOG(WARN, "fail to get colllation free rowkey and range cutoff", K(ret));
  } else {
    multi_version_range.get_range().set_left_closed();
    multi_version_range.get_range().set_right_open();
  }
  return ret;
}

// for multi version scan, the range would be converted into a range (with trans version),
// e.g. case 1 : (rowkey1, rowkey2) -> ((rowkey1, MIN_VERSION), (rowkey2, -MAX_VERSION))
//      case 2 : [rowkey1, rowkey2] -> [(rowkey1, -read_snapshot), (rowkey2, MIN_VERSION))
int ObVersionStoreRangeConversionHelper::range_to_multi_version_range(const ObExtStoreRange& src_range,
    const ObVersionRange& version_range, ObIAllocator& allocator, ObExtStoreRange& multi_version_range)
{
  int ret = OB_SUCCESS;
  const bool include_start = src_range.get_range().get_border_flag().inclusive_start();
  const bool include_end = src_range.get_range().get_border_flag().inclusive_end();

  // FIXME: hard coding
  if (OB_UNLIKELY(!version_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "version_range is not valid", K(ret), K(version_range));
  } else if (OB_FAIL(build_multi_version_store_rowkey(src_range.get_range().get_start_key(),
                 include_start ? -INT64_MAX : ObVersionRange::MIN_VERSION,
                 allocator,
                 multi_version_range.get_range().get_start_key()))) {
    COMMON_LOG(WARN, "build multi version store rowkey failed", K(ret), K(src_range), K(version_range));
  } else if (OB_FAIL(build_multi_version_store_rowkey(src_range.get_range().get_end_key(),
                 include_end ? ObVersionRange::MIN_VERSION : -ObVersionRange::MAX_VERSION,
                 allocator,
                 multi_version_range.get_range().get_end_key()))) {
    COMMON_LOG(WARN, "build multi version store rowkey failed", K(ret), K(src_range), K(version_range));
  } else if (OB_FAIL(multi_version_range.to_collation_free_range_on_demand_and_cutoff_range(allocator))) {
    COMMON_LOG(WARN, "fail to get collation free rowkey", K(ret));
  } else {
    multi_version_range.get_range().set_table_id(src_range.get_range().get_table_id());
    if (include_start) {
      multi_version_range.get_range().set_left_closed();
    } else {
      multi_version_range.get_range().set_left_open();
    }
    multi_version_range.get_range().set_right_open();
  }
  return ret;
}

int ObVersionStoreRangeConversionHelper::build_multi_version_store_rowkey(const ObStoreRowkey& store_rowkey,
    const int64_t trans_version, ObIAllocator& allocator, ObStoreRowkey& multi_version_store_rowkey)
{
  int ret = OB_SUCCESS;

  if (!store_rowkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "cannot build multi_version key based on an invalid store_rowkey", K(store_rowkey), K(ret));
  } else if (store_rowkey.is_min()) {
    multi_version_store_rowkey.set_min();
  } else if (store_rowkey.is_max()) {
    multi_version_store_rowkey.set_max();
  } else {
    ObObj* cells = NULL;
    // FIXME: hard coding
    const int64_t cell_cnt = store_rowkey.get_obj_cnt() + 1;
    if (OB_ISNULL(cells = (ObObj*)allocator.alloc(sizeof(ObObj) * cell_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "alloc failed.", K(ret));
    } else {
      int64_t i = 0;
      // shallow copy, we reply on the lifetime of store_rowkey is at least as long as
      // multi_version_store_rowkey
      for (; i < store_rowkey.get_obj_cnt(); ++i) {
        cells[i] = store_rowkey.get_obj_ptr()[i];
      }
      // FIXME: hard coding
      cells[i].set_int(trans_version);
      multi_version_store_rowkey.assign(cells, cell_cnt);
    }
  }
  return ret;
}

}  // end namespace common
}  // end namespace oceanbase
