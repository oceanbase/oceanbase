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

namespace oceanbase
{
namespace common
{
int64_t ObStoreRange::to_plain_string(char *buffer, const int64_t length) const
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

int ObStoreRange::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos,
      static_cast<int64_t>(table_id_)))) {
    COMMON_LOG(WARN, "serialize table_id failed", KP(buf), K(buf_len), K(table_id_), K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, border_flag_.get_data()))) {
    COMMON_LOG(WARN, "serialize border_falg failed",
        KP(buf), K(buf_len), K(pos), K(border_flag_), K(ret));
  } else if (OB_FAIL(start_key_.serialize(buf, buf_len, pos))) {
    COMMON_LOG(WARN, "serialize start_key failed",
        KP(buf), K(buf_len), K(pos), K(start_key_), K(ret));
  } else if (OB_FAIL(end_key_.serialize(buf, buf_len, pos))) {
    COMMON_LOG(WARN, "serialize end_key failed",
        KP(buf), K(buf_len), K(pos), K(end_key_), K(ret));
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

int ObStoreRange::deserialize(ObIAllocator &allocator, const char *buf, const int64_t data_len,
                              int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObObj array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
  ObStoreRange copy_range;
  if (OB_FAIL(copy_range.start_key_.assign(array, OB_MAX_ROWKEY_COLUMN_NUMBER))) {
    COMMON_LOG(WARN, "Failed to assign start key", K(ret));
  } else if (OB_FAIL(copy_range.end_key_.assign(array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER))) {
    COMMON_LOG(WARN, "Failed to assign end key", K(ret));
  } else if (OB_FAIL(copy_range.deserialize(buf, data_len, pos))) {
    COMMON_LOG(WARN, "deserialize range to shallow copy range failed.",
              KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(copy_range.deep_copy(allocator, *this))) {
    COMMON_LOG(WARN, "deep_copy range failed.",
               KP(buf), K(data_len), K(pos), K(copy_range), K(ret));
  }

  return ret;
}

int ObStoreRange::deserialize(const char *buf, const int64_t data_len,
                              int64_t &pos)
{
  int ret = OB_SUCCESS;
  int8_t flag = 0;

  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos,
                                                reinterpret_cast<int64_t *>(&table_id_)))) {
    COMMON_LOG(WARN, "deserialize table_id failed.",
               KP(buf), K(data_len), K(pos), K(table_id_), K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, &flag))) {
    COMMON_LOG(WARN, "deserialize flag failed.", KP(buf), K(data_len), K(pos), K(flag), K(ret));
  } else if (OB_FAIL(start_key_.deserialize(buf, data_len, pos))) {
    COMMON_LOG(WARN, "deserialize start_key failed.",
               KP(buf), K(data_len), K(pos), K(start_key_), K(ret));
  } else if (OB_FAIL(end_key_.deserialize(buf, data_len, pos))) {
    COMMON_LOG(WARN, "deserialize end_key failed.",
               KP(buf), K(data_len), K(pos), K(end_key_), K(ret));
  } else {
    border_flag_.set_data(flag);
  }
  return ret;
}

int ObStoreRange::deep_copy(ObIAllocator &allocator, ObStoreRange &dst) const
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

void ObStoreRange::change_boundary(const ObStoreRowkey &gap_key, bool is_reverse, bool exclusive)
{
  if (is_reverse) {
    set_end_key(gap_key);
    if (exclusive) {
      set_right_open();
    } else {
      set_right_closed();
    }
  }  else {
    set_start_key(gap_key);
    if (exclusive) {
      set_left_open();
    } else {
      set_left_closed();
    }
  }
}

// for multi version get, the rowkey would be converted into a range (with trans version),
// e.g. rowkey1 -> [(rowkey1, -read_snapshot), (rowkey1, MAX_VERSION)]
int ObVersionStoreRangeConversionHelper::store_rowkey_to_multi_version_range(
    const ObStoreRowkey &src_rowkey,
    const ObVersionRange &version_range,
    ObIAllocator &allocator,
    ObStoreRange &multi_version_range)
{
  int ret = OB_SUCCESS;
  // FIXME: hard coding
  if (OB_UNLIKELY(!version_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "version_range is not valid", K(ret), K(version_range));
  } else if (OB_FAIL(build_multi_version_store_rowkey(src_rowkey,
                                               //-version_range.snapshot_version_,
                                               true, // min_value
                                               allocator,
                                               multi_version_range.get_start_key()))) {
    COMMON_LOG(WARN, "build multi version store rowkey failed",
               K(ret), K(src_rowkey), K(version_range));
  } else if (OB_FAIL(build_multi_version_store_rowkey(src_rowkey,
                                                false, // min_value
                                                allocator,
                                                multi_version_range.get_end_key()))) {
    COMMON_LOG(WARN, "build multi version store rowkey failed",
                  K(ret), K(src_rowkey), K(version_range));
  } else {
    multi_version_range.set_left_closed();
    multi_version_range.set_right_closed();
  }
  return ret;
}


// for multi version scan, the range would be converted into a range (with trans version),
// e.g. case 1 : (rowkey1, rowkey2) -> ((rowkey1, MAX_VERSION), (rowkey2, -MAX_VERSION))
//      case 2 : [rowkey1, rowkey2] -> [(rowkey1, -MAX_VERSION), (rowkey2, MAX_VERSION)]
int ObVersionStoreRangeConversionHelper::range_to_multi_version_range(
    const ObStoreRange &src_range,
    const ObVersionRange &version_range,
    ObIAllocator &allocator,
    ObStoreRange &multi_version_range)
{
  int ret = OB_SUCCESS;
  const bool include_start = src_range.get_border_flag().inclusive_start();
  const bool include_end = src_range.get_border_flag().inclusive_end();

  // FIXME: hard coding
  if (OB_UNLIKELY(!version_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "version_range is not valid", K(ret), K(version_range));
  } else if (OB_FAIL(build_multi_version_store_rowkey(src_range.get_start_key(),
      include_start, allocator, multi_version_range.get_start_key()))) {
    COMMON_LOG(WARN, "build multi version store rowkey failed",
              K(ret), K(src_range), K(version_range));
  } else if (OB_FAIL(build_multi_version_store_rowkey(src_range.get_end_key(),
      !include_end, allocator, multi_version_range.get_end_key()))) {
    COMMON_LOG(WARN, "build multi version store rowkey failed",
               K(ret), K(src_range), K(version_range));
  } else {
    multi_version_range.set_table_id(src_range.get_table_id());
    if (include_start) {
      multi_version_range.set_left_closed();
    } else {
      multi_version_range.set_left_open();
    }
    if (include_end) {
      multi_version_range.set_right_closed();
    } else {
      multi_version_range.set_right_open();
    }
  }
  return ret;
}

int ObVersionStoreRangeConversionHelper::build_multi_version_store_rowkey(
        const ObStoreRowkey &store_rowkey,
        const bool min_value,
        ObIAllocator &allocator,
        ObStoreRowkey &multi_version_store_rowkey)
{
  int ret = OB_SUCCESS;

  if(!store_rowkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "cannot build multi_version key based on an invalid store_rowkey",
               K(store_rowkey), K(ret));
  } else if (store_rowkey.is_min()) {
    multi_version_store_rowkey.set_min();
  } else if (store_rowkey.is_max()) {
    multi_version_store_rowkey.set_max();
  } else {
    ObObj *cells = NULL;
    // FIXME: hard coding
    const int64_t cell_cnt = store_rowkey.get_obj_cnt() + 1;
    if (OB_ISNULL(cells = (ObObj*) allocator.alloc(sizeof(ObObj) * cell_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "Failed to alloc memory for multi version store rowkey", K(ret), K(cell_cnt));
    } else {
      int64_t i = 0;
      // shallow copy, we reply on the lifetime of store_rowkey is at least as long as
      // multi_version_store_rowkey
      for ( ; i < store_rowkey.get_obj_cnt(); ++ i) {
        cells[i] = store_rowkey.get_obj_ptr()[i];
      }
      // FIXME: hard coding
      if (min_value) {
        cells[i].set_min_value();
      } else {
        cells[i].set_max_value();
      }
      if (OB_FAIL(multi_version_store_rowkey.assign(cells, cell_cnt))) {
        COMMON_LOG(WARN, "Failed to assign multi version store rowkey", K(ret), KP(cells), K(cell_cnt));
      }
    }
  }
  return ret;
}

} //end namespace common
} //end namespace oceanbase
