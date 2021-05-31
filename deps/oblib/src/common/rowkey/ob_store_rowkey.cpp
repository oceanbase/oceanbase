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

#include "lib/allocator/ob_malloc.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/hash_func/murmur_hash.h"
#include "common/rowkey/ob_store_rowkey.h"
#include "common/rowkey/ob_rowkey_info.h"

namespace oceanbase {
namespace common {
ObObj ObStoreRowkey::MIN_OBJECT = ObObj::make_min_obj();
ObObj ObStoreRowkey::MAX_OBJECT = ObObj::make_max_obj();

ObStoreRowkey ObStoreRowkey::MIN_STORE_ROWKEY(&ObStoreRowkey::MIN_OBJECT, 1);
ObStoreRowkey ObStoreRowkey::MAX_STORE_ROWKEY(&ObStoreRowkey::MAX_OBJECT, 1);

// FIXME-: this method need to be removed later
// we should NOT allow ObStoreRowkey to be converted to ObRowkey,
// as conceptually it will lose column order info
// now it is used in liboblog,which is OK as liboblog only tests equality
ObRowkey ObStoreRowkey::to_rowkey() const
{
  COMMON_LOG(WARN, "converting ObStoreRowkey to ObRowkey, potentially dangerous!");
  return key_;
}

int ObStoreRowkey::is_min_max(const ObIArrayWrap<ObOrderType>& column_orders, const int64_t rowkey_cnt,
    bool& is_min_max, MinMaxFlag min_or_max) const
{
  int ret = OB_SUCCESS;
  is_min_max = true;

  if (rowkey_cnt <= 0 || column_orders.count() < rowkey_cnt) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "invalid rowkey count or colmn_orders.", K(rowkey_cnt), K(column_orders.count()), K(ret));
  } else if (!is_valid()) {
    // we do not return an error code here, as we allow asking if an invalid key
    // is min or max (it is neither).
    is_min_max = false;
  } else if (key_.get_obj_cnt() != rowkey_cnt) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "rowkey_cnt does not match!", K(key_.get_obj_cnt()), K(rowkey_cnt), K(ret));
  } else {
    int64_t i = 0;
    for (; OB_SUCCESS == ret && is_min_max && i < rowkey_cnt; i++) {
      if (ObOrderType::ASC == column_orders.at(i)) {
        if (MinMaxFlag::MIN == min_or_max && !key_.get_obj_ptr()[i].is_min_value()) {
          is_min_max = false;
        }
        if (MinMaxFlag::MAX == min_or_max && !key_.get_obj_ptr()[i].is_max_value()) {
          is_min_max = false;
        }
      } else if (ObOrderType::DESC == column_orders.at(i)) {
        if (MinMaxFlag::MIN == min_or_max && !key_.get_obj_ptr()[i].is_max_value()) {
          is_min_max = false;
        }
        if (MinMaxFlag::MAX == min_or_max && !key_.get_obj_ptr()[i].is_min_value()) {
          is_min_max = false;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(ERROR, "unknown column order value!", K(i), K(column_orders.at(i)), K(ret));
      }
    }
  }

  return ret;
}

// this implementation replies on that MIN/MAX object does not take extra space
// in addition to sizeof(ObObj); it will break if this assumption does not hold
int ObStoreRowkey::set_min_max(
    const ObIArray<ObOrderType>& column_orders, ObIAllocator& allocator, MinMaxFlag min_or_max)
{
  int ret = OB_SUCCESS;
  ObObj* ptr = NULL;

  if (column_orders.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "invalid rowkey cound or column_orders!", K(column_orders.count()), K(ret));
  } else if (OB_ISNULL(ptr = static_cast<ObObj*>(allocator.alloc(column_orders.count() * sizeof(ObObj))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "allocate mem for obj array failed.", K(column_orders.count()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCCESS == ret && i < column_orders.count(); i++) {
      if (ObOrderType::ASC == column_orders.at(i)) {
        if (MinMaxFlag::MIN == min_or_max) {
          const_cast<ObObj*>(ptr)[i].set_min_value();
        } else {
          const_cast<ObObj*>(ptr)[i].set_max_value();
        }
      } else if (ObOrderType::DESC == column_orders.at(i)) {
        if (MinMaxFlag::MIN == min_or_max) {
          const_cast<ObObj*>(ptr)[i].set_max_value();
        } else {
          const_cast<ObObj*>(ptr)[i].set_min_value();
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(ERROR, "unknown column order value!", K(i), K(column_orders.at(i)), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      assign(ptr, column_orders.count());
    }
  }

  if (OB_FAIL(ret) && NULL != ptr) {
    allocator.free(ptr);
    ptr = NULL;
  }

  return ret;
}

int ObStoreRowkey::compare(const ObStoreRowkey& rhs, const ObRowkeyInfo* rowkey_info, int& cmp) const
{
  int ret = OB_SUCCESS;

  ObSEArray<ObOrderType, OB_MAX_ROWKEY_COLUMN_NUMBER> column_orders;
  if (OB_FAIL(rowkey_info->get_column_orders(column_orders))) {
    COMMON_LOG(WARN, "get column order failed", K(ret));
  } else if (OB_FAIL(compare(rhs, column_orders, cmp))) {
    COMMON_LOG(WARN, "compare with column orders failed", K(ret));
  }

  return ret;
}

int ObStoreRowkey::compare(const ObStoreRowkey& rhs, const ObIArray<ObOrderType>& column_orders, int& cmp) const
{
  int ret = OB_SUCCESS;
  cmp = 0;

  if (column_orders.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "invalid rowkey count or column_orders.", K(column_orders.count()), K(ret));
  } else if (!is_valid() || !rhs.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "trying to compare invalid store_rowkey!", K(*this), K(rhs), K(ret));
  } else if (key_.get_obj_cnt() != column_orders.count() || rhs.key_.get_obj_cnt() != column_orders.count()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(
        ERROR, "rowkey count does not match!", K(key_.get_obj_cnt()), K(rhs.get_obj_cnt()), K(column_orders.count()));
  } else if (key_.get_obj_ptr() == rhs.key_.get_obj_ptr()) {
    cmp = 0;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_orders.count() && 0 == cmp; ++i) {
      if (OB_FAIL(key_.get_obj_ptr()[i].check_collation_free_and_compare(rhs.key_.get_obj_ptr()[i], cmp))) {
        COMMON_LOG(ERROR, "failed to compare", K(ret), K(key_), K(rhs.key_));
      } else if (ObOrderType::DESC == column_orders.at(i)) {
        cmp *= -1;
      } else if (ObOrderType::ASC != column_orders.at(i)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(ERROR, "unknown column order type", K(column_orders.at(i)), K(ret));
      }
    }
  }

  return ret;
}

uint64_t ObStoreRowkey::murmurhash(const uint64_t hash) const
{
  uint64_t hash_ret = hash;
  if (0 == hash_) {
    hash_ = key_.hash();
  }
  if (0 == hash_ret) {
    hash_ret = hash_;
  } else {
    hash_ret = common::murmurhash(&hash_ret, sizeof(int64_t), hash_);
  }
  return hash_ret;
}

bool ObStoreRowkey::contains_min_or_max_obj() const
{
  bool found = false;
  for (int64_t i = 0; !found && i < key_.get_obj_cnt(); i++) {
    if (key_.get_obj_ptr()[i].is_min_value() || key_.get_obj_ptr()[i].is_max_value()) {
      found = true;
    }
  }
  return found;
}

bool ObStoreRowkey::contains_null_obj() const
{
  bool found = false;
  for (int64_t i = 0; !found && i < key_.get_obj_cnt(); i++) {
    if (key_.get_obj_ptr()[i].is_null()) {
      found = true;
    }
  }
  return found;
}

ObExtStoreRowkey::ObExtStoreRowkey()
    : store_rowkey_(),
      collation_free_store_rowkey_(),
      range_cut_pos_(-1),
      first_null_pos_(-1),
      range_check_min_(true),
      range_array_idx_(0)
{}

ObExtStoreRowkey::ObExtStoreRowkey(const ObStoreRowkey& store_rowkey)
    : store_rowkey_(store_rowkey),
      collation_free_store_rowkey_(),
      range_cut_pos_(-1),
      first_null_pos_(-1),
      range_check_min_(true),
      range_array_idx_(0)
{}

ObExtStoreRowkey::ObExtStoreRowkey(const ObStoreRowkey& store_rowkey, const ObStoreRowkey& collation_free_store_rowkey)
    : store_rowkey_(store_rowkey),
      collation_free_store_rowkey_(collation_free_store_rowkey),
      range_cut_pos_(-1),
      first_null_pos_(-1),
      range_check_min_(true),
      range_array_idx_(0)
{}

int ObExtStoreRowkey::to_collation_free_on_demand_and_cutoff_range(ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  reset_collation_free_and_range();
  if (OB_FAIL(to_collation_free_store_rowkey_on_demand(allocator))) {
    COMMON_LOG(WARN, "Failed to transform collation free key", K(ret));
  } else if (OB_FAIL(get_possible_range_pos())) {
    COMMON_LOG(WARN, "Failed to get possible range pos", K(ret));
  }
  return ret;
}

int ObExtStoreRowkey::get_possible_range_pos()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!store_rowkey_.is_valid())) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObExtStoreRowkey is not init", K(ret));
  } else {
    for (int64_t i = 0; range_cut_pos_ < 0 && i < store_rowkey_.get_obj_cnt(); i++) {
      if (store_rowkey_.get_obj_ptr()[i].is_min_value()) {
        range_cut_pos_ = i;
        range_check_min_ = true;
      } else if (store_rowkey_.get_obj_ptr()[i].is_max_value()) {
        range_cut_pos_ = i;
        range_check_min_ = false;
      } else if (store_rowkey_.get_obj_ptr()[i].is_null()) {
        if (first_null_pos_ < 0) {
          first_null_pos_ = i;
        }
      }
    }
    if (range_cut_pos_ < 0) {
      range_cut_pos_ = store_rowkey_.get_obj_cnt();
      range_check_min_ = true;
    }
  }

  return ret;
}

int ObExtStoreRowkey::need_transform_to_collation_free(bool& need_transform) const
{
  int ret = OB_SUCCESS;
  need_transform = false;
  if (OB_FAIL(store_rowkey_.need_transform_to_collation_free(need_transform))) {
    COMMON_LOG(WARN, "fail to get if need to transform to collation free rowkey", K(ret));
  }
  return ret;
}

int ObExtStoreRowkey::to_collation_free_store_rowkey(ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(store_rowkey_.to_collation_free_store_rowkey(collation_free_store_rowkey_, allocator))) {
    COMMON_LOG(WARN, "fail to get collation free store rowkey.", K(ret), K(store_rowkey_));
  }
  return ret;
}

int ObExtStoreRowkey::to_collation_free_store_rowkey_on_demand(ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(store_rowkey_.to_collation_free_store_rowkey_on_demand(collation_free_store_rowkey_, allocator))) {
    COMMON_LOG(WARN, "fail to get collation free store rowkey.", K(ret), K(store_rowkey_));
  }

  return ret;
}

int ObExtStoreRowkey::check_use_collation_free(
    const bool exist_invalid_macro_meta_collation_free, bool& use_collation_free) const
{
  int ret = OB_SUCCESS;
  bool need_transform = false;
  use_collation_free = false;
  if (exist_invalid_macro_meta_collation_free) {
    use_collation_free = false;
  } else {
    if (OB_FAIL(store_rowkey_.need_transform_to_collation_free(need_transform))) {
      COMMON_LOG(WARN, "fail to check if need to transform to collation free store rowkey.", K(ret));
    } else {
      use_collation_free = need_transform && collation_free_store_rowkey_.is_valid();
    }
  }
  return ret;
}

int ObExtStoreRowkey::deserialize(ObIAllocator& allocator, const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  ObObj array[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObExtStoreRowkey copy_key;
  copy_key.store_rowkey_.assign(array, OB_MAX_ROWKEY_COLUMN_NUMBER);

  if (OB_FAIL(copy_key.deserialize(buf, data_len, pos))) {
    COMMON_LOG(WARN, "shallow deserialization failed.", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(copy_key.store_rowkey_.deep_copy(store_rowkey_, allocator))) {
    COMMON_LOG(WARN, "deep_copy failed.", KP(buf), K(data_len), K(pos), K(copy_key), K(ret));
  } else if (OB_FAIL(to_collation_free_on_demand_and_cutoff_range(allocator))) {
    COMMON_LOG(WARN, "to collation free and cutoff range failed", KP(buf), K(data_len), K(pos), K(copy_key), K(ret));
  }

  return ret;
}

int ObExtStoreRowkey::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t length = 0;
  int64_t cur_pos = 0;
  if (OB_FAIL(serialization::decode_i64(buf, data_len, cur_pos, &version))) {
    COMMON_LOG(WARN, "Fail to deserialize version, ", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, cur_pos, &length))) {
    COMMON_LOG(WARN, "Fail to deserialize length, ", K(ret));
  } else if (OB_FAIL(store_rowkey_.deserialize(buf, data_len, cur_pos))) {
    COMMON_LOG(WARN, "Fail to deserialize store_rowkey, ", K(ret));
  } else {
    pos += length;
  }
  return ret;
}

int ObExtStoreRowkey::deep_copy(ObExtStoreRowkey& extkey, ObIAllocator& allocator) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(store_rowkey_.deep_copy(extkey.store_rowkey_, allocator))) {
    COMMON_LOG(WARN, "Fail to deep copy store_rowkey, ", K(ret));
  } else if (OB_FAIL(collation_free_store_rowkey_.deep_copy(extkey.collation_free_store_rowkey_, allocator))) {
    COMMON_LOG(WARN, "Fail to deep copy collation_free store_rowkey, ", K(ret));
  } else {
    extkey.range_cut_pos_ = range_cut_pos_;
    extkey.first_null_pos_ = first_null_pos_;
    extkey.range_check_min_ = range_check_min_;
    extkey.range_array_idx_ = range_array_idx_;
  }
  return ret;
}

int ObExtStoreRowkey::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t length = get_serialize_size();

  if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, version))) {
    COMMON_LOG(WARN, "Fail to serialize version, ", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, length))) {
    COMMON_LOG(WARN, "Fail to serialize length, ", K(ret));
  } else if (OB_FAIL(store_rowkey_.serialize(buf, buf_len, pos))) {
    COMMON_LOG(WARN, "Fail to serialize store_rowkey, ", K(ret));
  }  // no need to serialize collation_free_store_rowkey, range_cut_pos, as it can be converted later

  return ret;
}

int64_t ObExtStoreRowkey::get_serialize_size(void) const
{
  int64_t size = 0;
  // version
  size += serialization::encoded_length_i64(1L);
  // length
  size += serialization::encoded_length_i64(1L);
  size += store_rowkey_.get_serialize_size();
  // no need to serialize collation_free_store_rowkey, as it can be converted later
  return size;
}

}  // end namespace common
}  // end namespace oceanbase
