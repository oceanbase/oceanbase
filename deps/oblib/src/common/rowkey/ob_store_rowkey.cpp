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

namespace oceanbase
{
namespace common
{
ObObj ObStoreRowkey::MIN_OBJECT = ObObj::make_min_obj();
ObObj ObStoreRowkey::MAX_OBJECT = ObObj::make_max_obj();

ObStoreRowkey ObStoreRowkey::MIN_STORE_ROWKEY(&ObStoreRowkey::MIN_OBJECT, 1);
ObStoreRowkey ObStoreRowkey::MAX_STORE_ROWKEY(&ObStoreRowkey::MAX_OBJECT, 1);

//FIXME-yangsuli: this method need to be removed later
//we should NOT allow ObStoreRowkey to be converted to ObRowkey,
//as conceptually it will lose column order info
//now it is used in liboblog,which is OK as liboblog only tests equality
ObRowkey ObStoreRowkey::to_rowkey() const
{
  COMMON_LOG_RET(WARN, OB_SUCCESS, "converting ObStoreRowkey to ObRowkey, potentially dangerous!");
  return key_;
}

void ObStoreRowkey::destroy(ObIAllocator &allocator)
{
  key_.destroy(allocator);
  hash_ = 0;
  group_idx_ = 0;
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
  for(int64_t i = 0; !found && i < key_.get_obj_cnt(); i++) {
    if (key_.get_obj_ptr()[i].is_min_value() || key_.get_obj_ptr()[i].is_max_value()) {
      found = true;
    }
  }
  return found;
}

bool ObStoreRowkey::contains_null_obj() const
{
  bool found = false;
  for(int64_t i = 0; !found && i < key_.get_obj_cnt(); i++) {
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
    group_idx_(0)
{
}

ObExtStoreRowkey::ObExtStoreRowkey(const ObStoreRowkey &store_rowkey)
  : store_rowkey_(store_rowkey),
    collation_free_store_rowkey_(),
    range_cut_pos_(-1),
    first_null_pos_(-1),
    range_check_min_(true),
    group_idx_(0)
{
}

ObExtStoreRowkey::ObExtStoreRowkey(const ObStoreRowkey &store_rowkey,
                                   const ObStoreRowkey &collation_free_store_rowkey)
  : store_rowkey_(store_rowkey),
    collation_free_store_rowkey_(collation_free_store_rowkey),
    range_cut_pos_(-1),
    first_null_pos_(-1),
    range_check_min_(true),
    group_idx_(0)
{
}

int ObExtStoreRowkey::to_collation_free_on_demand_and_cutoff_range(ObIAllocator &allocator)
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

int ObExtStoreRowkey::need_transform_to_collation_free(bool &need_transform) const
{
  int ret = OB_SUCCESS;
  need_transform = false;
  if (OB_FAIL(store_rowkey_.need_transform_to_collation_free(need_transform))) {
    COMMON_LOG(WARN, "fail to get if need to transform to collation free rowkey", K(ret));
  }
  return ret;
}

int ObExtStoreRowkey::to_collation_free_store_rowkey(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(store_rowkey_.to_collation_free_store_rowkey(collation_free_store_rowkey_, allocator))) {
    COMMON_LOG(WARN, "fail to get collation free store rowkey.", K(ret), K(store_rowkey_));
  }
  return ret;
}

int ObExtStoreRowkey::to_collation_free_store_rowkey_on_demand(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(store_rowkey_.to_collation_free_store_rowkey_on_demand(collation_free_store_rowkey_,
                                                                     allocator))) {
    COMMON_LOG(WARN, "fail to get collation free store rowkey.", K(ret), K(store_rowkey_));
  }

  return ret;
}

int ObExtStoreRowkey::check_use_collation_free(const bool exist_invalid_macro_meta_collation_free,
                                               bool &use_collation_free) const
{
  int ret = OB_SUCCESS;
  bool need_transform = false;
  use_collation_free = false;
  if (exist_invalid_macro_meta_collation_free) {
    use_collation_free = false;
  } else {
    if (OB_FAIL(store_rowkey_.need_transform_to_collation_free(need_transform))) {
      COMMON_LOG(WARN,
                 "fail to check if need to transform to collation free store rowkey.", K(ret));
    } else {
      use_collation_free = need_transform && collation_free_store_rowkey_.is_valid();
    }
  }
  return ret;
}

int ObExtStoreRowkey::deserialize(ObIAllocator &allocator, const char *buf,
                                  const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObObj array[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObExtStoreRowkey copy_key;

  if (OB_FAIL(copy_key.store_rowkey_.assign(array, OB_MAX_ROWKEY_COLUMN_NUMBER))) {
    COMMON_LOG(WARN, "Failed to assign store rowkey", K(ret), K(copy_key));
  } else if (OB_FAIL(copy_key.deserialize(buf, data_len, pos))) {
    COMMON_LOG(WARN, "shallow deserialization failed.", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(copy_key.store_rowkey_.deep_copy(store_rowkey_, allocator))) {
    COMMON_LOG(WARN, "deep_copy failed.", KP(buf), K(data_len), K(pos), K(copy_key), K(ret));
  } else if (OB_FAIL(to_collation_free_on_demand_and_cutoff_range(allocator))) {
    COMMON_LOG(WARN, "to collation free and cutoff range failed", KP(buf), K(data_len), K(pos), K(copy_key), K(ret));
  }

  return ret;
}

int ObExtStoreRowkey::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
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

int ObExtStoreRowkey::deep_copy(ObExtStoreRowkey &extkey, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(store_rowkey_.deep_copy(extkey.store_rowkey_, allocator))) {
    COMMON_LOG(WARN, "Fail to deep copy store_rowkey, ", K(ret));
  } else if (OB_FAIL(collation_free_store_rowkey_.deep_copy(
                    extkey.collation_free_store_rowkey_, allocator))) {
    COMMON_LOG(WARN, "Fail to deep copy collation_free store_rowkey, ", K(ret));
  } else {
    extkey.range_cut_pos_ = range_cut_pos_;
    extkey.first_null_pos_ = first_null_pos_;
    extkey.range_check_min_ = range_check_min_;
    extkey.group_idx_ = group_idx_;
  }
  return ret;
}

int ObExtStoreRowkey::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
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
  } // no need to serialize collation_free_store_rowkey, range_cut_pos, as it can be converted later

  return ret;
}

int64_t ObExtStoreRowkey::get_serialize_size(void) const
{
  int64_t size = 0;
  //version
  size += serialization::encoded_length_i64(1L);
  //length
  size += serialization::encoded_length_i64(1L);
  size += store_rowkey_.get_serialize_size();
  // no need to serialize collation_free_store_rowkey, as it can be converted later
  return size;
}

} //end namespace common
} //end namespace oceanbase
