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

#define USING_LOG_PREFIX LIB
#include "ob_roaringbitmap.h"
#include "lib/string/ob_string.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase {
namespace common {

uint64_t ObRoaringBitmap::get_cardinality()
{
  uint64_t cardinality = 0;
  if (is_empty_type()) {
    //do nonting
  } else if (is_single_type()) {
    cardinality = 1;
  } else if (is_set_type()) {
    cardinality = static_cast<uint64_t>(set_.size());
  } else if (is_bitmap_type()) {
    cardinality = roaring::api::roaring64_bitmap_get_cardinality(bitmap_);
  }
  return cardinality;
}

uint64_t ObRoaringBitmap::get_max()
{
  uint64_t max_val = 0;
  if (is_empty_type()) {
    //do nonting
  } else if (is_single_type()) {
    max_val = single_value_;
  } else if (is_set_type()) {
    hash::ObHashSet<uint64_t>::const_iterator iter;
    for (iter = set_.begin(); iter != set_.end(); iter++) {
      max_val = iter->first > max_val? iter->first : max_val;
    }
  } else if (is_bitmap_type()) {
    max_val = roaring::api::roaring64_bitmap_maximum(bitmap_);
  }
  return max_val;
}

bool ObRoaringBitmap::is_contains(uint64_t value)
{
  bool res = false;
  if (is_empty_type()) {
    //do nonting
  } else if (is_single_type()) {
    res = single_value_ == value;
  } else if (is_set_type()) {
    res = set_.exist_refactored(value) == OB_HASH_EXIST ? true : false;
  } else if (is_bitmap_type()) {
    res = roaring::api::roaring64_bitmap_contains(bitmap_, value);
  }
  return res;
}
int ObRoaringBitmap::value_add(uint64_t value) {
  int ret = OB_SUCCESS;
  switch (type_) {
    case ObRbType::EMPTY: {
      set_single(value);
      break;
    }
    case ObRbType::SINGLE: {
      if (single_value_ == value) {
        //do nothing
      } else {
        if (OB_FAIL(set_.create(MAX_BITMAP_SET_VALUES))) {
          LOG_WARN("failed to create set", K(ret));
        } else if (OB_FAIL(set_.set_refactored(single_value_))) {
          LOG_WARN("failed to set value to the set", K(ret), K(single_value_));
        } else if (OB_FAIL(set_.set_refactored(value))) {
          LOG_WARN("failed to set value to the set", K(ret), K(value));
        } else {
          type_ = ObRbType::SET;
        }
      }
      break;
    }
    case ObRbType::SET: {
      if (set_.size() < MAX_BITMAP_SET_VALUES) {
        if (OB_FAIL(set_.set_refactored(value))) {
          LOG_WARN("failed to set value to the set", K(ret), K(value));
        }
      } else if (set_.exist_refactored(value) != OB_HASH_EXIST) { // convert bitmap
        if (OB_FAIL(convert_to_bitmap())) {
          LOG_WARN("failed to convert roaringbitmap to bitmap type", K(ret));
        } else if (OB_FAIL(value_add(value))) {
          LOG_WARN("failed to add value");
        }
      }
      break;
    }
    case ObRbType::BITMAP: {
      roaring::api::roaring64_bitmap_add(bitmap_, value);
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unknown RbType", K(ret), K(type_));
      break;
    }
  } // end switch
  return ret;
}

int ObRoaringBitmap::value_remove(uint64_t value) {
  int ret = OB_SUCCESS;
  switch (type_) {
    case ObRbType::EMPTY: {
      // do nothing
      break;
    }
    case ObRbType::SINGLE: {
      if (single_value_ == value) {
        set_empty();
      }
      break;
    }
    case ObRbType::SET: {
      if (set_.exist_refactored(value) == OB_HASH_EXIST && OB_FAIL(set_.erase_refactored(value))) {
        LOG_WARN("failed to erase value from the set", K(ret), K(value));
      }
      break;
    }
    case ObRbType::BITMAP: {
      roaring::api::roaring64_bitmap_remove(bitmap_, value);
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unknown RbType", K(ret), K(type_));
      break;
    }
  } // end switch
  return ret;
}

int ObRoaringBitmap::value_and(ObRoaringBitmap *rb)
{
  int ret = OB_SUCCESS;
  if (is_empty_type()) {
    //do nothing
  } else if (is_single_type()) {
    if (rb->is_contains(single_value_)) {
      // do nothing
    } else {
      set_empty();
    }
  } else if (is_set_type()) {
    hash::ObHashSet<uint64_t>::const_iterator iter = set_.begin();
    int set_size = set_.size();
    for (int i = 0; OB_SUCC(ret) && i < set_size; i++) {
      if (i != 0) {
        iter++;
      }
      if (!rb->is_contains(iter->first) && OB_FAIL(value_remove(iter->first))) {
        LOG_WARN("failed to remove value", K(ret), K(iter->first));
      }
    }
  } else if (is_bitmap_type()) {
    if (rb->is_empty_type()) {
      set_empty();
    } else if (rb->is_single_type()) {
      if (is_contains(rb->single_value_)) {
        set_single(rb->single_value_);
      } else {
        set_empty();
      }
    } else if (rb->is_set_type()) {
      if (OB_FAIL(set_.create(MAX_BITMAP_SET_VALUES))) {
          LOG_WARN("failed to create set", K(ret));
      } else {
        hash::ObHashSet<uint64_t>::const_iterator iter;
        for (iter = rb->set_.begin(); OB_SUCC(ret) && iter != rb->set_.end(); iter++) {
          if (is_contains(iter->first) && OB_FAIL(set_.set_refactored(iter->first))) {
            LOG_WARN("failed to set_refactored to ObHashSet", K(ret), K(iter->first));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        roaring::api::roaring64_bitmap_free(bitmap_);
        bitmap_ = NULL;
        type_ = ObRbType::SET;
      }
    } else if (rb->is_bitmap_type()) {
      roaring::api::roaring64_bitmap_and_inplace(bitmap_, rb->bitmap_);
    }
  }
  return ret;
}

int ObRoaringBitmap::value_or(ObRoaringBitmap *rb)
{
  int ret = OB_SUCCESS;
  if (rb->is_empty_type()) {
    // do nothing
  } else if (rb->is_single_type()) {
    if (OB_FAIL(value_add(rb->single_value_))) {
      LOG_WARN("failed to add value", K(ret), K(rb->single_value_));
    }
  } else if (rb->is_set_type()) {
    hash::ObHashSet<uint64_t>::const_iterator iter;
    for (iter = rb->set_.begin(); OB_SUCC(ret) && iter != rb->set_.end(); iter++) {
      if (OB_FAIL(value_add(iter->first))) {
        LOG_WARN("failed to add value", K(ret), K(iter->first));
      }
    }
  } else if (rb->is_bitmap_type()) {
    if (is_bitmap_type()) {
      roaring::api::roaring64_bitmap_or_inplace(bitmap_, rb->bitmap_);
    } else if(OB_ISNULL(bitmap_ = roaring::api::roaring64_bitmap_copy(rb->bitmap_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to copy bitmap", K(ret));
    } else if (is_empty_type()) {
      type_ = ObRbType::BITMAP;
    } else if (is_single_type()) {
      roaring::api::roaring64_bitmap_add(bitmap_, single_value_);
      single_value_ = 0;
      type_ = ObRbType::BITMAP;
    } else if (is_set_type()) {
      hash::ObHashSet<uint64_t>::const_iterator iter;
      for (iter = set_.begin(); iter != set_.end(); iter++) {
        roaring::api::roaring64_bitmap_add(bitmap_, iter->first);
      }
      set_.destroy();
      type_ = ObRbType::BITMAP;
    }
  }
  return ret;
}

int ObRoaringBitmap::value_xor(ObRoaringBitmap *rb)
{
  int ret = OB_SUCCESS;
  if (rb->is_empty_type()) {
    // do nothing
  } else if (rb->is_single_type()) {
    if (is_contains(rb->single_value_)) {
      if (OB_FAIL(value_remove(rb->single_value_))) {
        LOG_WARN("failed to remove value", K(ret), K(rb->single_value_));
      }
    } else {
      if (OB_FAIL(value_add(rb->single_value_))) {
        LOG_WARN("failed to add value", K(ret), K(rb->single_value_));
      }
    }
  } else if (rb->is_set_type()) {
    hash::ObHashSet<uint64_t>::const_iterator iter;
    for (iter = rb->set_.begin(); OB_SUCC(ret) && iter != rb->set_.end(); iter++) {
      if (is_contains(iter->first)) {
        if (OB_FAIL(value_remove(iter->first))) {
          LOG_WARN("failed to remove value", K(ret), K(iter->first));
        }
      } else {
        if (OB_FAIL(value_add(iter->first))) {
          LOG_WARN("failed to add value", K(ret), K(iter->first));
        }
      }
    }
  } else if (rb->is_bitmap_type()) {
    if (is_bitmap_type()) {
      roaring::api::roaring64_bitmap_xor_inplace(bitmap_, rb->bitmap_);
    } else if(OB_ISNULL(bitmap_ = roaring::api::roaring64_bitmap_copy(rb->bitmap_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to copy bitmap", K(ret));
    } else if (is_empty_type()) {
      type_ = ObRbType::BITMAP;
    } else if (is_single_type()) {
      if (roaring::api::roaring64_bitmap_contains(bitmap_, single_value_)) {
        roaring::api::roaring64_bitmap_remove(bitmap_, single_value_);
      } else {
        roaring::api::roaring64_bitmap_add(bitmap_, single_value_);
      }
      single_value_ = 0;
      type_ = ObRbType::BITMAP;
    } else if (is_set_type()) {
      hash::ObHashSet<uint64_t>::const_iterator iter;
      for (iter = set_.begin(); iter != set_.end(); iter++) {
        if (roaring::api::roaring64_bitmap_contains(bitmap_, iter->first)) {
          roaring::api::roaring64_bitmap_remove(bitmap_, iter->first);
        } else {
          roaring::api::roaring64_bitmap_add(bitmap_, iter->first);
        }
      }
      set_.destroy();
      type_ = ObRbType::BITMAP;
    }
  }
  return ret;
}

int ObRoaringBitmap::value_andnot(ObRoaringBitmap *rb)
{
  int ret = OB_SUCCESS;
  if (rb->is_empty_type()) {
    // do nothing
  } else if (rb->is_single_type()) {
    if (is_contains(rb->single_value_) && OB_FAIL(value_remove(rb->single_value_))) {
      LOG_WARN("failed to remove value", K(ret), K(rb->single_value_));
    }
  } else if (rb->is_set_type()) {
    hash::ObHashSet<uint64_t>::const_iterator iter;
    for (iter = rb->set_.begin(); OB_SUCC(ret) && iter != rb->set_.end(); iter++) {
      if (is_contains(iter->first) && OB_FAIL(value_remove(iter->first))) {
        LOG_WARN("failed to remove value", K(ret), K(iter->first));
      }
    }
  } else if (rb->is_bitmap_type()) {
    if (is_empty_type()) {
      // do nothing
    } else if (is_single_type()) {
      if (roaring::api::roaring64_bitmap_contains(rb->bitmap_, single_value_)) {
        set_empty();
      }
    } else if (is_set_type()) {
      hash::ObHashSet<uint64_t>::const_iterator iter = set_.begin();
      int set_size = set_.size();
      for (int i = 0; OB_SUCC(ret) && i < set_size; i++) {
        if (i != 0) {
          iter++;
        }
        if (rb->is_contains(iter->first) && OB_FAIL(value_remove(iter->first))) {
          LOG_WARN("failed to remove value", K(ret), K(iter->first));
        }
      }
    } else if (is_bitmap_type()) {
      roaring::api::roaring64_bitmap_andnot_inplace(bitmap_, rb->bitmap_);
    }
  }
  return ret;
}

int ObRoaringBitmap::value_calc(ObRoaringBitmap *rb, ObRbOperation op)
{
  int ret = OB_SUCCESS;
  if (op == ObRbOperation::AND) {
    if (OB_FAIL(value_and(rb))) {
      LOG_WARN("failed to calculate value and", K(ret), K(op));
    }
  } else if (op == ObRbOperation::OR) {
    if (OB_FAIL(value_or(rb))) {
      LOG_WARN("failed to calculate value or", K(ret), K(op));
    }
  } else if (op == ObRbOperation::XOR) {
    if (OB_FAIL(value_xor(rb))) {
      LOG_WARN("failed to calculate value xor", K(ret), K(op));
    }
  } else if (op == ObRbOperation::ANDNOT) {
    if (OB_FAIL(value_andnot(rb))) {
      LOG_WARN("failed to calculate value andnot", K(ret), K(op));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("roaringbitmap operation not supported", K(ret), K(op));
  }
  return ret;
}

int ObRoaringBitmap::optimize()
{
  int ret = OB_SUCCESS;
  if (is_bitmap_type() && OB_FAIL(convert_bitmap_to_smaller_type())) {
    LOG_WARN("failed to convert bitmap to smaller type");
  } else if (is_set_type()){
    uint64_t cardinality = static_cast<uint64_t>(set_.size());
    if (cardinality == 0) {
      set_empty();
    } else if (cardinality == 1) {
      set_single (set_.begin()->first);
    }
  }
  return ret;
}

int ObRoaringBitmap::deserialize(const ObString &rb_bin)
{
  int ret = OB_SUCCESS;
  uint32_t offset = RB_VERSION_SIZE + RB_BIN_TYPE_SIZE;
  version_ = *(rb_bin.ptr());
  ObRbBinType bin_type = static_cast<ObRbBinType>(*(rb_bin.ptr() + RB_VERSION_SIZE));
  switch (bin_type) {
    case ObRbBinType::EMPTY: {
      set_empty();
      break;
    }
    case ObRbBinType::SINGLE_32: {
      uint32_t value_32 = *reinterpret_cast<const uint32_t*>(rb_bin.ptr() + offset);
      set_single(static_cast<uint64_t>(value_32));
      break;
    }
    case ObRbBinType::SINGLE_64: {
      set_single(*reinterpret_cast<const uint64_t*>(rb_bin.ptr() + offset));
      break;
    }
    case ObRbBinType::SET_32: {
      uint32_t value_32 = 0;
      uint8_t value_count = static_cast<uint8_t>(*(rb_bin.ptr() + offset));
      offset += RB_VALUE_COUNT_SIZE;
      for (int i = 0; OB_SUCC(ret) && i < value_count; i++) {
        value_32 = *reinterpret_cast<const uint32_t*>(rb_bin.ptr() + offset);
        offset += sizeof(uint32_t);
        if (OB_FAIL(value_add(static_cast<uint64_t>(value_32)))) {
          LOG_WARN("failed to add value to roaringbtimap", K(ret), K(value_32));
        }
      }
      break;
    }
    case ObRbBinType::SET_64: {
      uint64_t value_64 = 0;
      uint8_t value_count = static_cast<uint8_t>(*(rb_bin.ptr() + offset));
      offset += RB_VALUE_COUNT_SIZE;
      for (int i = 0; OB_SUCC(ret) && i < value_count; i++) {
        value_64 = *reinterpret_cast<const uint64_t*>(rb_bin.ptr() + offset);
        offset += sizeof(uint64_t);
        if (OB_FAIL(value_add(value_64))) {
          LOG_WARN("failed to add value to rb", K(ret), K(value_64));
        }
      }
      break;
    }
    case ObRbBinType::BITMAP_32: {
      uint64_t serial_size = sizeof(uint64_t) + sizeof(uint32_t) + rb_bin.length() - offset;
      uint64_t map_size = 1;
      uint32_t map_prefix = 0;
      ObStringBuffer tmp_buf(allocator_);
      if (OB_FAIL(tmp_buf.append(reinterpret_cast<const char*>(&map_size), sizeof(uint64_t)))) {
        LOG_WARN("failed to append map size", K(ret));
      } else if (OB_FAIL(tmp_buf.append(reinterpret_cast<const char*>(&map_prefix), sizeof(uint32_t)))) {
        LOG_WARN("failed to append map prefix", K(ret));
      } else if (OB_FAIL(tmp_buf.append(rb_bin.ptr() + offset, rb_bin.length() - offset))) {
        LOG_WARN("failed to append serialized string", K(ret), K(rb_bin));
      } else if (OB_ISNULL(bitmap_ = roaring::api::roaring64_bitmap_portable_deserialize_safe(
                                                tmp_buf.ptr(),
                                                tmp_buf.length()))) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("failed to deserialize the bitmap", K(ret));
      } else if (!roaring::api::roaring64_bitmap_internal_validate(bitmap_, NULL)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("bitmap internal consistency checks failed", K(ret));
      } else {
        type_ = ObRbType::BITMAP;
      }
      break;
    }
    case ObRbBinType::BITMAP_64: {
      if (OB_ISNULL(bitmap_ = roaring::api::roaring64_bitmap_portable_deserialize_safe(
                                                rb_bin.ptr() + offset,
                                                rb_bin.length() - offset))) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("failed to deserialize the bitmap", K(ret));
      } else if (!roaring::api::roaring64_bitmap_internal_validate(bitmap_, NULL)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("bitmap internal consistency checks failed", K(ret));
      } else {
        type_ = ObRbType::BITMAP;
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unknown RbBinType", K(ret), K(bin_type));
      break;
    }
  } // end switch
  return ret;
}

int ObRoaringBitmap::serialize(ObStringBuffer &res_buf)
{
  int ret = OB_SUCCESS;
  ObRbBinType bin_type;
  bool is_32bits_enough = (get_max() <= UINT32_MAX);

  if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&version_), RB_VERSION_SIZE))) {
    LOG_WARN("failed to append version", K(ret));
  } else {
    switch (type_) {
      case ObRbType::EMPTY: {
        bin_type = ObRbBinType::EMPTY;
        if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&bin_type), RB_BIN_TYPE_SIZE))) {
          LOG_WARN("failed to append bin_type", K(ret));
        }
        break;
      }
      case ObRbType::SINGLE: {
        if (is_32bits_enough) {
          bin_type = ObRbBinType::SINGLE_32;
          uint32_t single_value_32 = static_cast<uint32_t>(single_value_);
          if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&bin_type), RB_BIN_TYPE_SIZE))) {
            LOG_WARN("failed to append bin_type", K(ret));
          } else if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&single_value_32), sizeof(uint32_t)))) {
            LOG_WARN("failed to append single_value", K(ret));
          }
        } else {
          bin_type = ObRbBinType::SINGLE_64;
          if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&bin_type), RB_BIN_TYPE_SIZE))) {
            LOG_WARN("failed to append bin_type", K(ret));
          } else if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&single_value_), sizeof(uint64_t)))) {
          LOG_WARN("failed to append single_value", K(ret));
          }
        }
        break;
      }
      case ObRbType::SET: {
        int8_t set_size = static_cast<uint8_t>(set_.size());
        if (is_32bits_enough) {
          bin_type = ObRbBinType::SET_32;
          if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&bin_type), RB_BIN_TYPE_SIZE))) {
            LOG_WARN("failed to append bin_type", K(ret));
          } else if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&set_size), RB_VALUE_COUNT_SIZE))) {
            LOG_WARN("failed to append single_value", K(ret));
          } else {
            uint32_t value_32 = 0;
            hash::ObHashSet<uint64_t>::const_iterator iter;
            for (iter = set_.begin(); iter != set_.end(); iter++) {
              value_32 = static_cast<uint32_t>(iter->first);
              if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&value_32), sizeof(uint32_t)))) {
                LOG_WARN("failed to append value", K(ret));
              }
            }
          }
        } else {
          bin_type = ObRbBinType::SET_64;
          if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&bin_type), RB_BIN_TYPE_SIZE))) {
            LOG_WARN("failed to append bin_type", K(ret));
          } else if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&set_size), RB_VALUE_COUNT_SIZE))) {
            LOG_WARN("failed to append single_value", K(ret));
          } else {
            hash::ObHashSet<uint64_t>::const_iterator iter;
            for (iter = set_.begin(); iter != set_.end(); iter++) {
              if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&(iter->first)), sizeof(uint64_t)))) {
                LOG_WARN("failed to append value", K(ret));
              }
            }
          }
        }
        break;
      }
      case ObRbType::BITMAP: {
        ObStringBuffer tmp_buf(allocator_);
        if (roaring::api::roaring64_bitmap_is_empty(bitmap_)) {
          bin_type = ObRbBinType::BITMAP_32;
          uint32_t roaring32_cookie = 12346;
          uint32_t container_num = 0;
          if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&bin_type), RB_BIN_TYPE_SIZE))) {
            LOG_WARN("failed to append bin_type", K(ret));
          } else if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&roaring32_cookie), sizeof(uint32_t)))) {
            LOG_WARN("failed to append roaring32_cookie", K(ret));
          } else if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&container_num), sizeof(uint32_t)))) {
            LOG_WARN("failed to append container_num", K(ret));
          }
        } else {
          uint64_t header_size = RB_VERSION_SIZE + RB_BIN_TYPE_SIZE;
          uint64_t serial_size = static_cast<uint64_t>(roaring::api::roaring64_bitmap_portable_size_in_bytes(bitmap_));
          if (OB_FAIL(tmp_buf.reserve(serial_size))) {
              LOG_WARN("failed to reserve buffer", K(ret), K(serial_size));
          } else if (serial_size != roaring::api::roaring64_bitmap_portable_serialize(bitmap_, tmp_buf.ptr())) {
            ret = OB_SERIALIZE_ERROR;
            LOG_WARN("serialize size not match", K(ret), K(serial_size));
          } else if (OB_FAIL(tmp_buf.set_length(serial_size))) {
            LOG_WARN("failed to set buffer length", K(ret));
          } else if (is_32bits_enough) {
            bin_type = ObRbBinType::BITMAP_32;
            uint64_t roaring64_header_size = sizeof(uint64_t) + sizeof(uint32_t);
            if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&bin_type), RB_BIN_TYPE_SIZE))) {
              LOG_WARN("failed to append bin_type", K(ret));
            } else if (OB_FAIL(res_buf.append(tmp_buf.ptr() + roaring64_header_size, tmp_buf.length() - roaring64_header_size))) {
              LOG_WARN("failed to append serialized string", K(ret), K(tmp_buf));
            }
          } else {
            bin_type = ObRbBinType::BITMAP_64;
            if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&bin_type), RB_BIN_TYPE_SIZE))) {
              LOG_WARN("failed to append bin_type", K(ret));
            } else if (OB_FAIL(res_buf.append(tmp_buf.ptr(), tmp_buf.length()))) {
              LOG_WARN("failed to append serialized string", K(ret), K(tmp_buf));
            }
          }
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unknown RbType", K(ret), K(type_));
        break;
      }
    } //end switch
  }
  return ret;
}

int ObRoaringBitmap::convert_to_bitmap() {
  int ret = OB_SUCCESS;
  if (is_bitmap_type()) {
    // do nothing
  } else if (OB_ISNULL(bitmap_ = roaring::api::roaring64_bitmap_create())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create bitmap", K(ret));
  } else {
    if (is_single_type()) {
      roaring::api::roaring64_bitmap_add(bitmap_, single_value_);
      single_value_ = 0;
    } else if (is_set_type()) {
      hash::ObHashSet<uint64_t>::const_iterator iter;
      for (iter = set_.begin(); iter != set_.end(); iter++) {
        roaring::api::roaring64_bitmap_add(bitmap_, iter->first);
      }
      set_.destroy();
    }
    type_ = ObRbType::BITMAP;
  }
  return ret;
}

int ObRoaringBitmap::convert_bitmap_to_smaller_type() {
  int ret = OB_SUCCESS;
  if (is_bitmap_type()) {
    uint64_t cardinality = roaring::api::roaring64_bitmap_get_cardinality(bitmap_);
    if (cardinality == 0) {
      set_empty();
    } else if (cardinality == 1) {
      set_single(roaring::api::roaring64_bitmap_minimum(bitmap_));
    } else if (cardinality <= MAX_BITMAP_SET_VALUES) {
      roaring::api::roaring64_iterator_t* it = nullptr;
      if (OB_ISNULL(it = roaring::api::roaring64_iterator_create(bitmap_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create bitmap iterator", K(ret));
      } else if (OB_FAIL(set_.create(MAX_BITMAP_SET_VALUES))) {
        LOG_WARN("failed to create set", K(ret));
      } else if (OB_FALSE_IT(type_ = ObRbType::SET)) {
      } else {
        do {
          if (OB_FAIL(set_.set_refactored(roaring::api::roaring64_iterator_value(it)))) {
            LOG_WARN("failed to set value to the set", K(ret), K(roaring::api::roaring64_iterator_value(it)));
          }
        } while (roaring::api::roaring64_iterator_advance(it) && OB_SUCC(ret));
      }
      if (OB_NOT_NULL(it)) {
        roaring::api::roaring64_iterator_free(it);
      }
      if (OB_NOT_NULL(bitmap_)) {
        roaring::api::roaring64_bitmap_free(bitmap_);
        bitmap_ = nullptr;
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase