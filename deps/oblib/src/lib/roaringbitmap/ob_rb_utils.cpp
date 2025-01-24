
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
 * This file contains implementation support for the roaringbitmap utils abstraction.
 */

#define USING_LOG_PREFIX LIB
#include "lib/roaringbitmap/ob_rb_utils.h"
#include "roaring/roaring_array.h"

namespace oceanbase
{
namespace common
{

const uint64_t max_rb_to_string_cardinality = 1000000;

int ObRbUtils::get_bin_type(const ObString &rb_bin, ObRbBinType &bin_type)
{
  int ret = OB_SUCCESS;
  // get_bin_type
  if (rb_bin == nullptr || rb_bin.empty()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("roaringbitmap binary is empty", K(ret), K(rb_bin));
  } else if (rb_bin.length() < RB_VERSION_SIZE + RB_TYPE_SIZE) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid roaringbitmap binary length", K(ret), K(rb_bin.length()));
  } else {
    bin_type = static_cast<ObRbBinType>(*(rb_bin.ptr() + RB_VERSION_SIZE));
  }
  return  ret;
}

int ObRbUtils::check_binary(const ObString &rb_bin)
{
  int ret = OB_SUCCESS;
  ObRbBinType bin_type;
  uint32_t offset = RB_VERSION_SIZE + RB_TYPE_SIZE;
  // get_bin_type
  if (rb_bin == nullptr || rb_bin.empty()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("roaringbitmap binary is empty", K(ret), K(rb_bin));
  } else if (rb_bin.length() < RB_VERSION_SIZE + RB_TYPE_SIZE) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid roaringbitmap binary length", K(ret), K(rb_bin.length()));
  } else if (!IS_VALID_RB_VERSION(static_cast<uint8_t>(*(rb_bin.ptr())))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid version from roaringbitmap binary", K(ret), K(*(rb_bin.ptr())));
  } else if (*(rb_bin.ptr() + RB_VERSION_SIZE) >= static_cast<uint8_t>(ObRbBinType::MAX_TYPE)
             || *(rb_bin.ptr() + RB_VERSION_SIZE) < 0) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid binary type from roaringbitmap binary", K(ret), K(*(rb_bin.ptr() + RB_VERSION_SIZE)));
  } else {
    bin_type = static_cast<ObRbBinType>(*(rb_bin.ptr() + RB_VERSION_SIZE));
    // check binary
    switch (bin_type) {
      case ObRbBinType::EMPTY: {
        if (rb_bin.length() != offset) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid roaringbitmap binary length", K(ret), K(bin_type), K(rb_bin.length()));
        }
        break;
      }
      case ObRbBinType::SINGLE_32: {
        if (rb_bin.length() != offset + sizeof(uint32_t)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid roaringbitmap binary length", K(ret), K(bin_type), K(rb_bin.length()));
        }
        break;
      }
      case ObRbBinType::SINGLE_64: {
        if (rb_bin.length() != offset + sizeof(uint64_t)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid roaringbitmap binary length", K(ret), K(bin_type), K(rb_bin.length()));
        }
        break;
      }
      case ObRbBinType::SET_32: {
        uint8_t value_count = 0;
        if (rb_bin.length() < offset + RB_VALUE_COUNT_SIZE) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid roaringbitmap binary length", K(ret), K(rb_bin.length()), K(bin_type));
        } else if (OB_FALSE_IT(value_count = *(rb_bin.ptr() + offset))) {
        } else if (OB_FALSE_IT(offset += RB_VALUE_COUNT_SIZE)) {
        } else if (value_count < 2 || value_count > MAX_BITMAP_SET_VALUES) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid roaringbitmap value_count", K(ret), K(bin_type), K(value_count));
        } else if (rb_bin.length() != offset + value_count * sizeof(uint32_t)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid roaringbitmap binary length", K(ret), K(bin_type), K(value_count), K(rb_bin.length()));
        }
        break;
      }
      case ObRbBinType::SET_64: {
        uint8_t value_count = 0;
        if (rb_bin.length() < offset + RB_VALUE_COUNT_SIZE) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid roaringbitmap binary length", K(ret), K(rb_bin.length()), K(bin_type));
        } else if (OB_FALSE_IT(value_count = *(rb_bin.ptr() + offset))) {
        } else if (OB_FALSE_IT(offset += RB_VALUE_COUNT_SIZE)) {
        } else if (value_count < 2 || value_count > MAX_BITMAP_SET_VALUES) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid roaringbitmap value_count", K(ret), K(bin_type), K(value_count));
        } else if (rb_bin.length() != offset + value_count * sizeof(uint64_t)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid roaringbitmap binary length", K(ret), K(bin_type), K(value_count), K(rb_bin.length()));
        }
        break;
      }
      case ObRbBinType::BITMAP_32: {
        size_t deserialize_size = roaring::api::roaring_bitmap_portable_deserialize_size(rb_bin.ptr() + offset, rb_bin.length() - offset);
        if (deserialize_size == 0 || deserialize_size != rb_bin.length() - offset) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid roaringbitmap binary length", K(ret), K(bin_type), K(deserialize_size), K(rb_bin.length()));
        }
        break;
      }
      case ObRbBinType::BITMAP_64: {
        size_t deserialize_size = roaring::api::roaring64_bitmap_portable_deserialize_size(rb_bin.ptr() + offset, rb_bin.length() - offset);
        if (deserialize_size == 0 || deserialize_size != rb_bin.length() - offset) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid roaringbitmap binary length", K(ret), K(bin_type), K(deserialize_size), K(rb_bin.length()));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unknown RbType", K(ret), K(bin_type));
        break;
      }
    } // end switch
  }
  return ret;
}

int ObRbUtils::build_empty_binary(ObIAllocator &allocator, ObString &res_rb_bin)
{
  int ret = OB_SUCCESS;
  uint8_t version = BITMAP_VESION_1;
  ObRbBinType bin_type = ObRbBinType::EMPTY;
  ObStringBuffer res_buf(&allocator);
  if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&version), RB_VERSION_SIZE))) {
    LOG_WARN("failed to append version", K(ret));
  } else if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&bin_type), RB_BIN_TYPE_SIZE))) {
    LOG_WARN("failed to append bin_type", K(ret));
  } else {
    res_rb_bin.assign_ptr(res_buf.ptr(), res_buf.length());
  }
  return ret;
}

int ObRbUtils::to_roaring64_bin(ObIAllocator &allocator, ObRbBinType rb_type, ObString &rb_bin, ObString &roaring64_bin)
{
  int ret = OB_SUCCESS;
  uint32_t offset = RB_VERSION_SIZE + RB_BIN_TYPE_SIZE;
  if (rb_type == ObRbBinType::BITMAP_64) {
    roaring64_bin.assign_ptr(rb_bin.ptr() + offset, rb_bin.length() - offset);
  } else if (rb_type == ObRbBinType::BITMAP_32) {
    uint64_t map_size = 1;
    uint32_t high32 = 0;
    ObStringBuffer bin_buf(&allocator);
    if (OB_FAIL(bin_buf.append(reinterpret_cast<const char*>(&map_size), sizeof(uint64_t)))) {
      LOG_WARN("failed to append map size", K(ret));
    } else if (OB_FAIL(bin_buf.append(reinterpret_cast<const char*>(&high32), sizeof(uint32_t)))) {
      LOG_WARN("failed to append map prefix", K(ret));
    } else if (OB_FAIL(bin_buf.append(rb_bin.ptr() + offset, rb_bin.length() - offset))) {
      LOG_WARN("failed to append serialized string", K(ret), K(rb_bin));
    } else {
      roaring64_bin.assign_ptr(bin_buf.ptr(), bin_buf.length());
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ObRbBinType", K(ret), K(rb_type));
  }
  return ret;
}

int ObRbUtils::get_cardinality(ObIAllocator &allocator, const ObString &rb_bin, uint64_t &cardinality)
{
  int ret = OB_SUCCESS;
  cardinality = 0;
  ObRbBinType bin_type;
  uint32_t offset = RB_VERSION_SIZE + RB_BIN_TYPE_SIZE;
  if (OB_FAIL(get_bin_type(rb_bin, bin_type))) {
    LOG_WARN("failed to get binary type", K(ret));
  } else if (bin_type == ObRbBinType::EMPTY) {
    // do nothing
  } else if (bin_type == ObRbBinType::SINGLE_32 || bin_type == ObRbBinType::SINGLE_64) {
    cardinality = 1;
  } else if (bin_type == ObRbBinType::SET_32 || bin_type == ObRbBinType::SET_64) {
    uint8_t value_count = static_cast<uint8_t>(*(rb_bin.ptr() + offset));
    cardinality =  static_cast<uint64_t>(value_count);
  } else if (bin_type == ObRbBinType::BITMAP_32) {
    ObString binary_str;
    binary_str.assign_ptr(rb_bin.ptr() + offset, rb_bin.length() - offset);
    ObRoaringBin *roaring_bin = NULL;
    if (OB_ISNULL(roaring_bin = OB_NEWx(ObRoaringBin, &allocator, &allocator, binary_str))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for ObRoaringBin", K(ret));
    } else if (OB_FAIL(roaring_bin->init())) {
      LOG_WARN("failed to init ObRoaringBin", K(ret), K(binary_str));
    } else if (OB_FAIL(roaring_bin->get_cardinality(cardinality))) {
      LOG_WARN("failed to get roaring card", K(ret), K(binary_str));
    }
  } else if (bin_type == ObRbBinType::BITMAP_64) {
    ObString binary_str;
    binary_str.assign_ptr(rb_bin.ptr() + offset, rb_bin.length() - offset);
    ObRoaring64Bin *roaring64_bin = NULL;
    if (OB_ISNULL(roaring64_bin = OB_NEWx(ObRoaring64Bin, &allocator, &allocator, binary_str))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for ObRoaring64Bin", K(ret));
    } else if (OB_FAIL(roaring64_bin->init())) {
      LOG_WARN("failed to init ObRoaring64Bin", K(ret), K(binary_str));
    } else if (OB_FAIL(roaring64_bin->get_cardinality(cardinality))) {
      LOG_WARN("failed to get roaring card", K(ret), K(binary_str));
    }
  }
  return ret;
}

int ObRbUtils::get_calc_cardinality(ObIAllocator &allocator, ObString &rb1_bin, ObString &rb2_bin, uint64_t &cardinality, ObRbOperation op)
{
  int ret = OB_SUCCESS;
  ObRbBinType rb1_type;
  ObRbBinType rb2_type;
  uint64_t rb1_card = 0;
  uint64_t rb2_card = 0;
  uint64_t and_card = 0;
  if (OB_FAIL(get_bin_type(rb1_bin, rb1_type))) {
    LOG_WARN("invalid left roaringbitmap binary string", K(ret));
  } else if (OB_FAIL(get_bin_type(rb2_bin, rb2_type))) {
    LOG_WARN("invalid right roaringbitmap binary string", K(ret));
  } else if (op == ObRbOperation::AND) {
    rb1_card = 1; // no need to calculate rb1 cardinality
    rb2_card = 1; // no need to calculate rb2 cardinality
    if (OB_FAIL(get_and_cardinality(allocator, rb1_bin, rb1_type, rb2_bin, rb2_type, and_card, rb1_card, rb2_card))) {
      LOG_WARN("failed to calculate and cardinality", K(ret));
    } else {
      cardinality = and_card;
    }
  } else if (op == ObRbOperation::OR) {
    if (OB_FAIL(get_and_cardinality(allocator, rb1_bin, rb1_type, rb2_bin, rb2_type, and_card, rb1_card, rb2_card))) {
      LOG_WARN("failed to calculate and cardinality", K(ret));
    } else {
      cardinality = rb1_card + rb2_card - and_card;
    }
  } else if (op == ObRbOperation::XOR) {
    if (OB_FAIL(get_and_cardinality(allocator, rb1_bin, rb1_type, rb2_bin, rb2_type, and_card, rb1_card, rb2_card))) {
      LOG_WARN("failed to calculate and cardinality", K(ret));
    } else {
      cardinality = rb1_card + rb2_card - 2 * and_card;
    }
  } else if (op == ObRbOperation::ANDNOT) {
    rb2_card = 1; // no need to calculate rb2 cardinality
    if (OB_FAIL(get_and_cardinality(allocator, rb1_bin, rb1_type, rb2_bin, rb2_type, and_card, rb1_card, rb2_card))) {
      LOG_WARN("failed to calculate and cardinality", K(ret));
    } else {
      cardinality = rb1_card - and_card;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid RbOperation", K(ret), K(op));
  }
  return ret;
}

int ObRbUtils::get_and_cardinality(ObIAllocator &allocator,
                                   ObString &rb1_bin,
                                   ObRbBinType rb1_type,
                                   ObString &rb2_bin,
                                   ObRbBinType rb2_type,
                                   uint64_t &cardinality,
                                   uint64_t &rb1_card,
                                   uint64_t &rb2_card)
{
  int ret = OB_SUCCESS;
  if (!is_bitmap_bin(rb1_type) && !is_bitmap_bin(rb2_type)) {
    // do deserivalize for both roaringbitmap
    ObRoaringBitmap *rb1 = nullptr;
    ObRoaringBitmap *rb2 = nullptr;
    if (OB_FAIL(rb_deserialize(allocator, rb1_bin, rb1))) {
      LOG_WARN("failed to deserialize left roaringbitmap", K(ret));
    } else if (OB_FAIL(rb_deserialize(allocator, rb2_bin, rb2))) {
      LOG_WARN("failed to deserialize right roaringbitmap", K(ret));
    } else if (rb2->is_empty_type()) {
      cardinality = 0;
    } else if (rb2->is_single_type()) {
      if (rb1->is_contains(rb2->get_single_value())) {
        cardinality = 1;
      }
    } else if (rb2->is_set_type()) {
      hash::ObHashSet<uint64_t> *set = rb2->get_set();
      hash::ObHashSet<uint64_t>::const_iterator iter;
      cardinality = 0;
      for (iter = set->begin(); iter != set->end(); iter++) {
        if (rb1->is_contains(iter->first)) {
          cardinality += 1;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (rb1_card == 0 && OB_FALSE_IT(rb1_card = rb1->get_cardinality()) ) {
    } else if (rb2_card == 0 && OB_FALSE_IT(rb2_card = rb2->get_cardinality())) {
    }
    rb_destroy(rb1);
    rb_destroy(rb2);
  } else if (is_bitmap_bin(rb1_type) && !is_bitmap_bin(rb2_type)) {
    // do deserivalize for only right roaringbitmap
    ObString rb1_roaring64_bin;
    ObRoaring64Bin *rb1 = nullptr;
    ObRoaringBitmap *rb2 = nullptr;
    if (OB_FAIL(to_roaring64_bin(allocator, rb1_type, rb1_bin, rb1_roaring64_bin))) {
      LOG_WARN("failed to get roaring64 binary string from left roaringbitmap", K(ret), K(rb1_type));
    } else if (OB_ISNULL(rb1 = OB_NEWx(ObRoaring64Bin, &allocator, &allocator, rb1_roaring64_bin))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for left ObRoaring64Bin", K(ret));
    } else if (OB_FAIL(rb1->init())) {
      LOG_WARN("failed to init left ObRoaring64Bin", K(ret));
    } else if (OB_FAIL(rb_deserialize(allocator, rb2_bin, rb2))) {
      LOG_WARN("failed to deserialize right roaringbitmap", K(ret));
    } else if (rb2->is_empty_type()) {
      cardinality = 0;
    } else if (rb2->is_single_type()) {
      bool is_contains = false;
      if (OB_FAIL(rb1->contains(rb2->get_single_value(), is_contains))) {
        LOG_WARN("failed to check value is_contains", K(ret), K(rb2->get_single_value()));
      } else {
        cardinality = is_contains? 1 : 0;
      }
    } else if (rb2->is_set_type()) {
      cardinality = 0;
      hash::ObHashSet<uint64_t> *set = rb2->get_set();
      hash::ObHashSet<uint64_t>::const_iterator iter;
      for (iter = set->begin(); iter != set->end(); iter++) {
        bool is_contains = false;
        if (OB_FAIL(rb1->contains(iter->first, is_contains))) {
          LOG_WARN("failed to check value is_contains", K(ret), K(iter->first));
        } else if (is_contains) {
          cardinality += 1;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (rb1_card == 0 && OB_FAIL(rb1->get_cardinality(rb1_card))) {
      LOG_WARN("failed to get cardinality from left roaringbitmap", K(ret));
    } else if (rb2_card == 0 && OB_FALSE_IT(rb2_card = rb2->get_cardinality())) {
    }
    rb_destroy(rb2);
  } else if (!is_bitmap_bin(rb1_type) && is_bitmap_bin(rb2_type)) {
    // switch position of rb1 and rb2
    if (OB_FAIL(get_and_cardinality(allocator, rb2_bin, rb2_type, rb1_bin, rb1_type, cardinality, rb2_card, rb1_card))) {
      LOG_WARN("failed to calculate and cardinality", K(ret));
    }
  } else if (is_bitmap_bin(rb1_type) && is_bitmap_bin(rb2_type)) {
    // no deserialize for roaringbitmap
    ObRoaring64Bin *rb1 = nullptr;
    ObRoaring64Bin *rb2 = nullptr;
    ObString rb1_roaring64_bin;
    ObString rb2_roaring64_bin;
    if (OB_FAIL(to_roaring64_bin(allocator, rb1_type, rb1_bin, rb1_roaring64_bin))) {
      LOG_WARN("failed to get roaring64 binary string from left roaringbitmap", K(ret), K(rb1_type));
    } else if (OB_ISNULL(rb1 = OB_NEWx(ObRoaring64Bin, &allocator, &allocator, rb1_roaring64_bin))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for left ObRoaring64Bin", K(ret));
    } else if (OB_FAIL(rb1->init())) {
      LOG_WARN("failed to init left ObRoaring64Bin", K(ret));
    } else if (OB_FAIL(to_roaring64_bin(allocator, rb2_type, rb2_bin, rb2_roaring64_bin))) {
      LOG_WARN("failed to get roaring64 binary string from right roaringbitmap", K(ret), K(rb2_type));
    } else if (OB_ISNULL(rb2 = OB_NEWx(ObRoaring64Bin, &allocator, &allocator, rb2_roaring64_bin))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for right ObRoaring64Bin", K(ret));
    } else if (OB_FAIL(rb2->init())) {
      LOG_WARN("failed to init right ObRoaring64Bin", K(ret));
    } else if (OB_FAIL(rb1->calc_and_cardinality(rb2, cardinality))) {
      LOG_WARN("failed to calculate and cardinality", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (rb1_card == 0 && OB_FAIL(rb1->get_cardinality(rb1_card))) {
      LOG_WARN("failed to get cardinality from left roaringbitmap", K(ret));
    } else if (rb2_card == 0 && OB_FAIL(rb2->get_cardinality(rb2_card))) {
      LOG_WARN("failed to get cardinality from right roaringbitmap", K(ret));
    }
  }
  return ret;
}


int ObRbUtils::binary_calc(ObIAllocator &allocator, ObString &rb1_bin, ObString &rb2_bin, ObString &res_rb_bin, ObRbOperation op)
{
  int ret = OB_SUCCESS;
  ObRbBinType rb1_bin_type = ObRbBinType::EMPTY;
  ObRbBinType rb2_bin_type = ObRbBinType::EMPTY;
  if (rb1_bin.empty() || rb2_bin.empty()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("roaringbitmap binary is empty", K(ret), K(rb1_bin), K(rb2_bin));
  } else if (OB_FAIL(get_bin_type(rb1_bin, rb1_bin_type))) {
    LOG_WARN("failed to get binary type", K(ret));
  } else if (OB_FAIL(get_bin_type(rb2_bin, rb2_bin_type))) {
    LOG_WARN("failed to get binary type", K(ret));
  } else if (op != ObRbOperation::AND && op != ObRbOperation::ANDNOT) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("operaration for roaringbitmap binary calculation except AND and ANDNOT is not supported", K(ret), K(op));
  } else if (rb1_bin_type == ObRbBinType::EMPTY) {
    if (op == ObRbOperation::AND) {
      res_rb_bin.assign_ptr(rb1_bin.ptr(), rb1_bin.length());
    } else if (op == ObRbOperation::ANDNOT) {
      res_rb_bin.assign_ptr(rb1_bin.ptr(), rb1_bin.length());
    }
  } else if (rb2_bin_type == ObRbBinType::EMPTY) {
    if (op == ObRbOperation::AND) {
      res_rb_bin.assign_ptr(rb2_bin.ptr(), rb2_bin.length());
    } else if (op == ObRbOperation::ANDNOT) {
      res_rb_bin.assign_ptr(rb1_bin.ptr(), rb1_bin.length());
    }
  } else if (!is_bitmap_bin(rb1_bin_type) && !is_bitmap_bin(rb2_bin_type)) {
    // if there is no bitmap binary, binary calculation is no effective
    // deserivalize rb1 and rb2 -> calculate in place -> serialize rb1 as result
    ObRoaringBitmap *rb1 = nullptr;
    ObRoaringBitmap *rb2 = nullptr;
    if (OB_FAIL(rb_deserialize(allocator, rb1_bin, rb1))) {
      LOG_WARN("failed to deserialize roaringbitmap rb1", K(ret), K(rb1_bin));
    } else if (OB_FAIL(rb_deserialize(allocator, rb2_bin, rb2))) {
      LOG_WARN("failed to deserialize roaringbitmap rb2", K(ret), K(rb2_bin));
    } else if (OB_FAIL(calc_inplace(rb1, rb2, op))) {
      LOG_WARN("failed to calcutlate roaringbitmap inplace", K(ret), K(op), K(rb1), K(rb2));
    } else if (OB_FAIL(rb_serialize(allocator, res_rb_bin, rb1))) {
      LOG_WARN("failed to serialize roaringbitmap", K(ret));
    }
    rb_destroy(rb1);
    rb_destroy(rb2);
  } else {
    // convert rb1 and rb2 to bitmap binary -> init ObRoaring64Bin -> calculate binary_and
    ObString rb1_bitmap_bin;
    ObString rb2_bitmap_bin;
    ObString rb1_roaring64_bin;
    ObString rb2_roaring64_bin;
    ObRoaring64Bin *rb1 = nullptr;
    ObRoaring64Bin *rb2 = nullptr;
    ObStringBuffer res_buf(&allocator);
    ObRbBinType res_bin_type = ObRbBinType::BITMAP_64;
    uint64_t res_card = 0;
    if (OB_FAIL(convert_to_bitmap_binary(allocator, rb1_bin, rb1_bitmap_bin, rb1_bin_type))) {
      LOG_WARN("failed to convert rb1_bin to bitmap binary", K(ret));
    } else if (OB_FAIL(to_roaring64_bin(allocator, rb1_bin_type, rb1_bitmap_bin, rb1_roaring64_bin))) {
      LOG_WARN("failed to get roaring64 binary string from rb1 roaringbitmap", K(ret), K(rb1_bin_type));
    } else if (OB_ISNULL(rb1 = OB_NEWx(ObRoaring64Bin, &allocator, &allocator, rb1_roaring64_bin))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for rb1 ObRoaring64Bin", K(ret));
    } else if (OB_FAIL(rb1->init())) {
      LOG_WARN("failed to init rb1 ObRoaring64Bin", K(ret));
    } else if (OB_FAIL(convert_to_bitmap_binary(allocator, rb2_bin, rb2_bitmap_bin, rb2_bin_type))) {
      LOG_WARN("failed to convert rb2_bin to bitmap binary", K(ret));
    } else if (OB_FAIL(to_roaring64_bin(allocator, rb2_bin_type, rb2_bitmap_bin, rb2_roaring64_bin))) {
      LOG_WARN("failed to get roaring64 binary string from rb2 roaringbitmap", K(ret), K(rb2_bin_type));
    } else if (OB_ISNULL(rb2 = OB_NEWx(ObRoaring64Bin, &allocator, &allocator, rb2_roaring64_bin))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for rb2 ObRoaring64Bin", K(ret));
    } else if (OB_FAIL(rb2->init())) {
      LOG_WARN("failed to init rb2 ObRoaring64Bin", K(ret));
    } else if (OB_FAIL(res_buf.append(rb1_bitmap_bin.ptr(), RB_VERSION_SIZE))) {
      LOG_WARN("failed to append version", K(ret));
    } else if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&res_bin_type), RB_BIN_TYPE_SIZE))) {
      LOG_WARN("failed to append res_bin_type", K(ret));
    } else if (op == ObRbOperation::AND && OB_FAIL(rb1->calc_and(rb2, res_buf, res_card))) {
        LOG_WARN("failed to calculate and", K(ret));
    } else if (op == ObRbOperation::ANDNOT && OB_FAIL(rb1->calc_andnot(rb2, res_buf, res_card))) {
        LOG_WARN("failed to calculate andnot", K(ret));
    } else if (OB_FALSE_IT(res_rb_bin.assign_ptr(res_buf.ptr(), res_buf.length()))) {
    } else if (res_card <= MAX_BITMAP_SET_VALUES) {
    // convert to smaller bintype
      ObRoaringBitmap *rb = NULL;
      if (OB_FAIL(rb_deserialize(allocator, res_rb_bin, rb))) {
        LOG_WARN("failed to deserialize roaringbitmap", K(ret));
      } else if OB_FAIL(rb_serialize(allocator, res_rb_bin, rb)) {
        LOG_WARN("failed to serialize roaringbitmap", K(ret));
      }
      rb_destroy(rb);
    }
  }
  return ret;
}

int ObRbUtils::calc_inplace(ObRoaringBitmap *&rb1, ObRoaringBitmap *&rb2, ObRbOperation op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(rb1) || OB_ISNULL(rb2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("roaringbitmap is null", K(ret), K(rb1), K(rb2));
  } else if (op == ObRbOperation::AND) {
    if (OB_FAIL(rb1->value_and(rb2))) {
      LOG_WARN("failed to calculate value and", K(ret), K(op));
    }
  } else if (op == ObRbOperation::OR) {
    if (OB_FAIL(rb1->value_or(rb2))) {
      LOG_WARN("failed to calculate value or", K(ret), K(op));
    }
  } else if (op == ObRbOperation::XOR) {
    if (OB_FAIL(rb1->value_xor(rb2))) {
      LOG_WARN("failed to calculate value xor", K(ret), K(op));
    }
  } else if (op == ObRbOperation::ANDNOT) {
    if (OB_FAIL(rb1->value_andnot(rb2))) {
      LOG_WARN("failed to calculate value andnot", K(ret), K(op));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ObRbOperation", K(ret), K(op));
  }
  return ret;
}

void ObRbUtils::rb_destroy(ObRoaringBitmap *&rb)
{
  if (OB_NOT_NULL(rb)) {
    rb->set_empty();
  }
  return;
}
int ObRbUtils::rb_deserialize(ObIAllocator &allocator, const ObString &rb_bin, ObRoaringBitmap *&rb, bool need_validate)
{
  int ret = OB_SUCCESS;
  ObRbBinType bin_type;
  if (OB_ISNULL(rb = OB_NEWx(ObRoaringBitmap, &allocator, (&allocator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create alloc memory to roaringbitmap", K(ret));
  } else if (OB_FAIL(rb->deserialize(rb_bin, need_validate))) {
    LOG_WARN("failed to deserialize roaringbitmap", K(ret));
  }
  return ret;
}

int ObRbUtils::rb_serialize(ObIAllocator &allocator, ObString &res_rb_bin, ObRoaringBitmap *&rb)
{
  int ret = OB_SUCCESS;
  ObStringBuffer res_buf(&allocator);
  if (OB_FAIL(rb->optimize())) {
    LOG_WARN("failed to optimize the roaringbitmap", K(ret));
  } else if (OB_FAIL(rb->serialize(res_buf))) {
    LOG_WARN("failed to serialize the roaringbitmap");
  } else {
    res_rb_bin.assign_ptr(res_buf.ptr(), res_buf.length());
  }
  return ret;
}

int ObRbUtils::build_binary(ObIAllocator &allocator, ObString &rb_bin, ObString &res_rb_bin)
{
  int ret = OB_SUCCESS;
  ObRoaringBitmap *rb = NULL;
  bool need_validate = true;
  // binary_check -> deserialize -> serialize
  if (OB_FAIL(check_binary(rb_bin))) {
    LOG_WARN("invalid roaringbitmap binary string", K(ret));
  } else if (OB_FAIL(rb_deserialize(allocator, rb_bin, rb, need_validate))) {
    LOG_WARN("failed to deserialize roaringbitmap", K(ret));
  } else if OB_FAIL(rb_serialize(allocator, res_rb_bin, rb)) {
    LOG_WARN("failed to serialize roaringbitmap", K(ret));
  }
  rb_destroy(rb);
  return ret;
}

int ObRbUtils::convert_to_bitmap_binary(ObIAllocator &allocator, const ObString &rb_bin, ObString &bitmap_bin, ObRbBinType &bin_type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_bin_type(rb_bin, bin_type))) {
    LOG_WARN("failed to get binary type", K(ret));
  } else if (bin_type == ObRbBinType::BITMAP_32 || bin_type == ObRbBinType::BITMAP_64) {
    // no need to convert
    bitmap_bin.assign_ptr(rb_bin.ptr(), rb_bin.length());
  } else {
    // For empty/single/set type, convert to bitmap type: deserialize -> convert_to_bitmap -> serialize
    ObRoaringBitmap *rb = NULL;
    ObStringBuffer res_buf(&allocator);
    ObString tmp_res_bin;
    if (OB_ISNULL(rb = OB_NEWx(ObRoaringBitmap, &allocator, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create alloc memory to roaringbitmap", K(ret));
    } else if (OB_FAIL(rb->deserialize(rb_bin))) {
      LOG_WARN("failed to deserialize roaringbitmap", K(ret));
    } else if (OB_FAIL(rb->convert_to_bitmap())) {
      LOG_WARN("failed to convert roaringbitmap to bitmap type", K(ret));
    } else if (OB_FAIL(rb->serialize(res_buf))) {
      LOG_WARN("failed to serialize the roaringbitmap");
    } else if (OB_FALSE_IT(bitmap_bin.assign_ptr(res_buf.ptr(), res_buf.length()))) {
    } else if(get_bin_type(bitmap_bin, bin_type)) {
      LOG_WARN("failed to get binary type", K(ret));
    }
    rb_destroy(rb);
  }
  return ret;
}

int ObRbUtils::binary_format_convert(ObIAllocator &allocator, const ObString &rb_bin, ObString &binary_str)
{
  int ret = OB_SUCCESS;
  ObRbBinType bin_type = ObRbBinType::EMPTY;
  ObString bitmap_bin;
  if (rb_bin.empty()) {
    binary_str.assign_ptr(rb_bin.ptr(), rb_bin.length());
  } else if (OB_FAIL(convert_to_bitmap_binary(allocator, rb_bin, bitmap_bin, bin_type))) {
    LOG_WARN("failed to convert rb_bin to bitmap binary", K(ret), K(rb_bin));
  } else if (bin_type == ObRbBinType::BITMAP_32) {
    binary_str.assign_ptr(bitmap_bin.ptr(), bitmap_bin.length());
  } else if (bin_type == ObRbBinType::BITMAP_64) {
    // check and convert to 32-bit roaring binary
    uint32_t offset = RB_VERSION_SIZE + RB_BIN_TYPE_SIZE;
    uint64_t buckets = 0;
    uint32_t high32 = 0;
    if (bitmap_bin.length() < offset + sizeof(uint64_t)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("ran out of bytes while reading buckets", K(ret), K(bitmap_bin.length()));
    } else {
      buckets = *reinterpret_cast<uint64_t*>(bitmap_bin.ptr() + offset);
      offset += sizeof(uint64_t);
      if (buckets == 0) {
        // Generate an empty 32-bit roaring binary
        ObStringBuffer res_buf(&allocator);
        uint8_t version = *(bitmap_bin.ptr());
        bin_type = ObRbBinType::BITMAP_32;
        uint32_t roaring32_cookie = roaring::internal::SERIAL_COOKIE_NO_RUNCONTAINER;
        uint32_t container_num = 0;
        if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&version), RB_VERSION_SIZE))) {
          LOG_WARN("failed to append version", K(ret));
        } else if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&bin_type), RB_BIN_TYPE_SIZE))) {
          LOG_WARN("failed to append bin_type", K(ret));
        } else if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&roaring32_cookie), sizeof(uint32_t)))) {
          LOG_WARN("failed to append roaring32_cookie", K(ret));
        } else if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&container_num), sizeof(uint32_t)))) {
          LOG_WARN("failed to append container_num", K(ret));
        } else {
          binary_str.assign_ptr(res_buf.ptr(), res_buf.length());
        }
      } else if (buckets == 1) {
        // read high32 and check if high32 == 0
        if (bitmap_bin.length() < offset + sizeof(uint32_t)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("ran out of bytes while reading the first high32", K(ret), K(bitmap_bin.length()));
        } else {
          high32 = *reinterpret_cast<uint32_t*>(bitmap_bin.ptr() + offset);
          offset += sizeof(uint32_t);
          if (high32 == 0) {
            // convert to 32-bit roaring binary directly
            ObStringBuffer res_buf(&allocator);
            uint8_t version = *(bitmap_bin.ptr());
            bin_type = ObRbBinType::BITMAP_32;
            if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&version), RB_VERSION_SIZE))) {
              LOG_WARN("failed to append version", K(ret));
            } else if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&bin_type), RB_BIN_TYPE_SIZE))) {
              LOG_WARN("failed to append bin_type", K(ret));
            } else if (OB_FAIL(res_buf.append(bitmap_bin.ptr() + offset, bitmap_bin.length() - offset))) {
              LOG_WARN("failed to append roaring binary string" ,K(ret));
            } else {
              binary_str.assign_ptr(res_buf.ptr(), res_buf.length());
            }
          } else {
            // no need to convert
            binary_str.assign_ptr(bitmap_bin.ptr(), bitmap_bin.length());
          }
        }
      } else {
        // no need to convert
        binary_str.assign_ptr(bitmap_bin.ptr(), bitmap_bin.length());
      }
    }
  }
  return ret;
}

int ObRbUtils::rb_from_string(ObIAllocator &allocator, ObString &rb_str, ObRoaringBitmap *&rb)
{
  int ret = OB_SUCCESS;
  const char *str = rb_str.ptr();
  char *str_end = rb_str.ptr() + rb_str.length();
  char *value_end = nullptr;
  uint64_t value = 0;
  bool is_first = true;

  if (OB_ISNULL(rb = OB_NEWx(ObRoaringBitmap, &allocator, (&allocator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create alloc memory to roaringbitmap", K(ret));
  } else {
    // parse roaringbitmap string
    str_skip_space_(str, str_end);
    while (str < str_end && OB_SUCC(ret)) {
      if (is_first) {
        is_first = false;
      } else if (*str == ',') {
        str++;
        str_skip_space_(str, str_end);
      } else {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid roaringbitmap string", K(ret), K(*str));
      }
      // pares uint64 value
      if (OB_FAIL(ret)) {
      } else if (str == str_end) {
        ret = OB_INVALID_DATA;
        LOG_WARN("no value string after the comma", K(ret));
      } else if (OB_FAIL(str_read_value_(str, str_end - str, value_end, value))) {
        LOG_WARN("failed to transfer value string", K(ret), K(str));
      } else if (str == value_end) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid roaringbitmap string", K(ret), K(*str));
      } else if (OB_FAIL(rb->value_add(value))) {
        LOG_WARN("failed to add value to roaringbtimap", K(ret), K(value));
      } else {
        str = value_end;
        str_skip_space_(str, str_end);
      }
    }
  }
  return ret;
}

int ObRbUtils::rb_to_string(ObIAllocator &allocator, ObString &rb_bin, ObString &res_rb_str)
{
  int ret = OB_SUCCESS;
  ObRbBinType bin_type;
  uint32_t offset = RB_VERSION_SIZE + RB_BIN_TYPE_SIZE;
  ObStringBuffer res_buf(&allocator);
  if (OB_FAIL(get_bin_type(rb_bin, bin_type))) {
    LOG_WARN("invalid roaringbitmap binary string", K(ret));
  } else {
    switch(bin_type) {
      case ObRbBinType::EMPTY: {
        // do nothing
        break;
      }
      case ObRbBinType::SINGLE_32: {
        uint32_t value_32 = *reinterpret_cast<const uint32_t*>(rb_bin.ptr() + offset);
        ObFastFormatInt ffi(value_32);
        if (OB_FAIL(res_buf.append(ffi.ptr(), ffi.length(), 0))) {
          LOG_WARN("failed to append res_buf", K(ret), K(value_32), K(ffi.ptr()), K(ffi.length()));
        }
        break;
      }
      case ObRbBinType::SINGLE_64: {
        uint64_t value_64 = *reinterpret_cast<const uint64_t*>(rb_bin.ptr() + offset);
        ObFastFormatInt ffi(value_64);
        if (OB_FAIL(res_buf.append(ffi.ptr(), ffi.length(), 0))) {
          LOG_WARN("failed to append res_buf", K(ret), K(value_64), K(ffi.ptr()), K(ffi.length()));
        }
        break;
      }
      case ObRbBinType::SET_32: {
        bool is_first = true;
        uint8_t value_count = static_cast<uint8_t>(*(rb_bin.ptr() + offset));
        offset += RB_VALUE_COUNT_SIZE;
        if (value_count > 0) {
          uint32_t *value_ptr = reinterpret_cast<uint32_t *>(rb_bin.ptr() + offset);
          lib::ob_sort(value_ptr, value_ptr + value_count);
          for (int i = 0; OB_SUCC(ret) && i < value_count; i++) {
            uint32_t value_32 = *reinterpret_cast<const uint32_t*>(rb_bin.ptr() + offset);
            offset += sizeof(uint32_t);
            ObFastFormatInt ffi(value_32);
            if (!is_first && OB_FAIL(res_buf.append(","))) {
              LOG_WARN("failed to append res_buf", K(ret));
            } else if (is_first && OB_FALSE_IT(is_first = false)) {
            } else if (OB_FAIL(res_buf.append(ffi.ptr(), ffi.length(), 0))) {
              LOG_WARN("failed to append res_buf", K(ret), K(value_32), K(ffi.ptr()), K(ffi.length()));
            }
          }
        }
        break;
      }
      case ObRbBinType::SET_64: {
        bool is_first = true;
        uint8_t value_count = static_cast<uint8_t>(*(rb_bin.ptr() + offset));
        offset += RB_VALUE_COUNT_SIZE;
        if (value_count > 0) {
          uint64_t *value_ptr = reinterpret_cast<uint64_t *>(rb_bin.ptr() + offset);
          lib::ob_sort(value_ptr, value_ptr + value_count);
          for (int i = 0; OB_SUCC(ret) && i < value_count; i++) {
            uint64_t value_64 = *reinterpret_cast<const uint64_t*>(rb_bin.ptr() + offset);
            offset += sizeof(uint64_t);
            ObFastFormatInt ffi(value_64);
            if (!is_first && OB_FAIL(res_buf.append(","))) {
              LOG_WARN("failed to append res_buf", K(ret));
            } else if (is_first && OB_FALSE_IT(is_first = false)) {
            } else if (OB_FAIL(res_buf.append(ffi.ptr(), ffi.length(), 0))) {
              LOG_WARN("failed to append res_buf", K(ret), K(value_64), K(ffi.ptr()), K(ffi.length()));
            }
          }
        }
        break;
      }
      case ObRbBinType::BITMAP_32: {
        bool is_first = true;
        roaring::api::roaring_bitmap_t *bitmap = nullptr;
        roaring::api::roaring_uint32_iterator_t *iter = nullptr;
        ROARING_TRY_CATCH(bitmap = roaring::api::roaring_bitmap_portable_deserialize_safe(rb_bin.ptr() + offset, rb_bin.length() - offset));
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(bitmap)) {
            ret = OB_DESERIALIZE_ERROR;
            LOG_WARN("failed to deserialize the bitmap", K(ret));
        } else if (roaring::api::roaring_bitmap_get_cardinality(bitmap) > max_rb_to_string_cardinality) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("cardinality of roaringbitmap is over 1000000", K(ret), K(roaring::api::roaring_bitmap_get_cardinality(bitmap)));
        } else {
          ROARING_TRY_CATCH(iter = roaring::api::roaring_iterator_create(bitmap));
          if (OB_FAIL(ret)) {
          } else if (OB_ISNULL(iter)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to get iterate from bitmap", K(ret));
          } else if (iter->has_value) {
            do {
              ObFastFormatInt ffi(iter->current_value);
              if (!is_first && OB_FAIL(res_buf.append(","))) {
                LOG_WARN("failed to append res_buf", K(ret));
              } else if (is_first && OB_FALSE_IT(is_first = false)) {
              } else if (OB_FAIL(res_buf.append(ffi.ptr(), ffi.length(), 0))) {
                LOG_WARN("failed to append res_buf", K(ret), K(iter->current_value), K(ffi.ptr()), K(ffi.length()));
              }
            } while (OB_SUCC(ret) && roaring::api::roaring_uint32_iterator_advance(iter));
          }
        }
        if (OB_NOT_NULL(iter)) {
          roaring::api::roaring_uint32_iterator_free(iter);
        }
        if (OB_NOT_NULL(bitmap)) {
          roaring::api::roaring_bitmap_free(bitmap);
        }
        break;
      }
      case ObRbBinType::BITMAP_64: {
        bool is_first = true;
        roaring::api::roaring64_bitmap_t *bitmap = nullptr;
        roaring::api::roaring64_iterator_t *iter = nullptr;
        ROARING_TRY_CATCH(bitmap = roaring::api::roaring64_bitmap_portable_deserialize_safe(
                                                  rb_bin.ptr() + offset,
                                                  rb_bin.length() - offset));
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(bitmap)) {
          ret = OB_DESERIALIZE_ERROR;
          LOG_WARN("failed to deserialize the bitmap", K(ret));
        } else if (roaring::api::roaring64_bitmap_get_cardinality(bitmap) > max_rb_to_string_cardinality) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("cardinality of roaringbitmap is over 1000000", K(ret), K(roaring::api::roaring64_bitmap_get_cardinality(bitmap)));
        } else {
          ROARING_TRY_CATCH(iter = roaring::api::roaring64_iterator_create(bitmap));
          if (OB_FAIL(ret)) {
          } else if (OB_ISNULL(iter)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to get iterate from bitmap", K(ret));
          } else if (roaring::api::roaring64_iterator_has_value(iter)) {
            do {
              ObFastFormatInt ffi(roaring::api::roaring64_iterator_value(iter));
              if (!is_first && OB_FAIL(res_buf.append(","))) {
                LOG_WARN("failed to append res_buf", K(ret));
              } else if (is_first && OB_FALSE_IT(is_first = false)) {
              } else if (OB_FAIL(res_buf.append(ffi.ptr(), ffi.length(), 0))) {
                LOG_WARN("failed to append res_buf", K(ret), K(roaring::api::roaring64_iterator_value(iter)), K(ffi.ptr()), K(ffi.length()));
              }
            } while (OB_SUCC(ret) && roaring::api::roaring64_iterator_advance(iter));
          }
        }
        if (OB_NOT_NULL(iter)) {
          roaring::api::roaring64_iterator_free(iter);
        }
        if (OB_NOT_NULL(bitmap)) {
          roaring::api::roaring64_bitmap_free(bitmap);
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unknown RbBinType", K(ret), K(bin_type));
        break;
      }
    } // end switch
  }

  if (OB_SUCC(ret)) {
    res_rb_str.assign_ptr(res_buf.ptr(), res_buf.length());
  }
  return ret;
}

int ObRbUtils::str_read_value_(const char *str, size_t len,  char *&value_end, uint64_t &value)
{
  int ret = OB_SUCCESS;
  int err = 0;
  if (*str == '-') {
    int64_t val_64 = ObCharset::strntoll(str, len, 10, &value_end, &err);
    if (err == 0) {
      if (val_64 < INT32_MIN) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("negative integer not in the range of int32", K(ret), K(val_64));
      } else if (val_64 < 0) {
        // convert negative integer to uint32
        uint32_t val_u32 = static_cast<uint32_t>(val_64);
        value = static_cast<uint64_t>(val_u32);
      } else {
        value = static_cast<uint64_t>(val_64);
      }
    } else if (err == ERANGE) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("int64 value out of range", K(ret), K(str));
    } else {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid int64 value", K(ret), K(str));
    }
  } else {
    uint64_t val_u64 = ObCharset::strntoull(str, len, 10, &value_end, &err);
    if (err == 0) {
      value = val_u64;
    } else if (err == ERANGE) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("uint64 value out of range", K(ret), K(str));
    } else {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid uint64 value", K(ret), K(str));
    }
  }
  return ret;
}

ObRbAggCell::ObRbAggCell(ObRoaringBitmap *rb, const uint64_t tenant_id):
    allocator_("RbAggCell", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id),
    rb_(rb),
    max_cache_count_(0),
    cached_value_(),
    rb_bin_(),
    is_serialized_(false)
{
  max_cache_count_ = MAX_CACHED_COUNT;
  cached_value_.set_attr(lib::ObMemAttr(tenant_id, "RbAggArray"));
}

int ObRbAggCell::destroy()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(rb_)) {
    ObRbUtils::rb_destroy(rb_);
    rb_ = nullptr;
  }
  cached_value_.reset();
  rb_bin_.reset();
  allocator_.reset();
  is_serialized_ = false;
  return ret;
}

int ObRbAggCell::add_values(const ObArray<uint64_t> &values)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i< values.count(); ++i) {
    if (OB_FAIL(rb_->value_add(values.at(i)))) {
      LOG_WARN("add value fail", K(ret), K(i));
    }
  }
  return ret;
}

int ObRbAggCell::value_add(const uint64_t val)
{
  int ret = OB_SUCCESS;
  if (is_serialized_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is serialized, can not add value", K(ret), KPC(this));
  } else if (cached_value_.count() < max_cache_count_) {
    if (OB_FAIL(cached_value_.push_back(val))) {
      LOG_WARN("push back fail");
    }
  } else if (OB_ISNULL(rb_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rb is null", K(ret));
  } else if (OB_FAIL(rb_->value_add(val))) {
    LOG_WARN("add value fail", K(ret), K(val));
  } else if (OB_FAIL(add_values(cached_value_))) {
    LOG_WARN("add value fail", K(ret), K(val));
  } else {
    cached_value_.reuse();
  }
  return ret;
}

int ObRbAggCell::value_or(const ObRbAggCell *other)
{
  int ret = OB_SUCCESS;
  if (is_serialized_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is serialized, can not add value", K(ret), KPC(this));
  } else if (OB_ISNULL(rb_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rb is null", K(ret));
  } else if (OB_ISNULL(other)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("other rb is null", K(ret));
  } else if (OB_FAIL(add_values(cached_value_))) {
    LOG_WARN("add value fail", K(ret));
  } else if (OB_FAIL(add_values(other->cached_value_))) {
    LOG_WARN("add value fail", K(ret));
  } else if (OB_NOT_NULL(other->rb_) && OB_FAIL(rb_->value_or(other->rb_))) {
    LOG_WARN("or value fail", K(ret));
  } else {
    // reset for save memory
    cached_value_.reset();
  }
  return ret;
}

int ObRbAggCell::serialize(ObString &rb_bin)
{
  int ret = OB_SUCCESS;
  if (! is_serialized_ && OB_FAIL(serialize())) {
    LOG_WARN("serialize fail", K(ret));
  } else if (rb_bin_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("may be serialize fail", K(ret));
  } else {
    rb_bin = rb_bin_;
  }
  return ret;
}

int ObRbAggCell::serialize()
{
  int ret = OB_SUCCESS;
  if (is_serialized_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("serialized again", K(ret), KPC(this));
  } else if (OB_ISNULL(rb_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("serialize rb is null", K(ret));
  } else if (OB_FAIL(add_values(cached_value_))) {
    LOG_WARN("add values fail", K(ret));
  } else if (OB_FAIL(ObRbUtils::rb_serialize(allocator_, rb_bin_, rb_))) {
    LOG_WARN("rb_serialize fail", K(ret));
  } else {
    cached_value_.reset();
    ObRbUtils::rb_destroy(rb_);
    rb_ = nullptr;
    is_serialized_ = true;
  }
  return ret;
}

int ObRbAggAllocator::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(alloced_rb_.create(10, lib::ObMemAttr(tenant_id_, "RbAggAlloc")))) {
    LOG_WARN("failed to create set", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObRbAggAllocator::destroy_all_rb()
{
  if (alloced_rb_.size() > 0) {
    LOG_INFO("will destory by rb allocator", KP(this), K(alloced_rb_.size()));
    hash::ObHashSet<uint64_t, hash::NoPthreadDefendMode>::const_iterator iter;
    for (iter = alloced_rb_.begin(); iter != alloced_rb_.end(); iter++) {
      ObRbAggCell *rb = reinterpret_cast<ObRbAggCell*>(iter->first);
      if (OB_NOT_NULL(rb)) {
        rb->destroy();
      }
    }
  }
}

void ObRbAggAllocator::free(ObRbAggCell *rb)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rb)) {
  } else if (OB_FAIL(alloced_rb_.erase_refactored(reinterpret_cast<uint64_t>(rb)))) {
    LOG_WARN("failed to erase from the set", K(ret), KPC(rb));
  } else {
    rb->destroy();
  }
}

ObRbAggCell *ObRbAggAllocator::alloc()
{
  int ret = OB_SUCCESS;
  ObRbAggCell *res_ptr = nullptr;
  ObRoaringBitmap *rb = nullptr;
  ObRbAggCell *rb_cell = nullptr;
  if (IS_NOT_INIT && OB_FAIL(init())) {
    LOG_ERROR("init fail", KR(ret));
  } else if (OB_ISNULL(rb = OB_NEWx(ObRoaringBitmap, &rb_allocator_, &rb_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate rb memory failed", K(ret), "size", sizeof(ObRoaringBitmap));
  } else if (OB_ISNULL(rb_cell = OB_NEWx(ObRbAggCell, &rb_allocator_, rb, tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate cell memory failed", K(ret), "size", sizeof(ObRbAggCell));
  } else if (OB_FAIL(alloced_rb_.set_refactored(reinterpret_cast<uint64_t>(rb_cell)))) {
    LOG_WARN("push back failed", K(ret));
  } else {
    res_ptr = rb_cell;
  }
  return res_ptr;
}

int ObRbAggAllocator::rb_serialize(ObString &rb_bin, ObRbAggCell *rb)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rb)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rb is null", K(ret));
  } else if (OB_FAIL(rb->serialize(rb_bin))) {
    LOG_WARN("serialize failed", K(ret));
  }
  return ret;
}

int ObRbAggAllocator::rb_serialize(ObRbAggCell *rb)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rb)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rb is null", K(ret));
  } else if (OB_FAIL(rb->serialize())) {
    LOG_WARN("serialize failed", K(ret));
  }
  return ret;
}

}  // namespace common
}  // namespace oceanbase
