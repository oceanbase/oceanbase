
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
#include "lib/ob_errno.h"
#include "lib/utility/ob_sort.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_fast_convert.h"
#include "lib/roaringbitmap/ob_rb_utils.h"
#include "lib/roaringbitmap/ob_rb_bin.h"


namespace oceanbase
{
namespace common
{

const uint64_t max_rb_to_string_cardinality = 1000000;

int ObRbUtils::check_get_bin_type(const ObString &rb_bin, ObRbBinType &bin_type)
{
  int ret = OB_SUCCESS;
  uint32_t offset = RB_VERSION_SIZE + RB_TYPE_SIZE;
  // get_bin_type
  if (rb_bin == nullptr) {
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
        } else if (value_count < 0 || value_count > MAX_BITMAP_SET_VALUES) {
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
        } else if (value_count < 0 || value_count > MAX_BITMAP_SET_VALUES) {
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

int ObRbUtils::get_cardinality(ObIAllocator &allocator, const ObString &rb_bin, ObRbBinType bin_type, uint64_t &cardinality)
{
  int ret = OB_SUCCESS;
  uint32_t offset = RB_VERSION_SIZE + RB_BIN_TYPE_SIZE;
  if (bin_type == ObRbBinType::EMPTY) {
    cardinality = 0;
  } else if (bin_type == ObRbBinType::SINGLE_32 || bin_type == ObRbBinType::SINGLE_64) {
    cardinality = 1;
  } else if (bin_type == ObRbBinType::SET_32 || bin_type == ObRbBinType::SET_64) {
    uint8_t value_count = static_cast<uint8_t>(*(rb_bin.ptr() + offset));
    cardinality =  static_cast<uint64_t>(value_count);
  } else {
    ObString binary_str;
    binary_str.assign_ptr(rb_bin.ptr() + offset, rb_bin.length() - offset);
    if (bin_type == ObRbBinType::BITMAP_32) {
      ObRoaringBin *roaring_bin = NULL;
      if (OB_ISNULL(roaring_bin = OB_NEWx(ObRoaringBin, &allocator, &allocator, binary_str))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for ObRoaringBin", K(ret));
      } else if (OB_FAIL(roaring_bin->init())) {
        LOG_WARN("failed to get roaring card", K(ret), K(binary_str));
      } else if (OB_FAIL(roaring_bin->get_cardinality(cardinality))) {
        LOG_WARN("failed to get roaring card", K(ret), K(binary_str));
      }
    } else if (bin_type == ObRbBinType::BITMAP_64) {
      ObRoaring64Bin *roaring64_bin = NULL;
      if (OB_ISNULL(roaring64_bin = OB_NEWx(ObRoaring64Bin, &allocator, &allocator, binary_str))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for ObRoaring64Bin", K(ret));
      } else if (OB_FAIL(roaring64_bin->init())) {
        LOG_WARN("failed to get roaring card", K(ret), K(binary_str));
      } else if (OB_FAIL(roaring64_bin->get_cardinality(cardinality))) {
        LOG_WARN("failed to get roaring card", K(ret), K(binary_str));
      }
    }
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
int ObRbUtils::rb_deserialize(ObIAllocator &allocator, const ObString &rb_bin, ObRoaringBitmap *&rb)
{
  int ret = OB_SUCCESS;
  ObRbBinType bin_type;
  if (OB_FAIL(check_get_bin_type(rb_bin, bin_type))) {
    LOG_WARN("invalid roaringbitmap binary string", K(ret));
  } else if (OB_ISNULL(rb = OB_NEWx(ObRoaringBitmap, &allocator, (&allocator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create alloc memory to roaringbitmap", K(ret));
  } else if (OB_FAIL(rb->deserialize(rb_bin))) {
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
  ObRbBinType bin_type;
  uint64_t cardinality = 0;
  if (OB_FAIL(check_get_bin_type(rb_bin, bin_type))) {
    LOG_WARN("invalid roaringbitmap binary string", K(ret));
  } else if (OB_FAIL(get_cardinality(allocator, rb_bin, bin_type, cardinality))) {
    LOG_WARN("failed to get cardinality from roaringbitmap binary", K(ret));
  } else if (((bin_type == ObRbBinType::BITMAP_32 || bin_type == ObRbBinType::BITMAP_64) && cardinality <= MAX_BITMAP_SET_VALUES)
              || (bin_type == ObRbBinType::SET_32 && cardinality < 2)
              || bin_type == ObRbBinType::SET_64) {
    // deserialize -> optimize -> serialize
    ObRoaringBitmap *rb = NULL;
    if (OB_FAIL(rb_deserialize(allocator, rb_bin, rb))) {
      LOG_WARN("failed to deserialize roaringbitmap", K(ret));
    } else if (OB_FAIL(rb_serialize(allocator, res_rb_bin, rb))) {
      LOG_WARN("failed to serialize roaringbitmap", K(ret));
    }
    rb_destroy(rb);
  } else if (bin_type == ObRbBinType::BITMAP_64) {
    uint32_t offset = RB_VERSION_SIZE + RB_BIN_TYPE_SIZE;
    uint64_t buckets = *reinterpret_cast<uint64_t*>(rb_bin.ptr() + offset);
    offset += sizeof(uint64_t);
    uint32_t high32 = *reinterpret_cast<uint32_t*>(rb_bin.ptr() + offset);
    offset += sizeof(uint32_t);
    if (buckets == 1 && high32 == 0) {
      // BITMAP_32 is enough
      bin_type = ObRbBinType::BITMAP_32;
      ObStringBuffer res_buf(&allocator);
      if (OB_FAIL(res_buf.append(rb_bin.ptr(), RB_VERSION_SIZE))) {
        LOG_WARN("failed to append version", K(ret), K(rb_bin));
      } else if (OB_FAIL(res_buf.append(reinterpret_cast<const char*>(&bin_type), RB_BIN_TYPE_SIZE))) {
        LOG_WARN("failed to append bin_type", K(ret));
      } else if (OB_FAIL(res_buf.append(rb_bin.ptr() + offset, rb_bin.length() - offset))) {
        LOG_WARN("failed to append roaing binary", K(ret), K(rb_bin));
      } else {
        res_rb_bin.assign_ptr(res_buf.ptr(), res_buf.length());
      }
    } else {
      res_rb_bin = rb_bin;
    }
  } else {
    res_rb_bin = rb_bin;
  }
  return ret;
}

int ObRbUtils::binary_format_convert(ObIAllocator &allocator, const ObString &rb_bin, ObString &binary_str)
{
  int ret = OB_SUCCESS;
  ObRbBinType bin_type;
  if (rb_bin.empty()) {
    binary_str = rb_bin;
  } else if (OB_FAIL(check_get_bin_type(rb_bin, bin_type))) {
    LOG_WARN("invalid roaringbitmap binary string", K(ret));
  } else if (bin_type == ObRbBinType::BITMAP_32 || bin_type == ObRbBinType::BITMAP_64) {
    binary_str.assign_ptr(rb_bin.ptr(),rb_bin.length());
  } else {
    ObRoaringBitmap *rb = NULL;
    ObStringBuffer res_buf(&allocator);
    if (OB_ISNULL(rb = OB_NEWx(ObRoaringBitmap, &allocator, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create alloc memory to roaringbitmap", K(ret));
    } else if (OB_FAIL(rb->deserialize(rb_bin))) {
      LOG_WARN("failed to deserialize roaringbitmap", K(ret));
    } else if (OB_FAIL(rb->convert_to_bitmap())) {
      LOG_WARN("failed to convert roaringbitmap to bitmap type", K(ret));
    } else if (OB_FAIL(rb->serialize(res_buf))) {
      LOG_WARN("failed to serialize the roaringbitmap");
    } else {
      binary_str.assign_ptr(res_buf.ptr(), res_buf.length());
    }
    rb_destroy(rb);
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
  if (OB_FAIL(check_get_bin_type(rb_bin, bin_type))) {
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
        if (OB_ISNULL(bitmap = roaring::api::roaring_bitmap_portable_deserialize_safe(rb_bin.ptr() + offset, rb_bin.length() - offset))) {
          ret = OB_DESERIALIZE_ERROR;
          LOG_WARN("failed to deserialize the bitmap", K(ret));
        } else if (!roaring::api::roaring_bitmap_internal_validate(bitmap, NULL)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("bitmap internal consistency checks failed", K(ret));
        } else if (roaring::api::roaring_bitmap_get_cardinality(bitmap) > max_rb_to_string_cardinality) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("cardinality of roaringbitmap is over 1000000", K(ret), K(roaring::api::roaring_bitmap_get_cardinality(bitmap)));
        } else if (OB_ISNULL(iter = roaring_iterator_create(bitmap))) {
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
        if (OB_ISNULL(bitmap = roaring::api::roaring64_bitmap_portable_deserialize_safe(
                                                 rb_bin.ptr() + offset,
                                                 rb_bin.length() - offset))) {
          ret = OB_DESERIALIZE_ERROR;
          LOG_WARN("failed to deserialize the bitmap", K(ret));
        } else if (!roaring::api::roaring64_bitmap_internal_validate(bitmap, NULL)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("bitmap internal consistency checks failed", K(ret));
        } else if (roaring::api::roaring64_bitmap_get_cardinality(bitmap) > max_rb_to_string_cardinality) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("cardinality of roaringbitmap is over 1000000", K(ret), K(roaring::api::roaring64_bitmap_get_cardinality(bitmap)));
        } else if (OB_ISNULL(iter = roaring::api::roaring64_iterator_create(bitmap))) {
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

int ObRbUtils::and_cardinality(ObRoaringBitmap *&rb1, ObRoaringBitmap *&rb2, uint64_t &cardinality)
{
  int ret = OB_SUCCESS;
  cardinality = 0;
  if (rb2->is_bitmap_type() && !rb1->is_bitmap_type()) {
    ret = and_cardinality(rb2, rb1, cardinality);
  } else if (rb2->is_empty_type()) {
    // do noting
  } else if (rb2->is_single_type()) {
    if (rb1->is_contains(rb2->get_single_value())) {
      cardinality += 1;
    }
  } else if (rb2->is_set_type()) {
    hash::ObHashSet<uint64_t> *set = rb2->get_set();
    hash::ObHashSet<uint64_t>::const_iterator iter;
    for (iter = set->begin(); iter != set->end(); iter++) {
      if (rb1->is_contains(iter->first)) {
        cardinality += 1;
      }
    }
  } else { // both rb1 and rb2 is bitmap type
    cardinality = roaring::api::roaring64_bitmap_and_cardinality(rb1->get_bitmap(), rb2->get_bitmap());
  }
  return ret;
}

int ObRbUtils::or_cardinality(ObRoaringBitmap *&rb1, ObRoaringBitmap *&rb2, uint64_t &cardinality)
{
  int ret = OB_SUCCESS;
  uint64_t c1 = rb1->get_cardinality();
  uint64_t c2 = rb2->get_cardinality();
  uint64_t inter = 0;
  if (OB_FAIL(and_cardinality(rb1, rb2, inter))) {
    LOG_WARN("failed to get and_cardinality", K(ret));
  } else {
    cardinality = c1 + c2 - inter;
  }
  return ret;
}

int ObRbUtils::xor_cardinality(ObRoaringBitmap *&rb1, ObRoaringBitmap *&rb2, uint64_t &cardinality)
{
  int ret = OB_SUCCESS;
  uint64_t c1 = rb1->get_cardinality();
  uint64_t c2 = rb2->get_cardinality();
  uint64_t inter = 0;
  if (OB_FAIL(and_cardinality(rb1, rb2, inter))) {
    LOG_WARN("failed to get and_cardinality", K(ret));
  } else {
    cardinality = c1 + c2 - 2 * inter;
  }
  return ret;
}

int ObRbUtils::andnot_cardinality(ObRoaringBitmap *&rb1, ObRoaringBitmap *&rb2, uint64_t &cardinality)
{
  int ret = OB_SUCCESS;
  uint64_t c1 = rb1->get_cardinality();
  uint64_t inter = 0;
  if (OB_FAIL(and_cardinality(rb1, rb2, inter))) {
    LOG_WARN("failed to get and_cardinality", K(ret));
  } else {
    cardinality = c1 - inter;
  }
  return ret;
}

int ObRbUtils::calc_cardinality(ObRoaringBitmap *&rb1, ObRoaringBitmap *&rb2, uint64_t &cardinality, ObRbOperation op)
{
  int ret = OB_SUCCESS;
  if (op == ObRbOperation::AND) {
    if (OB_FAIL(and_cardinality(rb1, rb2, cardinality))) {
      LOG_WARN("failed to calculate cardinality", K(ret), K(op));
    }
  } else if (op == ObRbOperation::OR) {
    if (OB_FAIL(or_cardinality(rb1, rb2, cardinality))) {
      LOG_WARN("failed to calculate cardinality", K(ret), K(op));
    }
  } else if (op == ObRbOperation::XOR) {
    if (OB_FAIL(xor_cardinality(rb1, rb2, cardinality))) {
      LOG_WARN("failed to calculate cardinality", K(ret), K(op));
    }
  } else if (op == ObRbOperation::ANDNOT) {
    if (OB_FAIL(andnot_cardinality(rb1, rb2, cardinality))) {
      LOG_WARN("failed to calculate cardinality", K(ret), K(op));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("roaringbitmap operation not supported", K(ret), K(op));
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

}  // namespace common
}  // namespace oceanbase
