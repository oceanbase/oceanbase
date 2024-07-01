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

#ifndef OCEANBASE_LIB_ROARINGBITMAP_OB_ROARINGBITMAP_
#define OCEANBASE_LIB_ROARINGBITMAP_OB_ROARINGBITMAP_

#include <stdint.h>
#include <string.h>
#include "roaring/roaring.h"
#include "roaring/roaring64.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_string_buffer.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/hash/ob_hashset.h"
#include "lib/allocator/page_arena.h"


namespace oceanbase {
namespace common {

#define MAX_BITMAP_SET_VALUES 32
#define IS_VALID_RB_VERSION(ver) (ver == BITMAP_VESION_1)

static const uint32_t RB_VERSION_SIZE = sizeof(uint8_t);
static const uint32_t RB_TYPE_SIZE = sizeof(uint8_t);
static const uint32_t RB_BIN_TYPE_SIZE = sizeof(uint8_t);
static const uint32_t RB_VALUE_COUNT_SIZE = sizeof(uint8_t);

static const uint8_t BITMAP_VESION_1 = 1;

enum class ObRbBinType
{
    EMPTY = 0,  // empty bitmap
    SINGLE_32 = 1, // single uint32_t element
    SINGLE_64 = 2, // single uint64_t element
    SET_32 = 3,    // cardinality <= 32 && max element <= MAX_UINT32
    SET_64 = 4,    // cardinality <= 32
    BITMAP_32 = 5, // cardinality > 32 && max element <= MAX_UINT32, RoaringBitmap
    BITMAP_64 = 6, // cardinality > 32, RoaringBitmap
    MAX_TYPE = 7
};

enum class ObRbType
{
    EMPTY = 0,  // empty bitmap
    SINGLE = 1, // single element
    SET = 2,    // cardinality <= 32
    BITMAP = 3, // cardinality > 32, RoaringBitmap
};

enum class ObRbOperation
{
    OR = 0,
    AND = 1,
    XOR = 2,
    ANDNOT = 3,
};
class ObRoaringBitmap
{
public:
  ObRoaringBitmap(ObIAllocator *allocator)
      : allocator_(allocator),
        version_(BITMAP_VESION_1),
        type_(ObRbType::EMPTY),
        bitmap_(nullptr)  {}
  virtual ~ObRoaringBitmap() = default;

  inline uint8_t get_version() { return version_; }
  inline ObRbType get_type() { return type_; }
  inline uint64_t get_single_value() { return single_value_; }
  inline hash::ObHashSet<uint64_t>* get_set() { return &set_; }
  inline roaring::api::roaring64_bitmap_t * get_bitmap() { return bitmap_; }
  inline bool is_empty_type() { return ObRbType::EMPTY == type_; }
  inline bool is_single_type() { return ObRbType::SINGLE == type_; }
  inline bool is_set_type() { return ObRbType::SET == type_; }
  inline bool is_bitmap_type() { return ObRbType::BITMAP == type_; }

  uint64_t get_cardinality();
  uint64_t get_max();
  bool is_contains(uint64_t value);

  int value_add(uint64_t value);
  int value_remove(uint64_t value);
  int value_and(ObRoaringBitmap *rb);
  int value_or(ObRoaringBitmap *rb);
  int value_xor(ObRoaringBitmap *rb);
  int value_andnot(ObRoaringBitmap *rb);
  int value_calc(ObRoaringBitmap *rb, ObRbOperation op);

  int optimize();
  int deserialize(const ObString &rb_bin);
  int serialize(ObStringBuffer &res_rb_bin);

  inline void set_empty() {
    single_value_ = 0;
    if (set_.created()) {
      set_.destroy();
    }
    if (OB_NOT_NULL(bitmap_)) {
      roaring::api::roaring64_bitmap_free(bitmap_);
      bitmap_ = NULL;
    }
    type_ = ObRbType::EMPTY;
  }
  inline void set_single(uint64_t val) {
    single_value_ = val;
    if (set_.created()) {
      set_.destroy();
    }
    if (OB_NOT_NULL(bitmap_)) {
      roaring::api::roaring64_bitmap_free(bitmap_);
      bitmap_ = NULL;
    }
    type_ = ObRbType::SINGLE;
  }

  int convert_bitmap_to_smaller_type();
  int convert_to_bitmap();

private:
  ObIAllocator* allocator_;
  uint8_t version_;
  ObRbType type_;
  uint64_t single_value_;
  hash::ObHashSet<uint64_t> set_;
  roaring::api::roaring64_bitmap_t *bitmap_;

};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_ROARINGBITMAP_OB_ROARINGBITMAP_