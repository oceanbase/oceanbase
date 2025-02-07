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

#ifndef OCEANBASE_LIB_ROARINGBITMAP_OB_RB_UTILS_
#define OCEANBASE_LIB_ROARINGBITMAP_OB_RB_UTILS_

#include "ob_roaringbitmap.h"
#include "ob_rb_bin.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_string_buffer.h"

namespace oceanbase {
namespace common {
class ObRbUtils
{
public:
  // constructor
  ObRbUtils();
  virtual ~ObRbUtils() = default;

  // binary operation
  static int get_bin_type(const ObString &rb_bin, ObRbBinType &bin_type);
  static int check_binary(const ObString &rb_bin);
  static int build_empty_binary(ObIAllocator &allocator, ObString &res_rb_bin);
  static int to_roaring64_bin(ObIAllocator &allocator, ObRbBinType rb_type, ObString &rb_bin, ObString &roaring64_bin);
  static int get_cardinality(ObIAllocator &allocator, const ObString &rb_bin, uint64_t &cardinality);
  static int get_calc_cardinality(ObIAllocator &allocator, ObString &rb1_bin, ObString &rb2_bin, uint64_t &cardinality, ObRbOperation op);
  static int get_and_cardinality(ObIAllocator &allocator,
                                 ObString &rb1_bin,
                                 ObRbBinType rb1_type,
                                 ObString &rb2_bin,
                                 ObRbBinType rb2_type,
                                 uint64_t &cardinality,
                                 uint64_t &rb1_card,
                                 uint64_t &rb2_card);
  static int binary_calc(ObIAllocator &allocator, ObString &rb1_bin, ObString &rb2_bin, ObString &res_rb_bin, ObRbOperation op);
  static int calc_inplace(ObRoaringBitmap *&rb1, ObRoaringBitmap *&rb2, ObRbOperation op);

  // common
  static void rb_destroy(ObRoaringBitmap *&rb);
  static int rb_deserialize(ObIAllocator &allocator, const ObString &rb_bin, ObRoaringBitmap *&rb, bool need_validate = false);
  static int rb_serialize(ObIAllocator &allocator, ObString &res_rb_bin, ObRoaringBitmap *&rb);
  static int build_binary(ObIAllocator &allocator, ObString &rb_bin, ObString &res_rb_bin);
  static int convert_to_bitmap_binary(ObIAllocator &allocator, const ObString &rb_bin, ObString &bitmap_bin, ObRbBinType &bin_type);
  static int binary_format_convert(ObIAllocator &allocator, const ObString &rb_bin, ObString &roaring_bin);
  static int rb_from_string(ObIAllocator &allocator, ObString &rb_str, ObRoaringBitmap *&rb);
  static int rb_to_string(ObIAllocator &allocator, ObString &rb_bin, ObString &res_rb_str);

  // calculate
  static int rb_calc_equals(ObRoaringBitmap *&rb1, ObRoaringBitmap *&rb2, bool &result); //not impl

  // check
  static inline bool is_bitmap_bin(ObRbBinType bintype) {return ObRbBinType::BITMAP_32 == bintype || ObRbBinType::BITMAP_64 == bintype;}

private:
  inline static void str_skip_space_(const char *&str, const char *end) {
    while (str < end && (isspace(*str) || *str == '\0')) {
      str++;
    }
    return;
  }
  static int str_read_value_(const char *str, size_t len, char *&value_end, uint64_t &value);

};

struct ObRbAggCell
{
  // max memory cost 64KB = 8 * 8K
  static const uint64_t MAX_CACHED_COUNT = 8000;

  ObRbAggCell(ObRoaringBitmap *rb, const uint64_t tenant_id);

  int destroy();
  int reuse() { return destroy(); }
  int value_add(const uint64_t val);
  int add_values(const ObArray<uint64_t> &values);
  int value_or(const ObRbAggCell *other);
  int serialize(ObString &rb_bin);
  int serialize();

  common::ObArenaAllocator allocator_;
  ObRoaringBitmap *rb_;
  uint64_t max_cache_count_;
  common::ObArray<uint64_t> cached_value_;
  ObString rb_bin_;
  bool is_serialized_;

  TO_STRING_KV(KPC_(rb), K_(cached_value), K_(is_serialized), K_(rb_bin));
};

class ObRbAggAllocator
{
public:
  ObRbAggAllocator(const uint64_t tenant_id) :
    is_inited_(false),
    tenant_id_(tenant_id),
    rb_allocator_("RbAggAlloc", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id)
  {}

  ~ObRbAggAllocator()
  {
    reset();
  }

  int init();

  void reuse()
  {
    destroy_all_rb();
    alloced_rb_.reuse();
    rb_allocator_.reset_remain_one_page();
  }

  void reset()
  {
    destroy_all_rb();
    alloced_rb_.destroy();
    rb_allocator_.reset();
    is_inited_ = false;
  }

  ObRbAggCell *alloc();
  void free(ObRbAggCell *rb);
  int rb_serialize(ObString &rb_bin, ObRbAggCell *rb);
  int rb_serialize(ObRbAggCell *rb);

private:
  void destroy_all_rb();

private:
  bool is_inited_;
  uint64_t tenant_id_;
  hash::ObHashSet<uint64_t, hash::NoPthreadDefendMode> alloced_rb_;
  common::ObArenaAllocator rb_allocator_;

};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_ROARINGBITMAP_OB_RB_UTILS_