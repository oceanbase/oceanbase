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
  static int check_get_bin_type(const ObString &rb_bin, ObRbBinType &bin_type);
  static int get_cardinality(ObIAllocator &allocator, const ObString &rb_bin, ObRbBinType bin_type, uint64_t &cardinality);

  // common
  static void rb_destroy(ObRoaringBitmap *&rb);
  static int rb_deserialize(ObIAllocator &allocator, const ObString &rb_bin, ObRoaringBitmap *&rb);
  static int rb_serialize(ObIAllocator &allocator, ObString &res_rb_bin, ObRoaringBitmap *&rb);
  static int build_binary(ObIAllocator &allocator, ObString &rb_bin, ObString &res_rb_bin);
  static int binary_format_convert(ObIAllocator &allocator, const ObString &rb_bin, ObString &roaring_bin);
  static int rb_from_string(ObIAllocator &allocator, ObString &rb_str, ObRoaringBitmap *&rb);
  static int rb_to_string(ObIAllocator &allocator, ObString &rb_bin, ObString &res_rb_str);

  // calculate
  static int and_cardinality(ObRoaringBitmap *&rb1, ObRoaringBitmap *&rb2, uint64_t &cardinality);
  static int or_cardinality(ObRoaringBitmap *&rb1, ObRoaringBitmap *&rb2, uint64_t &cardinality);
  static int xor_cardinality(ObRoaringBitmap *&rb1, ObRoaringBitmap *&rb2, uint64_t &cardinality);
  static int andnot_cardinality(ObRoaringBitmap *&rb1, ObRoaringBitmap *&rb2, uint64_t &cardinality);
  static int calc_cardinality(ObRoaringBitmap *&rb1, ObRoaringBitmap *&rb2, uint64_t &cardinality, ObRbOperation op);
  static int rb_calc_equals(ObRoaringBitmap *&rb1, ObRoaringBitmap *&rb2, bool &result); //not impl

private:
  inline static void str_skip_space_(const char *&str, const char *end) {
    while (str < end && (*str == ' ' || *str == '\0')) {
      str++;
    }
    return;
  }
  static int str_read_value_(const char *str, size_t len, char *&value_end, uint64_t &value);

};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_ROARINGBITMAP_OB_ROARINGBITMAP_