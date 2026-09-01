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

#ifndef OCEANBASE_LIB_JSON_TYPE_OB_JSON_COMPARE
#define OCEANBASE_LIB_JSON_TYPE_OB_JSON_COMPARE
#include <stdint.h>

namespace oceanbase {
namespace common {

class ObIJsonBase;
class ObJsonBinView;
namespace number {
class ObNumber;
}

// Single source of truth for JSON value comparison shared by the DOM path
// (ObIJsonBase) and the binary view fast path (ObJsonBinView). The public
// overloads forward to one templated implementation; callers keep using
// ObIJsonBase::compare / ObJsonBinView::compare which are now 1-line shells.
class ObJsonCompare
{
public:
  // res: <0 if a<b, 0 if equal, >0 if a>b. is_path: path-mode semantics
  // (OBJECT and NULL-vs-non-NULL become CMP_ERROR).
  static int compare(const ObIJsonBase &a, const ObIJsonBase &b, int &res, bool is_path);
  static int compare(const ObJsonBinView &a, const ObJsonBinView &b, int &res, bool is_path);
  // Mixed representation: DOM/binary ObIJsonBase a vs binary-view b. Lets the
  // bin-view fast path compare directly against an ObIJsonBase without
  // materializing either side. b keeps the zero-virtual path; a keeps its
  // virtual dispatch. See compare_impl<TA, TB> for the per-side child access.
  static int compare(const ObIJsonBase &a, const ObJsonBinView &b, int &res, bool is_path);

  // Compare json decimal with uint64_t.
  static int compare_decimal_uint(const number::ObNumber &a, uint64_t b, int &res);

  // Compare int with json.
  static int compare_int_json(int a, ObIJsonBase* other, int& result);

  // Compare double with int.
  static int compare_double_int(double a, int64_t b, int &res);

  // Compare two numbers.
  template <class T>
  static inline int compare_numbers(T a, T b) {
    return a < b ? -1 : (a == b ? 0 : 1);
  }

  // Compare int64 with uint64_t.
  static int compare_int_uint(int64_t a, uint64_t b);

  // Compare json decimal with int64_t.
  static int compare_decimal_int(const number::ObNumber &a, int64_t b, int &res);

  // Compare json decimal with double.
  static int compare_decimal_double(const number::ObNumber &a, double b, int &res);
};

} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_LIB_JSON_TYPE_OB_JSON_COMPARE
