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

#define USING_LOG_PREFIX SQL

#include "ob_json_compare.h"
#include <type_traits>
#include "ob_json_base.h"
#include "ob_json_bin.h"
#include "ob_json_bin_view.h"
#include "lib/timezone/ob_time_convert.h"

namespace oceanbase {
namespace common {

const static int JSON_TYPE_NUM = static_cast<int>(ObJsonNodeType::J_MAX_TYPE) + 1;
// Semantic values stored in JSON_TYPE_COMPARISON and returned by the compare paths.
// Shared by the DOM compare path (ObIJsonBase::compare) and the binary view compare
// path (ObJsonBinView::compare) so both reference one definition instead of magic
// numbers. CMP_ERROR is the "incomparable in path mode" sentinel (e.g. OBJECT or
// NULL-vs-non-NULL under is_path); CMP_NOT_SUPPORT marks two types that cannot be
// compared and maps to OB_OP_NOT_ALLOW.
enum CMP_FUNC_TYPE {
  CMP_SMALLER = -1,
  CMP_LARGER = 1,
  CMP_FUNC = 0,
  CMP_NOT_SUPPORT = 2,
  CMP_ERROR = -3
};
// JSON type priority / comparability matrix, shared by the DOM compare path
// (ObIJsonBase::compare) and the binary view compare path (ObJsonBinView::compare).
// Keep both paths consistent by referencing this single definition.
// 0 means that the priority is the same, but it also means that types are directly comparable,
// i.e. decimal, int, uint, and double are all comparable.
// 1 means row type has a higher priority
// -1 means row type has a lower priority
// 2 means the two types are not comparable
const static int JSON_TYPE_COMPARISON[JSON_TYPE_NUM][JSON_TYPE_NUM] = {
  /*                     0   1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17  18  19  20  21  22  23  24  25  26  27  28  29   30   31  32*/
  /* 0  NULL */         {0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  -1,  -1,  2},
  /* 1  DECIMAL */      {1,  0,  0,  0,  0, -1, -1, -1, -1, -1, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  -1,  -1,  2},
  /* 2  INT */          {1,  0,  0,  0,  0, -1, -1, -1, -1, -1, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  -1,  -1,  2},
  /* 3  UINT */         {1,  0,  0,  0,  0, -1, -1, -1, -1, -1, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  -1,  -1,  2},
  /* 4  DOUBLE */       {1,  0,  0,  0,  0, -1, -1, -1, -1, -1, -1, -1, -1, -1,  2,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  -1,  -1,  2},
  /* 5  STRING */       {1,  1,  1,  1,  1,  0, -1, -1, -1, -1, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,  -1,  -1,  2},
  /* 6  OBJECT */       {1,  1,  1,  1,  1,  1,  0, -1, -1, -1, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  -1,  -1,  2},
  /* 7  ARRAY */        {1,  1,  1,  1,  1,  1,  1,  0, -1, -1, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  -1,  -1,  2},
  /* 8  BOOLEAN */      {1,  1,  1,  1,  1,  1,  1,  1,  0, -1, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  -1,  -1,  2},
  /* 9  DATE */         {1,  1,  1,  1,  1,  1,  1,  1,  1,  0, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  -1,  -1,  2},
  /* 10  TIME */        {1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,   1,  -1,  2},
  /* 11  DATETIME */    {1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0,  0, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,   1,   0,  2},
  /* 12  TIMESTAMP */   {1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0,  0, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  2,  2,  2,   1,   0,  2},
  /* 13  OPAQUE */      {1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,   1,   1,  2},
  /* 14  empty */       {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,   2,   2,  2},
  /*  ORACLE MODE */
  /* 15  OFLOAT */      {2,  2,  2,  2,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,   2,   2,  2},
  /* 16  ODOUBLE */     {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,   2,   2,  2},
  /* 17  ODECIMAL */    {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,   2,   2,  2},
  /* 18  OINT */        {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,   2,   2,  2},
  /* 19  OLONG */       {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,   2,   2,  2},
  /* 20  OBINARY */     {2,  2,  2,  2,  2,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,   2,   2,  2},
  /* 21  OOID */        {2,  2,  2,  2,  2,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,   2,   2,  2},
  /* 22  ORAWHEX */     {2,  2,  2,  2,  2,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,   2,   2,  2},
  /* 23  ORAWID */      {2,  2,  2,  2,  2,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  2,  2,  2,  2,  2,  2,   2,   2,  2},
  /* 24  ORACLEDATE*/   {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  2,  2,   2,   2,  2},
  /* 25  ODATE */       {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  2,  2,   2,   2,  2},
  /* 26  OTIMESTAMP */  {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  2,  2,   2,   2,  2},
  /* 27  TIMESTAMPTZ*/  {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  0,  0,  0,  2,  2,   2,   2,  2},
  /* 28  ODAYSECOND */  {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,  2,   2,   2,  2},
  /* 29  OYEARMONTH */  {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  0,   2,   2,  2},
  /* 30  MDATE */       {1,  1,  1,  1,  1,  1,  1,  1,  1,  1, -1, -1, -1, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,   0,  -1,  2},
  /* 31  MDATETIME */   {1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0,  0, -1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,   1,   0,  2},
  /* 32  MAX_OTYPE */   {2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,   2,   2,  2},
};

// 1 means this_type has a higher priority
// -1 means this_type has a lower priority
static int path_compare_string(const ObString &str_l, const ObString &str_r, int &res)
{
  int ret = OB_SUCCESS;
  res = 0;
  int l_len = str_l.length();
  int r_len = str_r.length();
  int i = 0;
  while (i < l_len && OB_SUCC(ret) && res == 0) {
    if (i < r_len) {
      if (str_l[i] < 0 && str_r[i] > 0) {
        res = 1;
      } else if (str_r[i] < 0 && str_l[i] > 0) {
        res = -1;
      } else if (str_l[i] < str_r[i]) {
        res = -1;
      } else if (str_l[i] > str_r[i]) {
        res = 1;
      } else {
        ++i;
      }
    } else {
      res = 1;
    }
  }
  if (OB_SUCC(ret) && res == 0 && i == l_len) {
    if (i < r_len) {
      res = -1;
    }
  }
  return ret;
}

template <typename TA, typename TB>
static int compare_impl(const TA &a, const TB &b, int &res, bool is_path);

// Compare double with uint64_t. Only used by compare_numeric_impl, so kept as a
// file-local free function instead of an ObJsonCompare member to keep it out of
// the public header.
static int compare_double_uint(double a, uint64_t b, int &res);

// Numeric category for compare_numeric_impl dispatch. Each side's category is
// packed into 2 bits (a in the high bits, b in the low bits) to form a 4-bit
// switch index over the 4 x 4 numeric type combinations, mirroring the
// GET_FORMAT_CONDITION bit-encoding pattern in ob_batch_eval_util.h.
// JN_NON_NUMERIC (= 4) marks any non-numeric type.
#define JN_INT         0
#define JN_UINT        1
#define JN_DOUBLE      2
#define JN_DECIMAL     3
#define JN_NON_NUMERIC 4
#define NUMERIC_COND(A, B) (((A) << 2) | (B))
constexpr int json_numeric_category(ObJsonNodeType t)
{
  int cat = JN_NON_NUMERIC;
  switch (t) {
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_OINT:      cat = JN_INT;     break;
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OLONG:     cat = JN_UINT;    break;
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_ODOUBLE:
    case ObJsonNodeType::J_OFLOAT:    cat = JN_DOUBLE;  break;
    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_ODECIMAL:  cat = JN_DECIMAL; break;
    default:                          cat = JN_NON_NUMERIC; break;
  }
  return cat;
}

template <typename TA, typename TB>
static int compare_numeric_impl(const TA &a, const TB &b, int &result)
{
  int ret = OB_SUCCESS;
  ObJsonNodeType ta = a.json_type();
  ObJsonNodeType tb = b.json_type();
  int a_cat = json_numeric_category(ta);
  int b_cat = json_numeric_category(tb);

  if (OB_UNLIKELY(a_cat > JN_DECIMAL || b_cat > JN_DECIMAL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected numeric type combination", K(ret), K(ta), K(tb));
  } else {
    switch (NUMERIC_COND(a_cat, b_cat)) {
      case NUMERIC_COND(JN_INT, JN_INT): {
        result = ObJsonCompare::compare_numbers(a.get_int(), b.get_int());
        break;
      }
      case NUMERIC_COND(JN_INT, JN_UINT): {
        result = ObJsonCompare::compare_int_uint(a.get_int(), b.get_uint());
        break;
      }
      case NUMERIC_COND(JN_INT, JN_DOUBLE): {
        double db = (tb == ObJsonNodeType::J_OFLOAT) ? static_cast<double>(b.get_float()) : b.get_double();
        if (OB_FAIL(ObJsonCompare::compare_double_int(db, a.get_int(), result))) {
          LOG_WARN("compare_double_int fail", K(ret));
        } else {
          result = -result;
        }
        break;
      }
      case NUMERIC_COND(JN_INT, JN_DECIMAL): {
        number::ObNumber nmb_b = b.get_decimal_data();
        if (OB_FAIL(ObJsonCompare::compare_decimal_int(nmb_b, a.get_int(), result))) {
          LOG_WARN("compare_decimal_int fail", K(ret));
        } else {
          result = -result;
        }
        break;
      }
      case NUMERIC_COND(JN_UINT, JN_INT): {
        result = -ObJsonCompare::compare_int_uint(b.get_int(), a.get_uint());
        break;
      }
      case NUMERIC_COND(JN_UINT, JN_UINT): {
        result = ObJsonCompare::compare_numbers(a.get_uint(), b.get_uint());
        break;
      }
      case NUMERIC_COND(JN_UINT, JN_DOUBLE): {
        double db = (tb == ObJsonNodeType::J_OFLOAT) ? static_cast<double>(b.get_float()) : b.get_double();
        if (OB_FAIL(compare_double_uint(db, a.get_uint(), result))) {
          LOG_WARN("compare_double_uint fail", K(ret));
        } else {
          result = -result;
        }
        break;
      }
      case NUMERIC_COND(JN_UINT, JN_DECIMAL): {
        number::ObNumber nmb_b = b.get_decimal_data();
        if (OB_FAIL(ObJsonCompare::compare_decimal_uint(nmb_b, a.get_uint(), result))) {
          LOG_WARN("compare_decimal_uint fail", K(ret));
        } else {
          result = -result;
        }
        break;
      }
      case NUMERIC_COND(JN_DOUBLE, JN_INT): {
        double da = (ta == ObJsonNodeType::J_OFLOAT) ? static_cast<double>(a.get_float()) : a.get_double();
        if (OB_FAIL(ObJsonCompare::compare_double_int(da, b.get_int(), result))) {
          LOG_WARN("compare_double_int fail", K(ret));
        }
        break;
      }
      case NUMERIC_COND(JN_DOUBLE, JN_UINT): {
        double da = (ta == ObJsonNodeType::J_OFLOAT) ? static_cast<double>(a.get_float()) : a.get_double();
        if (OB_FAIL(compare_double_uint(da, b.get_uint(), result))) {
          LOG_WARN("compare_double_uint fail", K(ret));
        }
        break;
      }
      case NUMERIC_COND(JN_DOUBLE, JN_DOUBLE): {
        double da = (ta == ObJsonNodeType::J_OFLOAT) ? static_cast<double>(a.get_float()) : a.get_double();
        double db = (tb == ObJsonNodeType::J_OFLOAT) ? static_cast<double>(b.get_float()) : b.get_double();
        if (ta == ObJsonNodeType::J_OFLOAT && tb != ObJsonNodeType::J_OFLOAT) {
          db = static_cast<double>(static_cast<float>(db));
        } else if (ta != ObJsonNodeType::J_OFLOAT && tb == ObJsonNodeType::J_OFLOAT) {
          da = static_cast<double>(static_cast<float>(da));
        }
        result = ObJsonCompare::compare_numbers(da, db);
        break;
      }
      case NUMERIC_COND(JN_DOUBLE, JN_DECIMAL): {
        number::ObNumber nmb_b = b.get_decimal_data();
        double da = (ta == ObJsonNodeType::J_OFLOAT) ? static_cast<double>(a.get_float()) : a.get_double();
        if (OB_FAIL(ObJsonCompare::compare_decimal_double(nmb_b, da, result))) {
          LOG_WARN("compare_decimal_double fail", K(ret));
        } else {
          result = -result;
        }
        break;
      }
      case NUMERIC_COND(JN_DECIMAL, JN_INT): {
        number::ObNumber nmb_a = a.get_decimal_data();
        if (OB_FAIL(ObJsonCompare::compare_decimal_int(nmb_a, b.get_int(), result))) {
          LOG_WARN("compare_decimal_int fail", K(ret));
        }
        break;
      }
      case NUMERIC_COND(JN_DECIMAL, JN_UINT): {
        number::ObNumber nmb_a = a.get_decimal_data();
        if (OB_FAIL(ObJsonCompare::compare_decimal_uint(nmb_a, b.get_uint(), result))) {
          LOG_WARN("compare_decimal_uint fail", K(ret));
        }
        break;
      }
      case NUMERIC_COND(JN_DECIMAL, JN_DOUBLE): {
        number::ObNumber nmb_a = a.get_decimal_data();
        double db = (tb == ObJsonNodeType::J_OFLOAT) ? static_cast<double>(b.get_float()) : b.get_double();
        if (OB_FAIL(ObJsonCompare::compare_decimal_double(nmb_a, db, result))) {
          LOG_WARN("compare_decimal_double fail", K(ret));
        }
        break;
      }
      case NUMERIC_COND(JN_DECIMAL, JN_DECIMAL): {
        number::ObNumber nmb_a = a.get_decimal_data();
        number::ObNumber nmb_b = b.get_decimal_data();
        if (nmb_a.is_zero() && nmb_b.is_zero()) {
          result = 0;
        } else {
          result = nmb_a.compare(nmb_b);
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected numeric type combination", K(ret), K(ta), K(tb));
        break;
      }
    }
  }
  return ret;
}

template <typename T>
static int get_node_string(const T &node, ObString &s)
{
  int ret = OB_SUCCESS;
  if constexpr (std::is_same_v<T, ObJsonBinView>) {
    s = node.get_string();
  } else {
    s = ObString(node.get_data_length(), node.get_data());
  }
  return ret;
}

template <typename TA, typename TB>
static int compare_datetime_impl(const TA &a, const TB &b, int &result)
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = a.json_type();
  const ObJsonNodeType j_type_b = b.json_type();
  ObDTMode dt_mode_a;
  ObDTMode dt_mode_b;
  ObTime t_a;
  ObTime t_b;
  if (OB_UNLIKELY(!ObJsonBaseUtil::is_time_type(j_type_a) || !ObJsonBaseUtil::is_time_type(j_type_b))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-time json type in compare_datetime_impl", K(ret), K(j_type_a), K(j_type_b));
  } else if (OB_FAIL(ObJsonBaseUtil::get_dt_mode_by_json_type(j_type_a, dt_mode_a))) {
    LOG_WARN("fail to get dt mode by json type", K(ret), K(j_type_a));
  } else if (OB_FAIL(ObJsonBaseUtil::get_dt_mode_by_json_type(j_type_b, dt_mode_b))) {
    LOG_WARN("fail to get dt mode by json type", K(ret), K(j_type_b));
  } else if (OB_FAIL(a.get_obtime(t_a))) {
    LOG_WARN("fail to decode obtime a", K(ret), K(j_type_a));
  } else if (OB_FAIL(b.get_obtime(t_b))) {
    LOG_WARN("fail to decode obtime b", K(ret), K(j_type_b));
  } else {
    int64_t int_a = ObTimeConverter::ob_time_to_int(t_a, dt_mode_a);
    int64_t int_b = ObTimeConverter::ob_time_to_int(t_b, dt_mode_b);
    result = ObJsonCompare::compare_numbers(int_a, int_b);
  }
  return ret;
}

// 1. If the arrays are equal in length and each array element is equal, then the two arrays are equal.
// 2. If you run into the first element of an unequal array,
//    the smaller element will have a smaller array, and in this example, a is smaller.
//    a[0, 1, 2, 3] vs b[0, 1, 3, 1, 1, 1, 1] ----> a < b
// 3. If the array is not equal, and the smaller array is equal to the larger array element by element,
//    then the smaller array is smaller, and example a is smaller.
//    a[0, 1, 2] vs b[0, 1, 2, 1, 1, 1, 1] ----> a < b
template <typename TA, typename TB>
static int compare_array_impl(const TA &a, const TB &b, int &res, bool is_path)
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = a.json_type();
  const ObJsonNodeType j_type_b = b.json_type();
  if (OB_UNLIKELY(j_type_a != ObJsonNodeType::J_ARRAY || j_type_b != ObJsonNodeType::J_ARRAY)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-array json type in compare_array_impl", K(ret), K(j_type_a), K(j_type_b));
  } else {
    uint32_t cnt_a = a.element_count();
    uint32_t cnt_b = b.element_count();
    uint32_t min_cnt = (cnt_a < cnt_b) ? cnt_a : cnt_b;
    for (uint32_t i = 0; OB_SUCC(ret) && res == 0 && i < min_cnt; ++i) {
      if constexpr (std::is_same_v<TA, ObJsonBinView>) {
        ObJsonBinView ea;
        if (OB_FAIL(a.element(i, ea))) {
          LOG_WARN("get array element a fail", K(ret), K(i));
        } else {
          if constexpr (std::is_same_v<TB, ObJsonBinView>) {
            ObJsonBinView eb;
            if (OB_FAIL(b.element(i, eb))) {
              LOG_WARN("get array element b fail", K(ret), K(i));
            } else if (OB_FAIL(compare_impl(ea, eb, res, is_path))) {
              LOG_WARN("compare array elements fail", K(ret), K(i));
            }
          } else {
            ObJsonBin j_bin_b(b.get_allocator());
            ObIJsonBase *pb = &j_bin_b;
            if (OB_FAIL(b.get_array_element(i, pb))) {
              LOG_WARN("get array element b fail", K(ret), K(i));
            } else if (OB_FAIL(compare_impl(ea, *pb, res, is_path))) {
              LOG_WARN("compare array elements fail", K(ret), K(i));
            }
          }
        }
      } else {
        ObJsonBin j_bin_a(a.get_allocator());
        ObIJsonBase *pa = &j_bin_a;
        if (OB_FAIL(a.get_array_element(i, pa))) {
          LOG_WARN("get array element a fail", K(ret), K(i));
        } else {
          if constexpr (std::is_same_v<TB, ObJsonBinView>) {
            ObJsonBinView eb;
            if (OB_FAIL(b.element(i, eb))) {
              LOG_WARN("get array element b fail", K(ret), K(i));
            } else if (OB_FAIL(compare_impl(*pa, eb, res, is_path))) {
              LOG_WARN("compare array elements fail", K(ret), K(i));
            }
          } else {
            ObJsonBin j_bin_b(b.get_allocator());
            ObIJsonBase *pb = &j_bin_b;
            if (OB_FAIL(b.get_array_element(i, pb))) {
              LOG_WARN("get array element b fail", K(ret), K(i));
            } else if (OB_FAIL(compare_impl(*pa, *pb, res, is_path))) {
              LOG_WARN("compare array elements fail", K(ret), K(i));
            }
          }
        }
      }
    }
    // Compare the array length if all the comparisons are equal.
    if (OB_SUCC(ret) && res == 0) {
      res = ObJsonCompare::compare_numbers(cnt_a, cnt_b);
    }
  }
  return ret;
}

// 1. Two objects are equal if their key-value number are equal and their keys are equal and their values are equal.
// 2. If the key-value number of two objects are not equal, then objects with fewer key-value pairs are smaller
// 3. Compare each key-value pair and return the result of the first unequal encounter.
template <typename TA, typename TB>
static int compare_object_impl(const TA &a, const TB &b, int &res, bool is_path)
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = a.json_type();
  const ObJsonNodeType j_type_b = b.json_type();
  if (OB_UNLIKELY(j_type_a != ObJsonNodeType::J_OBJECT || j_type_b != ObJsonNodeType::J_OBJECT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-object json type in compare_object_impl", K(ret), K(j_type_a), K(j_type_b));
  } else {
    uint32_t cnt_a = a.element_count();
    uint32_t cnt_b = b.element_count();
    res = ObJsonCompare::compare_numbers(cnt_a, cnt_b);
    for (uint32_t i = 0; OB_SUCC(ret) && res == 0 && i < cnt_a; ++i) {
      ObString key_a, key_b;
      if (OB_FAIL(a.get_key(i, key_a))) {
        LOG_WARN("get_key_value a fail", K(ret), K(i));
      } else if (OB_FAIL(b.get_key(i, key_b))) {
        LOG_WARN("get_key_value b fail", K(ret), K(i));
      } else if (FALSE_IT(res = key_a.compare(key_b))) {
      } else if (res != 0) {
        // keys differ, result decided
      } else {
        if constexpr (std::is_same_v<TA, ObJsonBinView>) {
          ObJsonBinView val_a;
          if (OB_FAIL(a.get_value(i, val_a))) {
            LOG_WARN("get object value a fail", K(ret), K(i));
          } else {
            if constexpr (std::is_same_v<TB, ObJsonBinView>) {
              ObJsonBinView val_b;
              if (OB_FAIL(b.get_value(i, val_b))) {
                LOG_WARN("get object value b fail", K(ret), K(i));
              } else if (OB_FAIL(compare_impl(val_a, val_b, res, is_path))) {
                LOG_WARN("compare object values fail", K(ret), K(i));
              }
            } else {
              ObJsonBin j_bin_b(b.get_allocator());
              ObIJsonBase *pb = &j_bin_b;
              if (OB_FAIL(b.get_object_value(i, pb))) {
                LOG_WARN("get object value b fail", K(ret), K(i));
              } else if (OB_FAIL(compare_impl(val_a, *pb, res, is_path))) {
                LOG_WARN("compare object values fail", K(ret), K(i));
              }
            }
          }
        } else {
          ObJsonBin j_bin_a(a.get_allocator());
          ObIJsonBase *pa = &j_bin_a;
          if (OB_FAIL(a.get_object_value(i, pa))) {
            LOG_WARN("get object value a fail", K(ret), K(i));
          } else {
            if constexpr (std::is_same_v<TB, ObJsonBinView>) {
              ObJsonBinView val_b;
              if (OB_FAIL(b.get_value(i, val_b))) {
                LOG_WARN("get object value b fail", K(ret), K(i));
              } else if (OB_FAIL(compare_impl(*pa, val_b, res, is_path))) {
                LOG_WARN("compare object values fail", K(ret), K(i));
              }
            } else {
              ObJsonBin j_bin_b(b.get_allocator());
              ObIJsonBase *pb = &j_bin_b;
              if (OB_FAIL(b.get_object_value(i, pb))) {
                LOG_WARN("get object value b fail", K(ret), K(i));
              } else if (OB_FAIL(compare_impl(*pa, *pb, res, is_path))) {
                LOG_WARN("compare object values fail", K(ret), K(i));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

template <typename TA, typename TB>
static int compare_impl(const TA &a, const TB &b, int &res, bool is_path)
{
  INIT_SUCC(ret);
  const ObJsonNodeType j_type_a = a.json_type();
  const ObJsonNodeType j_type_b = b.json_type();
  res = 0;

  int idx_a = static_cast<int>(j_type_a);
  int idx_b = static_cast<int>(j_type_b);
  if (OB_UNLIKELY(j_type_a == ObJsonNodeType::J_ERROR ||
                  j_type_b == ObJsonNodeType::J_ERROR)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json type", K(ret), K(idx_a), K(idx_b));
  } else if (is_path
             && (j_type_a == ObJsonNodeType::J_OBJECT || j_type_b == ObJsonNodeType::J_OBJECT
             || ((j_type_a == ObJsonNodeType::J_NULL || j_type_b == ObJsonNodeType::J_NULL)
                 && j_type_a != j_type_b))) {
    res = CMP_ERROR;
  } else {
    // Compare the matrix to get which json type has a higher priority, and return the result if the priority is different.
    int type_cmp = JSON_TYPE_COMPARISON[idx_a][idx_b];
    if (type_cmp != 0) {  // Different priorities, complete comparison.
      res = type_cmp;
      if (static_cast<CMP_FUNC_TYPE>(type_cmp) == CMP_NOT_SUPPORT) {
        ret = OB_OP_NOT_ALLOW;
      }
    } else {  // Same priority.
      switch (j_type_a) {
        case ObJsonNodeType::J_NULL: {
          res = 0;
          break;
        }
        case ObJsonNodeType::J_BOOLEAN: {
          res = ObJsonCompare::compare_numbers(static_cast<int>(a.get_boolean()),
                                               static_cast<int>(b.get_boolean()));
          break;
        }
        case ObJsonNodeType::J_INT:
        case ObJsonNodeType::J_OINT:
        case ObJsonNodeType::J_UINT:
        case ObJsonNodeType::J_OLONG:
        case ObJsonNodeType::J_DOUBLE:
        case ObJsonNodeType::J_ODOUBLE:
        case ObJsonNodeType::J_OFLOAT:
        case ObJsonNodeType::J_DECIMAL:
        case ObJsonNodeType::J_ODECIMAL: {
          if (OB_FAIL(compare_numeric_impl(a, b, res))) {
            LOG_WARN("compare numeric fail", K(ret), K(j_type_a), K(j_type_b));
          }
          break;
        }
        case ObJsonNodeType::J_STRING:
        case ObJsonNodeType::J_OBINARY:
        case ObJsonNodeType::J_OOID:
        case ObJsonNodeType::J_ORAWHEX:
        case ObJsonNodeType::J_ORAWID:
        case ObJsonNodeType::J_ODAYSECOND:
        case ObJsonNodeType::J_OYEARMONTH: {
          ObString str_a, str_b;
          if (OB_FAIL(get_node_string(a, str_a))) {
            LOG_WARN("get string a fail", K(ret));
          } else if (OB_FAIL(get_node_string(b, str_b))) {
            LOG_WARN("get string b fail", K(ret));
          } else if (lib::is_oracle_mode() && is_path) {
            if (OB_FAIL(path_compare_string(str_a, str_b, res))) {
              LOG_WARN("path compare string fail", K(ret));
            }
          } else {
            res = str_a.compare(str_b);
          }
          break;
        }
        case ObJsonNodeType::J_OPAQUE: {
          res = ObJsonCompare::compare_numbers(static_cast<int>(a.field_type()),
                                               static_cast<int>(b.field_type()));
          if (res == 0) {
            ObString str_a, str_b;
            if (OB_FAIL(get_node_string(a, str_a))) {
              LOG_WARN("get opaque string a fail", K(ret));
            } else if (OB_FAIL(get_node_string(b, str_b))) {
              LOG_WARN("get opaque string b fail", K(ret));
            } else {
              res = str_a.compare(str_b);
            }
          }
          break;
        }
        case ObJsonNodeType::J_DATE:
        case ObJsonNodeType::J_MYSQL_DATE:
        case ObJsonNodeType::J_ORACLEDATE:
        case ObJsonNodeType::J_TIME:
        case ObJsonNodeType::J_DATETIME:
        case ObJsonNodeType::J_MYSQL_DATETIME:
        case ObJsonNodeType::J_ODATE:
        case ObJsonNodeType::J_OTIMESTAMP:
        case ObJsonNodeType::J_OTIMESTAMPTZ:
        case ObJsonNodeType::J_TIMESTAMP: {
          if (OB_FAIL(compare_datetime_impl(a, b, res))) {
            LOG_WARN("compare datetime fail", K(ret), K(j_type_a), K(j_type_b));
          }
          break;
        }
        case ObJsonNodeType::J_ARRAY:
        case ObJsonNodeType::J_SEMI_HETE_COL: {
          if (OB_FAIL(compare_array_impl(a, b, res, is_path))) {
            LOG_WARN("compare array fail", K(ret));
          }
          break;
        }
        case ObJsonNodeType::J_OBJECT: {
          if (OB_FAIL(compare_object_impl(a, b, res, is_path))) {
            LOG_WARN("compare object fail", K(ret));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected json type in compare", K(ret), K(j_type_a));
          break;
        }
      }
    }
  }
  return ret;
}

int ObJsonCompare::compare(const ObIJsonBase &a, const ObIJsonBase &b, int &res, bool is_path)
{
  return compare_impl(a, b, res, is_path);
}

int ObJsonCompare::compare(const ObJsonBinView &a, const ObJsonBinView &b, int &res, bool is_path)
{
  return compare_impl(a, b, res, is_path);
}

int ObJsonCompare::compare(const ObIJsonBase &a, const ObJsonBinView &b, int &res, bool is_path)
{
  return compare_impl(a, b, res, is_path);
}

int ObJsonCompare::compare_int_uint(int64_t a, uint64_t b)
{
  int res = 0;
  if (a < 0) {
    res = -1;
  } else {
    res = compare_numbers(static_cast<uint64_t>(a), b);
  }
  return res;
}

int ObJsonCompare::compare_decimal_uint(const number::ObNumber &a, uint64_t b, int &res)
{
  INIT_SUCC(ret);

  const bool a_is_zero = a.is_zero();
  const bool a_is_negative = a.is_negative();
  const bool b_is_zero = (b == 0);

  if (a_is_zero) {  // So if A is 0, let's deal with both cases where B is 0 or positive.
    res = b_is_zero ? 0 : -1;
  } else if (a_is_negative) {  // A is negative, so it's definitely less than B.
    res = -1;
  } else {  // If A is positive, let's deal with both cases where B is 0 or positive.
    if (b_is_zero) {
      res = 1;
    } else {  // A and b are both positive.
      const int64_t MAX_BUF_SIZE = 256;
      char buf_alloc[MAX_BUF_SIZE];
      ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
      number::ObNumber b_num;

      if (OB_FAIL(b_num.from(b, allocator))) {
        LOG_WARN("fail to cast number from b", K(ret), K(a), K(b));
      } else {
        res = a.compare(b_num);
      }
    }
  }

  return ret;
}

int ObJsonCompare::compare_decimal_int(const number::ObNumber &a, int64_t b, int &res)
{
  INIT_SUCC(ret);

  const bool a_is_zero = a.is_zero();
  const bool a_is_negative = a.is_negative();
  const bool b_is_negative = (b < 0);
  const bool b_is_zero = (b == 0);

  if (a_is_negative != b_is_negative) {  // The two signs are different. Negative numbers are smaller.
    res = a_is_negative ? -1 : 1;
  } else if (a_is_zero) {  // If a is 0, b must be either 0 or positive, otherwise the first if statement is entered.
    res = b_is_zero ? 0 : -1;
  } else if (b_is_zero) {  // If b is 0, then a can only be 0 or positive, and the second if already rules out a being 0, so a is now positive.
    res = 1;
  } else {  // Both a and B are positive or negative.
    const int64_t MAX_BUF_SIZE = 256;
    char buf_alloc[MAX_BUF_SIZE];
    ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
    number::ObNumber b_num;

    if (OB_FAIL(b_num.from(b, allocator))) {
      LOG_WARN("fail to cast number from b", K(ret), K(a), K(b));
    } else {
      res = a.compare(b_num);
    }
  }

  return ret;
}

int ObJsonCompare::compare_decimal_double(const number::ObNumber &a, double b, int &res)
{
  INIT_SUCC(ret);

  const bool a_is_zero = a.is_zero();
  const bool a_is_negative = a.is_negative();
  const bool b_is_negative = (b < 0);
  const bool b_is_zero = (b == 0);

  if (a_is_negative != b_is_negative) {  // The two signs are different. Negative numbers are smaller.
    res = a_is_negative ? -1 : 1;
  } else if (a_is_zero) {  // If a is 0, b must be either 0 or positive, otherwise the first if statement is entered.
    res = b_is_zero ? 0 : -1;
  } else if (b_is_zero) {  // If b is 0, then a can only be 0 or positive, and the second if already rules out a being 0, so a is now positive.
    res = 1;
  } else {  // Both a and B are positive or negative.
    const int64_t MAX_BUF_SIZE = 256;
    char buf_alloc[MAX_BUF_SIZE];
    ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
    number::ObNumber b_num;
    if (OB_FAIL(ObJsonBaseUtil::double_to_number(b, allocator, b_num))) {
      if (ret == OB_NUMERIC_OVERFLOW) {
        res = a_is_negative ? 1 : -1;  // They're both negative numbers. The larger the number, the smaller the number.
      } else {  // Conversion error.
        LOG_WARN("fail to cast double to number", K(ret), K(b));
      }
    } else {
      res = a.compare(b_num);
    }
  }

  return ret;
}

int ObJsonCompare::compare_double_int(double a, int64_t b, int &res)
{
  INIT_SUCC(ret);

  double b_double = static_cast<double>(b);
  if (a < b_double) {
    res = -1;
  } else if (a > b_double) {
    res = 1;
  } else {
    /*
      The two numbers were equal when compared as double. Since
      conversion from int64_t to double isn't lossless, they could
      still be different. Convert to decimal to compare their exact
      values.
    */
    const int64_t MAX_BUF_SIZE = 256;
    char buf_alloc[MAX_BUF_SIZE];
    ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
    number::ObNumber num_b;
    if (OB_FAIL(num_b.from(b, allocator))) {
      LOG_WARN("fail to cast number from b", K(ret), K(b));
    } else if (OB_FAIL(compare_decimal_double(num_b, a, res))) {
      LOG_WARN("fail to compare json decimal with double", K(num_b), K(a), K(b));
    } else {
      res = -1 * res;
    }
  }

  return ret;
}

int ObJsonCompare::compare_int_json(int a, ObIJsonBase* other, int& result)
{
  INIT_SUCC(ret);
  if (other->json_type() != ObJsonNodeType::J_INT) {
    result = 1;
  } else {
    int64_t value = other->get_int();
    result = (a == value) ? 0 : (a > value ? 1 : -1);
  }
  return ret;
}

static int compare_double_uint(double a, uint64_t b, int &res)
{
  INIT_SUCC(ret);

  double b_double = static_cast<double>(b);
  if (a < b_double) {
    res = -1;
  } else if (a > b_double) {
    res = 1;
  } else {
    /*
      The two numbers were equal when compared as double. Since
      conversion from uint64_t to double isn't lossless, they could
      still be different. Convert to decimal to compare their exact
      values.
    */
    const int64_t MAX_BUF_SIZE = 256;
    char buf_alloc[MAX_BUF_SIZE];
    ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
    number::ObNumber num_b;
    if (OB_FAIL(num_b.from(b, allocator))) {
      LOG_WARN("fail to cast number from b", K(ret), K(b));
    } else if (OB_FAIL(ObJsonCompare::compare_decimal_double(num_b, a, res))) {
      LOG_WARN("fail to compare json decimal with double", K(num_b), K(a), K(b));
    } else {
      res = -1 * res;
    }
  }

  return ret;
}

} // namespace common
} // namespace oceanbase
