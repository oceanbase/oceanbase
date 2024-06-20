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

#ifndef OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_UTILS_
#define OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_UTILS_
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace sql
{
class ObLogPlan;
class ObSQLSessionInfo;
class ObOptimizerContext;
struct RangeExprs
{
  RangeExprs()
      : column_expr_(NULL),
        range_exprs_()
  { }
  TO_STRING_KV(K_(column_expr), K_(range_exprs));
  ObColumnRefRawExpr *column_expr_;
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> range_exprs_;
};

class ObOptEstUtils
{
public:
  //check if op is monotonic, some type may not considered.
  static bool is_monotonic_op(const ObItemType type);

  //@brief expr is cmp_op , has is_range_cond flag or is simple op_and,op_or
  //such as, c1 > 1 and c1 < 2, c1 between 1 and 1000, c1 > 100 or c1 < 10000
  //@param in qual
  //@param out is_range
  static int is_range_expr(const ObRawExpr *qual, bool &is_simple_filter, const int64_t level = 0);

  //extract column exprs with simple operator check.
  //level must be initialized with 0(default value)
  static int extract_column_exprs_with_op_check(const ObRawExpr *raw_expr,
                                                common::ObIArray<const ObColumnRefRawExpr*> &column_exprs,
                                                bool &only_monotonic_op,
                                                const int64_t level = 0);

  //@brief according different column, generate one or more range_exprs, push them into column_exprs_array.
  //@brief the item in range_exprs mush be is_range_expr() -> see notes below.
  //@param qual the expr need be dealed with
  //@param out can_be_extracted, if the expr can be extracted range here
  //@param out column_exprs_array, if generate new range_expr, pushed it into this array
  static int extract_simple_cond_filters(ObRawExpr &qual,
                                         bool &can_be_extracted,
                                         common::ObIArray<RangeExprs> &column_exprs_array);

  //is_calculable_expr:is PARAM with param idx[0, param_count) or const value, or calculable
  static bool is_calculable_expr(const ObRawExpr &expr, const int64_t param_count);

  //get the expr's value.
  //@@param get_value[out]:is_calculable_expr:true, otherwise:false
  //@@param value[out]:the value of calculable expr
  static int get_expr_value(const ParamStore *params,
                            const ObRawExpr &expr,
                            ObExecContext *exec_ctx,
                            common::ObIAllocator &allocator,
                            bool &get_value,
                            common::ObObj &value);

  //whether the value of calculable expr is null.
  static int if_expr_value_null(const ParamStore *params,
                                const ObRawExpr &expr,
                                ObExecContext *exec_ctx,
                                common::ObIAllocator &allocator,
                                bool &is_null);

  // whether the like condition's patten start with '%' or '_' and not start with escape sign
  static int if_expr_start_with_patten_sign(const ParamStore *params,
                                            const ObRawExpr *expr,
                                            const ObRawExpr *esp_expr,
                                            ObExecContext *exec_ctx,
                                            ObIAllocator &allocator,
                                            bool &is_start_with,
                                            bool &all_is_percent_sign);


  //whether the value of first_expr and second_expr is equal.
  //if someone of the exprs is not calculable, equal return false;
  //@@param null_safe, if think NULL equal NULL, null_safe should be true, otherwise false.
  static int if_expr_value_equal(ObOptimizerContext &context,
                                 const ObDMLStmt *stmt,
                                 const ObRawExpr &first_expr,
                                 const ObRawExpr &second_expr,
                                 const bool null_safe,
                                 bool &equal);

  static int columns_has_unique_subset(const ObIArray<uint64_t> &full,
                                       const ObRowkeyInfo &sub,
                                       bool &is_subset);

};

class ObOptEstObjToScalar
{
public:
  // Functionality: convert objs to scalars(double).
  // One Optimization : Limited precision of double type cannot distinguish long strs using
  // a static base of 256 (max distinguishable length is 6). We need to firstly truncate the
  // common headers, then use a dynamic base to convert strings.
  //
  // Choice of dynamic base : find min and max of all chars of all given strs beyond the already
  // found common prefix length, use (max - min) as base
  //
  // When to use this optimization : all given objs (2 to 4, min and max may be NULL) are string
  // or min or max.
  //
  // If this optimization is not used, use old method to convert objs.
  // dongyun.zdy
  static int convert_objs_to_scalars(const common::ObObj *min,
                                     const common::ObObj *max,
                                     const common::ObObj *start,
                                     const common::ObObj *end,
                                     common::ObObj *min_out,
                                     common::ObObj *max_out,
                                     common::ObObj *start_out,
                                     common::ObObj *end_out,
                                     bool convert2sortkey = true);
  /////////////Start convert obj to scalar related function//////
  // @param obj
  // @return
  static int convert_obj_to_scalar(const common::ObObj* obj, double &scalar_value);


  // double type cannot represent min (max can be represented using Double.INF).
  // wrap conversion result as obj, so it can represent min, max, etc.
  static int convert_obj_to_scalar_obj(const common::ObObj* obj, common::ObObj* out);

  static int convert_obj_to_double(const common::ObObj *obj, double &num);

  static int convert_string_to_scalar_for_number(const common::ObString &str, double &scala);
private:
  static int add_to_string_conversion_array(const common::ObObj &strobj,
                                            common::ObIArray<ObCollationType> &cs_type,
                                            common::ObIArray<common::ObString> &arr,
                                            uint64_t &convertable_map,
                                            int64_t pos);
  // 1, find common prefix length of strings
  // 2, find dynamic base and offset of strings
  // 3, use dynamic base and offset to convert strings to scalars
  static int convert_strings_to_scalar(const common::ObIArray<ObCollationType> &cs_type,
                                       const common::ObIArray<common::ObString> &origin_strs,
                                       common::ObIArray<double> &scalars);

  static double convert_string_to_scalar(const common::ObString &str,
                                         int64_t prefix_len = 0,
                                         uint8_t offset = 0,
                                         double base = 256.0);

  static int get_string_sort_key(ObIAllocator &alloc, ObCollationType cs_type, const common::ObString &str, common::ObString &sort_key);

  static int convert_string_to_scalar(ObCollationType cs_type, const common::ObString &str, double &scalar);

  static int find_common_prefix_len(const common::ObIArray<common::ObString> &strs,
                                    int64_t &length);
  static inline void expand_range(uint8_t &min, uint8_t &max, uint8_t rmin, uint8_t rmax)
  {
    if (rmin < min) {
      min = rmin;
    }
    if (rmax > max) {
      max = rmax;
    }
  }

  static int find_string_scalar_offset_base(const common::ObIArray<common::ObString> &strs,
                                            int64_t prefix_len,
                                            uint8_t &offset,
                                            double &base);
};
}
}
#endif
