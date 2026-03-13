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
#ifndef OCEANBASE_SQL_REWRITE_OB_SEARCH_INDEX_QUERY_RANGE_UTILS_H_
#define OCEANBASE_SQL_REWRITE_OB_SEARCH_INDEX_QUERY_RANGE_UTILS_H_

#include "sql/rewrite/ob_query_range_define.h"

namespace oceanbase
{

namespace sql
{
class ObSearchIndexQueryRangeUtils
{
public:
  static int build_column_idx_expr(ObQueryRangeCtx &ctx,
                                   const ObColumnRefRawExpr &column_expr,
                                   ObRawExpr *&column_idx_expr);
  static int build_null_path_expr(ObQueryRangeCtx &ctx, ObRawExpr *&path_expr);

  static int build_json_single_path_expr(ObQueryRangeCtx &ctx,
                                         const ObRawExpr &const_expr,
                                         ObRawExpr *&path);

  static int build_json_range_path_expr(ObIAllocator &allocator,
                                        ObQueryRangeCtx &ctx,
                                        const ObRawExpr &const_expr,
                                        ObItemType cmp_type,
                                        ObRawExpr *&start_path,
                                        ObRawExpr *&end_path);

  static int build_value_expr(ObQueryRangeCtx &ctx,
                              const ObRawExpr &const_expr,
                              const ObObjType *cmp_type,
                              ObRawExpr *&value_expr);

  static int json_value_can_extract_range(const ObRawExpr *origin_expr,
                                          const ObRawExpr *json_expr,
                                          bool &can_extract);

  static int json_prefix_path_encode(ObIAllocator &allocator,
                                     ObQueryRangeCtx &ctx,
                                     const ObRawExpr &path_expr,
                                     ObString &path,
                                     const bool is_range_cmp,
                                     bool &can_extract_range,
                                     const ObRawExpr *const_expr = nullptr);

  static int is_json_scalar_match_index(ObExecContext &exec_ctx, const ObObj &json_value,
                                        bool &is_match);

  static int is_json_scalar_or_array_match_index(ObExecContext &exec_ctx, const ObObj &json_value,
                                                 bool &is_match);

  static int add_json_scalar_constraint(ObQueryRangeCtx &ctx, const ObRawExpr *expr);

  static int add_json_scalar_or_array_constraint(ObQueryRangeCtx &ctx, const ObRawExpr *expr);
  static int add_array_string_length_constraint(ObQueryRangeCtx &ctx, const ObRawExpr *expr);
  static int add_string_type_length_constraint(ObQueryRangeCtx &ctx, const ObRawExpr *expr);
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_REWRITE_OB_SEARCH_INDEX_QUERY_RANGE_UTILS_H_
