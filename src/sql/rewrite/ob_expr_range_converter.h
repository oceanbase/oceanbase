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
#ifndef OCEANBASE_SQL_REWRITE_OB_RANGE_NODE_GENERATOR_H_
#define OCEANBASE_SQL_REWRITE_OB_RANGE_NODE_GENERATOR_H_

#include "sql/rewrite/ob_query_range_define.h"



namespace oceanbase
{
namespace sql
{

typedef ObSEArray<const ObRawExpr*, 4> TmpExprArray;

class ObExprRangeConverter
{
public:
  ObExprRangeConverter(ObIAllocator &allocator, ObQueryRangeCtx &ctx)
    : allocator_(allocator),
      ctx_(ctx),
      mem_used_(allocator.used())
  {}

  int convert_expr_to_range_node(const ObRawExpr *expr,
                                 ObRangeNode *&range_node,
                                 int64_t expr_depth,
                                 bool &is_precise);
  int generate_always_true_or_false_node(bool is_true, ObRangeNode *&range_node);
  int convert_const_expr(const ObRawExpr *expr, ObRangeNode *&range_node);
  int convert_basic_cmp_expr(const ObRawExpr *expr, ObRangeNode *&range_node);
  int get_basic_range_node(const ObRawExpr *l_expr,
                           const ObRawExpr *r_expr,
                           ObItemType cmp_type,
                           const ObExprResType &result_type,
                           ObRangeNode *&range_node);
  int convert_is_expr(const ObRawExpr *expr, ObRangeNode *&range_node);

  int convert_between_expr(const ObRawExpr *expr, ObRangeNode *&range_node);
  int convert_not_between_expr(const ObRawExpr *expr, ObRangeNode *&range_node);
  int convert_not_equal_expr(const ObRawExpr *expr, ObRangeNode *&range_node);
  int convert_like_expr(const ObRawExpr *expr, ObRangeNode *&range_node);
  int convert_in_expr(const ObRawExpr *expr, ObRangeNode *&range_node);


  int fill_range_node_for_basic_cmp(ObItemType cmp_type,
                                    const int64_t key_idx,
                                    const int64_t val_idx,
                                    ObRangeNode &range_node) const;
  int fill_range_node_for_basic_row_cmp(ObItemType cmp_type,
                                        const ObIArray<int64_t> &key_idxs,
                                        const ObIArray<int64_t> &val_idxs,
                                        ObRangeNode &range_node) const;
  int fill_range_node_for_like(const int64_t key_idx,
                               const int64_t start_val_idx,
                               const int64_t end_val_idx,
                               ObRangeNode &range_node) const;

  int check_expr_precise(const ObRawExpr &const_expr,
                         const ObExprCalcType &calc_type,
                         const ObExprResType &column_res_type);

  inline int64_t get_mem_used() const { return mem_used_; }

private:
  ObExprRangeConverter();
  int alloc_range_node(ObRangeNode *&range_node);
  int generate_deduce_const_expr(ObRawExpr *expr, int64_t &start_val, int64_t &end_val);
  int gen_column_cmp_node(const ObRawExpr &l_expr,
                          const ObRawExpr &r_expr,
                          ObItemType cmp_type,
                          const ObExprResType &result_type,
                          ObRangeNode *&range_node);
  int gen_row_column_cmp_node(const ObRawExpr &l_expr,
                              const ObRawExpr &r_expr,
                              ObItemType cmp_type,
                              const ObExprResType &result_type,
                              ObRangeNode *&range_node);
  int get_rowid_node(const ObRawExpr &l_expr,
                     const ObRawExpr &r_expr,
                     ObItemType cmp_type,
                     ObRangeNode *&range_node);
  int get_extract_rowid_range_infos(const ObRawExpr &calc_urowid_expr,
                                    ObIArray<const ObColumnRefRawExpr*> &pk_columns,
                                    bool &is_physical_rowid,
                                    uint64_t &part_column_id);
  int get_single_in_range_node(const ObColumnRefRawExpr *column_expr,
                               const ObRawExpr *r_expr,
                               const ObExprResType &res_type,
                               ObRangeNode *&range_node);
  int get_row_in_range_ndoe(const ObRawExpr &l_expr,
                            const ObRawExpr &r_expr,
                            const ObExprResType &res_type,
                            ObRangeNode *&range_node);
  int get_single_rowid_in_range_node(const ObRawExpr &rowid_expr,
                                   const ObRawExpr &row_expr,
                                   ObRangeNode *&range_node);
  int convert_not_in_expr(const ObRawExpr *expr, ObRangeNode *&range_node);
  int check_escape_valid(const ObRawExpr *escape, char &escape_ch, bool &is_valid);
  int build_decode_like_expr(ObRawExpr *pattern, ObRawExpr *escape, char escape_ch,
                             ObRangeColumnMeta *column_meta, int64_t &start_val_idx, int64_t &end_val_idx);
  int get_calculable_expr_val(const ObRawExpr *expr, ObObj &val, bool &is_valid, const bool ignore_error = true);
  int check_calculable_expr_valid(const ObRawExpr *expr, bool &is_valid, const bool ignore_error = true);
  int add_precise_constraint(const ObRawExpr *expr, bool is_precise);
  int add_prefix_pattern_constraint(const ObRawExpr *expr);
  int get_final_expr_idx(const ObRawExpr *expr, int64_t &idx);
  int get_final_in_array_idx(InParam *&in_param, int64_t &idx);
  bool is_range_key(const uint64_t column_id, int64_t &key_idx);
  ObRangeColumnMeta* get_column_meta(int64_t idx);
private:
  ObIAllocator &allocator_;
  ObQueryRangeCtx &ctx_;
  const int64_t mem_used_;
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_REWRITE_OB_QUERY_RANGE_DEFINE_H_
