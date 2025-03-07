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
#ifndef OCEANBASE_SQL_REWRITE_OB_RANGE_GRAPH_GENERATOR_H_
#define OCEANBASE_SQL_REWRITE_OB_RANGE_GRAPH_GENERATOR_H_

#include "sql/rewrite/ob_query_range_define.h"
#include "sql/rewrite/ob_expr_range_converter.h"



namespace oceanbase
{
namespace sql
{

struct ObPriciseExprItem
{
  ObPriciseExprItem()
  : expr_(nullptr), max_offset_(-1) {}
  ObPriciseExprItem(ObRawExpr *expr, int64_t max_offset)
  : expr_(expr), max_offset_(max_offset) {}
  TO_STRING_KV(KPC(expr_), K(max_offset_));
  const ObRawExpr *expr_;
  int64_t max_offset_;
};

struct RangeNodeCmp
{
  inline bool operator()(const ObRangeNode *left, const ObRangeNode *right)
  {
    bool bret = false;
    if (left != nullptr && right != nullptr) {
      if (left->always_false_ || right->always_false_) {
        bret = !right->always_false_;
      } else if (left->always_true_ || right->always_true_) {
        bret = !right->always_true_;
      } else {
        bret = left->min_offset_ < right->min_offset_;
      }
    }
    return bret;
  }
};

struct RangeNodeConnectInfo
{
  RangeNodeConnectInfo()
    : inited_(false),
      data_(nullptr),
      node_count_(0),
      per_node_info_len_(0) {
  }
  bool inited_;
  uint8_t* data_;
  uint64_t node_count_;
  uint64_t per_node_info_len_;
};

class ObRangeGraphGenerator
{
public:
  ObRangeGraphGenerator(ObIAllocator &allocator,
                        ObQueryRangeCtx &ctx,
                        ObPreRangeGraph *pre_range_graph,
                        int64_t column_cnt)
    : allocator_(allocator),
      ctx_(ctx),
      pre_range_graph_(pre_range_graph),
      max_precise_offset_(column_cnt),
      ss_max_precise_offset_(column_cnt)
  {}

  int generate_range_graph(const ObIArray<ObRawExpr*> &exprs,
                           ObExprRangeConverter &range_node_generator);
  static int and_range_nodes(ObIArray<ObRangeNode*> &range_nodes,
                             const int64_t column_cnt,
                             ObRangeNode *&range_node);
  static int or_range_nodes(ObExprRangeConverter &range_node_generator,
                            ObIArray<ObRangeNode*> &range_nodes,
                            const int64_t column_cnt,
                            ObRangeNode *&range_node);
private:
  int generate_range_node(ObRawExpr* expr,
                          ObExprRangeConverter &range_node_generator,
                          ObRangeNode *&range_node,
                          int64_t expr_depth,
                          bool &is_precise,
                          int64_t &max_offset);
  int generate_and_range_node(ObRawExpr *and_expr,
                              ObExprRangeConverter &range_node_generator,
                              ObRangeNode *&range_node,
                              int64_t expr_depth,
                              bool &is_precise,
                              int64_t &max_offset);

  static void set_new_start_key(ObRangeNode &l_node, ObRangeNode &r_node, const int64_t column_cnt, int64_t start_offset);
  static void set_new_end_key(ObRangeNode &l_node, ObRangeNode &r_node, const int64_t column_cnt, int64_t start_offset);

  int generate_or_range_node(ObRawExpr *or_expr,
                             ObExprRangeConverter &range_node_generator,
                             ObRangeNode *&range_node,
                             int64_t expr_depth,
                             bool &is_precise,
                             int64_t &max_offset);

  static int and_two_range_node(ObRangeNode *&l_node,
                                ObRangeNode *&r_node,
                                const int64_t column_cnt,
                                bool &is_merge);

  static int or_two_range_node(ObRangeNode *&l_node,
                               ObRangeNode *&r_node,
                               const int64_t column_cnt);

  static int and_link_range_node(ObRangeNode *&l_node, ObRangeNode *&r_node);
  static int get_and_tails(ObRangeNode *range_node, ObIArray<ObRangeNode*> &and_tails);

  int formalize_final_range_node(ObRangeNode *&range_node);
  int collect_graph_infos(ObRangeNode *range_node,
                          uint64_t *total_range_sizes,
                          uint64_t *range_sizes,
                          bool &start_from_zero,
                          int64_t &min_offset);
  int check_skip_scan_valid(ObRangeNode *range_node,
                            ObRangeNode *&ss_head);
  static int generate_node_id(ObRangeNode *range_node, uint64_t &node_count);

  int check_graph_type(ObRangeNode *range_node);

  bool is_precise_get(ObRangeNode *range_node) const;
  bool is_standard_range(ObRangeNode *range_node) const;
  int get_max_precise_pos(ObRangeNode *range_node, int64_t &max_precise_pos, int64_t start_pos = 0) const;
  int inner_get_max_precise_pos(const ObRangeNode *range_node, bool* equals, int64_t &max_offset, int64_t start_pos) const;
  int remove_useless_range_node(ObRangeNode *range_node, int64_t start_pos = 0) const;
  int is_strict_equal_graph(ObRangeNode *range_node, bool &is_strict_equal, bool &is_get) const;
  int inner_is_strict_equal_graph(const ObRangeNode *range_node,
                                  bool* equals,
                                  int64_t &max_offset,
                                  int64_t &max_node_offset,
                                  bool &is_strict_equal) const;
  int get_new_equal_idx(const ObRangeNode *range_node, bool* equals, ObIArray<int64_t> &new_idx) const;

  int check_and_set_get_for_unique_index(ObRangeNode *range_node, bool &is_get) const;
  int check_unique_index_range_valid(const ObRangeNode *range_node, bool &is_valid) const;
  int transform_unique_index_graph(ObRangeNode *range_node) const;
  bool is_strict_equal_node(const ObRangeNode *range_node) const;
  bool is_equal_node(const ObRangeNode *range_node) const;

  int fill_range_exprs(ObIArray<ObPriciseExprItem> &pricise_exprs,
                       ObIArray<ObPriciseExprItem> &unpricise_exprs);
  int generate_expr_final_info();

  inline void update_max_precise_offset(int64_t offset) { max_precise_offset_ = std::min(max_precise_offset_, offset); }
  inline void update_ss_max_precise_offset(int64_t offset) { ss_max_precise_offset_ = std::min(ss_max_precise_offset_, offset); }
  inline bool is_const_expr_or_null(int64_t idx) const
  {
    return idx < OB_RANGE_EXTEND_VALUE || OB_RANGE_NULL_VALUE == idx;
  }
  int relink_standard_range_if_needed(ObRangeNode *&range_node);
  static int crop_final_range_node(ObRangeNode *&range_node, int64_t crop_offset);
  static int crop_final_range_node(ObRangeNode *&range_node, int64_t crop_offset,
                                   RangeNodeConnectInfo &connect_info,
                                   common::hash::ObHashMap<uint64_t, ObRangeNode*> &refined_ranges,
                                   common::hash::ObHashSet<uint64_t> &shared_ranges);
  static int check_crop_range_node_valid(ObRangeNode *range_node,
                                         ObRangeNode *next_range_node,
                                         RangeNodeConnectInfo &connect_info);
  static int reset_node_id(ObRangeNode *range_node);
  static int generate_range_node_connect_info(ObIAllocator &allocator,
                                              ObRangeNode *range_node,
                                              uint64_t node_count,
                                              RangeNodeConnectInfo &connect_info);
  static int collect_range_node_connect_info(ObRangeNode *range_node,
                                      RangeNodeConnectInfo &connect_info);

  static int get_max_offset(const ObRangeNode *range_node, int64_t &max_offset);

  static int get_start_from_zero(const ObRangeNode *range_node, bool &start_from_zero);

  static int check_can_fast_nlj_range_extraction(const ObRangeNode *range_node,
                                                 const ObRangeMap &range_map,
                                                 bool is_equal_range,
                                                 bool &fast_nlj_range);

  static int formalize_one_range_node(ObRangeNode &range_node);
private:
  ObRangeGraphGenerator();
private:
  ObIAllocator &allocator_;
  ObQueryRangeCtx &ctx_;
  ObPreRangeGraph *pre_range_graph_;
  int64_t max_precise_offset_;
  int64_t ss_max_precise_offset_;
};


} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_REWRITE_OB_RANGE_GRAPH_GENERATOR_H_
