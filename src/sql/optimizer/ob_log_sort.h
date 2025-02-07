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

#ifndef OCEANBASE_SQL_OB_LOG_SORT_H
#define OCEANBASE_SQL_OB_LOG_SORT_H
#include "lib/container/ob_array.h"
#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase
{
namespace sql
{
class ObRawExpr;
class ObLogSort;

struct ObTopNFilterInfo
{
public:
  ObTopNFilterInfo()
      : enabled_(false), p2p_sequence_id_(OB_INVALID_ID), pushdown_topn_filter_expr_(nullptr),
        effective_sk_cnt_(0), is_shuffle_(false), topn_filter_node_(NULL)
  {}
  inline void init(int64_t p2p_sequence_id, ObRawExpr *pushdown_topn_filter_expr,
                   int64_t effective_sk_cnt, bool is_shuffle, ObLogicalOperator* node)
  {
    p2p_sequence_id_ = p2p_sequence_id;
    pushdown_topn_filter_expr_ = pushdown_topn_filter_expr;
    effective_sk_cnt_ = effective_sk_cnt;
    is_shuffle_ = is_shuffle;
    enabled_ = true;
    topn_filter_node_ = node;
  }
  TO_STRING_KV(K_(enabled), K_(p2p_sequence_id), KP_(pushdown_topn_filter_expr),
               K_(effective_sk_cnt), K_(is_shuffle));

public:
  bool enabled_;
  int64_t p2p_sequence_id_;
  ObRawExpr *pushdown_topn_filter_expr_;
  // the sort key may from different tables, we can only use the front keys from the same table
  int64_t effective_sk_cnt_;
  // the topn sort op and the tsc op not in same dfo, need shuffle
  bool is_shuffle_;
  ObLogicalOperator *topn_filter_node_;
};

class ObLogSort : public ObLogicalOperator
{
public:
  ObLogSort(ObLogPlan &plan)
      : ObLogicalOperator(plan),
        hash_sortkey_(),
        sort_keys_(),
        encode_sortkeys_(),
        topn_expr_(NULL),
        minimum_row_count_(0),
        topk_precision_(0),
        prefix_pos_(0),
        is_local_merge_sort_(false),
        topk_limit_expr_(NULL),
        topk_offset_expr_(NULL),
        is_fetch_with_ties_(false),
        part_cnt_(0),
        sort_key_width_(0.0)
  {}
  virtual ~ObLogSort()
  {}
  virtual int est_cost() override;
  virtual int est_width() override;
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
  int inner_est_cost(const int64_t parallel, double child_card, double &topn_count, double &op_cost);
  const OrderItem &get_hash_sortkey() const { return hash_sortkey_; }
  OrderItem &get_hash_sortkey() { return hash_sortkey_; }
  inline void set_hash_sortkey(const OrderItem &hash_sortkey) { hash_sortkey_ = hash_sortkey; }
  const common::ObIArray<OrderItem> &get_sort_keys() const { return sort_keys_; }
  common::ObIArray<OrderItem> &get_sort_keys() { return sort_keys_; }
  const common::ObIArray<OrderItem> &get_encode_sortkeys() const { return encode_sortkeys_; }
  common::ObIArray<OrderItem> &get_encode_sortkeys() { return encode_sortkeys_; }
  int get_sort_output_exprs(ObIArray<ObRawExpr *> &output_exprs);
  int create_encode_sortkey_expr(const common::ObIArray<OrderItem> &order_keys);
  int get_sort_exprs(common::ObIArray<ObRawExpr*> &sort_exprs);

  inline void set_topn_expr(ObRawExpr *expr) { topn_expr_ = expr; }
  inline void set_prefix_pos(int64_t prefix_pos) { prefix_pos_ = prefix_pos; }
  inline void set_local_merge_sort(bool is_local_merge_sort) { is_local_merge_sort_ = is_local_merge_sort; }
  inline void set_fetch_with_ties(bool is_fetch_with_ties) { is_fetch_with_ties_ = is_fetch_with_ties; }
  inline void set_part_cnt(uint64_t part_cnt) { part_cnt_ = part_cnt; }

  // check if the current sort is a pushed down
  inline bool is_prefix_sort() const { return prefix_pos_ != 0; }
  inline bool is_local_merge_sort() const { return is_local_merge_sort_; }
  inline bool is_part_sort() const { return part_cnt_ != 0; }
  inline bool is_fetch_with_ties() const { return is_fetch_with_ties_; }
  inline bool enable_encode_sortkey_opt() const { return encode_sortkeys_.count()!=0; }
  inline int64_t get_part_cnt() const { return part_cnt_; }
  inline int64_t get_prefix_pos() const { return prefix_pos_; }
  inline ObRawExpr *get_topn_expr() const { return topn_expr_; }
  inline void set_topk_limit_expr(ObRawExpr *top_limit_expr)
  {
    topk_limit_expr_ = top_limit_expr;
  }
  inline ObRawExpr *get_topk_limit_expr() { return topk_limit_expr_; }
  inline void set_topk_offset_expr(ObRawExpr *topk_offset_expr)
  {
    topk_offset_expr_ = topk_offset_expr;
  }
  inline ObRawExpr *get_topk_offset_expr() { return topk_offset_expr_; }
  int set_sort_keys(const common::ObIArray<OrderItem> &order_keys);
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;
  virtual uint64_t hash(uint64_t seed) const override;
  virtual const char *get_name() const;
  inline void set_minimal_row_count(int64_t minimum_row_count)
  {
    minimum_row_count_ = minimum_row_count;
  }
  inline int64_t get_minimum_row_count() const {return minimum_row_count_;}
  inline void set_topk_precision(int64_t topk_precision)
  {
    topk_precision_ = topk_precision;
  }
  inline int64_t get_topk_precision() const {return topk_precision_;}
  inline double get_sort_key_width() const { return sort_key_width_; }
  virtual bool is_block_op() const override { return !is_prefix_sort(); }
  virtual int compute_op_ordering() override;
  virtual int get_plan_item_info(PlanText &plan_text,
                              ObSqlPlanItem &plan_item) override;
  inline bool enable_pd_topn_filter() const { return topn_filter_info_.enabled_; }
  inline int64_t get_p2p_sequence_id() const { return topn_filter_info_.p2p_sequence_id_; }
  inline ObRawExpr *get_pushdown_topn_filter_expr() const
  {
    return topn_filter_info_.pushdown_topn_filter_expr_;
  }
  inline int64_t get_effective_sk_cnt_of_topn_filter() const
  {
    return topn_filter_info_.effective_sk_cnt_;
  }
  inline bool is_shuffle_pd_topn_filter() { return topn_filter_info_.is_shuffle_; }
  int try_allocate_pushdown_topn_runtime_filter();
  int check_use_child_ordering(bool &used, int64_t &inherit_child_ordering_index);
  ObLogicalOperator *get_topn_filter_node() const { return topn_filter_info_.topn_filter_node_; }
protected:
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer);
  int est_sort_key_width();
private:
  int get_candidate_pushdown_sort_keys(
      uint64_t &table_id,
      common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> &candidate_sk_exprs);
  int check_expr_can_pushdown(ObRawExpr *expr, uint64_t &table_id, bool &can_push_down);
  int is_expr_in_pushdown_whitelist(ObRawExpr *expr, bool &in_pushdown_whitelist);
private:
  OrderItem hash_sortkey_;
  common::ObSEArray<OrderItem, 8, common::ModulePageAllocator, true> sort_keys_;
  common::ObSEArray<OrderItem, 1, common::ModulePageAllocator, true> encode_sortkeys_;
  ObRawExpr *topn_expr_;
  int64_t minimum_row_count_;
  int64_t topk_precision_;
  int64_t prefix_pos_; //  for prefix_sort
  bool is_local_merge_sort_; // is used for final sort operator
  ObRawExpr *topk_limit_expr_;
  ObRawExpr *topk_offset_expr_;
  bool is_fetch_with_ties_;
  int64_t part_cnt_;
  double sort_key_width_;
  ObTopNFilterInfo topn_filter_info_; //top n filter can be pushdown to table scan
};

} // end of namespace sql
} // end of namespace oceanbase

#endif // OCEANBASE_SQL_OB_LOG_SORT_H
