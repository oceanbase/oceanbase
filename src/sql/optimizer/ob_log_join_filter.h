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

#ifndef _OB_LOG_JOIN_FILTER_H
#define _OB_LOG_JOIN_FILTER_H 1
#include "lib/container/ob_array.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/resolver/dml/ob_sql_hint.h"
#include "sql/engine/px/ob_px_basic_info.h"
#include "sql/engine/join/ob_join_filter_op.h"
#include "sql/engine/expr/ob_expr_join_filter.h"
namespace oceanbase
{
namespace sql
{

class ObLogJoinFilter : public ObLogicalOperator
{
public:
  ObLogJoinFilter(ObLogPlan &plan) :
  ObLogicalOperator(plan), is_create_(false),
      filter_id_(common::OB_INVALID_ID),
      filter_len_(0), paired_join_filter_(nullptr),
      join_exprs_(), is_use_filter_shuffle_(false),
      join_filter_cmp_funcs_(),
      join_filter_exprs_(),
      join_filter_types_(),
      p2p_sequence_ids_(),
      is_null_safe_cmps_(),
      filter_type_(JoinFilterSharedType::INVALID_TYPE),
      calc_tablet_id_expr_(NULL),
      skip_subpart_(false)
  { }
  virtual ~ObLogJoinFilter() = default;
  const char *get_name() const;
  virtual int est_cost() override;
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual uint64_t hash(uint64_t seed) const override;
  inline void set_is_create_filter(bool is_create) { is_create_ = is_create; }
  inline void set_filter_id(int64_t filter_id) { filter_id_ = filter_id; }
  inline int64_t get_filter_id() const { return filter_id_; }
  inline bool is_create_filter() { return is_create_; }
  inline void set_filter_length(double filter_len)
  {
    if (filter_len <= 0) {
      filter_len_ = 1;
    } else if (filter_len > INT64_MAX) {
      filter_len_ = INT64_MAX;
    } else {
      filter_len_ = filter_len;
    }
  }
  inline int64_t get_filter_length() const { return filter_len_; }
  inline void set_paired_join_filter(ObLogicalOperator *paired_join_filter) { paired_join_filter_ = paired_join_filter; }
  inline ObLogicalOperator *get_paired_join_filter() const { return paired_join_filter_; }
  inline void set_is_use_filter_shuffle(bool flag) { is_use_filter_shuffle_ = flag; }
  inline bool is_use_filter_shuffle() { return is_use_filter_shuffle_; }
  inline bool is_partition_filter() const
  { return filter_type_ == JoinFilterSharedType::NONSHARED_PARTITION_JOIN_FILTER ||
           filter_type_ == JoinFilterSharedType::SHARED_PARTITION_JOIN_FILTER; };
  common::ObIArray<ObRawExpr *> &get_join_exprs()
  { return join_exprs_; }
  int add_join_filter_expr(ObRawExpr *filter_expr)
      { return join_filter_exprs_.push_back(filter_expr); }
  const common::ObIArray<ObRawExpr *> &get_join_filter_exprs()
      { return join_filter_exprs_; }
common::ObIArray<ObRawExpr *> &get_join_filter_exprs_for_update()
      { return join_filter_exprs_; }
  int add_join_filter_cmp_funcs(const common::ObDatumCmpFuncType &cmp_fun)
      { return join_filter_cmp_funcs_.push_back(cmp_fun);}
  const common::ObIArray<common::ObDatumCmpFuncType> &get_join_filter_cmp_funcs()
      { return join_filter_cmp_funcs_; }
  common::ObIArray<bool> &get_is_null_safe_cmps()
      { return is_null_safe_cmps_; }
  const common::ObIArray<RuntimeFilterType> &get_join_filter_types()
      { return join_filter_types_; }
  int add_join_filter_type(RuntimeFilterType type)
      { return join_filter_types_.push_back(type); }
  const common::ObIArray<int64_t> &get_p2p_sequence_ids()
      { return p2p_sequence_ids_; }
  int add_p2p_sequence_id(int64_t id)
      { return p2p_sequence_ids_.push_back(id); }
  inline void set_tablet_id_expr(ObRawExpr *tablet_id_expr) { calc_tablet_id_expr_ = tablet_id_expr; }
  const ObRawExpr *get_tablet_id_expr() { return calc_tablet_id_expr_; }
  inline void set_is_shared_join_filter()
  { filter_type_ = JoinFilterSharedType::SHARED_JOIN_FILTER; }
  inline void set_is_non_shared_join_filter()
  { filter_type_ = JoinFilterSharedType::NONSHARED_JOIN_FILTER; }
  inline bool is_shared_join_filter() {
    return filter_type_ == JoinFilterSharedType::SHARED_JOIN_FILTER ||
           filter_type_ == JoinFilterSharedType::SHARED_PARTITION_JOIN_FILTER;
  }
  inline void set_is_shared_partition_join_filter()
  { filter_type_ = JoinFilterSharedType::SHARED_PARTITION_JOIN_FILTER; }
  inline void set_is_no_shared_partition_join_filter()
  { filter_type_ = JoinFilterSharedType::NONSHARED_PARTITION_JOIN_FILTER; }
  JoinFilterSharedType get_filter_type() { return filter_type_; }
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;
  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
private:
  bool is_create_;   //判断是否是create算子
  int64_t filter_id_; //设置filter_id
  int64_t filter_len_; //设置filter长度
  // if this is a join filter create op, the paired_join_filter_ is join filter use op, vice versa
  // if this is a partition join filter create op, the paired_join_filter_ is partition filter gi
  ObLogicalOperator *paired_join_filter_;
  //equal join condition expr
  common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> join_exprs_;
  bool is_use_filter_shuffle_; // 标记use端filter是否有shuffle
  // join_filter_cmp_funcs_ is for join filter use
  common::ObSEArray<common::ObDatumCmpFuncType, 8, common::ModulePageAllocator, true> join_filter_cmp_funcs_;
  common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> join_filter_exprs_;
  common::ObSEArray<RuntimeFilterType, 8, common::ModulePageAllocator, true> join_filter_types_;
  common::ObSEArray<int64_t, 8, common::ModulePageAllocator, true> p2p_sequence_ids_;
  common::ObSEArray<bool, 8, common::ModulePageAllocator, true> is_null_safe_cmps_;
  JoinFilterSharedType filter_type_;
  ObRawExpr *calc_tablet_id_expr_; // 计算tablet_id的expr
  bool skip_subpart_; // Ignore 2-level subpart_id when calculating partition id
  DISALLOW_COPY_AND_ASSIGN(ObLogJoinFilter);
};

}
}

#endif /* _OB_LOG_JOIN_FILTER_H */


