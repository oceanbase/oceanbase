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

#ifndef OCEANBASE_SQL_OPTIMITZER_OB_LOG_GRANULE_ITERATOR_
#define OCEANBASE_SQL_OPTIMITZER_OB_LOG_GRANULE_ITERATOR_ 1
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "lib/container/ob_se_array.h"
#include "sql/engine/px/ob_granule_util.h"

namespace oceanbase
{
namespace sql
{

class ObLogGranuleIterator : public ObLogicalOperator
{
public:
  ObLogGranuleIterator(ObLogPlan &plan) :
  ObLogicalOperator(plan),
	tablet_size_(common::OB_DEFAULT_TABLET_SIZE),
  gi_attri_flag_(0),
  partition_count_(0),
  hash_part_(false),
  bf_info_(),
  tablet_id_expr_(NULL),
  repartition_ref_table_id_(OB_INVALID_ID),
  used_by_external_table_(false)
  { }
  virtual ~ObLogGranuleIterator()
  { }

  const char *get_name() const;

  virtual int est_cost() override;
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  void set_tablet_size(int64_t tablet_size) { tablet_size_ = tablet_size; };
  int64_t get_tablet_size() { return tablet_size_; }
  uint64_t get_flag() {
    return gi_attri_flag_;
  }
  void add_flag(uint64_t attri);

  bool partition_filter() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_USE_PARTITION_FILTER); }
  bool pwj_gi() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_PARTITION_WISE); }
  bool affinitize() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_AFFINITIZE); }
  bool access_all() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_ACCESS_ALL); }
  bool with_param_down() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_NLJ_PARAM_DOWN); }
  bool asc_order() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_ASC_ORDER); }
  bool desc_order() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_DESC_ORDER); }
  bool force_partition_granule() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_FORCE_PARTITION_GRANULE); }
  bool slave_mapping_granule() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_SLAVE_MAPPING); }
  bool enable_partition_pruning() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_ENABLE_PARTITION_PRUNING); }

  virtual int compute_op_ordering() override;
  int set_range_order();

  inline uint64_t get_gi_flags() { return gi_attri_flag_; }
  void set_partition_count(int64_t partition_count) { partition_count_ = partition_count; }
  void set_hash_part(bool v) { hash_part_ = v; }
  bool is_hash_part() { return hash_part_; }

  int is_partition_gi(bool &partition_granule) const;

  ObPxBFStaticInfo &get_join_filter_info() { return bf_info_; }
  void set_join_filter_info(ObPxBFStaticInfo &bf_info) { bf_info_ = bf_info; }

  void set_tablet_id_expr(ObOpPseudoColumnRawExpr *tablet_id_expr) { tablet_id_expr_ = tablet_id_expr; }
  ObOpPseudoColumnRawExpr *get_tablet_id_expr() { return tablet_id_expr_; }
  void set_repartition_ref_table_id(int64_t table_id) { repartition_ref_table_id_ = table_id; }
  int64_t get_repartition_ref_table_id() { return repartition_ref_table_id_; }
  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
  virtual int allocate_expr_post(ObAllocExprContext &ctx) override;

  void set_used_by_external_table() { used_by_external_table_ = true; }
  bool is_used_by_external_table() const { return used_by_external_table_; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogGranuleIterator);
  int64_t tablet_size_;
  uint64_t gi_attri_flag_;
  int64_t partition_count_;
  bool hash_part_;
  ObPxBFStaticInfo bf_info_; // for join partition filter
  ObOpPseudoColumnRawExpr *tablet_id_expr_;
  int64_t repartition_ref_table_id_;
  bool used_by_external_table_;
};

}
}
#endif
