/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


/*
 * Overview
 * - Disjunction filter search operator, only used for filter Disjunction search.
 * - In the future, we will perform reasonable abstractions and compositions to unify Disjunction both for filter and score.
 * - And perhaps the same applies to Conjunction.
 * Key Responsibilities
 * -
*/

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_DISJUNCTION_FILTER_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_DISJUNCTION_FILTER_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"

namespace oceanbase
{
namespace sql
{

class ObDASDisjunctionFilterOpParam : public ObIDASSearchOpParam
{
public:
  ObDASDisjunctionFilterOpParam(
      const ObIArray<ObIDASSearchOp *> &optional_ops,
      int64_t minimum_should_match,
      ObDASSearchCost lead_cost)
    : ObIDASSearchOpParam(DAS_SEARCH_OP_DISJUNCTION_FILTER),
      optional_ops_(&optional_ops),
      minimum_should_match_(minimum_should_match),
      lead_cost_(lead_cost) {}
  ~ObDASDisjunctionFilterOpParam() {}

  OB_INLINE const ObIArray<ObIDASSearchOp *> *get_optional_ops() const { return optional_ops_; }
  OB_INLINE int64_t get_minimum_should_match() const { return minimum_should_match_; }
  OB_INLINE ObDASSearchCost get_lead_cost() const { return lead_cost_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;
  INHERIT_TO_STRING_KV("ObDASDisjunctionFilterOpParam", ObIDASSearchOpParam,
      KPC_(optional_ops),
      K_(minimum_should_match),
      K_(lead_cost));

private:
  const ObIArray<ObIDASSearchOp *> *optional_ops_;
  int64_t minimum_should_match_;
  ObDASSearchCost lead_cost_;
};

class ObDASDisjunctionFilterOp : public ObIDASSearchOp
{
public:
  ObDASDisjunctionFilterOp(ObDASSearchCtx &search_ctx)
    : ObIDASSearchOp(search_ctx),
      min_should_match_(0),
      lead_cost_(),
      cur_row_id_(),
      child_row_ids_()
  {}

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;

private:
  int64_t min_should_match_;
  ObDASSearchCost lead_cost_;
  ObDASRowID cur_row_id_;
  ObArray<ObDASRowID> child_row_ids_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_DISJUNCTION_FILTER_OP_H_