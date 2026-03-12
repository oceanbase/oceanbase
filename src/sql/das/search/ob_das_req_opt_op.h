/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_REQ_OPT_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_REQ_OPT_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"

namespace oceanbase
{
namespace sql
{

class ObDASReqOptOpParam : public ObIDASSearchOpParam
{
public:
  ObDASReqOptOpParam(ObIDASSearchOp *required, ObIDASSearchOp *optional, bool need_score);
  ~ObDASReqOptOpParam() {}

  OB_INLINE ObIDASSearchOp *get_required() const { return required_; }
  OB_INLINE ObIDASSearchOp *get_optional() const { return optional_; }
  OB_INLINE bool get_need_score() const { return need_score_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;

private:
  ObIDASSearchOp *required_;
  ObIDASSearchOp *optional_;
  bool need_score_;
};

class ObDASReqOptOp : public ObIDASSearchOp
{
public:
  ObDASReqOptOp(ObDASSearchCtx &search_ctx);

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple) override;
  int do_set_min_competitive_score(const double &threshold) override;
  int do_calc_max_score(double &threshold) override;

private:
  int inner_get_next_row(
      const ObDASRowID &req_id,
      const double &req_score,
      ObDASRowID &next_id,
      double &score);
  int inner_get_next_row_from_req(
      const ObDASRowID &req_id,
      const double &req_score,
      ObDASRowID &next_id,
      double &score);
  int inner_get_next_row_with_block_max_pruning(
      const ObDASRowID &req_id,
      const double &req_score,
      ObDASRowID &next_id,
      double &score);
  int inner_get_next_row_with_score_propagated(
      const ObDASRowID &req_id,
      const double &req_score,
      ObDASRowID &next_id,
      double &score);
private:
  ObIDASSearchOp *required_;
  ObIDASSearchOp *optional_;
  double required_max_score_;
  double optional_max_score_;
  ObDASRowID curr_opt_id_;
  double curr_opt_score_;
  bool is_conjunctive_;
  bool score_propagated_;
  bool max_score_calculated_;
  bool iter_end_;
  bool is_inited_;
};


} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_REQOPT_OP_H_