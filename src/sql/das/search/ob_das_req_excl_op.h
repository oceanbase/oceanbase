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

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_REQ_EXCL_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_REQ_EXCL_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"

namespace oceanbase
{
namespace sql
{

class ObDASReqExclOpParam : public ObIDASSearchOpParam
{
public:
  ObDASReqExclOpParam(ObIDASSearchOp *required, ObIDASSearchOp *excluded)
      : ObIDASSearchOpParam(DAS_SEARCH_OP_REQ_EXCL), required_(required), excluded_(excluded) {}
  ~ObDASReqExclOpParam() {}
  OB_INLINE ObIDASSearchOp *get_required() const { return required_; }
  OB_INLINE ObIDASSearchOp *get_excluded() const { return excluded_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;

private:
  ObIDASSearchOp *required_;
  ObIDASSearchOp *excluded_;
};

class ObDASReqExclOp : public ObIDASSearchOp {
public:
  ObDASReqExclOp(ObDASSearchCtx &search_ctx)
      : ObIDASSearchOp(search_ctx), required_(nullptr), excluded_(nullptr)
  { }

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

private:
  OB_INLINE bool is_valid() { return nullptr != required_ && nullptr != excluded_; }
  int check_excluded(const ObDASRowID &target_id, int &cmp);

private:
  ObIDASSearchOp *required_;
  ObIDASSearchOp *excluded_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_REQEXCL_OP_H_