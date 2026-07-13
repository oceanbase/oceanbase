/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


/*
 * Overview
 * - Conjunction search operator.
 *
 * Key Responsibilities
 * -
*/

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_CONJUNCTION_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_CONJUNCTION_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "sql/engine/basic/ob_temp_column_store.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace sql
{

class ObDASConjunctionOpParam : public ObIDASSearchOpParam
{
public:
  ObDASConjunctionOpParam(const ObIArray<ObIDASSearchOp *> &required_ops)
      : ObIDASSearchOpParam(DAS_SEARCH_OP_CONJUNCTION), required_ops_(&required_ops) {}
  ~ObDASConjunctionOpParam() {}
  OB_INLINE const ObIArray<ObIDASSearchOp *> *get_required_ops() const { return required_ops_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;
  INHERIT_TO_STRING_KV("ObDASConjunctionOpParam", ObIDASSearchOpParam,
                       KPC_(required_ops));
private:
  const ObIArray<ObIDASSearchOp *> *required_ops_;
};

class ObDASConjunctionOp : public ObIDASSearchOp
{
public:
  ObDASConjunctionOp(ObDASSearchCtx &search_ctx)
      : ObIDASSearchOp(search_ctx) {}
  virtual ~ObDASConjunctionOp() {}

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_advance_shallow(const ObDASRowID &target,
                         const bool inclusive,
                         const MaxScoreTuple *&max_score_tuple) override;

private:
  int inner_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score);
  // Splits children into driver_ops_ and probe_ops_ based on is_probe_mode().
  // Called from do_open / do_rescan so the conjunction can coordinate probe-capable children.
  int classify_children();
  int advance_driver_ops_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score);
  int inner_advance_to_with_probe(const ObDASRowID &target, ObDASRowID &curr_id, double &score);

private:
  int64_t last_idx_ = 0;
  bool has_probe_child_ = false;
  // Probe-capable children; the driver is never in this list.
  ObSEArray<ObIDASSearchOp *, 4> probe_ops_;
  ObSEArray<ObIDASSearchOp *, 4> driver_ops_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_CONJUNCTION_OP_H_
