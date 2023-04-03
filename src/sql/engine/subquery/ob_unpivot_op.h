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

#ifndef OCEANBASE_SRC_SQL_ENGINE_SUBQUERY_OB_UNPIVOT_OP_H_
#define OCEANBASE_SRC_SQL_ENGINE_SUBQUERY_OB_UNPIVOT_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/resolver/dml/ob_dml_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObUnpivotSpec : public ObOpSpec
{
  OB_UNIS_VERSION(1);
public:
  ObUnpivotSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(unpivot_info));

  int64_t max_part_count_;
  ObUnpivotInfo unpivot_info_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObUnpivotSpec);
};

class ObUnpivotOp : public ObOperator
{
public:
  ObUnpivotOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;

private:
  void reset();
  int64_t curr_part_idx_;
  int64_t curr_cols_idx_;
  const ObBatchRows *child_brs_;
  ObDatum *multiplex_;
  DISALLOW_COPY_AND_ASSIGN(ObUnpivotOp);
};

} // namespace sql
} // namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_SUBQUERY_OB_UNPIVOT_OP_H_ */
