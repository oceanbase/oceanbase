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

#ifndef OCEANBASE_SQL_OB_APPEND_OP_H_
#define OCEANBASE_SQL_OB_APPEND_OP_H_

#include "sql/engine/ob_operator.h"

namespace oceanbase {
namespace sql {
class ObExecContext;
class ObAppendSpec : public ObOpSpec {
  OB_UNIS_VERSION(1);

public:
  ObAppendSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type) : ObOpSpec(alloc, type)
  {}
  virtual ~ObAppendSpec(){};

private:
  DISALLOW_COPY_AND_ASSIGN(ObAppendSpec);
};

class ObAppendOp : public ObOperator {
public:
  ObAppendOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObOperator(exec_ctx, spec, input), current_child_op_idx_(0)
  {}
  ~ObAppendOp()
  {
    destroy();
  }

  int inner_open() override;
  virtual void destroy()
  {
    ObOperator::destroy();
  }

  /**
   * @brief called by get_next_row(), get a row from the child operator or row_store
   * @param ctx[in], execute context
   * @param row[out], ObSqlRow an obj array and row_size
   */
  virtual int inner_get_next_row() override;
  /**
   * @note not need to iterator a row to parent operator, so forbid this function
   */
  int get_next_row();

public:
  int64_t current_child_op_idx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAppendOp);
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_APPEND_H_ */
