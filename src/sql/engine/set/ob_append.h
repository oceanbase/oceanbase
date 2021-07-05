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

#ifndef OCEANBASE_SQL_OB_APPEND_H_
#define OCEANBASE_SQL_OB_APPEND_H_

#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_multi_children_phy_operator.h"
#include "common/row/ob_row.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
namespace sql {
class ObAppend : public ObMultiChildrenPhyOperator {
public:
  class ObAppendCtx;
  explicit ObAppend(common::ObIAllocator& alloc);
  ~ObAppend();

  virtual void reset();
  virtual void reuse();
  int inner_open(ObExecContext& ctx) const;
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
  /**
   * @brief called by get_next_row(), get a row from the child operator or row_store
   * @param ctx[in], execute context
   * @param row[out], ObSqlRow an obj array and row_size
   */
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @note not need to iterator a row to parent operator, so forbid this function
   */
  int get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;

  DISALLOW_COPY_AND_ASSIGN(ObAppend);
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_APPEND_H_ */
