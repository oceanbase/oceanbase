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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_TABLE_DELETE_RETURNING_H_
#define OCEANBASE_SQL_ENGINE_DML_OB_TABLE_DELETE_RETURNING_H_

#include "sql/engine/dml/ob_table_delete.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
namespace sql {

class ObTableDeleteReturningInput : public ObTableModifyInput {
  friend class ObTableDeleteReturning;

public:
  ObTableDeleteReturningInput() : ObTableModifyInput()
  {}
  virtual ~ObTableDeleteReturningInput()
  {}
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_DELETE_RETURNING;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableDeleteReturningInput);
};

class ObTableDeleteReturning : public ObTableDelete {
  class ObTableDeleteReturningCtx;

public:
  explicit ObTableDeleteReturning(common::ObIAllocator& alloc);
  ~ObTableDeleteReturning();

private:
  virtual int inner_open(ObExecContext& ctx) const override;
  virtual int get_next_row(ObExecContext& ctx, const ObNewRow*& row) const override;
  virtual int init_op_ctx(ObExecContext& ctx) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableDeleteReturning);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_DML_OB_TABLE_DELETE_RETURNING_H_ */
