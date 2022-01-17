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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_TABLE_INSERT_RETURNING_H_
#define OCEANBASE_SQL_ENGINE_DML_OB_TABLE_INSERT_RETURNING_H_

#include "sql/engine/dml/ob_table_insert.h"

namespace oceanbase {
namespace share {
class ObPartitionReplicaLocation;
}

namespace sql {
class ObPhyTableLocation;

class ObTableInsertReturningInput : public ObTableInsertInput {
public:
  ObTableInsertReturningInput() : ObTableInsertInput()
  {}
  virtual ~ObTableInsertReturningInput()
  {}
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_INSERT_RETURNING;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableInsertReturningInput);
};

class ObTableInsertReturning : public ObTableInsert {
  class ObTableInsertReturningCtx;
  OB_UNIS_VERSION(1);

public:
  explicit ObTableInsertReturning(common::ObIAllocator& alloc);
  ~ObTableInsertReturning();
  void reset();
  void reuse();
  int set_insert_row_exprs();

protected:
  int init_op_ctx(ObExecContext& ctx) const override;
  int inner_open(ObExecContext& ctx) const override;
  int get_next_row(ObExecContext& ctx, const ObNewRow*& row) const override;
  int inner_close(ObExecContext& ctx) const override;

protected:
  // exprs for calculating the inserted row
  common::ObDList<ObSqlExpression> insert_row_exprs_;
  // projector for building the inserted row
  int32_t* insert_projector_;
  int64_t insert_projector_size_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableInsertReturning);
};
}  // namespace sql
}  // namespace oceanbase

#endif
