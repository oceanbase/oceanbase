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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_TABLE_DELETE_RETURNING_OP_H_
#define OCEANBASE_SQL_ENGINE_DML_OB_TABLE_DELETE_RETURNING_OP_H_

#include "sql/engine/dml/ob_table_delete_op.h"
#include "sql/engine/dml/ob_table_delete_returning.h"

namespace oceanbase {
namespace sql {

class ObTableDeleteReturningSpec : public ObTableDeleteSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObTableDeleteReturningSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type) : ObTableDeleteSpec(alloc, type)
  {}

  virtual ~ObTableDeleteReturningSpec()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableDeleteReturningSpec);
};

class ObTableDeleteReturningOp : public ObTableDeleteOp {
public:
  ObTableDeleteReturningOp(ObExecContext& ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObTableDeleteOp(ctx, spec, input),
        partition_service_(NULL),
        child_row_count_(OB_INVALID_COUNT),
        delete_row_ceils_(NULL)
  {}

  virtual ~ObTableDeleteReturningOp()
  {}

protected:
  virtual int get_next_row() override;
  virtual int inner_open() override;

private:
  storage::ObPartitionService* partition_service_;
  // the following three members is for interface of old engine
  int64_t child_row_count_;
  ObObj* delete_row_ceils_;
};

class ObTableDeleteReturningOpInput : public ObTableModifyOpInput {
  OB_UNIS_VERSION_V(1);

public:
  ObTableDeleteReturningOpInput(ObExecContext& ctx, const ObOpSpec& spec) : ObTableModifyOpInput(ctx, spec)
  {}

  int init(ObTaskInfo& task_info) override
  {
    return ObTableModifyOpInput::init(task_info);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableDeleteReturningOpInput);
};

}  // namespace sql
}  // namespace oceanbase
#endif
