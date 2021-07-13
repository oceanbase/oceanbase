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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_TABLE_DELETE_OP_
#define OCEANBASE_SQL_ENGINE_DML_OB_TABLE_DELETE_OP_

#include "sql/engine/dml/ob_table_modify_op.h"

namespace oceanbase {
namespace sql {

class ObTableDeleteSpec : public ObTableModifySpec {
  OB_UNIS_VERSION(1);

public:
  ObTableDeleteSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type) : ObTableModifySpec(alloc, type)
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableDeleteSpec);
};

class ObTableDeleteOp : public ObTableModifyOp {
public:
  ObTableDeleteOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObTableModifyOp(exec_ctx, spec, input), part_row_cnt_(0)
  {}

  virtual ~ObTableDeleteOp()
  {}

  virtual void destroy() override
  {
    part_infos_.reset();
    ObTableModifyOp::destroy();
  }

protected:
  virtual int inner_open() override;
  virtual int inner_get_next_row() override;
  virtual int prepare_next_storage_row(const ObExprPtrIArray*& output) override;
  virtual int rescan() override;

  int do_table_delete();
  int delete_rows(int64_t& affected_rows);

protected:
  common::ObSEArray<DMLPartInfo, 1> part_infos_;
  storage::ObDMLBaseParam dml_param_;
  int64_t part_row_cnt_;
  DISALLOW_COPY_AND_ASSIGN(ObTableDeleteOp);
};

class ObTableDeleteOpInput : public ObTableModifyOpInput {
  OB_UNIS_VERSION_V(1);

public:
  ObTableDeleteOpInput(ObExecContext& ctx, const ObOpSpec& spec) : ObTableModifyOpInput(ctx, spec)
  {}
  int init(ObTaskInfo& task_info) override
  {
    return ObTableModifyOpInput::init(task_info);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableDeleteOpInput);
};

}  // namespace sql
}  // namespace oceanbase

#endif
