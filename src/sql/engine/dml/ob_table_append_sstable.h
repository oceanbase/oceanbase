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

#ifndef OCEANBASE_SQL_ENGINE_DML_TABLE_APPEND_SSTABLE_H_
#define OCEANBASE_SQL_ENGINE_DML_TABLE_APPEND_SSTABLE_H_

#include "sql/executor/ob_task_id.h"
#include "sql/engine/dml/ob_table_append.h"

namespace oceanbase {
namespace sql {

class ObTableAppendSSTableInput : public ObTableAppendInput {
  OB_UNIS_VERSION(1);

public:
  ObTableAppendSSTableInput() : ObTableAppendInput(), task_id_()
  {}
  virtual ~ObTableAppendSSTableInput()
  {}
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op) override;
  virtual ObPhyOperatorType get_phy_op_type() const override
  {
    return PHY_APPEND_SSTABLE;
  }
  const ObTaskID& get_task_id() const
  {
    return task_id_;
  }
  virtual void reset() override
  {
    ObTableAppendInput::reset();
    task_id_.reset();
  }

private:
  ObTaskID task_id_;
  DISALLOW_COPY_AND_ASSIGN(ObTableAppendSSTableInput);
};

class ObTableAppendSSTable : public ObTableAppend {
public:
  class ObTableAppendSSTableCtx : public ObPhyOperatorCtx {
    friend class ObTableAppendSSTable;

  public:
    explicit ObTableAppendSSTableCtx(ObExecContext& ctx) : ObPhyOperatorCtx(ctx)
    {}
    virtual ~ObTableAppendSSTableCtx() = default;
    virtual void destroy() override
    {
      ObPhyOperatorCtx::destroy_base();
    }
  };
  explicit ObTableAppendSSTable(common::ObIAllocator& allocator);
  virtual ~ObTableAppendSSTable();

  virtual int init_op_ctx(ObExecContext& ctx) const override;
  virtual int create_operator_input(ObExecContext& ctx) const override;

protected:
  virtual int inner_open(ObExecContext& ctx) const override;
  virtual int inner_close(ObExecContext& ctx) const override;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const override;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_SQL_ENGINE_DML_TABLE_APPEND_SSTABLE_H_
