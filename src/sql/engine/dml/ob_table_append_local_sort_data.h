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

#ifndef OCEANBASE_SQL_ENGINE_DML_APPEND_LOCAL_SORT_DATA_H_
#define OCEANBASE_SQL_ENGINE_DML_APPEND_LOCAL_SORT_DATA_H_

#include "sql/executor/ob_task_id.h"
#include "sql/engine/dml/ob_table_append.h"

namespace oceanbase {
namespace sql {

class ObTableAppendLocalSortDataInput : public ObTableAppendInput {
  OB_UNIS_VERSION_V(1);

public:
  ObTableAppendLocalSortDataInput() : ObTableAppendInput(), task_id_()
  {}
  virtual ~ObTableAppendLocalSortDataInput()
  {}
  virtual void reset() override
  {
    ObTableAppendInput::reset();
    task_id_.reset();
  }
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op) override;
  virtual ObPhyOperatorType get_phy_op_type() const override
  {
    return PHY_APPEND_LOCAL_SORT_DATA;
  }
  const ObTaskID& get_ob_task_id() const
  {
    return task_id_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableAppendLocalSortDataInput);
  ObTaskID task_id_;
};

class ObTableAppendLocalSortData : public ObTableAppend {
public:
  class ObTableAppendLocalSortDataCtx : public ObPhyOperatorCtx {
    friend class ObTableAppendLocalSortData;

  public:
  public:
    explicit ObTableAppendLocalSortDataCtx(ObExecContext& ctx) : ObPhyOperatorCtx(ctx)
    {}
    virtual ~ObTableAppendLocalSortDataCtx() = default;
    virtual void destroy() override
    {
      ObPhyOperatorCtx::destroy_base();
    }
  };

  explicit ObTableAppendLocalSortData(common::ObIAllocator& allocator);
  virtual ~ObTableAppendLocalSortData();

  virtual int init_op_ctx(ObExecContext& ctx) const override;
  virtual int create_operator_input(ObExecContext& ctx) const override;

  virtual reclaim_row_t reclaim_row_func() const override
  {
    return &reclaim_macro_block;
  }

protected:
  virtual int inner_open(ObExecContext& ctx) const override;
  virtual int inner_close(ObExecContext& ctx) const override;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const override;

  // called when interm result freed
  static void reclaim_macro_block(const common::ObNewRow& row);

private:
  mutable common::ObNewRow row_;
  mutable bool iter_end_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_SQL_ENGINE_DML_APPEND_LOCAL_SORT_DATA_H_
