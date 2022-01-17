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

#ifndef OB_PX_MULTI_PART_INSERT_OP_H_
#define OB_PX_MULTI_PART_INSERT_OP_H_

#include "lib/container/ob_fixed_array.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/pdml/static/ob_px_multi_part_modify_op.h"
#include "sql/engine/pdml/static/ob_pdml_op_batch_row_cache.h"
#include "sql/engine/pdml/static/ob_pdml_op_data_driver.h"

namespace oceanbase {
namespace sql {
class ObPxMultiPartInsertOpInput : public ObPxMultiPartModifyOpInput {
  OB_UNIS_VERSION_V(1);

public:
  ObPxMultiPartInsertOpInput(ObExecContext& ctx, const ObOpSpec& spec) : ObPxMultiPartModifyOpInput(ctx, spec)
  {}
  int init(ObTaskInfo& task_info) override
  {
    return ObPxMultiPartModifyOpInput::init(task_info);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartInsertOpInput);
};

class ObPxMultiPartInsertSpec : public ObTableModifySpec {
  OB_UNIS_VERSION_V(1);

public:
  ObPxMultiPartInsertSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObTableModifySpec(alloc, type), row_desc_(), table_desc_(), insert_row_exprs_(&alloc)
  {}
  virtual bool is_pdml_operator() const override
  {
    return true;
  }

public:
  ObDMLOpRowDesc row_desc_;
  ObDMLOpTableDesc table_desc_;
  ExprFixedArray insert_row_exprs_;
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartInsertSpec);
};

class ObPxMultiPartInsertOp : public ObDMLOpDataReader, public ObDMLOpDataWriter, public ObTableModifyOp {
  OB_UNIS_VERSION(1);

public:
  /**ObPDMLOpRowIteratorWrapper**/
  class ObPDMLOpRowIteratorWrapper : public common::ObNewRowIterator {
  public:
    ObPDMLOpRowIteratorWrapper(ObExecContext& exec_ctx, ObPxMultiPartInsertOp* op)
        : ctx_(exec_ctx), iter_(nullptr), insert_row_(), read_row_from_iter_(NULL), insert_row_exprs_(NULL), op_(op)
    {}
    ~ObPDMLOpRowIteratorWrapper()
    {
      reset();
    }

    void reset()
    {}

    int init(const ExprFixedArray* insert_exprs, const ExprFixedArray* read_row_from_cache);

    void set_iterator(ObPDMLOpRowIterator& iter)
    {
      iter_ = &iter;
    }
    int get_next_row(common::ObNewRow*& row) override;

  private:
    ObExecContext& ctx_;
    ObPDMLOpRowIterator* iter_;
    common::ObNewRow insert_row_;
    const ExprFixedArray* read_row_from_iter_;
    const ExprFixedArray* insert_row_exprs_;
    ObPxMultiPartInsertOp* op_;
  };

public:
  ObPxMultiPartInsertOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObTableModifyOp(exec_ctx, spec, input),
        data_driver_(exec_ctx.get_eval_ctx(), exec_ctx.get_allocator(), op_monitor_info_),
        row_iter_wrapper_(exec_ctx, this)
  {}

public:
  virtual bool has_foreign_key() const
  {
    return false;
  }  // default implementation, and does not consider about foreign keys

  int read_row(ObExecContext& ctx, const ObExprPtrIArray*& row, int64_t& part_id) override;

  int write_rows(ObExecContext& ctx, common::ObPartitionKey& pkey, ObPDMLOpRowIterator& iterator) override;

  virtual int inner_get_next_row();
  virtual int inner_open();
  virtual int inner_close();

private:
  int fill_dml_base_param(uint64_t index_tid, ObSQLSessionInfo& my_session, const ObPhysicalPlan& my_phy_plan,
      const ObPhysicalPlanCtx& my_plan_ctx, storage::ObDMLBaseParam& dml_param) const;

  int process_row();

private:
  ObPDMLOpDataDriver data_driver_;
  // Used to calc the child output row and return the row required by the DML operation
  ObPDMLOpRowIteratorWrapper row_iter_wrapper_;
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartInsertOp);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OB_PX_MULTI_PART_INSERT_OP_H_ */
