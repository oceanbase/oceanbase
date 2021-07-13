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

#ifndef _OB_SQL_ENGINE_PDML_PX_MULTI_PART_UPDATE_OP_H_
#define _OB_SQL_ENGINE_PDML_PX_MULTI_PART_UPDATE_OP_H_

#include "lib/container/ob_fixed_array.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/pdml/static/ob_pdml_op_data_driver.h"

namespace oceanbase {
namespace sql {
class ObPxMultiPartUpdateOpInput : public ObPxMultiPartModifyOpInput {
  OB_UNIS_VERSION_V(1);

public:
  ObPxMultiPartUpdateOpInput(ObExecContext& ctx, const ObOpSpec& spec) : ObPxMultiPartModifyOpInput(ctx, spec)
  {}
  int init(ObTaskInfo& task_info) override
  {
    return ObPxMultiPartModifyOpInput::init(task_info);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartUpdateOpInput);
};

class ObPxMultiPartUpdateSpec : public ObTableModifySpec {
  OB_UNIS_VERSION_V(1);

public:
  ObPxMultiPartUpdateSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObTableModifySpec(alloc, type),
        row_desc_(),
        table_desc_(),
        updated_column_ids_(alloc),
        updated_column_infos_(alloc),
        old_row_exprs_(&alloc),
        new_row_exprs_(&alloc)
  {}

  int set_updated_column_info(
      int64_t array_index, uint64_t column_id, uint64_t projector_index, bool auto_filled_timestamp);

  const common::ObIArrayWrap<ColumnContent>& get_assign_columns() const
  {
    return updated_column_infos_;
  }

  int init_updated_column_count(common::ObIAllocator& allocator, int64_t count);

  virtual bool is_pdml_operator() const override
  {
    return true;
  }

public:
  ObDMLOpRowDesc row_desc_;
  ObDMLOpTableDesc table_desc_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> updated_column_ids_;
  common::ObFixedArray<ColumnContent, common::ObIAllocator> updated_column_infos_;
  ExprFixedArray old_row_exprs_;
  ExprFixedArray new_row_exprs_;
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartUpdateSpec);
};

class ObPxMultiPartUpdateOp : public ObDMLOpDataReader, public ObDMLOpDataWriter, public ObTableModifyOp {
  OB_UNIS_VERSION(1);

public:
  /**ObPDMLOpRowIteratorWrapper**/
  class ObPDMLOpRowIteratorWrapper : public common::ObNewRowIterator {
  public:
    ObPDMLOpRowIteratorWrapper(common::ObPartitionKey& pkey, storage::ObDMLBaseParam& dml_param,
        ObPDMLOpRowIterator& iter, ObPxMultiPartUpdateOp& op)
        : pkey_(pkey), dml_param_(dml_param), iter_(iter), op_(op)
    {}
    ~ObPDMLOpRowIteratorWrapper()
    {}
    virtual void reset() override
    {}
    int get_next_row(common::ObNewRow*& row) override;

  private:
    common::ObPartitionKey& pkey_;
    storage::ObDMLBaseParam& dml_param_;
    ObPDMLOpRowIterator& iter_;
    ObPxMultiPartUpdateOp& op_;
  };

public:
  ObPxMultiPartUpdateOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObTableModifyOp(exec_ctx, spec, input),
        data_driver_(exec_ctx.get_eval_ctx(), exec_ctx.get_allocator(), op_monitor_info_),
        has_got_old_row_(false),
        found_rows_(0),
        changed_rows_(0),
        affected_rows_(0),
        old_row_(),
        new_row_()
  {}
  virtual ~ObPxMultiPartUpdateOp() = default;

public:
  virtual bool has_foreign_key() const
  {
    return false;
  }

  int read_row(ObExecContext& ctx, const ObExprPtrIArray*& row, int64_t& part_id) override;

  int write_rows(ObExecContext& ctx, common::ObPartitionKey& pkey, ObPDMLOpRowIterator& iterator) override;

  virtual int inner_get_next_row();
  virtual int inner_open();
  virtual int inner_close();

  // for row multiplexing
  int get_next_row(common::ObPartitionKey& pkey, storage::ObDMLBaseParam& dml_param, ObPDMLOpRowIterator& iter,
      common::ObNewRow*& row);

  void inc_affected_rows()
  {
    affected_rows_++;
  }
  void inc_found_rows()
  {
    found_rows_++;
  }
  void inc_changed_rows()
  {
    changed_rows_++;
  }
  int64_t get_found_rows() const
  {
    return found_rows_;
  }

private:
  int fill_dml_base_param(uint64_t index_tid, ObSQLSessionInfo& my_session, const ObPhysicalPlan& my_phy_plan,
      const ObPhysicalPlanCtx& my_plan_ctx, storage::ObDMLBaseParam& dml_param) const;
  int process_row();

private:
  ObPDMLOpDataDriver data_driver_;
  bool has_got_old_row_;
  int64_t found_rows_;
  int64_t changed_rows_;
  int64_t affected_rows_;
  common::ObNewRow old_row_;
  common::ObNewRow new_row_;
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartUpdateOp);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_SQL_ENGINE_PDML_PX_MULTI_PART_UPDATE_OP_H_ */
