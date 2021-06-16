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

#ifndef OCEANBASE_DML_OB_TABLE_INSERT_UP_OP_H_
#define OCEANBASE_DML_OB_TABLE_INSERT_UP_OP_H_

#include "sql/engine/dml/ob_table_modify_op.h"

namespace oceanbase {
namespace sql {

class ObTableInsertUpOpInput : public ObTableModifyOpInput {
  OB_UNIS_VERSION_V(1);

public:
  ObTableInsertUpOpInput(ObExecContext& ctx, const ObOpSpec& spec) : ObTableModifyOpInput(ctx, spec)
  {}
  virtual int init(ObTaskInfo& task_info) override
  {
    return ObTableModifyOpInput::init(task_info);
  }
};

class ObTableInsertUpSpec : public ObTableModifySpec {
  OB_UNIS_VERSION_V(1);

public:
  ObTableInsertUpSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type);
  ~ObTableInsertUpSpec()
  {}

  int set_updated_column_info(
      int64_t array_index, uint64_t column_id, uint64_t projector_index, bool auto_filled_timestamp);
  int init_updated_column_count(common::ObIAllocator& allocator, int64_t count);

  INHERIT_TO_STRING_KV("table_modify_spec", ObTableModifySpec, K_(scan_column_ids), K_(update_related_column_ids),
      K_(updated_column_ids), K_(updated_column_infos), K_(insert_row), K_(old_row), K_(new_row));

public:
  common::ObFixedArray<uint64_t, common::ObIAllocator> scan_column_ids_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> update_related_column_ids_;

  // for assignment column
  common::ObFixedArray<uint64_t, common::ObIAllocator> updated_column_ids_;
  common::ObFixedArray<ColumnContent, common::ObIAllocator> updated_column_infos_;

  // row for insert
  ExprFixedArray insert_row_;
  // old && new row for update
  ExprFixedArray old_row_;
  ExprFixedArray new_row_;
};

class ObTableInsertUpOp : public ObTableModifyOp {
public:
  ObTableInsertUpOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);

  int inner_open() override;
  int inner_get_next_row() override;
  int rescan() override;
  int prepare_next_storage_row(const ObExprPtrIArray*& output) override;
  int inner_close() override;
  void destroy() override
  {
    row2exprs_projector_.destroy();
    ObTableModifyOp::destroy();
  }

protected:
  int init_autoinc_param(const common::ObPartitionKey& pkey);

  int calc_insert_row();

  int process_on_duplicate_update(common::ObNewRowIterator* duplicated_rows, storage::ObDMLBaseParam& dml_param,
      int64_t& n_duplicated_rows, int64_t& affected_rows, int64_t& update_count);

  int build_scan_param(const share::ObPartitionReplicaLocation& part_replica, const common::ObNewRow* dup_row,
      storage::ObTableScanParam& scan_param);

  int calc_update_rows(bool& is_row_changed);

  int calc_new_row(bool& is_row_changed);

  int update_auto_increment(const ObExpr& expr, const uint64_t cid, bool& is_auto_col_changed);

private:
  int do_table_insert_up();
  void reset();

protected:
  storage::ObRow2ExprsProjector row2exprs_projector_;
  int64_t found_rows_;
  int64_t get_count_;
  // insert row (project from MY_SPEC.insert_row_)
  common::ObNewRow insert_row_;

  storage::ObDMLBaseParam dml_param_;
  common::ObSEArray<DMLPartInfo, 1> part_infos_;
  bool cur_gi_task_iter_end_;
};

}  // end namespace sql
}  // end namespace oceanbase
#endif  // OCEANBASE_DML_OB_TABLE_INSERT_UP_OP_H_
