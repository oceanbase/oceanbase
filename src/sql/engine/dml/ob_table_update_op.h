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

#ifndef OCEANBASE_DML_OB_TABLE_UPDATE_OP_H_
#define OCEANBASE_DML_OB_TABLE_UPDATE_OP_H_

#include "sql/engine/dml/ob_table_modify_op.h"

namespace oceanbase {
namespace sql {

class ObTableUpdateOpInput : public ObTableModifyOpInput {
  OB_UNIS_VERSION_V(1);

public:
  ObTableUpdateOpInput(ObExecContext& ctx, const ObOpSpec& spec) : ObTableModifyOpInput(ctx, spec)
  {}
  virtual int init(ObTaskInfo& task_info) override
  {
    return ObTableModifyOpInput::init(task_info);
  }
};

class ObTableUpdateSpec : public ObTableModifySpec {
  OB_UNIS_VERSION_V(1);

public:
  ObTableUpdateSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type);
  ~ObTableUpdateSpec();

  int set_updated_column_info(
      int64_t array_index, uint64_t column_id, uint64_t projector_index, bool auto_filled_timestamp);

  const common::ObIArrayWrap<ColumnContent>& get_assign_columns() const
  {
    return updated_column_infos_;
  }

  int init_updated_column_count(common::ObIAllocator& allocator, int64_t count);

  INHERIT_TO_STRING_KV("table_modify_spec", ObTableModifySpec, K_(updated_column_ids), K_(updated_column_infos),
      K_(old_row), K_(new_row));

public:
  common::ObFixedArray<uint64_t, common::ObIAllocator> updated_column_ids_;
  common::ObFixedArray<ColumnContent, common::ObIAllocator> updated_column_infos_;
  ExprFixedArray old_row_;
  ExprFixedArray new_row_;
};

class ObTableUpdateOp : public ObTableModifyOp {
public:
  friend ObTableModifyOp;

  ObTableUpdateOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);

  int inner_open() override;
  int rescan() override;
  int switch_iterator() override;
  int inner_get_next_row() override;
  int prepare_next_storage_row(const ObExprPtrIArray*& output) override;
  int inner_close() override;
  void destroy() override
  {
    part_infos_.reset();
    ObTableModifyOp::destroy();
  }

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

protected:
  bool check_row_whether_changed() const;
  int update_rows(int64_t& affected_rows);
  // update all rows in open phase,
  // the update returning will override this to do update in get_next_row().
  virtual int do_table_update();

  // update single row
  int do_row_update();

protected:
  bool has_got_old_row_;
  int64_t found_rows_;
  int64_t changed_rows_;
  int64_t affected_rows_;
  storage::ObDMLBaseParam dml_param_;
  common::ObPartitionKey part_key_;
  int64_t part_row_cnt_;
  common::ObSEArray<DMLPartInfo, 4> part_infos_;
  int64_t cur_part_idx_;

  // Row has no change also returned in prepare_next_storage_row(), since the
  // returning need it, need_update_ indicate that need update row before returning.
  bool need_update_;

  // project MY_SPEC.old_row_, MY_SPEC.new_row_ to old_row_, new_row_ for multi part update
  // which call storage update service per row.
  common::ObNewRow old_row_;
  common::ObNewRow new_row_;
  ObSEArray<ForeignKeyHandle::ObFkRowResInfo, 8> old_row_res_infos_;
  ObSEArray<ForeignKeyHandle::ObFkRowResInfo, 8> new_row_res_infos_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_DML_OB_TABLE_UPDATE_OP_H_
