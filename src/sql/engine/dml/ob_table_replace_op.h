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

#ifndef OCEANBASE_SQL_OB_TABLE_REPLACE_OP_
#define OCEANBASE_SQL_OB_TABLE_REPLACE_OP_

#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/dml/ob_table_delete_op.h"
#include "storage/ob_partition_service.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/executor/ob_task_executor_ctx.h"

namespace oceanbase {
namespace sql {

class ObTableReplaceSpec : public ObTableModifySpec {
  OB_UNIS_VERSION_V(1);

public:
  ObTableReplaceSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObTableModifySpec(alloc, type), only_one_unique_key_(false), table_column_exprs_(alloc)
  {}

  bool only_one_unique_key_;
  ExprFixedArray table_column_exprs_;  // table column exprs
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableReplaceSpec);
};

class ObTableReplaceOp : public ObTableModifyOp {
public:
  ObTableReplaceOp(ObExecContext& ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObTableModifyOp(ctx, spec, input),
        row2exprs_projector_(ctx.get_allocator()),
        record_(0),
        affected_rows_(0),
        delete_count_(0),
        dml_param_(),
        part_infos_()
  {}
  virtual ~ObTableReplaceOp()
  {}

  virtual int inner_open() override;
  virtual int rescan() override;
  virtual int inner_get_next_row() override;
  virtual void destroy() override;
  int do_table_replace();

protected:
  int check_values(bool& is_equal) const;

private:
  int try_insert(ObSQLSessionInfo& my_session, const ObPartitionKey& part_key,
      storage::ObPartitionService& partition_service, const storage::ObDMLBaseParam& dml_param,
      ObNewRowIterator*& dup_rows_iter);
  int do_replace(ObSQLSessionInfo& my_session, const ObPartitionKey& part_key,
      storage::ObPartitionService& partition_service, const ObNewRowIterator& dup_rows_iter,
      const storage::ObDMLBaseParam& dml_param);
  int scan_row(ObSQLSessionInfo& my_session, const ObPartitionKey& part_key,
      storage::ObPartitionService& partition_service, storage::ObTableScanParam& scan_param,
      const common::ObNewRowIterator& dup_rows_iter, common::ObNewRowIterator** result);

protected:
  storage::ObRow2ExprsProjector row2exprs_projector_;
  int64_t record_;
  int64_t affected_rows_;
  int64_t delete_count_;
  storage::ObDMLBaseParam dml_param_;
  common::ObSEArray<DMLPartInfo, 1> part_infos_;
};

class ObTableReplaceOpInput : public ObTableModifyOpInput {
  OB_UNIS_VERSION_V(1);

public:
  ObTableReplaceOpInput(ObExecContext& ctx, const ObOpSpec& spec) : ObTableModifyOpInput(ctx, spec)
  {}
  int init(ObTaskInfo& task_info) override
  {
    return ObTableModifyOpInput::init(task_info);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableReplaceOpInput);
};

}  // end namespace sql
}  // end namespace oceanbase

#endif
