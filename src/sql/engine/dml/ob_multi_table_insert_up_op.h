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

#ifndef OBDEV_SRC_SQL_ENGINE_DML_OB_MULTI_TABLE_INSERT_UP_OP_H_
#define OBDEV_SRC_SQL_ENGINE_DML_OB_MULTI_TABLE_INSERT_UP_OP_H_

#include "lib/allocator/ob_allocator.h"
#include "sql/engine/dml/ob_table_insert_up_op.h"
#include "sql/engine/dml/ob_duplicated_key_checker.h"
namespace oceanbase {
namespace sql {
class ObMultiTableInsertUpSpec : public ObTableInsertUpSpec, public ObMultiDMLInfo {
  OB_UNIS_VERSION_V(1);

public:
  ObMultiTableInsertUpSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObTableInsertUpSpec(alloc, type), ObMultiDMLInfo(alloc), table_column_exprs_(alloc)
  {}

  virtual ~ObMultiTableInsertUpSpec(){};

  virtual bool has_foreign_key() const override
  {
    return sesubplan_has_foreign_key();
  }
  virtual bool is_multi_dml() const
  {
    return true;
  }

public:
  ObDuplicatedKeyChecker duplicate_key_checker_;
  ExprFixedArray table_column_exprs_;
};

class ObMultiTableInsertUpOp : public ObTableInsertUpOp, public ObMultiDMLCtx {
public:
  static const int64_t DELETE_OP = 0;
  static const int64_t INSERT_OP = 1;
  static const int64_t UPDATE_OP = 2;
  static const int64_t DML_OP_CNT = 3;

public:
  ObMultiTableInsertUpOp(ObExecContext& ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObTableInsertUpOp(ctx, spec, input),
        ObMultiDMLCtx(ctx.get_allocator()),
        replace_row_store_(&ctx.get_allocator()),
        dupkey_checker_ctx_(ctx.get_allocator(), NULL /*expr_ctx_*/, mini_task_executor_, NULL /*checker_row_store*/,
            &eval_ctx_, &replace_row_store_),
        changed_rows_(0),
        affected_rows_(0),
        duplicate_rows_(0)
  {}
  ~ObMultiTableInsertUpOp()
  {}

  int shuffle_final_delete_row(ObExecContext& ctx, const ObExprPtrIArray& delete_row);
  int shuffle_final_insert_row(ObExecContext& ctx, const ObExprPtrIArray& insert_row);
  virtual int inner_open() override;
  virtual int inner_close() override;
  int load_insert_up_row();

  int shuffle_insert_up_row(bool& got_row);
  int shuffle_insert_row(const ObChunkDatumStore::StoredRow& insert_row);
  int shuffle_insert_row(const ObExprPtrIArray& insert_exprs, const ObChunkDatumStore::StoredRow& insert_row);
  int shuffle_update_row(const ObChunkDatumStore::StoredRow& duplicated_row);
  virtual void destroy() override
  {
    ObTableInsertUpOp::destroy();
    ObMultiDMLCtx::destroy_ctx();
    dupkey_checker_ctx_.destroy();
    replace_row_store_.reset();
  }
  void inc_changed_rows()
  {
    ++changed_rows_;
  }
  void inc_affected_rows()
  {
    ++affected_rows_;
  }
  int convert_exprs_to_stored_row(common::ObIAllocator& allocator, ObEvalCtx& eval_ctx, const ObExprPtrIArray& exprs,
      ObChunkDatumStore::StoredRow*& new_row);

private:
  ObChunkDatumStore replace_row_store_;
  ObDupKeyCheckerCtx dupkey_checker_ctx_;
  int64_t changed_rows_;
  int64_t affected_rows_;
  int64_t duplicate_rows_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_ENGINE_DML_OB_MULTI_TABLE_INSERT_UP_OP_H_ */
