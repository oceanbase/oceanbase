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

#ifndef OBDEV_SRC_SQL_ENGINE_DML_OB_MULTI_TABLE_REPLACE_OP_H_
#define OBDEV_SRC_SQL_ENGINE_DML_OB_MULTI_TABLE_REPLACE_OP_H_
#include "sql/engine/dml/ob_table_replace_op.h"
#include "sql/engine/dml/ob_duplicated_key_checker.h"
namespace oceanbase {
namespace common {
class ObIAllocator;
}  // namespace common
namespace sql {
class ObMultiTableReplaceSpec : public ObTableReplaceSpec, public ObMultiDMLInfo {
  OB_UNIS_VERSION_V(1);

public:
  ObMultiTableReplaceSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObTableReplaceSpec(alloc, type), ObMultiDMLInfo(alloc)
  {}

  virtual ~ObMultiTableReplaceSpec(){};

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
};

class ObMultiTableReplaceOp : public ObTableReplaceOp, public ObMultiDMLCtx {
public:
  static const int64_t DELETE_OP = 0;
  static const int64_t INSERT_OP = 1;
  static const int64_t DML_OP_CNT = 2;

public:
  ObMultiTableReplaceOp(ObExecContext& ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObTableReplaceOp(ctx, spec, input),
        ObMultiDMLCtx(ctx.get_allocator()),
        replace_row_store_(&ctx.get_allocator()),
        dupkey_checker_ctx_(ctx.get_allocator(), NULL /*expr_ctx_*/, mini_task_executor_, NULL /*checker_row_store*/,
            &eval_ctx_, &replace_row_store_)
  {}
  ~ObMultiTableReplaceOp()
  {}

  int shuffle_final_delete_row(ObExecContext& ctx, const ObExprPtrIArray& delete_row);
  int shuffle_final_insert_row(ObExecContext& ctx, const ObExprPtrIArray& insert_row);
  virtual int inner_open() override;
  virtual int inner_close() override;
  int load_replace_row(ObChunkDatumStore& row_store);
  int shuffle_replace_row(bool& got_row);
  virtual void destroy() override
  {
    ObTableReplaceOp::destroy();
    ObMultiDMLCtx::destroy_ctx();
    dupkey_checker_ctx_.destroy();
    replace_row_store_.reset();
  }

private:
  ObChunkDatumStore replace_row_store_;
  ObDupKeyCheckerCtx dupkey_checker_ctx_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_ENGINE_DML_OB_MULTI_TABLE_REPLACE_OP_H_ */
