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

#ifndef DEV_SRC_SQL_ENGINE_TABLE_OB_MULTI_TABLE_INSERT_OP_
#define DEV_SRC_SQL_ENGINE_TABLE_OB_MULTI_TABLE_INSERT_OP_
#include "lib/allocator/ob_allocator.h"
#include "sql/engine/dml/ob_table_insert_op.h"
namespace oceanbase {
namespace sql {
class ObMultiPartInsertSpec : public ObTableInsertSpec, public ObMultiDMLInfo {
  OB_UNIS_VERSION_V(1);

public:
  ObMultiPartInsertSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObTableInsertSpec(alloc, type), ObMultiDMLInfo(alloc)
  {}

  virtual ~ObMultiPartInsertSpec(){};

  virtual bool has_foreign_key() const override
  {
    return sesubplan_has_foreign_key();
  }
  virtual bool is_multi_dml() const
  {
    return true;
  }
};

class ObMultiPartInsertOp : public ObTableInsertOp, public ObMultiDMLCtx {
public:
  static const int64_t INSERT_OP = 0;
  static const int64_t DML_OP_CNT = 1;

public:
  ObMultiPartInsertOp(ObExecContext& ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObTableInsertOp(ctx, spec, input), ObMultiDMLCtx(ctx.get_allocator())
  {}
  ~ObMultiPartInsertOp()
  {}
  virtual int inner_open();
  virtual int get_next_row() override;
  virtual int inner_close();
  int shuffle_insert_row(bool& got_row);
  virtual void destroy() override
  {
    ObTableInsertOp::destroy();
    ObMultiDMLCtx::destroy_ctx();
  }
};
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_ENGINE_TABLE_OB_MULTI_TABLE_INSERT_OP_ */
