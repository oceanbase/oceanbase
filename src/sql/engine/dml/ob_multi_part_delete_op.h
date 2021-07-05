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

#ifndef DEV_SRC_SQL_ENGINE_DML_OB_MULTI_PART_DELETE_OP_
#define DEV_SRC_SQL_ENGINE_DML_OB_MULTI_PART_DELETE_OP_

#include "sql/engine/dml/ob_table_delete_op.h"
namespace oceanbase {
namespace sql {
class ObMultiPartDeleteSpec : public ObTableDeleteSpec, public ObMultiDMLInfo {
  OB_UNIS_VERSION_V(1);

public:
  ObMultiPartDeleteSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObTableDeleteSpec(alloc, type), ObMultiDMLInfo(alloc)
  {}

  virtual ~ObMultiPartDeleteSpec(){};

  virtual bool has_foreign_key() const override
  {
    return sesubplan_has_foreign_key();
  }
  virtual bool is_multi_dml() const
  {
    return true;
  }
};

class ObMultiPartDeleteOp : public ObTableDeleteOp, public ObMultiDMLCtx {
public:
  static const int64_t DELETE_OP = 0;
  // There is only one dml operation in multi table delete
  static const int64_t DML_OP_CNT = 1;

public:
  ObMultiPartDeleteOp(ObExecContext& ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObTableDeleteOp(ctx, spec, input), ObMultiDMLCtx(ctx.get_allocator())
  {}
  ~ObMultiPartDeleteOp()
  {}

public:
  virtual int inner_open() override;
  virtual int get_next_row() override;
  virtual int inner_close();
  int shuffle_delete_row(bool& got_row);
  virtual int inner_get_next_row();
  virtual void destroy() override
  {
    ObTableDeleteOp::destroy();
    ObMultiDMLCtx::destroy_ctx();
  }
};
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_ENGINE_DML_OB_MULTI_PART_DELETE_OP_*/
