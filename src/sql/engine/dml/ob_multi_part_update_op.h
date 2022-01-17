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

#ifndef DEV_SRC_SQL_ENGINE_DML_OB_MULTI_PART_UPDATE_OP_
#define DEV_SRC_SQL_ENGINE_DML_OB_MULTI_PART_UPDATE_OP_
#include "sql/engine/dml/ob_table_update_op.h"
namespace oceanbase {
namespace sql {

class ObMultiPartUpdateSpec : public ObTableUpdateSpec, public ObMultiDMLInfo {
  OB_UNIS_VERSION_V(1);

public:
  ObMultiPartUpdateSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObTableUpdateSpec(alloc, type), ObMultiDMLInfo(alloc)
  {}

  virtual ~ObMultiPartUpdateSpec(){};

  virtual bool has_foreign_key() const override
  {
    return sesubplan_has_foreign_key();
  }
  virtual bool is_multi_dml() const
  {
    return true;
  }
};

class ObMultiPartUpdateOp : public ObTableUpdateOp, public ObMultiDMLCtx {
public:
  static const int64_t DELETE_OP = 0;
  static const int64_t INSERT_OP = 1;
  static const int64_t UPDATE_OP = 2;
  static const int64_t DML_OP_CNT = 3;  // multi table update mybe contain 3 kinds dml op
public:
  ObMultiPartUpdateOp(ObExecContext& ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObTableUpdateOp(ctx, spec, input),
        ObMultiDMLCtx(ctx.get_allocator()),
        found_rows_(0),
        changed_rows_(0),
        affected_rows_(0)
  {}
  ~ObMultiPartUpdateOp()
  {}

public:
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row();
  virtual int get_next_row() override;
  int shuffle_update_row(bool& got_row);
  int merge_implicit_cursor(
      const common::ObNewRow& full_row, bool is_update, bool client_found_rows, ObPhysicalPlanCtx& plan_ctx) const;
  virtual void destroy() override
  {
    ObTableModifyOp::destroy();
    ObMultiDMLCtx::destroy_ctx();
  }
  void inc_found_rows()
  {
    ++found_rows_;
  }
  void inc_changed_rows()
  {
    ++changed_rows_;
  }
  void inc_affected_rows()
  {
    ++affected_rows_;
  }

private:
  int64_t found_rows_;
  int64_t changed_rows_;
  int64_t affected_rows_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* SQL_ENGINE_DML_OB_MULTI_TABLE_UPDATE_H_ */
