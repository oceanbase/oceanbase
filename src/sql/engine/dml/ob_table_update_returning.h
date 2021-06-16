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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_TABLE_UPDATE_RETURNING_H_
#define OCEANBASE_SQL_ENGINE_DML_OB_TABLE_UPDATE_RETURNING_H_

#include "sql/engine/dml/ob_table_update.h"

namespace oceanbase {
namespace sql {
class ObTableUpdateReturningInput : public ObTableModifyInput {
  friend class ObTableUpdateReturning;

public:
  ObTableUpdateReturningInput() : ObTableModifyInput()
  {}
  virtual ~ObTableUpdateReturningInput()
  {}
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_UPDATE_RETURNING;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableUpdateReturningInput);
};

class ObTableUpdateReturning : public ObTableUpdate {
  class ObTableUpdateReturningCtx;
  OB_UNIS_VERSION(1);

public:
  explicit ObTableUpdateReturning(common::ObIAllocator& alloc)
      : ObTableUpdate(alloc), updated_projector_(NULL), updated_projector_size_(0)
  {}
  ~ObTableUpdateReturning()
  {}
  int inner_open(ObExecContext& ctx) const;
  int get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  void set_updated_projector(int32_t* projector, int64_t projector_size)
  {
    updated_projector_ = projector;
    updated_projector_size_ = projector_size;
  }
  void reset();
  void reuse();

private:
  int init_op_ctx(ObExecContext& ctx) const;
  DISALLOW_COPY_AND_ASSIGN(ObTableUpdateReturning);

private:
  int32_t* updated_projector_;
  int64_t updated_projector_size_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_DML_OB_TABLE_UPDATE_RETURNING_H_ */
