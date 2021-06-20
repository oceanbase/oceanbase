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

#ifndef OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_TRANSFORMATION_H_
#define OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_TRANSFORMATION_H_

#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_multi_children_phy_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "common/row/ob_row.h"

namespace oceanbase {
namespace sql {
class ObTempTableTransformation : public ObMultiChildrenPhyOperator {
  OB_UNIS_VERSION_V(1);

protected:
  class ObTempTableTransformationCtx : public ObPhyOperatorCtx {
    friend class ObTempTableTransformation;

  public:
    explicit ObTempTableTransformationCtx(ObExecContext& ctx) : ObPhyOperatorCtx(ctx)
    {}
    virtual ~ObTempTableTransformationCtx()
    {}
    virtual void destroy()
    {
      ObPhyOperatorCtx::destroy_base();
    }
  };

public:
  explicit ObTempTableTransformation(common::ObIAllocator& alloc) : ObMultiChildrenPhyOperator(alloc)
  {}
  ~ObTempTableTransformation()
  {}

  virtual void reset();
  virtual void reuse();

  virtual int rescan(ObExecContext& ctx) const;
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int init_op_ctx(ObExecContext& ctx) const;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int inner_close(ObExecContext& ctx) const;

  DISALLOW_COPY_AND_ASSIGN(ObTempTableTransformation);
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_TRANSFORMATION_H_ */
