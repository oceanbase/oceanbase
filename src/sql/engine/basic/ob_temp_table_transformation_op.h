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

#ifndef OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_TRANSFORMATION_OP_H_
#define OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_TRANSFORMATION_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_no_children_phy_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/basic/ob_ra_row_store.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "sql/executor/ob_interm_result_manager.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/engine/ob_physical_plan_ctx.h"

namespace oceanbase {
namespace sql {
class ObExecContext;
class ObTempTableTransformationOpSpec : public ObOpSpec {
public:
  OB_UNIS_VERSION_V(1);

public:
  ObTempTableTransformationOpSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type) : ObOpSpec(alloc, type)
  {}
  virtual ~ObTempTableTransformationOpSpec()
  {}
  DECLARE_VIRTUAL_TO_STRING;
};

class ObTempTableTransformationOp : public ObOperator {
public:
  ObTempTableTransformationOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObOperator(exec_ctx, spec, input)
  {}
  ~ObTempTableTransformationOp()
  {}

  virtual int inner_open() override;
  virtual int rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_close() override;
  virtual void destroy() override;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_TRANSFORMATION_OP_H_ */
