/** * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SRC_SQL_ENGINE_JOIN_OB_RESCAN_LIMIT_OP_H_
#define SRC_SQL_ENGINE_JOIN_OB_RESCAN_LIMIT_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_temp_column_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"

namespace oceanbase
{
namespace sql
{

class ObRescanLimitSpec: public ObOpSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObRescanLimitSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
    rescan_limit_(0)
  {}

public:
  uint64_t rescan_limit_;
};

class ObRescanLimitOp : public ObOperator
{
public:
  ObRescanLimitOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
    iter_end_(false),
    current_rescan_count_(0)
  {}

  // virtual int rescan() override;
  virtual int inner_get_next_row() override { return common::OB_NOT_IMPLEMENT; }
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override {}

public:
  bool iter_end_;
  uint64_t current_rescan_count_;
};

} // namespace sql
} // namespace oceanbase

#endif /*SRC_SQL_ENGINE_JOIN_OB_RESCAN_LIMIT_OP_H_*/
