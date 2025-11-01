/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "observer/table_load/plan/ob_table_load_merge_op.h"
#include "observer/table_load/plan/ob_table_load_plan.h"
#include "observer/table_load/plan/ob_table_load_table_op.h"
#include "observer/table_load/plan/ob_table_load_write_op.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadStoreTableCtx;

template <typename TableOpType>
class ObTableLoadTableOpBuilder
{
public:
  // TableOp with write_input
  static int build(ObTableLoadPlan *plan, ObTableLoadStoreTableCtx *store_table_ctx,
                   const ObTableLoadWriteType::Type write_type, TableOpType *&table_op);
  // TableOp with channel_input
  static int build(ObTableLoadPlan *plan, ObTableLoadStoreTableCtx *store_table_ctx,
                   TableOpType *&table_op);
};

template <typename TableOpType>
int ObTableLoadTableOpBuilder<TableOpType>::build(ObTableLoadPlan *plan,
                                                  ObTableLoadStoreTableCtx *store_table_ctx,
                                                  const ObTableLoadWriteType::Type write_type,
                                                  TableOpType *&table_op)
{
  int ret = OB_SUCCESS;
  table_op = nullptr;
  if (OB_FAIL(plan->alloc_table_op(table_op, store_table_ctx))) {
    SERVER_LOG(WARN, "fail to alloc table op", KR(ret));
  } else {
    // 构建TableOp内部成员
    ObTableLoadTableOpOpenOp *open_op = nullptr;
    ObTableLoadWriteOp *write_op = nullptr;
    ObTableLoadMergeDataOp2 *merge_data_op = nullptr;
    ObTableLoadTableOpCloseOp *close_op = nullptr;
    // 1. open_op
    if (OB_FAIL(table_op->alloc_op(open_op, table_op))) {
      SERVER_LOG(WARN, "fail to alloc open op", KR(ret));
    }
    // 2. write_op
    else if (OB_FAIL(ObTableLoadWriteOp::build(table_op, write_type, write_op))) {
      SERVER_LOG(WARN, "fail to build write op", KR(ret));
    }
    // 3. merge_data_op
    else if (OB_FAIL(ObTableLoadMergeDataOp2::build(table_op, merge_data_op))) {
      SERVER_LOG(WARN, "fail to build write op", KR(ret));
    }
    // 4. close_op
    else if (OB_FAIL(table_op->alloc_op(close_op, table_op))) {
      SERVER_LOG(WARN, "fail to alloc close op", KR(ret));
    }
  }
  return ret;
}

template <typename TableOpType>
int ObTableLoadTableOpBuilder<TableOpType>::build(ObTableLoadPlan *plan,
                                                  ObTableLoadStoreTableCtx *store_table_ctx,
                                                  TableOpType *&table_op)
{
  int ret = OB_SUCCESS;
  table_op = nullptr;
  if (OB_FAIL(plan->alloc_table_op(table_op, store_table_ctx))) {
    SERVER_LOG(WARN, "fail to alloc table op", KR(ret));
  } else {
    // 构建TableOp内部成员
    ObTableLoadTableOpOpenOp *open_op = nullptr;
    ObTableLoadMergeDataOp2 *merge_data_op = nullptr;
    ObTableLoadTableOpCloseOp *close_op = nullptr;
    // 1. open_op
    if (OB_FAIL(table_op->alloc_op(open_op, table_op))) {
      SERVER_LOG(WARN, "fail to alloc open op", KR(ret));
    }
    // 2. merge_data_op
    else if (OB_FAIL(ObTableLoadMergeDataOp2::build(table_op, merge_data_op))) {
      SERVER_LOG(WARN, "fail to build write op", KR(ret));
    }
    // 3. close_op
    else if (OB_FAIL(table_op->alloc_op(close_op, table_op))) {
      SERVER_LOG(WARN, "fail to alloc close op", KR(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
