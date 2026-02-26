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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/plan/ob_table_load_write_op.h"
#include "observer/table_load/dag/ob_table_load_dag_direct_write.h"
#include "observer/table_load/dag/ob_table_load_dag_pre_sort_write.h"
#include "observer/table_load/dag/ob_table_load_dag_store_write.h"

namespace oceanbase
{
namespace observer
{
int ObTableLoadWriteOp::build(ObTableLoadTableOp *table_op,
                              const ObTableLoadWriteType::Type write_type,
                              ObTableLoadWriteOp *&write_op)
{
  int ret = OB_SUCCESS;
  write_op = nullptr;
  switch (write_type) {
    case ObTableLoadWriteType::DIRECT_WRITE: {
      ObTableLoadDirectWriteOp *direct_write_op = nullptr;
      if (OB_FAIL(table_op->alloc_op(direct_write_op))) {
        LOG_WARN("fail to alloc op", KR(ret));
      } else {
        write_op = direct_write_op;
      }
      break;
    }
    case ObTableLoadWriteType::STORE_WRITE: {
      ObTableLoadStoreWriteOp *store_write_op = nullptr;
      if (OB_FAIL(table_op->alloc_op(store_write_op))) {
        LOG_WARN("fail to alloc op", KR(ret));
      } else {
        write_op = store_write_op;
      }
      break;
    }
    case ObTableLoadWriteType::PRE_SORT_WRITE: {
      ObTableLoadPreSortWriteOp *pre_sort_write_op = nullptr;
      if (OB_FAIL(table_op->alloc_op(pre_sort_write_op))) {
        LOG_WARN("fail to alloc op", KR(ret));
      } else {
        write_op = pre_sort_write_op;
      }
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected write type", KR(ret), K(write_type));
      break;
  }
  return ret;
}

// direct_write
ObTableLoadDirectWriteOp::ObTableLoadDirectWriteOp(ObTableLoadTableBaseOp *parent)
  : ObTableLoadWriteOp(parent), write_channel_(nullptr)
{
  op_type_ = ObTableLoadOpType::DIRECT_WRITE_OP;
}

ObTableLoadDirectWriteOp::~ObTableLoadDirectWriteOp()
{
  ObTableLoadDirectWriteOpFinishTask::reset_op(this);
}

// store_write
ObTableLoadStoreWriteOp::ObTableLoadStoreWriteOp(ObTableLoadTableBaseOp *parent)
  : ObTableLoadWriteOp(parent), write_channel_(nullptr)
{
  op_type_ = ObTableLoadOpType::STORE_WRITE_OP;
}

ObTableLoadStoreWriteOp::~ObTableLoadStoreWriteOp()
{
  ObTableLoadStoreWriteOpFinishTask::reset_op(this);
}

// pre_sort_write
ObTableLoadPreSortWriteOp::ObTableLoadPreSortWriteOp(ObTableLoadTableBaseOp *parent)
  : ObTableLoadWriteOp(parent), write_channel_(nullptr)
{
  op_type_ = ObTableLoadOpType::PRE_SORT_WRITE_OP;
}

ObTableLoadPreSortWriteOp::~ObTableLoadPreSortWriteOp()
{
  ObTableLoadPreSortWriteOpFinishTask::reset_op(this);
}

} // namespace observer
} // namespace oceanbase
