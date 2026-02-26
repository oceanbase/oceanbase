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

#include "observer/table_load/plan/ob_table_load_merge_op.h"
#include "observer/table_load/dag/ob_table_load_dag_compact_table_task.h"
#include "observer/table_load/dag/ob_table_load_dag_insert_sstable_task.h"
#include "observer/table_load/dag/ob_table_load_dag_mem_sort.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"

namespace oceanbase
{
namespace observer
{
int ObTableLoadMergeDataOp2::build(ObTableLoadTableOp *table_op,
                                   ObTableLoadMergeDataOp2 *&merge_data_op)
{
  int ret = OB_SUCCESS;
  merge_data_op = nullptr;
  ObTableLoadPlan *plan = table_op->get_plan();
  if (OB_FAIL(table_op->alloc_op(merge_data_op))) {
    LOG_WARN("fail to alloc op", KR(ret));
  } else {
    // 创建下级op
    ObTableLoadMemSortOp *mem_sort_op = nullptr;
    ObTableLoadCompactDataOp *compact_data_op = nullptr;
    ObTableLoadInsertSSTableOp *insert_sstable_op = nullptr;
    // 1. mem_sort_op
    if (OB_FAIL(merge_data_op->alloc_op(mem_sort_op))) {
      LOG_WARN("fail to alloc op", KR(ret));
    }
    // 2. compact_data_op
    else if (OB_FAIL(merge_data_op->alloc_op(compact_data_op))) {
      LOG_WARN("fail to alloc op", KR(ret));
    }
    // 3. insert_sstable_op
    else if (OB_FAIL(merge_data_op->alloc_op(insert_sstable_op))) {
      LOG_WARN("fail to alloc op", KR(ret));
    }
  }
  return ret;
}

/**
 * ObTableLoadMemSortOp
 */

ObTableLoadMemSortOp::ObTableLoadMemSortOp(ObTableLoadTableBaseOp *parent)
  : ObTableLoadTableBaseOp(parent), pk_mem_sorter_(nullptr), heap_mem_sorter_(nullptr)
{
  op_type_ = ObTableLoadOpType::MEM_SORT_OP;
}

ObTableLoadMemSortOp::~ObTableLoadMemSortOp() { ObTableLoadMemSortOpFinishTask::reset_op(this); }

/**
 * ObTableLoadCompactDataOp
 */

ObTableLoadCompactDataOp::ObTableLoadCompactDataOp(ObTableLoadTableBaseOp *parent)
  : ObTableLoadTableBaseOp(parent), sstable_compactor_(nullptr), heap_table_compactor_(nullptr)
{
  op_type_ = ObTableLoadOpType::COMPACT_DATA_OP;
}

ObTableLoadCompactDataOp::~ObTableLoadCompactDataOp()
{
  ObTableLoadDagCompactTableOpFinishTask::reset_op(this);
}

/**
 * ObTableLoadInsertSSTableOp
 */

ObTableLoadInsertSSTableOp::ObTableLoadInsertSSTableOp(ObTableLoadTableBaseOp *parent)
  : ObTableLoadTableBaseOp(parent), parallel_merger_(nullptr)
{
  op_type_ = ObTableLoadOpType::INSERT_SSTABLE_OP;
}

ObTableLoadInsertSSTableOp::~ObTableLoadInsertSSTableOp()
{
  ObTableLoadDagInsertSSTableOpFinishTask::reset_op(this);
}

} // namespace observer
} // namespace oceanbase
