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

#include "observer/table_load/dag/ob_table_load_dag_mem_sort.h"
#include "observer/table_load/dag/ob_table_load_dag.h"
#include "observer/table_load/dag/ob_table_load_dag_heap_mem_sort.h"
#include "observer/table_load/dag/ob_table_load_dag_pk_mem_sort.h"
#include "observer/table_load/plan/ob_table_load_merge_op.h"

namespace oceanbase
{
namespace observer
{
using namespace share;
using namespace storage;

/**
 * ObTableLoadMemSortOpTask
 */

ObTableLoadMemSortOpTask::ObTableLoadMemSortOpTask(ObTableLoadDag *dag, ObTableLoadOp *op)
  : ObITask(TASK_TYPE_DIRECT_LOAD_MEM_SORT_OP), ObTableLoadDagOpTaskBase(dag, op)
{
}

int ObTableLoadMemSortOpTask::process()
{
  int ret = OB_SUCCESS;
  ObTableLoadMemSortOp *op = static_cast<ObTableLoadMemSortOp *>(op_);
  FLOG_INFO("[DIRECT_LOAD_OP] mem sort op start", KP(op));
  op->start_time_ = ObTimeUtil::current_time();

  ObDirectLoadTableStore &table_store = op->op_ctx_->table_store_;
  ObITask *first_task = nullptr;
  if (table_store.empty()) {
    op->end_time_ = op->start_time_;
    FLOG_INFO("[DIRECT_LOAD_OP] mem sort op skip", KP(op));
  } else if (table_store.get_table_data_desc().row_flag_.uncontain_hidden_pk_) {
    // 无主键数据排序路径
    //  external_table: 堆表排序场景写入
    if (OB_UNLIKELY(!table_store.is_external_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected not external table", KR(ret), K(table_store));
    } else if (OB_UNLIKELY(1 != table_store.size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table store size not equal one", KR(ret), K(table_store));
    } else if (OB_ISNULL(op->heap_mem_sorter_ =
                           OB_NEWx(ObTableLoadHeapMemSorter, &op->op_ctx_->allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadHeapMemSorter", KR(ret));
    } else if (OB_FAIL(op->heap_mem_sorter_->init(dag_, op))) {
      LOG_WARN("fail to init heap mem sorter", KR(ret));
    } else {
      ObTableLoadHeapMemSortTask *task = nullptr;
      if (OB_FAIL(dag_->alloc_task(task, dag_, op->heap_mem_sorter_))) {
        LOG_WARN("fail to alloc task", KR(ret));
      } else {
        first_task = task;
      }
    }
  } else {
    // 有主键数据排序路径
    //  sstable: 备份导入场景写入, 不需要排序
    //  multiple_sstable: 预排序场景写入产生, 不需要排序
    //  external_table: 数据通道产生
    if (table_store.is_sstable() || table_store.is_multiple_sstable()) {
      // do nothing
    } else if (OB_UNLIKELY(!table_store.is_external_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected not external table", KR(ret), K(table_store));
    } else if (OB_UNLIKELY(1 != table_store.size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table store size not equal one", KR(ret), K(table_store));
    } else if (OB_ISNULL(op->pk_mem_sorter_ =
                           OB_NEWx(ObTableLoadPKMemSorter, &op->op_ctx_->allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadPKMemSorter", KR(ret));
    } else if (OB_FAIL(op->pk_mem_sorter_->init(dag_, op))) {
      LOG_WARN("fail to init pk mem sorter", KR(ret));
    } else {
      ObTableLoadPKMemSortTask *task = nullptr;
      if (OB_FAIL(dag_->alloc_task(task, dag_, op->pk_mem_sorter_))) {
        LOG_WARN("fail to alloc task", KR(ret));
      } else {
        first_task = task;
      }
    }
  }
  if (OB_SUCC(ret) && nullptr != first_task) {
    ObTableLoadMemSortOpFinishTask *finish_task = nullptr;
    if (OB_FAIL(dag_->alloc_task(finish_task, dag_, op_))) {
      LOG_WARN("fail to alloc task", KR(ret));
    }
    // 建立依赖关系: first_task -> finish_task -> [next_op_task]
    else if (OB_FAIL(first_task->add_child(*finish_task))) {
      LOG_WARN("fail to add child", KR(ret));
    } else if (OB_FAIL(finish_task->deep_copy_children(get_child_nodes()))) {
      LOG_WARN("fail to deep copy children", KR(ret));
    }
    // 添加task
    else if (OB_FAIL(dag_->add_task(*finish_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else if (OB_FAIL(dag_->add_task(*first_task))) {
      LOG_WARN("fail to add task", KR(ret));
    }
  }
  return ret;
}

/**
 * ObTableLoadMemSortOpFinishTask
 */

ObTableLoadMemSortOpFinishTask::ObTableLoadMemSortOpFinishTask(ObTableLoadDag *dag,
                                                               ObTableLoadOp *op)
  : ObITask(TASK_TYPE_DIRECT_LOAD_MEM_SORT_OP_FINISH), ObTableLoadDagOpTaskBase(dag, op)
{
}

int ObTableLoadMemSortOpFinishTask::process()
{
  int ret = OB_SUCCESS;
  ObTableLoadMemSortOp *op = static_cast<ObTableLoadMemSortOp *>(op_);
  if (nullptr != op->pk_mem_sorter_) {
    if (OB_FAIL(op->pk_mem_sorter_->close())) {
      LOG_WARN("fail to close pk mem sorter", KR(ret));
    }
  } else if (nullptr != op->heap_mem_sorter_) {
    if (OB_FAIL(op->heap_mem_sorter_->close())) {
      LOG_WARN("fail to close heap mem sorter", KR(ret));
    }
  }

  op->end_time_ = ObTimeUtil::current_time();
  FLOG_INFO("[DIRECT_LOAD_OP] mem sort op finish", KP(op), "time_cost",
            op->end_time_ - op->start_time_);

  reset_op(op);
  return ret;
}

void ObTableLoadMemSortOpFinishTask::reset_op(ObTableLoadMemSortOp *op)
{
  if (OB_NOT_NULL(op)) {
    OB_DELETEx(ObTableLoadPKMemSorter, &op->op_ctx_->allocator_, op->pk_mem_sorter_);
    OB_DELETEx(ObTableLoadHeapMemSorter, &op->op_ctx_->allocator_, op->heap_mem_sorter_);
  }
}

} // namespace observer
} // namespace oceanbase
