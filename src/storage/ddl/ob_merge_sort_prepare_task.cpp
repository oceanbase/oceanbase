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

#include "storage/ddl/ob_merge_sort_prepare_task.h"

#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/ddl/ob_ddl_merge_sort_task.h"
#include "storage/ddl/ob_ddl_sort_provider.h"
#include "storage/ddl/ob_final_merge_sort_write_task.h"
#define USING_LOG_PREFIX STORAGE

using namespace oceanbase;
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::storage;

ObMergeSortPrepareTask::ObMergeSortPrepareTask(const ObITaskType type)
  : ObITaskWithMonitor(type),
    ddl_dag_(nullptr)
{
}

ObMergeSortPrepareTask::ObMergeSortPrepareTask()
  : ObITaskWithMonitor(TASK_TYPE_DDL_MERGE_SORT_PREPARE_TASK),
    ddl_dag_(nullptr)
{
}

ObMergeSortPrepareTask::~ObMergeSortPrepareTask()
{
}

int ObMergeSortPrepareTask::init(ObDDLIndependentDag *ddl_dag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == ddl_dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ddl_dag_ = ddl_dag;
  }
  return ret;
}

share::ObITask::ObITaskPriority ObMergeSortPrepareTask::get_priority()
{
  int ret = OB_SUCCESS;
  ObITask::ObITaskPriority priority = ObITask::get_priority();
  if (OB_ISNULL(ddl_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret), KP(ddl_dag_));
  } else {
    priority = ddl_dag_->is_scan_finished() && 0 == ddl_dag_->get_pipeline_count()
        ? ObITask::TASK_PRIO_2 : ObITask::TASK_PRIO_0;
  }
  return priority;
}

int ObMergeSortPrepareTask::schedule_slice_merge(ObDDLTabletContext *tablet_ctx,
                                                 ObDDLSlice *ddl_slice,
                                                 const share::ObLSID &ls_id,
                                                 const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  int64_t final_merge_ways = 0;
  ObArray<ObITask *> need_schedule_tasks;
  ObDDLFinalMergeSortWriteTask *final_merge_sort_write_task = nullptr;
  if (OB_ISNULL(ddl_dag_) || OB_ISNULL(ddl_dag_->get_sort_provider())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag or sort provider is null", K(ret), KPC(ddl_dag_));
  }

  /* create final merge sort write task, which shoul always exist */
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ddl_dag_->alloc_task(final_merge_sort_write_task))) {
    LOG_WARN("alloc final merge sort write task failed", K(ret));
  } else if (OB_FAIL(final_merge_sort_write_task->init(ddl_dag_, tablet_id, ddl_slice->get_slice_idx()))) {
    LOG_WARN("init final merge sort write task failed", K(ret));
  } else if (OB_FAIL(copy_children_to(*final_merge_sort_write_task))) {
    LOG_WARN("copy children to final merge sort write task failed", K(ret));
  } else if (OB_FAIL(need_schedule_tasks.push_back(final_merge_sort_write_task))) {
    LOG_WARN("push back final merge sort write task failed", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ddl_dag_->get_sort_provider()->get_final_merge_ways(final_merge_ways))) {
    LOG_WARN("get final merge ways failed", K(ret));
  } else if (final_merge_ways < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("final merge ways is less than 2", K(ret), K(final_merge_ways));
  } else if (ddl_slice->get_sorted_chunk_count() <= final_merge_ways) {
    FLOG_INFO("no need merge sort", K(tablet_id), K(ddl_slice->get_sorted_chunk_count()), K(final_merge_ways));
    // do nothing
  } else {
    // because the merge ways of each task is dynamic, so create merge sort tasks for each thread
    // merge sort task get sorted chunks from ddl slice directly
    for (int64_t i = 0; OB_SUCC(ret) && i < ddl_dag_->get_ddl_thread_count(); ++i) {
      ObDDLMergeSortTask *merge_sort_task = nullptr;
      if (OB_FAIL(ddl_dag_->alloc_task(merge_sort_task))) {
        LOG_WARN("alloc merge sort task failed", K(ret));
      } else if (OB_FAIL(merge_sort_task->init(ddl_dag_, ddl_slice, final_merge_ways))) {
        LOG_WARN("init merge sort task failed", K(ret));
      } else if (OB_FAIL(merge_sort_task->add_child(*final_merge_sort_write_task))) {
        LOG_WARN("add child final merge sort write task failed", K(ret));
      } else if (OB_FAIL(need_schedule_tasks.push_back(merge_sort_task))) {
        LOG_WARN("push back merge sort task failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && need_schedule_tasks.count() > 0)  {
    if (OB_FAIL(ddl_dag_->batch_add_task(need_schedule_tasks))) {
      LOG_WARN("add merge sort tasks failed", K(ret));
    }
  }
  return ret;
}

int ObMergeSortPrepareTask::schedule_tablet_merge(ObDDLTabletContext *tablet_ctx,
                                                  const share::ObLSID &ls_id,
                                                  const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDDLSlice *, 8> slices;
  if (OB_FAIL(tablet_ctx->get_all_slices(slices))) {
    LOG_WARN("get slices failed", K(ret), K(tablet_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < slices.count(); ++i) {
    if (OB_FAIL(schedule_slice_merge(tablet_ctx, slices.at(i), ls_id, tablet_id))) {
      LOG_WARN("schedule slice merge failed", K(ret), K(tablet_id), "slice_idx", slices.at(i)->get_slice_idx());
    }
  }
  return ret;
}

int ObMergeSortPrepareTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret));
  } else {
    const ObIArray<std::pair<ObLSID, ObTabletID>> &ls_tablet_ids = ddl_dag_->get_sort_ls_tablet_ids();
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_tablet_ids.count(); ++i) {
      const ObLSID &ls_id = ls_tablet_ids.at(i).first;
      const ObTabletID &tablet_id = ls_tablet_ids.at(i).second;
      ObDDLTabletContext *tablet_ctx = nullptr;
      if (OB_FAIL(ddl_dag_->get_tablet_context(tablet_id, tablet_ctx))) {
        LOG_WARN("get tablet context failed", K(ret), K(tablet_id));
      } else if (OB_ISNULL(tablet_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet context is null", K(ret), K(tablet_id));
      } else if (OB_FAIL(schedule_tablet_merge(tablet_ctx, ls_id, tablet_id))) {
        LOG_WARN("schedule tablet merges failed", K(ret), K(tablet_id));
      }
    }
  }
  return ret;
}
