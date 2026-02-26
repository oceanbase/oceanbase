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

#include "storage/ddl/ob_column_clustered_dag.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/ddl/ob_tablet_slice_row_iterator.h"
#include "storage/ddl/ob_cg_macro_block_write_task.h"
#include "rootserver/ddl_task/ob_ddl_task.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/column_store/ob_column_store_replica_util.h"
#include "storage/ddl/ob_ddl_merge_task_v2.h"
#include "storage/ddl/ob_group_write_macro_block_task.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase;
using namespace oceanbase::storage;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObColumnClusteredDag::ObColumnClusteredDag()
  : px_thread_count_(0), px_finished_count_(0), is_range_count_ready_(false), total_slice_count_(0), use_static_plan_(true), is_vec_tablet_rebuild_(false)
{

}

ObColumnClusteredDag::~ObColumnClusteredDag()
{

}

int ObColumnClusteredDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  ObITask *merge_parent_task = nullptr;
  const ObColumnClusteredDagInitParam *init_param = static_cast<const ObColumnClusteredDagInitParam*>(param);
  if (OB_UNLIKELY(nullptr == init_param || !init_param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(init_param));
  } else if (OB_FAIL(ObDDLIndependentDag::init_by_param(init_param))) {
    LOG_WARN("init ddl independent dag failed", K(ret), KPC(init_param));
  } else {
    px_thread_count_ = init_param->px_thread_count_;
    is_inited_ = true;

    ObArray<ObITask *> write_macro_block_tasks;
    if (OB_FAIL(generate_write_macro_block_tasks(write_macro_block_tasks))) {
      LOG_WARN("fail to generate write macro block tasks", KR(ret));
    } else if (OB_FAIL(batch_add_task(write_macro_block_tasks))) {
      LOG_WARN("batch add task failed", K(ret), K(write_macro_block_tasks.count()));
    }
  }
  FLOG_INFO("columnn clustered dag init", K(ret), KPC(this));
  return ret;
}

int ObColumnClusteredDag::set_px_finished()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ATOMIC_INC(&px_finished_count_);
    if (is_scan_finished() && !use_static_plan_) {
      /* do nothing */
    }
  }
  FLOG_INFO("set px finished", K(px_finished_count_), K(px_thread_count_));
  return ret;
}

int ObColumnClusteredDag::update_tablet_range_count()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObMutexGuard mutex_guard(mutex_);
    if (is_range_count_ready_) {
      // do nothing
    } else {
      ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
      bool use_idempotent_mode = false;
      ObArenaAllocator arena(ObMemAttr(MTL_ID(), "ddl_slice_info"));
      rootserver::ObDDLSliceInfo ddl_slice_info;

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(sql_proxy)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql proxy is null", K(ret));
      } else if (is_vec_tablet_rebuild_) {
        if (ls_tablet_ids_.count() != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet count not match", K(ret),  K(ls_tablet_ids_));
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::get_inner_sql_slice_info(ddl_task_param_.ddl_task_id_, arena, ddl_slice_info))) {
          LOG_WARN("fail to set vec async task slice into", K(ret), K(ddl_task_param_.ddl_task_id_), K(ddl_slice_info));
        }
      } else if (OB_FAIL(rootserver::ObDDLTaskRecordOperator::get_schedule_info(
                     *sql_proxy, MTL_ID(), ddl_task_param_.ddl_task_id_, arena, false/*is_for_update*/, ddl_slice_info, use_idempotent_mode))) {
        LOG_WARN("fail to get schedule info", K(ret), K(MTL_ID()), K(ddl_task_param_));
      } else if (!use_idempotent_mode) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl dag always use idempotent mode", K(ret), K(use_idempotent_mode), K(ddl_task_param_));
      }

      if (OB_FAIL(ret)) {
      } else {
        total_slice_count_ = 0;
        const common::Ob2DArray<sql::ObPxTabletRange> &part_ranges = ddl_slice_info.part_ranges_;
        if (0 == part_ranges.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("no partition range", K(ret), K(ddl_slice_info));
        } else if (1 == part_ranges.count() && 0 == part_ranges.at(0).tablet_id_) {
          // for unpartitioned table, there is only one tablet and its tablet id is 0
          if (ls_tablet_ids_.count() != 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tablet count not match", K(ret), K(part_ranges), K(ls_tablet_ids_));
          } else {
            const ObTabletID &tablet_id = ls_tablet_ids_.at(0).second;
            total_slice_count_ = part_ranges.at(0).range_cut_.count() + 1;
            ObDDLTabletContext *tablet_context = nullptr;
            if (OB_FAIL(get_tablet_context(tablet_id, tablet_context))) {
              LOG_WARN("get tablet context failed", K(ret), K(tablet_id));
            } else {
              tablet_context->slice_count_ = total_slice_count_;
              tablet_context->table_slice_offset_ = 0;
            }
          }
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < part_ranges.count(); ++i) {
            const ObPxTabletRange &cur_part_range = part_ranges.at(i);
            const int64_t tablet_slice_count = cur_part_range.range_cut_.count() + 1;
            ObTabletID tablet_id(cur_part_range.tablet_id_);
            ObDDLTabletContext *tablet_context = nullptr;
            if (OB_FAIL(get_tablet_context(tablet_id, tablet_context))) {
              if (OB_HASH_NOT_EXIST != ret) {
                LOG_WARN("get tablet context failed", K(ret), K(tablet_id));
              } else {
                // may get tablet not in this node, skip it, but add total slice count
                total_slice_count_ += tablet_slice_count;
                ret = OB_SUCCESS;
              }
            } else {
              tablet_context->slice_count_ = tablet_slice_count;
              tablet_context->table_slice_offset_ = total_slice_count_;
              total_slice_count_ += tablet_slice_count;
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        is_range_count_ready_ = true;
      }
    }
  }
  return ret;
}
