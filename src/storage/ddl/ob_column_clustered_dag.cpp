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
#include "storage/ddl/ob_fts_sample_pipeline.h"
#include "storage/ddl/ob_merge_sort_prepare_task.h"
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
  : px_thread_count_(0),
    px_finished_count_(0),
    sample_scan_finished_count_(0),
    mutex_(common::ObLatchIds::OB_COLUMN_CLUSTERED_DAG_MUTEX),
    is_range_count_ready_(false),
    total_slice_count_(0),
    is_vec_tablet_rebuild_(false),
    is_partition_local_(false),
    need_update_tablet_range_count_(false)
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
  } else if (init_param->ddl_task_param_.is_partition_local_ && is_fts_doc_word_aux(ddl_table_schema_.table_item_.index_type_) && OB_FAIL(init_sort_provider())) {
    LOG_WARN("init sort provider failed", K(ret));
  } else {
    const bool is_partition_local = init_param->ddl_task_param_.is_partition_local_;
    px_thread_count_ = init_param->px_thread_count_;
    is_inited_ = true;
    if (OB_SUCC(ret)) {
      ObArray<ObITask *> need_schedule_tasks;
      if (is_partition_local && is_fts_doc_word_aux(ddl_table_schema_.table_item_.index_type_)) {
        if (OB_FAIL(generate_partition_local_fixed_tasks(need_schedule_tasks))) {
          LOG_WARN("fail to generate partition local fixed tasks", KR(ret));
        }
      } else {
        if (OB_FAIL(generate_fixed_tasks(need_schedule_tasks))) {
          LOG_WARN("fail to generate write macro block tasks", KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(batch_add_task(need_schedule_tasks))) {
        LOG_WARN("batch add task failed", K(ret), K(need_schedule_tasks.count()));
      }
    }
    if (OB_SUCC(ret) && is_partition_local &&
        OB_FAIL(sample_cond_.init(ObWaitEventIds::INDEPENDENT_DAG_COND_WAIT))) {
      LOG_WARN("fail to init sample cond", K(ret), KPC(this));
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
  }
  FLOG_INFO("set px finished", K(px_finished_count_), K(px_thread_count_));
  return ret;
}

int ObColumnClusteredDag::set_sample_scan_finished()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ATOMIC_INC(&sample_scan_finished_count_);
  }
  FLOG_INFO("set sample scan finished", K(sample_scan_finished_count_), K(px_thread_count_));
  return ret;
}

int ObColumnClusteredDag::wait_sample_finish()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ddl_task_param_.is_partition_local_) {
    // partition local mode only
  } else {
    common::ObThreadCondGuard guard(sample_cond_);
    if (OB_FAIL(set_sample_scan_finished())) { // avoid notify before wait
      LOG_WARN("fail to set sample scan finished", K(ret));
    } else {
      bool need_wait = true;
      while (need_wait && OB_SUCC(ret)) {
        if (OB_FAIL(THIS_WORKER.check_status())) { // when px failed,we need to aware it and do not to keep on
          LOG_WARN("check status failed", K(ret));
        } else if (get_dag_ret() != OB_SUCCESS) { // when dag failed,we need to aware it and do not to keep on
          ret = get_dag_ret();
          LOG_WARN("ddl dag failed", K(ret));
        } else {
          if (OB_FAIL(sample_cond_.wait(1000))) { // 1s
            if (ret != OB_TIMEOUT) {
              LOG_WARN("failed to wait sample finish.", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          } else {
            need_wait = false;
          }
        }
      }
    }
  }
  return ret;
}

int ObColumnClusteredDag::notify_sample_finished()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ddl_task_param_.is_partition_local_) {
    // partition local mode only
  } else {
    common::ObThreadCondGuard guard(sample_cond_);
    if (OB_FAIL(sample_cond_.broadcast())) {
      LOG_WARN("fail to broadcast sample cond", K(ret));
    }
  }
  return ret;
}

int ObColumnClusteredDag::sample_signal_notify()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(notify_sample_finished())) {
    LOG_WARN("fail to notify sample finished", K(ret), K(get_dag_status()), K(get_dag_ret()));
  }
  return ret;
}

int ObColumnClusteredDag::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLIndependentDag::process())) {
    LOG_WARN("parent process failed", K(ret));
  } else {
    // After parent process completes, call sample_signal_notify
    if (OB_FAIL(sample_signal_notify())) {
      LOG_WARN("fail to notify sample finished after process", K(ret));
    }
  }
  return ret;
}

int ObColumnClusteredDag::append_sample_ranges(const bool is_inverted,
                                               const common::Ob2DArray<sql::ObPxTabletRange> &part_ranges,
                                               const int64_t expect_range_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag not init", K(ret));
  } else if (part_ranges.count() <= 0) {
    // nothing to do
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_ranges.count(); ++i) {
      const sql::ObPxTabletRange &range = part_ranges.at(i);
      int64_t real_tablet_id = range.tablet_id_;
      if (ls_tablet_ids_.count() != 1) { // fts doc word tablet complement inner sql only support one tablet right now
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet count not match", K(ret), K(ls_tablet_ids_));
      } else if (real_tablet_id == 0) {
        real_tablet_id = ls_tablet_ids_.at(0).second.id();
      } else if (real_tablet_id != ls_tablet_ids_.at(0).second.id()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet id not match", K(ret), K(real_tablet_id), K(ls_tablet_ids_.at(0).second.id()));
      }
      ObDDLTabletContext *tablet_context = nullptr;
      ObTabletID tablet_id(real_tablet_id);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(get_tablet_context(tablet_id, tablet_context))) {
        LOG_WARN("get tablet context failed when append sample range", K(ret), K(tablet_id));
      } else if (OB_ISNULL(tablet_context))  {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null tablet context", K(ret), K(tablet_id));
      } else if (OB_FAIL(tablet_context->append_sample_range(is_inverted, range))) {
        LOG_WARN("append sample range to tablet context failed", K(ret), K(tablet_id), K(is_inverted));
      } else if (OB_FAIL(tablet_context->set_expect_range_count(expect_range_cnt))) {
        LOG_WARN("set expect range count failed", K(ret), K(expect_range_cnt));
      }
    }
  }
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

int ObColumnClusteredDag::generate_partition_local_fixed_tasks(
    ObIArray<ObITask *> &need_schedule_tasks,
    ObITask *next_task)
{
  int ret = OB_SUCCESS;
  need_schedule_tasks.reset();
  // fts_sample_pipeline -> doc_word_tablet_group_write_task -> merge_sort_prepare_task -> group_write_task -> merge_tasks -> [next_task]
  ObITask *fts_sample_task = nullptr;
  ObMergeSortPrepareTask *merge_sort_prepare_task = nullptr;
  ObGroupWriteMacroBlockTask *doc_word_tablet_group_write_task = nullptr; // group write macro block for doc word tablet
  ObGroupWriteMacroBlockTask *group_write_task = nullptr;
  ObArray<ObITask*> data_merge_tasks;
  ObArray<ObITask*> lob_merge_tasks;
  ObArray<ObTabletID> word_doc_tablet_ids;
  for (int64_t i = 0; OB_SUCC(ret) && i < sort_ls_tablet_ids_.count(); i++) {
    if (OB_FAIL(word_doc_tablet_ids.push_back(sort_ls_tablet_ids_.at(i).second))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(alloc_fts_sample_task(fts_sample_task))) {
    LOG_WARN("fail to alloc fts sample pipeline", K(ret));
  } else if (OB_FAIL(need_schedule_tasks.push_back(fts_sample_task))) {
    LOG_WARN("fail to push back", KR(ret));
  } else if (OB_FAIL(alloc_task(doc_word_tablet_group_write_task))) {
    LOG_WARN("fail to alloc doc word tablet group write task", K(ret));
  } else if (OB_FAIL(doc_word_tablet_group_write_task->init(this))) { // TODO@wenqu: set doc word tablet id
    LOG_WARN("fail to init doc word tablet group write task", K(ret));
  } else if (OB_FAIL(need_schedule_tasks.push_back(doc_word_tablet_group_write_task))) {
    LOG_WARN("fail to push back", KR(ret));
  } else if (OB_FAIL(alloc_task(merge_sort_prepare_task))) {
    LOG_WARN("fail to alloc merge sort prepare task", K(ret));
  } else if (OB_FAIL(merge_sort_prepare_task->init(this))) {
    LOG_WARN("fail to init merge sort prepare task", K(ret));
  } else if (OB_FAIL(need_schedule_tasks.push_back(merge_sort_prepare_task))) {
    LOG_WARN("fail to push back", KR(ret));
  } else if (OB_FAIL(alloc_task(group_write_task))) {
    LOG_WARN("fail to alloc group write task", K(ret));
  } else if (OB_FAIL(group_write_task->init(this, word_doc_tablet_ids))) {
    LOG_WARN("fail to init group write task", K(ret));
  } else if (OB_FAIL(need_schedule_tasks.push_back(group_write_task))) {
    LOG_WARN("fail to push back", KR(ret));
  } else if (OB_FAIL(fts_sample_task->add_child(*doc_word_tablet_group_write_task))) {
    LOG_WARN("fail to add child", K(ret));
  } else if (OB_FAIL(doc_word_tablet_group_write_task->add_child(*merge_sort_prepare_task))) {
    LOG_WARN("fail to add child", K(ret));
  } else if (OB_FAIL(merge_sort_prepare_task->add_child(*group_write_task))) {
    LOG_WARN("fail to add child", K(ret));
  } else if (OB_FAIL(init_merge_tasks(true /*for_major*/, ls_tablet_ids_, data_merge_tasks, lob_merge_tasks))) {
    LOG_WARN("fail to init merge tasks", KR(ret));
  } else if (OB_FAIL(add_merge_tasks(data_merge_tasks, lob_merge_tasks, need_schedule_tasks))) {
    LOG_WARN("fail to add merge tasks", KR(ret));
  } else if (OB_FAIL(set_merge_tasks(data_merge_tasks, lob_merge_tasks, doc_word_tablet_group_write_task, next_task))) {
    LOG_WARN("fail to set merge tasks", KR(ret));
  }

  data_merge_tasks.reuse();
  lob_merge_tasks.reuse();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_merge_tasks(true /*for_major*/, sort_ls_tablet_ids_, data_merge_tasks, lob_merge_tasks))) {
    LOG_WARN("fail to init merge tasks", KR(ret));
  } else if (OB_FAIL(add_merge_tasks(data_merge_tasks, lob_merge_tasks, need_schedule_tasks))) {
    LOG_WARN("fail to add merge tasks", KR(ret));
  } else if (OB_FAIL(set_merge_tasks(data_merge_tasks, lob_merge_tasks, group_write_task, next_task))) {
    LOG_WARN("fail to set merge tasks", KR(ret));
  }
  return ret;
}

int ObColumnClusteredDag::get_tablet_forward_sample_ranges(const ObTabletID &tablet_id,
                                                           ObIAllocator &allocator,
                                                           common::ObIArray<common::ObNewRange> &ranges)
{
  int ret = OB_SUCCESS;
  ranges.reset();
  const uint64_t table_id = ddl_table_schema_.data_table_id_;

  ObDDLTabletContext *tablet_ctx = nullptr;
  if (OB_FAIL(get_tablet_context(tablet_id, tablet_ctx))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet context", K(ret), K(tablet_id));
    }
  } else if (OB_ISNULL(tablet_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet context is null", K(ret), K(tablet_id));
  } else {
    const sql::ObPxTabletRange &forward_range = tablet_ctx->get_forward_final_sample_range();
    if (OB_FAIL(convert_range_cut_to_ranges(forward_range.range_cut_, table_id, allocator, ranges))) {
      LOG_WARN("failed to convert range cut to ranges", K(ret), K(tablet_id));
    }
    FLOG_INFO("get tablet forward sample ranges", K(ret), K(tablet_id), K(forward_range.range_cut_.count()), K(ranges.count()));
  }
  return ret;
}

//only convert doc_id column to range
int ObColumnClusteredDag::convert_range_cut_to_ranges(const sql::ObPxTabletRange::RangeCut &range_cut,
                                                      const uint64_t table_id,
                                                      ObIAllocator &allocator,
                                                      common::ObIArray<common::ObNewRange> &ranges) const
{
  int ret = OB_SUCCESS;

  if (range_cut.empty()) { //when there is no sample ranges, just set ranges to (min, max)
    common::ObNewRange range;
    if (OB_FAIL(ObSQLUtils::make_whole_range(allocator, table_id, 1, range))) {
      LOG_WARN("failed to make whole range", K(ret));
    } else if (OB_FAIL(ranges.push_back(range))) {
      LOG_WARN("failed to push back range", K(ret));
    }
  } else {
    bool find_docid_col = false;
    const common::ObObjMeta *docid_col_meta = nullptr;
    for (int64_t i = 0; !find_docid_col && i < ddl_table_schema_.column_items_.count(); i++) {
      const ObColumnSchemaItem &col_item = ddl_table_schema_.column_items_.at(i);
      if (col_item.is_rowkey_column_) {
        docid_col_meta = &col_item.col_type_;
        find_docid_col = true;
      }
    }
    if (!find_docid_col || OB_ISNULL(docid_col_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("doc id column not find", K(find_docid_col), KP(docid_col_meta), K(ret));
    }
    common::ObNewRange range;
    for (int64_t i = 0; OB_SUCC(ret) && i <= range_cut.count(); ++i) {
      range.reset();
      range.table_id_ = table_id;
      range.flag_ = 0;

      if (0 == i) {
        range.border_flag_.unset_inclusive_start();
        range.border_flag_.unset_inclusive_end();
      } else {
        range.border_flag_.set_inclusive_start();
        range.border_flag_.unset_inclusive_end();
      }

      common::ObObj *start_obj = static_cast<common::ObObj*>(allocator.alloc(sizeof(common::ObObj)));
      common::ObObj *end_obj = static_cast<common::ObObj*>(allocator.alloc(sizeof(common::ObObj)));

      if (OB_ISNULL(start_obj) || OB_ISNULL(end_obj)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for ObObj", K(ret));
      } else {
        if (0 == i) {
          start_obj->set_min_value();
        } else {
          const sql::ObPxTabletRange::DatumKey &prev_key = range_cut.at(i - 1);
          if (prev_key.count() <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("prev key is empty", K(ret), K(i));
          } else if (OB_FAIL(prev_key.at(0).to_obj(*start_obj, *docid_col_meta))) {
            LOG_WARN("failed to convert datum to obj", K(ret), K(i), K(prev_key.at(0)), KP(docid_col_meta));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (i == range_cut.count()) {
          end_obj->set_max_value();
        } else {
          const sql::ObPxTabletRange::DatumKey &curr_key = range_cut.at(i);
          if (curr_key.count() <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("curr key is empty", K(ret), K(i));
          } else if (OB_FAIL(curr_key.at(0).to_obj(*end_obj, *docid_col_meta))) {
            LOG_WARN("failed to convert datum to obj", K(ret), K(i), K(curr_key.at(0)), KP(docid_col_meta));
          }
        }

        if (OB_SUCC(ret)) {
          range.start_key_.assign(start_obj, 1);
          range.end_key_.assign(end_obj, 1);
          if (OB_FAIL(ranges.push_back(range))) {
            range.start_key_.reset();
            range.end_key_.reset();
            LOG_WARN("failed to push back range", K(ret));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      common::ObObj *start_obj = range.start_key_.get_obj_ptr();
      common::ObObj *end_obj = range.end_key_.get_obj_ptr();
      if (OB_NOT_NULL(start_obj)) {
        start_obj->~ObObj();
        allocator.free(start_obj);
        start_obj = nullptr;
      }
      if (OB_NOT_NULL(end_obj)) {
        end_obj->~ObObj();
        allocator.free(end_obj);
        end_obj = nullptr;
      }
      for (int64_t j = 0; j < ranges.count(); j ++) {
        if (OB_NOT_NULL(ranges.at(j).start_key_.get_obj_ptr())) {
          ranges.at(j).start_key_.get_obj_ptr()->~ObObj();
          allocator.free(ranges.at(j).start_key_.get_obj_ptr());
          ranges.at(j).start_key_.reset();
        }
        if (OB_NOT_NULL(ranges.at(j).end_key_.get_obj_ptr())) {
          ranges.at(j).end_key_.get_obj_ptr()->~ObObj();
          allocator.free(ranges.at(j).end_key_.get_obj_ptr());
          ranges.at(j).end_key_.reset();
        }
      }
    }
  }

  return ret;
}

int ObColumnClusteredDag::alloc_fts_sample_task(share::ObITask *&fts_sample_task)
{
  int ret = OB_SUCCESS;
  fts_sample_task = nullptr;

  if (get_ls_tablet_ids().count() <= 0 || get_ls_tablet_ids().count() > 1) { // only one tablet is supported now for FTS sampling
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet count for FTS sampling", K(ret), "tablet_count", get_ls_tablet_ids().count());
  } else {
    const ObTabletID &tablet_id = get_ls_tablet_ids().at(0).second;
    ObFtsSamplePipeline *pipeline = nullptr;
    if (OB_FAIL(alloc_task(pipeline))) {
      LOG_WARN("fail to alloc fts sample pipeline", K(ret));
    } else if (OB_FAIL(pipeline->init(tablet_id))) {
      LOG_WARN("fail to init fts sample pipeline", K(ret), K(tablet_id));
    } else {
      fts_sample_task = pipeline;
      LOG_INFO("successfully allocated FTS sample pipeline", K(tablet_id), K(*pipeline));
    }
  }

  return ret;
}
