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

#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_merge_ctx.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "storage/direct_load/ob_direct_load_external_table.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_block.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable.h"
#include "storage/direct_load/ob_direct_load_partition_merge_task.h"
#include "storage/direct_load/ob_direct_load_range_splitter.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "share/stat/ob_opt_table_stat.h"
#include "share/stat/ob_opt_stat_monitor_manager.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_stat_item.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;
using namespace common;
using namespace share;
using namespace table;
using namespace sql;
using namespace observer;

/**
 * ObDirectLoadMergeParam
 */

ObDirectLoadMergeParam::ObDirectLoadMergeParam()
  : table_id_(OB_INVALID_ID),
    target_table_id_(OB_INVALID_ID),
    rowkey_column_num_(0),
    store_column_count_(0),
    snapshot_version_(0),
    datum_utils_(nullptr),
    col_descs_(nullptr),
    cmp_funcs_(nullptr),
    is_heap_table_(false),
    is_fast_heap_table_(false),
    online_opt_stat_gather_(false),
    insert_table_ctx_(nullptr),
    dml_row_handler_(nullptr)
{
}

ObDirectLoadMergeParam::~ObDirectLoadMergeParam()
{
}

bool ObDirectLoadMergeParam::is_valid() const
{
  return OB_INVALID_ID != table_id_ && 0 < rowkey_column_num_ && 0 < store_column_count_ &&
         snapshot_version_ > 0 && table_data_desc_.is_valid() && nullptr != datum_utils_ &&
         nullptr != col_descs_ && nullptr != cmp_funcs_ && nullptr != insert_table_ctx_ &&
         nullptr != dml_row_handler_;
}

/**
 * ObDirectLoadMergeCtx
 */

ObDirectLoadMergeCtx::ObDirectLoadMergeCtx()
  : allocator_("TLD_MergeCtx"), is_inited_(false)
{
}

ObDirectLoadMergeCtx::~ObDirectLoadMergeCtx()
{
  for (int64_t i = 0; i < tablet_merge_ctx_array_.count(); ++i) {
    ObDirectLoadTabletMergeCtx *tablet_ctx = tablet_merge_ctx_array_.at(i);
    tablet_ctx->~ObDirectLoadTabletMergeCtx();
    allocator_.free(tablet_ctx);
  }
  tablet_merge_ctx_array_.reset();
}

int ObDirectLoadMergeCtx::init(const ObDirectLoadMergeParam &param,
                               const ObIArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
                               const ObIArray<ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMerger init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid()
                         || ls_partition_ids.empty()
                         || target_ls_partition_ids.empty()
                         || (ls_partition_ids.count() != target_ls_partition_ids.count()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param), K(ls_partition_ids), K(target_ls_partition_ids));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    param_ = param;
    if (OB_FAIL(create_all_tablet_ctxs(ls_partition_ids, target_ls_partition_ids))) {
      LOG_WARN("fail to create all tablet ctxs", KR(ret));
    } else {
      std::sort(tablet_merge_ctx_array_.begin(), tablet_merge_ctx_array_.end(),
                [](const ObDirectLoadTabletMergeCtx *lhs, const ObDirectLoadTabletMergeCtx *rhs) {
                  return lhs->get_tablet_id().compare(rhs->get_tablet_id()) < 0;
                });
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMergeCtx::create_all_tablet_ctxs(
  const ObIArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
  const ObIArray<ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_partition_ids.count(); ++i) {
    const ObTableLoadLSIdAndPartitionId &ls_partition_id = ls_partition_ids.at(i);
    const ObTableLoadLSIdAndPartitionId &target_ls_partition_id = target_ls_partition_ids.at(i);
    ObDirectLoadTabletMergeCtx *partition_ctx = nullptr;
    if (OB_ISNULL(partition_ctx = OB_NEWx(ObDirectLoadTabletMergeCtx, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadTabletMergeCtx", KR(ret));
    } else if (OB_FAIL(partition_ctx->init(param_, ls_partition_id, target_ls_partition_id))) {
      LOG_WARN("fail to init tablet ctx", KR(ret), K(param_), K(ls_partition_id), K(target_ls_partition_id));
    } else if (OB_FAIL(tablet_merge_ctx_array_.push_back(partition_ctx))) {
      LOG_WARN("fail to push back", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != partition_ctx) {
        partition_ctx->~ObDirectLoadTabletMergeCtx();
        allocator_.free(partition_ctx);
        partition_ctx = nullptr;
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadTabletMergeCtx
 */

ObDirectLoadTabletMergeCtx::ObDirectLoadTabletMergeCtx()
  : allocator_("TLD_MegTbtCtx"), task_finish_count_(0), is_inited_(false)
{
}

ObDirectLoadTabletMergeCtx::~ObDirectLoadTabletMergeCtx()
{
  for (int64_t i = 0; i < task_array_.count(); ++i) {
    ObDirectLoadPartitionMergeTask *task = task_array_.at(i);
    task->~ObDirectLoadPartitionMergeTask();
    allocator_.free(task);
  }
  task_array_.reset();
}

int ObDirectLoadTabletMergeCtx::init(const ObDirectLoadMergeParam &param,
                                     const ObTableLoadLSIdAndPartitionId &ls_partition_id,
                                     const ObTableLoadLSIdAndPartitionId &target_ls_partition_id)

{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadTabletMergeCtx init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid()
                         || !ls_partition_id.is_valid()
                         || !target_ls_partition_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param), K(ls_partition_id));
  } else {
    ObDirectLoadOriginTableCreateParam origin_table_param;
    origin_table_param.table_id_ = param.table_id_;
    origin_table_param.tablet_id_ = ls_partition_id.part_tablet_id_.tablet_id_;
    origin_table_param.ls_id_ = ls_partition_id.ls_id_;
    if (OB_FAIL(origin_table_.init(origin_table_param))) {
      LOG_WARN("fail to init origin sstable", KR(ret));
    } else {
      allocator_.set_tenant_id(MTL_ID());
      param_ = param;
      target_partition_id_ = target_ls_partition_id.part_tablet_id_.partition_id_;
      tablet_id_ = ls_partition_id.part_tablet_id_.tablet_id_;
      target_tablet_id_ = target_ls_partition_id.part_tablet_id_.tablet_id_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::collect_sql_statistics(
  const ObIArray<ObDirectLoadFastHeapTable *> &fast_heap_table_array, ObTableLoadSqlStatistics &sql_statistics)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTabletMergeCtx not init", KR(ret), KP(this));
  } else if (OB_FAIL(ObTableLoadSchema::get_table_schema(tenant_id, param_.target_table_id_, schema_guard,
                                                  table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(param_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else {
    int64_t table_row_cnt = 0;
    int64_t table_avg_len = 0;
    int64_t col_cnt = param_.table_data_desc_.column_count_;
    ObOptTableStat *table_stat = nullptr;
    StatLevel stat_level;
    if (table_schema->get_part_level() == PARTITION_LEVEL_ZERO) {
      stat_level = TABLE_LEVEL;
    } else if (table_schema->get_part_level() == PARTITION_LEVEL_ONE) {
      stat_level = PARTITION_LEVEL;
    } else if (table_schema->get_part_level() == PARTITION_LEVEL_TWO) {
      stat_level = SUBPARTITION_LEVEL;
    } else {
      stat_level = INVALID_LEVEL;
    }
    if (OB_FAIL(sql_statistics.allocate_table_stat(table_stat))) {
      LOG_WARN("fail to allocate table stat", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
        int64_t col_id = param_.is_heap_table_ ? i + 1 : i;
        int64_t row_count = 0;
        ObOptOSGColumnStat *osg_col_stat = nullptr;
        if (OB_FAIL(sql_statistics.allocate_col_stat(osg_col_stat))) {
          LOG_WARN("fail to allocate table stat", KR(ret));
        }
        // scan task_array
        for (int64_t j = 0; OB_SUCC(ret) && j < task_array_.count(); ++j) {
          ObOptOSGColumnStat *tmp_col_stat = task_array_.at(j)->get_column_stat_array().at(i);
          if (task_array_.at(j)->get_row_count() != 0) {
            if (OB_FAIL(osg_col_stat->merge_column_stat(*tmp_col_stat))) {
              LOG_WARN("fail to merge column stat", KR(ret));
            } else {
              row_count += task_array_.at(j)->get_row_count();
            }
          }
        }
        // scan fast heap table
        for (int64_t j = 0; OB_SUCC(ret) && j < fast_heap_table_array.count(); ++j) {
          ObOptOSGColumnStat *tmp_col_stat = fast_heap_table_array.at(j)->get_column_stat_array().at(i);
          if (fast_heap_table_array.at(j)->get_row_count() != 0) {
            if (OB_FAIL(osg_col_stat->merge_column_stat(*tmp_col_stat))) {
              LOG_WARN("fail to merge column stat", KR(ret));
            } else {
              row_count += fast_heap_table_array.at(j)->get_row_count();
            }
          }
        }
        if (OB_SUCC(ret)) {
          table_row_cnt = row_count;
          osg_col_stat->col_stat_->calc_avg_len();
          table_avg_len += osg_col_stat->col_stat_->get_avg_len();
          osg_col_stat->col_stat_->set_table_id(param_.target_table_id_);
          osg_col_stat->col_stat_->set_partition_id(target_partition_id_);
          osg_col_stat->col_stat_->set_stat_level(stat_level);
          osg_col_stat->col_stat_->set_column_id(param_.col_descs_->at(col_id).col_id_);
          osg_col_stat->col_stat_->set_num_distinct(ObGlobalNdvEval::get_ndv_from_llc(osg_col_stat->col_stat_->get_llc_bitmap()));
          if (OB_FAIL(osg_col_stat->set_min_max_datum_to_obj())) {
            LOG_WARN("failed to set min max datum to obj", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        table_stat->set_table_id(param_.target_table_id_);
        table_stat->set_partition_id(target_partition_id_);
        table_stat->set_object_type(stat_level);
        table_stat->set_row_count(table_row_cnt);
        table_stat->set_avg_row_size(table_avg_len);
      }
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::collect_dml_stat(const common::ObIArray<ObDirectLoadFastHeapTable *> &fast_heap_table_array, ObTableLoadDmlStat &dml_stats)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  int64_t insert_row_cnt = 0;
  ObOptDmlStat *dml_stat = nullptr;
  if (OB_FAIL(dml_stats.allocate_dml_stat(dml_stat))) {
    LOG_WARN("fail to allocate table stat", KR(ret));
  } else {
    // scan task_array
    for (int64_t i = 0; OB_SUCC(ret) && i < task_array_.count(); ++i) {
      insert_row_cnt += task_array_.at(i)->get_row_count();
    }
    // scan fast heap table
    for (int64_t i = 0; OB_SUCC(ret) && i < fast_heap_table_array.count(); ++i) {
      insert_row_cnt += fast_heap_table_array.at(i)->get_row_count();
    }
    dml_stat->tenant_id_ = tenant_id;
    dml_stat->table_id_ = param_.target_table_id_;
    dml_stat->tablet_id_ = target_tablet_id_.id();
    dml_stat->insert_row_count_ = insert_row_cnt;
  }
  return ret;
}


int ObDirectLoadTabletMergeCtx::init_sstable_array(
  const ObIArray<ObIDirectLoadPartitionTable *> &table_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_array.count(); ++i) {
    ObDirectLoadSSTable *sstable = nullptr;
    if (OB_ISNULL(sstable = dynamic_cast<ObDirectLoadSSTable *>(table_array.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table", KR(ret), K(i), K(table_array));
    } else if (OB_FAIL(sstable_array_.push_back(sstable))) {
      LOG_WARN("fail to push back sstable", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::init_multiple_sstable_array(
  const ObIArray<ObIDirectLoadPartitionTable *> &table_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_array.count(); ++i) {
    ObDirectLoadMultipleSSTable *sstable = nullptr;
    if (OB_ISNULL(sstable = dynamic_cast<ObDirectLoadMultipleSSTable *>(table_array.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table", KR(ret), K(i), K(table_array));
    } else if (OB_FAIL(multiple_sstable_array_.push_back(sstable))) {
      LOG_WARN("fail to push back multiple sstable", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::init_multiple_heap_table_array(
  const ObIArray<ObIDirectLoadPartitionTable *> &table_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_array.count(); ++i) {
    ObDirectLoadMultipleHeapTable *heap_table = nullptr;
    if (OB_ISNULL(heap_table = dynamic_cast<ObDirectLoadMultipleHeapTable *>(table_array.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table", KR(ret), K(i), K(table_array));
    } else if (OB_FAIL(multiple_heap_table_array_.push_back(heap_table))) {
      LOG_WARN("fail to push back multiple heap table", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_merge_task(
  const ObIArray<ObIDirectLoadPartitionTable *> &table_array,
  const ObIArray<ObColDesc> &col_descs,
  int64_t max_parallel_degree,
  bool is_multiple_mode)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTabletMergeCtx not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(max_parallel_degree <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(max_parallel_degree));
  } else {
    if (table_array.empty()) {
      if (OB_FAIL(build_empty_data_merge_task(col_descs, max_parallel_degree))) {
        LOG_WARN("fail to build empty data merge task", KR(ret));
      }
    } else if (!param_.is_heap_table_) {
      if (!is_multiple_mode) {
        if (OB_FAIL(build_pk_table_merge_task(table_array, col_descs, max_parallel_degree))) {
          LOG_WARN("fail to build pk table merge task", KR(ret));
        }
      } else {
        if (OB_FAIL(
              build_pk_table_multiple_merge_task(table_array, col_descs, max_parallel_degree))) {
          LOG_WARN("fail to build pk table multiple merge task", KR(ret));
        }
      }
    } else {
      if (!is_multiple_mode) {
        if (OB_FAIL(build_heap_table_merge_task(table_array, col_descs, max_parallel_degree))) {
          LOG_WARN("fail to build heap table merge task", KR(ret));
        }
      } else {
        if (OB_FAIL(
              build_heap_table_multiple_merge_task(table_array, col_descs, max_parallel_degree))) {
          LOG_WARN("fail to build heap table multiple merge task", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_empty_data_merge_task(const ObIArray<ObColDesc> &col_descs,
                                                            int64_t max_parallel_degree)
{
  int ret = OB_SUCCESS;
  // only existing data, construct task by split range
  if (OB_SUCC(ret)) {
    ObDirectLoadMergeRangeSplitter range_splitter;
    if (OB_FAIL(
          range_splitter.init(&origin_table_, sstable_array_, param_.datum_utils_, col_descs))) {
      LOG_WARN("fail to init range splitter", KR(ret));
    } else if (OB_FAIL(range_splitter.split_range(range_array_, max_parallel_degree, allocator_))) {
      LOG_WARN("fail to split range", KR(ret));
    } else if (OB_UNLIKELY(range_array_.count() > max_parallel_degree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected range count", KR(ret), K(max_parallel_degree), K(range_array_.count()));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < range_array_.count(); ++i) {
    const ObDatumRange &range = range_array_.at(i);
    ObDirectLoadPartitionRangeMergeTask *merge_task = nullptr;
    if (OB_ISNULL(merge_task = OB_NEWx(ObDirectLoadPartitionRangeMergeTask, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadPartitionRangeMergeTask", KR(ret));
    } else if (OB_FAIL(merge_task->init(param_, this, &origin_table_, sstable_array_, range, i))) {
      LOG_WARN("fail to init merge task", KR(ret));
    } else if (OB_FAIL(task_array_.push_back(merge_task))) {
      LOG_WARN("fail to push back merge task", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != merge_task) {
        merge_task->~ObDirectLoadPartitionRangeMergeTask();
        allocator_.free(merge_task);
        merge_task = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_pk_table_merge_task(
  const ObIArray<ObIDirectLoadPartitionTable *> &table_array,
  const ObIArray<ObColDesc> &col_descs,
  int64_t max_parallel_degree)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_sstable_array(table_array))) {
    LOG_WARN("fail to init sstable array", KR(ret));
  }
  // split range
  if (OB_SUCC(ret)) {
    ObDirectLoadMergeRangeSplitter range_splitter;
    if (OB_FAIL(
          range_splitter.init(&origin_table_, sstable_array_, param_.datum_utils_, col_descs))) {
      LOG_WARN("fail to init range splitter", KR(ret));
    } else if (OB_FAIL(range_splitter.split_range(range_array_, max_parallel_degree, allocator_))) {
      LOG_WARN("fail to split range", KR(ret));
    } else if (OB_UNLIKELY(range_array_.count() > max_parallel_degree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected range count", KR(ret), K(max_parallel_degree), K(range_array_.count()));
    }
  }
  // construct task per range
  for (int64_t i = 0; OB_SUCC(ret) && i < range_array_.count(); ++i) {
    const ObDatumRange &range = range_array_.at(i);
    ObDirectLoadPartitionRangeMergeTask *merge_task = nullptr;
    if (OB_ISNULL(merge_task = OB_NEWx(ObDirectLoadPartitionRangeMergeTask, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadPartitionRangeMergeTask", KR(ret));
    } else if (OB_FAIL(merge_task->init(param_, this, &origin_table_, sstable_array_, range, i))) {
      LOG_WARN("fail to init merge task", KR(ret));
    } else if (OB_FAIL(task_array_.push_back(merge_task))) {
      LOG_WARN("fail to push back merge task", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != merge_task) {
        merge_task->~ObDirectLoadPartitionRangeMergeTask();
        allocator_.free(merge_task);
        merge_task = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_pk_table_multiple_merge_task(
  const ObIArray<ObIDirectLoadPartitionTable *> &table_array,
  const ObIArray<ObColDesc> &col_descs,
  int64_t max_parallel_degree)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_multiple_sstable_array(table_array))) {
    LOG_WARN("fail to init multiple sstable array", KR(ret));
  }
  // split range
  if (OB_SUCC(ret)) {
    ObDirectLoadMultipleMergeTabletRangeSplitter range_splitter;
    if (OB_FAIL(range_splitter.init(tablet_id_, &origin_table_, multiple_sstable_array_,
                                    param_.table_data_desc_, param_.datum_utils_, col_descs))) {
      LOG_WARN("fail to init range splitter", KR(ret));
    } else if (OB_FAIL(range_splitter.split_range(range_array_, max_parallel_degree, allocator_))) {
      LOG_WARN("fail to split range", KR(ret));
    } else if (OB_UNLIKELY(range_array_.count() > max_parallel_degree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected range count", KR(ret), K(max_parallel_degree), K(range_array_.count()));
    }
  }
  // construct task per range
  for (int64_t i = 0; OB_SUCC(ret) && i < range_array_.count(); ++i) {
    const ObDatumRange &range = range_array_.at(i);
    ObDirectLoadPartitionRangeMultipleMergeTask *merge_task = nullptr;
    if (OB_ISNULL(merge_task =
                    OB_NEWx(ObDirectLoadPartitionRangeMultipleMergeTask, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadPartitionRangeMultipleMergeTask", KR(ret));
    } else if (OB_FAIL(merge_task->init(param_, this, &origin_table_, multiple_sstable_array_,
                                        range, i))) {
      LOG_WARN("fail to init merge task", KR(ret));
    } else if (OB_FAIL(task_array_.push_back(merge_task))) {
      LOG_WARN("fail to push back merge task", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != merge_task) {
        merge_task->~ObDirectLoadPartitionRangeMultipleMergeTask();
        allocator_.free(merge_task);
        merge_task = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_merge_task_for_multiple_pk_table(
  const ObIArray<ObDirectLoadMultipleSSTable *> &multiple_sstable_array,
  ObDirectLoadMultipleMergeRangeSplitter &range_splitter,
  int64_t max_parallel_degree)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTabletMergeCtx not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(max_parallel_degree <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(max_parallel_degree));
  } else {
    if (OB_FAIL(multiple_sstable_array_.assign(multiple_sstable_array))) {
      LOG_WARN("fail to assign multiple sstable array", KR(ret));
    } else if (OB_FAIL(range_splitter.split_range(tablet_id_, &origin_table_, max_parallel_degree,
                                                  range_array_, allocator_))) {
      LOG_WARN("fail to split range", KR(ret));
    } else if (OB_UNLIKELY(range_array_.count() > max_parallel_degree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected range count", KR(ret), K(max_parallel_degree), K(range_array_.count()));
    }
    // construct task per range
    for (int64_t i = 0; OB_SUCC(ret) && i < range_array_.count(); ++i) {
      const ObDatumRange &range = range_array_.at(i);
      ObDirectLoadPartitionRangeMultipleMergeTask *merge_task = nullptr;
      if (OB_ISNULL(merge_task =
                      OB_NEWx(ObDirectLoadPartitionRangeMultipleMergeTask, (&allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObDirectLoadPartitionRangeMultipleMergeTask", KR(ret));
      } else if (OB_FAIL(merge_task->init(param_, this, &origin_table_, multiple_sstable_array_,
                                          range, i))) {
        LOG_WARN("fail to init merge task", KR(ret));
      } else if (OB_FAIL(task_array_.push_back(merge_task))) {
        LOG_WARN("fail to push back merge task", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != merge_task) {
          merge_task->~ObDirectLoadPartitionRangeMultipleMergeTask();
          allocator_.free(merge_task);
          merge_task = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_heap_table_merge_task(
  const ObIArray<ObIDirectLoadPartitionTable *> &table_array,
  const ObIArray<ObColDesc> &col_descs,
  int64_t max_parallel_degree)
{
  int ret = OB_SUCCESS;
  int64_t parallel_idx = 0;
  // for existing data, construct task by split range
  if (OB_SUCC(ret)) {
    ObDirectLoadMergeRangeSplitter range_splitter;
    if (OB_FAIL(
          range_splitter.init(&origin_table_, sstable_array_, param_.datum_utils_, col_descs))) {
      LOG_WARN("fail to init range splitter", KR(ret));
    } else if (OB_FAIL(range_splitter.split_range(range_array_, max_parallel_degree, allocator_))) {
      LOG_WARN("fail to split range", KR(ret));
    } else if (OB_UNLIKELY(range_array_.count() > max_parallel_degree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected range count", KR(ret), K(max_parallel_degree), K(range_array_.count()));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < range_array_.count(); ++i) {
    const ObDatumRange &range = range_array_.at(i);
    ObDirectLoadPartitionRangeMergeTask *merge_task = nullptr;
    if (OB_ISNULL(merge_task = OB_NEWx(ObDirectLoadPartitionRangeMergeTask, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadPartitionRangeMergeTask", KR(ret));
    } else if (OB_FAIL(merge_task->init(param_, this, &origin_table_, sstable_array_, range,
                                        parallel_idx++))) {
      LOG_WARN("fail to init merge task", KR(ret));
    } else if (OB_FAIL(task_array_.push_back(merge_task))) {
      LOG_WARN("fail to push back merge task", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != merge_task) {
        merge_task->~ObDirectLoadPartitionRangeMergeTask();
        allocator_.free(merge_task);
        merge_task = nullptr;
      }
    }
  }
  // for imported data, construct task per external table
  for (int64_t i = 0; OB_SUCC(ret) && !param_.is_fast_heap_table_ && i < table_array.count(); ++i) {
    ObDirectLoadExternalTable *external_table = nullptr;
    ObDirectLoadPartitionHeapTableMergeTask *merge_task = nullptr;
    ObTabletCacheInterval pk_interval;
    if (OB_ISNULL(external_table = dynamic_cast<ObDirectLoadExternalTable *>(table_array.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table", KR(ret), K(i), K(table_array));
    } else if (OB_FAIL(get_autoincrement_value(external_table->get_row_count(), pk_interval))) {
      LOG_WARN("fail to get autoincrement value", KR(ret), K(external_table->get_row_count()));
    } else if (OB_ISNULL(merge_task =
                           OB_NEWx(ObDirectLoadPartitionHeapTableMergeTask, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadPartitionHeapTableMergeTask", KR(ret));
    } else if (OB_FAIL(merge_task->init(param_, this, external_table, pk_interval,
                                        parallel_idx++))) {
      LOG_WARN("fail to init merge task", KR(ret));
    } else if (OB_FAIL(task_array_.push_back(merge_task))) {
      LOG_WARN("fail to push back merge task", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != merge_task) {
        merge_task->~ObDirectLoadPartitionHeapTableMergeTask();
        allocator_.free(merge_task);
        merge_task = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_heap_table_multiple_merge_task(
  const ObIArray<ObIDirectLoadPartitionTable *> &table_array,
  const ObIArray<ObColDesc> &col_descs,
  int64_t max_parallel_degree)
{
  int ret = OB_SUCCESS;
  int64_t parallel_idx = 0;
  if (OB_FAIL(init_multiple_heap_table_array(table_array))) {
    LOG_WARN("fail to init multiple heap table array", KR(ret));
  }
  // for existing data, construct task by split range
  if (OB_SUCC(ret)) {
    ObDirectLoadMergeRangeSplitter range_splitter;
    if (OB_FAIL(
          range_splitter.init(&origin_table_, sstable_array_, param_.datum_utils_, col_descs))) {
      LOG_WARN("fail to init range splitter", KR(ret));
    } else if (OB_FAIL(range_splitter.split_range(range_array_, max_parallel_degree, allocator_))) {
      LOG_WARN("fail to split range", KR(ret));
    } else if (OB_UNLIKELY(range_array_.count() > max_parallel_degree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected range count", KR(ret), K(max_parallel_degree), K(range_array_.count()));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < range_array_.count(); ++i) {
    const ObDatumRange &range = range_array_.at(i);
    ObDirectLoadPartitionRangeMergeTask *merge_task = nullptr;
    if (OB_ISNULL(merge_task = OB_NEWx(ObDirectLoadPartitionRangeMergeTask, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadPartitionRangeMergeTask", KR(ret));
    } else if (OB_FAIL(merge_task->init(param_, this, &origin_table_, sstable_array_, range,
                                        parallel_idx++))) {
      LOG_WARN("fail to init merge task", KR(ret));
    } else if (OB_FAIL(task_array_.push_back(merge_task))) {
      LOG_WARN("fail to push back merge task", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != merge_task) {
        merge_task->~ObDirectLoadPartitionRangeMergeTask();
        allocator_.free(merge_task);
        merge_task = nullptr;
      }
    }
  }
  // for imported data, construct task by multiple heap table
  for (int64_t i = 0; OB_SUCC(ret) && !param_.is_fast_heap_table_ && i < multiple_heap_table_array_.count(); ++i) {
    ObDirectLoadMultipleHeapTable *heap_table = multiple_heap_table_array_.at(i);
    ObDirectLoadPartitionHeapTableMultipleMergeTask *merge_task = nullptr;
    int64_t row_count = 0;
    ObTabletCacheInterval pk_interval;
    if (OB_FAIL(heap_table->get_tablet_row_count(tablet_id_, param_.table_data_desc_, row_count))) {
      LOG_WARN("fail to get tablet row count", KR(ret), K(tablet_id_));
    } else if (0 == row_count) {
      // ignore
    } else if (OB_FAIL(get_autoincrement_value(row_count, pk_interval))) {
      LOG_WARN("fail to get autoincrement value", KR(ret), K(row_count));
    } else if (OB_ISNULL(merge_task = OB_NEWx(ObDirectLoadPartitionHeapTableMultipleMergeTask,
                                              (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadPartitionHeapTableMultipleMergeTask", KR(ret));
    } else if (OB_FAIL(merge_task->init(param_, this, heap_table, pk_interval, parallel_idx++))) {
      LOG_WARN("fail to init merge task", KR(ret));
    } else if (OB_FAIL(task_array_.push_back(merge_task))) {
      LOG_WARN("fail to push back merge task", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != merge_task) {
        merge_task->~ObDirectLoadPartitionHeapTableMultipleMergeTask();
        allocator_.free(merge_task);
        merge_task = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_aggregate_merge_task_for_multiple_heap_table(
  const ObIArray<ObIDirectLoadPartitionTable *> &table_array)
{
  int ret = OB_SUCCESS;
  int64_t total_row_count = 0;
  ObTabletCacheInterval pk_interval;
  ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask *merge_task = nullptr;
  if (OB_FAIL(init_multiple_heap_table_array(table_array))) {
    LOG_WARN("fail to init multiple heap table array", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < multiple_heap_table_array_.count(); ++i) {
    ObDirectLoadMultipleHeapTable *heap_table = multiple_heap_table_array_.at(i);
    int64_t row_count = 0;
    if (OB_FAIL(heap_table->get_tablet_row_count(tablet_id_, param_.table_data_desc_, row_count))) {
      LOG_WARN("fail to get tablet row count", KR(ret), K(tablet_id_));
    } else {
      total_row_count += row_count;
    }
  }
  if (OB_SUCC(ret)) {
    if (total_row_count > 0 && OB_FAIL(get_autoincrement_value(total_row_count, pk_interval))) {
      LOG_WARN("fail to get autoincrement value", KR(ret), K(total_row_count));
    } else if (OB_ISNULL(merge_task =
                           OB_NEWx(ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask,
                                   (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask", KR(ret));
    } else if (OB_FAIL(merge_task->init(param_, this, &origin_table_, multiple_heap_table_array_,
                                        pk_interval))) {
      LOG_WARN("fail to init merge task", KR(ret));
    } else if (OB_FAIL(task_array_.push_back(merge_task))) {
      LOG_WARN("fail to push back merge task", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != merge_task) {
      merge_task->~ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask();
      allocator_.free(merge_task);
      merge_task = nullptr;
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::get_autoincrement_value(uint64_t count,
                                                        ObTabletCacheInterval &interval)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(count));
  } else {
    const uint64_t tenant_id = MTL_ID();
    ObTabletAutoincrementService &auto_inc = ObTabletAutoincrementService::get_instance();
    interval.tablet_id_ = tablet_id_;
    interval.cache_size_ = count;
    if (OB_FAIL(auto_inc.get_tablet_cache_interval(tenant_id, interval))) {
      LOG_WARN("fail to get tablet cache interval", K(ret), K(tenant_id), K_(tablet_id));
    } else if (OB_UNLIKELY(count > interval.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected autoincrement value count", K(ret), K(count), K(interval));
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::inc_finish_count(bool &is_ready)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTabletMergeCtx not init", KR(ret), KP(this));
  } else {
    const int64_t finish_count = ATOMIC_AAF(&task_finish_count_, 1);
    is_ready = (finish_count >= task_array_.count());
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
