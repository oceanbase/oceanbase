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
#include "storage/direct_load/ob_direct_load_external_multi_partition_table.h"
#include "storage/direct_load/ob_direct_load_insert_lob_table_ctx.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable.h"
#include "storage/direct_load/ob_direct_load_partition_del_lob_task.h"
#include "storage/direct_load/ob_direct_load_partition_merge_task.h"
#include "storage/direct_load/ob_direct_load_partition_rescan_task.h"
#include "storage/direct_load/ob_direct_load_range_splitter.h"
#include "storage/direct_load/ob_direct_load_table_store.h"
#include "storage/direct_load/ob_direct_load_sstable.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;
using namespace common;
using namespace share;
using namespace table;

DEFINE_ENUM_FUNC(ObDirectLoadMergeMode::Type, type, OB_DIRECT_LOAD_MERGE_MODE_DEF,
                 ObDirectLoadMergeMode::);

/**
 * ObDirectLoadMergeParam
 */

ObDirectLoadMergeParam::ObDirectLoadMergeParam()
  : table_id_(OB_INVALID_ID),
    rowkey_column_num_(0),
    column_count_(0),
    col_descs_(nullptr),
    datum_utils_(nullptr),
    lob_column_idxs_(nullptr),
    merge_mode_(ObDirectLoadMergeMode::INVALID_MERGE_MODE),
    dml_row_handler_(nullptr),
    insert_table_ctx_(nullptr),
    trans_param_(),
    file_mgr_(nullptr),
    ctx_(nullptr)
{
}

ObDirectLoadMergeParam::~ObDirectLoadMergeParam() {}

bool ObDirectLoadMergeParam::is_valid() const
{
  return OB_INVALID_ID != table_id_ && 0 < rowkey_column_num_ && 0 < column_count_ &&
         nullptr != col_descs_ && nullptr != datum_utils_ && nullptr != lob_column_idxs_ &&
         ObDirectLoadMergeMode::is_type_valid(merge_mode_) && nullptr != insert_table_ctx_ &&
         nullptr != file_mgr_ && nullptr != ctx_;
}

/**
 * ObDirectLoadMergeCtx
 */

ObDirectLoadMergeCtx::ObDirectLoadMergeCtx() : allocator_("TLD_MergeCtx"), is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  tablet_merge_ctx_array_.set_tenant_id(MTL_ID());
}

ObDirectLoadMergeCtx::~ObDirectLoadMergeCtx()
{
  reset();
}

void ObDirectLoadMergeCtx::reset()
{
  is_inited_ = false;
  for (int64_t i = 0; i < tablet_merge_ctx_array_.count(); ++i) {
    ObDirectLoadTabletMergeCtx *tablet_ctx = tablet_merge_ctx_array_.at(i);
    tablet_ctx->~ObDirectLoadTabletMergeCtx();
    allocator_.free(tablet_ctx);
  }
  tablet_merge_ctx_array_.reset();
  allocator_.reset();
}

int ObDirectLoadMergeCtx::init(const ObDirectLoadMergeParam &param,
                               const ObIArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMerger init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid() || ls_partition_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param), K(ls_partition_ids));
  } else {
    param_ = param;
    if (OB_FAIL(create_all_tablet_ctxs(ls_partition_ids))) {
      LOG_WARN("fail to create all tablet ctxs", KR(ret));
    } else {
      struct
      {
        bool operator()(const ObDirectLoadTabletMergeCtx *lhs,
                        const ObDirectLoadTabletMergeCtx *rhs)
        {
          return lhs->get_tablet_id().compare(rhs->get_tablet_id()) < 0;
        }
      } merge_ctx_compare;
      lib::ob_sort(tablet_merge_ctx_array_.begin(), tablet_merge_ctx_array_.end(),
                   merge_ctx_compare);
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMergeCtx::create_all_tablet_ctxs(
  const ObIArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_partition_ids.count(); ++i) {
    const ObTableLoadLSIdAndPartitionId &ls_partition_id = ls_partition_ids.at(i);
    ObDirectLoadTabletMergeCtx *tablet_ctx = nullptr;
    if (OB_ISNULL(tablet_ctx = OB_NEWx(ObDirectLoadTabletMergeCtx, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadTabletMergeCtx", KR(ret));
    } else if (OB_FAIL(tablet_ctx->init(this, ls_partition_id))) {
      LOG_WARN("fail to init tablet ctx", KR(ret), K(ls_partition_id));
    } else if (OB_FAIL(tablet_merge_ctx_array_.push_back(tablet_ctx))) {
      LOG_WARN("fail to push back", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != tablet_ctx) {
        tablet_ctx->~ObDirectLoadTabletMergeCtx();
        allocator_.free(tablet_ctx);
        tablet_ctx = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadMergeCtx::build_merge_task(ObDirectLoadTableStore &table_store, int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMergeCtx not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!table_store.is_valid() || thread_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_store), K(thread_cnt));
  } else {
    if (table_store.empty()) {
      // 无导入数据
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_merge_ctx_array_.count(); ++i) {
        ObDirectLoadTabletMergeCtx *tablet_merge_ctx = tablet_merge_ctx_array_.at(i);
        if (OB_FAIL(tablet_merge_ctx->build_empty_data_merge_task(table_store.get_table_data_desc(),
                                                                  thread_cnt))) {
          LOG_WARN("fail to build empty data merge task", KR(ret));
        }
      }
    } else if (table_store.is_multiple_heap_table()) {
      // 无主键排序数据, 多分区混合
      abort_unless(1 == table_store.size());
      const ObDirectLoadTableHandleArray &multiple_heap_table_array = *table_store.begin()->second;
      if (tablet_merge_ctx_array_.count() > thread_cnt * 2) {
        // 分区数目超过线程数目的2倍, 一个分区构造一个合并任务
        for (int64_t i = 0; OB_SUCC(ret) && i < tablet_merge_ctx_array_.count(); ++i) {
          ObDirectLoadTabletMergeCtx *tablet_merge_ctx = tablet_merge_ctx_array_.at(i);
          if (OB_FAIL(tablet_merge_ctx->build_aggregate_merge_task_for_multiple_heap_table(
                table_store.get_table_data_desc(), multiple_heap_table_array))) {
            LOG_WARN("fail to build aggregate merge task", KR(ret));
          }
        }
      } else {
        // 每个分区都构造多个合并任务
        for (int64_t i = 0; OB_SUCC(ret) && i < tablet_merge_ctx_array_.count(); ++i) {
          ObDirectLoadTabletMergeCtx *tablet_merge_ctx = tablet_merge_ctx_array_.at(i);
          if (OB_FAIL(tablet_merge_ctx->build_merge_task_for_multiple_heap_table(
                table_store.get_table_data_desc(), multiple_heap_table_array, thread_cnt))) {
            LOG_WARN("fail to build multiple heap table merge task", KR(ret));
          }
        }
      }
    } else if (table_store.is_multiple_sstable()) {
      // 现在的sstable是通过multiple_sstable模拟的
      // multiple_sstable和sstable在这里的主要差别是划分range的方式不同
      // 这里先简单通过table_map.size()是否大于1判断是不是sstable
      // 当table_map.size()为1时, 可能是sstable也可能是multiple_sstable, 这时候划分range的方式就没差别了, 统一走multiple_sstable逻辑
      if (table_store.size() > 1) { // sstable
        // 有主键表不排序, 各分区独立
        ObDirectLoadTableHandleArray *sstable_array = nullptr;
        for (int64_t i = 0; OB_SUCC(ret) && i < tablet_merge_ctx_array_.count(); ++i) {
          ObDirectLoadTabletMergeCtx *tablet_merge_ctx = tablet_merge_ctx_array_.at(i);
          const ObTabletID &tablet_id = tablet_merge_ctx->get_tablet_id();
          if (OB_FAIL(table_store.get_tablet_tables(tablet_id, sstable_array))) {
            if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
              LOG_WARN("fail to get tablet tables", KR(ret), K(tablet_id));
            } else {
              ret = OB_SUCCESS;
              if (OB_FAIL(tablet_merge_ctx->build_empty_data_merge_task(
                    table_store.get_table_data_desc(), thread_cnt))) {
                LOG_WARN("fail to build empty data merge task", KR(ret));
              }
            }
          } else if (OB_FAIL(tablet_merge_ctx->build_merge_task_for_sstable(
                       table_store.get_table_data_desc(), *sstable_array, thread_cnt))) {
            LOG_WARN("fail to build sstable merge task", KR(ret));
          }
        }
      } else { // multipe_sstable
        // 有主键表排序数据, 多分区混合
        const ObDirectLoadTableHandleArray &multiple_sstable_array = *table_store.begin()->second;
        ObDirectLoadMultipleMergeRangeSplitter range_splitter;
        if (OB_FAIL(range_splitter.init(multiple_sstable_array, table_store.get_table_data_desc(),
                                        param_.datum_utils_, *param_.col_descs_))) {
          LOG_WARN("fail to init range splitter", KR(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < tablet_merge_ctx_array_.count(); ++i) {
          ObDirectLoadTabletMergeCtx *tablet_merge_ctx = tablet_merge_ctx_array_.at(i);
          if (OB_FAIL(tablet_merge_ctx->build_merge_task_for_multiple_sstable(
                table_store.get_table_data_desc(), multiple_sstable_array, range_splitter,
                thread_cnt))) {
            LOG_WARN("fail to build multiple sstable merge task", KR(ret));
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table store", KR(ret), K(table_store));
    }
  }
  return ret;
}

int ObDirectLoadMergeCtx::build_del_lob_task(ObDirectLoadTableStore &table_store,
                                             int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMergeCtx not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!table_store.is_valid() || thread_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_store));
  } else {
    if (table_store.empty()) {
      // 没有需要删除的lob数据
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_merge_ctx_array_.count(); ++i) {
        ObDirectLoadTabletMergeCtx *tablet_merge_ctx = tablet_merge_ctx_array_.at(i);
        if (OB_FAIL(tablet_merge_ctx->build_empty_data_merge_task(table_store.get_table_data_desc(),
                                                                  thread_cnt))) {
          LOG_WARN("fail to build empty data merge task", KR(ret));
        }
      }
    } else if (OB_UNLIKELY(!table_store.is_multiple_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected not multiple table", KR(ret), K(table_store));
    } else {
      abort_unless(1 == table_store.size());
      const ObDirectLoadTableHandleArray &table_handle_array = *table_store.begin()->second;
      ObDirectLoadMultipleMergeRangeSplitter range_splitter;
      if (OB_FAIL(range_splitter.init(table_handle_array, table_store.get_table_data_desc(),
                                      param_.datum_utils_, *param_.col_descs_))) {
        LOG_WARN("fail to init range splitter", KR(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_merge_ctx_array_.count(); ++i) {
        ObDirectLoadTabletMergeCtx *tablet_merge_ctx = tablet_merge_ctx_array_.at(i);
        ObDirectLoadInsertTabletContext *insert_tablet_ctx =
          tablet_merge_ctx->get_insert_tablet_ctx();
        if (OB_FAIL(tablet_merge_ctx->build_del_lob_task(
              table_store.get_table_data_desc(), table_handle_array, range_splitter, thread_cnt))) {
          LOG_WARN("fail to build del lob task", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadMergeCtx::build_rescan_task(int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMergeCtx not init", KR(ret), KP(this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_merge_ctx_array_.count(); ++i) {
      ObDirectLoadTabletMergeCtx *tablet_merge_ctx = tablet_merge_ctx_array_.at(i);
      if (OB_FAIL(tablet_merge_ctx->build_rescan_task(thread_cnt))) {
        LOG_WARN("fail to build rescan task", KR(ret));
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadTabletMergeCtx
 */

ObDirectLoadTabletMergeCtx::ObDirectLoadTabletMergeCtx()
  : allocator_("TLD_MegTbtCtx"),
    merge_ctx_(nullptr),
    param_(nullptr),
    insert_tablet_ctx_(nullptr),
    parallel_idx_(0),
    task_finish_cnt_(0),
    task_ret_code_(OB_SUCCESS),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  range_array_.set_tenant_id(MTL_ID());
  merge_task_array_.set_tenant_id(MTL_ID());
}

ObDirectLoadTabletMergeCtx::~ObDirectLoadTabletMergeCtx() { reset(); }

void ObDirectLoadTabletMergeCtx::reset()
{
  is_inited_ = false;
  merge_ctx_ = nullptr;
  param_ = nullptr;
  tablet_id_.reset();
  insert_tablet_ctx_ = nullptr;
  origin_table_.reset();
  range_array_.reset();
  for (int64_t i = 0; i < merge_task_array_.count(); ++i) {
    ObDirectLoadIMergeTask *task = merge_task_array_.at(i);
    task->~ObDirectLoadIMergeTask();
    allocator_.free(task);
  }
  merge_task_array_.reset();
  parallel_idx_ = 0;
  task_finish_cnt_ = 0;
  task_ret_code_ = OB_SUCCESS;
  allocator_.reset();
}

int ObDirectLoadTabletMergeCtx::init(ObDirectLoadMergeCtx *merge_ctx,
                                     const ObTableLoadLSIdAndPartitionId &ls_partition_id)

{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadTabletMergeCtx init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == merge_ctx || !ls_partition_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(merge_ctx), K(ls_partition_id));
  } else {
    merge_ctx_ = merge_ctx;
    param_ = &merge_ctx->param_;
    tablet_id_ = ls_partition_id.part_tablet_id_.tablet_id_;
    if (OB_FAIL(param_->insert_table_ctx_->get_tablet_context(tablet_id_, insert_tablet_ctx_))) {
      LOG_WARN("fail to get insert tablet ctx", KR(ret), K(tablet_id_));
    } else if (ObDirectLoadMergeMode::merge_need_origin_table(param_->merge_mode_)) {
      ObDirectLoadOriginTableCreateParam origin_table_param;
      origin_table_param.table_id_ = param_->table_id_;
      origin_table_param.ls_id_ = ls_partition_id.ls_id_;
      origin_table_param.tablet_id_ = tablet_id_;
      origin_table_param.tx_id_ = param_->trans_param_.tx_id_;
      origin_table_param.tx_seq_ = param_->trans_param_.tx_seq_;
      if (OB_FAIL(origin_table_.init(origin_table_param))) {
        LOG_WARN("fail to init origin table", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

// construct empty task for close tablet
int ObDirectLoadTabletMergeCtx::build_empty_merge_task()
{
  int ret = OB_SUCCESS;
  ObDirectLoadPartitionEmptyMergeTask *merge_task = nullptr;
  if (OB_ISNULL(merge_task = OB_NEWx(ObDirectLoadPartitionEmptyMergeTask, (&allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObDirectLoadPartitionEmptyMergeTask", KR(ret));
  } else if (OB_FAIL(merge_task->init(this))) {
    LOG_WARN("fail to init merge task", KR(ret));
  } else if (OB_FAIL(merge_task_array_.push_back(merge_task))) {
    LOG_WARN("fail to push back merge task", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != merge_task) {
      merge_task->~ObDirectLoadPartitionEmptyMergeTask();
      allocator_.free(merge_task);
      merge_task = nullptr;
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_origin_data_merge_task(
  const ObDirectLoadTableDataDesc &table_data_desc, const int64_t max_parallel_degree)
{
  int ret = OB_SUCCESS;
  range_array_.reset();
  if (max_parallel_degree <= 1) {
    ObDatumRange whole_range;
    whole_range.set_whole_range();
    if (OB_FAIL(range_array_.push_back(whole_range))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  } else {
    ObDirectLoadTableHandleArray empty_table_array;
    ObDirectLoadMergeRangeSplitter range_splitter;
    if (OB_FAIL(range_splitter.init(tablet_id_, &origin_table_, empty_table_array, table_data_desc,
                                    param_->datum_utils_, *param_->col_descs_))) {
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
    ObDirectLoadPartitionOriginDataMergeTask *merge_task = nullptr;
    if (OB_ISNULL(merge_task = OB_NEWx(ObDirectLoadPartitionOriginDataMergeTask, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadPartitionOriginDataMergeTask", KR(ret));
    } else if (OB_FAIL(merge_task->init(this, origin_table_, range, parallel_idx_++))) {
      LOG_WARN("fail to init merge task", KR(ret));
    } else if (OB_FAIL(merge_task_array_.push_back(merge_task))) {
      LOG_WARN("fail to push back merge task", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != merge_task) {
        merge_task->~ObDirectLoadPartitionOriginDataMergeTask();
        allocator_.free(merge_task);
        merge_task = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_origin_data_unrescan_merge_task(
  const ObDirectLoadTableDataDesc &table_data_desc, const int64_t max_parallel_degree)
{
  int ret = OB_SUCCESS;
  range_array_.reset();
  if (max_parallel_degree <= 1) {
    ObDatumRange whole_range;
    whole_range.set_whole_range();
    if (OB_FAIL(range_array_.push_back(whole_range))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  } else {
    ObDirectLoadTableHandleArray empty_table_array;
    ObDirectLoadMergeRangeSplitter range_splitter;
    if (OB_FAIL(range_splitter.init(tablet_id_, &origin_table_, empty_table_array, table_data_desc,
                                    param_->datum_utils_, *param_->col_descs_))) {
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
    ObDirectLoadPartitionOriginDataUnrescanMergeTask *merge_task = nullptr;
    if (OB_ISNULL(merge_task = OB_NEWx(ObDirectLoadPartitionOriginDataUnrescanMergeTask, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadPartitionOriginDataUnrescanMergeTask", KR(ret));
    } else if (OB_FAIL(merge_task->init(this, origin_table_, range, parallel_idx_++))) {
      LOG_WARN("fail to init merge task", KR(ret));
    } else if (OB_FAIL(merge_task_array_.push_back(merge_task))) {
      LOG_WARN("fail to push back merge task", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != merge_task) {
        merge_task->~ObDirectLoadPartitionOriginDataUnrescanMergeTask();
        allocator_.free(merge_task);
        merge_task = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_empty_data_merge_task(
  const ObDirectLoadTableDataDesc &table_data_desc, int64_t max_parallel_degree)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTabletMergeCtx not init", KR(ret), KP(this));
  } else if (!merge_with_origin_data()) {
    // construct empty merge task for close tablet
    if (OB_FAIL(build_empty_merge_task())) {
      LOG_WARN("fail to build empty merge task", KR(ret));
    }
  } else {
    // only origin data, construct task by split range
    if (OB_FAIL(build_origin_data_merge_task(table_data_desc, max_parallel_degree))) {
      LOG_WARN("fail to build origin data merge task", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_merge_task_for_sstable(
  const ObDirectLoadTableDataDesc &table_data_desc,
  const ObDirectLoadTableHandleArray &sstable_array, int64_t max_parallel_degree)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTabletMergeCtx not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(sstable_array.empty() || max_parallel_degree <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(sstable_array), K(max_parallel_degree));
  } else {
    // check is sstable
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
      const ObDirectLoadTableHandle &table_handle = sstable_array.at(i);
      ObDirectLoadMultipleSSTable *sstable = nullptr;
      if (OB_UNLIKELY(!table_handle.is_valid() ||
                      !table_handle.get_table()->is_multiple_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected not sstable", KR(ret), K(table_handle));
      } else if (FALSE_IT(sstable =
                            static_cast<ObDirectLoadMultipleSSTable *>(table_handle.get_table()))) {
      } else if (OB_UNLIKELY(sstable->get_tablet_id() != tablet_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected tablet id", KR(ret), K(tablet_id_), KPC(sstable));
      }
    }
    // split range
    if (OB_SUCC(ret)) {
      ObDirectLoadMergeRangeSplitter range_splitter;
      if (OB_FAIL(range_splitter.init(
            tablet_id_, (merge_with_origin_data() ? &origin_table_ : nullptr), sstable_array,
            table_data_desc, param_->datum_utils_, *param_->col_descs_))) {
        LOG_WARN("fail to init range splitter", KR(ret));
      } else if (OB_FAIL(
                   range_splitter.split_range(range_array_, max_parallel_degree, allocator_))) {
        LOG_WARN("fail to split range", KR(ret));
      } else if (OB_UNLIKELY(range_array_.count() > max_parallel_degree)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected range count", KR(ret), K(max_parallel_degree),
                 K(range_array_.count()));
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
      } else if (OB_FAIL(merge_task->init(this, origin_table_, table_data_desc, sstable_array,
                                          range, i))) {
        LOG_WARN("fail to init merge task", KR(ret));
      } else if (OB_FAIL(merge_task_array_.push_back(merge_task))) {
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

int ObDirectLoadTabletMergeCtx::build_merge_task_for_multiple_sstable(
  const ObDirectLoadTableDataDesc &table_data_desc,
  const ObDirectLoadTableHandleArray &multiple_sstable_array,
  ObDirectLoadMultipleMergeRangeSplitter &range_splitter, int64_t max_parallel_degree)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTabletMergeCtx not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(multiple_sstable_array.empty() || max_parallel_degree <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(multiple_sstable_array), K(max_parallel_degree));
  } else {
    // split range
    if (OB_FAIL(range_splitter.split_range(tablet_id_,
                                           (merge_with_origin_data() ? &origin_table_ : nullptr),
                                           max_parallel_degree, range_array_, allocator_))) {
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
      } else if (OB_FAIL(merge_task->init(this, origin_table_, table_data_desc,
                                          multiple_sstable_array, range, i))) {
        LOG_WARN("fail to init merge task", KR(ret));
      } else if (OB_FAIL(merge_task_array_.push_back(merge_task))) {
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

int ObDirectLoadTabletMergeCtx::build_merge_task_for_multiple_heap_table(
  const ObDirectLoadTableDataDesc &table_data_desc,
  const ObDirectLoadTableHandleArray &multiple_heap_table_array, int64_t max_parallel_degree)
{
  int ret = OB_SUCCESS;
  // 1. build origin data task
  if (merge_with_origin_data() &&
      OB_FAIL(build_origin_data_merge_task(table_data_desc, max_parallel_degree))) {
    LOG_WARN("fail to build origin data merge task", KR(ret));
  }
  // 2. build multiple heap table task
  for (int64_t i = 0; OB_SUCC(ret) && i < multiple_heap_table_array.count(); ++i) {
    const ObDirectLoadTableHandle &table_handle = multiple_heap_table_array.at(i);
    ObDirectLoadMultipleHeapTable *heap_table = nullptr;
    ObDirectLoadPartitionHeapTableMultipleMergeTask *merge_task = nullptr;
    int64_t row_count = 0;
    ObTabletCacheInterval pk_interval;
    if (OB_UNLIKELY(!table_handle.is_valid() ||
                    !table_handle.get_table()->is_multiple_heap_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected not multiple heap table", KR(ret), K(table_handle));
    } else if (FALSE_IT(heap_table =
                          static_cast<ObDirectLoadMultipleHeapTable *>(table_handle.get_table()))) {
    } else if (OB_FAIL(heap_table->get_tablet_row_count(tablet_id_, table_data_desc, row_count))) {
      LOG_WARN("fail to get tablet row count", KR(ret), K(tablet_id_));
    } else if (0 == row_count) {
      // ignore
    } else if (OB_FAIL(get_autoincrement_value(row_count, pk_interval))) {
      LOG_WARN("fail to get autoincrement value", KR(ret), K(row_count));
    } else if (OB_ISNULL(merge_task = OB_NEWx(ObDirectLoadPartitionHeapTableMultipleMergeTask,
                                              (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadPartitionHeapTableMultipleMergeTask", KR(ret));
    } else if (OB_FAIL(merge_task->init(this, table_data_desc, table_handle, pk_interval,
                                        parallel_idx_++))) {
      LOG_WARN("fail to init merge task", KR(ret));
    } else if (OB_FAIL(merge_task_array_.push_back(merge_task))) {
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
  // 如果没有构造出merge_task, 则需要补上一个空的任务来关闭tablet
  if (OB_SUCC(ret) && merge_task_array_.empty()) {
    if (OB_FAIL(build_empty_merge_task())) {
      LOG_WARN("fail to build empty merge task", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_aggregate_merge_task_for_multiple_heap_table(
  const ObDirectLoadTableDataDesc &table_data_desc,
  const ObDirectLoadTableHandleArray &multiple_heap_table_array)
{
  int ret = OB_SUCCESS;
  // 1. 统计所有multiple_heap_table中的行数
  int64_t total_row_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < multiple_heap_table_array.count(); ++i) {
    const ObDirectLoadTableHandle &table_handle = multiple_heap_table_array.at(i);
    ObDirectLoadMultipleHeapTable *heap_table = nullptr;
    int64_t row_count = 0;
    if (OB_UNLIKELY(!table_handle.is_valid() ||
                    !table_handle.get_table()->is_multiple_heap_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected not multiple heap table", KR(ret), K(table_handle));
    } else if (FALSE_IT(heap_table =
                          static_cast<ObDirectLoadMultipleHeapTable *>(table_handle.get_table()))) {
    } else if (OB_FAIL(heap_table->get_tablet_row_count(tablet_id_, table_data_desc, row_count))) {
      LOG_WARN("fail to get tablet row count", KR(ret), K(tablet_id_));
    } else {
      total_row_count += row_count;
    }
  }
  // 2. 构造merge_task
  if (OB_SUCC(ret)) {
    if (total_row_count == 0) {
      // 没有这个分区的数据
      if (OB_FAIL(build_empty_data_merge_task(table_data_desc, 1 /*max_parallel_degree*/))) {
        LOG_WARN("fail to build empty data merge task", KR(ret));
      }
    } else {
      ObDirectLoadIMergeTask *merge_task = nullptr;
      ObTabletCacheInterval pk_interval;
      if (OB_FAIL(get_autoincrement_value(total_row_count, pk_interval))) {
        LOG_WARN("fail to get autoincrement value", KR(ret), K(total_row_count));
      } else if (merge_with_origin_data()) { // origin + multiple_heap_tables
        ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask *aggregate_merge_task = nullptr;
        if (OB_ISNULL(merge_task = aggregate_merge_task = OB_NEWx(
                        ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask, (&allocator_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask", KR(ret));
        } else if (OB_FAIL(aggregate_merge_task->init(this, origin_table_, table_data_desc,
                                                      multiple_heap_table_array, pk_interval))) {
          LOG_WARN("fail to init merge task", KR(ret));
        }
      } else { // multiple_heap_tables only
        ObDirectLoadPartitionHeapTableMultipleMergeTask *multiple_merge_task = nullptr;
        if (OB_ISNULL(merge_task = multiple_merge_task =
                        OB_NEWx(ObDirectLoadPartitionHeapTableMultipleMergeTask, (&allocator_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new ObDirectLoadPartitionHeapTableMultipleMergeTask", KR(ret));
        } else if (OB_FAIL(multiple_merge_task->init(this, table_data_desc,
                                                     multiple_heap_table_array, pk_interval))) {
          LOG_WARN("fail to init merge task", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(merge_task_array_.push_back(merge_task))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
        if (nullptr != merge_task) {
          merge_task->~ObDirectLoadIMergeTask();
          allocator_.free(merge_task);
          merge_task = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_del_lob_task(
  const ObDirectLoadTableDataDesc &table_data_desc,
  const ObDirectLoadTableHandleArray &multiple_sstable_array,
  ObDirectLoadMultipleMergeRangeSplitter &range_splitter, const int64_t max_parallel_degree)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTabletMergeCtx not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(max_parallel_degree <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(max_parallel_degree), K(multiple_sstable_array));
  } else {
    // 预期中, 删除的lob_id < 插入的lob_id
    // 由于dml和旁路生成lob_id的规则不一致, 导致dml的lob_id > 旁路的lob_id
    // 也就是: 待删除的旁路lob_id < 插入的旁路lob_id < 待删除的dml的lob_id
    // 1. 当本次导入没有插入旁路的outrow数据, 则可以忽略这个问题
    // 2. 本次导入有插入旁路的outrow数据, 则del_lob数据要以插入的旁路lob_id(取min_insert_lob_id)为界, 分成两部分写
    //      sstable数据分布: [删除的旁路的lob_id] [插入的旁路的lob_id] [删除的dml的lob_id]
    //      range: (min, dl_k1], ..., (dl_kn, min_insert_lob_id] + (min_insert_lob_id, dml_k1], ..., (dml_km, max)
    //      data_seq: [0, parallel] + [last_data_seq.parallel_idx_ + 1, ...]
    const ObLobId &min_insert_lob_id =
      static_cast<ObDirectLoadInsertLobTabletContext *>(insert_tablet_ctx_)
        ->get_min_insert_lob_id();
    const ObMacroDataSeq &last_data_seq = insert_tablet_ctx_->get_last_data_seq();
    int64_t first_no_insert_front_idx = -1;
    if (OB_FAIL(range_splitter.split_range(tablet_id_, nullptr /*origin_table*/,
                                           max_parallel_degree, range_array_, allocator_))) {
      LOG_WARN("fail to split range", KR(ret));
    } else if (OB_UNLIKELY(range_array_.count() > max_parallel_degree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected range count", KR(ret), K(max_parallel_degree), K(range_array_.count()));
    } else if (min_insert_lob_id.is_valid()) {
      // 本次导入有插入旁路的outrow数据, 将range以min_insert_lob_id为界拆分
      // first_no_insert_front_idx是第一个待删除的dml的lob_id所属的range
      int cmp_ret = 0;
      ObStorageDatum min_insert_lob_id_datum;
      ObDatumRowkey tmp_min_insert_lob_id_rowkey;
      ObDatumRowkey min_insert_lob_id_rowkey;
      min_insert_lob_id_datum.set_string(reinterpret_cast<const char *>(&min_insert_lob_id),
                                         sizeof(ObLobId));
      if (OB_FAIL(tmp_min_insert_lob_id_rowkey.assign(&min_insert_lob_id_datum, 1))) {
        LOG_WARN("fail to assign min insert lob id rowkey", KR(ret));
      } else if (OB_FAIL(
                   tmp_min_insert_lob_id_rowkey.deep_copy(min_insert_lob_id_rowkey, allocator_))) {
        LOG_WARN("fail to deep copy rowkey", KR(ret), K(tmp_min_insert_lob_id_rowkey));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < range_array_.count(); ++i) {
        ObDatumRange &range = range_array_.at(i);
        const ObDatumRowkey &end_key = range.get_end_key();
        if (OB_FAIL(end_key.compare(min_insert_lob_id_rowkey, *param_->datum_utils_, cmp_ret))) {
          LOG_WARN("fail to compare lob id", KR(ret), K(end_key), K(min_insert_lob_id_rowkey));
        } else if (cmp_ret > 0) {
          // 将当前range拆分成2个range: range, new_range
          ObDatumRange new_range = range;
          range.end_key_ = min_insert_lob_id_rowkey;
          range.set_right_closed();
          new_range.start_key_ = min_insert_lob_id_rowkey;
          new_range.set_left_open();
          if (OB_FAIL(range_array_.push_back(new_range))) {
            LOG_WARN("fail to push back", KR(ret));
          } else if (i < range_array_.count() - 1) {
            // 拆分的不是最后一个range, 需要把后面的range都后移
            for (int64_t j = range_array_.count() - 1; j > i; --j) {
              range_array_[j] = range_array_[j - 1];
            }
            range_array_[i + 1] = new_range;
          }
          first_no_insert_front_idx = i + 1;
          break;
        }
      }
      LOG_INFO("split range by min insert lob id", KR(ret), K(min_insert_lob_id),
               K(min_insert_lob_id_datum), K(range_array_), K(first_no_insert_front_idx));
    }
    // construct task per range
    bool insert_front = true;
    int64_t parallel_idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < range_array_.count(); ++i) {
      const ObDatumRange &range = range_array_.at(i);
      ObMacroDataSeq data_seq;
      ObDirectLoadPartitionDelLobTask *del_lob_task = nullptr;
      if (i == first_no_insert_front_idx) {
        insert_front = false;
        parallel_idx = 0;
      }
      if (insert_front) {
        if (OB_FAIL(data_seq.set_parallel_degree(parallel_idx))) {
          LOG_WARN("fail to set parallel degree", KR(ret), K(parallel_idx));
        }
      } else {
        if (OB_FAIL(
              data_seq.set_parallel_degree(last_data_seq.get_parallel_idx() + parallel_idx + 1))) {
          LOG_WARN("fail to set parallel degree", KR(ret), K(parallel_idx));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(del_lob_task =
                             OB_NEWx(ObDirectLoadPartitionDelLobTask, (&allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObDirectLoadPartitionDelLobTask", KR(ret));
      } else if (OB_FAIL(del_lob_task->init(this, origin_table_, table_data_desc,
                                            multiple_sstable_array, range, data_seq,
                                            parallel_idx++))) {
        LOG_WARN("fail to init del lob task", KR(ret));
      } else if (OB_FAIL(merge_task_array_.push_back(del_lob_task))) {
        LOG_WARN("fail to push back del lob task", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != del_lob_task) {
          del_lob_task->~ObDirectLoadPartitionDelLobTask();
          allocator_.free(del_lob_task);
          del_lob_task = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_rescan_task(int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTabletMergeCtx not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(thread_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(thread_cnt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < thread_cnt; ++i) {
    ObDirectLoadPartitionRescanTask *rescan_task = nullptr;
    if (OB_ISNULL(rescan_task = OB_NEWx(ObDirectLoadPartitionRescanTask, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadPartitionRescanTask", KR(ret));
    } else if (OB_FAIL(rescan_task->init(this, thread_cnt, i))) {
      LOG_WARN("fail to init merge task", KR(ret));
    } else if (OB_FAIL(merge_task_array_.push_back(rescan_task))) {
      LOG_WARN("fail to push back rescan task", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != rescan_task) {
        rescan_task->~ObDirectLoadPartitionRescanTask();
        allocator_.free(rescan_task);
        rescan_task = nullptr;
      }
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

int ObDirectLoadTabletMergeCtx::inc_finish_count(int ret_code, bool &is_ready)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  is_ready = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTabletMergeCtx not init", KR(ret), KP(this));
  } else {
    ObMutexGuard guard(mutex_);
    ++task_finish_cnt_;
    if (OB_TMP_FAIL(task_ret_code_)) {
      // 其他任务已经失败,
    } else if (OB_TMP_FAIL(ret_code)) {
      // 当前任务失败, 设置错误码
      task_ret_code_ = ret_code;
    } else {
      // 没有任务失败
      is_ready = (task_finish_cnt_ >= merge_task_array_.count());
    }
  }
  return ret;
}

/**
 * ObDirectLoadMergeTaskIterator
 */

ObDirectLoadMergeTaskIterator::ObDirectLoadMergeTaskIterator()
  : merge_ctx_(nullptr), tablet_merge_ctx_(nullptr), tablet_pos_(0), task_pos_(0), is_inited_(false)
{
}

ObDirectLoadMergeTaskIterator::~ObDirectLoadMergeTaskIterator() {}

int ObDirectLoadMergeTaskIterator::init(ObDirectLoadMergeCtx *merge_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMergeTaskIterator init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == merge_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(merge_ctx));
  } else {
    merge_ctx_ = merge_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadMergeTaskIterator::get_next_task(ObDirectLoadIMergeTask *&task)
{
  int ret = OB_SUCCESS;
  task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMergeTaskIterator not init", KR(ret), KP(this));
  } else {
    while (OB_SUCC(ret) && nullptr == task) {
      if (nullptr == tablet_merge_ctx_) {
        // get next partition merge ctx
        const ObIArray<ObDirectLoadTabletMergeCtx *> &tablet_merge_ctxs =
          merge_ctx_->tablet_merge_ctx_array_;
        if (tablet_pos_ >= tablet_merge_ctxs.count()) {
          ret = OB_ITER_END;
        } else {
          tablet_merge_ctx_ = tablet_merge_ctxs.at(tablet_pos_++);
          task_pos_ = 0;
        }
      }
      if (OB_SUCC(ret)) {
        const ObIArray<ObDirectLoadIMergeTask *> &tasks = tablet_merge_ctx_->merge_task_array_;
        if (task_pos_ >= tasks.count()) {
          // try next partition
          tablet_merge_ctx_ = nullptr;
        } else {
          task = tasks.at(task_pos_++);
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
