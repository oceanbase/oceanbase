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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_parallel_table_compactor.h"
#include "observer/table_load/ob_table_load_merge_compact_table_op.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_task.h"
#include "observer/table_load/ob_table_load_task_scheduler.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_builder.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_compactor.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_scan_merge.h"
#include "storage/direct_load/ob_direct_load_range_splitter.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace lib;

/**
 * ObTableLoadParallelCompactTabletCtx
 */

ObTableLoadParallelCompactTabletCtx::ObTableLoadParallelCompactTabletCtx()
  : allocator_("TLD_ParalMerge"),
    merge_sstable_count_(0),
    range_count_(0),
    range_sstable_count_(0),
    range_allocator_("TLD_ParalMerge")
{
  allocator_.set_tenant_id(MTL_ID());
  range_allocator_.set_tenant_id(MTL_ID());
  ranges_.set_tenant_id(MTL_ID());
  range_sstables_.set_tenant_id(MTL_ID());
}

ObTableLoadParallelCompactTabletCtx::~ObTableLoadParallelCompactTabletCtx()
{
  sstables_.reset();
  range_sstables_.reset();
}

int ObTableLoadParallelCompactTabletCtx::set_parallel_merge_param(int64_t merge_sstable_count,
                                                                  int64_t range_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(merge_sstable_count <= 0 || range_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_sstable_count), K(range_count));
  } else if (OB_UNLIKELY(merge_sstable_count_ > 0 || range_count_ > 0 || range_sstable_count_ > 0 ||
                         ranges_.empty() || ranges_.count() != range_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tablet ctx", KR(ret), KPC(this));
  } else {
    merge_sstable_count_ = merge_sstable_count;
    range_count_ = range_count;
    range_sstable_count_ = 0;
    range_sstables_.reset();
    if (OB_FAIL(range_sstables_.prepare_allocate(range_count))) {
      LOG_WARN("fail to prepare allocate array", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadParallelCompactTabletCtx::finish_range_merge(
  int64_t range_idx, const ObDirectLoadTableHandle &range_sstable, bool &all_range_finish)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(range_idx < 0 || range_idx > range_count_ || !range_sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(range_idx), K(range_sstable));
  } else {
    ObMutexGuard guard(mutex_);
    if (range_sstables_.at(range_idx).is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected range idx", KR(ret), K(range_idx));
    } else {
      range_sstables_.at(range_idx) = range_sstable;
      all_range_finish = (++range_sstable_count_ >= range_count_);
    }
  }
  return ret;
}

int ObTableLoadParallelCompactTabletCtx::apply_merged_sstable(
  const ObDirectLoadTableHandle &merged_sstable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!merged_sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merged_sstable));
  } else if (OB_UNLIKELY(merge_sstable_count_ <= 0 || range_count_ <= 0 ||
                         range_count_ != range_sstable_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tablet ctx", KR(ret), KPC(this));
  } else {
    ObDirectLoadTableHandleArray result_sstables;
    LOG_INFO("parallel merge apply merged", K(merge_sstable_count_), K(sstables_.count()));
    if (OB_FAIL(result_sstables.add(merged_sstable))) {
      LOG_WARN("fail to push back sstable", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sstables_.count(); ++i) {
      ObDirectLoadTableHandle sstable_handle;
      if (OB_FAIL(sstables_.get_table(i, sstable_handle))) {
        LOG_WARN("fail to get table", KR(ret), K(i));
      } else {
        if (i >= merge_sstable_count_) {
          if (OB_FAIL(result_sstables.add(sstable_handle))) {
            LOG_WARN("fail to push back sstable", KR(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sstables_.assign(result_sstables))) {
        LOG_WARN("fail to assign sstables", KR(ret));
      } else {
        // clear merge ctx
        merge_sstable_count_ = 0;
        range_count_ = 0;
        range_sstable_count_ = 0;
        range_sstables_.reset();
        ranges_.reset();
        range_allocator_.reset();
      }
    }
  }
  return ret;
}

/**
 * SSTableCompare
 */

ObTableLoadParallelTableCompactor::SSTableCompare::SSTableCompare() : result_code_(OB_SUCCESS) {}

ObTableLoadParallelTableCompactor::SSTableCompare::~SSTableCompare() {}

bool ObTableLoadParallelTableCompactor::SSTableCompare::operator()(
  const ObDirectLoadTableHandle lhs, const ObDirectLoadTableHandle rhs)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (OB_UNLIKELY(!lhs.is_valid() || !rhs.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(lhs), K(rhs));
  } else {
    ObDirectLoadMultipleSSTable *lhs_multi_sstable =
      static_cast<ObDirectLoadMultipleSSTable *>(lhs.get_table());
    ObDirectLoadMultipleSSTable *rhs_multi_sstable =
      static_cast<ObDirectLoadMultipleSSTable *>(rhs.get_table());
    cmp_ret = lhs_multi_sstable->get_meta().row_count_ - rhs_multi_sstable->get_meta().row_count_;
  }
  if (OB_FAIL(ret)) {
    result_code_ = ret;
  }
  return cmp_ret < 0;
}

/**
 * SplitRangeTaskProcessor
 */

class ObTableLoadParallelTableCompactor::SplitRangeTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  SplitRangeTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx,
                          ObTableLoadParallelTableCompactor *parallel_table_compactor,
                          ObTableLoadParallelCompactTabletCtx *tablet_ctx)
    : ObITableLoadTaskProcessor(task),
      ctx_(ctx),
      parallel_table_compactor_(parallel_table_compactor),
      tablet_ctx_(tablet_ctx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~SplitRangeTaskProcessor() { ObTableLoadService::put_ctx(ctx_); }
  int process() override
  {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(INFO, table_compactor_time_us);
    int ret = OB_SUCCESS;
    const int64_t merge_count_per_round = ctx_->store_ctx_->merge_count_per_round_;
    if (OB_UNLIKELY(tablet_ctx_->merge_sstable_count_ > 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected tablet ctx", KPC(tablet_ctx_));
    } else if (OB_UNLIKELY(tablet_ctx_->sstables_.count() <= merge_count_per_round)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected tablet sstable count", KPC(tablet_ctx_), K(merge_count_per_round));
    } else {
      const int64_t merge_sstable_count =
        MIN(tablet_ctx_->sstables_.count() - merge_count_per_round + 1, merge_count_per_round);
      ObDirectLoadTableHandleArray sstable_array;
      // sort sstable
      SSTableCompare compare;
      lib::ob_sort(tablet_ctx_->sstables_.begin(), tablet_ctx_->sstables_.end(), compare);
      // collect merged sstables
      for (int64_t i = 0; OB_SUCC(ret) && i < merge_sstable_count; ++i) {
        ObDirectLoadTableHandle sstable;
        if (OB_FAIL(tablet_ctx_->sstables_.get_table(i, sstable))) {
          LOG_WARN("fail to get table", KR(ret), K(i));
        } else if (OB_FAIL(sstable_array.add(sstable))) {
          LOG_WARN("fail to push back sstable", KR(ret));
        }
      }
      // split range
      if (OB_SUCC(ret)) {
        ObDirectLoadMultipleSSTableRangeSplitter range_splitter;
        if (OB_FAIL(range_splitter.init(sstable_array, parallel_table_compactor_->table_data_desc_,
                                        &ctx_->schema_.datum_utils_))) {
          LOG_WARN("fail to init range splitter", KR(ret));
        } else if (OB_FAIL(range_splitter.split_range(tablet_ctx_->ranges_,
                                                      parallel_table_compactor_->thread_count_,
                                                      tablet_ctx_->range_allocator_))) {
          LOG_WARN("fail to split range", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        LOG_INFO("parallel merge split range finish", K(tablet_ctx_->sstables_.count()),
                 K(merge_sstable_count), K(tablet_ctx_->ranges_.count()));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(tablet_ctx_->set_parallel_merge_param(merge_sstable_count,
                                                          tablet_ctx_->ranges_.count()))) {
          LOG_WARN("fail to set parallel merge param", KR(ret));
        } else if (OB_FAIL(
                     parallel_table_compactor_->handle_tablet_split_range_finish(tablet_ctx_))) {
          LOG_WARN("fail to handle tablet split range finish", KR(ret));
        }
      }
    }
    return ret;
  }

private:
  ObTableLoadTableCtx *ctx_;
  ObTableLoadParallelTableCompactor *parallel_table_compactor_;
  ObTableLoadParallelCompactTabletCtx *tablet_ctx_;
};

/**
 * MergeRangeTaskProcessor
 */

class ObTableLoadParallelTableCompactor::MergeRangeTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  MergeRangeTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx,
                          ObTableLoadParallelTableCompactor *parallel_table_compactor,
                          ObTableLoadParallelCompactTabletCtx *tablet_ctx, int64_t range_idx)
    : ObITableLoadTaskProcessor(task),
      ctx_(ctx),
      parallel_table_compactor_(parallel_table_compactor),
      tablet_ctx_(tablet_ctx),
      range_idx_(range_idx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~MergeRangeTaskProcessor() { ObTableLoadService::put_ctx(ctx_); }
  int process() override
  {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(INFO, table_compactor_time_us);
    int ret = OB_SUCCESS;
    const ObDirectLoadMultipleDatumRow *datum_row = nullptr;
    ObDirectLoadTableHandleArray table_array;
    bool all_range_finish = false;
    if (OB_FAIL(init_scan_merge())) {
      LOG_WARN("fail to init scan merge", KR(ret));
    } else if (OB_FAIL(init_sstable_builder())) {
      LOG_WARN("fail to init sstable builder", KR(ret));
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(scan_merge_.get_next_row(datum_row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(sstable_builder_.append_row(*datum_row))) {
        LOG_WARN("fail to append row", KR(ret));
      } else {
        ATOMIC_AAF(&ctx_->job_stat_->store_.compact_stage_merge_write_rows_, 1);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sstable_builder_.close())) {
        LOG_WARN("fail to close sstable builder", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObMutexGuard guard(tablet_ctx_->mutex_);
      if (OB_FAIL(sstable_builder_.get_tables(table_array, ctx_->store_ctx_->table_mgr_))) {
        LOG_WARN("fail to get tables", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObDirectLoadMultipleSSTable *sstable = nullptr;
      ObDirectLoadTableHandle sstable_handle;
      if (OB_UNLIKELY(1 != table_array.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table array count", KR(ret), K(table_array));
      } else if (OB_FAIL(table_array.get_table(0, sstable_handle))) {
        LOG_WARN("fail to get table", KR(ret));
      } else if (OB_FAIL(
                   tablet_ctx_->finish_range_merge(range_idx_, sstable_handle, all_range_finish))) {
        LOG_WARN("fail to finish range merge", KR(ret));
      }
      if (OB_FAIL(ret)) {
        table_array.reset();
      }
    }
    if (OB_SUCC(ret) && all_range_finish) {
      if (OB_FAIL(parallel_table_compactor_->handle_tablet_range_merge_finish(tablet_ctx_))) {
        LOG_WARN("fail to handle tablet range merge finish", KR(ret));
      }
    }
    return ret;
  }
  int init_scan_merge()
  {
    int ret = OB_SUCCESS;
    ObDirectLoadMultipleSSTableScanMergeParam scan_merge_param;
    scan_merge_param.table_data_desc_ = parallel_table_compactor_->table_data_desc_;
    scan_merge_param.datum_utils_ =
      &parallel_table_compactor_->op_->merge_table_ctx_->store_table_ctx_->schema_->datum_utils_;
    scan_merge_param.dml_row_handler_ =
      parallel_table_compactor_->op_->merge_table_ctx_->dml_row_handler_;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ctx_->merge_sstable_count_; ++i) {
      ObDirectLoadTableHandle sstable;
      if (OB_FAIL(tablet_ctx_->sstables_.get_table(i, sstable))) {
        LOG_WARN("fail to get table", KR(ret), K(i));
      } else if (OB_FAIL(sstable_array_.add(sstable))) {
        LOG_WARN("fail to push back sstable", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(scan_merge_.init(scan_merge_param, sstable_array_,
                                   tablet_ctx_->ranges_.at(range_idx_)))) {
        LOG_WARN("fail to init sstable scan merge", KR(ret));
      }
    }
    return ret;
  }
  int init_sstable_builder()
  {
    int ret = OB_SUCCESS;
    ObDirectLoadMultipleSSTableBuildParam build_param;
    build_param.tablet_id_ = tablet_ctx_->tablet_id_;
    build_param.table_data_desc_ = parallel_table_compactor_->table_data_desc_;
    build_param.datum_utils_ =
      &parallel_table_compactor_->op_->merge_table_ctx_->store_table_ctx_->schema_->datum_utils_;
    build_param.file_mgr_ = ctx_->store_ctx_->tmp_file_mgr_;
    build_param.extra_buf_ = reinterpret_cast<char *>(1); // unuse, delete in future
    build_param.extra_buf_size_ = 4096;
    if (OB_FAIL(sstable_builder_.init(build_param))) {
      LOG_WARN("fail to init sstable builder", KR(ret));
    }
    return ret;
  }

private:
  ObTableLoadTableCtx *ctx_;
  ObTableLoadParallelTableCompactor *parallel_table_compactor_;
  ObTableLoadParallelCompactTabletCtx *tablet_ctx_;
  int64_t range_idx_;
  ObDirectLoadTableHandleArray sstable_array_;
  ObDirectLoadMultipleSSTableScanMerge scan_merge_;
  ObDirectLoadMultipleSSTableBuilder sstable_builder_;
};

class ObTableLoadParallelTableCompactor::CompactSSTableTaskProcessor
  : public ObITableLoadTaskProcessor
{
public:
  CompactSSTableTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx,
                              ObTableLoadParallelTableCompactor *parallel_table_compactor,
                              ObTableLoadParallelCompactTabletCtx *tablet_ctx)
    : ObITableLoadTaskProcessor(task),
      ctx_(ctx),
      parallel_table_compactor_(parallel_table_compactor),
      tablet_ctx_(tablet_ctx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~CompactSSTableTaskProcessor() { ObTableLoadService::put_ctx(ctx_); }
  int process() override
  {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(INFO, table_compactor_time_us);
    int ret = OB_SUCCESS;
    ObDirectLoadTableHandle sstable_handle;
    if (OB_FAIL(compact_sstable(sstable_handle))) {
      LOG_WARN("fail to compact sstable", KR(ret));
    } else if (OB_FAIL(tablet_ctx_->apply_merged_sstable(sstable_handle))) {
      LOG_WARN("fail to apply merged sstable", KR(ret));
    } else if (OB_FAIL(
                 parallel_table_compactor_->handle_tablet_compact_sstable_finish(tablet_ctx_))) {
      LOG_WARN("fail to handle compact sstable finish", KR(ret));
    }
    return ret;
  }
  int compact_sstable(ObDirectLoadTableHandle &result_sstable)
  {
    int ret = OB_SUCCESS;
    ObDirectLoadMultipleSSTableCompactParam compact_param;
    compact_param.tablet_id_ = tablet_ctx_->tablet_id_;
    compact_param.table_data_desc_ = parallel_table_compactor_->table_data_desc_;
    compact_param.datum_utils_ = &ctx_->schema_.datum_utils_;
    if (OB_FAIL(compactor_.init(compact_param))) {
      LOG_WARN("fail to init sstable compactor", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ctx_->range_sstables_.count(); ++i) {
      ObDirectLoadTableHandle sstable_handle = tablet_ctx_->range_sstables_.at(i);
      if (OB_FAIL(compactor_.add_table(sstable_handle))) {
        LOG_WARN("fail to add table", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(compactor_.compact())) {
        LOG_WARN("fail to do compact", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(compactor_.get_table(result_sstable, ctx_->store_ctx_->table_mgr_))) {
        LOG_WARN("fail to get table", KR(ret));
      }
    }
    return ret;
  }

private:
  ObTableLoadTableCtx *ctx_;
  ObTableLoadParallelTableCompactor *parallel_table_compactor_;
  ObTableLoadParallelCompactTabletCtx *tablet_ctx_;
  ObDirectLoadMultipleSSTableCompactor compactor_;
};

/**
 * ParallelMergeTaskCallback
 */

class ObTableLoadParallelTableCompactor::ParallelMergeTaskCallback : public ObITableLoadTaskCallback
{
public:
  ParallelMergeTaskCallback(ObTableLoadTableCtx *ctx,
                            ObTableLoadParallelTableCompactor *parallel_table_compactor,
                            int64_t thread_idx)
    : ctx_(ctx), parallel_table_compactor_(parallel_table_compactor), thread_idx_(thread_idx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~ParallelMergeTaskCallback() { ObTableLoadService::put_ctx(ctx_); }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(parallel_table_compactor_->handle_task_finish(thread_idx_, ret_code))) {
      LOG_WARN("fail to handle task finish", KR(ret));
    }
    if (OB_FAIL(ret)) {
      ctx_->store_ctx_->set_status_error(ret);
    }
    ctx_->free_task(task);
  }

private:
  ObTableLoadTableCtx *const ctx_;
  ObTableLoadParallelTableCompactor *const parallel_table_compactor_;
  int64_t thread_idx_;
};

/**
 * ObTableLoadParallelTableCompactor
 */

ObTableLoadParallelTableCompactor::ObTableLoadParallelTableCompactor()
  : store_ctx_(nullptr),
    store_table_ctx_(nullptr),
    op_(nullptr),
    thread_count_(0),
    allocator_("TLD_ParalMerge"),
    has_error_(false),
    is_stop_(false),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  light_task_list_.set_tenant_id(MTL_ID());
  heavy_task_list_.set_tenant_id(MTL_ID());
  idle_thread_list_.set_tenant_id(MTL_ID());
}

ObTableLoadParallelTableCompactor::~ObTableLoadParallelTableCompactor()
{
  FOREACH(it, tablet_ctx_map_)
  {
    ObTableLoadParallelCompactTabletCtx *tablet_ctx = it->second;
    tablet_ctx->~ObTableLoadParallelCompactTabletCtx();
    allocator_.free(tablet_ctx);
  }
  tablet_ctx_map_.destroy();
  for (int64_t i = 0; i < light_task_list_.count(); ++i) {
    ObTableLoadTask *task = light_task_list_.at(i);
    store_ctx_->ctx_->free_task(task);
  }
  light_task_list_.reset();
  for (int64_t i = 0; i < heavy_task_list_.count(); ++i) {
    ObTableLoadTask *task = heavy_task_list_.at(i);
    store_ctx_->ctx_->free_task(task);
  }
  heavy_task_list_.reset();
}

int ObTableLoadParallelTableCompactor::init(ObTableLoadMergeCompactTableOp *op)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadParallelTableCompactor init twice", KR(ret), KP(this));
  } else if (OB_ISNULL(op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(op));
  } else {
    store_ctx_ = op->store_ctx_;
    store_table_ctx_ = op->merge_table_ctx_->store_table_ctx_;
    op_ = op;
    thread_count_ = op_->store_ctx_->thread_cnt_;
    table_data_desc_ = op->merge_table_ctx_->table_store_->get_table_data_desc();
    if (OB_FAIL(tablet_ctx_map_.create(1024, "TLD_CptCtxMap", "TLD_CptCtxMap", MTL_ID()))) {
      LOG_WARN("fail to create ctx map", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadParallelTableCompactor::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadParallelTableCompactor not init", KR(ret), KP(this));
  } else if (OB_FAIL(construct_compactors())) {
    LOG_WARN("fail to construct compactors", KR(ret));
  } else {
    FOREACH_X(it, tablet_ctx_map_, OB_SUCC(ret))
    {
      ObTableLoadParallelCompactTabletCtx *tablet_ctx = it->second;
      if (tablet_ctx->sstables_.count() > store_ctx_->merge_count_per_round_) {
        // need merge
        if (OB_FAIL(construct_split_range_task(tablet_ctx))) {
          LOG_WARN("fail to construct split range task", KR(ret));
        }
      } else {
        ATOMIC_AAF(&store_ctx_->ctx_->job_stat_->store_.compact_stage_consume_tmp_files_,
                   tablet_ctx->sstables_.count());
      }
    }
    if (OB_SUCC(ret)) {
      if (get_task_count() > 0) {
        if (OB_FAIL(start_merge())) {
          LOG_WARN("fail to start merge", KR(ret));
        }
      } else {
        // no need to merge
        if (OB_FAIL(handle_parallel_compact_success())) {
          LOG_WARN("fail to handle parallel compact success", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableLoadParallelTableCompactor::construct_compactors()
{
  int ret = OB_SUCCESS;
  ObDirectLoadTableStore *table_store = op_->merge_table_ctx_->table_store_;
  if (OB_UNLIKELY(table_store->empty() || !table_store->is_multiple_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not multiple sstable", KR(ret), KPC(table_store));
  } else {
    // 根据table_store构造tablet_ctx
    FOREACH_X(it, *table_store, OB_SUCC(ret))
    {
      const ObTabletID &tablet_id = it->first;
      ObDirectLoadTableHandleArray *table_handle_array = it->second;
      ObTableLoadParallelCompactTabletCtx *tablet_ctx = nullptr;
      if (OB_ISNULL(tablet_ctx = OB_NEWx(ObTableLoadParallelCompactTabletCtx, (&allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObTableLoadParallelCompactTabletCtx", KR(ret));
      } else if (OB_FAIL(tablet_ctx->sstables_.assign(*table_handle_array))) {
        LOG_WARN("fail to assign table handle array", KR(ret));
      } else if (FALSE_IT(tablet_ctx->tablet_id_ = tablet_id)) {
      } else if (OB_FAIL(tablet_ctx_map_.set_refactored(tablet_id, tablet_ctx))) {
        LOG_WARN("fail to set refactored", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != tablet_ctx) {
          tablet_ctx->~ObTableLoadParallelCompactTabletCtx();
          allocator_.free(tablet_ctx);
          tablet_ctx = nullptr;
        }
      }
    }
    if (OB_SUCC(ret)) {
      // 清空table_store, 以便在compact过程中能释放磁盘空间
      table_store->clear();
    }
  }
  return ret;
}

int ObTableLoadParallelTableCompactor::add_light_task(ObTableLoadTask *task)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  if (OB_FAIL(light_task_list_.push_back(task))) {
    LOG_WARN("fail to push back task", KR(ret));
  }
  return ret;
}

int ObTableLoadParallelTableCompactor::add_heavy_task(ObTableLoadTask *task)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  if (OB_FAIL(heavy_task_list_.push_back(task))) {
    LOG_WARN("fail to push back task", KR(ret));
  }
  return ret;
}

int64_t ObTableLoadParallelTableCompactor::get_task_count() const
{
  ObMutexGuard guard(mutex_);
  return light_task_list_.count() + heavy_task_list_.count();
}

int ObTableLoadParallelTableCompactor::add_idle_thread(int64_t thread_idx)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  if (OB_FAIL(idle_thread_list_.push_back(thread_idx))) {
    LOG_WARN("fail to push back thread idx", KR(ret));
  }
  return ret;
}

int64_t ObTableLoadParallelTableCompactor::get_idle_thread_count() const
{
  ObMutexGuard guard(mutex_);
  return idle_thread_list_.count();
}

void ObTableLoadParallelTableCompactor::stop() { is_stop_ = true; }

int ObTableLoadParallelTableCompactor::start_merge()
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  for (int64_t thread_idx = 0; OB_SUCC(ret) && thread_idx < thread_count_; ++thread_idx) {
    if (OB_FAIL(idle_thread_list_.push_back(thread_idx))) {
      LOG_WARN("fail to push back idle thread", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(schedule_merge_unlock())) {
      LOG_WARN("fail to schedule merge", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    has_error_ = true;
  }
  return ret;
}

int ObTableLoadParallelTableCompactor::schedule_merge_unlock()
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableLoadTask *> *task_list = nullptr;
  if (heavy_task_list_.count() < thread_count_ && !light_task_list_.empty()) {
    task_list = &light_task_list_;
  } else {
    task_list = &heavy_task_list_;
  }
  while (OB_SUCC(ret) && !task_list->empty() && !idle_thread_list_.empty()) {
    ObTableLoadTask *task = nullptr;
    int64_t thread_idx = -1;
    if (OB_FAIL(task_list->pop_back(task))) {
      LOG_WARN("fail to pop back task", KR(ret));
    } else if (OB_FAIL(idle_thread_list_.pop_back(thread_idx))) {
      LOG_WARN("fail to pop back thread idx", KR(ret));
    }
    // 设置task的callback
    else if (OB_FAIL(
               task->set_callback<ParallelMergeTaskCallback>(store_ctx_->ctx_, this, thread_idx))) {
      LOG_WARN("fail to set task callback", KR(ret));
    }
    // 把task放入调度器
    else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(thread_idx, task))) {
      LOG_WARN("fail to add task", KR(ret), K(thread_idx), KPC(task));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != task) {
        store_ctx_->ctx_->free_task(task);
      }
    }
  }
  return ret;
}

int ObTableLoadParallelTableCompactor::construct_split_range_task(
  ObTableLoadParallelCompactTabletCtx *tablet_ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadTask *task = nullptr;
  // 1. 分配task
  if (OB_FAIL(store_ctx_->ctx_->alloc_task(task))) {
    LOG_WARN("fail to alloc task", KR(ret));
  }
  // 2. 设置processor
  else if (OB_FAIL(
             task->set_processor<SplitRangeTaskProcessor>(store_ctx_->ctx_, this, tablet_ctx))) {
    LOG_WARN("fail to set split range task processor", KR(ret));
  }
  // 3. 添加到任务队列
  else if (OB_FAIL(add_light_task(task))) {
    LOG_WARN("fail to add light task", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != task) {
      store_ctx_->ctx_->free_task(task);
    }
  }
  return ret;
}

int ObTableLoadParallelTableCompactor::construct_merge_range_task(
  ObTableLoadParallelCompactTabletCtx *tablet_ctx, int64_t range_idx)
{
  int ret = OB_SUCCESS;
  ObTableLoadTask *task = nullptr;
  // 1. 分配task
  if (OB_FAIL(store_ctx_->ctx_->alloc_task(task))) {
    LOG_WARN("fail to alloc task", KR(ret));
  }
  // 2. 设置processor
  else if (OB_FAIL(task->set_processor<MergeRangeTaskProcessor>(store_ctx_->ctx_, this, tablet_ctx,
                                                                range_idx))) {
    LOG_WARN("fail to set merge range task processor", KR(ret));
  }
  // 3. 添加到任务队列
  else if (OB_FAIL(add_heavy_task(task))) {
    LOG_WARN("fail to add heavy task", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != task) {
      store_ctx_->ctx_->free_task(task);
    }
  }
  return ret;
}

int ObTableLoadParallelTableCompactor::construct_compact_sstable_task(
  ObTableLoadParallelCompactTabletCtx *tablet_ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadTask *task = nullptr;
  // 1. 分配task
  if (OB_FAIL(store_ctx_->ctx_->alloc_task(task))) {
    LOG_WARN("fail to alloc task", KR(ret));
  }
  // 2. 设置processor
  else if (OB_FAIL(task->set_processor<CompactSSTableTaskProcessor>(store_ctx_->ctx_, this,
                                                                    tablet_ctx))) {
    LOG_WARN("fail to set compact sstable task processor", KR(ret));
  }
  // 3. 添加到任务队列
  else if (OB_FAIL(add_light_task(task))) {
    LOG_WARN("fail to add light task", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != task) {
      store_ctx_->ctx_->free_task(task);
    }
  }
  return ret;
}

int ObTableLoadParallelTableCompactor::handle_tablet_split_range_finish(
  ObTableLoadParallelCompactTabletCtx *tablet_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == tablet_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(tablet_ctx));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ctx->range_count_; ++i) {
      if (OB_FAIL(construct_merge_range_task(tablet_ctx, i))) {
        LOG_WARN("fail to construct merge range task", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadParallelTableCompactor::handle_tablet_range_merge_finish(
  ObTableLoadParallelCompactTabletCtx *tablet_ctx)
{
  LOG_INFO("parallel merge all merge finish");
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == tablet_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(tablet_ctx));
  } else if (OB_FAIL(construct_compact_sstable_task(tablet_ctx))) {
    LOG_WARN("fail to construct compact sstable task", KR(ret));
  }
  return ret;
}

int ObTableLoadParallelTableCompactor::handle_tablet_compact_sstable_finish(
  ObTableLoadParallelCompactTabletCtx *tablet_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == tablet_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(tablet_ctx));
  } else if (tablet_ctx->sstables_.count() > store_ctx_->merge_count_per_round_) {
    // still need merge
    ATOMIC_AAF(&store_ctx_->ctx_->job_stat_->store_.compact_stage_consume_tmp_files_,
               tablet_ctx->merge_sstable_count_ - 1);
    if (OB_FAIL(construct_split_range_task(tablet_ctx))) {
      LOG_WARN("fail to construct split range task", KR(ret));
    }
  } else {
    ATOMIC_AAF(&store_ctx_->ctx_->job_stat_->store_.compact_stage_consume_tmp_files_,
               tablet_ctx->merge_sstable_count_);
  }
  return ret;
}

int ObTableLoadParallelTableCompactor::handle_task_finish(int64_t thread_idx, int ret_code)
{
  int ret = OB_SUCCESS;
  bool is_merge_completed = false;
  {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(ret_code)) {
      has_error_ = true;
    }
    if (OB_UNLIKELY(is_stop_ || has_error_)) {
    } else {
      if (OB_FAIL(idle_thread_list_.push_back(thread_idx))) {
        LOG_WARN("fail to push back idle thread", KR(ret));
      } else if (OB_FAIL(schedule_merge_unlock())) {
        LOG_WARN("fail to schedule merge", KR(ret));
      } else {
        is_merge_completed = (idle_thread_list_.count() == thread_count_);
      }
      if (OB_FAIL(ret)) {
        has_error_ = true;
      }
    }
  }
  if (is_merge_completed) {
    if (OB_FAIL(handle_parallel_compact_success())) {
      LOG_WARN("fail to handle parallel compact success", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadParallelTableCompactor::handle_parallel_compact_success()
{
  int ret = OB_SUCCESS;
  LOG_INFO("LOAD PARALLEL COMPACT TABLE COMPLETED");
  ObDirectLoadTableStore *table_store = op_->merge_table_ctx_->table_store_;
  table_store->clear();
  table_store->set_table_data_desc(table_data_desc_);
  table_store->set_multiple_sstable();
  // get tables from tablet ctx
  FOREACH_X(it, tablet_ctx_map_, OB_SUCC(ret))
  {
    const ObTabletID &tablet_id = it->first;
    ObTableLoadParallelCompactTabletCtx *tablet_ctx = it->second;
    if (OB_FAIL(table_store->add_tablet_tables(tablet_id, tablet_ctx->sstables_))) {
      LOG_WARN("fail to add tables", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(op_->on_success())) {
      LOG_WARN("fail to handle success", KR(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
