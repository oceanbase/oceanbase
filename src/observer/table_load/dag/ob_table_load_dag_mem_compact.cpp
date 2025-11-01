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

#include "observer/table_load/dag/ob_table_load_dag_mem_compact.h"
#include "observer/table_load/dag/ob_table_load_dag.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"
#include "storage/direct_load/ob_direct_load_external_merger.h"
#include "storage/direct_load/ob_direct_load_mem_context.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_builder.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_compactor.h"
#include "storage/direct_load/ob_direct_load_table_store.h"

namespace oceanbase
{
namespace observer
{
using namespace share;
using namespace storage;
using namespace table;

/**
 * ObTableLoadMemCompactCtx
 */

ObTableLoadMemCompactCtx::ObTableLoadMemCompactCtx()
  : store_ctx_(nullptr),
    column_descs_(nullptr),
    datum_utils_(nullptr),
    dml_row_handler_(nullptr),
    max_round_ctx_cnt_(0),
    compact_chunk_cnt_(0),
    range_cnt_(0),
    round_ctx_cnt_(0),
    is_inited_(false)
{
  enc_params_.set_tenant_id(MTL_ID());
}

ObTableLoadMemCompactCtx::~ObTableLoadMemCompactCtx() {}

void ObTableLoadMemCompactCtx::reset()
{
  is_inited_ = false;
  store_ctx_ = nullptr;
  table_data_desc_.reset();
  column_descs_ = nullptr;
  datum_utils_ = nullptr;
  enc_params_.reset();
  dml_row_handler_ = nullptr;
  max_round_ctx_cnt_ = 0;
  compact_chunk_cnt_ = 0;
  range_cnt_ = 0;
  round_ctx_cnt_ = 0;
  result_tables_handle_.reset();
}

int ObTableLoadMemCompactCtx::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadMemCompactCtx init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == store_ctx_ || !table_data_desc_.is_valid() ||
                         nullptr == column_descs_ || nullptr == datum_utils_ ||
                         nullptr == dml_row_handler_ || max_round_ctx_cnt_ <= 0 ||
                         compact_chunk_cnt_ <= 0 || range_cnt_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(store_ctx_), K(table_data_desc_), KP(column_descs_),
             KP(datum_utils_), KP(dml_row_handler_), K(max_round_ctx_cnt_), K(compact_chunk_cnt_),
             K(range_cnt_));
  } else if (OB_FAIL(chunk_allocator_.init("TLD_MCChunk", MTL_ID()))) {
    LOG_WARN("fail to init chunk allocator", KR(ret));
  } else if (OB_FAIL(ObDirectLoadMemContext::init_enc_params(
               *column_descs_, table_data_desc_.rowkey_column_num_,
               store_ctx_->ctx_->param_.dup_action_, enc_params_))) {
    LOG_WARN("fail to init enc params", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadMemCompactCtx::acquire_chunk(ChunkType *&chunk)
{
  int ret = OB_SUCCESS;
  chunk = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPKMemSorter not init", KR(ret), KP(this));
  } else {
    int64_t sort_memory = 0;
    if (ObTableLoadExeMode::MAX_TYPE == store_ctx_->ctx_->param_.exe_mode_) {
      sort_memory = store_ctx_->mem_chunk_size_;
    } else if (OB_FAIL(ObTableLoadService::get_sort_memory(sort_memory))) {
      LOG_WARN("fail to get sort memory", KR(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(chunk = chunk_allocator_.alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc chunk", KR(ret));
      } else if (OB_FAIL(chunk->init(MTL_ID(), sort_memory))) {
        LOG_WARN("fail to init chunk", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
      if (nullptr != chunk) {
        chunk_allocator_.free(chunk);
        chunk = nullptr;
      }
    }
  }
  return ret;
}

void ObTableLoadMemCompactCtx::release_chunk(ChunkType *chunk)
{
  if (OB_NOT_NULL(chunk)) {
    chunk_allocator_.free(chunk);
    chunk = nullptr;
  }
}

int ObTableLoadMemCompactCtx::make_round_ctx(ObTableLoadMemCompactRoundCtxHandle &round_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMemCompactCtx not init", KR(ret));
  } else {
    int64_t start = ATOMIC_LOAD(&round_ctx_cnt_);
    int64_t end = 0;
    int64_t old_start = start;
    while ((end = start + 1) <= max_round_ctx_cnt_ &&
           old_start != (start = ATOMIC_CAS(&round_ctx_cnt_, old_start, end))) {
      old_start = start;
      PAUSE();
    }
    if (OB_UNLIKELY(end > max_round_ctx_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected round ctx full", KR(ret));
    } else {
      if (OB_FAIL(ObTableLoadMemCompactRoundCtxHandle::make_handle(round_ctx, this))) {
        LOG_WARN("fail to make round ctx handle", KR(ret));
      } else if (OB_FAIL(round_ctx->init())) {
        LOG_WARN("fail to init ctx", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadMemCompactCtx::add_result_table(const ObDirectLoadTableHandle &table_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMemCompactCtx not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!table_handle.is_valid() ||
                         !table_handle.get_table()->is_multiple_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_handle));
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(result_tables_handle_.add(table_handle))) {
      LOG_WARN("fail to add table", KR(ret));
    }
  }
  return ret;
}

/**
 * ObTableLoadMemCompactRoundCtx
 */

ObTableLoadMemCompactRoundCtx::ObTableLoadMemCompactRoundCtx(ObTableLoadMemCompactCtx *ctx)
  : ctx_(ctx), is_inited_(false)
{
  chunks_.set_tenant_id(MTL_ID());
  ranges_.set_tenant_id(MTL_ID());
  table_handles_.set_tenant_id(MTL_ID());
}

ObTableLoadMemCompactRoundCtx::~ObTableLoadMemCompactRoundCtx()
{
  for (int64_t i = 0; i < chunks_.count(); ++i) {
    ChunkType *chunk = chunks_.at(i);
    ctx_->release_chunk(chunk);
  }
  chunks_.reset();
  ctx_->dec_round_ctx_cnt();
}

int ObTableLoadMemCompactRoundCtx::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadMemCompactRoundCtx init twice", KR(ret), KP(this));
  } else if (OB_FAIL(table_handles_.prepare_allocate(ctx_->range_cnt_))) {
    LOG_WARN("fail to prepare allocate", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadMemCompactRoundCtx::add_chunk(ChunkType *chunk)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMemCompactRoundCtx not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == chunk)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(chunk));
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(chunks_.push_back(chunk))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadMemCompactRoundCtx::add_range(const RangeType &range)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMemCompactRoundCtx not init", KR(ret), KP(this));
  } else {
    // 只有一个sampe_task会添加range, 可以不加锁
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(ranges_.push_back(range))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadMemCompactRoundCtx::add_table(const int64_t range_idx,
                                             const ObDirectLoadTableHandle &table_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMemCompactRoundCtx not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(range_idx >= table_handles_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(range_idx), K(table_handles_.count()));
  } else if (!table_handle.is_valid()) {
    // 一个range不包含任何数据
  } else if (OB_UNLIKELY(!table_handle.get_table()->is_multiple_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table", KR(ret), K(table_handle));
  } else {
    // 指定下标访问, 可以不加锁
    ObMutexGuard guard(mutex_);
    if (OB_UNLIKELY(table_handles_.at(range_idx).is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected add table twice", KR(ret), K(range_idx));
    } else {
      table_handles_.at(range_idx) = table_handle;
    }
  }
  return ret;
}

/**
 * ObTableLoadMemCompactTaskBase
 */

ObTableLoadMemCompactTaskBase::ObTableLoadMemCompactTaskBase(ObTableLoadDag *dag,
                                                             ObTableLoadMemCompactCtx *ctx)
  : ObTableLoadDagTaskBase(dag), ctx_(ctx)
{
}

ObTableLoadMemCompactTaskBase::ObTableLoadMemCompactTaskBase(ObTableLoadMemCompactTaskBase *parent)
  : ObTableLoadDagTaskBase(parent->dag_), ctx_(parent->ctx_), round_ctx_(parent->round_ctx_)
{
}

/**
 * ObTableLoadMemCompactSampleTask
 */

ObTableLoadMemCompactSampleTask::ObTableLoadMemCompactSampleTask(
  ObTableLoadMemCompactTaskBase *parent)
  : ObITask(TASK_TYPE_DIRECT_LOAD_MEM_COMPACT_SAMPLE), ObTableLoadMemCompactTaskBase(parent)
{
}

int ObTableLoadMemCompactSampleTask::process()
{
  int ret = OB_SUCCESS;
  if (round_ctx_->empty()) {
    // do nothing
  } else {
    const ObIArray<ChunkType *> &chunks = round_ctx_->get_chunks();

    // 随机采样
    ObArray<ConstRowType *> sample_rows;
    sample_rows.set_tenant_id(MTL_ID());
    int64_t row_cnt = 0;
    for (int64_t i = 0; i < chunks.count(); ++i) {
      row_cnt += chunks.at(i)->get_size();
    }
    const int64_t sample_cnt = MIN(row_cnt, DEFAULT_SAMPLE_TIMES);
    for (int64_t i = 0; OB_SUCC(ret) && i < sample_cnt; ++i) {
      const int chunk_idx = ObRandom::rand(0, chunks.count() - 1);
      ChunkType *chunk = chunks.at(chunk_idx);
      const int row_idx = ObRandom::rand(0, chunk->get_size() - 1);
      ConstRowType *row = chunk->get_item(row_idx);
      if (OB_FAIL(sample_rows.push_back(row))) {
        LOG_WARN("fail to push row", KR(ret));
      }
    }

    // 对采样行进行排序
    if (OB_SUCC(ret)) {
      CompareType compare;
      if (OB_FAIL(compare.init(*ctx_->datum_utils_, store_ctx_->ctx_->param_.dup_action_))) {
        LOG_WARN("fail to init compare", KR(ret));
      } else {
        lib::ob_sort(sample_rows.begin(), sample_rows.end(), compare);
      }
    }

    // 根据采样行生成range
    const int64_t range_cnt = ctx_->range_cnt_;
    const int64_t step = sample_rows.count() / range_cnt;
    ConstRowType *last_row = nullptr;
    for (int64_t i = 1; OB_SUCC(ret) && i <= range_cnt; ++i) {
      if (i != range_cnt) {
        ConstRowType *row = sample_rows.at(i * step);
        if (OB_FAIL(round_ctx_->add_range(RangeType(last_row, row)))) {
          LOG_WARN("fail to add range", KR(ret));
        } else {
          last_row = row;
        }
      } else {
        if (OB_FAIL(round_ctx_->add_range(RangeType(last_row, nullptr)))) {
          LOG_WARN("fail to add range", KR(ret));
        } else {
          last_row = nullptr;
        }
      }
    }
  }
  return ret;
}

/**
 * ObTableLoadMemCompactDumpTask
 */

ObTableLoadMemCompactDumpTask::ObTableLoadMemCompactDumpTask(ObTableLoadMemCompactTaskBase *parent,
                                                             const int64_t range_idx)
  : ObITask(TASK_TYPE_DIRECT_LOAD_MEM_COMPACT_DUMP),
    ObTableLoadMemCompactTaskBase(parent),
    range_idx_(range_idx)
{
}

int ObTableLoadMemCompactDumpTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  const int64_t next_range_idx = range_idx_ + 1;
  if (next_range_idx >= round_ctx_->get_ranges().count()) {
    ret = OB_ITER_END;
  } else {
    ObTableLoadMemCompactDumpTask *task = nullptr;
    if (OB_FAIL(dag_->alloc_task(task, this, next_range_idx))) {
      LOG_WARN("failed to alloc task", KR(ret));
    } else {
      next_task = task;
    }
  }
  return ret;
}

int ObTableLoadMemCompactDumpTask::process()
{
  typedef ObDirectLoadMemChunkIter<ConstRowType, CompareType> ChunkIteratorType;
  typedef ObDirectLoadExternalIterator<ConstRowType> IteratorType;
  typedef ObDirectLoadExternalMerger<ConstRowType, CompareType> MergerType;

  int ret = OB_SUCCESS;
  if (round_ctx_->empty()) {
    // do nothing
  } else {
    const ObIArray<ChunkType *> &chunks = round_ctx_->get_chunks();
    const RangeType &range = round_ctx_->get_ranges().at(range_idx_);

    const ConstRowType *row = nullptr;
    ObDirectLoadDatumRow datum_row;

    if (OB_FAIL(datum_row.init(ctx_->table_data_desc_.column_count_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    }

    // init merger
    MergerType merger;
    ObArray<ChunkIteratorType> chunk_iters; // 用于暂存iters
    ObArray<IteratorType *> iters;
    CompareType compare;
    CompareType compare1; // 不带上seq_no的排序
    if (OB_SUCC(ret)) {
      if (OB_FAIL(compare.init(*ctx_->datum_utils_, store_ctx_->ctx_->param_.dup_action_))) {
        LOG_WARN("fail to init compare", KR(ret));
      } else if (OB_FAIL(compare1.init(*ctx_->datum_utils_, store_ctx_->ctx_->param_.dup_action_,
                                       true))) {
        LOG_WARN("fail to init compare1", KR(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < chunks.count(); ++i) {
        ChunkType *chunk = chunks.at(i);
        ChunkIteratorType iter = chunk->scan(range.start_, range.end_, compare1);
        if (OB_FAIL(chunk_iters.push_back(iter))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < chunk_iters.size(); ++i) {
        ChunkIteratorType &chunk_iter = chunk_iters.at(i);
        if (OB_FAIL(iters.push_back(&chunk_iter))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(merger.init(iters, &compare))) {
          LOG_WARN("fail to init merger", KR(ret));
        }
      }
    }

    // init builder
    ObDirectLoadMultipleSSTableBuilder sstable_builder;
    ObDirectLoadMultipleSSTableBuildParam sstable_build_param;
    sstable_build_param.table_data_desc_ = ctx_->table_data_desc_;
    sstable_build_param.file_mgr_ = store_ctx_->tmp_file_mgr_;
    sstable_build_param.datum_utils_ = ctx_->datum_utils_;
    sstable_build_param.extra_buf_ = reinterpret_cast<char *>(1); // unuse, delete in future
    sstable_build_param.extra_buf_size_ = 4096;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sstable_builder.init(sstable_build_param))) {
        LOG_WARN("fail to init sstable builder", KR(ret));
      }
    }

    while (OB_SUCC(ret)) {
      if (OB_FAIL(dag_->check_status())) {
        LOG_WARN("fail to check status", KR(ret));
      } else if (OB_FAIL(merger.get_next_item(row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row");
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(row->to_datum_row(datum_row))) {
        LOG_WARN("fail to transfer dataum row", KR(ret));
      } else if (OB_FAIL(sstable_builder.append_row(row->tablet_id_, datum_row))) {
        if (OB_LIKELY(OB_ERR_PRIMARY_KEY_DUPLICATE == ret)) {
          if (OB_FAIL(ctx_->dml_row_handler_->handle_update_row(row->tablet_id_, datum_row))) {
            LOG_WARN("fail to handle update row", KR(ret), K(datum_row));
          }
        } else {
          LOG_WARN("fail to append row", KR(ret), K(datum_row));
        }
      } else {
        ATOMIC_AAF(&store_ctx_->ctx_->job_stat_->store_.compact_stage_dump_rows_, 1);
      }
    }

    ObDirectLoadTableHandle table_handle;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sstable_builder.close())) {
        LOG_WARN("fail to close sstable builder", KR(ret));
      } else if (sstable_builder.get_row_count() > 0) {
        ObDirectLoadTableHandleArray table_handle_array;
        if (OB_FAIL(sstable_builder.get_tables(table_handle_array, store_ctx_->table_mgr_))) {
          LOG_WARN("fail to get tables", KR(ret));
        } else if (OB_UNLIKELY(1 != table_handle_array.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected table count not equal 1", KR(ret));
        } else {
          table_handle = table_handle_array.at(0);
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(round_ctx_->add_table(range_idx_, table_handle))) {
        LOG_WARN("fail to add table", KR(ret));
      }
    }
  }
  return ret;
}

/**
 * ObTableLoadMemCompactCompactTask
 */

ObTableLoadMemCompactCompactTask::ObTableLoadMemCompactCompactTask(
  ObTableLoadMemCompactTaskBase *parent)
  : ObITask(TASK_TYPE_DIRECT_LOAD_MEM_COMPACT_COMPACT), ObTableLoadMemCompactTaskBase(parent)
{
}

int ObTableLoadMemCompactCompactTask::process()
{
  int ret = OB_SUCCESS;
  if (round_ctx_->empty()) {
    // do nothing
  } else {
    const ObIArray<ObDirectLoadTableHandle> &table_handles = round_ctx_->get_table_handles();
    ObDirectLoadMultipleSSTableCompactor sstable_compactor;
    ObDirectLoadMultipleSSTableCompactParam param;
    param.table_data_desc_ = ctx_->table_data_desc_;
    param.datum_utils_ = ctx_->datum_utils_;
    if (OB_FAIL(sstable_compactor.init(param))) {
      LOG_WARN("fail to init compactor", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_handles.count(); ++i) {
      const ObDirectLoadTableHandle &table_handle = table_handles.at(i);
      if (!table_handle.is_valid()) {
      } else if (OB_FAIL(sstable_compactor.add_table(table_handle))) {
        LOG_WARN("fail to add table", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObDirectLoadTableHandle result_table_handle;
      if (OB_FAIL(sstable_compactor.compact())) {
        LOG_WARN("fail to compact sstables", KR(ret));
      } else if (OB_FAIL(
                   sstable_compactor.get_table(result_table_handle, store_ctx_->table_mgr_))) {
        LOG_WARN("fail to get table", KR(ret));
      } else if (OB_FAIL(ctx_->add_result_table(result_table_handle))) {
        LOG_WARN("fail to add result table", KR(ret));
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
