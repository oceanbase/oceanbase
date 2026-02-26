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

#include "observer/table_load/dag/ob_table_load_dag_pk_mem_sort.h"
#include "observer/table_load/dag/ob_table_load_dag.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/plan/ob_table_load_merge_op.h"
#include "storage/direct_load/ob_direct_load_external_table.h"
#include "storage/direct_load/ob_direct_load_table_store.h"

namespace oceanbase
{
namespace observer
{
using namespace share;
using namespace storage;
using namespace table;

/**
 * ObTableLoadPKMemSorter
 */

ObTableLoadPKMemSorter::ObTableLoadPKMemSorter()
  : store_ctx_(nullptr), dag_(nullptr), op_(nullptr), is_closed_(false), is_inited_(false)
{
}

ObTableLoadPKMemSorter::~ObTableLoadPKMemSorter()
{
  ObTableLoadPKMemSortLoader *loader = nullptr;
  // release loader in idle_loader_list_
  DLIST_FOREACH_REMOVESAFE_NORET(loader, idle_loader_list_)
  {
    idle_loader_list_.remove(loader);
    loader_allocator_.free(loader);
  }
  // used_loader_list_预期为空
  // release loader in used_loader_list_
  DLIST_FOREACH_REMOVESAFE_NORET(loader, used_loader_list_)
  {
    used_loader_list_.remove(loader);
    loader_allocator_.free(loader);
  }
}

int ObTableLoadPKMemSorter::init(ObTableLoadDag *dag, ObTableLoadMemSortOp *op)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadPKMemSorter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == dag || nullptr == op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(dag), KP(op));
  } else {
    store_ctx_ = dag->store_ctx_;
    dag_ = dag;
    op_ = op;
    // init mem_compact_ctx_
    mem_compact_ctx_.store_ctx_ = store_ctx_;
    mem_compact_ctx_.table_data_desc_ = op->op_ctx_->table_store_.get_table_data_desc();
    mem_compact_ctx_.column_descs_ = &op->op_ctx_->store_table_ctx_->schema_->column_descs_;
    mem_compact_ctx_.datum_utils_ = &op->op_ctx_->store_table_ctx_->schema_->datum_utils_;
    mem_compact_ctx_.dml_row_handler_ = op->op_ctx_->dml_row_handler_;
    mem_compact_ctx_.max_round_ctx_cnt_ = 2;
    mem_compact_ctx_.compact_chunk_cnt_ = MAX(store_ctx_->max_mem_chunk_count_ / 2, 1);
    mem_compact_ctx_.range_cnt_ = store_ctx_->thread_cnt_;
    if (OB_FAIL(mem_compact_ctx_.init())) {
      LOG_WARN("fail to init mem compact ctx", KR(ret));
    }
    // init loaders
    else if (OB_FAIL(loader_allocator_.init("TLD_MSLoader", MTL_ID()))) {
      LOG_WARN("fail to init loader allocator", KR(ret));
    } else if (OB_FAIL(construct_loaders())) {
      LOG_WARN("fail to construct loaders", KR(ret));
    } else {
      // 清空table_store, 以便在排序过程中能释放磁盘空间
      op_->op_ctx_->table_store_.clear();
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadPKMemSorter::construct_loaders()
{
  int ret = OB_SUCCESS;
  ObDirectLoadTableStore &table_store = op_->op_ctx_->table_store_;
  FOREACH_X(it, table_store, OB_SUCC(ret))
  {
    ObDirectLoadTableHandleArray *tables_handle = it->second;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle->count(); ++i) {
      const ObDirectLoadTableHandle &table_handle = tables_handle->at(i);
      const ObDirectLoadExternalFragmentArray &fragments =
        static_cast<ObDirectLoadExternalTable *>(table_handle.get_table())->get_fragments();
      for (int64_t j = 0; OB_SUCC(ret) && j < fragments.count(); ++j) {
        const ObDirectLoadExternalFragment &fragment = fragments.at(j);
        ObTableLoadPKMemSortLoader *loader = nullptr;
        if (OB_ISNULL(loader = loader_allocator_.alloc())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc loader", KR(ret));
        } else if (OB_FAIL(loader->init(this, fragment))) {
          LOG_WARN("fail to init loader", KR(ret));
        } else {
          // 加入idle_loader_list_
          abort_unless(idle_loader_list_.add_last(loader));
        }
        if (OB_FAIL(ret)) {
          if (nullptr != loader) {
            loader_allocator_.free(loader);
            loader = nullptr;
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLoadPKMemSorter::pop_loader(ObTableLoadPKMemSortLoader *&loader)
{
  int ret = OB_SUCCESS;
  loader = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPKMemSorter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is closed", KR(ret));
  } else {
    ObMutexGuard guard(mutex_);
    if (!idle_loader_list_.is_empty()) {
      // 取出第一个空闲的loader, 从idle_loader_list_移除并加入used_loader_list_
      loader = idle_loader_list_.get_first();
      abort_unless(OB_NOT_NULL(idle_loader_list_.remove(loader)));
      abort_unless(used_loader_list_.add_last(loader));
    } else if (!used_loader_list_.is_empty()) {
      ret = OB_EAGAIN;
    } else {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObTableLoadPKMemSorter::push_loader(ObTableLoadPKMemSortLoader *loader)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPKMemSorter not init", KR(ret), KP(this));
  } else if (OB_ISNULL(loader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(loader));
  } else {
    ObMutexGuard guard(mutex_);
    // 先从used_loader_list_移除
    abort_unless(OB_NOT_NULL(used_loader_list_.remove(loader)));
    if (!loader->is_iter_end()) {
      // loader中的数据没读完, 放回空闲队列开头
      abort_unless(idle_loader_list_.add_first(loader));
      loader = nullptr;
    }
  }
  if (nullptr != loader) {
    loader_allocator_.free(loader);
    loader = nullptr;
  }
  return ret;
}

bool ObTableLoadPKMemSorter::is_finish() const
{
  ObMutexGuard guard(mutex_);
  return idle_loader_list_.is_empty() && used_loader_list_.is_empty();
}

int ObTableLoadPKMemSorter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPKMemSorter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is closed", KR(ret));
  } else {
    ObDirectLoadTableStore &table_store = op_->op_ctx_->table_store_;
    table_store.clear();
    table_store.set_multiple_sstable();
    if (OB_FAIL(table_store.add_tables(mem_compact_ctx_.get_result_tables_handle()))) {
      LOG_WARN("fail to add tables", KR(ret));
    } else {
      mem_compact_ctx_.reset();
      is_closed_ = true;
    }
  }
  return ret;
}

/**
 * ObTableLoadPKMemSortLoader
 */

ObTableLoadPKMemSortLoader::ObTableLoadPKMemSortLoader()
  : mem_sorter_(nullptr), row_(nullptr), is_iter_end_(false), is_inited_(false)
{
}

int ObTableLoadPKMemSortLoader::init(ObTableLoadPKMemSorter *mem_sorter,
                                     const ObDirectLoadExternalFragment &fragment)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadPKMemSortLoader init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == mem_sorter || !fragment.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(mem_sorter), K(fragment));
  } else if (OB_FAIL(fragment_.assign(fragment))) {
    LOG_WARN("fail to assign fragment", KR(ret));
  } else {
    mem_sorter_ = mem_sorter;
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadPKMemSortLoader::process(ChunkType &chunk)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPKMemSortLoader not init", KR(ret), KP(this));
  } else if (is_iter_end_) {
    ret = OB_ITER_END;
  } else {
    ObTableLoadDag *dag = mem_sorter_->dag_;
    ConstRowType const_row;
    // init reader
    if (!reader_.is_opened()) {
      const ObDirectLoadTableDataDesc &table_data_desc =
        mem_sorter_->mem_compact_ctx_.table_data_desc_;
      if (OB_FAIL(reader_.init(table_data_desc.external_data_block_size_,
                               table_data_desc.compressor_type_))) {
        LOG_WARN("fail to init reader", KR(ret));
      } else if (OB_FAIL(reader_.open(fragment_.file_handle_, 0, fragment_.file_size_))) {
        LOG_WARN("fail to open file", KR(ret));
      }
    }
    // read rows
    while (OB_SUCC(ret)) {
      if (OB_FAIL(dag->check_status())) {
        LOG_WARN("fail to check status", KR(ret));
      } else if (nullptr == row_ && OB_FAIL(reader_.get_next_item(row_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next item", KR(ret));
        } else {
          is_iter_end_ = true;
          break;
        }
      } else {
        const_row = *row_;
        if (OB_FAIL(chunk.add_item(const_row))) {
          if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
            LOG_WARN("fail to add item", KR(ret));
          }
        } else {
          row_ = nullptr;
        }
      }
    }
  }
  return ret;
}

/**
 * ObTableLoadPKMemSortTaskBase
 */

ObTableLoadPKMemSortTaskBase::ObTableLoadPKMemSortTaskBase(ObTableLoadDag *dag,
                                                           ObTableLoadPKMemSorter *mem_sorter)
  : ObTableLoadMemCompactTaskBase(dag, &mem_sorter->mem_compact_ctx_), mem_sorter_(mem_sorter)
{
}

ObTableLoadPKMemSortTaskBase::ObTableLoadPKMemSortTaskBase(ObTableLoadPKMemSortTaskBase *parent)
  : ObTableLoadMemCompactTaskBase(parent), mem_sorter_(parent->mem_sorter_)
{
}

/**
 * ObTableLoadPKMemSortLoadTask
 */

ObTableLoadPKMemSortLoadTask::ObTableLoadPKMemSortLoadTask(ObTableLoadPKMemSortTaskBase *parent,
                                                           const int64_t chunk_idx)
  : ObITask(TASK_TYPE_DIRECT_LOAD_PK_MEM_SORT_LOAD),
    ObTableLoadPKMemSortTaskBase(parent),
    chunk_idx_(chunk_idx),
    chunk_(nullptr),
    is_first_generation_(true)
{
}

ObTableLoadPKMemSortLoadTask::ObTableLoadPKMemSortLoadTask(ObTableLoadPKMemSortLoadTask *parent)
  : ObITask(TASK_TYPE_DIRECT_LOAD_PK_MEM_SORT_LOAD),
    ObTableLoadPKMemSortTaskBase(parent),
    chunk_idx_(parent->chunk_idx_),
    chunk_(parent->chunk_),
    is_first_generation_(false)
{
}

ObTableLoadPKMemSortLoadTask::~ObTableLoadPKMemSortLoadTask()
{
  if (nullptr != chunk_) {
    ctx_->release_chunk(chunk_);
    chunk_ = nullptr;
  }
}

int ObTableLoadPKMemSortLoadTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  const int64_t next_chunk_idx = chunk_idx_ + 1;
  if (!is_first_generation_) {
    ret = OB_ITER_END;
  } else if (next_chunk_idx >= ctx_->compact_chunk_cnt_) {
    ret = OB_ITER_END;
  } else {
    ObTableLoadPKMemSortLoadTask *task = nullptr;
    if (OB_FAIL(dag_->alloc_task(task, this, next_chunk_idx))) {
      LOG_WARN("failed to alloc task", KR(ret));
    } else {
      next_task = task;
    }
  }
  return ret;
}

int ObTableLoadPKMemSortLoadTask::process()
{
  int ret = OB_SUCCESS;
  bool is_finish = false;
  if (nullptr == chunk_ && OB_FAIL(ctx_->acquire_chunk(chunk_))) {
    LOG_WARN("fail to acquire chunk", KR(ret));
  }
  while (OB_SUCC(ret) && !is_finish) {
    ObTableLoadPKMemSortLoader *loader = nullptr;
    if (OB_FAIL(dag_->check_status())) {
      LOG_WARN("fail to check status", KR(ret));
    } else if (OB_FAIL(mem_sorter_->pop_loader(loader))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        // 没有数据可以读了, 结束
        ret = OB_SUCCESS;
        is_finish = true;
        break;
      } else if (OB_LIKELY(OB_EAGAIN == ret)) {
        // 还有数据可以读, 但是loader都被其他load_task占用了
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to pop loader", KR(ret));
      }
    } else if (OB_FAIL(loader->process(*chunk_))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        // 当前loader中的数据读完了, 切到下一个loader继续读
        ret = OB_SUCCESS;
      } else if (OB_LIKELY(OB_BUF_NOT_ENOUGH == ret)) {
        // chunk写满了, 结束
        ret = OB_SUCCESS;
        is_finish = true;
      } else {
        LOG_WARN("fail to do load", KR(ret));
      }
    }
    if (nullptr != loader) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(mem_sorter_->push_loader(loader))) {
        LOG_WARN("fail to push loader", KR(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      } else {
        loader = nullptr;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!is_finish) {
      ret = OB_DAG_TASK_IS_SUSPENDED;
    } else if (chunk_->get_size() > 0) {
      // 将chunk加入round_ctx
      CompareType compare;
      if (OB_FAIL(compare.init(*ctx_->datum_utils_, store_ctx_->ctx_->param_.dup_action_))) {
        LOG_WARN("fail to init compare", KR(ret));
      } else if (OB_FAIL(chunk_->sort(compare, ctx_->enc_params_))) {
        LOG_WARN("fail to sort chunk", KR(ret));
      } else if (OB_FAIL(round_ctx_->add_chunk(chunk_))) {
        LOG_WARN("fail to add chunk", KR(ret));
      } else {
        chunk_ = nullptr;
      }
    } else {
      // 没有数据, 直接释放chunk
      ctx_->release_chunk(chunk_);
      chunk_ = nullptr;
    }
  }
  if (OB_SUCC(ret) && !is_finish) {
    ret = OB_DAG_TASK_IS_SUSPENDED;
  }
  return ret;
}

/**
 * ObTableLoadPKMemSortTask
 */

ObTableLoadPKMemSortTask::ObTableLoadPKMemSortTask(ObTableLoadDag *dag,
                                                   ObTableLoadPKMemSorter *mem_sorter)
  : ObITask(TASK_TYPE_DIRECT_LOAD_PK_MEM_SORT), ObTableLoadPKMemSortTaskBase(dag, mem_sorter)
{
}

ObTableLoadPKMemSortTask::ObTableLoadPKMemSortTask(ObTableLoadPKMemSortTask *parent)
  : ObITask(TASK_TYPE_DIRECT_LOAD_PK_MEM_SORT),
    ObTableLoadPKMemSortTaskBase(parent->dag_, parent->mem_sorter_)
{
}

ObITask::ObITaskPriority ObTableLoadPKMemSortTask::get_priority()
{
  return ObTableLoadDagTaskBase::get_priority(ctx_->can_make_round_ctx());
}

int ObTableLoadPKMemSortTask::process()
{
  int ret = OB_SUCCESS;
  if (mem_sorter_->is_finish()) {
  } else if (OB_FAIL(ctx_->make_round_ctx(round_ctx_))) {
    LOG_WARN("fail to make round ctx", KR(ret));
  } else {
    ObTableLoadPKMemSortLoadTask *load_task = nullptr;
    ObTableLoadMemCompactSampleTask *sample_task = nullptr;
    ObTableLoadMemCompactDumpTask *dump_task = nullptr;
    ObTableLoadMemCompactCompactTask *compact_task = nullptr;
    ObTableLoadPKMemSortTask *next_task = nullptr;
    // alloc task
    if (OB_FAIL(dag_->alloc_task(load_task, this, 0 /*chunk_idx*/))) {
      LOG_WARN("fail to alloc task", KR(ret));
    } else if (OB_FAIL(dag_->alloc_task(sample_task, this))) {
      LOG_WARN("fail to alloc task", KR(ret));
    } else if (OB_FAIL(dag_->alloc_task(dump_task, this, 0 /*range_idx*/))) {
      LOG_WARN("fail to alloc task", KR(ret));
    } else if (OB_FAIL(dag_->alloc_task(compact_task, this))) {
      LOG_WARN("fail to alloc task", KR(ret));
    } else if (OB_FAIL(dag_->alloc_task(next_task, this))) {
      LOG_WARN("fail to alloc task", KR(ret));
    }
    // 建立依赖关系: load_task -> sample_task -> dump_task -> compact_task -> [next_op_task]
    //                 |
    //                 +-> next_task -> [next_op_task]
    else if (OB_FAIL(load_task->add_child(*sample_task))) {
      LOG_WARN("fail to add child", KR(ret));
    } else if (OB_FAIL(sample_task->add_child(*dump_task))) {
      LOG_WARN("fail to add child", KR(ret));
    } else if (OB_FAIL(dump_task->add_child(*compact_task))) {
      LOG_WARN("fail to add child", KR(ret));
    } else if (OB_FAIL(compact_task->deep_copy_children(get_child_nodes()))) {
      LOG_WARN("fail to deep copy children", KR(ret));
    } else if (OB_FAIL(load_task->add_child(*next_task))) {
      LOG_WARN("fail to add child", KR(ret));
    } else if (OB_FAIL(next_task->deep_copy_children(get_child_nodes()))) {
      LOG_WARN("fail to deep copy children", KR(ret));
    }
    // 添加task
    else if (OB_FAIL(dag_->add_task(*next_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else if (OB_FAIL(dag_->add_task(*compact_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else if (OB_FAIL(dag_->add_task(*dump_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else if (OB_FAIL(dag_->add_task(*sample_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else if (OB_FAIL(dag_->add_task(*load_task))) {
      LOG_WARN("fail to add task", KR(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
