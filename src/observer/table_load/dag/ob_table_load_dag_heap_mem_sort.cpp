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
#define USING_LOG_PREFIX STORAGE

#include "observer/table_load/dag/ob_table_load_dag_heap_mem_sort.h"
#include "observer/table_load/dag/ob_table_load_dag.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/plan/ob_table_load_merge_op.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_external_block_reader.h"
#include "storage/direct_load/ob_direct_load_external_table.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_builder.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_compactor.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_map.h"
#include "storage/direct_load/ob_direct_load_tmp_file.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share;
using namespace storage;

/**
 * ObTableLoadHeapMemSorter
 */

ObTableLoadHeapMemSorter::ObTableLoadHeapMemSorter()
  : dag_(nullptr), op_(nullptr), next_source_idx_(0), is_closed_(false), is_inited_(false)
{
}

int ObTableLoadHeapMemSorter::init(ObTableLoadDag *dag, ObTableLoadMemSortOp *op)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadHeapMemSorter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == dag || nullptr == op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(dag), KP(op));
  } else {
    dag_ = dag;
    op_ = op;
    ObDirectLoadTableStore &table_store = op->op_ctx_->table_store_;
    FOREACH_X(it, table_store, OB_SUCC(ret))
    {
      ObDirectLoadTableHandleArray *tables_handle = it->second;
      for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle->count(); ++i) {
        const ObDirectLoadTableHandle &table_handle = tables_handle->at(i);
        const ObDirectLoadExternalFragmentArray &fragments =
          static_cast<ObDirectLoadExternalTable *>(table_handle.get_table())->get_fragments();
        if (OB_FAIL(source_fragments_.push_back(fragments))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      // 清空table_store, 以便在排序过程中能释放磁盘空间
      table_store.clear();
      table_data_desc_ = table_store.get_table_data_desc();
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadHeapMemSorter::get_next_source_fragment(ObDirectLoadExternalFragment &fragment)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadHeapMemSorter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is closed", KR(ret));
  } else {
    const int64_t idx = ATOMIC_FAA(&next_source_idx_, 1);
    if (idx >= source_fragments_.count()) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(fragment.assign(source_fragments_.at(idx)))) {
      LOG_WARN("fail to assign fragment", KR(ret));
    } else {
      source_fragments_.at(idx).reset(); // 清除引用计数
    }
  }
  return ret;
}

int ObTableLoadHeapMemSorter::add_result_table(const ObDirectLoadTableHandle &table_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadHeapMemSorter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is closed", KR(ret));
  } else if (OB_UNLIKELY(!table_handle.is_valid() ||
                         !table_handle.get_table()->is_multiple_heap_table())) {
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

int ObTableLoadHeapMemSorter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadHeapMemSorter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is closed", KR(ret));
  } else {
    ObDirectLoadTableStore &table_store = op_->op_ctx_->table_store_;
    table_store.clear();
    table_store.set_multiple_heap_table();
    if (OB_FAIL(table_store.add_tables(result_tables_handle_))) {
      LOG_WARN("fail to add tables", KR(ret));
    } else {
      result_tables_handle_.reset();
      is_closed_ = true;
    }
  }
  return ret;
}

/**
 * base
 */

ObTableLoadHeapMemSortTaskBase::ObTableLoadHeapMemSortTaskBase(ObTableLoadDag *dag,
                                                               ObTableLoadHeapMemSorter *mem_sorter)
  : ObTableLoadDagTaskBase(dag), mem_sorter_(mem_sorter)
{
}

ObTableLoadHeapMemSortTaskBase::ObTableLoadHeapMemSortTaskBase(
  ObTableLoadHeapMemSortTaskBase *parent)
  : ObTableLoadDagTaskBase(parent->dag_), mem_sorter_(parent->mem_sorter_)
{
}

/**
 * Sorter
 */

class ObTableLoadHeapMemSortTask::Sorter
{
  typedef ObDirectLoadExternalMultiPartitionRow RowType;
  typedef ObDirectLoadConstExternalMultiPartitionRow ConstRowType;
  typedef ObDirectLoadMultipleHeapTableMap ChunkType;
  typedef ObDirectLoadExternalBlockReader<RowType> ExternalReader;

public:
  Sorter(ObTableLoadHeapMemSortTask *parent)
    : parent_(parent), dag_(parent->dag_), store_ctx_(parent->store_ctx_), is_inited_(false)
  {
  }
  ~Sorter() = default;
  int init();
  int process(const ObDirectLoadExternalFragment &fragment);
  int close();

private:
  int resize_chunk();
  int close_chunk();

private:
  ObTableLoadHeapMemSortTask *parent_;
  ObTableLoadDag *dag_;
  ObTableLoadStoreCtx *store_ctx_;
  ObDirectLoadTableDataDesc table_data_desc_;
  ChunkType chunk_;
  bool is_inited_;
};

int ObTableLoadHeapMemSortTask::Sorter::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Sorter init twice", KR(ret), KP(this));
  } else {
    if (OB_FAIL(chunk_.init())) {
      LOG_WARN("fail to init chunk", KR(ret));
    } else {
      if (ObTableLoadExeMode::MAX_TYPE == store_ctx_->ctx_->param_.exe_mode_) {
        chunk_.set_mem_limit(store_ctx_->heap_table_mem_chunk_size_);
      }
      table_data_desc_ = parent_->mem_sorter_->get_table_data_desc();
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadHeapMemSortTask::Sorter::process(const ObDirectLoadExternalFragment &fragment)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Sorter not init", KR(ret), KP(this));
  } else {
    const RowType *row = nullptr;
    ConstRowType const_row;
    ExternalReader external_reader;
    if (OB_FAIL(external_reader.init(table_data_desc_.external_data_block_size_,
                                     table_data_desc_.compressor_type_))) {
      LOG_WARN("fail to init external reader", KR(ret));
    } else if (OB_FAIL(external_reader.open(fragment.file_handle_, 0, fragment.file_size_))) {
      LOG_WARN("fail to open file", KR(ret));
    } else if (OB_FAIL(resize_chunk())) {
      LOG_WARN("fail to resize chunk", KR(ret));
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(dag_->check_status())) {
        LOG_WARN("fail to check status", KR(ret));
      } else if (OB_FAIL(external_reader.get_next_item(row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next item", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        const_row = *row;
        if (OB_FAIL(chunk_.add_row(row->tablet_id_, const_row))) {
          if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
            LOG_WARN("fail to add row", KR(ret));
          } else {
            ret = OB_SUCCESS;
            if (OB_FAIL(close_chunk())) {
              LOG_WARN("fail to close chunk", KR(ret));
            } else if (OB_FAIL(chunk_.add_row(row->tablet_id_, const_row))) {
              LOG_WARN("fail to add row", KR(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          ATOMIC_AAF(&store_ctx_->ctx_->job_stat_->store_.compact_stage_load_rows_, 1);
        }
      }
    }
  }
  return ret;
}

int ObTableLoadHeapMemSortTask::Sorter::resize_chunk()
{
  int ret = OB_SUCCESS;
  if (ObTableLoadExeMode::MAX_TYPE != store_ctx_->ctx_->param_.exe_mode_) {
    int64_t sort_memory = 0;
    if (OB_FAIL(ObTableLoadService::get_sort_memory(sort_memory))) {
      LOG_WARN("fail to get sort memory", KR(ret));
    } else {
      chunk_.set_mem_limit(sort_memory);
    }
  }
  return ret;
}

int ObTableLoadHeapMemSortTask::Sorter::close_chunk()
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> keys;
  ObDirectLoadDatumRow datum_row;
  ObDirectLoadMultipleHeapTableBuilder table_builder;
  ObDirectLoadMultipleHeapTableBuildParam table_builder_param;

  keys.set_tenant_id(MTL_ID());
  table_builder_param.table_data_desc_ = table_data_desc_;
  table_builder_param.file_mgr_ = store_ctx_->tmp_file_mgr_;
  table_builder_param.extra_buf_ = reinterpret_cast<char *>(1); // unuse, delete in future
  table_builder_param.extra_buf_size_ = 4096;
  table_builder_param.index_dir_id_ = parent_->index_dir_id_;
  table_builder_param.data_dir_id_ = parent_->data_dir_id_;
  if (OB_FAIL(chunk_.get_all_key_sorted(keys))) {
    LOG_WARN("fail to get keys", KR(ret));
  } else if (OB_FAIL(datum_row.init(table_data_desc_.column_count_))) {
    LOG_WARN("fail to init datum row", KR(ret));
  } else if (OB_FAIL(table_builder.init(table_builder_param))) {
    LOG_WARN("fail to init table builder", KR(ret));
  }
  // 按tablet_id顺序写入table_builder
  for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); i++) {
    const ObTabletID &tablet_id = keys.at(i);
    ObArray<const ConstRowType *> bag;
    bag.set_tenant_id(MTL_ID());
    if (OB_FAIL(chunk_.get(tablet_id, bag))) {
      LOG_WARN("fail to get bag", KR(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < bag.count(); j++) {
      const ConstRowType *row = bag.at(j);
      if (OB_FAIL(row->to_datum_row(datum_row))) {
        LOG_WARN("fail to transfer dataum row", KR(ret));
      } else if (OB_FAIL(table_builder.append_row(tablet_id, datum_row))) {
        LOG_WARN("fail to append row", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObDirectLoadTableHandleArray tables_handle;
    if (OB_FAIL(table_builder.close())) {
      LOG_WARN("fail to close table builder", KR(ret));
    } else if (OB_FAIL(table_builder.get_tables(tables_handle, store_ctx_->table_mgr_))) {
      LOG_WARN("fail to get tables", KR(ret));
    } else if (OB_FAIL(parent_->tables_handle_.add(tables_handle))) {
      LOG_WARN("fail to add tables", KR(ret));
    } else {
      chunk_.reuse();
    }
  }
  return ret;
}

int ObTableLoadHeapMemSortTask::Sorter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Sorter not init", KR(ret), KP(this));
  } else if (!chunk_.empty() && OB_FAIL(close_chunk())) {
    LOG_WARN("fail to close chunk", KR(ret));
  }
  return ret;
}

/**
 * Compactor
 */

class ObTableLoadHeapMemSortTask::Compactor
{
  class Compare
  {
  public:
    Compare() : result_code_(OB_SUCCESS) {}
    ~Compare() = default;
    bool operator()(const ObDirectLoadTableHandle lhs, const ObDirectLoadTableHandle rhs)
    {
      int ret = OB_SUCCESS;
      int cmp_ret = 0;
      if (OB_UNLIKELY(!lhs.is_valid() || !lhs.get_table()->is_multiple_heap_table() ||
                      !rhs.is_valid() || !rhs.get_table()->is_multiple_heap_table())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid args", KR(ret), K(lhs), K(rhs));
      } else {
        ObDirectLoadMultipleHeapTable *lhs_multi_sstable =
          static_cast<ObDirectLoadMultipleHeapTable *>(lhs.get_table());
        ObDirectLoadMultipleHeapTable *rhs_multi_sstable =
          static_cast<ObDirectLoadMultipleHeapTable *>(rhs.get_table());
        cmp_ret = lhs_multi_sstable->get_meta().index_entry_count_ -
                  rhs_multi_sstable->get_meta().index_entry_count_;
      }
      if (OB_FAIL(ret)) {
        result_code_ = ret;
      }
      return cmp_ret < 0;
    }
    int get_error_code() const { return result_code_; }
    int result_code_;
  };

public:
  Compactor(ObTableLoadHeapMemSortTask *parent)
    : parent_(parent),
      dag_(parent->dag_),
      store_ctx_(parent->store_ctx_),
      curr_round_(nullptr),
      next_round_(nullptr)
  {
  }
  ~Compactor() = default;
  int init();
  int process(ObDirectLoadTableHandleArray &source_tables_handle,
              ObDirectLoadTableHandle &result_table_handle);

private:
  int compact_one_round(const int64_t compact_cnt);

private:
  ObTableLoadHeapMemSortTask *parent_;
  ObTableLoadDag *dag_;
  ObTableLoadStoreCtx *store_ctx_;
  ObDirectLoadTableHandleArray *curr_round_;
  ObDirectLoadTableHandleArray *next_round_;
  ObDirectLoadMultipleHeapTableCompactor compactor_;
};

int ObTableLoadHeapMemSortTask::Compactor::init()
{
  int ret = OB_SUCCESS;
  ObDirectLoadMultipleHeapTableCompactParam heap_table_compact_param;
  heap_table_compact_param.table_data_desc_ = parent_->mem_sorter_->get_table_data_desc();
  heap_table_compact_param.file_mgr_ = store_ctx_->tmp_file_mgr_;
  heap_table_compact_param.index_dir_id_ = parent_->index_dir_id_;
  if (OB_FAIL(compactor_.init(heap_table_compact_param))) {
    LOG_WARN("fail to init heap table compactor", KR(ret));
  }
  return ret;
}

int ObTableLoadHeapMemSortTask::Compactor::process(
  ObDirectLoadTableHandleArray &source_tables_handle, ObDirectLoadTableHandle &result_table_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(source_tables_handle.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(source_tables_handle));
  } else {
    ObDirectLoadTableHandleArray empty_tables_handle;
    curr_round_ = &source_tables_handle;
    next_round_ = &empty_tables_handle;
    while (OB_SUCC(ret) && curr_round_->count() > 1) {
      const int64_t compact_cnt = MIN(curr_round_->count(), store_ctx_->merge_count_per_round_);
      if (OB_FAIL(dag_->check_status())) {
        LOG_WARN("fail to check status", KR(ret));
      } else if (OB_FAIL(compact_one_round(compact_cnt))) {
        LOG_WARN("fail to compact one round", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(1 != curr_round_->count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected final table cnt not equal one", KR(ret));
      } else {
        result_table_handle = curr_round_->at(0);
      }
    }
    // release table array
    curr_round_->reset();
    next_round_->reset();
  }
  return ret;
}

int ObTableLoadHeapMemSortTask::Compactor::compact_one_round(const int64_t compact_cnt)
{
  int ret = OB_SUCCESS;
  // 对table根据文件大小从小到大排序
  if (compact_cnt < curr_round_->count()) {
    Compare compare;
    lib::ob_sort(curr_round_->begin(), curr_round_->end(), compare);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < curr_round_->count(); ++i) {
    ObDirectLoadTableHandle table_handle;
    if (OB_FAIL(curr_round_->get_table(i, table_handle))) {
      LOG_WARN("fail to get table", KR(ret));
    } else if (i < compact_cnt) {
      // 取最小的compact_cnt个table进行合并
      if (OB_FAIL(compactor_.add_table(table_handle))) {
        LOG_WARN("fail to add table", KR(ret));
      }
    } else {
      // 将未参与合并的table进入next_round_中
      if (OB_FAIL(next_round_->add(table_handle))) {
        LOG_WARN("fail to add table", KR(ret));
      }
    }
  }
  // 执行合并, 并将合并结果加入到next_round_中
  if (OB_SUCC(ret)) {
    curr_round_->reset();
    ObDirectLoadTableHandle compacted_table_handle;
    if (OB_FAIL(compactor_.compact())) {
      LOG_WARN("fail to do compact", KR(ret));
    } else if (OB_FAIL(compactor_.get_table(compacted_table_handle, store_ctx_->table_mgr_))) {
      LOG_WARN("fail to get table", KR(ret));
    } else if (OB_FAIL(next_round_->add(compacted_table_handle))) {
      LOG_WARN("fail to push back", KR(ret));
    } else {
      compactor_.reuse();
      std::swap(curr_round_, next_round_);
      ATOMIC_AAF(&store_ctx_->ctx_->job_stat_->store_.compact_stage_consume_tmp_files_,
                 compact_cnt - 1);
    }
  }
  return ret;
}

/*
 * sort_task
 */

ObTableLoadHeapMemSortTask::ObTableLoadHeapMemSortTask(ObTableLoadDag *dag,
                                                       ObTableLoadHeapMemSorter *mem_sorter)
  : ObITask(TASK_TYPE_DIRECT_LOAD_HEAP_MEM_SORT),
    ObTableLoadHeapMemSortTaskBase(dag, mem_sorter),
    thread_idx_(0),
    index_dir_id_(-1),
    data_dir_id_(-1)
{
}

ObTableLoadHeapMemSortTask::ObTableLoadHeapMemSortTask(ObTableLoadHeapMemSortTaskBase *parent,
                                                       const int64_t thread_idx)
  : ObITask(TASK_TYPE_DIRECT_LOAD_HEAP_MEM_SORT),
    ObTableLoadHeapMemSortTaskBase(parent),
    thread_idx_(thread_idx),
    index_dir_id_(-1),
    data_dir_id_(-1)
{
}

int ObTableLoadHeapMemSortTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  const int64_t next_thread_idx = thread_idx_ + 1;
  if (next_thread_idx >= store_ctx_->thread_cnt_) {
    ret = OB_ITER_END;
  } else {
    ObTableLoadHeapMemSortTask *task = nullptr;
    if (OB_FAIL(dag_->alloc_task(task, this, next_thread_idx))) {
      LOG_WARN("fail to alloc task", KR(ret), K(next_thread_idx));
    } else {
      next_task = task;
    }
  }
  return ret;
}

int ObTableLoadHeapMemSortTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(store_ctx_->tmp_file_mgr_->alloc_dir(index_dir_id_))) {
    LOG_WARN("fail to alloc dir", KR(ret));
  } else if (OB_FAIL(store_ctx_->tmp_file_mgr_->alloc_dir(data_dir_id_))) {
    LOG_WARN("fail to alloc dir", KR(ret));
  } else if (OB_FAIL(do_sort())) {
    LOG_WARN("fail to do sort", KR(ret));
  } else if (OB_FAIL(do_compact())) {
    LOG_WARN("fail to do compact", KR(ret));
  }
  return ret;
}

int ObTableLoadHeapMemSortTask::do_sort()
{
  int ret = OB_SUCCESS;
  Sorter sorter(this);
  if (OB_FAIL(sorter.init())) {
    LOG_WARN("fail to init sorter", KR(ret));
  }
  while (OB_SUCC(ret)) {
    ObDirectLoadExternalFragment fragment;
    if (OB_FAIL(dag_->check_status())) {
      LOG_WARN("fail to check status", KR(ret));
    } else if (OB_FAIL(mem_sorter_->get_next_source_fragment(fragment))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next source fragment", KR(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_FAIL(sorter.process(fragment))) {
      LOG_WARN("fail to do sort fragment", KR(ret));
    }
  }
  if (FAILEDx(sorter.close())) {
    LOG_WARN("fail to close sorter", KR(ret));
  }
  return ret;
}

int ObTableLoadHeapMemSortTask::do_compact()
{
  int ret = OB_SUCCESS;
  if (tables_handle_.empty()) {
    // do nothing
  } else {
    Compactor compactor(this);
    ObDirectLoadTableHandle table_handle;
    if (OB_FAIL(compactor.init())) {
      LOG_WARN("fail to init compactor", KR(ret));
    } else if (OB_FAIL(compactor.process(tables_handle_, table_handle))) {
      LOG_WARN("fail to compact", KR(ret));
    } else if (OB_FAIL(mem_sorter_->add_result_table(table_handle))) {
      LOG_WARN("fail to add result table", KR(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
