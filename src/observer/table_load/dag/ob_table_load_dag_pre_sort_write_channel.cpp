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

#include "observer/table_load/dag/ob_table_load_dag_pre_sort_write_channel.h"
#include "observer/table_load/dag/ob_table_load_dag.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/plan/ob_table_load_write_op.h"
#include "storage/direct_load/ob_direct_load_batch_rows.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share;
using namespace storage;

/**
 * ObTableLoadDagPreSortWriteChannel
 */

ObTableLoadDagPreSortWriteChannel::ObTableLoadDagPreSortWriteChannel()
  : op_(nullptr), finish_add_closed_chunk_(false), no_more_closed_chunk_(false)
{
  chunk_nodes_.set_tenant_id(MTL_ID());
  closed_chunks_.set_tenant_id(MTL_ID());
  sort_chunk_tasks_.set_tenant_id(MTL_ID());
}

ObTableLoadDagPreSortWriteChannel::~ObTableLoadDagPreSortWriteChannel()
{
  // release chunk in chunk_nodes_
  for (int64_t i = 0; i < chunk_nodes_.count(); ++i) {
    ChunkNode &chunk_node = chunk_nodes_.at(i);
    if (nullptr != chunk_node.chunk_) {
      mem_compact_ctx_.release_chunk(chunk_node.chunk_);
      chunk_node.chunk_ = nullptr;
    }
  }
  chunk_nodes_.reset();
  // release chunk in closed_chunks_
  for (int64_t i = 0; i < closed_chunks_.count(); ++i) {
    ChunkType *chunk = closed_chunks_.at(i);
    mem_compact_ctx_.release_chunk(chunk);
  }
  closed_chunks_.reset();
}

int ObTableLoadDagPreSortWriteChannel::init(ObTableLoadDag *dag, ObTableLoadPreSortWriteOp *op)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadDagPreSortWriteChannel init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == dag || nullptr == op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(dag), KP(op));
  } else {
    store_ctx_ = dag->store_ctx_;
    dag_ = dag;
    op_ = op;
    if (OB_FAIL(inner_init())) {
      LOG_WARN("fail to init", KR(ret));
    }
    // init mem_compact_ctx_
    if (OB_SUCC(ret)) {
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
    }
    // init chunk_nodes_
    if (OB_SUCC(ret)) {
      const int64_t chunk_node_cnt = MAX(store_ctx_->max_mem_chunk_count_ / 2, 1);
      if (OB_FAIL(chunk_nodes_.prepare_allocate(chunk_node_cnt))) {
        LOG_WARN("fail to prepare allocate chunk node", KR(ret), K(chunk_node_cnt));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadDagPreSortWriteChannel::create_writer(ObTableLoadDagChunkWriter *&writer,
                                                     ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(writer = OB_NEWx(ObTableLoadDagPreSortChunkWriter, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadDagPreSortChunkWriter", KR(ret));
  }
  return ret;
}

int ObTableLoadDagPreSortWriteChannel::do_flush()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < chunk_nodes_.count(); ++i) {
    ChunkNode &chunk_node = chunk_nodes_.at(i);
    if (OB_UNLIKELY(chunk_node.is_used())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("chunk node should not be used", KR(ret), K(i), K(chunk_node));
    } else if (nullptr == chunk_node.chunk_) {
      // do nothing
    } else if (OB_FAIL(add_closed_chunk(chunk_node.chunk_))) {
      LOG_WARN("fail to push", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(finish_add_closed_chunk())) {
      LOG_WARN("fail to finish add closed chunk", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadDagPreSortWriteChannel::do_close()
{
  int ret = OB_SUCCESS;
  ObDirectLoadTableStore &table_store = op_->op_ctx_->table_store_;
  table_store.clear();
  table_store.set_multiple_sstable();
  if (OB_FAIL(table_store.add_tables(mem_compact_ctx_.get_result_tables_handle()))) {
    LOG_WARN("fail to add tables", KR(ret));
  } else {
    mem_compact_ctx_.reset();
  }
  return ret;
}

int ObTableLoadDagPreSortWriteChannel::acquire_chunk(ChunkType *&chunk)
{
  int ret = OB_SUCCESS;
  chunk = nullptr;
  ObMutexGuard guard(acquire_chunk_mutex_);
  if (mem_compact_ctx_.get_chunk_count() >= store_ctx_->max_mem_chunk_count_) {
    ret = OB_EAGAIN;
  } else {
    if (OB_FAIL(mem_compact_ctx_.acquire_chunk(chunk))) {
      LOG_WARN("fail to acquire chunk", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadDagPreSortWriteChannel::get_chunk(int64_t &chunk_node_id, ChunkType *&chunk)
{
  int ret = OB_SUCCESS;
  chunk_node_id = -1;
  chunk = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagPreSortWriteChannel not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_flushed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is flushed", KR(ret));
  } else {
    int64_t cur_chunk_node_id = ObRandom::rand(0, chunk_nodes_.count() - 1);
    while (OB_SUCC(ret) && -1 == chunk_node_id) {
      ChunkNode &chunk_node = chunk_nodes_.at(cur_chunk_node_id);
      if (!chunk_node.is_used() && chunk_node.set_used()) {
        if (nullptr == chunk_node.chunk_) {
          if (OB_FAIL(acquire_chunk(chunk_node.chunk_))) {
            if (OB_UNLIKELY(OB_EAGAIN != ret)) {
              LOG_WARN("fail to acquire chunk", KR(ret));
            } else {
              // chunk数目已经达到上限, 等待一段时间后再来获取
              if (OB_UNLIKELY(!chunk_node.set_unused())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected set chunk node unused failed", KR(ret), K(chunk_node_id),
                         K(chunk_node));
              }
              break;
            }
          } else {
            chunk_node_id = cur_chunk_node_id;
            chunk = chunk_node.chunk_;
          }
        } else {
          chunk_node_id = cur_chunk_node_id;
          chunk = chunk_node.chunk_;
        }
      } else {
        // switch next chunk
        ++cur_chunk_node_id;
        cur_chunk_node_id %= chunk_nodes_.count();
      }
    }
  }
  return ret;
}

int ObTableLoadDagPreSortWriteChannel::push_chunk(int64_t chunk_node_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagPreSortWriteChannel not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(chunk_node_id < 0 || chunk_node_id >= chunk_nodes_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(chunk_node_id));
  } else {
    ChunkNode &chunk_node = chunk_nodes_.at(chunk_node_id);
    if (OB_UNLIKELY(!chunk_node.is_used())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("chunk node should be used", KR(ret), K(chunk_node_id), K(chunk_node));
    } else if (OB_UNLIKELY(!chunk_node.set_unused())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected set chunk node unused failed", KR(ret), K(chunk_node_id), K(chunk_node));
    }
  }
  return ret;
}

int ObTableLoadDagPreSortWriteChannel::close_chunk(int64_t chunk_node_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagPreSortWriteChannel is not init", KR(ret));
  } else if (OB_UNLIKELY(chunk_node_id < 0 || chunk_node_id >= chunk_nodes_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(chunk_node_id));
  } else {
    ChunkNode &chunk_node = chunk_nodes_.at(chunk_node_id);
    if (OB_UNLIKELY(!chunk_node.is_used() || nullptr == chunk_node.chunk_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("chunk node should be used and chunk not null", KR(ret), K(chunk_node_id),
               K(chunk_node));
    } else if (OB_FAIL(add_closed_chunk(chunk_node.chunk_))) {
      LOG_WARN("fail to push", KR(ret));
    } else if (OB_UNLIKELY(!chunk_node.set_unused())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected set chunk node unused failed", KR(ret), K(chunk_node_id), K(chunk_node));
    }
  }
  return ret;
}

int ObTableLoadDagPreSortWriteChannel::add_sort_chunk_task(SortChunkTask *task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagPreSortWriteChannel not init", KR(ret), KP(this));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(task));
  } else {
    if (no_more_closed_chunk_) {
    } else {
      ObMutexGuard guard(closed_chunk_mutex_);
      if (!closed_chunks_.empty()) {
        ChunkType *chunk = closed_chunks_.at(closed_chunks_.count() - 1);
        closed_chunks_.pop_back();
        task->set_chunk(chunk);
        no_more_closed_chunk_ = finish_add_closed_chunk_ && closed_chunks_.empty();
      } else if (finish_add_closed_chunk_) {
      } else {
        if (OB_FAIL(sort_chunk_tasks_.push_back(task))) {
          LOG_WARN("fail to push back", KR(ret));
        } else {
          task = nullptr;
        }
      }
    }
    if (OB_SUCC(ret) && nullptr != task) {
      if (OB_FAIL(dag_->add_task(*task))) {
        LOG_WARN("fail to add task", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadDagPreSortWriteChannel::add_closed_chunk(ChunkType *&chunk)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(chunk)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected chunk is null", KR(ret));
  } else {
    SortChunkTask *task = nullptr;
    {
      ObMutexGuard guard(closed_chunk_mutex_);
      if (OB_UNLIKELY(finish_add_closed_chunk_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not add closed chunk", KR(ret));
      } else if (!sort_chunk_tasks_.empty()) {
        task = sort_chunk_tasks_.at(sort_chunk_tasks_.count() - 1);
        sort_chunk_tasks_.pop_back();
        task->set_chunk(chunk);
        chunk = nullptr;
      } else {
        if (OB_FAIL(closed_chunks_.push_back(chunk))) {
          LOG_WARN("fail to push back", KR(ret));
        } else {
          chunk = nullptr;
        }
      }
    }
    if (OB_SUCC(ret) && nullptr != task) {
      if (OB_FAIL(dag_->add_task(*task))) {
        LOG_WARN("fail to add task", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadDagPreSortWriteChannel::finish_add_closed_chunk()
{
  int ret = OB_SUCCESS;
  ObArray<SortChunkTask *> sort_chunk_tasks;
  {
    ObMutexGuard guard(closed_chunk_mutex_);
    finish_add_closed_chunk_ = true;
    no_more_closed_chunk_ = closed_chunks_.empty();
    if (OB_FAIL(sort_chunk_tasks.assign(sort_chunk_tasks_))) {
      LOG_WARN("fail to assign", KR(ret));
    } else {
      sort_chunk_tasks_.reset();
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sort_chunk_tasks.count(); ++i) {
    SortChunkTask *task = sort_chunk_tasks.at(i);
    if (OB_FAIL(dag_->add_task(*task))) {
      LOG_WARN("fail to add task", KR(ret));
    }
  }
  return ret;
}

/**
 * ObTableLoadDagPreSortChunkWriter
 */

ObTableLoadDagPreSortChunkWriter::ObTableLoadDagPreSortChunkWriter()
  : store_ctx_(nullptr),
    write_channel_(nullptr),
    mem_compact_ctx_(nullptr),
    chunk_node_id_(-1),
    chunk_(nullptr),
    is_closed_(false)
{
}

ObTableLoadDagPreSortChunkWriter::~ObTableLoadDagPreSortChunkWriter() {}

int ObTableLoadDagPreSortChunkWriter::init(ObTableLoadDagWriteChannel *write_channel,
                                           ObTableLoadStoreTrans *trans,
                                           ObTableLoadTransStoreWriter *store_writer,
                                           const int32_t session_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadDagPreSortChunkWriter init twice", KR(ret));
  } else if (OB_UNLIKELY(nullptr == write_channel || nullptr == trans || nullptr == store_writer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(write_channel), KP(trans), KP(store_writer));
  } else {
    dag_ = write_channel->dag_;
    trans_ = trans;
    store_writer_ = store_writer;
    session_id_ = session_id;
    store_ctx_ = write_channel->store_ctx_;
    write_channel_ = static_cast<ObTableLoadDagPreSortWriteChannel *>(write_channel);
    mem_compact_ctx_ = &write_channel_->mem_compact_ctx_;
    if (OB_FAIL(datum_row_.init(
          write_channel_->op_->op_ctx_->table_store_.get_table_data_desc().column_count_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadDagPreSortChunkWriter::inner_append_row(const ObTabletID &tablet_id,
                                                       const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  ConstRowType const_row;
  row_.tablet_id_ = tablet_id;
  if (OB_FAIL(row_.external_row_.from_datum_row(
        datum_row, mem_compact_ctx_->table_data_desc_.rowkey_column_num_))) {
    LOG_WARN("fail to cast row from datum row", KR(ret));
  } else {
    const_row = row_;
    while (OB_SUCC(ret)) {
      if (chunk_ == nullptr && OB_FAIL(write_channel_->get_chunk(chunk_node_id_, chunk_))) {
        if (OB_UNLIKELY(OB_EAGAIN != ret)) {
          LOG_WARN("fail to get chunk", KR(ret));
        } else {
          break;
        }
      } else if (OB_FAIL(chunk_->add_item(const_row))) {
        if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
          LOG_WARN("fail to add item", KR(ret));
        } else {
          ret = OB_SUCCESS;
          if (OB_FAIL(write_channel_->close_chunk(chunk_node_id_))) {
            LOG_WARN("fail to close chunk", KR(ret));
          } else {
            chunk_ = nullptr;
            chunk_node_id_ = -1;
          }
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObTableLoadDagPreSortChunkWriter::append_row(const ObTabletID &tablet_id,
                                                 const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagPreSortChunkWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is closed", KR(ret));
  } else if (OB_FAIL(inner_append_row(tablet_id, datum_row))) {
    if (OB_UNLIKELY(OB_EAGAIN != ret)) {
      LOG_WARN("fail to append row", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadDagPreSortChunkWriter::append_batch(ObIVector *tablet_id_vector,
                                                   const ObDirectLoadBatchRows &batch_rows,
                                                   int64_t &start)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagPreSortChunkWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is closed", KR(ret));
  } else if (OB_UNLIKELY(nullptr == tablet_id_vector || batch_rows.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(tablet_id_vector), K(batch_rows));
  } else {
    datum_row_.seq_no_ = 0;
    while (OB_SUCC(ret) && start < batch_rows.size()) {
      const ObTabletID tablet_id = ObDirectLoadVectorUtils::get_tablet_id(tablet_id_vector, start);
      if (OB_FAIL(batch_rows.get_datum_row(start, datum_row_))) {
        LOG_WARN("fail to get datum row", KR(ret), K(start));
      } else if (OB_FAIL(inner_append_row(tablet_id, datum_row_))) {
        if (OB_UNLIKELY(OB_EAGAIN != ret)) {
          LOG_WARN("fail to append row", KR(ret));
        } else {
          break;
        }
      } else {
        ++start;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(push_chunk())) {
        LOG_WARN("fail to push chunk", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadDagPreSortChunkWriter::push_chunk()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(-1 != chunk_node_id_ && nullptr != chunk_)) {
    if (OB_FAIL(write_channel_->push_chunk(chunk_node_id_))) {
      LOG_WARN("fail to push chunk", K(chunk_node_id_), KR(ret));
    } else {
      chunk_ = nullptr;
      chunk_node_id_ = -1;
    }
  }
  return ret;
}

int ObTableLoadDagPreSortChunkWriter::close(ObTableLoadStoreTrans *trans, const int32_t session_id)
{
  UNUSEDx(trans, session_id);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagPreSortChunkWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is closed", KR(ret));
  } else if (OB_FAIL(push_chunk())) {
    LOG_WARN("fail to push chunk", KR(ret));
  } else {
    is_closed_ = true;
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
