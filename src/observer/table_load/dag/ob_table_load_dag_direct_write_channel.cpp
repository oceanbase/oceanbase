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

#include "observer/table_load/dag/ob_table_load_dag_direct_write_channel.h"
#include "observer/table_load/dag/ob_table_load_dag.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/plan/ob_table_load_write_op.h"
#include "storage/direct_load/ob_direct_load_batch_rows.h"
#include "storage/direct_load/ob_direct_load_dag_insert_table_row_writer.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share;
using namespace storage;

/**
 * ObTableLoadDagDirectWriteChannel
 */

ObTableLoadDagDirectWriteChannel::ObTableLoadDagDirectWriteChannel() : op_(nullptr) {}

int ObTableLoadDagDirectWriteChannel::init(ObTableLoadDag *dag, ObTableLoadDirectWriteOp *op)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadDagDirectWriteChannel init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == dag || nullptr == op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(dag), KP(op));
  } else {
    store_ctx_ = dag->store_ctx_;
    dag_ = dag;
    op_ = op;
    if (OB_FAIL(inner_init())) {
      LOG_WARN("fail to init", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadDagDirectWriteChannel::create_writer(ObTableLoadDagChunkWriter *&writer,
                                                    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(writer = OB_NEWx(ObTableLoadDagDirectChunkWriter, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadDagDirectChunkWriter", KR(ret));
  }
  return ret;
}

int ObTableLoadDagDirectWriteChannel::do_close()
{
  int ret = OB_SUCCESS;
  ObDirectLoadTableStore &table_store = op_->op_ctx_->table_store_;
  table_store.clear();
  table_store.set_external_table();
  return ret;
}

/**
 * ObTableLoadDagDirectChunkWriter
 */

ObTableLoadDagDirectChunkWriter::ObTableLoadDagDirectChunkWriter()
  : store_ctx_(nullptr),
    write_channel_(nullptr),
    batch_writer_allocator_("TLD_BatchWriter"),
    allocator_("TLD_DirectWrite"),
    max_batch_size_(0),
    selector_(nullptr),
    tablet_offsets_(nullptr),
    is_single_part_(false),
    is_closed_(false)
{
  batch_writer_allocator_.set_tenant_id(MTL_ID());
  batch_writers_.set_attr(ObMemAttr(MTL_ID(), "TLD_BatchWriter"));
  allocator_.set_tenant_id(MTL_ID());
}

ObTableLoadDagDirectChunkWriter::~ObTableLoadDagDirectChunkWriter()
{
  for (int64_t i = 0; i < batch_writers_.count(); ++i) {
    BatchWriter *batch_writer = batch_writers_.at(i);
    batch_writer->~BatchWriter();
    batch_writer_allocator_.free(batch_writer);
  }
  batch_writers_.reset();
  batch_writer_map_.destroy();
}

int ObTableLoadDagDirectChunkWriter::init(ObTableLoadDagWriteChannel *write_channel,
                                          ObTableLoadStoreTrans *trans,
                                          ObTableLoadTransStoreWriter *store_writer,
                                          const int32_t session_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadDagDirectChunkWriter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == write_channel || nullptr == trans || nullptr == store_writer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(write_channel), KP(trans), KP(store_writer));
  } else {
    dag_ = write_channel->dag_;
    trans_ = trans;
    store_writer_ = store_writer;
    session_id_ = session_id;
    store_ctx_ = write_channel->store_ctx_;
    write_channel_ = static_cast<ObTableLoadDagDirectWriteChannel *>(write_channel);
    max_batch_size_ = store_ctx_->ctx_->param_.batch_size_;
    const int64_t tablet_cnt =
      write_channel_->op_->op_ctx_->store_table_ctx_->ls_partition_ids_.count();
    if (1 == tablet_cnt) {
      is_single_part_ = true;
      single_tablet_id_ = write_channel_->op_->op_ctx_->store_table_ctx_->ls_partition_ids_[0]
                            .part_tablet_id_.tablet_id_;
    }
    if (OB_FAIL(batch_writer_map_.create(64, "TLD_BW_Map", "TLD_BW_Map", MTL_ID()))) {
      LOG_WARN("fail to create hashmap", KR(ret));
    } else if (OB_ISNULL(selector_ = static_cast<uint16_t *>(
                           allocator_.alloc(sizeof(uint16_t) * max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", KR(ret), K(max_batch_size_));
    } else if (OB_ISNULL(tablet_offsets_ = static_cast<uint16_t *>(
                           allocator_.alloc(sizeof(uint16_t) * (tablet_cnt + 1))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", KR(ret), K(tablet_cnt));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadDagDirectChunkWriter::new_batch_writer(const ObTabletID &tablet_id,
                                                      BatchWriter *&batch_writer)
{
  int ret = OB_SUCCESS;
  batch_writer = nullptr;
  ObDirectLoadInsertTabletContext *insert_tablet_ctx = nullptr;
  if (OB_FAIL(write_channel_->op_->op_ctx_->insert_table_ctx_->get_tablet_context(
        tablet_id, insert_tablet_ctx))) {
    LOG_WARN("fail to get tablet context ", KR(ret), K(tablet_id));
  } else if (OB_ISNULL(batch_writer = OB_NEWx(BatchWriter, &batch_writer_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new BatchWriter", KR(ret));
  } else if (OB_FAIL(batch_writer->init(insert_tablet_ctx,
                                        write_channel_->op_->op_ctx_->dml_row_handler_))) {
    LOG_WARN("fail to init batch writer", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != batch_writer) {
      batch_writer->~BatchWriter();
      batch_writer_allocator_.free(batch_writer);
      batch_writer = nullptr;
    }
  }
  return ret;
}

int ObTableLoadDagDirectChunkWriter::get_batch_writer(const ObTabletID &tablet_id,
                                                      BatchWriter *&batch_writer)
{
  int ret = OB_SUCCESS;
  batch_writer = nullptr;
  if (is_single_part_) {
    if (!batch_writers_.empty()) {
      batch_writer = batch_writers_.at(0);
    }
  } else {
    if (OB_FAIL(batch_writer_map_.get_refactored(tablet_id, batch_writer))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get refactored", KR(ret), K(tablet_id));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  if (OB_SUCC(ret) && nullptr == batch_writer) {
    // new batch writer
    if (OB_FAIL(new_batch_writer(tablet_id, batch_writer))) {
      LOG_WARN("fail to new batch writer", KR(ret), K(tablet_id));
    } else if (OB_FAIL(batch_writer_map_.set_refactored(tablet_id, batch_writer))) {
      LOG_WARN("fail to set refactored", KR(ret), K(tablet_id));
    } else if (OB_FAIL(batch_writers_.push_back(batch_writer))) {
      LOG_WARN("fail to push back", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != batch_writer) {
        batch_writer->~BatchWriter();
        batch_writer_allocator_.free(batch_writer);
        batch_writer = nullptr;
      }
    }
  }
  return ret;
}

int ObTableLoadDagDirectChunkWriter::append_row(const ObTabletID &tablet_id,
                                                const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagDirectChunkWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is closed", KR(ret));
  } else {
    BatchWriter *batch_writer = nullptr;
    if (OB_FAIL(get_batch_writer(tablet_id, batch_writer))) {
      LOG_WARN("fail to get batch writer", KR(ret), K(tablet_id));
    } else if (OB_FAIL(batch_writer->append_row(datum_row))) {
      LOG_WARN("fail to append row", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadDagDirectChunkWriter::append_batch(ObIVector *tablet_id_vector,
                                                  const ObDirectLoadBatchRows &batch_rows,
                                                  int64_t &start)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagDirectChunkWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is closed", KR(ret));
  } else if (OB_UNLIKELY(nullptr == tablet_id_vector || batch_rows.empty() || 0 != start)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(tablet_id_vector), K(batch_rows), K(start));
  } else if (is_single_part_) { // 单分区场景
    BatchWriter *batch_writer = nullptr;
    if (OB_FAIL(get_batch_writer(single_tablet_id_, batch_writer))) {
      LOG_WARN("fail to get batch writer", KR(ret));
    } else if (OB_FAIL(batch_writer->append_batch(batch_rows))) {
      LOG_WARN("fail to append batch", KR(ret));
    }
  } else { // 多分区场景
    const int64_t num_rows = batch_rows.size();
    const uint64_t *tablet_ids =
      reinterpret_cast<uint64_t *>(static_cast<ObFixedLengthBase *>(tablet_id_vector)->get_data());
    const bool all_tablet_id_is_same =
      ObDirectLoadVectorUtils::check_all_tablet_id_is_same(tablet_ids, num_rows);
    BatchWriter *batch_writer = nullptr;
    if (all_tablet_id_is_same) { // 数据属于同一个分区
      const ObTabletID tablet_id(tablet_ids[0]);
      if (OB_FAIL(get_batch_writer(tablet_id, batch_writer))) {
        LOG_WARN("fail to get batch writer", KR(ret));
      } else if (OB_FAIL(batch_writer->append_batch(batch_rows))) {
        LOG_WARN("fail to append batch", KR(ret));
      }
    } else { // 数据属于多个分区
      const ObTableLoadStoreWriteCtx::TabletIdxMap &tablet_idx_map =
        store_ctx_->write_ctx_.tablet_idx_map_;
      const int64_t tablet_cnt = tablet_idx_map.size();
      MEMSET(selector_, 0, sizeof(uint16_t) * max_batch_size_);
      MEMSET(tablet_offsets_, 0, sizeof(uint16_t) * (tablet_cnt + 1));
      for (int64_t i = 0; OB_SUCC(ret) && i < num_rows; ++i) {
        const uint64_t tablet_id = tablet_ids[i];
        const int64_t *tablet_idx = tablet_idx_map.get(tablet_id);
        if (OB_ISNULL(tablet_idx)) {
          ret = OB_TABLET_NOT_EXIST;
          LOG_WARN("unexpected tablet id not found", KR(ret), K(tablet_id));
        } else {
          tablet_offsets_[*tablet_idx]++;
        }
      }
      for (int64_t i = 1; OB_SUCC(ret) && i <= tablet_cnt; ++i) {
        tablet_offsets_[i] += tablet_offsets_[i - 1];
      }
      for (int i = num_rows - 1; OB_SUCC(ret) && i >= 0; --i) {
        const uint64_t tablet_id = tablet_ids[i];
        const int64_t *tablet_idx = tablet_idx_map.get(tablet_id);
        selector_[tablet_offsets_[*tablet_idx] - 1] = i;
        tablet_offsets_[*tablet_idx]--;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cnt; ++i) {
        const int64_t start = tablet_offsets_[i];
        const int64_t size = tablet_offsets_[i + 1] - start;
        if (size > 0) {
          const ObTabletID tablet_id(tablet_ids[selector_[start]]);
          if (OB_FAIL(get_batch_writer(tablet_id, batch_writer))) {
            LOG_WARN("fail to get batch writer", KR(ret));
          } else if (OB_FAIL(batch_writer->append_selective(batch_rows, selector_ + start, size))) {
            LOG_WARN("fail to px write", KR(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    start = batch_rows.size();
  }
  return ret;
}

int ObTableLoadDagDirectChunkWriter::close(ObTableLoadStoreTrans *trans, const int32_t session_id)
{
  UNUSEDx(trans, session_id);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagDirectChunkWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is closed", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_writers_.count(); ++i) {
      BatchWriter *batch_writer = batch_writers_.at(i);
      if (OB_FAIL(batch_writer->close())) {
        LOG_WARN("fail to close", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_closed_ = true;
    }
  }
  return ret;
}
} // namespace observer
} // namespace oceanbase
