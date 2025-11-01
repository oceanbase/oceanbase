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

#include "observer/table_load/dag/ob_table_load_dag_write_channel.h"
#include "observer/table_load/dag/ob_table_load_dag.h"
#include "observer/table_load/ob_table_load_error_row_handler.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_trans.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_trans_store.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share;
using namespace storage;
using namespace table;

/**
 * ObTableLoadDagWriteChannel
 */

ObTableLoadDagWriteChannel::ObTableLoadDagWriteChannel()
  : store_ctx_(nullptr),
    dag_(nullptr),
    flush_task_(nullptr),
    is_flushed_(false),
    is_closed_(false),
    is_inited_(false)
{
}

int ObTableLoadDagWriteChannel::inner_init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == dag_ || nullptr != flush_task_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), KP(dag_), KP(flush_task_));
  } else if (OB_FAIL(dag_->alloc_task(flush_task_, this))) {
    LOG_WARN("fail to alloc task", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadDagWriteChannel::create_writer(ObTableLoadStoreTrans *trans,
                                              ObTableLoadTransStoreWriter *store_writer,
                                              const int32_t session_id,
                                              ObTableLoadDagWriter *&writer,
                                              ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  writer = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagWriteChannel not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_flushed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is flushed", KR(ret));
  } else {
    ObTableLoadDagChunkWriter *chunk_writer = nullptr;
    if (OB_FAIL(create_writer(chunk_writer, allocator))) {
      LOG_WARN("fail to create writer", KR(ret));
    } else if (OB_FAIL(chunk_writer->init(this, trans, store_writer, session_id))) {
      LOG_WARN("fail to init writer", KR(ret));
    } else {
      writer = chunk_writer;
      LOG_INFO("create writer", KR(ret), KP(trans), K(session_id), KP(writer));
    }
    if (OB_FAIL(ret)) {
      OB_DELETEx(ObTableLoadDagChunkWriter, &allocator, chunk_writer);
    }
  }
  return ret;
}

int ObTableLoadDagWriteChannel::flush()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagWriteChannel is not init", KR(ret));
  } else if (OB_ISNULL(flush_task_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected flush task is null", KR(ret));
  } else if (OB_FAIL(dag_->add_task(*flush_task_))) {
    LOG_WARN("fail to add task", KR(ret));
  } else {
    flush_task_ = nullptr;
  }
  return ret;
}

int ObTableLoadDagWriteChannel::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagWriteChannel is not init", KR(ret));
  } else if (OB_UNLIKELY(!is_flushed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is not flushed", KR(ret));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is closed", KR(ret));
  } else if (OB_FAIL(do_close())) {
    LOG_WARN("fail to close", KR(ret));
  } else {
    is_closed_ = true;
  }
  return ret;
}

int ObTableLoadDagWriteChannel::inner_flush()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_flushed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is flushed", KR(ret));
  } else if (OB_FAIL(do_flush())) {
    LOG_WARN("fail to flush", KR(ret));
  } else {
    is_flushed_ = true;
  }
  return ret;
}

ObTableLoadDagChunkWriter::ObTableLoadDagChunkWriter()
  : dag_(nullptr), trans_(nullptr), store_writer_(nullptr), session_id_(0), is_inited_(false)
{
}

int ObTableLoadDagChunkWriter::write(const ObTableLoadTabletObjRowArray &row_array)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagObjRowWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(row_array.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected no row to write", KR(ret), K(row_array.count()));
  } else {
    const ObDirectLoadDatumRow *datum_row = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < row_array.count(); ++i) {
      const ObTableLoadTabletObjRow &obj_row = row_array.at(i);
      if (OB_FAIL(store_writer_->cast_row(session_id_, obj_row.obj_row_, datum_row, obj_row.tablet_id_))) {
        ObTableLoadErrorRowHandler *error_row_handler = dag_->store_ctx_->error_row_handler_;
        LOG_INFO("write row error", K(ret), K(obj_row));
        if (OB_FAIL(error_row_handler->handle_error_row(ret))) {
          LOG_WARN("fail to handle error row", K(ret), K(obj_row));
        } else {
          ret = OB_SUCCESS;
          continue;
        }
      }
      while (OB_SUCC(ret)) {
        if (OB_FAIL(append_row(obj_row.tablet_id_, *datum_row))) {
          if (OB_UNLIKELY(OB_EAGAIN != ret)) {
            LOG_WARN("fail to append row", KR(ret));
          } else {
            ret = OB_SUCCESS;
            ob_usleep(50 * 1000);
            if (OB_FAIL(dag_->check_status())) {
              LOG_WARN("fail to check status", KR(ret));
            }
          }
        } else {
          break;
        }
      }
    }
  }
  return ret;
}

int ObTableLoadDagChunkWriter::px_write(ObIVector *tablet_id_vector,
                                        const ObDirectLoadBatchRows &batch_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagPXWriter not init", KR(ret), KP(this));
  } else {
    int64_t start = 0;
    while (OB_SUCC(ret) && start < batch_rows.size()) {
      if (OB_FAIL(append_batch(tablet_id_vector, batch_rows, start))) {
        if (OB_UNLIKELY(OB_EAGAIN != ret)) {
          LOG_WARN("fail to append batch", KR(ret));
        } else {
          ret = OB_SUCCESS;
          ob_usleep(50 * 1000);
          if (OB_FAIL(dag_->check_status())) {
            LOG_WARN("fail to check status", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
