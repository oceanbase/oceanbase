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

#ifndef OB_DTL_BASIC_CHANNEL_H
#define OB_DTL_BASIC_CHANNEL_H

#include <stdint.h>
#include <functional>
#include "lib/lock/ob_scond.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/dtl/ob_dtl_buf_allocator.h"
#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "share/ob_scanner.h"
#include "observer/ob_server_struct.h"
#include "sql/dtl/ob_dtl_rpc_proxy.h"
#include "sql/dtl/ob_dtl_fc_server.h"
#include "sql/engine/px/ob_px_row_store.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "lib/ob_define.h"
#include "lib/lock/ob_futex.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/dtl/ob_dtl_vectors_buffer.h"
#include "sql/engine/basic/ob_temp_row_store.h"

namespace oceanbase {

// forward declarations
namespace common {
class ObNewRow;
class ObScanner;
class ObThreadCond;
}  // common

namespace sql {
namespace dtl {


class  ObDtlBcastService;

enum DtlWriterType
{
  CONTROL_WRITER = 0,
  CHUNK_ROW_WRITER = 1,
  CHUNK_DATUM_WRITER = 2,
  VECTOR_WRITER = 3,
  VECTOR_FIXED_WRITER = 4,
  VECTOR_ROW_WRITER = 5,
  MAX_WRITER = 6
};

static DtlWriterType msg_writer_map[] =
{
  MAX_WRITER, // 0
  MAX_WRITER, // 1
  MAX_WRITER, // 2
  MAX_WRITER, // 3
  MAX_WRITER, // 4
  MAX_WRITER, // 5
  MAX_WRITER, // 6
  MAX_WRITER, // 7
  MAX_WRITER, // 8
  MAX_WRITER, // 9
  CONTROL_WRITER, // TESTING
  CONTROL_WRITER, // INIT_SQC_RESULT
  CONTROL_WRITER, // FINISH_SQC_RESULT
  CONTROL_WRITER, // FINISH_TASK_RESULT
  CONTROL_WRITER, // PX_RECEIVE_DATA_CHANNEL
  CONTROL_WRITER, // PX_TRANSMIT_DATA_CHANNEL
  CONTROL_WRITER, // PX_CANCEL_DFO
  MAX_WRITER, // PX_NEW_ROW
  CONTROL_WRITER, // UNBLOCKING_DATA_FLOW
  CHUNK_ROW_WRITER, // PX_CHUNK_ROW
  CONTROL_WRITER, // DRAIN_DATA_FLOW
  CONTROL_WRITER, // PX_BLOOM_FILTER_CHANNEL
  CONTROL_WRITER, // PX_BLOOM_FILTER_DATA
  CHUNK_DATUM_WRITER, // PX_DATUM_ROW
  CONTROL_WRITER, // DH_BARRIER_PIECE_MSG,
  CONTROL_WRITER, // DH_BARRIER_WHOLE_MSG,
  CONTROL_WRITER, // DH_WINBUF_PIECE_MSG,
  CONTROL_WRITER, // DH_WINBUF_WHOLE_MSG,
  CONTROL_WRITER, // FINISH_DAS_TASK_RESULT
  CONTROL_WRITER, // DH_DYNAMIC_SAMPLE_PIECE_MSG,
  CONTROL_WRITER, // DH_DYNAMIC_SAMPLE_WHOLE_MSG,
  CONTROL_WRITER, // DH_ROLLUP_KEY_PIECE_MSG,
  CONTROL_WRITER, // DH_ROLLUP_KEY_WHOLE_MSG,
  CONTROL_WRITER, // DH_RANGE_DIST_WF_PIECE_MSG,
  CONTROL_WRITER, // DH_RANGE_DIST_WF_WHOLE_MSG,
  CONTROL_WRITER, // DH_INIT_CHANNEL_PIECE_MSG,
  CONTROL_WRITER, // DH_INIT_CHANNEL_WHOLE_MSG,
  CONTROL_WRITER, // DH_SECOND_STAGE_REPORTING_WF_PIECE_MSG,
  CONTROL_WRITER, // DH_SECOND_STAGE_REPORTING_WF_WHOLE_MSG,
  CONTROL_WRITER, // DH_OPT_STATS_GATHER_PIECE_MSG,
  CONTROL_WRITER, // DH_OPT_STATS_GATHER_WHOLE_MSG,
  VECTOR_WRITER,  //PX_VECTOR,
  VECTOR_FIXED_WRITER, //PX_FIXED_VECTOR
  VECTOR_ROW_WRITER,  //PX_VECTOR_ROW,
  CONTROL_WRITER, // DH_SP_WINFUNC_PX_PIECE_MSG
  CONTROL_WRITER, // DH_SP_WINFUNC_PX_WHOLE_MSG
  CONTROL_WRITER, // DH_RD_WINFUNC_PX_PIECE_MSG
  CONTROL_WRITER, // DH_RD_WINFUNC_PX_WHOLE_MSG
};

static_assert(ARRAYSIZEOF(msg_writer_map) == ObDtlMsgType::MAX, "invalid ms_writer_map size");

// 添加Encoder接口，方便broadcast的dtl channel agent和dtl channel采用该接口统一write msg逻辑
// 3种Encoder
// 1) 控制消息
// 2) ObRow消息
// 3) Array<ObExprs> 新引擎消息
class ObDtlChannelEncoder
{
public:
  virtual int write(const ObDtlMsg &msg, ObEvalCtx *eval_ctx, const bool is_eof) = 0;
  virtual int need_new_buffer(const ObDtlMsg &msg, ObEvalCtx *ctx, int64_t &need_size, bool &need_new) = 0;
  virtual void write_msg_type(ObDtlLinkedBuffer*) = 0;
  virtual int init(ObDtlLinkedBuffer *buffer, uint64_t tenant_id) = 0;
  virtual int serialize() = 0;

  virtual void reset() = 0;
  virtual int64_t used() = 0;
  virtual int64_t rows() = 0;
  virtual int64_t remain() = 0;
  virtual int handle_eof() = 0;
  virtual DtlWriterType type() = 0;
};


class ObDtlControlMsgWriter : public ObDtlChannelEncoder
{
public:
  ObDtlControlMsgWriter() : type_(CONTROL_WRITER), write_buffer_(nullptr)
  {}
  virtual DtlWriterType type() { return type_; }
  virtual int write(const ObDtlMsg &msg, ObEvalCtx *eval_ctx, const bool is_eof);
  virtual int need_new_buffer(
    const ObDtlMsg &msg, ObEvalCtx *ctx, int64_t &need_size, bool &need_new)
  {
    UNUSED(ctx);
    ObDtlMsgHeader header;
    need_size = header.get_serialize_size() + msg.get_serialize_size();
    need_new = nullptr == write_buffer_ || (write_buffer_->size() - write_buffer_->pos() < need_size);
    return common::OB_SUCCESS;
  }
  virtual void write_msg_type(ObDtlLinkedBuffer* buffer) { UNUSED(buffer); }
  virtual int init(ObDtlLinkedBuffer *buffer, uint64_t tenant_id)
  {
    UNUSED(tenant_id);
    write_buffer_ = buffer;
    return common::OB_SUCCESS;
  }
  virtual int serialize() { return common::OB_SUCCESS; }
  virtual void reset() { write_buffer_ = nullptr; }
  virtual int64_t used() { return write_buffer_->pos(); }
  virtual int64_t rows() { return 1; }
  virtual int64_t remain() { return write_buffer_->size() - write_buffer_->pos(); }
  int handle_eof() { return common::OB_SUCCESS; };
  DtlWriterType type_;
  ObDtlLinkedBuffer *write_buffer_;
};

class ObDtlRowMsgWriter : public ObDtlChannelEncoder
{
public:
  ObDtlRowMsgWriter();
  virtual ~ObDtlRowMsgWriter();

  virtual DtlWriterType type() { return type_; }
  int init(ObDtlLinkedBuffer *buffer, uint64_t tenant_id);
  void reset();

  int write(const ObDtlMsg &msg, ObEvalCtx *eval_ctx, const bool is_eof);
  int serialize();

  virtual int need_new_buffer(
    const ObDtlMsg &msg, ObEvalCtx *ctx, int64_t &need_size, bool &need_new);

  OB_INLINE int64_t used() { return block_->data_size(); }
  OB_INLINE int64_t rows() { return static_cast<int64_t>(block_->rows()); }
  OB_INLINE int64_t remain() { return block_->remain(); }
  int handle_eof() { return common::OB_SUCCESS; };

  virtual void write_msg_type(ObDtlLinkedBuffer* buffer)
  {
    buffer->msg_type() = ObDtlMsgType::PX_CHUNK_ROW;
  }
private:
  DtlWriterType type_;
  ObChunkRowStore row_store_;
  ObChunkRowStore::Block* block_;
  ObDtlLinkedBuffer *write_buffer_;
};

OB_INLINE int ObDtlRowMsgWriter::write(
  const ObDtlMsg &msg, ObEvalCtx *eval_ctx, const bool is_eof)
{
  int ret = OB_SUCCESS;
  UNUSED(eval_ctx);
  UNUSED(is_eof);
  const ObPxNewRow &px_row = static_cast<const ObPxNewRow&>(msg);
  const ObNewRow *row = px_row.get_row();
  if (nullptr != row) {
    if (OB_FAIL(row_store_.add_row(*row))) {
      SQL_DTL_LOG(WARN, "failed to add row", K(ret));
    }
    write_buffer_->pos() = used();
  } else {
    if (OB_FAIL(serialize())) {
      SQL_DTL_LOG(WARN, "failed to serialize", K(ret));
    }
    write_buffer_->is_eof() = is_eof;
    // 这里特殊处理，如果没有数据行，只有头部字节，也必须发送，但对于数据部分如果没有行，则不发送
    write_buffer_->pos() = used();
  }
  return ret;
}

class ObDtlDatumMsgWriter : public ObDtlChannelEncoder
{
public:
  ObDtlDatumMsgWriter();
  virtual ~ObDtlDatumMsgWriter();

  virtual DtlWriterType type() { return type_; }
  int init(ObDtlLinkedBuffer *buffer, uint64_t tenant_id);
  void reset();

  int write(const ObDtlMsg &msg, ObEvalCtx *eval_ctx, const bool is_eof);
  int serialize();

  int need_new_buffer(const ObDtlMsg &msg, ObEvalCtx *ctx, int64_t &need_size, bool &need_new);

  OB_INLINE int64_t used() { return block_->data_size(); }
  OB_INLINE int64_t rows() { return static_cast<int64_t>(block_->rows()); }
  OB_INLINE int64_t remain() { return block_->remain(); }
  int handle_eof() {
    int ret = common::OB_SUCCESS;
    if (NULL != register_block_buf_ptr_ && register_block_buf_ptr_->rows_ > 0) {
      block_->rows_ += register_block_buf_ptr_->rows_;
      *(block_->get_buffer()) =
           static_cast<ObChunkDatumStore::BlockBuffer &>(*register_block_buf_ptr_);
      write_buffer_->pos() = used();
      register_block_buf_ptr_->reset();
    }
    return ret;
  }
  void set_register_block_buf_ptr(ObChunkDatumStore::BlockBufferWrap *block_ptr)
  {
    register_block_buf_ptr_ = block_ptr;
  }
  void set_register_block_ptr(ObChunkDatumStore::Block **block_ptr)
  {
    register_block_ptr_ = block_ptr;
  }
  virtual void write_msg_type(ObDtlLinkedBuffer* buffer)
  {
    buffer->msg_type() = ObDtlMsgType::PX_DATUM_ROW;
  }
private:
  DtlWriterType type_;
  ObDtlLinkedBuffer *write_buffer_;
  ObChunkDatumStore::Block* block_;
  ObChunkDatumStore::Block** register_block_ptr_;
  ObChunkDatumStore::BlockBufferWrap* register_block_buf_ptr_;
  int write_ret_;
};

OB_INLINE int ObDtlDatumMsgWriter::write(
  const ObDtlMsg &msg, ObEvalCtx *eval_ctx, const bool is_eof)
{
  int ret = OB_SUCCESS;
  const ObPxNewRow &px_row = static_cast<const ObPxNewRow&>(msg);
  const ObIArray<ObExpr *> *row = px_row.get_exprs();
  if (nullptr != row) {
    if (OB_FAIL(block_->append_row(*row, eval_ctx, block_->get_buffer(), 0, nullptr, true,
                                   px_row.get_vector_row_idx()))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        SQL_DTL_LOG(WARN, "failed to add row", K(ret));
      } else {
        write_ret_ = OB_BUF_NOT_ENOUGH;
      }
    } else if (NULL != register_block_buf_ptr_) {
      *(static_cast<ObChunkDatumStore::BlockBuffer *>(register_block_buf_ptr_)) = *block_->get_buffer();
    }
    write_buffer_->pos() = used();
  } else {
    write_buffer_->is_eof() = is_eof;
    // 这里特殊处理，如果没有数据行，只有头部字节，也必须发送，但对于数据部分如果没有行，则不发送
    write_buffer_->pos() = used();
  }
  return ret;
}

class ObDtlVectorRowMsgWriter : public ObDtlChannelEncoder
{
public:
  ObDtlVectorRowMsgWriter();
  virtual ~ObDtlVectorRowMsgWriter();

  virtual DtlWriterType type() { return type_; }
  int init(ObDtlLinkedBuffer *buffer, uint64_t tenant_id);
  bool is_inited() const { return nullptr != write_buffer_; }
  int try_append_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx);
  int try_append_batch(const common::ObIArray<ObExpr*> &exprs,
                       const common::ObIArray<ObIVector *> &vectors,
                       ObEvalCtx &ctx, const uint16_t selector[],
                       const int64_t size, uint32_t row_size_arr[],
                       ObCompactRow **new_rows);
  void prefetch()
  {
    if (nullptr != block_buffer_) {
      __builtin_prefetch(block_buffer_->head(),
                               1, // for write
                               1); // low temporal locality
    }
  }
  void reset();

  int write(const ObDtlMsg &msg, ObEvalCtx *eval_ctx, const bool is_eof);
  int serialize();

  int need_new_buffer(const ObDtlMsg &msg, ObEvalCtx *ctx, int64_t &need_size, bool &need_new);

  OB_INLINE int64_t used() { return block_buffer_->head_size(); }
  OB_INLINE int64_t rows() { return block_->cnt_; }
  OB_INLINE int64_t remain() { return block_buffer_->remain(); }
  int handle_eof() {
    int ret = common::OB_SUCCESS;
    if (nullptr != write_buffer_) {
      write_buffer_->pos() = used();
    }
    return ret;
  }
  virtual void write_msg_type(ObDtlLinkedBuffer* buffer)
  {
    buffer->msg_type() = ObDtlMsgType::PX_VECTOR_ROW;
  }
  OB_INLINE ObTempRowStore::DtlRowBlock *get_block() { return block_; }
  OB_INLINE ObDtlLinkedBuffer *get_write_buffer() { return write_buffer_; }
private:
  DtlWriterType type_;
  ObDtlLinkedBuffer *write_buffer_;
  ObTempRowStore::DtlRowBlock *block_;
  ObTempRowStore::ShrinkBuffer *block_buffer_;
  RowMeta row_meta_;
  int64_t row_cnt_;
  int write_ret_;
};

OB_INLINE int ObDtlVectorRowMsgWriter::try_append_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(write_buffer_->get_row_meta().col_cnt_ <= 0)) {
    if (OB_FAIL(write_buffer_->get_row_meta().init(exprs, 0, false))) {
      SQL_DTL_LOG(WARN, "failed init row meta", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(row_meta_.col_cnt_ <= 0)) {
    if (OB_FAIL(row_meta_.init(exprs, 0, false))) {
      SQL_DTL_LOG(WARN, "failed init row meta", K(ret));
    }
  }
  ObCompactRow *new_row = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(block_->add_row(*block_buffer_, exprs, write_buffer_->get_row_meta(),
                                     ctx, new_row))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      SQL_DTL_LOG(WARN, "failed to add row", K(ret));
    } else {
      write_ret_ = OB_BUF_NOT_ENOUGH;
    }
  } else {
    ++row_cnt_;
  }
  write_buffer_->pos() = used();
  return ret;
}

OB_INLINE int ObDtlVectorRowMsgWriter::try_append_batch(const common::ObIArray<ObExpr*> &exprs,
                                                    const common::ObIArray<ObIVector *> &vectors,
                                                    ObEvalCtx &ctx, const uint16_t selector[],
                                                    const int64_t size, uint32_t row_size_arr[],
                                                    ObCompactRow **new_rows)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(write_buffer_->get_row_meta().col_cnt_ <= 0)) {
    if (OB_FAIL(write_buffer_->get_row_meta().init(exprs, 0, false))) {
      SQL_DTL_LOG(WARN, "failed init row meta", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(row_meta_.col_cnt_ <= 0)) {
    if (OB_FAIL(row_meta_.init(exprs, 0, false))) {
      SQL_DTL_LOG(WARN, "failed init row meta", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTempRowStore::RowBlock::calc_rows_size(vectors, write_buffer_->get_row_meta(),
                                                       selector, size, row_size_arr))) {
    SQL_DTL_LOG(WARN, "failed to calc size", K(ret));
  } else {
    int64_t sum_size = 0;
    for (int64_t i = 0; i < size; ++i) {
      sum_size += row_size_arr[i];
    }
    if (OB_FAIL(block_->add_batch(*block_buffer_, vectors, write_buffer_->get_row_meta(), selector,
                                  size, row_size_arr, sum_size,
                                  new_rows))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        SQL_DTL_LOG(WARN, "failed to add row", K(ret));
      } else {
        write_ret_ = OB_BUF_NOT_ENOUGH;
      }
    } else {
      row_cnt_ += size;
    }
    write_buffer_->pos() = used();
  }
  return ret;
}

OB_INLINE int ObDtlVectorRowMsgWriter::write(
  const ObDtlMsg &msg, ObEvalCtx *eval_ctx, const bool is_eof)
{
  int ret = OB_SUCCESS;
  const ObPxNewRow &px_row = static_cast<const ObPxNewRow&>(msg);
  const ObIArray<ObExpr *> *row = px_row.get_exprs();
  if (nullptr != row) {
    if (OB_FAIL(try_append_row(*row, *eval_ctx))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        SQL_DTL_LOG(WARN, "failed to append row", K(ret));
      }
    }
  } else {
    write_buffer_->is_eof() = is_eof;
    write_buffer_->pos() = used();
  }
  return ret;
}

class ObDtlVectorMsgWriter : public ObDtlChannelEncoder
{
public:
  ObDtlVectorMsgWriter();
  virtual ~ObDtlVectorMsgWriter();

  virtual DtlWriterType type() { return type_; }
  int init(ObDtlLinkedBuffer *buffer, uint64_t tenant_id);
  void reset();

  int write(const ObDtlMsg &msg, ObEvalCtx *eval_ctx, const bool is_eof);
  int serialize();

  int need_new_buffer(const ObDtlMsg &msg, ObEvalCtx *ctx, int64_t &need_size, bool &need_new);

  OB_INLINE int64_t used() { return block_buffer_->data_size(); }
  OB_INLINE int64_t rows() { return static_cast<int64_t>(block_->rows()); }
  OB_INLINE int64_t remain() { return block_buffer_->remain(); }
  int handle_eof() {
    int ret = common::OB_SUCCESS;
    if (nullptr != write_buffer_) {
      write_buffer_->pos() = used();
    }
    return ret;
  }
  virtual void write_msg_type(ObDtlLinkedBuffer* buffer)
  {
    buffer->msg_type() = ObDtlMsgType::PX_VECTOR;
  }
private:
  DtlWriterType type_;
  ObDtlLinkedBuffer *write_buffer_;
  ObDtlVectorsBlock *block_;
  ObDtlVectorsBuffer* block_buffer_;
  int write_ret_;
};

OB_INLINE int ObDtlVectorMsgWriter::write(
  const ObDtlMsg &msg, ObEvalCtx *eval_ctx, const bool is_eof)
{
  int ret = OB_SUCCESS;
  const ObPxNewRow &px_row = static_cast<const ObPxNewRow&>(msg);
  const ObIArray<ObExpr *> *row = px_row.get_exprs();
  if (nullptr != row) {
    if (OB_FAIL(block_buffer_->append_row(*row, static_cast<int32_t> (eval_ctx->get_batch_idx()), *eval_ctx))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        SQL_DTL_LOG(WARN, "failed to add row", K(ret));
      } else {
        write_ret_ = OB_BUF_NOT_ENOUGH;
        SQL_DTL_LOG(WARN, "to be remove, failed to add row", K(ret), K(used()), K(remain()), K(row->count()));
      }
    }
    write_buffer_->pos() = used();
  } else {
    write_buffer_->is_eof() = is_eof;
    // 这里特殊处理，如果没有数据行，只有头部字节，也必须发送，但对于数据部分如果没有行，则不发送
    write_buffer_->pos() = used();
  }
  return ret;
}

class ObDtlVectorFixedMsgWriter : public ObDtlChannelEncoder
{
public:
  ObDtlVectorFixedMsgWriter();
  virtual ~ObDtlVectorFixedMsgWriter();

  virtual DtlWriterType type() { return type_; }
  int init(ObDtlLinkedBuffer *buffer, uint64_t tenant_id);
  bool is_inited() const { return nullptr != write_buffer_; }
  void reset();
  OB_INLINE int64_t used() { return vector_buffer_.get_mem_used(); }
  OB_INLINE int64_t rows() { return vector_buffer_.get_row_cnt(); }
  OB_INLINE int64_t remain() { return vector_buffer_.get_mem_limit() - vector_buffer_.get_row_cnt(); }
  int write(const ObDtlMsg &msg, ObEvalCtx *eval_ctx, const bool is_eof);
  int append_row(const common::ObIArray<ObExpr*> &exprs, const int32_t batch_idx, ObEvalCtx &ctx)
  { return vector_buffer_.append_row(exprs, batch_idx, ctx); }
  int append_batch(const common::ObIArray<ObExpr*> &exprs, const ObIArray<ObIVector *> &vectors,
                   const uint16_t selector[], const int64_t size, ObEvalCtx &ctx)
  { return vector_buffer_.append_batch(exprs, vectors, selector, size, ctx); }
  void update_buffer_used() { write_buffer_->pos() = used(); }
  void update_write_ret() { write_ret_ = OB_BUF_NOT_ENOUGH; }
  void prefetch()
  {
    if (nullptr != write_buffer_) {
      for (int64_t i = 0; i < vector_buffer_.get_col_cnt(); ++i) {
        __builtin_prefetch(vector_buffer_.get_data(i),
                               1, // for write
                               1); // low temporal locality
      }
    }
  }
  int serialize();

  int need_new_buffer(const ObDtlMsg &msg, ObEvalCtx *ctx, int64_t &need_size, bool &need_new);
  int handle_eof() {
    int ret = common::OB_SUCCESS;
    if (nullptr != write_buffer_) {
      write_buffer_->pos() = used();
    }
    return ret;
  }
  virtual void write_msg_type(ObDtlLinkedBuffer* buffer)
  {
    buffer->msg_type() = ObDtlMsgType::PX_VECTOR_FIXED;
  }
private:
  DtlWriterType type_;
  ObDtlLinkedBuffer *write_buffer_;
  ObDtlVectors vector_buffer_;
  int write_ret_;
};

OB_INLINE int ObDtlVectorFixedMsgWriter::write(
  const ObDtlMsg &msg, ObEvalCtx *eval_ctx, const bool is_eof)
{
  int ret = OB_SUCCESS;
  const ObPxNewRow &px_row = static_cast<const ObPxNewRow&>(msg);
  const ObIArray<ObExpr *> *row = px_row.get_exprs();
  if (nullptr != row) {
    if (OB_FAIL(vector_buffer_.append_row(*row, static_cast<int32_t> (eval_ctx->get_batch_idx()), *eval_ctx))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        SQL_DTL_LOG(WARN, "failed to add row", K(ret));
      } else {
        write_ret_ = OB_BUF_NOT_ENOUGH;
      }
    }
    write_buffer_->pos() = used();
  } else {
    if (!vector_buffer_.is_inited() && OB_FAIL(vector_buffer_.init())) {
      SQL_DTL_LOG(WARN, "failed to init buffer", K(ret));
    } else {
      write_buffer_->is_eof() = is_eof;
      // 这里特殊处理，如果没有数据行，只有头部字节，也必须发送，但对于数据部分如果没有行，则不发送
      write_buffer_->pos() = used();
    }
  }
  return ret;
}

class SendMsgResponse
{
public:
  SendMsgResponse();
  virtual ~SendMsgResponse();

  int init();
  bool is_init() { return inited_; }

  bool is_in_process() const { return in_process_; }
  int start();
  int on_start_fail();
  int on_finish(const bool is_block, const int return_code);
  // wait async rpc finish and return ret_
  int wait();
  int is_block() { return is_block_; }
  void reset_block() { is_block_ = false; }
  void set_id(uint64_t id) { ch_id_ = id; }
  uint64_t get_id() { return ch_id_; }

  TO_STRING_KV(KP_(inited), K_(ret));
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(SendMsgResponse);
  bool inited_;
  int ret_;
  bool in_process_;
  bool finish_;
  bool is_block_;
  common::ObThreadCond cond_;
  uint64_t ch_id_;
};

// Rpc channel is "rpc version" of channel. As the name explained,
// this kind of channel will do exchange between two tasks by using
// rpc calls.
class ObDtlBasicChannel
    : public ObDtlChannel
{
  friend class ObDtlChanAgent;
public:
  explicit ObDtlBasicChannel(const uint64_t tenant_id,
     const uint64_t id, const common::ObAddr &peer, DtlChannelType type);
  explicit ObDtlBasicChannel(const uint64_t tenant_id,
     const uint64_t id, const common::ObAddr &peer, const int64_t hash_val, DtlChannelType type);
  virtual ~ObDtlBasicChannel();

  class ObDtlChannelBlockProc : public ObIDltChannelLoopPred
  {
  public:
    void set_ch_idx_var(int64_t *chan_idx) { chan_idx_ = chan_idx; }
    virtual bool pred_process(int64_t idx, ObDtlChannel *chan) override
    {
      UNUSED(chan);
      return *chan_idx_ == idx;
    }
    int64_t *chan_idx_;
  };

  int init() override;
  void destroy();

  virtual int send(const ObDtlMsg &msg, int64_t timeout_ts,
      ObEvalCtx *eval_ctx = nullptr, bool is_eof = false) override;
  virtual int feedup(ObDtlLinkedBuffer *&buffer) override;
  virtual int attach(ObDtlLinkedBuffer *&linked_buffer, bool inc_recv_buf_cnt = true);
  // don't call send&flush in different threads.
  virtual int flush(bool wait=true, bool wait_response = true) override;

  virtual int send_message(ObDtlLinkedBuffer *&buf);

  virtual int process1(
      ObIDtlChannelProc *proc,
      int64_t timeout, bool &last_row_in_buffer) override;
  virtual int send1(
      std::function<int(const ObDtlLinkedBuffer &buffer)> &proc,
      int64_t timeout) override;

  bool is_empty() const override;

  virtual int64_t get_peer_id() const { return peer_id_; }
  virtual uint64_t get_tenant_id() const { return tenant_id_; }
  virtual int64_t get_hash_val() const { return hash_val_; }

  int wait_unblocking_if_blocked();
  int block_on_increase_size(int64_t size);
  int unblock_on_decrease_size(int64_t size);
  bool belong_to_receive_data();
  bool belong_to_transmit_data();
  virtual int clear_response_block();
  virtual int wait_response();
  void inc_msg_seq_no() { ++seq_no_; }
  int64_t get_msg_seq_no() { return seq_no_; }
  void inc_send_buffer_cnt() { ++send_buffer_cnt_; }
  void inc_recv_buffer_cnt() { ++recv_buffer_cnt_; }
  void inc_processed_buffer_cnt() { ++processed_buffer_cnt_; }
  int64_t get_send_buffer_cnt() { return send_buffer_cnt_; }
  int64_t get_recv_buffer_cnt() { return recv_buffer_cnt_; }
  int64_t get_processed_buffer_cnt() { return processed_buffer_cnt_; }

  int get_processed_buffer(int64_t timeout);
  virtual int clean_recv_list ();
  void clean_broadcast_buffer();

  // Only DTL use unblock logic for merge sort coord
  inline bool has_less_buffer_cnt()
  {
    return recv_buffer_cnt_ - processed_buffer_cnt_ <= MAX_BUFFER_CNT;
  }
  int push_back_send_list(ObDtlLinkedBuffer *buffer);

  void set_dfc_idx(int64_t idx) { dfc_idx_ = idx; }

  int switch_writer(const ObDtlMsg &msg);

  int mock_eof_buffer(int64_t timeout_ts);
  ObDtlLinkedBuffer *alloc_buf(const int64_t payload_size);
  
  void set_bc_service(ObDtlBcastService *bc_service) { bc_service_ = bc_service; }

  ObDtlDatumMsgWriter &get_datum_writer() { return datum_msg_writer_; }
  ObDtlVectorRowMsgWriter &get_vector_row_writer() { return vector_row_msg_writer_; }
  ObDtlVectorMsgWriter &get_vector_msg_writer() { return vector_msg_writer_; }
  ObDtlVectorFixedMsgWriter &get_vector_fixed_msg_writer() { return vector_fixed_msg_writer_; }
  virtual int push_buffer_batch_info() override;
  void switch_msg_type(const ObDtlMsg &msg);

  TO_STRING_KV(KP_(id), K_(peer));
protected:
  int push_back_send_list();
  int wait_unblocking();
  int switch_buffer(const int64_t min_size, const bool is_eof,
      const int64_t timeout_ts, ObEvalCtx *eval_ctx);
  int write_msg(const ObDtlMsg &msg, int64_t timeout_ts,
      ObEvalCtx *eval_ctx, bool is_eof);
  int inner_write_msg(const ObDtlMsg &msg, int64_t timeout_ts, ObEvalCtx *eval_ctx, bool is_eof);

  void free_buf(ObDtlLinkedBuffer *buf);

  int send_buffer(ObDtlLinkedBuffer *&buffer);

  SendMsgResponse *get_msg_response() { return &msg_response_; }

  OB_INLINE virtual bool has_msg() { return recv_buffer_cnt_ > processed_buffer_cnt_; }

  virtual void reset_px_row_iterator() { datum_iter_.reset(); }
protected:
  bool is_inited_;
  const uint64_t local_id_;
  const int64_t peer_id_;
  ObSimpleLinkQueue send_list_;
  ObDtlLinkedBuffer *write_buffer_;
  common::ObSpLinkQueue recv_list_;
  ObDtlLinkedBuffer *process_buffer_;
  SimpleCond send_sem_;
  SimpleCond recv_sem_;
  common::ObSpLinkQueue free_list_;

  SendMsgResponse msg_response_;
  bool alloc_new_buf_;

  // some statistics
  int64_t seq_no_;
  int64_t send_buffer_cnt_;
  int64_t recv_buffer_cnt_;
  int64_t processed_buffer_cnt_;
  uint64_t tenant_id_;
  bool is_data_msg_;
  bool use_crs_writer_;
  int64_t hash_val_;
  int64_t dfc_idx_;
  int64_t got_from_dtl_cache_;

  ObDtlControlMsgWriter ctl_msg_writer_;
  ObDtlRowMsgWriter row_msg_writer_;
  ObDtlDatumMsgWriter datum_msg_writer_;
  ObDtlVectorRowMsgWriter vector_row_msg_writer_;
  ObDtlVectorMsgWriter vector_msg_writer_;
  ObDtlVectorFixedMsgWriter vector_fixed_msg_writer_;
  ObDtlChannelEncoder *msg_writer_;
  // row/datum store iterator for interm result iteration.
  ObChunkDatumStore::Iterator datum_iter_;

  ObDtlBcastService *bc_service_;

  ObDtlChannelBlockProc block_proc_;
  static const int64_t MAX_BUFFER_CNT = 2;
public:
  //TODO delete muhang
  int64_t times_;
  int64_t write_buf_use_time_;
  int64_t send_use_time_;
  int64_t msg_count_;
  dtl::ObDTLIntermResultInfoGuard result_info_guard_;
};

OB_INLINE bool ObDtlBasicChannel::is_empty() const
{
  return send_list_.is_empty();
}


OB_INLINE bool ObDtlBasicChannel::belong_to_transmit_data()
{
  return nullptr != dfc_ && dfc_->is_transmit();
}

OB_INLINE bool ObDtlBasicChannel::belong_to_receive_data()
{
  return nullptr != dfc_ && dfc_->is_receive();
}

}  // dtl
}  // sql
}  // oceanbase

#endif /* OB_DTL_BASIC_CHANNEL_H */
