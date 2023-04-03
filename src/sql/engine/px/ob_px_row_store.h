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

#ifndef _OB_SQL_ENGINE_PX_NEW_ROW_H_
#define _OB_SQL_ENGINE_PX_NEW_ROW_H_

#include "lib/allocator/ob_allocator.h"
#include "common/row/ob_row.h"
#include "common/object/ob_object.h"
#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl_msg_type.h"
#include "sql/dtl/ob_dtl_processor.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase
{
namespace sql
{

class ObReceiveRowReader
{
public:
  ObReceiveRowReader() :
      recv_head_(NULL),
      recv_tail_(NULL),
      iterated_buffers_(NULL),
      cur_iter_pos_(0),
      cur_iter_rows_(0),
      recv_list_rows_(0),
      datum_iter_(NULL),
      row_iter_(NULL)
  {
  }
  ~ObReceiveRowReader()
  {
    reset();
  }

  int add_buffer(dtl::ObDtlLinkedBuffer &buf, bool &transferred);

  bool has_more() const
  {
    return (recv_list_rows_ > cur_iter_rows_)
        || (NULL != datum_iter_ && datum_iter_->is_valid() && datum_iter_->has_next())
        || (NULL != row_iter_ && row_iter_->is_valid() && row_iter_->has_next());
  }

  // return left rows for non interm result.
  // For interm result (%datum_iter_ or %row_iter_ not null):
  //   return 0 for no more rows, return INT64_MAX for has more rows.
  int64_t left_rows() const
  {
    int64_t rows = 0;
    if (NULL != datum_iter_) {
      rows = (datum_iter_->is_valid() && datum_iter_->has_next()) ? INT64_MAX : 0;
    } else if (OB_UNLIKELY(NULL != row_iter_)) {
      rows = (row_iter_->is_valid() && row_iter_->has_next()) ? INT64_MAX : 0;
    } else {
      rows = recv_list_rows_ - cur_iter_rows_;
    }
    return rows;
  }

  static int to_expr(const ObChunkDatumStore::StoredRow *srow,
                     const ObIArray<ObExpr*> &dynamic_const_exprs,
                     const ObIArray<ObExpr*> &exprs,
                     ObEvalCtx &eval_ctx);

  static int attach_rows(const common::ObIArray<ObExpr*> &exprs,
                          const ObIArray<ObExpr*> &dynamic_const_exprs,
                          ObEvalCtx &eval_ctx,
                          const ObChunkDatumStore::StoredRow **srows,
                          const int64_t read_rows);

  // get row interface for PX_CHUNK_ROW
  int get_next_row(common::ObNewRow &row);

  // get row interface for PX_DATUM_ROW
  int get_next_row(const ObIArray<ObExpr*> &exprs,
                   const ObIArray<ObExpr*> &dynamic_const_exprs,
                   ObEvalCtx &eval_ctx);

  // get next batch rows
  // set read row count to %read_rows
  // return OB_ITER_END and set %read_rows to zero for iterate end.
  int get_next_batch(const ObIArray<ObExpr*> &exprs,
                     const ObIArray<ObExpr*> &dynamic_const_exprs,
                     ObEvalCtx &eval_ctx,
                     const int64_t max_rows, int64_t &read_rows,
                     const ObChunkDatumStore::StoredRow **srows);

  void reset();

private:
  template <typename BLOCK, typename ROW>
  // return NULL for iterate end.
  const ROW *next_store_row();

  void move_to_iterated(const int64_t rows);
  void free(dtl::ObDtlLinkedBuffer *buf);
  inline void free_iterated_buffers()
  {
    if (NULL != iterated_buffers_) {
      free_buffer_list(iterated_buffers_);
      iterated_buffers_ = NULL;
    }
  }
  void free_buffer_list(dtl::ObDtlLinkedBuffer *buf);

private:
  dtl::ObDtlLinkedBuffer *recv_head_;
  dtl::ObDtlLinkedBuffer *recv_tail_;

  dtl::ObDtlLinkedBuffer *iterated_buffers_;

  int64_t cur_iter_pos_;
  int64_t cur_iter_rows_;
  int64_t recv_list_rows_;

  // store iterator for interm result iteration.
  ObChunkDatumStore::Iterator *datum_iter_;
  ObChunkRowStore::Iterator *row_iter_;
};

class ObPxNewRow
  : public dtl::ObDtlMsgTemp<dtl::ObDtlMsgType::PX_NEW_ROW>
{
  OB_UNIS_VERSION_V(1);
public:
  // for deserialize
  ObPxNewRow()
    : des_row_buf_(nullptr),
      des_row_buf_size_(0),
      row_(nullptr),
      exprs_(nullptr),
      row_cell_count_(0),
      type_(dtl::ObDtlMsgType::PX_NEW_ROW) {}
  // for serialize
  ObPxNewRow(const common::ObNewRow &row)
    : des_row_buf_(nullptr),
      des_row_buf_size_(0),
      row_(&row),
      exprs_(nullptr),
      row_cell_count_(row.get_count()),
      type_(dtl::ObDtlMsgType::PX_CHUNK_ROW)
      {}
  ObPxNewRow(const common::ObIArray<ObExpr*> &exprs)
    : des_row_buf_(nullptr),
      des_row_buf_size_(0),
      row_(nullptr),
      exprs_(&exprs),
      row_cell_count_(exprs.count()),
      type_(dtl::ObDtlMsgType::PX_DATUM_ROW)
      {}
  ~ObPxNewRow() { }
  void set_eof_row();
  void reset() {}

  OB_INLINE const common::ObNewRow* get_row() const { return row_; }
  OB_INLINE const common::ObIArray<ObExpr*>* get_exprs() const { return exprs_; }
  int deep_copy(common::ObIAllocator &alloc, const ObPxNewRow &other);
  int get_row_from_serialization(ObNewRow &row);
  inline dtl::ObDtlMsgType get_data_type() const
  { return type_; }
  inline void set_data_type(const dtl::ObDtlMsgType type)
  {  type_ = type; }
  TO_STRING_KV(K_(row_cell_count), K_(des_row_buf_size));
private:
  static const int64_t EOF_ROW_FLAG = -1;
  char *des_row_buf_; // 反序列化时用于指向 row_ 的序列化内容
  int64_t des_row_buf_size_; // 反序列化时用于记录 row_ 的序列化内容的 buffer 长度，get_row 时需要参考
  const common::ObNewRow *row_; // 序列化之前传入 row_，用于序列化
  const common::ObIArray<ObExpr*> *exprs_;
  int64_t row_cell_count_; // row_cell_count_ 取特殊值 -1 时表示 EOFRow，get_row 返回 OB_ITER_END
  dtl::ObDtlMsgType type_;
  DISALLOW_COPY_AND_ASSIGN(ObPxNewRow);
};
}
}
#endif /* _OB_SQL_ENGINE_PX_NEW_ROW_H_ */
//// end of header file

