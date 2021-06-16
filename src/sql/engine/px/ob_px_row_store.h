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

namespace oceanbase {
namespace sql {

class ObDtlMsgReader : public dtl::ObDtlMsgIterator {
public:
  virtual void set_iterator_end() = 0;
  virtual bool has_next() = 0;

  virtual void reset() = 0;
  virtual bool is_inited() = 0;
  virtual bool is_iter_end() = 0;
  virtual int load_buffer(const dtl::ObDtlLinkedBuffer& buffer) = 0;
  virtual void set_end() = 0;
};

class ObPxNewRowIterator : public ObDtlMsgReader {
public:
  ObPxNewRowIterator();
  virtual ~ObPxNewRowIterator();

  int get_next_row(common::ObNewRow& row);
  int get_next_row(const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx)
  {
    UNUSED(exprs);
    UNUSED(eval_ctx);
    return common::OB_ERR_UNEXPECTED;
  }
  void reset();

  bool is_inited()
  {
    return is_inited_;
  }
  void set_inited()
  {
    is_inited_ = true;
  }
  bool has_next()
  {
    return rows_ > 0 && row_store_it_.has_next();
  }
  bool is_eof()
  {
    return is_eof_;
  }

  bool is_iter_end()
  {
    return is_iter_end_;
  }

  int64_t get_row_cnt()
  {
    return row_store_.get_row_cnt();
  }

  void set_iterator_end();
  int load_buffer(const dtl::ObDtlLinkedBuffer& buffer);
  void set_rows(int64_t rows)
  {
    rows_ = rows;
  }
  ObChunkRowStore::Iterator& get_row_store_it()
  {
    return row_store_it_;
  }
  void set_end() override;

private:
  bool is_eof_;
  bool is_iter_end_;
  int64_t rows_;
  ObChunkRowStore row_store_;
  ObChunkRowStore::Iterator row_store_it_;
  bool is_inited_;
};

class ObPxDatumRowIterator : public ObDtlMsgReader {
public:
  ObPxDatumRowIterator();
  virtual ~ObPxDatumRowIterator();

  int get_next_row(const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx);
  int get_next_row(common::ObNewRow& row)
  {
    UNUSED(row);
    return common::OB_ERR_UNEXPECTED;
  }
  void reset();

  bool is_inited()
  {
    return is_inited_;
  }
  void set_inited()
  {
    is_inited_ = true;
  }
  bool has_next()
  {
    return rows_ > 0 && datum_store_it_.has_next();
  }
  bool is_eof()
  {
    return is_eof_;
  }

  bool is_iter_end()
  {
    return is_iter_end_;
  }

  int64_t get_row_cnt()
  {
    return datum_store_.get_row_cnt();
  }

  void set_iterator_end();
  int load_buffer(const dtl::ObDtlLinkedBuffer& buffer);
  void set_rows(int64_t rows)
  {
    rows_ = rows;
  }
  ObChunkDatumStore::Iterator& get_row_store_it()
  {
    return datum_store_it_;
  }
  void set_end() override;

private:
  bool is_eof_;
  bool is_iter_end_;
  int64_t rows_;
  ObChunkDatumStore datum_store_;
  ObChunkDatumStore::Iterator datum_store_it_;
  bool is_inited_;
};

class ObPxNewRow : public dtl::ObDtlMsgTemp<dtl::ObDtlMsgType::PX_NEW_ROW> {
  OB_UNIS_VERSION_V(1);

public:
  // for deserialize
  ObPxNewRow()
      : des_row_buf_(nullptr),
        des_row_buf_size_(0),
        row_(nullptr),
        exprs_(nullptr),
        row_cell_count_(0),
        iter_(nullptr),
        type_(dtl::ObDtlMsgType::PX_NEW_ROW)
  {}
  // for serialize
  ObPxNewRow(const common::ObNewRow& row)
      : des_row_buf_(nullptr),
        des_row_buf_size_(0),
        row_(&row),
        exprs_(nullptr),
        row_cell_count_(row.get_count()),
        iter_(nullptr),
        type_(dtl::ObDtlMsgType::PX_CHUNK_ROW)
  {}
  ObPxNewRow(const common::ObIArray<ObExpr*>& exprs)
      : des_row_buf_(nullptr),
        des_row_buf_size_(0),
        row_(nullptr),
        exprs_(&exprs),
        row_cell_count_(exprs.count()),
        iter_(nullptr),
        type_(dtl::ObDtlMsgType::PX_DATUM_ROW)
  {}
  ~ObPxNewRow()
  {}
  void set_eof_row();
  void reset()
  {}
  virtual OB_INLINE void set_iter(dtl::ObDtlMsgIterator* iter)
  {
    iter_ = iter;
  }
  virtual OB_INLINE bool has_iter()
  {
    return nullptr != iter_;
  }
  virtual OB_INLINE bool has_next()
  {
    return nullptr != iter_ && iter_->has_next();
  }
  virtual OB_INLINE void set_iterator_end()
  {
    if (nullptr != iter_) {
      iter_->set_iterator_end();
    }
  }

  OB_INLINE const common::ObNewRow* get_row() const
  {
    return row_;
  }
  OB_INLINE const common::ObIArray<ObExpr*>* get_exprs() const
  {
    return exprs_;
  }
  virtual int get_row(common::ObNewRow& row);
  virtual int get_next_row(const ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx);
  int deep_copy(common::ObIAllocator& alloc, const ObPxNewRow& other);
  int get_row_from_serialization(ObNewRow& row);
  inline dtl::ObDtlMsgType get_data_type() const
  {
    return type_;
  }
  inline void set_data_type(const dtl::ObDtlMsgType type)
  {
    type_ = type;
  }
  TO_STRING_KV(K_(row_cell_count), K_(des_row_buf_size));

private:
  static const int64_t EOF_ROW_FLAG = -1;
  char* des_row_buf_;
  int64_t des_row_buf_size_;
  const common::ObNewRow* row_;
  const common::ObIArray<ObExpr*>* exprs_;
  // When row_cell_count_ takes a special value of -1, it means EOFRow, get_row returns OB_ITER_END
  int64_t row_cell_count_;
  dtl::ObDtlMsgIterator* iter_;
  dtl::ObDtlMsgType type_;
  DISALLOW_COPY_AND_ASSIGN(ObPxNewRow);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_SQL_ENGINE_PX_NEW_ROW_H_ */
//// end of header file
