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

#ifndef OCEANBASE_BASIC_OB_DATUM_STORE2_H_
#define OCEANBASE_BASIC_OB_DATUM_STORE2_H_

#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/list/ob_dlist.h"
#include "common/row/ob_row.h"
#include "common/row/ob_row_iterator.h"
#include "share/datum/ob_datum.h"
#include "sql/engine/expr/ob_expr.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "sql/engine/basic/ob_sql_mem_callback.h"
#include "sql/engine/basic/ob_batch_result_holder.h"

namespace oceanbase
{
namespace sql
{

class ObIOEventObserver;
// Random access row store, support disk store.
// All row must have same cell count and  projector.
class ObChunkDatumStore
{
  OB_UNIS_VERSION_V(1);
public:
  static inline int deep_copy_unswizzling(
      const ObDatum &src, ObDatum *dst, char *buf, int64_t max_size, int64_t &pos)
  {
    int ret = OB_SUCCESS;
    *dst = src;
    if (!dst->null_) {
      if (pos + dst->len_ > max_size) {
        ret = OB_BUF_NOT_ENOUGH;
      } else {
        MEMCPY(buf + pos, src.ptr_, dst->len_);
        dst->ptr_ = reinterpret_cast<const char *>(pos); // do unswizzling : set offset to ptr directly
        pos += dst->len_;
      }
    }
    return ret;
  }
  /*
   * StoredRow内存编排
   * 前面N个Datum + 中间是extend_size(可能为0) + 后面是真正的数据data
   * | datum1 | datum2 | ... | datumN | extend_size = 0 | data1 | data2 | ... | dataN |
   *     |__________________________________________________|^      ^              ^
   *              |_________________________________________________|              |
   *                             |_________________________________________________|
   */
  struct StoredRow
  {
    StoredRow() : cnt_(0), row_size_(0) {}
    // Build a stored row by exprs.
    // @param [out] sr, result stored row
    // @param epxrs,
    // @param ctx
    // @param buf
    // @param buf_len, use Block::row_store_size() to detect the needed buffer size.
    // @param extra_size, extra store size
    // @param unswizzling
    // @return OB_SUCCESS or OB_BUF_NOT_ENOUGH if buf not enough
    static int build(StoredRow *&sr,
                     const ObExprPtrIArray &exprs,
                     ObEvalCtx &ctx,
                     char *buf,
                     const int64_t buf_len,
                     const uint32_t extra_size = 0,
                     const bool unswizzling = false);
    static int build(StoredRow *&sr,
                     const ObExprPtrIArray &exprs,
                     ObEvalCtx &ctx,
                     common::ObIAllocator &alloc,
                     const uint32_t extra_size = 0);

    //shadow_datums means that the memory of datum may come from other datum or ObObj
    //the memory of shadow_datums is not continuous,
    //so you cannot directly copy the memory of the entire datum array,
    //and you should make a deep copy of each datum in turn
    OB_INLINE int copy_shadow_datums(const common::ObDatum *datums, const int64_t cnt,
      char *buf, const int64_t size, const int64_t row_size, const uint32_t row_extend_size);
    template <bool fill_invariable_res_buf = false>
    int to_expr(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx) const;
    int to_expr(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, int64_t count) const;
    inline common::ObDatum *cells() { return reinterpret_cast<common::ObDatum *>(payload_); }
    inline const common::ObDatum *cells() const
        { return reinterpret_cast<const common::ObDatum *>(payload_); }
    inline void *get_extra_payload() const
    { return static_cast<void*>(const_cast<char*>(payload_ + sizeof(ObDatum) * cnt_)); }
    template <typename T>
    inline T &extra_payload() const { return *static_cast<T *>(get_extra_payload()); };
    int assign(const StoredRow *sr);
    int set_null(int64_t nth_col);
    void unswizzling(char *base = NULL);
    static void unswizzling_datum(common::ObDatum *datum, uint32_t cnt, char *base);
    void swizzling(char *base = NULL);
    TO_STRING_KV(K_(cnt), K_(row_size), "cells",
        common::ObArrayWrap<common::ObDatum>(cells(), cnt_));

  private:
    template <bool UNSWIZZLING>
    static int do_build(StoredRow *&sr,
                        const ObExprPtrIArray &exprs,
                        ObEvalCtx &ctx,
                        char *buf,
                        const int64_t buf_len,
                        const uint32_t extra_size);

  public:
    uint32_t cnt_;
    uint32_t row_size_;
    char payload_[0];
  } __attribute__((packed));

  /**
   * 考虑到很多场景都会临时保存下上一行数据，所以写一个class提供这种方式
   * 如 Sort、MergeDisintct等
   * 功能：
   *    1）提供从ObIArray<ObExpr*> 到StoredRow的存储
   *    2）提供reuse模式，内存可以反复利用
   *    3）提供从StoredRow到ObIArray<ObExpr*>的转换
   */
  class LastStoredRow
  {
  public:
    LastStoredRow(ObIAllocator &alloc)
      : store_row_(nullptr), alloc_(alloc), max_size_(0), reuse_(true),
       pre_alloc_row1_(nullptr), pre_alloc_row2_(nullptr)
    {}
    ~LastStoredRow() {}

    int save_store_row(const common::ObIArray<ObExpr*> &exprs,
      ObEvalCtx &ctx,
      const int64_t extra_size = 0)
    {
      int ret = OB_SUCCESS;
      bool reuse = reuse_;
      char *buf = NULL;
      int64_t row_size = 0;
      int64_t buffer_len = 0;
      StoredRow *new_row = NULL;
      if (OB_UNLIKELY(0 == exprs.count())) {
        // 没有任何列
        // 场景：如distinct 1，对于常量，不会有任何处理
      } else if (OB_UNLIKELY(extra_size < 0 || extra_size > INT32_MAX)) {
        ret = OB_INVALID_ARGUMENT;
        SQL_ENG_LOG(ERROR, "invalid extra size", K(ret), K(extra_size));
      } else if (OB_FAIL(ObChunkDatumStore::row_copy_size(exprs, ctx, row_size))) {
        SQL_ENG_LOG(WARN, "failed to calc copy size", K(ret));
      } else {
        int64_t head_size = sizeof(StoredRow);
        reuse = OB_ISNULL(store_row_) ? false : reuse &&
              (max_size_ >= row_size + head_size + extra_size);
        if (reuse && OB_NOT_NULL(store_row_)) {
          //switch buffer for write
          if (store_row_ != pre_alloc_row1_ && store_row_ != pre_alloc_row2_) {
            ret = OB_ERR_UNEXPECTED;
            SQL_ENG_LOG(ERROR, "unexpected status: store_row is invalid", K(ret),
              K(store_row_), K(pre_alloc_row1_), K(pre_alloc_row2_));
          } else {
            store_row_ = (store_row_ == pre_alloc_row1_ ? pre_alloc_row2_ : pre_alloc_row1_);
            buf = reinterpret_cast<char*>(store_row_);
            new_row = store_row_;
            buffer_len = max_size_;
          }
        } else {
          //alloc 2 buffer with same length
          buffer_len = (!reuse_ ? row_size : row_size * 2) + head_size + extra_size;
          char *buf1 = nullptr;
          char *buf2 = nullptr;
          if (OB_ISNULL(buf1 = reinterpret_cast<char*>(alloc_.alloc(buffer_len)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SQL_ENG_LOG(ERROR, "alloc buf failed", K(ret));
          } else if (OB_ISNULL(buf2 = reinterpret_cast<char*>(alloc_.alloc(buffer_len)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SQL_ENG_LOG(ERROR, "alloc buf failed", K(ret));
          } else if (OB_ISNULL(pre_alloc_row1_ = new(buf1)StoredRow())) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SQL_ENG_LOG(ERROR, "failed to new row", K(ret));
          } else if (OB_ISNULL(pre_alloc_row2_ = new(buf2)StoredRow())) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SQL_ENG_LOG(ERROR, "failed to new row", K(ret));
          } else {
            buf = buf1;
            new_row = pre_alloc_row1_;
          }
        }
        if (OB_SUCC(ret)) {
          int64_t pos = head_size;
          if (OB_FAIL(StoredRow::build(store_row_, exprs, ctx, buf, buffer_len, static_cast<int32_t>(extra_size)))) {
            SQL_ENG_LOG(WARN, "failed to build stored row", K(ret), K(buffer_len), K(row_size));
          } else {
            max_size_ = buffer_len;
          }
        }
      }
      return ret;
    }
    int save_store_row(const ObChunkDatumStore::StoredRow &row,
                       const int64_t extra_size = 0)
    {
      int ret = OB_SUCCESS;
      bool reuse = reuse_;
      char *buf = NULL;
      int64_t buffer_len = 0;
      StoredRow *new_row = NULL;
      int64_t row_size = row.row_size_;
      int64_t head_size = sizeof(StoredRow);
      reuse = OB_ISNULL(store_row_) ? false : reuse &&
            (max_size_ >= row_size + head_size + extra_size);
      if (reuse && OB_NOT_NULL(store_row_)) {
        //switch buffer for write
        store_row_ = (store_row_ == pre_alloc_row1_ ? pre_alloc_row2_ : pre_alloc_row1_);
        buf = reinterpret_cast<char*>(store_row_);
        new_row = store_row_;
        buffer_len = max_size_;
      } else {
        //alloc 2 buffer with same length
        buffer_len = (!reuse_ ? row_size : row_size * 2) + head_size + extra_size;
        char *buf1 = nullptr;
        char *buf2 = nullptr;
        if (OB_ISNULL(buf1 = reinterpret_cast<char*>(alloc_.alloc(buffer_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_ENG_LOG(ERROR, "alloc buf failed", K(ret));
        } else if (OB_ISNULL(buf2 = reinterpret_cast<char*>(alloc_.alloc(buffer_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_ENG_LOG(ERROR, "alloc buf failed", K(ret));
        } else if (OB_ISNULL(pre_alloc_row1_ = new(buf1)StoredRow())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_ENG_LOG(ERROR, "failed to new row", K(ret));
        } else if (OB_ISNULL(pre_alloc_row2_ = new(buf2)StoredRow())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_ENG_LOG(ERROR, "failed to new row", K(ret));
        } else {
          buf = buf1;
          new_row = pre_alloc_row1_;
        }
      }
      if (OB_SUCC(ret)) {
        int64_t pos = head_size;
        if (OB_FAIL(new_row->assign(&row))) {
          SQL_ENG_LOG(WARN, "stored row assign failed", K(ret));
        } else {
          max_size_ = buffer_len;
          store_row_ = new_row;
        }
      }
      return ret;
    }

    void reset()
    {
      store_row_ = nullptr;
      max_size_ = 0;
    }

    TO_STRING_KV(K_(max_size), K_(reuse), KPC_(store_row));
    StoredRow *store_row_;
    ObIAllocator &alloc_;
    int64_t max_size_;
    bool reuse_;
    private:
    //To avoid writing memory overwrite, alloc 2 row for alternate writing
    StoredRow *pre_alloc_row1_;
    StoredRow *pre_alloc_row2_;
  };

  class ShadowStoredRow
  {
  public:
    ShadowStoredRow() : alloc_(nullptr),
                        store_row_(nullptr),
                        saved_(false)
    {}
    virtual ~ShadowStoredRow() { reset(); }

    bool is_saved() const { return saved_; }

    int init(common::ObIAllocator &allocator, int64_t datum_cnt)
    {
      int ret = common::OB_SUCCESS;
      int64_t buffer_len = datum_cnt * sizeof(ObDatum) + sizeof(StoredRow);
      const int64_t row_size = datum_cnt * sizeof(ObDatum) + sizeof(StoredRow);
      char *buf = nullptr;
      if (nullptr != store_row_) {
        // inited
        // reuse
      } else if (NULL != alloc_) {
        ret = common::OB_INIT_TWICE;
        SQL_ENG_LOG(WARN, "init twice", K(ret));
      } else if (row_size < 0 || row_size > INT32_MAX) {
        ret = OB_INVALID_ARGUMENT;
        SQL_ENG_LOG(WARN, "invalid row_size", K(ret), K(datum_cnt), K(row_size));
      } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buffer_len)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(WARN, "alloc buf failed", K(ret));
      } else {
        alloc_ = &allocator;
        store_row_ = new (buf) StoredRow();
        store_row_->cnt_ = static_cast<int32_t>(datum_cnt);
        store_row_->row_size_ = static_cast<int32_t>(row_size);
        saved_ = false;
      }
      return ret;
    }
    // copy exprs to store row
    virtual int shadow_copy(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx)
    {
      int ret = common::OB_SUCCESS;
      if (OB_ISNULL(store_row_) || OB_UNLIKELY(store_row_->cnt_ != exprs.count())) {
        ret = common::OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "NULL datums or count mismatch", K(ret),
                    KPC(store_row_), K(exprs.count()));
      } else {
        ObDatum *datum = nullptr;
        ObDatum *cells = store_row_->cells();
        for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
          if (OB_FAIL(exprs.at(i)->eval(ctx, datum))) {
            SQL_ENG_LOG(WARN, "failed to evaluate expr datum", K(ret), K(i));
          } else {
            cells[i] = *datum;
          }
          if (OB_SUCC(ret)) {
            store_row_->row_size_ += cells[i].len_;
          }
        }
        saved_ = true;
      }
      return ret;
    }
    // restore store row for shadow row
    int restore(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(store_row_)) {
        ret = common::OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "NULL store_row_", K(ret), KP(store_row_));
      } else if (saved_) {
        ret = store_row_->to_expr(exprs, ctx);
      }
      return ret;
    }
    // reset && release referenced memory
    virtual void reset()
    {
      if (NULL != alloc_ && NULL != store_row_) {
        alloc_->free(store_row_);
      }
      alloc_ = NULL;
      store_row_ = NULL;
      saved_ = false;
    }
    // reset && NOT release referenced memory
    virtual void reuse()
    {
      if (nullptr != store_row_) {
        uint32_t payload_len = store_row_->cnt_ * sizeof(ObDatum);
        //reserve the memory of datum.ptr_ to assign ObObj
        bzero(store_row_->payload_, payload_len);
        store_row_->row_size_ = payload_len + sizeof(StoredRow);
      }
      saved_ = false;
    }

    StoredRow *get_store_row() const { return store_row_; }
    TO_STRING_KV(KPC_(store_row));

  protected:
    common::ObIAllocator *alloc_;
    StoredRow *store_row_;
    bool saved_;
  };

  class BlockBuffer;
  struct Block
  {
    static const int64_t MAGIC = 0xbc054e02d8536315;
    static const int32_t ROW_HEAD_SIZE = sizeof(StoredRow);
    Block() : magic_(0), blk_size_(0), rows_(0){}

    static int inline min_buf_size(const common::ObIArray<ObExpr*> &exprs,
                                   int64_t row_extend_size,
                                   ObEvalCtx &ctx,
                                   int64_t &size)
    {
      int ret = OB_SUCCESS;
      size = 0;
      if (OB_FAIL(row_store_size(exprs, ctx, size))) {
        SQL_ENG_LOG(WARN, "failed to calc store row size", K(ret));
      } else {
        size += BlockBuffer::HEAD_SIZE + sizeof(BlockBuffer) + row_extend_size;
      }
      return ret;
    }
    static int inline row_store_size(
      const common::ObIArray<ObExpr*> &exprs,
      ObEvalCtx &ctx,
      int64_t &size,
      uint32_t row_extend_size = 0)
    {
      int ret = OB_SUCCESS;
      size = 0;
      if (OB_FAIL(ObChunkDatumStore::row_copy_size(exprs, ctx, size))) {
        SQL_ENG_LOG(WARN, "failed to calc store row size", K(ret));
      } else {
        size += ROW_HEAD_SIZE + row_extend_size;
      }
      return ret;
    }
    static int64_t inline min_buf_size(const int64_t row_store_size)
    {
      return BlockBuffer::HEAD_SIZE + sizeof(BlockBuffer) + row_store_size;
    }

    static int64_t inline min_buf_size(common::ObDatum *datums, const int64_t cnt)
    {
      return BlockBuffer::HEAD_SIZE + sizeof(BlockBuffer) + row_store_size(datums, cnt);
    }

    static int64_t inline row_store_size(
      const common::ObDatum *datums, const int64_t cnt, uint32_t row_extend_size = 0)
    {
      return ROW_HEAD_SIZE + row_extend_size + ObChunkDatumStore::row_copy_size(datums, cnt);
    }

    int append_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx *ctx,
          BlockBuffer *buf, int64_t row_extend_size, StoredRow **stored_row,
          const bool unswizzling);
    int add_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx,
      const int64_t row_size, uint32_t row_extend_size, StoredRow **stored_row = nullptr);
    int copy_stored_row(const StoredRow &stored_row, StoredRow **dst_sr);
    int copy_datums(const ObDatum *datums,
                    const int64_t cnt,
                    const int64_t extra_size,
                    StoredRow **dst_sr);
    //the memory of shadow stored row is not continuous,
    //so you cannot directly copy the memory of the entire stored row,
    //and you should make a deep copy of each datum in turn
    int add_shadow_stored_row(const StoredRow &stored_row,
                               const uint32_t row_extend_size,
                               StoredRow **dst_sr);
    // 将block payload 拷贝到unswizzling_payload, 并进行unswizzling,
    // 不改变原始payload中内存
    int gen_unswizzling_payload(char *unswizzling_payload, uint32 size);
    int unswizzling();
    int swizzling(int64_t *col_cnt);
    inline bool magic_check() { return MAGIC == magic_; }
    int get_store_row(int64_t &cur_pos, const StoredRow *&sr);
    inline Block* get_next() const { return next_; }
    inline bool is_empty() { return get_buffer()->is_empty(); }
    inline void set_block_size(uint32 blk_size) { blk_size_ = blk_size; }
    inline BlockBuffer* get_buffer()
    {
      return static_cast<BlockBuffer*>(
              static_cast<void*>(payload_ + blk_size_ - BlockBuffer::HEAD_SIZE));
    }
    inline int64_t data_size() { return get_buffer()->data_size(); }
    inline uint32_t rows() { return rows_; }
    inline int64_t remain() { return get_buffer()->remain(); }
    friend class BlockBuffer;
    TO_STRING_KV(K_(magic), K_(blk_size), K_(rows));
    union{
      int64_t magic_;   //for dump
      Block* next_;      //for block list in mem
    };
    uint32 blk_size_;  /* current blk's size, for dump/read */
    uint32 rows_;
    char payload_[0];
  } __attribute__((packed));

  struct BlockList
  {
  public:
    BlockList() : head_(NULL), last_(NULL), size_(0) {}
    inline int64_t get_size() const { return size_; }
    inline bool is_empty() const { return size_ == 0; }
    inline Block* get_first() const { return head_; }
    inline void reset() { size_ = 0; head_ = NULL; last_ = NULL; }

    inline int prefetch()
    {
      int ret = OB_SUCCESS;
      int64_t tmp_count = size_;
      Block* cur_buf = head_;
      while (0 < tmp_count && OB_SUCC(ret)) {
        if (nullptr == cur_buf) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          __builtin_prefetch(cur_buf, 0, 3);
          __builtin_prefetch(cur_buf->get_buffer(), 0, 3);
          cur_buf = cur_buf->next_;
          --tmp_count;
        }
      }
      return ret;
    }

    inline void add_last(Block* blk)
    {
      if (NULL == head_) {
        head_ = blk;
        last_ = blk;
        blk->next_ = NULL;
      } else {
        last_->next_ = blk;
        blk->next_ = NULL;
        last_ = blk;
      }
      size_++;
    }

    inline Block* remove_first() {
      Block* cur = head_;
      if (NULL != head_) {
        head_ = head_->next_;
        cur->next_ = NULL;
        size_--;
        if (0 == size_) {
          last_ = NULL;
        }
      }
      return cur;
    }
    TO_STRING_KV(K_(size), K_(head), K_(last), K_(*head), K_(last));
  private:
    Block* head_;
    Block* last_;
    int64_t size_;
  };

  /* contiguous memory:
   * |----------------|---Block
   * |next_           |-|-------------|
   * |cnt_            | |--HEAD_SIZE  |
   * |block_size_     |-|             |--block_size
   * |payload[]       |               |
   * |                |---------------|
   * |----------------|--BlockBuffer
   * |data->BlockHead |-|
   * |cur_pos         | |--TAIL_SIZE
   * |cap_=block_size |-|
   * |----------------|
   * */
  class BlockBuffer
  {
  public:
    static const int64_t HEAD_SIZE = sizeof(Block); /* n_rows, check_sum */
    BlockBuffer() : data_(NULL), cur_pos_(0), cap_(0){}

    int init(char *buf, const int64_t buf_size);
    inline int64_t remain() const { return cap_ - cur_pos_; }
    inline char *data() { return data_; }
    inline Block *get_block() { return block; }
    inline void set_block(Block *b) { block = b; }
    inline char *head() const { return data_ + cur_pos_; }
    inline int64_t capacity() const { return cap_; }
    inline void set_capacity(int64_t cap) { cap_ = cap; }
    inline int64_t mem_size() const { return cap_ + sizeof(BlockBuffer); }
    inline int64_t data_size() const { return cur_pos_; }
    inline void set_data_size(int64_t v) { cur_pos_ = v; }
    inline bool is_inited() const { return NULL != data_; }
    inline bool is_empty() const { return HEAD_SIZE >= cur_pos_; }

    inline void reset() { cur_pos_ = 0; cap_ = 0; data_ = NULL; }
    inline void reuse() { cur_pos_ = 0; fast_advance(HEAD_SIZE); block->rows_ = 0; }
    inline int advance(int64_t size);
    inline void fast_advance(int64_t size) { cur_pos_ += size; }
    TO_STRING_KV(KP_(data), K_(cur_pos), K_(cap));

    friend ObChunkDatumStore;
    friend Block;
  private:
    union {
      char *data_;
      Block *block;
    };
    int64_t cur_pos_;
    int64_t cap_;
  };

  class BlockBufferWrap : public BlockBuffer {
  public:
    BlockBufferWrap() : BlockBuffer(), rows_(0) {}

    int append_row(const common::ObIArray<ObExpr*> &exprs,
                   ObEvalCtx *ctx, int64_t row_extend_size);
    void reset() { rows_ = 0; BlockBuffer::reset(); }

  public:
    uint32_t rows_;
  };

  class RowIterator
  {
  public:
    friend class ObChunkDatumStore;
    RowIterator();
    virtual ~RowIterator() { reset(); }
    int init(ObChunkDatumStore *store);

    /* from StoredRow to NewRow */
    int get_next_row(const StoredRow *&sr);
    int get_next_batch(const StoredRow **rows, const int64_t max_rows, int64_t &read_rows);
    int get_next_batch(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx,
                       const int64_t max_rows, int64_t &read_rows, const StoredRow **rows);
    int convert_to_row(const StoredRow *sr, const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx);

    void reset() { reset_cursor(); }
    bool is_valid() const { return store_ != NULL && cur_iter_blk_ != NULL; }
    inline bool cur_blk_has_next() const
    {
      return (cur_iter_blk_ != NULL && cur_row_in_blk_ < cur_iter_blk_->rows_);
    }
    inline bool has_next() const
    {
      return cur_iter_blk_ != NULL
          && (cur_iter_blk_->get_next() != NULL || cur_row_in_blk_ < cur_iter_blk_->rows_);
    }

    TO_STRING_KV(KP_(store), K_(*store), K_(cur_iter_blk), K_(cur_row_in_blk), K_(cur_pos_in_blk),
        K_(n_blocks), K_(cur_nth_block));
  private:
    explicit RowIterator(ObChunkDatumStore *row_store);
    void reset_cursor()
    {
      cur_iter_blk_ = NULL;
      cur_row_in_blk_ = 0;
      cur_pos_in_blk_ = 0;
      n_blocks_ = 0;
      cur_nth_block_ = 0;
    }

  protected:
    ObChunkDatumStore* store_;
    Block* cur_iter_blk_;
    int64_t cur_row_in_blk_;  //cur nth row in cur block for in-mem debug
    int64_t cur_pos_in_blk_;  //cur nth row in cur block
    int64_t n_blocks_;
    int64_t cur_nth_block_;
  };

  // Iteration age used for iterated rows life cycle control, iterated rows' memory are available
  // until age increased. E.g.:
  //
  //   IterationAge iter_age;
  //   Iterator it;
  //   store.begin(it);
  //   it.set_iteration_age(iter_age);
  //
  //   while (...) {
  //     iter_age.inc();
  //
  //     row1 = it.get_next_row();
  //     row2 = it.get_next_row();
  //
  //     // row1 and row2's memory are still available here, until the get_next_row() is called
  //     // after iteration age increased.
  //   }
  class IterationAge
  {
  public:
    IterationAge() : age_(0) {}
    int64_t get(void) const { return age_; }
    void inc(void) { age_ += 1; }
  private:
    int64_t age_;
  };
  struct IteratedBlockHolder
  {
    IteratedBlockHolder() : block_list_() {}
    ~IteratedBlockHolder()
    {
      release();
    }
    void release();

    BlockList block_list_;
  };
  class Iterator
  {
  public:
    enum IterEndState
    {
      PROCESSING = 0x00,
      MEM_ITER_END = 0x01,
      DISK_ITER_END = 0x02
    };
    friend class ObChunkDatumStore;
    friend class IteratedBlockHolder;
    Iterator() : start_iter_(false),
                 store_(NULL),
                 cur_iter_blk_(NULL),
                 aio_read_handle_(),
                 cur_nth_blk_(-1),
                 cur_chunk_n_blocks_(0),
                 cur_iter_pos_(0),
                 file_size_(0),
                 chunk_mem_(NULL),
                 chunk_n_rows_(0),
                 iter_end_flag_(IterEndState::PROCESSING),
                 read_blk_(NULL),
                 read_blk_buf_(NULL),
                 aio_blk_(NULL),
                 aio_blk_buf_(NULL),
                 age_(NULL),
                 blk_holder_ptr_(NULL) {}
    virtual ~Iterator() { reset_cursor(0); }
    int init(ObChunkDatumStore *row_store, const IterationAge *age = NULL);
    void set_iteration_age(const IterationAge *age) { age_ = age; }
    int get_next_row(const common::ObIArray<ObExpr*> &exprs,
                     ObEvalCtx &ctx,
                     const StoredRow **sr = nullptr);
    int get_next_row(common::ObDatum **datums);
    int get_next_row(const StoredRow *&sr);
    template <bool fill_invariable_res_buf = false>
    int get_next_row(ObEvalCtx &ctx, const common::ObIArray<ObExpr*> &exprs);

    // read next batch rows
    // return OB_ITER_END and set %read_rows to zero for iterate end.
    int get_next_batch(const StoredRow **rows, const int64_t max_rows, int64_t &read_rows);
    template <bool fill_invariable_res_buf = false>
    int get_next_batch(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx,
                       const int64_t max_rows, int64_t &read_rows,
                       const StoredRow **rows = NULL);

    // attach read store rows to expressions
    template <bool fill_invariable_res_buf = false>
    static void attach_rows(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx,
                            const StoredRow **srows, const int64_t read_rows);

    int convert_to_row(const StoredRow *sr, const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx)
    { return row_it_.convert_to_row(sr, exprs, ctx); }
    // 暂未使用
    int convert_to_row(const StoredRow *sr, common::ObDatum **datums);

    void reset() { row_it_.reset(); reset_cursor(0); chunk_n_rows_ = 0; start_iter_ = false; }
    inline bool has_next()
    { return has_next_block() || (row_it_.is_valid() && row_it_.has_next()); }
    inline bool has_next_block()
    {
      return store_->n_blocks_ > 0 && (cur_nth_blk_ < store_->n_blocks_ - 1);
    }
    bool is_valid() { return nullptr != store_; }
    int load_next_block(RowIterator& it);
    void reset_cursor(const int64_t file_size);
    void begin_new_batch()
    {
      if (NULL == age_) {
        age_ = &inner_age_;
      }
      inner_age_.inc();
    }
    inline bool read_file_iter_end() { return iter_end_flag_ & DISK_ITER_END; }
    inline void set_read_file_iter_end() { iter_end_flag_ |= DISK_ITER_END; }
    inline bool read_mem_iter_end() { return iter_end_flag_ & MEM_ITER_END; }
    inline void set_read_mem_iter_end() { iter_end_flag_ |= MEM_ITER_END; }
    int prefetch_next_blk();
    int read_next_blk();
    int aio_read(char *buf, const int64_t size);
    int aio_wait();
    int alloc_block(Block *&blk, const int64_t size);
    void free_block(Block *blk, const int64_t size, bool force_free = false);
    void try_free_cached_blocks();
    int64_t get_cur_chunk_row_cnt() const { return chunk_n_rows_;}
    void set_blk_holder_ptr(IteratedBlockHolder *ptr) { blk_holder_ptr_ = ptr;}
    TO_STRING_KV(KP_(store), KP_(cur_iter_blk),
         K_(cur_chunk_n_blocks), K_(cur_iter_pos), K_(file_size),
         KP_(chunk_mem), KP_(read_blk), KP_(read_blk_buf), KP_(aio_blk),
         KP_(aio_blk_buf), K_(default_block_size), KP_(blk_holder_ptr));
  private:
     explicit Iterator(ObChunkDatumStore *row_store);
  protected:
     bool start_iter_;
     RowIterator row_it_;
     // cp from chunk iter
     ObChunkDatumStore* store_;
     Block* cur_iter_blk_;
     blocksstable::ObTmpFileIOHandle aio_read_handle_;
     int64_t cur_nth_blk_;     //reading nth blk start from 1
     int64_t cur_chunk_n_blocks_; //the number of blocks of current chunk
     int64_t cur_iter_pos_;    //pos in file
     int64_t file_size_;
     char* chunk_mem_;
     int64_t chunk_n_rows_;
     int32_t iter_end_flag_;

     Block *read_blk_;
     // Block::get_buffer() depends on blk_size_ which is overwrite by read data.
     // We need %read_blk_buf_ to the memory size of read block.
     BlockBuffer *read_blk_buf_;
     Block *aio_blk_; // not null means aio is reading.
     BlockBuffer *aio_blk_buf_;

     /*
      * Currently, the design philosophy of ObChunkDatumStore is as follows:
      *  - Support concurrent read. External readers have no "side effects" on
      *    ObChunkDatumStore and will not change any state in ObChunkDatumStore.
      *  - From the reader/writer's perspective, the data in ObChunkDatumStore
      *    is divided into two parts: blocks cached in memory and data persisted in files.
      *
      * ObChunkDatumStore's write strategy:
      * All data is first written to the cache block.
      * If the cache is full, the oldest data in the cache block is written to the disk,
      * and then the data is written to the cache block. The final effect is that old data
      * is on the disk and new data is in the cache, with the order of writing preserved.

      * ObChunkDatumStore's read strategy:
      * The reading order of data is consistent with the writing order,
      * first reading from the disk file, and then reading from the cache block after
      * finishing reading from the disk. An Iterator must be used to access data in ObChunkDatumStore.
      * When the Iterator accesses data, it first reads from the disk and caches the disk data
      * in the Iterator's private cache. When the Iterator is released, these cached data will also
      * be released with the Iterator. After the disk data is read, the Iterator directly
      * reads the block cache in ObChunkDatumStore based on its own record of the block offset.
      *
      * To wrap it up, the write & read mode is FIFO.
      */

      /*
        In this illustration, ObChunkDatumStore is readonly.
        Iterator 1 and Iterator 2 are accessing the same ObChunkDatumStore in parallel.
        The Iterator caches data independently, and have its own private cur_nth_blk_.

           +------------------------------------------------------+
           |                Iterator 1                            |
           |                      +-----------+  +----------+     |
           |  cur_nth_blk_        | cached    |  |cached    |     |
           |       |              | block     |  |block     |     |
           |       +------+       +-----+-----+  +----+-----+     |
           |              |             |             |           |
           +--------------+-------------+-------------+-----------+
                          |             |             |
                          |             |             |
                          |             |             |
      +-------------------+-------------+-------------+----------------+
      |                   |             |             |                |
      |     +-------+ +---v---+     +---v-------------v----------+     |
      |     |       | |       |     |                            |     |
      |     |in mem | |in mem |     |       in disk              |     |
      |     |block  | |block  |     |       block                |     |
      |     |       | |       |     |                            |     |
      |     |       | |       |     |                            |     |
      |     |       | |       |     |                            |     |
      |     +---^---+ +-------+     +------^--------^------------+     |
      |         |                          |        |                  |
      |         |      ObChunkDatumStore   |        |                  |
      |         |                          |        |                  |
      +---------+--------------------------+--------+------------------+
                |                          |        |
                |                          |        |
                |                          |        |
          +-----+--------------------------+--------+------------+
          |     |                          |        |            |
          |                      +---------+-+  +---+------+     |
          |  cur_nth_blk_        | cached    |  |cached    |     |
          |                      | block     |  |block     |     |
          |                      +-----------+  +----------+     |
          |               Iterator 2                             |
          +------------------------------------------------------+

     */
     // idle memory blocks cached by iterator,
     // used to cache data read from aio file
     BlockList ifree_list_;
     // active blocks for batch iterate
     // used to output data
     BlockList icached_;

     // inner iteration age is used for batch iteration with no outside age control.
     IterationAge inner_age_;
     const IterationAge *age_;
     int64_t default_block_size_;
     IteratedBlockHolder *blk_holder_ptr_;
  };

  struct BatchCtx
  {
    const ObDatum **datums_;
    StoredRow **stored_rows_;
    uint32_t *row_size_array_;
    uint16_t *selector_;
  };

  struct DisableDumpGuard
  {
    DisableDumpGuard(ObChunkDatumStore &store)
        : store_(store), memory_limit_(store_.mem_limit_)
    {
      store_.mem_limit_ = 0;
    }

    ~DisableDumpGuard()
    {
      store_.mem_limit_ = memory_limit_;
    }

  private:
    ObChunkDatumStore &store_;
    int64_t memory_limit_ = 0;
  };

public:
  const static int64_t BLOCK_SIZE = (64L << 10);
  const static int64_t MIN_BLOCK_SIZE = (4L << 10);
  static const int32_t DATUM_SIZE = sizeof(common::ObDatum);
  static const int64_t OLD_WORK_AREA_ID = 21;

  explicit ObChunkDatumStore(const lib::ObLabel &label, common::ObIAllocator *alloc = NULL);

  virtual ~ObChunkDatumStore() { reset(); }

  int init(int64_t mem_limit,
      uint64_t tenant_id = common::OB_SERVER_TENANT_ID,
      int64_t mem_ctx_id = common::ObCtxIds::DEFAULT_CTX_ID,
      const char *label = common::ObModIds::OB_SQL_CHUNK_ROW_STORE,
      bool enable_dump = true,
      uint32_t row_extra_size = 0,
      int64_t default_block_size = BLOCK_SIZE);

  void set_allocator(common::ObIAllocator &alloc) { allocator_ = &alloc; }

  void reset();

  /// begin iterator
  int begin(Iterator &it, const IterationAge *age = NULL)
  {
    return it.init(this, age);
  }

  int add_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx *ctx,
              StoredRow **stored_row = nullptr);
  int add_row(const StoredRow &sr, StoredRow **stored_row = nullptr);
  int add_row(const ObDatum *datums, const int64_t cnt,
              const int64_t extra_size, StoredRow **stored_row);
  int add_row(const StoredRow &sr, ObEvalCtx *ctx, StoredRow **stored_row = nullptr);
  int add_row(const ShadowStoredRow &sr, StoredRow **stored_row = nullptr);

  // Add batch rows to row store, the returned %stored_rows is dense.
  // the returned %stored_rows_count is equal to %batch_size - %skip.accumulate_bit_cnt()
  int add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                const ObBitVector &skip, const int64_t batch_size,
                int64_t &stored_rows_count,
                StoredRow **stored_rows = NULL,
                const int64_t start_pos = 0);

  // Add batch rows to row store, only row in %selector is added.
  // The selector size (%size) must equal to %batch_size - %skip.accumulate_bit_cnt().
  int add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                const ObBitVector &skip, const int64_t batch_size,
                const uint16_t selector[], const int64_t size,
                StoredRow **stored_rows = nullptr);

  int finish_add_row(bool need_dump = true);

  // Try add row to row store if the memory not exceeded after row added.
  // return OB_SUCCESS too when row not added (%row_added flag set to false).
  int try_add_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx *ctx,
                  const int64_t memory_limit, bool &row_added);
  int try_add_row(const ShadowStoredRow &sr,
                  const int64_t memory_limit,
                  bool &row_added,
                  StoredRow **real_sr);
  int try_add_row(const StoredRow &sr, const int64_t memory_limit, bool &row_added);
  int try_add_batch(const StoredRow ** stored_rows,
                    const int64_t batch_size,
                    const int64_t memory_limit,
                    bool &batch_added);
  // Try add a batch to row store if memory is enough to hold this batch
  int try_add_batch(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx *ctx,
                    const int64_t batch_size, const int64_t memory_limit,
                    bool &batch_added);
  OB_INLINE bool is_inited() const { return inited_; }
  bool is_file_open() const { return io_.fd_ >= 0; }

  //void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  //void set_mem_ctx_id(const int64_t ctx_id) { ctx_id_ = ctx_id; }
  void set_mem_limit(const int64_t limit) { mem_limit_ = limit; }
  void set_dumped(bool dumped) { enable_dump_ = dumped; }
  inline int64_t get_mem_limit() { return mem_limit_; }
  void set_block_size(const int64_t size) { default_block_size_ = size; }
  inline int64_t get_block_cnt() const { return n_blocks_; }
  inline int64_t get_block_list_cnt() { return blocks_.get_size(); }
  inline int64_t get_row_cnt() const { return row_cnt_; }
  inline int64_t get_col_cnt() const { return col_count_; }
  inline int64_t get_mem_hold() const { return mem_hold_; }
  inline int64_t get_mem_used() const { return mem_used_; }
  inline int64_t get_max_hold_mem() const { return max_hold_mem_; }
  inline int64_t get_file_fd() const { return io_.fd_; }
  inline int64_t get_file_dir_id() const { return io_.dir_id_; }
  inline int64_t get_file_size() const { return file_size_; }
  inline int64_t min_blk_size(const int64_t row_store_size)
  {
    int64_t size = std::max(default_block_size_, row_store_size);
    size = common::next_pow2(size);
    return size;
  }
  static int init_block_buffer(void* mem, const int64_t size, Block *&block);
  int add_block(Block* block, bool need_swizzling, bool *added = nullptr);
  int append_block(char *buf, int size,  bool need_swizzling);
  int append_block_payload(char *payload, int size, int rows, bool need_swizzling);
  void remove_added_blocks();
  bool has_dumped() { return has_dumped_; }
  inline int64_t get_row_cnt_in_memory() const { return row_cnt_ - dumped_row_cnt_; }
  inline int64_t get_row_cnt_on_disk() const { return dumped_row_cnt_; }
  void set_callback(ObSqlMemoryCallback *callback) { callback_ = callback; }
  void reset_callback()
  {
    callback_ = nullptr;
    io_event_observer_ = nullptr;
  }
  int dump(bool reuse, bool all_dump, int64_t dumped_size = INT64_MAX);
  // 目前dir id 的策略是上层逻辑（一般是算子）统一申请，然后再set过来
  void set_dir_id(int64_t dir_id) { io_.dir_id_ = dir_id; }
  int alloc_dir_id();
  TO_STRING_KV(K_(tenant_id), K_(label), K_(ctx_id),  K_(mem_limit),
      K_(row_cnt), K_(file_size), K_(enable_dump));

  int append_datum_store(const ObChunkDatumStore &other_store);
  int assign(const ObChunkDatumStore &other_store);
  bool is_empty() const { return blocks_.is_empty(); }

  inline int64_t get_last_buffer_mem_size()
  {
    return nullptr == cur_blk_ ? 0 : cur_blk_->get_buffer()->mem_size();
  }
  uint64_t get_tenant_id() { return tenant_id_; }
  const char* get_label() { return label_; }
  void free_tmp_dump_blk();
  inline void set_io_event_observer(ObIOEventObserver *observer)
  {
    io_event_observer_ = observer;
  }
  inline ObIOEventObserver *get_io_event_observer()
  {
    return io_event_observer_;
  }
  inline int64_t get_max_blk_size() const { return max_blk_size_; }
private:
  OB_INLINE int add_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx *ctx,
                        const int64_t row_size, StoredRow **stored_row);
  int inner_add_batch(const common::ObDatum **datums, const common::ObIArray<ObExpr *> &exprs,
                      const uint16_t selector[], const int64_t size,
                      StoredRow **stored_rows);
  // fallback to add_row() loop.
  int add_batch_fallback(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                         const ObBitVector &skip, const int64_t batch_size,
                         const uint16_t selector[], const int64_t size,
                         StoredRow **stored_rows);
  static int get_timeout(int64_t &timeout_ms);
  void *alloc_blk_mem(const int64_t size, const bool for_iterator);
  void free_blk_mem(void *mem, const int64_t size = 0);
  void free_block(Block *item);
  void free_blk_list();
  bool shrink_block(int64_t size);
  int alloc_block_buffer(Block *&block, const int64_t min_size, const bool for_iterator);
  int alloc_block_buffer(Block *&block, const int64_t data_size,
        const int64_t min_size, const bool for_iterator);
  inline int ensure_write_blk(const int64_t row_size);
  // new block is not needed if %min_size is zero. (finish add row)
  int switch_block(const int64_t min_size);
  int clean_memory_data(bool reuse);

  inline void use_block(Block *item)
    {
      cur_blk_ = item;
      cur_blk_buffer_ = cur_blk_->get_buffer();
      int64_t used = cur_blk_buffer_->capacity() + sizeof(BlockBuffer);
      mem_used_ += used;
    }
  inline int dump_one_block(BlockBuffer *item);

  int write_file(void *buf, int64_t size);
  int read_file(
      void *buf, const int64_t size, const int64_t offset, blocksstable::ObTmpFileIOHandle &handle,
      const int64_t file_size, const int64_t cur_pos, int64_t &tmp_file_size);
  int aio_read_file(void *buf,
                    const int64_t size,
                    const int64_t offset,
                    blocksstable::ObTmpFileIOHandle &handle);
  bool need_dump(int64_t extra_size);
  BlockBuffer* new_block();
  void set_io(int64_t size, char *buf) { io_.size_ = size; io_.buf_ = buf; }
  static void set_io(int64_t size, char *buf, blocksstable::ObTmpFileIOInfo &io) { io.size_ = size; io.buf_ = buf; }
  bool find_block_can_hold(const int64_t size, bool &need_shrink);
  int get_store_row(RowIterator &it, const StoredRow *&sr);
  inline void callback_alloc(int64_t size) { if (callback_ != nullptr) callback_->alloc(size); }
  inline void callback_free(int64_t size) { if (callback_ != nullptr) callback_->free(size); }

  int init_batch_ctx(const int64_t col_cnt, const int64_t max_batch_size);

  // 这里暂时以ObExpr的数组形式写入数据到DatumStore，主要是为了上层Operator在写入数据时，可以无脑调用ObExpr的插入
  // 可以看下面参数为common::ObDatum **datums的函数进行对比
  static inline int row_copy_size(
    const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, int64_t &size)
  {
    int ret = OB_SUCCESS;
    ObExpr *expr = nullptr;
    common::ObDatum *datum = nullptr;
    size = DATUM_SIZE * exprs.count();
    for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); ++i) {
      expr = exprs.at(i);
      if (OB_ISNULL(expr)) {
      } else if (OB_FAIL(expr->eval(ctx, datum))) {
        SQL_ENG_LOG(WARN, "failed to eval expr datum", KPC(expr), K(ret));
      } else {
        size += datum->len_;
      }
    }
    return ret;
  }

  // 提供给从chunk datum store获取后
  // 由于整体是compact模式，所以采用指针形式指向第一个datum，后续以++或下标方式可以获取所有datum
  // 暂时没有使用
  static inline int64_t row_copy_size(const common::ObDatum *datums, const int64_t cnt)
  {
    int64_t size = DATUM_SIZE * cnt;
    for (int64_t i = 0; i < cnt; ++i) {
      size += datums[i].len_;
    }
    return size;
  }
private:
  bool inited_;
  uint64_t tenant_id_;
  const char *label_;
  int64_t ctx_id_;
  int64_t mem_limit_;

  Block* cur_blk_;
  BlockBuffer* cur_blk_buffer_;
  BlockList blocks_;  // ASSERT: all linked blocks has at least one row stored
  BlockList free_list_;  // empty blocks
  int64_t max_blk_size_; //max block ever allocated
  int64_t min_blk_size_; //min block ever allocated
  int64_t default_block_size_; //default(min) block size; blocks larger then this will not be reused
  int64_t n_blocks_;
  int64_t row_cnt_;
  int64_t col_count_;

  blocksstable::ObTmpFileIOHandle aio_write_handle_;

  bool enable_dump_;
  bool has_dumped_;
  int64_t dumped_row_cnt_;
  // stat time used read/write disk. measured by cpu cycles, using rdtsc()
  ObIOEventObserver *io_event_observer_;

  //int fd_;
  blocksstable::ObTmpFileIOInfo io_;
  int64_t file_size_;
  int64_t n_block_in_file_;

  //BlockList blocks_;  // ASSERT: all linked blocks has at least one row stored
  int64_t mem_hold_;
  int64_t mem_used_;
  int64_t max_hold_mem_;
  common::DefaultPageAllocator inner_allocator_;
  common::ObIAllocator *allocator_;

  uint32_t row_extend_size_;
  ObSqlMemoryCallback *callback_;
  BatchCtx *batch_ctx_;
  Block *tmp_dump_blk_;

  DISALLOW_COPY_AND_ASSIGN(ObChunkDatumStore);
};

typedef ObChunkDatumStore::StoredRow ObStoredDatumRow;

inline int ObChunkDatumStore::BlockBuffer::advance(int64_t size)
{
  int ret = common::OB_SUCCESS;
  if (size < -cur_pos_) {
    //overflow
    ret = common::OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(size), K_(cur_pos));
  } else if (size > remain()) {
    ret = common::OB_BUF_NOT_ENOUGH;
    SQL_ENG_LOG(WARN, "buffer not enough", K(size), "remain", remain());
  } else {
    cur_pos_ += size;
  }
  return ret;
}

//shadow_datums means that the memory of datum may come from other datum or ObObj
//the memory of shadow_datums is not continuous,
//so you cannot directly copy the memory of the entire datum array,
//and you should make a deep copy of each datum in turn
OB_INLINE int ObChunkDatumStore::StoredRow::copy_shadow_datums(const common::ObDatum *datums,
                                                               const int64_t cnt,
                                                               char *buf,
                                                               const int64_t size,
                                                               const int64_t row_size,
                                                               const uint32_t row_extend_size)
{
  int ret = OB_SUCCESS;
  if (payload_ != buf || size < 0 || nullptr == datums) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), KP(payload_), KP(buf), K(size), K(datums));
  } else {
    cnt_ = static_cast<uint32_t>(cnt);
    row_size_ = static_cast<int32_t>(row_size);
    int64_t pos = sizeof(ObDatum) * cnt_ + row_extend_size;
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
      if (OB_FAIL(cells()[i].deep_copy(datums[i], buf, size, pos))) {
        SQL_ENG_LOG(WARN, "deep copy datum failed", K(ret), K(i));
      }
    }
  }
  return ret;
}

template <bool fill_invariable_res_buf>
int ObChunkDatumStore::StoredRow::to_expr(
    const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cnt_ != exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "datum count mismatch", K(ret), K(cnt_), K(exprs.count()));
  } else {
    for (uint32_t i = 0; i < cnt_; ++i) {
      ObExpr *expr = exprs.at(i);
      if (expr->is_const_expr()) {
        continue;
      } else {
        const ObDatum &src = cells()[i];
        if (OB_UNLIKELY(fill_invariable_res_buf && !expr->is_variable_res_buf())) {
          ObDatum &dst = expr->locate_datum_for_write(ctx);
          dst.pack_ = src.pack_;
          MEMCPY(const_cast<char *>(dst.ptr_), src.ptr_, src.len_);
        } else {
          ObDatum &dst = expr->locate_expr_datum(ctx);
          dst = src;
        }
        expr->set_evaluated_projected(ctx);
        SQL_ENG_LOG(DEBUG, "succ to_expr", K(cnt_), K(exprs.count()),
                  KPC(exprs.at(i)), K(cells()[i]), K(lbt()));
      }
    }
  }
  return ret;
}

template <bool fill_invariable_res_buf>
int ObChunkDatumStore::Iterator::get_next_row(
    ObEvalCtx &ctx, const common::ObIArray<ObExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow *sr = NULL;
  if (OB_FAIL(get_next_row(sr))) {
    if (OB_ITER_END != ret) {
      SQL_ENG_LOG(WARN, "get next stored row failed", K(ret));
    }
  } else if (OB_ISNULL(sr)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "returned stored row is NULL", K(ret));
  } else if (exprs.count() != sr->cnt_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "stored row cell count mismatch", K(ret), K(sr->cnt_), K(exprs.count()));
  } else {
    // Ignore const exprs to avoid const value overwrite problem, the const expr may be overwrite
    // by local mini task execution.
    // In main plan the const value overwrite problem is avoided by ad remove_const() expr,
    // but in subplan of mini task the subplan is generated in CG time, has no corresponding
    // logical operator, we can not add expr in CG.
    for (int64_t i = 0; i < sr->cnt_; i++) {
      const ObExpr *expr = exprs.at(i);
      if (expr->is_const_expr()) { // T_QUESTIONMARK is included in is_dynamic_const
        continue;
      } else {
        const ObDatum &src = sr->cells()[i];
        if (OB_UNLIKELY(fill_invariable_res_buf && !exprs.at(i)->is_variable_res_buf())) {
          ObDatum &dst = expr->locate_datum_for_write(ctx);
          dst.pack_ = src.pack_;
          OB_ASSERT(dst.ptr_ == expr->get_rev_buf(ctx) + expr->res_buf_len_ * expr->get_datum_idx(ctx));
          MEMCPY(const_cast<char *>(dst.ptr_), src.ptr_, src.len_);
        } else {
          ObDatum &dst = expr->locate_expr_datum(ctx);
          dst = src;
        }
        expr->set_evaluated_projected(ctx);
      }
    }
  }

  return ret;
}

template <bool fill_invariable_res_buf>
int ObChunkDatumStore::Iterator::get_next_batch(
    const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx,
    const int64_t max_rows, int64_t &read_rows, const StoredRow **rows)
{
  int ret = OB_SUCCESS;
  int64_t max_batch_size = ctx.max_batch_size_;
  const StoredRow **srows = rows;
  if (NULL == rows) {
    if (!is_valid()) {
      ret = OB_NOT_INIT;
      SQL_ENG_LOG(WARN, "not init", K(ret));
    } else if (OB_FAIL(store_->init_batch_ctx(exprs.count(), max_batch_size))) {
      SQL_ENG_LOG(WARN, "init batch ctx failed", K(ret), K(max_batch_size));
    } else {
      srows = const_cast<const StoredRow **>(store_->batch_ctx_->stored_rows_);
    }
  }
  if (OB_SUCC(ret) && max_rows > max_batch_size) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(max_rows), K(max_batch_size));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_next_batch(srows, max_rows, read_rows))) {
    if (OB_ITER_END != ret) {
      SQL_ENG_LOG(WARN, "get next batch failed", K(ret), K(max_rows));
    } else {
      read_rows = 0;
    }
  } else {
    attach_rows<fill_invariable_res_buf>(exprs, ctx, srows, read_rows);
  }

  return ret;
}

template <bool fill_invariable_res_buf>
void ObChunkDatumStore::Iterator::attach_rows(
    const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx,
    const StoredRow **srows, const int64_t read_rows)
{
  // FIXME bin.lb: change to row style?
  if (NULL != srows) {
    for (int64_t col_idx = 0; col_idx < exprs.count(); col_idx++) {
      ObExpr *e = exprs.at(col_idx);
      if (e->is_const_expr()) {
        continue;
      }
      if (OB_LIKELY(!fill_invariable_res_buf || e->is_variable_res_buf())) {
        ObDatum *datums = e->locate_batch_datums(ctx);
        if (!e->is_batch_result()) {
          datums[0] = srows[0]->cells()[col_idx];
        } else {
          for (int64_t i = 0; i < read_rows; i++) {
            datums[i] = srows[i]->cells()[col_idx];
          }
        }
      } else {
        if (!e->is_batch_result()) {
          ObDatum *datums = e->locate_datums_for_update(ctx, 1);
          const ObDatum &src = srows[0]->cells()[col_idx];
          ObDatum &dst = datums[0];
          dst.pack_ = src.pack_;
          MEMCPY(const_cast<char *>(dst.ptr_), src.ptr_, src.len_);
        } else {
          ObDatum *datums = e->locate_datums_for_update(ctx, read_rows);
          for (int64_t i = 0; i < read_rows; i++) {
            const ObDatum &src = srows[i]->cells()[col_idx];
            ObDatum &dst = datums[i];
            dst.pack_ = src.pack_;
            MEMCPY(const_cast<char *>(dst.ptr_), src.ptr_, src.len_);
            SQL_ENG_LOG(DEBUG, "from datum store", K(src), K(dst), K(col_idx), K(i), K(read_rows));
          }
        }
      }
      e->set_evaluated_projected(ctx);
      ObEvalInfo &info = e->get_eval_info(ctx);
      info.notnull_ = false;
      info.point_to_frame_ = false;
    }
  }
}

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_DATUM_STORE2_H_
