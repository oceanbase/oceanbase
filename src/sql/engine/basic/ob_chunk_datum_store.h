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

namespace oceanbase {
namespace sql {

// Random access row store, support disk store.
// All row must have same cell count and  projector.
class ObChunkDatumStore {
  OB_UNIS_VERSION_V(1);

public:
  static inline int row_copy_size(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx, int64_t& size)
  {
    int ret = OB_SUCCESS;
    common::ObDatum* datum = nullptr;
    size = DATUM_SIZE * exprs.count();
    for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(exprs.at(i)->eval(ctx, datum))) {
        SQL_ENG_LOG(WARN, "failed to eval expr datum", KPC(exprs.at(i)), K(ret));
      } else {
        size += datum->len_;
      }
    }
    return ret;
  }
  static inline int64_t row_copy_size(common::ObDatum** datums, const int64_t cnt)
  {
    int64_t size = DATUM_SIZE * cnt;
    for (int64_t i = 0; i < cnt; ++i) {
      size += datums[i]->len_;
    }
    return size;
  }
  static inline int64_t row_copy_size(common::ObDatum* datums, const int64_t cnt)
  {
    int64_t size = DATUM_SIZE * cnt;
    for (int64_t i = 0; i < cnt; ++i) {
      size += datums[i].len_;
    }
    return size;
  }
  /*
   * StoredRow memory layout
   * N Datum + extend_size(can be 0) + real data
   * | datum1 | datum2 | ... | datumN | extend_size = 0 | data1 | data2 | ... | dataN |
   *     |__________________________________________________|^      ^              ^
   *              |_________________________________________________|              |
   *                             |_________________________________________________|
   */
  struct StoredRow {
    StoredRow() : cnt_(0), row_size_(0)
    {}
    int copy_datums(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx, char* buf, const int64_t size,
        const int64_t row_size, const uint32_t row_extend_size);
    int copy_datums(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx, int64_t& row_size, char* buf,
        const int64_t max_buf_size, const uint32_t row_extend_size);
    int copy_datums(common::ObDatum** datums, const int64_t cnt, char* buf, const int64_t size, const int64_t row_size,
        const uint32_t row_extend_size);
    int copy_datums(common::ObDatum* datums, const int64_t cnt, char* buf, const int64_t size, const int64_t row_size,
        const uint32_t row_extend_size);
    int to_expr(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx) const;
    int to_expr(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx, int64_t count) const;

    inline common::ObDatum* cells()
    {
      return reinterpret_cast<common::ObDatum*>(payload_);
    }
    inline const common::ObDatum* cells() const
    {
      return reinterpret_cast<const common::ObDatum*>(payload_);
    }
    inline void* get_extra_payload() const
    {
      return static_cast<void*>(const_cast<char*>(payload_ + sizeof(ObDatum) * cnt_));
    }
    int assign(const StoredRow* sr);
    void unswizzling(char* base = NULL);
    void swizzling(char* base = NULL);

    TO_STRING_KV(K_(cnt), K_(row_size), "cells", common::ObArrayWrap<common::ObDatum>(cells(), cnt_));

    uint32_t cnt_;
    uint32_t row_size_;
    char payload_[0];
  } __attribute__((packed));

  /*
   * Considering that many scenarios will temporarily save the next row of data,
   * so write a class to provide this way, Such as Sort, MergeDisintct, etc.
   * Features:
   * 1) Provide storage from ObIArray<ObExpr*> to StoredRow
   * 2) Provide reuse mode, memory can be reused
   * 3) Provide conversion from StoredRow to ObIArray<ObExpr*>
   */
  template <typename T = ObChunkDatumStore::StoredRow>
  class LastStoredRow {
  public:
    LastStoredRow(ObIAllocator& alloc) : store_row_(nullptr), alloc_(alloc), max_size_(0), reuse_(false)
    {}
    ~LastStoredRow()
    {}

    int save_store_row(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx, const int64_t extra_size = 0)
    {
      int ret = OB_SUCCESS;
      bool reuse = reuse_;
      char* buf = NULL;
      int64_t row_size = 0;
      int64_t buffer_len = 0;
      T* new_row = NULL;
      if (0 == exprs.count()) {
        // no column. scenario like distinct 1
      } else if (OB_FAIL(ObChunkDatumStore::row_copy_size(exprs, ctx, row_size))) {
        SQL_ENG_LOG(WARN, "failed to calc copy size", K(ret));
      } else {
        int64_t head_size = sizeof(T);
        reuse = OB_ISNULL(store_row_) ? false : reuse && (max_size_ >= row_size + head_size + extra_size);
        if (reuse && OB_NOT_NULL(store_row_)) {
          buf = reinterpret_cast<char*>(store_row_);
          new_row = store_row_;
          buffer_len = max_size_;
        } else {
          buffer_len = (!reuse_ ? row_size : row_size * 2) + head_size + extra_size;
          if (OB_ISNULL(buf = reinterpret_cast<char*>(alloc_.alloc(buffer_len)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SQL_ENG_LOG(ERROR, "alloc buf failed", K(ret));
          } else if (OB_ISNULL(new_row = new (buf) T())) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SQL_ENG_LOG(ERROR, "failed to new row", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          int64_t pos = head_size;
          if (OB_FAIL(new_row->copy_datums(exprs, ctx, buf + pos, buffer_len - head_size, row_size, extra_size))) {
            SQL_ENG_LOG(WARN, "failed to deep copy row", K(ret), K(buffer_len), K(row_size));
          } else {
            max_size_ = buffer_len;
            store_row_ = new_row;
          }
        }
      }
      return ret;
    }
    int save_store_row(const ObChunkDatumStore::StoredRow& row, const int64_t extra_size = 0)
    {
      int ret = OB_SUCCESS;
      bool reuse = reuse_;
      char* buf = NULL;
      int64_t buffer_len = 0;
      T* new_row = NULL;
      int64_t row_size = row.row_size_;
      int64_t head_size = sizeof(T);
      reuse = OB_ISNULL(store_row_) ? false : reuse && (max_size_ >= row_size + head_size + extra_size);
      if (reuse && OB_NOT_NULL(store_row_)) {
        buf = reinterpret_cast<char*>(store_row_);
        new_row = store_row_;
        buffer_len = max_size_;
      } else {
        buffer_len = (!reuse_ ? row_size : row_size * 2) + head_size + extra_size;
        if (OB_ISNULL(buf = reinterpret_cast<char*>(alloc_.alloc(buffer_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_ENG_LOG(ERROR, "alloc buf failed", K(ret));
        } else if (OB_ISNULL(new_row = new (buf) T())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_ENG_LOG(ERROR, "failed to new row", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        int64_t pos = head_size;
        if (OB_FAIL(new_row->copy_datums(const_cast<common::ObDatum*>(row.cells()),
                row.cnt_,
                buf + pos,
                buffer_len - head_size,
                row_size,
                extra_size))) {
          SQL_ENG_LOG(WARN, "failed to deep copy row", K(ret), K(buffer_len), K(row_size));
        } else {
          max_size_ = buffer_len;
          store_row_ = new_row;
        }
      }
      return ret;
    }
    void set_store_row(T* in_store_row)
    {
      store_row_ = in_store_row;
    }
    void reset()
    {
      store_row_ = nullptr;
      max_size_ = 0;
    }

    TO_STRING_KV(K_(max_size), K_(reuse), KPC_(store_row));
    T* store_row_;
    ObIAllocator& alloc_;
    int64_t max_size_;
    bool reuse_;
  };

  template <typename T = ObChunkDatumStore::StoredRow>
  class ShadowStoredRow {
  public:
    ShadowStoredRow() : alloc_(nullptr), store_row_(nullptr), saved_(false)
    {}
    ~ShadowStoredRow()
    {
      reset();
    }

    int init(common::ObIAllocator& allocator, int64_t datum_cnt)
    {
      int ret = OB_SUCCESS;
      int64_t buffer_len = datum_cnt * sizeof(ObDatum) + sizeof(T);
      char* buf = nullptr;
      if (NULL != alloc_) {
        ret = common::OB_INIT_TWICE;
        SQL_ENG_LOG(WARN, "init twice", K(ret));
      } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buffer_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(ERROR, "alloc buf failed", K(ret));
      } else {
        alloc_ = &allocator;
        store_row_ = new (buf) T();
        store_row_->cnt_ = datum_cnt;
        store_row_->row_size_ = datum_cnt * sizeof(ObDatum);
        saved_ = false;
      }
      return ret;
    }
    // copy exprs to store row
    int shadow_copy(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(store_row_) || OB_UNLIKELY(store_row_->cnt_ != exprs.count())) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "NULL datums or count mismatch", K(ret), KPC(store_row_), K(exprs.count()));
      } else {
        ObDatum* datum = nullptr;
        ObDatum* cells = store_row_->cells();
        for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
          if (OB_FAIL(exprs.at(i)->eval(ctx, datum))) {
            SQL_ENG_LOG(WARN, "failed to evaluate expr datum", K(ret), K(i));
          } else {
            cells[i] = *datum;
          }
        }
        saved_ = true;
      }
      return ret;
    }
    // restore store row for shadow row
    int restore(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx)
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
    void reset()
    {
      if (NULL != alloc_ && NULL != store_row_) {
        alloc_->free(store_row_);
      }
      alloc_ = NULL;
      store_row_ = NULL;
      saved_ = false;
    }

    T* get_store_row() const
    {
      return store_row_;
    }
    TO_STRING_KV(KPC_(store_row));

  private:
    common::ObIAllocator* alloc_;
    T* store_row_;
    bool saved_;
  };

  class BlockBuffer;
  struct Block {
    static const int64_t MAGIC = 0xbc054e02d8536315;
    static const int32_t ROW_HEAD_SIZE = sizeof(StoredRow);
    Block() : magic_(0), blk_size_(0), rows_(0)
    {}

    static int inline min_buf_size(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx, int64_t& size)
    {
      int ret = OB_SUCCESS;
      size = 0;
      if (OB_FAIL(row_store_size(exprs, ctx, size))) {
        SQL_ENG_LOG(WARN, "failed to calc store row size", K(ret));
      } else {
        size += BlockBuffer::HEAD_SIZE + sizeof(BlockBuffer);
      }
      return ret;
    }
    static int inline row_store_size(
        const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx, int64_t& size, uint32_t row_extend_size = 0)
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

    // following interface for ObDatum only,unused for now
    static int64_t inline min_buf_size(common::ObDatum** datums, const int64_t cnt)
    {
      return BlockBuffer::HEAD_SIZE + sizeof(BlockBuffer) + row_store_size(datums, cnt);
    }
    static int64_t inline min_buf_size(common::ObDatum* datums, const int64_t cnt)
    {
      return BlockBuffer::HEAD_SIZE + sizeof(BlockBuffer) + row_store_size(datums, cnt);
    }
    static int64_t inline row_store_size(common::ObDatum** datums, const int64_t cnt, uint32_t row_extend_size = 0)
    {
      return ROW_HEAD_SIZE + row_extend_size + ObChunkDatumStore::row_copy_size(datums, cnt);
    }
    static int64_t inline row_store_size(common::ObDatum* datums, const int64_t cnt, uint32_t row_extend_size = 0)
    {
      return ROW_HEAD_SIZE + row_extend_size + ObChunkDatumStore::row_copy_size(datums, cnt);
    }

    int append_row(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx* ctx, BlockBuffer* buf, int64_t row_extend_size,
        StoredRow** stored_row = nullptr);
    int add_row(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx, const int64_t row_size,
        uint32_t row_extend_size, StoredRow** stored_row = nullptr);
    int copy_stored_row(const StoredRow& stored_row, StoredRow** dst_sr);
    // copy block payload to unswizzling_payload
    // orginal payload memory unchanged
    int gen_unswizzling_payload(char* unswizzling_payload, uint32 size);
    int unswizzling();
    int swizzling(int64_t* col_cnt);
    inline bool magic_check()
    {
      return MAGIC == magic_;
    }
    int get_store_row(int64_t& cur_pos, const StoredRow*& sr);
    inline Block* get_next() const
    {
      return next_;
    }
    inline bool is_empty()
    {
      return get_buffer()->is_empty();
    }
    inline void set_block_size(uint32 blk_size)
    {
      blk_size_ = blk_size;
    }
    inline BlockBuffer* get_buffer()
    {
      return static_cast<BlockBuffer*>(static_cast<void*>(payload_ + blk_size_ - BlockBuffer::HEAD_SIZE));
    }
    inline int64_t data_size()
    {
      return get_buffer()->data_size();
    }
    inline uint32_t rows()
    {
      return rows_;
    }
    inline int64_t remain()
    {
      return get_buffer()->remain();
    }
    friend class BlockBuffer;
    TO_STRING_KV(K_(magic), K_(blk_size), K_(rows));
    union {
      int64_t magic_;  // for dump
      Block* next_;    // for block list in mem
    };
    uint32 blk_size_; /* current blk's size, for dump/read */
    uint32 rows_;
    char payload_[0];
  } __attribute__((packed));

  struct BlockList {
  public:
    BlockList() : head_(NULL), last_(NULL), size_(0)
    {}
    inline int64_t get_size() const
    {
      return size_;
    }
    inline bool is_empty() const
    {
      return size_ == 0;
    }
    inline Block* get_first() const
    {
      return head_;
    }
    inline void reset()
    {
      size_ = 0;
      head_ = NULL;
      last_ = NULL;
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

    inline Block* remove_first()
    {
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
  class BlockBuffer {
  public:
    static const int64_t HEAD_SIZE = sizeof(Block); /* n_rows, check_sum */
    BlockBuffer() : data_(NULL), cur_pos_(0), cap_(0)
    {}

    int init(char* buf, const int64_t buf_size);
    inline int64_t remain() const
    {
      return cap_ - cur_pos_;
    }
    inline char* data()
    {
      return data_;
    }
    inline Block* get_block()
    {
      return block;
    }
    inline char* head() const
    {
      return data_ + cur_pos_;
    }
    inline int64_t capacity() const
    {
      return cap_;
    }
    inline int64_t mem_size() const
    {
      return cap_ + sizeof(BlockBuffer);
    }
    inline int64_t data_size() const
    {
      return cur_pos_;
    }
    inline bool is_inited() const
    {
      return NULL != data_;
    }
    inline bool is_empty() const
    {
      return HEAD_SIZE >= cur_pos_;
    }

    inline void reset()
    {
      cur_pos_ = 0;
      cap_ = 0;
      data_ = NULL;
    }
    inline void reuse()
    {
      cur_pos_ = 0;
      advance(HEAD_SIZE);
      block->rows_ = 0;
    }
    inline int advance(int64_t size);

    TO_STRING_KV(KP_(data), K_(cur_pos), K_(cap));

    friend ObChunkDatumStore;
    friend Block;

  private:
    union {
      char* data_;
      Block* block;
    };
    int64_t cur_pos_;
    int64_t cap_;
  };

  class ChunkIterator;
  class RowIterator {
  public:
    friend class ObChunkDatumStore;
    RowIterator();
    virtual ~RowIterator()
    {
      reset();
    }
    int init(ChunkIterator* chunk_it);

    /* from StoredRow to NewRow */
    int get_next_row(const StoredRow*& sr);
    int get_next_block_row(const StoredRow*& sr);
    int convert_to_row(const StoredRow* sr, const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx);

    void reset()
    {
      reset_cursor();
    }
    bool is_valid() const
    {
      return store_ != NULL && cur_iter_blk_ != NULL;
    }
    inline bool cur_blk_has_next() const
    {
      return (cur_iter_blk_ != NULL && cur_row_in_blk_ < cur_iter_blk_->rows_);
    }
    inline bool has_next() const
    {
      return cur_iter_blk_ != NULL && (cur_iter_blk_->get_next() != NULL || cur_row_in_blk_ < cur_iter_blk_->rows_);
    }

    TO_STRING_KV(KP_(store), K_(*store), K_(cur_iter_blk), K_(cur_row_in_blk), K_(cur_pos_in_blk), K_(n_blocks),
        K_(cur_nth_block));

  private:
    explicit RowIterator(ObChunkDatumStore* row_store);
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
    int64_t cur_row_in_blk_;  // cur nth row in cur block for in-mem debug
    int64_t cur_pos_in_blk_;  // cur nth row in cur block
    int64_t n_blocks_;
    int64_t cur_nth_block_;
  };

  class ChunkIterator {
  public:
    enum IterEndState { PROCESSING = 0x00, MEM_ITER_END = 0x01, DISK_ITER_END = 0x02 };

  public:
    friend class ObChunkDatumStore;
    ChunkIterator();
    virtual ~ChunkIterator();
    int init(ObChunkDatumStore* row_store, int64_t chunk_read_size = 0);
    int load_next_chunk(RowIterator& it);
    inline bool has_next_chunk()
    {
      return store_->n_blocks_ > 0 && (cur_nth_blk_ < store_->n_blocks_ - 1);
    }
    void set_chunk_read_size(int64_t chunk_read_size)
    {
      chunk_read_size_ = chunk_read_size;
    }
    inline int64_t get_chunk_read_size()
    {
      return chunk_read_size_;
    }
    inline int64_t get_row_cnt() const
    {
      return store_->get_row_cnt();
    }
    inline int64_t get_cur_chunk_row_cnt() const
    {
      return chunk_n_rows_;
    }
    inline int64_t get_chunk_read_size() const
    {
      return chunk_read_size_;
    }
    void reset();
    inline bool is_valid()
    {
      return store_ != NULL;
    }
    inline bool read_file_iter_end()
    {
      return iter_end_flag_ & DISK_ITER_END;
    }
    inline void set_read_file_iter_end()
    {
      iter_end_flag_ |= DISK_ITER_END;
    }
    inline bool read_mem_iter_end()
    {
      return iter_end_flag_ & MEM_ITER_END;
    }
    inline void set_read_mem_iter_end()
    {
      iter_end_flag_ |= MEM_ITER_END;
    }

    TO_STRING_KV(KP_(store), KP_(cur_iter_blk), KP_(cur_iter_blk_buf), K_(cur_chunk_n_blocks), K_(cur_iter_pos),
        K_(file_size), K_(chunk_read_size), KP_(chunk_mem));

  private:
    void reset_cursor(const int64_t file_size);

  protected:
    ObChunkDatumStore* store_;
    Block* cur_iter_blk_;
    BlockBuffer* cur_iter_blk_buf_; /*for reuse of cur_iter_blk_;
                                      cause Block::get_buffer() depends on blk_size_
                                      but blk_size_ will change with block reusing
                                    */
    Block* swap_iter_blk_;
    blocksstable::ObTmpFileIOHandle aio_read_handle_;
    blocksstable::ObTmpFileIOHandle swap_aio_read_handle_;
    blocksstable::ObTmpFileIOHandle* cur_aio_read_handle_;
    blocksstable::ObTmpFileIOHandle* next_aio_read_handle_;
    bool next_iter_end_;
    int64_t cur_nth_blk_;         // reading nth blk start from 1
    int64_t cur_chunk_n_blocks_;  // the number of blocks of current chunk
    int64_t cur_iter_pos_;        // pos in file
    int64_t file_size_;
    int64_t chunk_read_size_;
    char* chunk_mem_;
    int64_t chunk_n_rows_;
    int32_t iter_end_flag_;
  };

  class Iterator {
  public:
    friend class ObChunkDatumStore;
    Iterator() : start_iter_(false)
    {}
    virtual ~Iterator()
    {}
    int init(ObChunkDatumStore* row_store, int64_t chunk_read_size = 0);
    void set_chunk_read_size(int64_t chunk_read_size)
    {
      chunk_it_.set_chunk_read_size(chunk_read_size);
    }
    int get_next_row(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx, const StoredRow** sr = nullptr);
    int get_next_row(common::ObDatum** datums);
    int get_next_row(const StoredRow*& sr);
    int get_next_row_skip_const(ObEvalCtx& ctx, const common::ObIArray<ObExpr*>& exprs);
    int convert_to_row(const StoredRow* sr, const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx)
    {
      return row_it_.convert_to_row(sr, exprs, ctx);
    }
    // unsed
    int convert_to_row(const StoredRow* sr, common::ObDatum** datums);

    void reset()
    {
      row_it_.reset();
      chunk_it_.reset();
      start_iter_ = false;
    }
    inline bool has_next()
    {
      return chunk_it_.has_next_chunk() || (row_it_.is_valid() && row_it_.has_next());
    }
    bool is_valid()
    {
      return chunk_it_.is_valid();
    }
    inline int64_t get_chunk_read_size()
    {
      return chunk_it_.get_chunk_read_size();
    }

  private:
    explicit Iterator(ObChunkDatumStore* row_store);

  protected:
    bool start_iter_;
    ChunkIterator chunk_it_;
    RowIterator row_it_;
  };

public:
  const static int64_t BLOCK_SIZE = (64L << 10);
  static const int32_t DATUM_SIZE = sizeof(common::ObDatum);

  explicit ObChunkDatumStore(common::ObIAllocator* alloc = NULL);

  virtual ~ObChunkDatumStore()
  {
    reset();
  }

  int init(int64_t mem_limit, uint64_t tenant_id = common::OB_SERVER_TENANT_ID,
      int64_t mem_ctx_id = common::ObCtxIds::DEFAULT_CTX_ID,
      const char* label = common::ObModIds::OB_SQL_CHUNK_ROW_STORE, bool enable_dump = true,
      uint32_t row_extra_size = 0);

  void set_allocator(common::ObIAllocator& alloc)
  {
    allocator_ = &alloc;
  }

  void reset();
  // Keeping one memory block, reader must call reuse() too.
  void reuse();

  /// begin iterator
  int begin(ChunkIterator& it, int64_t chunk_read_size = 0)
  {
    return it.init(this, chunk_read_size);
  }

  int begin(Iterator& it, int64_t chunk_read_size = 0)
  {
    return it.init(this, chunk_read_size);
  }

  int add_row(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx* ctx, StoredRow** stored_row = nullptr);
  int add_row(const StoredRow& sr, StoredRow** stored_row = nullptr);
  int add_row(const StoredRow& sr, ObEvalCtx* ctx, StoredRow** stored_row = nullptr);
  int finish_add_row(bool need_dump = true);

  // Try add row to row store if the memory not exceeded after row added.
  // return OB_SUCCESS too when row not added (%row_added flag set to false).
  int try_add_row(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx* ctx, const int64_t memory_limit, bool& row_added);
  OB_INLINE bool is_inited() const
  {
    return inited_;
  }
  bool is_file_open() const
  {
    return io_.fd_ >= 0;
  }

  // void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  // void set_mem_ctx_id(const int64_t ctx_id) { ctx_id_ = ctx_id; }
  void set_mem_limit(const int64_t limit)
  {
    mem_limit_ = limit;
  }
  void set_dumped(bool dumped)
  {
    enable_dump_ = dumped;
  }
  inline int64_t get_mem_limit()
  {
    return mem_limit_;
  }
  void set_block_size(const int64_t size)
  {
    default_block_size_ = size;
  }
  inline int64_t get_block_cnt() const
  {
    return n_blocks_;
  }
  inline int64_t get_block_list_cnt()
  {
    return blocks_.get_size();
  }
  inline int64_t get_row_cnt() const
  {
    return row_cnt_;
  }
  inline int64_t get_col_cnt() const
  {
    return col_count_;
  }
  inline int64_t get_mem_hold() const
  {
    return mem_hold_;
  }
  inline int64_t get_mem_used() const
  {
    return mem_used_;
  }
  inline int64_t get_file_size() const
  {
    return file_size_;
  }
  inline int64_t min_blk_size(const int64_t row_store_size)
  {
    int64_t size = std::max(std::max(static_cast<int64_t>(BLOCK_SIZE), default_block_size_), row_store_size);
    size = next_pow2(size);
    return size;
  }
  static int init_block_buffer(void* mem, const int64_t size, Block*& block);
  int add_block(Block* block, bool need_swizzling, bool* added = nullptr);
  int append_block(char* buf, int size, bool need_swizzling);
  void remove_added_blocks();
  bool has_dumped()
  {
    return has_dumped_;
  }
  inline int64_t get_row_cnt_in_memory() const
  {
    return row_cnt_ - dumped_row_cnt_;
  }
  inline int64_t get_row_cnt_on_disk() const
  {
    return dumped_row_cnt_;
  }
  void set_callback(ObSqlMemoryCallback* callback)
  {
    callback_ = callback;
  }
  int dump(bool reuse, bool all_dump);
  void set_dir_id(int64_t dir_id)
  {
    io_.dir_id_ = dir_id;
  }
  int alloc_dir_id();
  TO_STRING_KV(K_(tenant_id), K_(label), K_(ctx_id), K_(mem_limit), K_(row_cnt), K_(file_size));

  int append_datum_store(const ObChunkDatumStore& other_store);
  int assign(const ObChunkDatumStore& other_store);
  bool is_empty() const
  {
    return blocks_.is_empty();
  }

  int update_iterator(Iterator& org_it);
  int clean_block(Block* clean_block);

private:
  OB_INLINE int add_row(
      const common::ObIArray<ObExpr*>& exprs, ObEvalCtx* ctx, const int64_t row_size, StoredRow** stored_row);
  static int get_timeout(int64_t& timeout_ms);
  void* alloc_blk_mem(const int64_t size, const bool for_iterator);
  void free_blk_mem(void* mem, const int64_t size = 0);
  void free_block(Block* item);
  void free_blk_list();
  bool shrink_block(int64_t size);
  int alloc_block_buffer(Block*& block, const int64_t min_size, const bool for_iterator);
  int alloc_block_buffer(Block*& block, const int64_t data_size, const int64_t min_size, const bool for_iterator);
  // new block is not needed if %min_size is zero. (finish add row)
  int switch_block(const int64_t min_size);
  int clean_memory_data(bool reuse);

  inline void use_block(Block* item)
  {
    cur_blk_ = item;
    cur_blk_buffer_ = cur_blk_->get_buffer();
    int64_t used = cur_blk_buffer_->capacity() + sizeof(BlockBuffer);
    mem_used_ += used;
  }
  inline int dump_one_block(BlockBuffer* item);

  int write_file(void* buf, int64_t size);
  int read_file(void* buf, const int64_t size, const int64_t offset, blocksstable::ObTmpFileIOHandle& handle);
  int aio_read_file(void* buf, const int64_t size, const int64_t offset, blocksstable::ObTmpFileIOHandle& handle);
  int aio_read_file(ChunkIterator& it, int64_t read_size);
  bool need_dump(int64_t extra_size);
  BlockBuffer* new_block();
  void set_io(int64_t size, char* buf)
  {
    io_.size_ = size;
    io_.buf_ = buf;
  }
  bool find_block_can_hold(const int64_t size, bool& need_shrink);
  int get_store_row(RowIterator& it, const StoredRow*& sr);
  int load_next_block(ChunkIterator& it);
  int load_next_chunk_blocks(ChunkIterator& it);
  inline void callback_alloc(int64_t size)
  {
    if (callback_ != nullptr)
      callback_->alloc(size);
  }
  inline void callback_free(int64_t size)
  {
    if (callback_ != nullptr)
      callback_->free(size);
  }

private:
  bool inited_;
  uint64_t tenant_id_;
  const char* label_;
  int64_t ctx_id_;
  int64_t mem_limit_;

  Block* cur_blk_;
  BlockBuffer* cur_blk_buffer_;
  BlockList blocks_;            // ASSERT: all linked blocks has at least one row stored
  BlockList free_list_;         // empty blocks
  int64_t max_blk_size_;        // max block ever allocated
  int64_t min_blk_size_;        // min block ever allocated
  int64_t default_block_size_;  // default(min) block size; blocks larger then this will not be reused
  int64_t n_blocks_;
  int64_t row_cnt_;
  int64_t col_count_;

  blocksstable::ObTmpFileIOHandle aio_write_handle_;

  bool enable_dump_;
  bool has_dumped_;
  int64_t dumped_row_cnt_;

  // int fd_;
  blocksstable::ObTmpFileIOInfo io_;
  int64_t file_size_;
  int64_t n_block_in_file_;

  // BlockList blocks_;  // ASSERT: all linked blocks has at least one row stored
  int64_t mem_hold_;
  int64_t mem_used_;
  common::DefaultPageAllocator inner_allocator_;
  common::ObIAllocator* allocator_;

  uint32_t row_extend_size_;
  ObSqlMemoryCallback* callback_;

  DISALLOW_COPY_AND_ASSIGN(ObChunkDatumStore);
};

inline int ObChunkDatumStore::BlockBuffer::advance(int64_t size)
{
  int ret = common::OB_SUCCESS;
  if (size < -cur_pos_) {
    // overflow
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

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_BASIC_OB_DATUM_STORE2_H_
