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

#ifndef OCEANBASE_BASIC_OB_RA_DATUM_STORE_H_
#define OCEANBASE_BASIC_OB_RA_DATUM_STORE_H_

#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/list/ob_dlist.h"
#include "common/row/ob_row.h"
#include "share/datum/ob_datum.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/basic/ob_sql_mem_callback.h"

namespace oceanbase
{
namespace sql
{

class ObIOEventObserver;

// Random access row store, support disk store.
// All row must have same cell count and  projector.
class ObRADatumStore
{
public:

  class ShrinkBuffer
  {
  public:
    ShrinkBuffer() : data_(NULL), head_(0), tail_(0), cap_(0) {}

    int init(char *buf, const int64_t buf_size);
    inline int64_t remain() const { return tail_ - head_; }
    inline char *data() { return data_; }
    inline char *head() const { return data_ + head_; }
    inline int64_t head_size() const { return head_; }
    inline char *tail() const { return data_ + tail_; }
    inline int64_t tail_size() const { return cap_ - tail_; }
    inline int64_t capacity() const { return cap_; }
    inline bool is_inited() const { return NULL != data_; }

    inline void reset() { *this = ShrinkBuffer(); }
    inline void reuse() { head_ = 0; tail_ = cap_; }
    inline int fill_head(int64_t size);
    inline int fill_tail(int64_t size);
    inline int compact();

    TO_STRING_KV(KP_(data), K_(head), K_(tail), K_(cap));
  private:
    char *data_;
    int64_t head_;
    int64_t tail_;
    int64_t cap_;
  };

  class LinkNode : public common::ObDLinkBase<LinkNode>
  {
  };

  // 这里暂时以ObExpr的数组形式写入数据到DatumStore，主要是为了上层Operator在写入数据时，可以无脑调用ObExpr的插入
  // 可以看下面参数为common::ObDatum **datums的函数进行对比
  template<bool NEED_EVAL = true>
  static inline int row_copy_size(
    const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, int64_t &size)
  {
    int ret = common::OB_SUCCESS;
    common::ObDatum *datum = nullptr;
    size = DATUM_SIZE * exprs.count();
    for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); ++i) {
      if (NEED_EVAL && OB_FAIL(exprs.at(i)->eval(ctx, datum))) {
        SQL_ENG_LOG(WARN, "failed to eval expr datum", K(ret));
      } else if (!NEED_EVAL && FALSE_IT(datum = &exprs.at(i)->locate_expr_datum(ctx))) {
      } else {
        size += datum->len_;
      }
    }
    return ret;
  }
  static inline int row_copy_size(
    const common::ObIArray<common::ObDatum> &datums, int64_t &size)
  {
    int ret = common::OB_SUCCESS;
    size = DATUM_SIZE * datums.count();
    for (int64_t i = 0; i < datums.count(); ++i) {
      size += datums.at(i).len_;
    }
    return ret;
  }
  struct StoredRow
  {
    StoredRow() : readable_(true), cnt_(0), row_size_(0) {}

    int copy_datums(const common::ObIArray<ObExpr*> &exprs,
                    ObEvalCtx &ctx,
                    char *buf,
                    const int64_t size,
                    const int64_t row_size,
                    const uint32_t row_extend_size);
    int copy_datums(const common::ObIArray<common::ObDatum> &datums,
                    char *buf,
                    const int64_t size,
                    const int64_t row_size,
                    const uint32_t row_extend_size);
    int to_expr(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx) const;
    inline common::ObDatum *cells()
    { return reinterpret_cast<common::ObDatum *>(payload_); }
    inline const common::ObDatum *cells() const
    { return reinterpret_cast<const common::ObDatum *>(payload_); }
    inline void *get_extra_payload() const
    { return static_cast<void*>(const_cast<char*>(payload_ + sizeof(common::ObDatum) * cnt_)); }
    int assign(const StoredRow *sr);

    int to_copyable();
    int to_readable();
    TO_STRING_KV(K_(readable), K_(cnt), K_(row_size), "cells", common::ObArrayWrap<common::ObDatum>(cells(), cnt_));

    uint32_t readable_:1;
    uint32_t cnt_:31;
    uint32_t row_size_;
    char payload_[0];
  } __attribute__((packed));

  struct Block
  {
    static const int64_t MAGIC = 0x35f4451b9b56eb12;
    typedef uint32_t row_idx_t;
    const static int64_t ROW_HEAD_SIZE = sizeof(StoredRow);
    const static int64_t ROW_INDEX_SIZE = sizeof(row_idx_t);

    Block() : magic_(MAGIC), row_id_(0), rows_(0), idx_off_(0) {}

    static int64_t inline min_buf_size(const int64_t row_store_size)
    {
      return sizeof(Block) + row_store_size;
    }
    template<bool NEED_EVAL>
    static int inline row_store_size(const common::ObIArray<ObExpr*> &exprs,
                                     ObEvalCtx &ctx,
                                     int64_t &size,
                                     uint32_t row_extend_size = 0)
    {
      int ret = common::OB_SUCCESS;
      size = 0;
      if (OB_FAIL(row_copy_size<NEED_EVAL>(exprs, ctx, size))) {
        SQL_ENG_LOG(WARN, "failed to calc store row size", K(ret));
      } else {
        size += ROW_HEAD_SIZE + ROW_INDEX_SIZE + row_extend_size;
      }
      return ret;
    }
    static int inline row_store_size(const common::ObIArray<common::ObDatum> &datums,
                                     int64_t &size,
                                     uint32_t row_extend_size = 0)
    {
      int ret = common::OB_SUCCESS;
      size = 0;
      if (OB_FAIL(row_copy_size(datums, size))) {
        SQL_ENG_LOG(WARN, "failed to calc store row size", K(ret));
      } else {
        size += ROW_HEAD_SIZE + ROW_INDEX_SIZE + row_extend_size;
      }
      return ret;
    }

    int add_row(ShrinkBuffer &buf, const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx,
                const int64_t row_size, uint32_t row_extend_size, StoredRow **stored_row = NULL);
    int add_row(ShrinkBuffer &buf, const common::ObIArray<common::ObDatum> &datums,
                const int64_t row_size, uint32_t row_extend_size, StoredRow **stored_row = NULL);
    int copy_stored_row(ShrinkBuffer &buf, const StoredRow &stored_row, StoredRow **dst_sr);
    int compact(ShrinkBuffer &buf);
    int to_copyable();
    const row_idx_t *indexes() const
    {
      return reinterpret_cast<const row_idx_t *>(payload_ + idx_off_);
    }
    inline bool contain(const int64_t row_id) const
    {
      return row_id_ <= row_id && row_id < row_id_ + rows_;
    }

    int get_store_row(const int64_t row_id, const StoredRow *&sr);

    TO_STRING_KV(K_(magic), K_(row_id), K_(rows), K_(idx_off));

    int64_t magic_;
    int64_t row_id_;
    int32_t rows_;
    int32_t idx_off_;
    char payload_[0];
  } __attribute__((packed));

  struct IndexBlock;
  struct BlockIndex
  {
    static bool compare(const BlockIndex &bi, const int64_t row_id) { return bi.row_id_ < row_id; }
    TO_STRING_KV(K_(is_idx_block), K_(on_disk), K_(row_id), K_(offset), K_(length));

    uint64_t is_idx_block_:1;
    uint64_t on_disk_:1;
    uint64_t row_id_ : 62;
    union {
      IndexBlock *idx_blk_;
      Block *blk_;
      int64_t offset_;
    };
    int32_t length_;
  } __attribute__((packed));

  struct IndexBlock
  {
    const static int64_t MAGIC = 0x4847bcb053c3703f;
    const static int64_t INDEX_BLOCK_SIZE = (64 << 10) - sizeof(LinkNode);
    constexpr static inline int64_t capacity() {
      return (INDEX_BLOCK_SIZE - sizeof(IndexBlock)) / sizeof(BlockIndex);
    }

    IndexBlock() : magic_(MAGIC), cnt_(0) {}

    inline int64_t buffer_size() const { return sizeof(*this) + sizeof(BlockIndex) * cnt_; }
    inline bool is_full() const { return cnt_ == capacity(); }

    // may return false when row in position (false negative),
    // since block index only contain start row_id, we can not detect right boundary.
    inline bool row_in_pos(const int64_t row_id, const int64_t pos);

    void reset() { cnt_ = 0; }

    TO_STRING_KV(K_(magic), K_(cnt));

    int64_t magic_;
    int32_t cnt_;
    BlockIndex block_indexes_[0];
  } __attribute__((packed));

  struct BlockBuffer
  {
    BlockBuffer() : buf_(), blk_() {}

    void reset() { buf_.reset(); blk_ = NULL; }
    ShrinkBuffer buf_;
    Block *blk_;
  };
  // Iteration age used for iterated rows life cycle control, iterated rows' memory are available
  // until age increased. E.g.:
  //
  //   IterationAge iter_age;
  //   Reader it(ra_row_store);
  //   it.set_iteration_age(iter_age);
  //
  //   while (...) {
  //     iter_age.inc();
  //
  //     it.get_row(idx1, row1);
  //     it.get_row(iex2, row2);
  //
  //     // row1 and row2's memory are still available here, until the get_row() is called
  //     // after iteration age increased.
  //   }
  class IterationAge
  {
  public:
    int64_t get(void) const { return age_; }
    void inc(void) { age_ += 1; }
  private:
    int64_t age_;
  };

  struct TryFreeMemBlk
  {
    TryFreeMemBlk *next_;
    int64_t age_;
    int64_t size_;

    TryFreeMemBlk() = delete;
  };

  static_assert(std::is_pod<TryFreeMemBlk>::value == true, "TryFreeMemBlk should be pod");

  // Reader must be deleted before ObRADatumStore
  class Reader
  {
    friend class ObRADatumStore;
  public:
    explicit Reader(ObRADatumStore &store)
        : store_(store), file_size_(0), idx_blk_(NULL), ib_pos_(0), blk_(NULL),
        age_(NULL), try_free_list_(NULL) {}
    virtual ~Reader() { reset(); }

    inline int64_t get_row_cnt() const { return store_.get_row_cnt(); }
    // row memory will hold until the next get_row called or non-const method of ObRADatumStore called
    int get_row(const int64_t row_id, const StoredRow *&sr);

    void set_iteration_age(IterationAge *age) { age_ = age; }

    void reset();
    void reuse();

  private:
    void reset_cursor(const int64_t file_size);
    void free_all_blks();

  private:
    ObRADatumStore &store_;
    // idx_blk_, blk_ may point to the writing block,
    // we need to invalid the pointers if file_size_ change.
    int64_t file_size_;
    IndexBlock *idx_blk_;
    int64_t ib_pos_; // current block index position in index block
    Block *blk_;

    ShrinkBuffer buf_;
    ShrinkBuffer idx_buf_;

    IterationAge *age_;
    TryFreeMemBlk *try_free_list_;

    DISALLOW_COPY_AND_ASSIGN(Reader);
  };

public:
  const static int64_t BLOCK_SIZE = (64L << 10) - sizeof(LinkNode);
  const static int64_t BIG_BLOCK_SIZE = (256L << 10) - sizeof(LinkNode);
  // alloc first index block after store 1MB data.
  const static int64_t DEFAULT_BLOCK_CNT = (1L << 20) / BLOCK_SIZE;
  static const int32_t DATUM_SIZE = sizeof(common::ObDatum);

  explicit ObRADatumStore(common::ObIAllocator *alloc = NULL);
  virtual ~ObRADatumStore() { reset(); }

  int init(int64_t mem_limit,
           uint64_t tenant_id = common::OB_SERVER_TENANT_ID,
           int64_t mem_ctx_id = common::ObCtxIds::DEFAULT_CTX_ID,
           const char *label = common::ObModIds::OB_SQL_ROW_STORE,
           uint32_t row_extend_size = 0);

  void reset();
  // Keeping one memory block, reader must call reuse() too.
  void reuse();

  template<bool NEED_EVAL = true>
  int add_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx *ctx,
              StoredRow **stored_row = nullptr);
  int add_row(const common::ObIArray<common::ObDatum> &datums,
              StoredRow **stored_row = nullptr);
  int add_row(const StoredRow &src_stored_row,
              StoredRow **stored_row = nullptr);
  int finish_add_row();
  // row memory will hold until the next non-const method of ObRADatumStore called.
  int get_row(const int64_t row_id, const StoredRow *&sr)
  {
    return inner_reader_.get_row(row_id, sr);
  }

  // set iteration age for inner reader.
  void set_iteration_age(IterationAge *age) { inner_reader_.set_iteration_age(age); }

  bool is_inited() const { return inited_; }
  bool is_file_open() const { return fd_ >= 0; }

  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_mem_ctx_id(const int64_t ctx_id) { ctx_id_ = ctx_id; }
  void set_mem_limit(const int64_t limit) { mem_limit_ = limit; }

  void set_mem_stat(ObSqlMemoryCallback *mem_stat) { mem_stat_ = mem_stat; }
  void set_io_observer(ObIOEventObserver *io_observer) { io_observer_ = io_observer; }

  inline int64_t get_row_cnt() const { return row_cnt_; }
  inline int64_t get_mem_hold() const { return mem_hold_; }
  inline int64_t get_file_size() const { return file_size_; }

  void set_mem_hold(int64_t hold);
  void inc_mem_hold(int64_t hold);
  void set_allocator(common::ObIAllocator &alloc) { allocator_ = &alloc; }
  void set_dir_id(int64_t dir_id) { dir_id_ = dir_id; }

  TO_STRING_KV(K_(tenant_id), K_(label), K_(ctx_id),  K_(mem_limit),
      K_(save_row_cnt), K_(row_cnt), K_(fd), K_(file_size));

private:
  static int get_timeout(int64_t &timeout_ms);
  void *alloc_blk_mem(const int64_t size);
  void free_blk_mem(void *mem, const int64_t size = 0);
  int setup_block(BlockBuffer &blkbuf) const;
  int alloc_block(BlockBuffer &blkbuf, const int64_t min_size);
  // new block is not needed if %min_size is zero. (finish add row)
  int switch_block(const int64_t min_size);
  int add_block_idx(const BlockIndex &bi);
  int alloc_idx_block(IndexBlock *&ib);
  int build_idx_block();
  int switch_idx_block(bool finish_add = false);

  int load_block(Reader &reader, const int64_t row_id);
  int load_idx_block(Reader &reader, IndexBlock *&ib, const BlockIndex &bi);
  int find_block_idx(Reader &reader, BlockIndex &bi, const int64_t row_id);

  int get_store_row(Reader &reader, const int64_t row_id, const StoredRow *&sr);

  int ensure_reader_buffer(Reader &reader, ShrinkBuffer &buf, const int64_t size);

  int write_file(BlockIndex &bi, void *buf, int64_t size);
  int read_file(void *buf, const int64_t size, const int64_t offset);

  bool need_dump();
  inline bool has_index_block() const { return nullptr != idx_blk_; }

private:
  bool inited_;
  uint64_t tenant_id_;
  const char *label_;
  int64_t ctx_id_;
  int64_t mem_limit_;


  BlockBuffer blkbuf_;
  IndexBlock *idx_blk_;

  int64_t save_row_cnt_;
  int64_t row_cnt_;

  int64_t fd_;
  int64_t dir_id_;
  int64_t file_size_;

  Reader inner_reader_;

  common::ObDList<LinkNode> blk_mem_list_;
  common::ObSEArray<BlockIndex, DEFAULT_BLOCK_CNT> blocks_;

  int64_t mem_hold_;
  common::DefaultPageAllocator inner_allocator_;
  common::ObIAllocator *allocator_;

  uint32_t row_extend_size_;
  ObSqlMemoryCallback *mem_stat_;
  ObIOEventObserver *io_observer_;

  DISALLOW_COPY_AND_ASSIGN(ObRADatumStore);
};

inline int ObRADatumStore::ShrinkBuffer::fill_head(int64_t size)
{
  int ret = common::OB_SUCCESS;
  if (size < -head_) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(size), K_(head));
  } else if (size > remain()) {
    ret = common::OB_BUF_NOT_ENOUGH;
    SQL_ENG_LOG(WARN, "buffer not enough", K(size), "remain", remain());
  } else {
    head_ += size;
  }
  return ret;
}

inline int ObRADatumStore::ShrinkBuffer::fill_tail(int64_t size)
{
  int ret = common::OB_SUCCESS;
  if (size < -tail_size()) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(size), "tail_size", tail_size());
  } else if (size > remain()) {
    ret = common::OB_BUF_NOT_ENOUGH;
    SQL_ENG_LOG(WARN, "buffer not enough", K(size), "remain", remain());
  } else {
    tail_ -= size;
  }
  return ret;
}

inline int ObRADatumStore::ShrinkBuffer::compact()
{
  int ret = common::OB_SUCCESS;
  if (!is_inited()) {
    ret = common::OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not inited", K(ret));
  } else {
    MEMMOVE(head(), tail(), tail_size());
    head_ += tail_size();
    tail_ = cap_;
  }
  return ret;
}

inline bool ObRADatumStore::IndexBlock::row_in_pos(const int64_t row_id, const int64_t pos)
{
  bool in_pos = false;
  if (cnt_ > 0 && pos >= 0 && pos < cnt_) {
    if (pos + 1 == cnt_) {
      in_pos = block_indexes_[pos].row_id_ == row_id;
    } else {
      in_pos = block_indexes_[pos].row_id_ <= row_id && row_id < block_indexes_[pos + 1].row_id_;
    }
  }
  return in_pos;
}
} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_RA_DATUM_STORE_H_
