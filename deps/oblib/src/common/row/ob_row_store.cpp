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

#define USING_LOG_PREFIX COMMON
#include "common/row/ob_row_store.h"
#include "common/row/ob_row_util.h"
#include "lib/utility/utility.h"
namespace oceanbase
{
namespace common
{

////////////////////////////////////////////////////////////////
struct ObRowStore::BlockInfo
{
  explicit BlockInfo(int64_t block_size)
      :magic_(0xabcd4444abcd4444),
       next_(NULL),
       prev_(NULL),
       curr_data_pos_(0),
       block_size_(block_size)
  {
  }
  OB_INLINE int64_t get_remain_size() const{return block_size_ - curr_data_pos_ - sizeof(BlockInfo);}
  OB_INLINE int64_t get_remain_size_for_read(int64_t pos) const{return curr_data_pos_ - pos;}
  OB_INLINE char *get_buffer(){return data_ + curr_data_pos_;}
  OB_INLINE const char* get_buffer(int64_t pos) const{return data_ + pos;}
  OB_INLINE const char *get_buffer_head() const{return data_;}
  OB_INLINE void advance(const int64_t length){curr_data_pos_ += length;}
  OB_INLINE int64_t get_curr_data_pos() const{return curr_data_pos_;}
  OB_INLINE void set_curr_data_pos(int64_t pos){curr_data_pos_ = pos;}
  OB_INLINE BlockInfo *get_next_block() {return next_;}
  OB_INLINE const BlockInfo *get_next_block() const { return next_; }
  OB_INLINE void set_next_block(BlockInfo *blk) {next_ = blk;};
  OB_INLINE bool is_empty() const { return (0 == curr_data_pos_); }
  OB_INLINE void set_block_size(const int64_t block_size) {block_size_ = block_size;};
  OB_INLINE int64_t get_block_size() const {return block_size_;};

  int append_row(const ObIArray<int64_t> &reserved_columns,
                 const ObNewRow &row,
                 int64_t payload,
                 StoredRow *&stored_row);
  int append_row_by_copy(const ObIArray<int64_t> &reserved_columns,
                         const ObNewRow &row,
                         int64_t payload,
                         StoredRow *&stored_row);
  friend class ObRowStore::BlockList;
private:
#ifdef __clang__
  int64_t magic_ [[gnu::unused]];
#else
  int64_t magic_;
#endif
  BlockInfo *next_;
  BlockInfo *prev_;
  /**
   * cur_data_pos_ must be set when BlockInfo deserialized
   */
  int64_t curr_data_pos_;
  int64_t block_size_;
  char data_[0];
};

int ObRowStore::BlockInfo::append_row(const ObIArray<int64_t> &reserved_columns,
                                      const ObNewRow &row,
                                      int64_t payload,
                                      StoredRow *&stored_row)
{
  int ret = OB_SUCCESS;
  // store header
  const int64_t reserved_columns_count = reserved_columns.count();
  const int64_t reserved_cells_size = get_reserved_cells_size(reserved_columns_count);
  stored_row = reinterpret_cast<StoredRow *>(get_buffer());

  advance(reserved_cells_size);
  if (OB_ISNULL(stored_row)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "get invalid stored row", K(ret));
  } else if (OB_UNLIKELY(curr_data_pos_ > block_size_ - SIZEOF(*this))) {
    ret = OB_BUF_NOT_ENOUGH;
    OB_LOG(DEBUG, "buffer size is not enough for head, ", K(ret), K(curr_data_pos_),
        K(block_size_), K(SIZEOF(*this)));
  } else {
    stored_row->reserved_cells_count_ = static_cast<int32_t>(reserved_columns_count);
    stored_row->payload_ = payload;
    // store row data
    ObObj cell_clone;
    ObCellWriter cell_writer;
    if (OB_FAIL(cell_writer.init(get_buffer(), get_remain_size(), DENSE))) {
      // empty
      OB_LOG(WARN, "init cell writer failed", K(ret));
    } else {
      int32_t reserved_count = 0;
      int64_t count = row.get_count();
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        // honor the projector
        const ObObj &cell = row.get_cell(i);
        if (cell.is_ext() && ObActionFlag::OP_END_FLAG == cell.get_ext()) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "append row is invalid", K(row));
        } else if (OB_FAIL(cell_writer.append(OB_INVALID_ID, cell, &cell_clone))) {
          if (OB_BUF_NOT_ENOUGH != ret) {
            OB_LOG(WARN, "failed to append cell", K(ret));
          }
        }
        // whether reserve this cell
        for (int64_t j = 0; OB_SUCC(ret) && j < reserved_columns_count; ++j) {
          const int64_t index = reserved_columns.at(j);
          if (index == i) {
            stored_row->reserved_cells_[j] = cell_clone;
            ++reserved_count;
            // should not break when ORDER BY c1, c1 DESC, ...
            // If the break goes out here, reserved_count will be less than reserved_columns_count
            // break;
          }
        } // end for j
      } // end for i
      if (OB_SUCC(ret)) {
        if (reserved_columns_count != reserved_count) {
          OB_LOG(WARN, "reserved columns count not equeal to actual add count",
                 K(reserved_columns_count), K(reserved_count));
          ret = OB_ERR_UNEXPECTED;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(cell_writer.row_finish())) {
          if (OB_BUF_NOT_ENOUGH != ret) {
            OB_LOG(WARN, "failed to append cell", K(ret));
          }
        } else {
          // success
          stored_row->compact_row_size_ = static_cast<int32_t>(cell_writer.size());
          advance(cell_writer.size());
          if (OB_UNLIKELY(curr_data_pos_ > block_size_ - SIZEOF(*this))) {
            ret = OB_ERR_UNEXPECTED;
            OB_LOG(ERROR, "fail to append row, overwrite the head", K(ret), K_(curr_data_pos),
                   K_(block_size), K(SIZEOF(*this)));
          }
        }
      }
    }
  }
  if (OB_BUF_NOT_ENOUGH == ret) {
    // rollback header
    advance(-reserved_cells_size);
    if (OB_UNLIKELY(0 > get_curr_data_pos())) {
      ret = OB_ERR_SYS;
      OB_LOG(WARN, "out of memory range, ",
          K(ret), K_(block_size), K_(curr_data_pos), K(reserved_cells_size));
    }
  }
  return ret;
}

int ObRowStore::BlockInfo::append_row_by_copy(const ObIArray<int64_t> &reserved_columns,
                                              const ObNewRow &row,
                                              int64_t payload,
                                              StoredRow *&stored_row)
{
  int ret = OB_SUCCESS;
  // store header
  stored_row = reinterpret_cast<StoredRow *>(get_buffer());
  if (OB_UNLIKELY(row.projector_size_ > 0)) {
    ret = OB_NOT_SUPPORTED;
    OB_LOG(WARN, "row with projector is not supported", K(ret), K(row.projector_size_));
  } else if (OB_ISNULL(stored_row)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "get invalid stored row", K(ret));
  } else {
    const int64_t header_size = get_reserved_cells_size(reserved_columns.count());
    const int64_t new_row_size = sizeof(ObNewRow) + row.get_deep_copy_size();

    if (OB_UNLIKELY(get_remain_size() < header_size + new_row_size)) {
      ret = OB_BUF_NOT_ENOUGH;
      OB_LOG(DEBUG, "buffer size is not enough for the row", K(ret), K(curr_data_pos_),
             K(block_size_), K(SIZEOF(*this)), K(header_size), K(new_row_size));
    } else {
      stored_row->reserved_cells_count_ = static_cast<int32_t>(reserved_columns.count());
      stored_row->payload_ = payload;
      stored_row->compact_row_size_ = static_cast<int32_t>(new_row_size);
      advance(header_size);

      ObNewRow *row_clone = new(get_buffer()) ObNewRow();
      int64_t pos = sizeof(ObNewRow);
      if (OB_FAIL(row_clone->deep_copy(row, get_buffer(), new_row_size, pos))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("failed to deep copy row", K(ret), K(new_row_size), K(pos));
      } else if (new_row_size != pos) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("size != pos", K(ret), K(new_row_size), K(pos));
      } else {
        advance(new_row_size);
        for (int64_t i = 0; OB_SUCC(ret) && i < reserved_columns.count(); ++i) {
          int64_t index = reserved_columns.at(i);
          if (OB_UNLIKELY(index >= row.count_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid index", K(index), K(row.count_), K(ret));
          } else {
            stored_row->reserved_cells_[i] = row_clone->cells_[reserved_columns.at(i)];
          }
        }
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObRowStore::BlockList::BlockList()
    :first_(NULL),
     last_(NULL),
     count_(0),
     used_mem_size_(0)
{
};

void ObRowStore::BlockList::reset()
{
  first_ = NULL;
  last_ = NULL;
  count_ = 0;
  used_mem_size_ = 0;
}

int ObRowStore::BlockList::add_last(BlockInfo *block)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(block)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(block));
  } else {
    block->next_ = NULL;
    block->prev_ = last_;
    if (OB_ISNULL(last_)) {
      if (OB_ISNULL(first_)) {
        first_ = block;
        last_ = block;
      } else {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "invalid block list", K(ret), K_(last), K_(first));
      }
    } else {
      last_->next_ = block;
      last_ = block;
    }
  }
  if (OB_SUCC(ret)) {
    used_mem_size_ += block->get_block_size();
    ++count_;
  }
  return ret;
}

ObRowStore::BlockInfo *ObRowStore::BlockList::pop_last()
{
  BlockInfo *block = last_;
  if (NULL != block) {
    if (NULL != block->prev_) {
      block->prev_->next_ = NULL;
      last_ = block->prev_;
    } else {
      // the only one
      first_ = NULL;
      last_ = NULL;
    }
    block->next_ = NULL;
    block->prev_ = NULL;
    --count_;
    used_mem_size_ -= block->get_block_size();
  }
  return block;
}

////////////////////////////////////////////////////////////////
ObRowStore::Iterator::Iterator()
  :row_store_(NULL),
   cur_iter_block_(NULL),
   cur_iter_pos_(0)
{
}

ObRowStore::Iterator::Iterator(const ObRowStore *row_store)
    : row_store_(row_store),
      cur_iter_block_(NULL),
      cur_iter_pos_(0)
{
}


//Why not reset row_store_, which does not satisfy the semantics of reset, and renamed to reuse?
void ObRowStore::Iterator::reset()
{
  // don't reset row_store_
  cur_iter_block_ = NULL;
  cur_iter_pos_ = 0;
}

void ObRowStore::Iterator::set_invalid()
{
  row_store_ = NULL;
  cur_iter_block_ = NULL;
  cur_iter_pos_ = 0;
}

int ObRowStore::Iterator::get_next_stored_row(StoredRow *&stored_row)
{
  int ret = OB_SUCCESS;
  stored_row = NULL;
  if (NULL == row_store_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row store is NULL", K(ret));
  } else {
    if (NULL == cur_iter_block_) {
      cur_iter_block_ = row_store_->blocks_.get_first();
      cur_iter_pos_ = 0;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(next_iter_pos(cur_iter_block_, cur_iter_pos_))) {
    if (OB_ITER_END != ret) {
      OB_LOG(WARN, "failed to get next pos", K(ret));
    }
  } else if (OB_ISNULL(cur_iter_block_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "invalid cur iter block", K(ret), K(cur_iter_block_));
  } else {
    stored_row = const_cast<StoredRow*>(reinterpret_cast<const StoredRow *>
                                          (cur_iter_block_->get_buffer_head() + cur_iter_pos_));
    // next
    cur_iter_pos_ += (row_store_->get_reserved_cells_size(stored_row->reserved_cells_count_)
                      + stored_row->compact_row_size_);
  } //end else
  return ret;
}

int ObRowStore::Iterator::get_next_row(ObNewRow &row)
{
  return get_next_row(row, NULL, NULL);
}

int ObRowStore::Iterator::get_next_row(ObNewRow &row,
                                       common::ObString *compact_row,
                                       StoredRow **r_stored_row)
{
  int ret = OB_SUCCESS;
  StoredRow *stored_row = NULL;
  if (OB_ISNULL(row_store_)) {
    OB_LOG(WARN, "row_store_ is NULL, should not reach here");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(row.count_ < row_store_->get_col_count())) {
    ret = OB_BUF_NOT_ENOUGH;
    OB_LOG(WARN, "column buffer count is not enough", K_(row.count),
               K(row_store_->get_col_count()));
  } else if (OB_FAIL(get_next_stored_row(stored_row))) {
    // do nothing
  } else if (OB_ISNULL(stored_row)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "stored_row should not be NULL", K(ret));
  } else if (OB_FAIL(ObRowUtil::convert(stored_row->get_compact_row(), row))) {
    OB_LOG(WARN, "fail to convert compact row to ObRow", K(ret));
  } else {
    if (NULL != r_stored_row) {
      *r_stored_row = stored_row;
    }
    if (NULL != compact_row) {
      *compact_row = stored_row->get_compact_row();
    }
  }
  return ret;
}

int ObRowStore::Iterator::get_next_row(ObNewRow *&row, StoredRow **r_stored_row)
{
  int ret = OB_SUCCESS;
  StoredRow *stored_row = NULL;
  if (OB_ISNULL(row_store_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(ERROR, "row_store_ is NULL", K(ret));
  } else if (row_store_->use_compact_row()) {
    ret = OB_NOT_SUPPORTED;
    OB_LOG(WARN, "this row store uses compact_row and you should not use this iterator", K(ret));
  } else if (OB_FAIL(get_next_stored_row(stored_row))) {
    // do nothing
  } else if (OB_ISNULL(stored_row)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "stored_row should not be NULL", K(ret));
  } else {
    row = reinterpret_cast<ObNewRow*>(stored_row->get_compact_row_ptr());
    if (NULL != r_stored_row) {
      *r_stored_row = stored_row;
    }
  }
  return ret;
}

int ObRowStore::Iterator::next_iter_pos(const BlockInfo *&iter_block, int64_t &iter_pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter_block)) {
    ret = OB_ITER_END;
  } else if (0 >= iter_block->get_remain_size_for_read(iter_pos)) {
    if (OB_ISNULL(iter_block->get_next_block())) {
      // the last block
      ret = OB_ITER_END;
    } else {
      // next block
      iter_block = iter_block->get_next_block();
      iter_pos = 0;
    }
  } else {}
  return ret;
}

bool ObRowStore::Iterator::has_next() const
{
  return (NULL != row_store_
          && !((cur_iter_block_ == NULL)
               || (cur_iter_block_ == row_store_->blocks_.get_last()
                   && cur_iter_block_->get_remain_size_for_read(cur_iter_pos_) <= 0)));
}

////////////////////////////////////////////////////////////////
ObRowStore::ObRowStore(ObIAllocator &alloc,
                       const lib::ObLabel &label/*=ObModIds::OB_SQL_ROW_STORE*/,
                       const uint64_t tenant_id/*=OB_INVALID_TENANT_ID*/,
                       bool use_compact_row /*= true*/)
:   inner_alloc_(),
    reserved_columns_(alloc),
    blocks_(),
    block_size_(NORMAL_BLOCK_SIZE),
    data_size_(0),
    row_count_(0),
    col_count_(0),
    last_last_row_size_(-1),
    last_row_size_(-1),
    tenant_id_(tenant_id),
    label_(label),
    ctx_id_(ObCtxIds::DEFAULT_CTX_ID),
    is_read_only_(false),
    has_big_block_(false),
    use_compact_row_(use_compact_row),
    alloc_(alloc),
    pre_project_buf_(NULL),
    pre_alloc_block_(false)
{
  inner_alloc_.set_label(label);
}
ObRowStore::ObRowStore(const lib::ObLabel &label/*=ObModIds::OB_SQL_ROW_STORE*/,
                       const uint64_t tenant_id/*=OB_INVALID_TENANT_ID*/,
                       bool use_compact_row /*= true*/)
:   inner_alloc_(),
    reserved_columns_(inner_alloc_),
    blocks_(),
    block_size_(NORMAL_BLOCK_SIZE),
    data_size_(0),
    row_count_(0),
    col_count_(0),
    last_last_row_size_(-1),
    last_row_size_(-1),
    tenant_id_(tenant_id),
    label_(label),
    ctx_id_(ObCtxIds::DEFAULT_CTX_ID),
    is_read_only_(false),
    has_big_block_(false),
    use_compact_row_(use_compact_row),
    alloc_(inner_alloc_),
    pre_project_buf_(NULL),
    pre_alloc_block_(false)
{
  inner_alloc_.set_label(label);
}


ObRowStore::~ObRowStore()
{
  clear_rows();
}

int ObRowStore::add_reserved_column(int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > index)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(index));
  } else if (OB_FAIL(reserved_columns_.push_back(index))) {
    OB_LOG(WARN, "failed to push into array", K(ret));
  }
  return ret;
}

void ObRowStore::reset()
{
  clear_rows();
  reserved_columns_.reset();
  pre_alloc_block_ = false;
}

// method for ObAggregateFunction::prepare()
// prepare need to reuse ObRowStore for WRITE,
// it needs to reuse reserved_columns_ which should not be cleared
void ObRowStore::clear_rows()
{
  data_size_ = 0;
  row_count_ = 0;
  col_count_ = 0;
  last_last_row_size_ = -1;
  last_row_size_ = -1;
  is_read_only_ = false;
  has_big_block_ = false;

  // free all blocks
  BlockInfo *block = blocks_.get_first();
  while (NULL != block) {
    BlockInfo *next = block->get_next_block();
    block->~BlockInfo();
    alloc_.free(block);
    block = next;
  }
  blocks_.reset();
}

/*
 * 小心使用，不合理的使用会造成内存泄漏，
 * 根据alloc_和自己使用场景的内存管理来决定是否使用此函数,
 * 这个函数保留最新分配的block，删除其他的block，效果类似于reuse
*/
void ObRowStore::reuse_hold_one_block()
{
  data_size_ = 0;
  row_count_ = 0;
  col_count_ = 0;
  last_last_row_size_ = -1;
  last_row_size_ = -1;
  is_read_only_ = false;
  has_big_block_ = false;

  BlockInfo *hold_block = blocks_.get_first();
  if (NULL != hold_block) {
    BlockInfo *block = hold_block->get_next_block();
    while (NULL != block) {
      BlockInfo *next = block->get_next_block();
      block->~BlockInfo();
      alloc_.free(block);
      block = next;
    }
    blocks_.reset();
    hold_block->set_next_block(NULL);
    hold_block->set_curr_data_pos(0);
    blocks_.add_last(hold_block);
  } else {
  	blocks_.reset();
  }
}

int ObRowStore::assign_block(char *buf, int64_t block_size)
{
  int ret = OB_SUCCESS;
  BlockInfo *block = (BlockInfo *)(buf);
  if (OB_ISNULL(block)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else {
    block = new(block) BlockInfo(block_size);
    pre_alloc_block_ = true;
    block_size_ = block_size;
    ret = blocks_.add_last(block);
  }
  return ret;
}

ObRowStore::BlockInfo* ObRowStore::new_block(int64_t block_size)
{
  BlockInfo *block = NULL;
  // normalize block size
  if (block_size > BIG_BLOCK_SIZE) {
    block_size = OB_MAX_ROW_LENGTH_IN_MEMTABLE;
  } else if (block_size > NORMAL_BLOCK_SIZE) {
    block_size = BIG_BLOCK_SIZE;
  } else {
    block_size = NORMAL_BLOCK_SIZE;
  }
  // make sure all memory allocated under the right tenant
  ObMemAttr attr(tenant_id_, label_, ctx_id_);
  block = static_cast<BlockInfo *>(alloc_.alloc(block_size, attr));
  if (OB_ISNULL(block)) {
    OB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "no memory");
  } else {
    block = new(block) BlockInfo(block_size);
    if (!has_big_block_ && block_size == BIG_BLOCK_SIZE) {
      has_big_block_ = true;
    }
  }
  return block;
}

// If there are already big blocks, then all new blocks are big.
ObRowStore::BlockInfo *ObRowStore::new_block()
{
  BlockInfo *blk = NULL;
  if (OB_LIKELY(!has_big_block_)) { // use config BLOCK size
    if (OB_ISNULL(blk = new_block(block_size_))) {
      OB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "failed to new block", K_(block_size));
    }
  } else { // use BIG_BLOCK
    if (OB_ISNULL(blk = new_block(BIG_BLOCK_SIZE))) {
      OB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "failed to new big block", K(blk));
    }
  }
  return blk;
}

int ObRowStore::add_row(const ObNewRow &row,
                        const StoredRow *&stored_row,
                        int64_t payload,
                        bool by_projector)
{
  int ret = OB_SUCCESS;
  if (!by_projector) {
    //Remove row's projector
    ObNewRow tmp_row;
    tmp_row.cells_ = row.cells_;
    tmp_row.count_ = row.count_;
    ret = add_row_by_projector(tmp_row, stored_row, payload);
  } else {
    ret = add_row_by_projector(row, stored_row, payload);
  }
  return ret;
}

int ObRowStore::add_row(const ObNewRow &row, bool by_projector)
{
  const StoredRow *stored_row = NULL; // value ignored
  return add_row(row, stored_row, 0, by_projector);
}

int ObRowStore::add_row_by_projector(const ObNewRow &row,
                                     const StoredRow *&stored_row,
                                     int64_t payload)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_read_only_)) {
    ret = OB_ERR_READ_ONLY;
    OB_LOG(WARN, "read only ObRowStore, not allowed to add row", K(ret));
  } else if (OB_UNLIKELY(0 < col_count_) && OB_UNLIKELY(row.get_count() != col_count_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "all rows should have the same columns", K(col_count_), K(row.get_count()));
  } else {
    stored_row = NULL;
    const int32_t reserved_columns_count = static_cast<int32_t>(reserved_columns_.count());
    int64_t min_need_size = get_compact_row_min_size(row.get_count()) +
        get_reserved_cells_size(reserved_columns_count);
    BlockInfo *block = NULL;
    if (OB_ISNULL(blocks_.get_last()) || blocks_.get_last()->get_remain_size() <= min_need_size) {
      if (pre_alloc_block_) {
        ret = OB_SIZE_OVERFLOW;
      } else if (OB_ISNULL(block = new_block())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(ERROR, "failed to new block", K(ret));
      }
    } else {
      block = blocks_.get_last();
    }
    if (OB_SUCC(ret) && OB_ISNULL(block)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "fail to get valid block", K(ret), K(block));
    }
    // try three times:
    // first, try to append into the current block
    // if failed, then try to append into a new NORMAL block
    // if still failed, then append into a new BIG block
    // if the row is larger than the BIG block, it should fail anyway
    int64_t retry = 0;
    StoredRow *stored = NULL;
    while (OB_SUCC(ret) && retry <= 3) {
      if (use_compact_row_) {
        ret = block->append_row(reserved_columns_, row, payload, stored);
      } else {
        if (NULL != row.projector_) {
          if (OB_UNLIKELY(NULL == pre_project_buf_)) {
            if (OB_FAIL(init_pre_project_row(row.get_count()))) {
              OB_LOG(WARN, "fail to init pre project row", K(ret), K(block));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(pre_project(row))) {
              OB_LOG(WARN, "fail to pre-project row", K(ret), K(block));
            } else {
              ret = block->append_row_by_copy(reserved_columns_, pre_project_row_, payload, stored);
            }
          }
        } else {
          ret = block->append_row_by_copy(reserved_columns_, row, payload, stored);
        }
      }
      if (OB_FAIL(ret)) {
        if (OB_BUF_NOT_ENOUGH == ret) {
          if (pre_alloc_block_) {
            ret = OB_SIZE_OVERFLOW;
          } else {
            ret = OB_SUCCESS;
            ++retry;
            if (block->is_empty()) {
              // a fresh block is not large enough, free it and new a BIG one
              int64_t big_block_size = block->get_block_size() >= BIG_BLOCK_SIZE ? OB_MAX_ROW_LENGTH_IN_MEMTABLE : BIG_BLOCK_SIZE;
              if (block == blocks_.get_last()) { // to be robust
                blocks_.pop_last();
              }
              alloc_.free(block);
              if (OB_ISNULL(block = new_block(big_block_size))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                OB_LOG(ERROR, "failed to new block", K(ret));
              }
            } else if (OB_ISNULL(block = new_block())) {  // normal case
              ret = OB_ALLOCATE_MEMORY_FAILED;
              OB_LOG(ERROR, "failed to new block", K(ret));
            }
          }
        } else {
          OB_LOG(WARN, "failed to append row", K(ret));
        }
      } else if (OB_ISNULL(stored)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "get invalid stored row", K(ret), K(stored));
      } else {
        if (block != blocks_.get_last()) {
          // new block
          ret = blocks_.add_last(block);
        }
        stored_row = stored;
        ++row_count_;
        last_last_row_size_ = last_row_size_;
        last_row_size_ = stored_row->compact_row_size_ +
            get_reserved_cells_size(stored_row->reserved_cells_count_);
        data_size_ += last_row_size_;
        if (0 >= col_count_) {
          col_count_ = row.get_count();
        }
        break;                  // done
      }
    } // end while
    if (OB_SUCC(ret) && 3 < retry) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(ERROR, "unexpected branch");
    }
  }
  return ret;
}

int ObRowStore::init_pre_project_row(int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == pre_project_buf_)) {
    if (OB_UNLIKELY(count <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid count", K(count), K(ret));
    } else {
      ObMemAttr attr(tenant_id_, label_, ctx_id_);
      pre_project_buf_ = static_cast<ObObj *>(
          alloc_.alloc(count * sizeof(ObObj), attr));
      if (OB_UNLIKELY(NULL == pre_project_buf_)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "invalid count", K(count), K(ret));
      } else {
        memset(pre_project_buf_, 0, count * sizeof(ObObj));
        pre_project_row_.assign(pre_project_buf_, count);
      }
    }
  }
  return ret;
}

int ObRowStore::pre_project(const ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row.is_invalid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid row", K(row), K(ret));
  } else {
    int64_t count = row.get_count();
    for (int64_t i = 0; i < count; ++i) {
      pre_project_buf_[i] = row.get_cell(i);
    }
  }
  return ret;
}

int ObRowStore::rollback_last_row()
{
  int ret = OB_SUCCESS;
  // only support add_row->rollback->add_row->rollback->add_row->add_row->rollback
  // NOT support  ..->rollback->rollback->...
  BlockInfo *last = blocks_.get_last();
  if (0 >= last_row_size_
      || (0 < last_row_size_ && 0 >= last_last_row_size_ && 1 < row_count_)/*already rollbacked once*/) {
    ret = OB_NOT_SUPPORTED;
    OB_LOG(WARN, "only one row could be rollback after called add_row() once");
  } else if (OB_ISNULL(last)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "last blockinfo is null ,can't rollback", K(ret), K(last));
  } else {
    last->advance(-last_row_size_);
    if (OB_UNLIKELY(0 > last->get_curr_data_pos())) {
      ret = OB_ERR_SYS;
      OB_LOG(ERROR, "out of memory range, ",
          K(ret), "block_size", last->get_block_size(), "curr_data_pos", last->get_curr_data_pos(),
          K_(last_row_size));
    } else {
      --row_count_;
      if (last->is_empty()) {
        blocks_.pop_last();
        alloc_.free(last);
        last = NULL;
      }
      last_row_size_ = last_last_row_size_;
      last_last_row_size_ = -1;
    }
  }
  return ret;
}

int ObRowStore::get_last_row(const StoredRow *&stored_row) const
{
  int ret = OB_SUCCESS;
  if (0 >= last_row_size_) {
    ret = OB_ENTRY_NOT_EXIST;
    OB_LOG(WARN, "has no last stored row");
  } else if (OB_ISNULL(blocks_.get_last())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "fail to get last row", K(ret), K(blocks_.get_last()));
  } else if (OB_UNLIKELY(0 > blocks_.get_last()->get_curr_data_pos() - last_row_size_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "wrong last row store size", K(ret), K(last_row_size_),
           K(blocks_.get_last()->get_curr_data_pos()));
  } else {
    stored_row = reinterpret_cast<const StoredRow *>(
        blocks_.get_last()->get_buffer(blocks_.get_last()->get_curr_data_pos() - last_row_size_));
  }
  return ret;
}

ObRowStore::Iterator ObRowStore::begin() const
{
  return Iterator(this);
}

void ObRowStore::dump() const
{
  int ret = OB_SUCCESS;
  OB_LOG(DEBUG, "DUMP row store:", K(*this));
  StoredRow *stored_row = NULL;
  ObNewRow row;
  static const int64_t INLINE_OBJS_SIZE = 16;
  ObObj inline_objs[INLINE_OBJS_SIZE];
  row.cells_ = inline_objs;
  char *buf = NULL;
  if (INLINE_OBJS_SIZE < col_count_) {
    ObMemAttr attr(tenant_id_, label_);
    buf = static_cast<char *>(ob_malloc(sizeof(ObObj) * col_count_, attr));
    if (OB_ISNULL(buf)) {
      OB_LOG(WARN, "failed to alloc memory for objs");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      row.cells_ = new(buf) ObObj[col_count_];
    }
  }
  if (OB_SUCC(ret)) {
    row.count_ = static_cast<int32_t>(col_count_);
    Iterator it = begin();
    int64_t row_count = 1;
    while (OB_SUCCESS == (ret = it.get_next_row(row, NULL, &stored_row))) {
      OB_LOG(DEBUG, "DUMP row", K(row_count), K(*stored_row));
      OB_LOG(DEBUG, "DUMP row data", K(row_count), K(row));
      ++row_count;
    }
  }
  if (NULL != buf) {
    ob_free(buf);
  }
}

int32_t ObRowStore::get_copy_size() const
{
  return static_cast<int32_t>(data_size_ +
                              sizeof(BlockInfo) * blocks_.get_block_count() + get_meta_size());
}

ObRowStore *ObRowStore::clone(char *buffer, int64_t buffer_length) const
{
  ObRowStore *copy = NULL;
  int64_t used_length = 0;
  if (OB_ISNULL(buffer)) {
    OB_LOG_RET(WARN, OB_INVALID_ARGUMENT, "buffer is NULL");
  } else if (0 < reserved_columns_.count()) {
    OB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "row store with reserved columns should not be cloned");
  } else if (OB_UNLIKELY(buffer_length < get_meta_size())) {
    OB_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid buffer length", K(buffer_length), K(get_meta_size()));
  } else {
    copy = new(buffer) ObRowStore();
    if (OB_ISNULL(copy)) {
      OB_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "fail to new ObRowStore", K(copy));
    } else {
      used_length += get_meta_size();
      copy->is_read_only_ = true;
      copy->block_size_ = block_size_;
      copy->data_size_ = data_size_;
      copy->row_count_ = row_count_;
      copy->col_count_ = col_count_;
      copy->tenant_id_ = tenant_id_;
      copy->label_ = label_;

      buffer += get_meta_size();
      const BlockInfo *bip = blocks_.get_first();
      BlockInfo *tmp = NULL;
      while (NULL != bip) {
        const int64_t sz = sizeof(BlockInfo) + bip->get_curr_data_pos();
        used_length += sz;
        if (OB_UNLIKELY(buffer_length < used_length)) {
          copy = NULL;
          OB_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid buffer length", K(buffer_length), K(used_length));
          break;
        }
        MEMCPY(buffer, bip, sz);
        tmp = new(buffer) BlockInfo(sz);
        if (OB_ISNULL(tmp)) {
          OB_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "fail to new BlockInfo");
          copy = NULL;
          break;
        } else {
          tmp->set_curr_data_pos(bip->get_curr_data_pos());
          int64_t tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = copy->blocks_.add_last(tmp))) {
            OB_LOG_RET(WARN, tmp_ret, "fail to add last to block", K(tmp_ret));
            copy = NULL;
            break;
          } else {
            buffer += sz;
            bip = bip->get_next_block();
          }
        }
      }
    }
  }
  return copy;
}
// @todo improve performance, copy by block
int ObRowStore::assign(const ObRowStore &other_store)
{
  int ret = OB_SUCCESS;
  ObRowStore::Iterator row_store_it = other_store.begin();
  ObNewRow cur_row;
  int64_t col_count = other_store.get_col_count();
  ObObj *cell = NULL;
  clear_rows();
  tenant_id_ = other_store.tenant_id_;
  use_compact_row_ = other_store.use_compact_row_;

  if (OB_FAIL(set_col_count(col_count))) {
    OB_LOG(WARN, "fail to set rowstore columns count", K(ret));
  } else if (OB_ISNULL(cell = static_cast<ObObj *>(alloca(sizeof(ObObj) * col_count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc obj array", K(ret));
  } else {
    cur_row.cells_ = cell;
    cur_row.count_ = col_count;
    while (true) {
      if (OB_FAIL(row_store_it.get_next_row(cur_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          OB_LOG(WARN, "fail to get next row", K(ret));
        }
        break;
      } else if (OB_FAIL(add_row(cur_row))) {
        OB_LOG(WARN, "fail to add row", K(ret));
        break;
      }
    }
  }
  return ret;
}

int ObRowStore::adjust_row_cells_reference()
{
  int ret = OB_SUCCESS;
  //If row compact is not used, the complete data structure of ObNewRow is stored in the data block.
  //The serialization of row_store is the complete serialization of the records on the data block
  //And there is a pointer in ObNewRow, so the value of the pointer becomes illegal after deserialization
  //So if row compact is used, the cells_ pointer in ObNewRow needs to be adjusted after deserialization
  //The adjustment rule is that the memory of ObNewRow and obj cells_ are arranged adjacently, so you only need to move the row ptr forward by sizeof(ObNewRow).
  //Is the value of obj cells_
  if (OB_UNLIKELY(!use_compact_row_)) {
    ObNewRow *cur_row = NULL;
    StoredRow *stored_row = NULL;
    ObRowStore::Iterator row_store_it = begin();
    while (OB_SUCC(ret)) {
      if (OB_FAIL(row_store_it.get_next_stored_row(stored_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next stored row from row store failed", K(ret));
        }
      } else {
        int64_t pos = 0;
        char *compact_row = reinterpret_cast<char*>(stored_row->get_compact_row_ptr());
        if (OB_FAIL(ObNewRow::construct(compact_row, stored_row->compact_row_size_, pos, cur_row))) {
          LOG_WARN("construct row failed", K(ret), K(stored_row->compact_row_size_), K(pos));
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

DEFINE_SERIALIZE(ObRowStore)
{
  int ret = OB_SUCCESS;
  int64_t count = blocks_.get_block_count();
  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, count))) {
    OB_LOG(WARN, "failed to serialize", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    const BlockInfo *block = blocks_.get_first();

    while (NULL != block && OB_SUCC(ret)) {
      // serialize data_buf_size
      if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, block->get_curr_data_pos()))) {
        OB_LOG(WARN, "fail to serialize data_buffer_size",
            K(ret), KP(buf), K(buf_len), K(pos));
      } else {
        // serialize block data
        if (buf_len - pos < block->get_curr_data_pos()) {
          ret = OB_SIZE_OVERFLOW;
        } else {
          MEMCPY(buf + pos, block->get_buffer_head(), block->get_curr_data_pos());
          pos += block->get_curr_data_pos();
          // serialize next block
          block = block->get_next_block();
        }
      }
    } //end while
  }

  OB_UNIS_ENCODE(block_size_);
  OB_UNIS_ENCODE(row_count_);
  OB_UNIS_ENCODE(col_count_);
  OB_UNIS_ENCODE(is_read_only_);
  OB_UNIS_ENCODE(last_row_size_);
  OB_UNIS_ENCODE(tenant_id_);
  OB_UNIS_ENCODE(use_compact_row_);
  return ret;
}


DEFINE_DESERIALIZE(ObRowStore)
{
  int ret = OB_SUCCESS;
  int64_t old_pos = pos;
  int64_t block_size = 0;
  int64_t data_buffer_size = 0;
  int64_t block_count = 0;
  BlockInfo *block = NULL;
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &block_count))) {
    OB_LOG(WARN, "failed to serialize", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    reuse();
    is_read_only_ = true;
    for (int64_t i = 0; i < block_count && OB_SUCC(ret); ++i) {
      if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &data_buffer_size))) {
        OB_LOG(WARN, "failed to deserialize data_buffer_size",
            K(ret), KP(buf), K(data_len), K(pos));
      } else {
        block_size = data_buffer_size + sizeof(BlockInfo);
        if (OB_ISNULL(block = new_block(block_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          OB_LOG(ERROR, "fail to allocate new block", K(ret), K(block_size));
        } else if (block->get_remain_size() >= data_buffer_size) {
          // copy data to store
          MEMCPY(block->get_buffer(), buf + pos, data_buffer_size);
          block->advance(data_buffer_size);
          pos += data_buffer_size;
          data_size_ += data_buffer_size;
          if (OB_FAIL(blocks_.add_last(block))) {
            OB_LOG(WARN, "fail to add last to block", K(ret));
          }
        } else {
          ret = OB_BUF_NOT_ENOUGH;
          OB_LOG(WARN, "block is not enough, ",
              K(ret), "remain_size", block->get_remain_size(), K(data_buffer_size));
        }
      }
    } // end for
  }

  OB_UNIS_DECODE(block_size_);
  OB_UNIS_DECODE(row_count_);
  OB_UNIS_DECODE(col_count_);
  OB_UNIS_DECODE(is_read_only_);
  OB_UNIS_DECODE(last_row_size_);
  OB_UNIS_DECODE(tenant_id_);
  OB_UNIS_DECODE(use_compact_row_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(adjust_row_cells_reference())) {
      LOG_WARN("adjust row cells reference failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    pos = old_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObRowStore)
{
  int64_t count = blocks_.get_block_count();
  int64_t len = serialization::encoded_length_vi64(count);
  const BlockInfo *block = blocks_.get_first();
  while (NULL != block) {
    len += serialization::encoded_length_vi64(block->get_curr_data_pos());
    len += block->get_curr_data_pos();
    block = block->get_next_block();
  }
  OB_UNIS_ADD_LEN(block_size_);
  OB_UNIS_ADD_LEN(row_count_);
  OB_UNIS_ADD_LEN(col_count_);
  OB_UNIS_ADD_LEN(is_read_only_);
  OB_UNIS_ADD_LEN(last_row_size_);
  OB_UNIS_ADD_LEN(tenant_id_);
  OB_UNIS_ADD_LEN(use_compact_row_);
  return len;
}
} //end common
} //end oceanbase
