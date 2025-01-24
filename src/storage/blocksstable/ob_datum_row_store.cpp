/**
 * Copyright (c) 2024 OceanBase
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
#include "ob_datum_row_store.h"
namespace oceanbase
{
namespace blocksstable
{

////////////////////////////////////////////////////////////////
struct ObDatumRowStore::BlockInfo
{
  explicit BlockInfo(int64_t block_size)
      :magic_(0xabcd4444abcd4444),
       next_(NULL),
       curr_data_pos_(0),
       block_size_(block_size)
  {
  }
  OB_INLINE int64_t get_remain_size() const { return block_size_ - curr_data_pos_; }
  OB_INLINE int64_t get_remain_size_for_read(int64_t pos) const { return curr_data_pos_ - pos; }
  OB_INLINE char *get_buffer() { return data_ + curr_data_pos_; }
  OB_INLINE const char *get_buffer_head() const { return data_; }
  OB_INLINE void advance(const int64_t length) { curr_data_pos_ += length; }
  OB_INLINE BlockInfo *get_next_block() { return next_; }
  OB_INLINE const BlockInfo *get_next_block() const { return next_; }
  OB_INLINE int64_t get_block_size() const { return block_size_; };

  int append_row(const ObDatumRow &row, const int64_t length);
  friend class ObDatumRowStore::BlockList;
private:
#ifdef __clang__
  int64_t magic_ [[gnu::unused]];
#else
  int64_t magic_;
#endif
  BlockInfo *next_;
  /**
   * cur_data_pos_ must be set when BlockInfo deserialized
   */
  int64_t curr_data_pos_;
  int64_t block_size_;
  char data_[0];
};

int ObDatumRowStore::BlockInfo::append_row(const ObDatumRow &row, const int64_t length)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(0 > curr_data_pos_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "out of memory range",
        K(ret), K_(block_size), K_(curr_data_pos), K(length));
  } else if (OB_FAIL(row.serialize(get_buffer(), get_remain_size(), pos))) {
    STORAGE_LOG(WARN, "fail to serialize datum row", K(ret), K(row));
  } else {
    advance(pos);
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObDatumRowStore::BlockList::BlockList()
    :first_(NULL),
     last_(NULL),
     count_(0),
     used_mem_size_(0)
{
};

void ObDatumRowStore::BlockList::reset()
{
  first_ = NULL;
  last_ = NULL;
  count_ = 0;
  used_mem_size_ = 0;
}

int ObDatumRowStore::BlockList::add_last(BlockInfo *block)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(block)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(block));
  } else {
    block->next_ = NULL;
    if (OB_ISNULL(last_)) {
      if (OB_ISNULL(first_)) {
        first_ = block;
        last_ = block;
      } else {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid block list", K(ret), K_(last), K_(first));
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

////////////////////////////////////////////////////////////////
ObDatumRowStore::Iterator::Iterator(const ObDatumRowStore &row_store)
    : row_store_(row_store),
      cur_iter_block_(row_store_.blocks_.get_first()),
      cur_iter_pos_(0)
{
}

int ObDatumRowStore::Iterator::get_next_row(ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(row.count_ < row_store_.get_col_count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "column buffer count is not enough", K(ret), K_(row.count), K(row_store_.get_col_count()));
  } else if (OB_ISNULL(cur_iter_block_)) {
    // the last block
    ret = OB_ITER_END;
  } else if (OB_FAIL(row.deserialize(cur_iter_block_->get_buffer_head() + cur_iter_pos_, cur_iter_block_->get_remain_size_for_read(cur_iter_pos_), pos))) {
    STORAGE_LOG(WARN, "failed to deserialize datum row", K(ret), K(cur_iter_block_->get_block_size()), K(cur_iter_pos_),
                                                          K(row.get_serialize_size()), K(pos), K(row));
  } else {
    // next
    cur_iter_pos_ += pos;
    // update current block when current block reach its end
    if (cur_iter_block_->get_remain_size_for_read(cur_iter_pos_) <= 0) {
      // next block
      cur_iter_block_ = cur_iter_block_->get_next_block();
      cur_iter_pos_ = 0;
    }
  } // end else
  return ret;
}

////////////////////////////////////////////////////////////////
ObDatumRowStore::ObDatumRowStore()
:   inner_alloc_("DatumRowStore", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID),
    blocks_(),
    row_count_(0),
    col_count_(0)
{
}


ObDatumRowStore::~ObDatumRowStore()
{
  clear_rows();
}

void ObDatumRowStore::clear_rows()
{
  row_count_ = 0;
  col_count_ = 0;

  inner_alloc_.reset();
  blocks_.reset();
}

int ObDatumRowStore::new_block(int64_t block_size, ObDatumRowStore::BlockInfo *&block)
{
  int ret = OB_SUCCESS;
  // normalize block size
  if (block_size > BIG_BLOCK_SIZE) {
    block_size = OB_MAX_ROW_LENGTH_IN_MEMTABLE;
  } else if (block_size > NORMAL_BLOCK_SIZE) {
    block_size = BIG_BLOCK_SIZE;
  } else {
    block_size = NORMAL_BLOCK_SIZE;
  }
  // make sure all memory allocated under the right tenant
  block = static_cast<BlockInfo *>(inner_alloc_.alloc(block_size + sizeof(BlockInfo)));
  if (OB_ISNULL(block)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc block memory", K(ret), K(block_size), K(sizeof(BlockInfo)));
  } else {
    block = new(block) BlockInfo(block_size);
    if (OB_FAIL(blocks_.add_last(block))) {
      STORAGE_LOG(WARN, "failed to add a new block to block list", K(ret));
    }
  }
  return ret;
}

int ObDatumRowStore::add_row(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 < col_count_) && OB_UNLIKELY(row.count_ != col_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "all rows should have the same columns", K(col_count_), K(row.count_));
  } else {
    int64_t length = row.get_serialize_size();
    BlockInfo *block = blocks_.get_last();
    if (OB_ISNULL(block) || block->get_remain_size() < length) {
      if (OB_FAIL(new_block(length, block))) {
        STORAGE_LOG(WARN, "failed to new block", K(ret), K(length));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(block->append_row(row, length))) {
        STORAGE_LOG(WARN, "failed to append row", K(ret), K(row));
      } else {
        ++row_count_;
      }
    }
  }
  return ret;
}

ObDatumRowStore::Iterator ObDatumRowStore::begin() const
{
  return Iterator(*this);
}

} //end common
} //end oceanbase
