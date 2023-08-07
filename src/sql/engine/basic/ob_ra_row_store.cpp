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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_ra_row_store.h"
#include "lib/container/ob_se_array_iterator.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/config/ob_server_config.h"


namespace oceanbase
{
using namespace common;

namespace sql
{

int ObRARowStore::ShrinkBuffer::init(char *buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    data_ = buf;
    head_ = 0;
    tail_ = buf_size;
    cap_ = buf_size;
  }
  return ret;
}

namespace ra_row_store {

template <typename T, typename B>
void pointer2off(T *&pointer, B *base)
{
  pointer = reinterpret_cast<T *>(
      reinterpret_cast<const char *>(pointer) - reinterpret_cast<const char *>(base));
}

template <typename T, typename B>
void off2pointer(T *&pointer, B *base)
{
  pointer = reinterpret_cast<T *>(
      reinterpret_cast<intptr_t>(pointer) + reinterpret_cast<char *>(base));
}

}

int64_t ObRARowStore::StoreRow::row_copy_size(const ObNewRow &row)
{
  int64_t size = sizeof(common::ObObj) * row.get_count();
  for (int64_t i = 0; i < row.get_count(); ++i) {
    size += row.get_cell(i).get_deep_copy_size();
  }
  return size;
}

int ObRARowStore::StoreRow::copy_row(const ObNewRow &r, char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (payload_ != buf || size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(payload_), KP(buf), K(size));
  } else {
    readable_ = true;
    cnt_ = ~(1U << 31) & static_cast<int32>(r.get_count());
    int64_t pos = sizeof(ObObj) * cnt_;
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; ++i) {
      new (&cells()[i])ObObj();
      if (OB_FAIL(cells()[i].deep_copy(r.get_cell(i), buf, size, pos))) {
        LOG_WARN("copy cell failed", K(ret));
      }
    }
  }

  return ret;
}

int ObRARowStore::StoreRow::to_copyable()
{
  int ret = OB_SUCCESS;
  if (0 != readable_) {
    for (int64_t i = 0; i < cnt_; ++i) {
      if (cells()[i].need_deep_copy()) {
        ra_row_store::pointer2off(cells()[i].v_.string_, this);
      }
    }
    readable_ = false;
  }
  return ret;
}

int ObRARowStore::StoreRow::to_readable()
{
  int ret = OB_SUCCESS;
  if (0 == readable_) {
    for (int64_t i = 0; i < cnt_; ++i) {
      if (cells()[i].need_deep_copy()) {
        ra_row_store::off2pointer(cells()[i].v_.string_, this);
      }
    }
    readable_ = true;
  }
  return ret;
}

int ObRARowStore::Block::add_row(
    ShrinkBuffer &buf, const ObNewRow &row, const int64_t row_size)
{
  int ret = OB_SUCCESS;
  if (!buf.is_inited() || row_size <= ROW_INDEX_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(row_size));
  } else if (row_size > buf.remain()) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer not enough", K(row_size), "remain", buf.remain());
  } else {
    StoreRow *sr = new (buf.head())StoreRow;
    if (OB_FAIL(sr->copy_row(
        row, buf.head() + sizeof(*sr), row_size - sizeof(*sr) - ROW_INDEX_SIZE))) {
      LOG_WARN("copy row failed", K(ret), K(row), K(row_size));
    } else if (OB_FAIL(buf.fill_tail(ROW_INDEX_SIZE))) {
      LOG_WARN("fill buffer tail failed", K(ret), K(buf), LITERAL_K(ROW_INDEX_SIZE));
    } else {
      *reinterpret_cast<row_idx_t *>(buf.tail()) = static_cast<row_idx_t>(
          buf.head() - payload_);
      idx_off_ -= static_cast<int32_t>(ROW_INDEX_SIZE);
      if (OB_FAIL(buf.fill_head(row_size - ROW_INDEX_SIZE))) {
        LOG_WARN("fill buffer head failed", K(ret), K(buf), K(row_size - ROW_INDEX_SIZE));
      } else {
        rows_++;
      }
    }
  }

  return ret;
}

int ObRARowStore::Block::get_store_row(const int64_t row_id, const StoreRow *&sr)
{
  int ret = OB_SUCCESS;
  if (!contain(row_id)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("invalid index", K(ret), K(row_id), K(*this));
  } else {
    StoreRow *row = reinterpret_cast<StoreRow *>(
        &payload_[indexes()[rows_ - (row_id - row_id_) - 1]]);
    if (0 == row->readable_) {
      if (OB_FAIL(row->to_readable())) {
        LOG_WARN("store row to readable failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      sr = row;
    }
  }
  return ret;
}

int ObRARowStore::Block::compact(ShrinkBuffer &buf)
{
  int ret = OB_SUCCESS;
  if (!buf.is_inited()) {
    LOG_WARN("invalid argument", K(ret), K(buf));
  } else if (OB_FAIL(buf.compact())) {
    LOG_WARN("block buffer compact failed", K(ret));
  } else {
    idx_off_ = static_cast<int32_t>(buf.head() - rows_ * ROW_INDEX_SIZE - payload_);
  }
  return ret;
}

int ObRARowStore::Block::to_copyable()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < rows_; ++i) {
    StoreRow *sr = reinterpret_cast<StoreRow *>(&payload_[indexes()[i]]);
    if (OB_FAIL(sr->to_copyable())) {
      LOG_WARN("convert store row to copyable row failed", K(ret));
    }
  }
  return ret;
}

ObRARowStore::ObRARowStore(common::ObIAllocator *alloc /* = NULL */,
    const bool keep_projector /* = false */ )
  : inited_(false), tenant_id_(0), label_(0), ctx_id_(0), mem_limit_(0),
    idx_blk_(NULL), save_row_cnt_(0), row_cnt_(0), fd_(-1), dir_id_(-1), file_size_(0),
    inner_reader_(*this), mem_hold_(0), allocator_(NULL == alloc ? inner_allocator_ : *alloc),
    keep_projector_(keep_projector), projector_(NULL), projector_size_(0)
{
}

int ObRARowStore::init(int64_t mem_limit,
    uint64_t tenant_id /* = common::OB_SERVER_TENANT_ID */,
    int64_t mem_ctx_id /* = common::ObCtxIds::DEFAULT_CTX_ID */,
    const char *label /* = common::ObNewModIds::OB_SQL_ROW_STORE) */)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    tenant_id_ = tenant_id;
    ctx_id_ = mem_ctx_id;
    label_ = label;
    mem_limit_ = mem_limit;
    inited_ = true;
  }
  return ret;
}

void ObRARowStore::reset()
{
  int ret = OB_SUCCESS;
  tenant_id_ = common::OB_SERVER_TENANT_ID;
  label_ = common::ObModIds::OB_SQL_ROW_STORE;
  ctx_id_ = common::ObCtxIds::DEFAULT_CTX_ID;
  mem_limit_ = 0;

  blkbuf_.reset();
  idx_blk_ = NULL;

  save_row_cnt_ = 0;
  row_cnt_ = 0;
  inner_reader_.reset();

  if (is_file_open()) {
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.remove(fd_))) {
      LOG_WARN("remove file failed", K(ret), K_(fd));
    } else {
      LOG_INFO("close file success", K(ret), K_(fd));
    }
    fd_ = -1;
    dir_id_ = -1;
    file_size_ = 0;
  }

  while (!blk_mem_list_.is_empty()) {
    LinkNode *node = blk_mem_list_.remove_first();
    if (NULL != node) {
      node->~LinkNode();
      allocator_.free(node);
    }
  }
  blocks_.reset();
  mem_hold_ = 0;
  if (NULL != projector_) {
    allocator_.free(projector_);
  }
  projector_ = NULL;
  projector_size_ = 0;
  inited_ = false;
}

void ObRARowStore::reuse()
{
  int ret = OB_SUCCESS;
  save_row_cnt_ = 0;
  row_cnt_ = 0;
  inner_reader_.reuse();
  if (is_file_open()) {
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.remove(fd_))) {
      LOG_WARN("remove file failed", K(ret), K_(fd));
    } else {
      LOG_INFO("close file success", K(ret), K_(fd));
    }
    fd_ = -1;
    dir_id_ = -1;
    file_size_ = 0;
  }
  idx_blk_ = NULL;
  DLIST_FOREACH_REMOVESAFE_NORET(node, blk_mem_list_) {
    if (&(*node) + 1 != static_cast<LinkNode *>(static_cast<void *>(blkbuf_.buf_.data()))) {
      node->unlink();
      node->~LinkNode();
      allocator_.free(node);
    }
  }
  if (NULL != blkbuf_.buf_.data()) {
    if (OB_FAIL(setup_block(blkbuf_))) {
      LOG_WARN("setup block failed", K(ret));
    }
    mem_hold_ = blkbuf_.buf_.capacity() + sizeof(LinkNode);
  }
  blocks_.reset();
}

int ObRARowStore::setup_block(BlockBuf &blkbuf) const
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!blkbuf.buf_.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("block buffer not inited", K(ret));
  } else {
    blkbuf.buf_.reuse();
    blkbuf.blk_ = new (blkbuf.buf_.head()) Block;
    blkbuf.blk_->row_id_ = row_cnt_;
    blkbuf.blk_->idx_off_ = static_cast<int32_t>(
        blkbuf.buf_.tail() - blkbuf.blk_->payload_);
    if (OB_FAIL(blkbuf.buf_.fill_head(sizeof(Block)))) {
      LOG_WARN("fill buffer head failed", K(ret), K(blkbuf.buf_), K(sizeof(Block)));
    }
  }
  return ret;
}

void *ObRARowStore::alloc_blk_mem(const int64_t size)
{
  void *blk = NULL;
  int ret = OB_SUCCESS;
  if (size < 0) {
    LOG_WARN("invalid argument", K(size));
  } else {
    ObMemAttr attr(tenant_id_, label_, ctx_id_);
    void *mem = allocator_.alloc(size + sizeof(LinkNode), attr);
    if (NULL == mem) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), KP(mem));
    } else {
      LinkNode *node = new (mem) LinkNode;
      if (!blk_mem_list_.add_last(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("add node to list failed", K(ret));
        node->~LinkNode();
        allocator_.free(mem);
      } else {
        blk = static_cast<char *>(mem) + sizeof(LinkNode);
        mem_hold_ += size + sizeof(LinkNode);
      }
    }
  }
  return blk;
}

void ObRARowStore::free_blk_mem(void *mem, const int64_t size /* = 0 */)
{
  if (NULL != mem) {
    LinkNode *node = static_cast<LinkNode *>(mem) - 1;
    if (NULL != node->get_next()) {
      node->unlink();
    }
    node->~LinkNode();
    allocator_.free(node);
    mem_hold_ -= (size + sizeof(LinkNode));
  }
}

int ObRARowStore::alloc_block(BlockBuf &blkbuf, const int64_t min_size)
{
  int ret = OB_SUCCESS;
  int64_t size = std::max(static_cast<int64_t>(BLOCK_SIZE), min_size);
  if (row_cnt_ > 0 || need_dump()) {
    size = std::max(size, static_cast<int64_t>(BIG_BLOCK_SIZE));
  }
  size += sizeof(LinkNode);
  size = next_pow2(size);
  size -= sizeof(LinkNode);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    void *mem = alloc_blk_mem(size);
    if (OB_ISNULL(mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), K(size));
    } else if (OB_FAIL(blkbuf.buf_.init(static_cast<char *>(mem), size))) {
      LOG_WARN("init shrink buffer failed", K(ret));
    } else if (OB_FAIL(setup_block(blkbuf))) {
      LOG_WARN("setup block buffer fail", K(ret));
    }
    if (OB_FAIL(ret) && !OB_ISNULL(mem)) {
      blkbuf.reset();
      free_blk_mem(mem, size);
    }
  }
  return ret;
}

int ObRARowStore::switch_block(const int64_t min_size)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (min_size < 0 || OB_ISNULL(blkbuf_.blk_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(min_size));
  } else if (OB_FAIL(blkbuf_.blk_->compact(blkbuf_.buf_))) {
    LOG_WARN("block compact failed", K(ret));
  } else {
    const bool finish_add = (0 == min_size);
    BlockBuf new_blkbuf;
    const bool force_new_block = (min_size > blkbuf_.buf_.capacity())
        || !GCONF.is_sql_operator_dump_enabled();
    BlockIndex bi;
    bi.is_idx_block_ = false;
    bi.on_disk_ = false;
    bi.row_id_ = ~(0b11UL << 62) & save_row_cnt_;
    bi.blk_ = blkbuf_.blk_;
    bi.length_ = static_cast<int32_t>(blkbuf_.buf_.head_size());
    bool dump = need_dump();
    if (!finish_add && (force_new_block || !dump)) { // need alloc new block
      if (OB_FAIL(alloc_block(new_blkbuf, min_size))) {
        LOG_WARN("alloc block failed", K(ret), K(min_size));
        if (!force_new_block) {
          dump = true;
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_SUCC(ret) && dump) {
      if (OB_FAIL(blkbuf_.blk_->to_copyable())) {
        LOG_WARN("convert block to copyable failed", K(ret));
      } else {
        if (OB_FAIL(write_file(bi, blkbuf_.buf_.data(), blkbuf_.buf_.head_size()))) {
          LOG_WARN("write block to file failed");
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(add_block_idx(bi))) {
      LOG_WARN("add block index failed", K(ret));
    } else {
      save_row_cnt_ = row_cnt_;
      if (!dump) {
        blkbuf_.reset();
      }

      if (new_blkbuf.buf_.is_inited()) {
        if (blkbuf_.buf_.is_inited()) {
          // free block, if block dumped and need bigger block
          free_blk_mem(blkbuf_.buf_.data(), blkbuf_.buf_.capacity());
          blkbuf_.reset();
        }
        blkbuf_ = new_blkbuf;
        new_blkbuf.reset();
      } else if (blkbuf_.buf_.is_inited()) {
        if (OB_FAIL(setup_block(blkbuf_))) {
          LOG_WARN("setup block failed", K(ret));
        }
      }
    }
    if (OB_FAIL(ret) && new_blkbuf.buf_.is_inited()) {
      free_blk_mem(new_blkbuf.blk_, new_blkbuf.buf_.capacity());
      new_blkbuf.reset();
    }
  }
  return ret;
}

int ObRARowStore::add_block_idx(const BlockIndex &bi)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (NULL == idx_blk_) {
      if (OB_FAIL(blocks_.push_back(bi))) {
        LOG_WARN("add block index to array failed", K(ret));
      } else {
        if (blocks_.count() >= DEFAULT_BLOCK_CNT) {
          if (OB_FAIL(build_idx_block())) {
            LOG_WARN("build index block failed", K(ret));
          }
        }
      }
    } else {
      if (idx_blk_->is_full()) {
        if (OB_FAIL(switch_idx_block())) {
          LOG_WARN("switch index block failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        idx_blk_->block_indexes_[idx_blk_->cnt_++] = bi;
      }
    }
  }
  return ret;
}

int ObRARowStore::alloc_idx_block(IndexBlock *&ib)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    void *mem = alloc_blk_mem(IndexBlock::INDEX_BLOCK_SIZE);
    if (OB_ISNULL(mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else {
      ib = new (mem) IndexBlock;
    }
  }
  return ret;
}

int ObRARowStore::build_idx_block()
{
  STATIC_ASSERT(IndexBlock::capacity() > DEFAULT_BLOCK_CNT,
      "DEFAULT_BLOCK_CNT block indexes must fit in one index block");
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(alloc_idx_block(idx_blk_))) {
    LOG_WARN("alloc idx block failed", K(ret));
  } else if (NULL == idx_blk_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloc null index block", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < blocks_.count(); ++i) {
      if (OB_FAIL(add_block_idx(blocks_.at(i)))) {
        LOG_WARN("add block idx failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      blocks_.reset();
    }
  }
  return ret;
}

int ObRARowStore::switch_idx_block(bool finish_add /* = false */)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == idx_blk_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index block should not be null");
  } else {
    IndexBlock *ib = NULL;
    BlockIndex bi;
    bi.is_idx_block_ = true;
    bi.on_disk_ = false;
    bi.row_id_ = idx_blk_->block_indexes_[0].row_id_;
    bi.idx_blk_ = idx_blk_;
    bi.length_ = static_cast<int32_t>(idx_blk_->buffer_size());
    bool dump = need_dump();
    if (!finish_add && !dump) {
      if (OB_FAIL(alloc_idx_block(ib))) {
        LOG_WARN("alloc index block failed", K(ret));
        if (GCONF.is_sql_operator_dump_enabled()) {
          ret = OB_SUCCESS;
          dump = true;
        }
      } else if (OB_ISNULL(ib)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alloc null block", K(ret));
      }
    }
    if (OB_SUCC(ret) && dump) {
      if (OB_FAIL(write_file(bi, idx_blk_, idx_blk_->buffer_size()))) {
        LOG_WARN("write index block to file failed");
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(blocks_.push_back(bi))) {
      LOG_WARN("add block index to array failed", K(ret));
    } else {
      if (!dump) {
        idx_blk_ = NULL;
      }
      if (NULL != ib) {
        idx_blk_ = ib;
        ib = NULL;
      } else if (NULL != idx_blk_) {
        idx_blk_->reset();
      }
    }
    if (!OB_SUCC(ret) && NULL != ib) {
      ib->~IndexBlock();
      free_blk_mem(ib, IndexBlock::INDEX_BLOCK_SIZE);
      ib = NULL;
    }
  }
  return ret;
}

int ObRARowStore::add_row(const common::ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int64_t bak_projector_size = 0;
    int32_t *bak_projector_ = NULL;
    ObNewRow &r = const_cast<ObNewRow &>(row);
    if (keep_projector_ && NULL != row.projector_ && row.projector_size_ > 0) {
      if (OB_ISNULL(projector_)) {
        ObMemAttr attr(tenant_id_, label_, ctx_id_);
        projector_size_ = row.projector_size_;
        const int64_t size = projector_size_ * sizeof(*projector_);
        projector_ = static_cast<int32_t *>(allocator_.alloc(size));
        if (OB_ISNULL(projector_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          MEMCPY(projector_, row.projector_, size);
        }
      } else if (projector_size_ != row.projector_size_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("projector size mismatch", K(ret), K(projector_size_), K(row.projector_size_));
      }
      if (OB_SUCC(ret)) {
        bak_projector_ = r.projector_;
        bak_projector_size = r.projector_size_;
        r.projector_size_ = 0;
        r.projector_ = NULL;
      }
    }

    if (OB_SUCC(ret) && NULL == blkbuf_.blk_) {
      if (OB_FAIL(alloc_block(blkbuf_, Block::min_buf_size(row)))) {
        LOG_WARN("alloc block failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t row_size = Block::row_store_size(row);
      if (row_size > blkbuf_.buf_.remain()) {
        if (OB_FAIL(switch_block(Block::min_buf_size(row)))) {
          LOG_WARN("switch block failed", K(ret), K(row));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(blkbuf_.blk_->add_row(blkbuf_.buf_, row, row_size))) {
          LOG_WARN("add row to block failed", K(ret), K(row), K(row_size));
        } else {
          row_cnt_++;
        }
      }
    }
    // always keep row unchanged.
    if (NULL != bak_projector_) {
      r.projector_size_ = bak_projector_size;
      r.projector_ = bak_projector_;
    }
  }

  return ret;
}

int ObRARowStore::find_block_idx(Reader &reader, BlockIndex &bi, const int64_t row_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (row_id < 0 || row_id >= save_row_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row should be saved", K(ret), K(row_id), K_(save_row_cnt));
  } else {
    bool found = false;
    if (NULL != reader.idx_blk_) {
      if (reader.ib_pos_ < 0 || reader.ib_pos_ >= reader.idx_blk_->cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ib_pos out of range", K(ret), K(reader.ib_pos_), K(*reader.idx_blk_));
      } else {
        int64_t pos = reader.ib_pos_;
        if (row_id > reader.idx_blk_->block_indexes_[pos].row_id_) {
          pos += 1;
          if (reader.idx_blk_->row_in_pos(row_id, pos)) {
            found = true;
            reader.ib_pos_ = pos;
          }
        } else {
          pos -= 1;
          if (reader.idx_blk_->row_in_pos(row_id, pos)) {
            found = true;
            reader.ib_pos_ = pos;
          }
        }
      }
      if (!found) {
        reader.reset_cursor(file_size_);
      } else {
        bi = reader.idx_blk_->block_indexes_[reader.ib_pos_];
      }
    }
    if (OB_FAIL(ret) || found) {
    } else {
      IndexBlock *ib = NULL;
      if (NULL != idx_blk_ && idx_blk_->cnt_ > 0
          && row_id >= idx_blk_->block_indexes_[0].row_id_) {
        ib = idx_blk_;
      }

      if (NULL == ib && blocks_.count() > 0) {
        auto it = std::lower_bound(blocks_.begin(), blocks_.end(),
            row_id, &BlockIndex::compare);
        if (it == blocks_.end() || it->row_id_ != row_id) {
          it--;
        }
        bi = *it;
        if (!bi.is_idx_block_) {
          found = true;
        } else {
          if (OB_FAIL(load_idx_block(reader, ib, bi))) {
            LOG_WARN("load index block failed", K(ret), K(bi));
          }
        }
      }

      if (OB_FAIL(ret) || found) {
      } else if (NULL == ib || ib->cnt_ <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("block index not found and index block is NULL or empty", K(ret));
      } else {
        auto it = std::lower_bound(&ib->block_indexes_[0], &ib->block_indexes_[ib->cnt_],
            row_id, &BlockIndex::compare);
        if (it == ib->block_indexes_ + ib->cnt_ || it->row_id_ != row_id) {
          it--;
        }
        bi = *it;
        reader.idx_blk_ = ib;
        reader.ib_pos_ = it - ib->block_indexes_;
      }
    }
  }
  return ret;
}

int ObRARowStore::load_idx_block(Reader &reader, IndexBlock *&ib, const BlockIndex &bi)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!bi.is_idx_block_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid block index", K(ret), K(bi));
  } else {
    if (!bi.on_disk_) {
      ib = bi.idx_blk_;
    } else {
      if (bi.length_ > IndexBlock::INDEX_BLOCK_SIZE) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid argument", K(ret), K(bi));
      } else if (OB_FAIL(ensure_reader_buffer(
          reader.idx_buf_, IndexBlock::INDEX_BLOCK_SIZE))) {
        LOG_WARN("ensure reader buffer failed", K(ret));
      } else if (OB_FAIL(read_file(
          reader.idx_buf_.data(), bi.length_, bi.offset_))) {
        LOG_WARN("read block index from file failed", K(ret), K(bi));
      } else {
        ib = reinterpret_cast<IndexBlock *>(reader.idx_buf_.data());
      }
    }
  }
  return ret;
}

int ObRARowStore::load_block(Reader &reader, const int64_t row_id)
{
  int ret = OB_SUCCESS;
  BlockIndex bi;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (row_id < 0 || row_id >= save_row_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row should be saved", K(ret), K(row_id), K_(save_row_cnt));
  } else if (OB_FAIL(find_block_idx(reader, bi, row_id))) {
    LOG_WARN("find block index failed", K(ret), K(row_id));
  } else {
    if (!bi.on_disk_) {
      reader.blk_ = bi.blk_;
    } else {
      if (OB_FAIL(ensure_reader_buffer(reader.buf_, bi.length_))) {
        LOG_WARN("ensure reader buffer failed", K(ret));
      } else if (OB_FAIL(read_file(reader.buf_.data(), bi.length_, bi.offset_))) {
        LOG_WARN("read block from file failed", K(ret), K(bi));
      } else {
        reader.blk_ = reinterpret_cast<Block *>(reader.buf_.data());
      }
    }
  }
  return ret;
}

int ObRARowStore::get_store_row(Reader &reader, const int64_t row_id, const StoreRow *&sr)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (row_id < 0 || row_id >= row_cnt_) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("invalid of row_id", K(ret), K(row_id), K_(row_cnt));
  } else {
    if (reader.file_size_ != file_size_) {
      reader.reset_cursor(file_size_);
    }
    if (NULL != reader.blk_ && reader.blk_->contain(row_id)) {
      // found in previous visited block
    } else if (row_id >= save_row_cnt_) {
      // found in write block
      reader.blk_ = blkbuf_.blk_;
    } else {
      if (NULL != reader.blk_) {
        reader.blk_ = NULL;
      }
      if (OB_FAIL(load_block(reader, row_id))) {
        LOG_WARN("load block failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (NULL == reader.blk_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null block", K(ret), K(row_id), K(*this));
      } else if (OB_FAIL(reader.blk_->get_store_row(row_id, sr))) {
        LOG_WARN("get row from block failed", K(ret), K(row_id), K(*reader.blk_));
      }
    }
  }
  return ret;
}

void ObRARowStore::Reader::reset()
{
  const int64_t file_size = 0;
  reset_cursor(file_size);
  store_.free_blk_mem(buf_.data(), buf_.capacity());
  buf_.reset();
  store_.free_blk_mem(idx_buf_.data(), idx_buf_.capacity());
  idx_buf_.reset();
}

void ObRARowStore::Reader::reuse()
{
  reset_cursor(0);
  buf_.reset();
  idx_buf_.reset();
}

void ObRARowStore::Reader::reset_cursor(const int64_t file_size)
{
  file_size_ = file_size;
  idx_blk_ = NULL;
  ib_pos_ = 0;
  blk_ = NULL;
}

int ObRARowStore::Reader::get_row(const int64_t row_id, const ObNewRow *&row)
{
  const StoreRow *sr = NULL;
  int ret = OB_SUCCESS;
  if (row_id < 0 || row_id >= get_row_cnt()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("invalid row_id", K(ret), K(row_id), K(get_row_cnt()));
  } else if (OB_FAIL(store_.get_store_row(*this, row_id, sr))) {
    LOG_WARN("get store row failed", K(ret), K(row_id));
  } else if (OB_ISNULL(sr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL store row returned", K(ret));
  } else {
    row_.count_ = sr->cnt_;
    row_.cells_ = const_cast<ObObj *>(sr->cells());
    if (OB_ISNULL(store_.projector_)) {
      row_.projector_ = NULL;
      row_.projector_size_ = 0;
    } else {
      row_.projector_ = store_.projector_;
      row_.projector_size_ = store_.projector_size_;
    }
    row = &row_;
  }
  return ret;
}


int ObRARowStore::Reader::get_row(const int64_t row_id, const common::ObNewRow &row)
{
  const StoreRow *sr = NULL;
  int ret = OB_SUCCESS;
  if (row_id < 0 || row_id >= get_row_cnt() || OB_ISNULL(row.cells_)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("invalid row_id", K(ret), K(row_id), K(get_row_cnt()));
  } else if (OB_FAIL(store_.get_store_row(*this, row_id, sr))) {
    LOG_WARN("get store row failed", K(ret), K(row_id));
  } else if (OB_ISNULL(sr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL store row returned", K(ret));
  } else if (row.count_ < sr->cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row cells count less than stored", K(ret), K(row.count_), K(sr->cnt_));
  } else {
    MEMCPY(row.cells_, sr->cells(), sr->cnt_ * sizeof(ObObj));
  }
  return ret;
}

int ObRARowStore::get_timeout(int64_t &timeout_ms)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_us = THIS_WORKER.get_timeout_remain();
  if (timeout_us / 1000 <= 0) {
    ret = OB_TIMEOUT;
    LOG_WARN("query is timeout", K(ret), K(timeout_us));
  } else {
    timeout_ms = timeout_us / 1000;
  }
  return ret;
}

int ObRARowStore::write_file(BlockIndex &bi, void *buf, int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t timeout_ms = 0;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (size < 0 || (size > 0 && NULL == buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(size), KP(buf));
  } else if (OB_FAIL(get_timeout(timeout_ms))) {
    LOG_WARN("get timeout failed", K(ret));
  } else {
    if (!is_file_open()) {
      if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.alloc_dir(dir_id_))) {
        LOG_WARN("alloc file directory failed", K(ret));
      } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.open(fd_, dir_id_))) {
        LOG_WARN("open file failed", K(ret));
      } else {
        file_size_ = 0;
        LOG_INFO("open file success", K_(fd), K_(dir_id));
      }
    }
    ret = OB_E(EventTable::EN_8) ret;
  }
  if (OB_SUCC(ret) && size > 0) {
    blocksstable::ObTmpFileIOInfo io;
    io.fd_ = fd_;
    io.buf_ = static_cast<char *>(buf);
    io.size_ = size;
    io.tenant_id_ = tenant_id_;
    io.io_desc_.set_wait_event(ObWaitEventIds::ROW_STORE_DISK_WRITE);
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.write(io, timeout_ms))) {
      LOG_WARN("write to file failed", K(ret), K(io), K(timeout_ms));
    }
  }
  if (OB_SUCC(ret)) {
    bi.on_disk_ = true;
    bi.offset_ = file_size_;
    file_size_ += size;
  }
  return ret;
}

int ObRARowStore::read_file(void *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_SUCCESS;
  int64_t timeout_ms = 0;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (offset < 0 || size < 0 || (size > 0 && NULL == buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(size), K(offset), KP(buf));
  } else if (OB_FAIL(get_timeout(timeout_ms))) {
    LOG_WARN("get timeout failed", K(ret));
  }

  if (OB_SUCC(ret) && size > 0) {
    blocksstable::ObTmpFileIOInfo io;
    io.fd_ = fd_;
    io.dir_id_ = dir_id_;
    io.buf_ = static_cast<char *>(buf);
    io.size_ = size;
    io.tenant_id_ = tenant_id_;
    io.io_desc_.set_wait_event(ObWaitEventIds::ROW_STORE_DISK_READ);
    blocksstable::ObTmpFileIOHandle handle;
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.pread(io, offset, timeout_ms, handle))) {
      LOG_WARN("read form file failed", K(ret), K(io), K(offset), K(timeout_ms));
    } else if (handle.get_data_size() != size) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("read data less than expected",
          K(ret), K(io), "read_size", handle.get_data_size());
    }
  }
  return ret;
}

int ObRARowStore::ensure_reader_buffer(ShrinkBuffer &buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    if (buf.is_inited() && buf.capacity() < size) {
      free_blk_mem(buf.data(), buf.capacity());
      buf.reset();
    }
    if (!buf.is_inited()) {
      const int64_t alloc_size = next_pow2(size);
      char *mem = static_cast<char *>(alloc_blk_mem(alloc_size));
      if (NULL == mem) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret), K(alloc_size));
      } else if (OB_FAIL(buf.init(mem, alloc_size))) {
        LOG_WARN("init buffer failed", K(ret));
        free_blk_mem(mem);
        mem = NULL;
      }
    }
  }

  return ret;
}

bool ObRARowStore::need_dump()
{
  bool dump = false;
  int ret = OB_SUCCESS;
  if (is_file_open()) {
    dump = true;
  } else if (mem_limit_ > 0) {
    if (mem_hold_ > mem_limit_) {
      dump = true;
      LOG_TRACE("need dump", K(dump), K(mem_hold_), K(mem_limit_));
    }
  } else if (!GCONF.is_sql_operator_dump_enabled()) {
    // no dump
  } else {
    const int64_t mem_ctx_pct_trigger = 80;
    lib::ObMallocAllocator *instance = lib::ObMallocAllocator::get_instance();
    lib::ObTenantCtxAllocatorGuard allocator = NULL;
    if (NULL == instance) {
      ret = common::OB_ERR_SYS;
      LOG_ERROR("NULL allocator", K(ret));
    } else if (OB_ISNULL(allocator = instance->get_tenant_ctx_allocator(
        tenant_id_, ctx_id_))) {
      // no tenant allocator, do nothing
    } else {
      const int64_t limit = allocator->get_limit();
      const int64_t hold = allocator->get_hold();
      int64_t mod_hold = 0;
      if (limit / 100 * mem_ctx_pct_trigger <= hold) {
        dump = true;
      }
      if (dump) {
        LOG_TRACE("check need dump", K(dump), K(limit), K(hold));
      }
    }
  }
  return dump;
}

int ObRARowStore::finish_add_row()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (!is_file_open()) {
      // all in memory, do nothing
    } else {
      int64_t timeout_ms = 0;
      const int64_t min_size = 0;
      const bool finish_add = true;
      if (OB_FAIL(switch_block(min_size))) {
        LOG_WARN("write last block to file failed", K(ret), K(min_size));
      } else if (OB_FAIL(switch_idx_block(finish_add))) {
        LOG_WARN("write last index block to file failed", K(ret), K(ret));
      } else if (OB_FAIL(get_timeout(timeout_ms))) {
        LOG_WARN("get timeout failed", K(ret));
      } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.sync(fd_, timeout_ms))) {
        LOG_WARN("sync file failed", K(ret), K(fd_), K(timeout_ms));
      } else {
        if (blkbuf_.buf_.is_inited()) {
          free_blk_mem(blkbuf_.buf_.data(), blkbuf_.buf_.capacity());
          blkbuf_.reset();
        }

        if (NULL != idx_blk_) {
          free_blk_mem(idx_blk_, IndexBlock::INDEX_BLOCK_SIZE);
          idx_blk_ = NULL;
        }
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
