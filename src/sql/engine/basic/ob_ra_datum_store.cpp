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

#include "ob_ra_datum_store.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"


namespace oceanbase
{
using namespace common;

namespace sql
{

const int64_t ObRADatumStore::IndexBlock::INDEX_BLOCK_SIZE;
const int64_t ObRADatumStore::BIG_BLOCK_SIZE;

int ObRADatumStore::ShrinkBuffer::init(char *buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf) || OB_UNLIKELY(buf_size <= 0)) {
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

namespace ra_datum_store {

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

template <typename T, typename B>
void point2pointer(T *&dst_pointer, B *dst_base, T *src_pointer, const B *src_base)
{
  dst_pointer = reinterpret_cast<T *>(reinterpret_cast<char *>(dst_base) +
      reinterpret_cast<intptr_t>(src_pointer) - reinterpret_cast<const char *>(src_base));
}

}

// 目前仅有ObIArray<ObExpr>方式写入Store
// 其他基于ObDatum指针方式写入，暂时未使用
int ObRADatumStore::StoredRow::copy_datums(const common::ObIArray<ObExpr*> &exprs,
  ObEvalCtx &ctx, char *buf, const int64_t size, const int64_t row_size,
  const uint32_t row_extend_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(payload_ != buf) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(payload_), KP(buf), K(size));
  } else {
    readable_ = true;
    cnt_ = ~(1U << 31) & static_cast<int32>(exprs.count());
    row_size_ = static_cast<int32_t>(row_size);
    int64_t pos = sizeof(ObDatum) * cnt_ + row_extend_size;
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; ++i) {
      ObDatum &in_datum = static_cast<ObDatum&>(exprs.at(i)->locate_expr_datum(ctx));
      ObDatum *datum = new (&cells()[i])ObDatum();
      if (OB_FAIL(datum->deep_copy(in_datum, buf, size, pos))) {
        LOG_WARN("failed to copy datum", K(ret), K(i), K(pos), K(size), K(row_size),
                 K(in_datum), K(*datum));
      }
    }
  }
  return ret;
}

int ObRADatumStore::StoredRow::copy_datums(const common::ObIArray<ObDatum> &datums,
  char *buf, const int64_t size, const int64_t row_size,
  const uint32_t row_extend_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(payload_ != buf) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(payload_), KP(buf), K(size));
  } else {
    readable_ = true;
    cnt_ = ~(1U << 31) & static_cast<int32>(datums.count());
    row_size_ = static_cast<int32_t>(row_size);
    int64_t pos = sizeof(ObDatum) * cnt_ + row_extend_size;
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; ++i) {
      const ObDatum &in_datum = datums.at(i);
      ObDatum *datum = new (&cells()[i])ObDatum();
      if (OB_FAIL(datum->deep_copy(in_datum, buf, size, pos))) {
        LOG_WARN("failed to copy datum", K(ret), K(i), K(pos), K(size), K(row_size),
                 K(in_datum), K(*datum));
      }
    }
  }
  return ret;
}

// always skip const expr to avoid overwriting const expr value,
// as const expr value never changes
int ObRADatumStore::StoredRow::to_expr(const common::ObIArray<ObExpr*> &exprs,
    ObEvalCtx &ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cnt_ < exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datum count mismatch", K(ret), K(cnt_), K(exprs.count()));
  } else {
    for (uint32_t i = 0; i < exprs.count(); ++i) {
      if (exprs.at(i)->is_const_expr()) {
        continue;
      } else {
        exprs.at(i)->locate_expr_datum(ctx) = cells()[i];
        exprs.at(i)->set_evaluated_projected(ctx);
        LOG_DEBUG("succ to_expr", K(cnt_), K(i), KPC(exprs.at(i)), K(cells()[i]));
      }
    }
  }
  return ret;
}

int ObRADatumStore::StoredRow::assign(const StoredRow *sr)
{
  int ret = OB_SUCCESS;
  MEMCPY(this, static_cast<const void*>(sr), sr->row_size_);
  ObDatum* src_cells = const_cast<ObDatum*>(sr->cells());
  for (int64_t i = 0; i < cnt_; ++i) {
    ra_datum_store::point2pointer(*(const char **)&cells()[i].ptr_,
                  this,
                  *(const char **)&src_cells[i].ptr_, sr);
  }
  LOG_DEBUG("trace unswizzling", K(ret), KPC(this), KPC(sr));
  return ret;
}

int ObRADatumStore::StoredRow::to_copyable()
{
  int ret = OB_SUCCESS;
  if (0 != readable_) {
    for (int64_t i = 0; i < cnt_; ++i) {
      ra_datum_store::pointer2off(*(const char **)&cells()[i].ptr_, this);
    }
    readable_ = false;
  }
  return ret;
}

int ObRADatumStore::StoredRow::to_readable()
{
  int ret = OB_SUCCESS;
  if (0 == readable_) {
    for (int64_t i = 0; i < cnt_; ++i) {
      ra_datum_store::off2pointer(*(const char **)&cells()[i].ptr_, this);
    }
    readable_ = true;
  }
  return ret;
}

int ObRADatumStore::Block::add_row(ShrinkBuffer &buf,
                                   const common::ObIArray<ObExpr*> &exprs,
                                   ObEvalCtx &ctx,
                                   const int64_t row_size,
                                   uint32_t row_extend_size,
                                   StoredRow **stored_row/* = nullptr*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!buf.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf));
  } else if (OB_UNLIKELY(row_size > buf.remain())
             || OB_UNLIKELY(row_size <= ROW_INDEX_SIZE)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer not enough", K(row_size), "remain", buf.remain());
  } else {
    StoredRow *sr = new (buf.head())StoredRow;
    if (OB_FAIL(sr->copy_datums(exprs,
                                ctx, buf.head() + ROW_HEAD_SIZE,
                                row_size - ROW_HEAD_SIZE - ROW_INDEX_SIZE,
                                row_size,
                                row_extend_size))) {
      LOG_WARN("copy row failed", K(ret), K(row_size));
    } else if (OB_FAIL(buf.fill_tail(ROW_INDEX_SIZE))) {
      LOG_WARN("fill buffer tail failed", K(ret), K(buf), LITERAL_K(ROW_INDEX_SIZE));
    } else {
      *reinterpret_cast<row_idx_t *>(buf.tail()) = static_cast<row_idx_t>(buf.head() - payload_);
      idx_off_ -= static_cast<int32_t>(ROW_INDEX_SIZE);
      if (OB_FAIL(buf.fill_head(row_size - ROW_INDEX_SIZE))) {
        LOG_WARN("fill buffer head failed", K(ret), K(buf), K(row_size - ROW_INDEX_SIZE));
      } else {
        rows_++;
        LOG_DEBUG("finish add_row", K(rows_), K(row_size), KPC(sr));
        if (NULL != stored_row) {
          *stored_row = sr;
        }
      }
    }
  }

  return ret;
}

int ObRADatumStore::Block::add_row(ShrinkBuffer &buf,
                                   const ObIArray<ObDatum> &datums,
                                   const int64_t row_size,
                                   uint32_t row_extend_size,
                                   StoredRow **stored_row/* = nullptr*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!buf.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf));
  } else if (OB_UNLIKELY(row_size > buf.remain())
             || OB_UNLIKELY(row_size <= ROW_INDEX_SIZE)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer not enough", K(row_size), "remain", buf.remain());
  } else {
    StoredRow *sr = new (buf.head())StoredRow;
    if (OB_FAIL(sr->copy_datums(datums,
                                buf.head() + ROW_HEAD_SIZE,
                                row_size - ROW_HEAD_SIZE - ROW_INDEX_SIZE,
                                row_size,
                                row_extend_size))) {
      LOG_WARN("copy row failed", K(ret), K(row_size));
    } else if (OB_FAIL(buf.fill_tail(ROW_INDEX_SIZE))) {
      LOG_WARN("fill buffer tail failed", K(ret), K(buf), LITERAL_K(ROW_INDEX_SIZE));
    } else {
      *reinterpret_cast<row_idx_t *>(buf.tail()) = static_cast<row_idx_t>(buf.head() - payload_);
      idx_off_ -= static_cast<int32_t>(ROW_INDEX_SIZE);
      if (OB_FAIL(buf.fill_head(row_size - ROW_INDEX_SIZE))) {
        LOG_WARN("fill buffer head failed", K(ret), K(buf), K(row_size - ROW_INDEX_SIZE));
      } else {
        rows_++;
        LOG_DEBUG("finish add_row", K(rows_), K(row_size), KPC(sr));
        if (NULL != stored_row) {
          *stored_row = sr;
        }
      }
    }
  }

  return ret;
}

int ObRADatumStore::Block::copy_stored_row(ShrinkBuffer &buf, const StoredRow &stored_row,
    StoredRow **dst_sr)
{
  int ret = OB_SUCCESS;
  int64_t row_size =  stored_row.row_size_;
  if (OB_UNLIKELY(!buf.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(row_size));
  } else if (OB_UNLIKELY(row_size <= ROW_INDEX_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_size));
  } else {
    StoredRow *sr = new (buf.head())StoredRow;
    sr->assign(&stored_row);

    if (OB_FAIL(buf.fill_tail(ROW_INDEX_SIZE))) {
      LOG_WARN("fill buffer tail failed", K(ret), K(buf), LITERAL_K(ROW_INDEX_SIZE));
    } else {
      *reinterpret_cast<row_idx_t *>(buf.tail()) = static_cast<row_idx_t>(buf.head() - payload_);
      idx_off_ -= static_cast<int32_t>(ROW_INDEX_SIZE);
      if (OB_FAIL(buf.fill_head(row_size - ROW_INDEX_SIZE))) {
        LOG_WARN("fill buffer head failed", K(ret), K(buf), K(row_size - ROW_INDEX_SIZE));
      } else {
        rows_++;
        LOG_DEBUG("finish copy_stored_row", K(rows_), K(row_size), KPC(sr));
        if (nullptr != dst_sr) {
          *dst_sr = sr;
        }
      }
    }
  }
  return ret;
}

int ObRADatumStore::Block::get_store_row(const int64_t row_id, const StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!contain(row_id))) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("invalid index", K(ret), K(row_id), K(*this));
  } else {
    StoredRow *row = reinterpret_cast<StoredRow *>(
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

int ObRADatumStore::Block::compact(ShrinkBuffer &buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!buf.is_inited())) {
    LOG_WARN("invalid argument", K(ret), K(buf));
  } else if (OB_FAIL(buf.compact())) {
    LOG_WARN("block buffer compact failed", K(ret));
  } else {
    idx_off_ = static_cast<int32_t>(buf.head() - rows_ * ROW_INDEX_SIZE - payload_);
  }
  return ret;
}

int ObRADatumStore::Block::to_copyable()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < rows_; ++i) {
    StoredRow *sr = reinterpret_cast<StoredRow *>(&payload_[indexes()[i]]);
    if (OB_FAIL(sr->to_copyable())) {
      LOG_WARN("convert store row to copyable row failed", K(ret));
    }
  }
  return ret;
}

ObRADatumStore::ObRADatumStore(common::ObIAllocator *alloc /* = NULL */)
  : inited_(false), tenant_id_(0), label_(nullptr), ctx_id_(0), mem_limit_(0),
    idx_blk_(NULL), save_row_cnt_(0), row_cnt_(0), fd_(-1), dir_id_(-1), file_size_(0),
    inner_reader_(*this), mem_hold_(0), allocator_(NULL == alloc ? &inner_allocator_ : alloc),
    row_extend_size_(0), mem_stat_(NULL), io_observer_(NULL)
{
}

int ObRADatumStore::init(int64_t mem_limit,
    uint64_t tenant_id,
    int64_t mem_ctx_id /* = common::ObCtxIds::DEFAULT_CTX_ID */,
    const char *label /* = common::ObModIds::OB_SQL_ROW_STORE) */,
    uint32_t row_extend_size /* = 0 */)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    tenant_id_ = tenant_id;
    ctx_id_ = mem_ctx_id;
    label_ = label;
    mem_limit_ = mem_limit;
    row_extend_size_ = row_extend_size;
    inited_ = true;
  }
  return ret;
}

void ObRADatumStore::set_mem_hold(int64_t hold)
{
  inc_mem_hold(hold - mem_hold_);
}

void ObRADatumStore::inc_mem_hold(int64_t hold)
{
  if (NULL != mem_stat_) {
    if (hold > 0) {
      mem_stat_->alloc(hold);
    } else if (hold < 0) {
      mem_stat_->free(-hold);
    }
  }
  mem_hold_ += hold;
}

void ObRADatumStore::reset()
{
  int ret = OB_SUCCESS;
  label_ = common::ObModIds::OB_SQL_ROW_STORE;
  ctx_id_ = common::ObCtxIds::DEFAULT_CTX_ID;
  mem_limit_ = 0;

  blkbuf_.reset();
  // the last index block may not be linked to `blk_mem_list_` and needs to be released manually
  if (NULL != idx_blk_) {
    free_blk_mem(idx_blk_);
    idx_blk_ = NULL;
  }

  save_row_cnt_ = 0;
  row_cnt_ = 0;
  inner_reader_.reset();

  if (is_file_open()) {
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(tenant_id_, fd_))) {
      LOG_WARN("remove file failed", K(ret), K_(fd));
    } else {
      LOG_INFO("close file success", K(ret), K_(fd));
    }
    fd_ = -1;
    dir_id_ = -1;
    file_size_ = 0;
  }
  tenant_id_ = common::OB_SERVER_TENANT_ID;

  while (!blk_mem_list_.is_empty()) {
    LinkNode *node = blk_mem_list_.remove_first();
    if (NULL != node) {
      node->~LinkNode();
      allocator_->free(node);
    }
  }
  blocks_.reset();
  set_mem_hold(0);
  row_extend_size_ = 0;
  inited_ = false;
}

void ObRADatumStore::reuse()
{
  int ret = OB_SUCCESS;
  save_row_cnt_ = 0;
  row_cnt_ = 0;
  inner_reader_.reset();
  if (is_file_open()) {
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(tenant_id_, fd_))) {
      LOG_WARN("remove file failed", K(ret), K_(fd));
    } else {
      LOG_INFO("close file success", K(ret), K_(fd));
    }
    fd_ = -1;
    dir_id_ = -1;
    file_size_ = 0;
  }
  if (NULL != idx_blk_) {
    free_blk_mem(idx_blk_);
    idx_blk_ = NULL;
  }
  DLIST_FOREACH_REMOVESAFE_NORET(node, blk_mem_list_) {
    if (&(*node) + 1 != static_cast<LinkNode *>(static_cast<void *>(blkbuf_.buf_.data()))) {
      node->unlink();
      node->~LinkNode();
      allocator_->free(node);
    }
  }
  set_mem_hold(0);
  if (NULL != blkbuf_.buf_.data()) {
    if (OB_FAIL(setup_block(blkbuf_))) {
      LOG_WARN("setup block failed", K(ret));
    }
    set_mem_hold(blkbuf_.buf_.capacity() + sizeof(LinkNode));
  }
  blocks_.reset();
}

int ObRADatumStore::setup_block(BlockBuffer &blkbuf) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!blkbuf.buf_.is_inited())) {
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

// index block will add to list while idx_blk is full or finish_add
int ObRADatumStore::link_idx_block(IndexBlock *idx_blk)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(idx_blk)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("idx_blk_ is null", K(ret));
  } else {
    void *mem = idx_blk;
    LinkNode *node = static_cast<LinkNode *>(mem) - 1;
    if (OB_UNLIKELY(!blk_mem_list_.add_last(node))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("add node to list failed", K(ret));
    }
  }
  return ret;
}

void *ObRADatumStore::alloc_blk_mem(const int64_t size, const bool link_mem_list)
{
  void *blk = NULL;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size < 0)) {
    LOG_WARN("invalid argument", K(size));
  } else {
    ObMemAttr attr(tenant_id_, label_, ctx_id_);
    void *mem = allocator_->alloc(size + sizeof(LinkNode), attr);
    if (OB_UNLIKELY(NULL == mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), KP(mem));
    } else {
      LinkNode *node = new (mem) LinkNode;
      if (link_mem_list && OB_UNLIKELY(!blk_mem_list_.add_last(node))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("add node to list failed", K(ret));
        node->~LinkNode();
        allocator_->free(mem);
      } else {
        blk = static_cast<char *>(mem) + sizeof(LinkNode);
        inc_mem_hold(size + sizeof(LinkNode));
      }
    }
  }
  return blk;
}

void ObRADatumStore::free_blk_mem(void *mem, const int64_t size /* = 0 */)
{
  if (NULL != mem) {
    LinkNode *node = static_cast<LinkNode *>(mem) - 1;
    if (NULL != node->get_next()) {
      node->unlink();
    }
    node->~LinkNode();
    allocator_->free(node);
    inc_mem_hold(-(size + sizeof(LinkNode)));
  }
}

int ObRADatumStore::alloc_block(BlockBuffer &blkbuf, const int64_t min_size)
{
  int ret = OB_SUCCESS;
  int64_t size = std::max(static_cast<int64_t>(BLOCK_SIZE), min_size);
  if (row_cnt_ > 0 || need_dump(size)) {
    size = std::max(size, static_cast<int64_t>(BIG_BLOCK_SIZE));
  }
  size += sizeof(LinkNode);
  size = next_pow2(size);
  size -= sizeof(LinkNode);
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    void *mem = alloc_blk_mem(size, true /* link_mem_list */);
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

int ObRADatumStore::switch_block(const int64_t min_size)
{
  int ret = OB_SUCCESS;
  const bool finish_add = (0 == min_size);
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(min_size < 0) || OB_ISNULL(blkbuf_.blk_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(min_size));
  } else if (OB_FAIL(blkbuf_.blk_->compact(blkbuf_.buf_))) {
    LOG_WARN("block compact failed", K(ret));
  } else if (!finish_add && OB_FAIL(dump_block_if_need(min_size))) {
    LOG_WARN("fail to dump block if need", K(ret), K(min_size));
  } else {
    BlockBuffer new_blkbuf;
    BlockIndex bi;
    bi.is_idx_block_ = false;
    bi.on_disk_ = false;
    bi.row_id_ = ~(0b11UL << 62) & save_row_cnt_;
    bi.blk_ = blkbuf_.blk_;
    bi.length_ = static_cast<int32_t>(blkbuf_.buf_.head_size());
    bi.capacity_ = static_cast<int32_t>(blkbuf_.buf_.capacity()); // used to calc mem_hold for dump
    if (OB_SUCC(ret) && !finish_add) { // need alloc new block
      if (OB_FAIL(alloc_block(new_blkbuf, min_size))) {
        LOG_WARN("alloc block failed", K(ret), K(min_size));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_block_idx(bi))) {
        LOG_WARN("add block index failed", K(ret));
      } else {
        save_row_cnt_ = row_cnt_;
        blkbuf_.reset();
        if (new_blkbuf.buf_.is_inited()) { // finish_add won't come here
          blkbuf_ = new_blkbuf;
          new_blkbuf.reset();
          if (OB_FAIL(setup_block(blkbuf_))) {
            LOG_WARN("setup block failed", K(ret));
          }
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

int ObRADatumStore::add_block_idx(const BlockIndex &bi)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (!has_index_block()) {
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

int ObRADatumStore::alloc_idx_block(IndexBlock *&ib)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    void *mem = alloc_blk_mem(IndexBlock::INDEX_BLOCK_SIZE, false /* link_mem_list */);
    if (OB_ISNULL(mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else {
      ib = new (mem) IndexBlock;
    }
  }
  return ret;
}

int ObRADatumStore::build_idx_block()
{
  STATIC_ASSERT(IndexBlock::capacity() > DEFAULT_BLOCK_CNT,
      "DEFAULT_BLOCK_CNT block indexes must fit in one index block");
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(alloc_idx_block(idx_blk_))) {
    LOG_WARN("alloc idx block failed", K(ret));
  } else if (OB_UNLIKELY(NULL == idx_blk_)) {
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

// add a mode controlled by the `all_dump`
// `all_dump` is false means :
//     only dumps blocks that have already been filled up and leaving the last block along
// `all_dump` is true means :
//     dumps all blocks if needed
int ObRADatumStore::dump(const bool all_dump, const int64_t target_dump_size)
{
  LOG_TRACE("before dump block", K(all_dump), K(target_dump_size), K(*this));
  int ret = OB_SUCCESS;
  int64_t tmp_dumped_size = 0;
  LinkNode *node = blk_mem_list_.get_first();
  LinkNode *next_node = NULL;
  void *mem = nullptr;
  Block *blk = nullptr;
  IndexBlock *idx_blk = nullptr;

  if (all_dump) {
    // If need to dump all, first switch block and index block to ensure
    // that all block indexes are established.
    if (blkbuf_.buf_.is_inited() && OB_FAIL(switch_block(0 /*finish_add */))) {
      LOG_WARN("fail to dump last block", K(ret));
    } else if (has_index_block() && OB_FAIL(switch_idx_block(true /* finish_add */))) {
      LOG_WARN("fail to dump last index block", K(ret));
    }
    LOG_TRACE("dump all blocks", K(blk_mem_list_.get_size()));
  }

  bool is_ib = false;
  int64_t row_id = 0;
  while (OB_SUCC(ret) && node != blk_mem_list_.get_header() &&
      (all_dump || tmp_dumped_size < target_dump_size)) {
    next_node = node->get_next();
    if (OB_ISNULL(mem = static_cast<void *>(node + 1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur block is null", K(ret));
    } else if (is_last_block(mem)) {
      // skip the last block or index block
    } else {
      // step 1 : find bi of blk
      // step 2 : write file and update bi
      // step 3 : free mem
      if (is_block(mem)) {
        is_ib = false;
        blk = static_cast<Block *>(mem);
        row_id = blk->row_id_;
      } else if (is_index_block(mem)) {
        is_ib = true;
        idx_blk = static_cast<IndexBlock *>(mem);
        if (OB_UNLIKELY(0 == idx_blk->cnt_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("empty index block is unexpected", K(ret), K(idx_blk));
        } else {
          row_id = idx_blk->row_id();
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("magic_ of cur block is unexpected", K(ret), K(block_magic(mem)),
                                                      K(blk_mem_list_.get_size()));
      }
      BlockIndex *bi = NULL;
      if (OB_FAIL(ret)) {
      } else if (is_ib && OB_FAIL(find_block_idx<true>(inner_reader_, row_id, bi))) {
        LOG_WARN("fail to find block idx of index block", K(ret), K(row_id));
      } else if (!is_ib && OB_FAIL(find_block_idx(inner_reader_, row_id, bi))) {
        LOG_WARN("fail to find block idx of data block", K(ret), K(row_id));
      } else if (OB_ISNULL(bi)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null ptr is unexpected", K(ret), K(is_ib));
      } else if (bi->on_disk_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("block on disk is unexpected", K(ret), KPC(bi));
      } else {
        // dump
        if (OB_ISNULL(blk_mem_list_.remove(node))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("remove node failed", K(ret), K(blk_mem_list_.get_size()));
        } else if (!is_ib && OB_FAIL(blk->to_copyable())) {
          LOG_WARN("convert block to copyable failed", K(ret));
        } else if (OB_FAIL(write_file(*bi, mem, bi->length_))) { // write file and update bi
          LOG_WARN("write block to file failed", K(ret), K(is_ib), KPC(bi));
        } else {
          if (blkbuf_.blk_ == blk) {
            blkbuf_.reset();
          } else if (idx_blk_ == idx_blk) {
            idx_blk_ = NULL;
          }
          tmp_dumped_size += bi->length_;
        }
        free_blk_mem(mem, bi->capacity_);
      }
    }
    if (OB_SUCC(ret)) {
      node = next_node;
    }
  }
  if (OB_SUCC(ret) && all_dump && blk_mem_list_.is_empty() && is_file_open()) {
    int64_t timeout_ms = 0;
    if (OB_FAIL(get_timeout(timeout_ms))) {
      LOG_WARN("get timeout failed", K(ret));
    }
  }

  LOG_TRACE("after dump block", K(all_dump), K(tmp_dumped_size), K(*this));
  return ret;
}

int ObRADatumStore::switch_idx_block(bool finish_add /* = false */)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(idx_blk_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("idx_blk_ should not be null", K(ret));
  } else if (OB_FAIL(link_idx_block(idx_blk_))) { // idx_blk is full or finish_add
    // idx_blk must dump later than the corresponding data block
    // so add the idx_blk in blk_mem_list after the corresponding data block
    LOG_WARN("add_idx_blk_to_blk_mem_list failed", K(ret));
  } else {
    IndexBlock *ib = NULL;
    BlockIndex bi;
    bi.is_idx_block_ = true;
    bi.on_disk_ = false;
    bi.row_id_ = idx_blk_->block_indexes_[0].row_id_;
    bi.idx_blk_ = idx_blk_;
    bi.length_ = static_cast<int32_t>(idx_blk_->buffer_size());
    bi.capacity_ = static_cast<int32_t>(IndexBlock::INDEX_BLOCK_SIZE);
    if (OB_SUCC(ret) && !finish_add) {
      if (OB_FAIL(alloc_idx_block(ib))) {
        LOG_WARN("alloc index block failed", K(ret));
      } else if (OB_ISNULL(ib)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alloc null block", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(blocks_.push_back(bi))) {
      LOG_WARN("add block index to array failed", K(ret));
    } else {
      idx_blk_ = NULL;
      if (NULL != ib) { // finish_add won't come here
        idx_blk_ = ib;
        ib = NULL;
      }
    }
    if (OB_FAIL(ret) && NULL != ib) {
      ib->~IndexBlock();
      free_blk_mem(ib, IndexBlock::INDEX_BLOCK_SIZE);
      ib = NULL;
    }
  }
  return ret;
}

/*
 * 从ObChunkDatumStore读出数据，然后再写入ObChunkDatumStore时，使用copy_row
 * 从operator的ObExpr的ObDatum中写入到ObChunkDatumStore时，使用add_row
 * 理论上只有这两个接口
 */
template<bool NEED_EVAL>
int ObRADatumStore::add_row(const common::ObIArray<ObExpr*> &exprs,
    ObEvalCtx *ctx, StoredRow **stored_row)
{
  int ret = OB_SUCCESS;
  int64_t row_size = 0;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(Block::row_store_size<NEED_EVAL>(exprs, *ctx, row_size, row_extend_size_))) {
    // row store size确保exprs被计算过
    LOG_WARN("failed to calc store size");
  } else {
    const int64_t min_buf_size = Block::min_buf_size(row_size);
    if (OB_SUCC(ret) && NULL == blkbuf_.blk_) {
      if (OB_FAIL(alloc_block(blkbuf_, min_buf_size))) {
        LOG_WARN("alloc block failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (row_size > blkbuf_.buf_.remain() && OB_FAIL(switch_block(min_buf_size))) {
        LOG_WARN("switch block failed", K(ret), K(row_size), K(min_buf_size));
      } else if (OB_FAIL(blkbuf_.blk_->add_row(blkbuf_.buf_, exprs, *ctx, row_size,
                                               row_extend_size_, stored_row))) {
        LOG_WARN("add row to block failed", K(ret), K(exprs), K(row_size));
      } else {
        row_cnt_++;
      }
    }
  }
  return ret;
}

template int ObRADatumStore::add_row<true>(const common::ObIArray<ObExpr*> &exprs,
                                           ObEvalCtx *ctx,
                                           StoredRow **stored_row);
template int ObRADatumStore::add_row<false>(const common::ObIArray<ObExpr*> &exprs,
                                           ObEvalCtx *ctx,
                                           StoredRow **stored_row);

int ObRADatumStore::add_row(const common::ObIArray<ObDatum> &datums,
    StoredRow **stored_row)
{
  int ret = OB_SUCCESS;
  int64_t row_size = 0;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(Block::row_store_size(datums, row_size, row_extend_size_))) {
    // row store size确保exprs被计算过
    LOG_WARN("failed to calc store size");
  } else {
    const int64_t min_buf_size = Block::min_buf_size(row_size);
    if (OB_SUCC(ret) && NULL == blkbuf_.blk_) {
      if (OB_FAIL(alloc_block(blkbuf_, min_buf_size))) {
        LOG_WARN("alloc block failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (row_size > blkbuf_.buf_.remain() && OB_FAIL(switch_block(min_buf_size))) {
        LOG_WARN("switch block failed", K(ret), K(row_size), K(min_buf_size));
      } else if (OB_FAIL(blkbuf_.blk_->add_row(blkbuf_.buf_, datums, row_size,
                                               row_extend_size_, stored_row))) {
        LOG_WARN("add row to block failed", K(ret), K(datums), K(row_size));
      } else {
        row_cnt_++;
      }
    }
  }
  return ret;
}

int ObRADatumStore::add_row(const StoredRow &src_stored_row, StoredRow **stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t row_size = src_stored_row.row_size_;
    const int64_t min_buf_size = Block::min_buf_size(row_size);
    if (OB_SUCC(ret) && NULL == blkbuf_.blk_) {
      if (OB_FAIL(alloc_block(blkbuf_, min_buf_size))) {
        LOG_WARN("alloc block failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (row_size > blkbuf_.buf_.remain() && OB_FAIL(switch_block(min_buf_size))) {
        LOG_WARN("switch block failed", K(ret), K(row_size), K(min_buf_size));
      } else if (OB_FAIL(blkbuf_.blk_->copy_stored_row(blkbuf_.buf_, src_stored_row, stored_row))){
        LOG_WARN("add row to block failed", K(ret), K(src_stored_row), K(row_size));
      } else {
        row_cnt_++;
      }
    }
  }
  return ret;
}

template <bool IS_IB>
int ObRADatumStore::find_block_idx(Reader &reader, const int64_t row_id, BlockIndex *&bi)
{
  int ret = OB_SUCCESS;
  bi = NULL;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(row_id < 0) || OB_UNLIKELY(row_id >= save_row_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row should be saved", K(ret), K(row_id), K_(save_row_cnt));
  } else if (IS_IB) {
    if (OB_UNLIKELY(blocks_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("try to find block index in empty list", K(ret), K(row_id));
    } else {
      auto it = std::lower_bound(blocks_.begin(), blocks_.end(), row_id, &BlockIndex::compare);
      if (it == blocks_.end() || it->row_id_ != row_id) {
        it--;
      }
      bi = &(*it);
      if (!bi->is_idx_block_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bi is data_block_, expect idx block", K(ret), K(row_id), K(blocks_.count()));
      }
    }
  } else {
    bool found = false;
    if (NULL != reader.idx_blk_) {
      if (OB_UNLIKELY(reader.ib_pos_ < 0)
          || OB_UNLIKELY(reader.ib_pos_ >= reader.idx_blk_->cnt_)) {
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
        bi = &reader.idx_blk_->block_indexes_[reader.ib_pos_];
      }
    }
    if (OB_FAIL(ret) || found) {
    } else {
      IndexBlock *ib = NULL;
      if (NULL != idx_blk_ && idx_blk_->cnt_ > 0 && row_id >= idx_blk_->block_indexes_[0].row_id_) {
        ib = idx_blk_;
      }
      if (NULL == ib && blocks_.count() > 0) {
        auto it = std::lower_bound(blocks_.begin(), blocks_.end(), row_id, &BlockIndex::compare);
        if (it == blocks_.end() || it->row_id_ != row_id) {
          it--;
        }
        bi = &(*it);
        if (!bi->is_idx_block_) {
          found = true;
        } else {
          if (OB_FAIL(load_idx_block(reader, ib, *bi))) {
            LOG_WARN("load index block failed", K(ret), K(bi));
          }
        }
      }
      if (OB_FAIL(ret) || found) {
      } else if (OB_UNLIKELY(NULL == ib) || OB_UNLIKELY(ib->cnt_ <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("block index not found and index block is NULL or empty", K(ret));
      } else {
        auto it = std::lower_bound(&ib->block_indexes_[0], &ib->block_indexes_[ib->cnt_],
            row_id, &BlockIndex::compare);
        if (it == ib->block_indexes_ + ib->cnt_ || it->row_id_ != row_id) {
          it--;
        }
        bi = &(*it);
        reader.idx_blk_ = ib;
        reader.ib_pos_ = it - ib->block_indexes_;
      }
    }
  }
  return ret;
}

int ObRADatumStore::load_idx_block(Reader &reader, IndexBlock *&ib, const BlockIndex &bi)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!bi.is_idx_block_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid block index", K(ret), K(bi));
  } else {
    if (!bi.on_disk_) {
      ib = bi.idx_blk_;
    } else {
      if (OB_UNLIKELY(bi.length_ > IndexBlock::INDEX_BLOCK_SIZE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid argument", K(ret), K(bi));
      } else if (OB_FAIL(ensure_reader_buffer(
          reader, reader.idx_buf_, IndexBlock::INDEX_BLOCK_SIZE))) {
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

int ObRADatumStore::load_block(Reader &reader, const int64_t row_id)
{
  int ret = OB_SUCCESS;
  BlockIndex *bi = NULL;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(row_id < 0) || OB_UNLIKELY(row_id >= save_row_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row should be saved", K(ret), K(row_id), K_(save_row_cnt));
  } else if (OB_FAIL(find_block_idx(reader, row_id, bi))) {
    LOG_WARN("find block index failed", K(ret), K(row_id));
  } else if (OB_ISNULL(bi)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("changeable_bi is nullptr", K(ret), K(row_id));
  } else {
    if (!bi->on_disk_) {
      reader.blk_ = bi->blk_;
      if (!is_block(reader.blk_)) { // defense check
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("magic of blk is unexpected",
            K(ret), K(row_id), KPC(bi), K(bi->blk_), KPC(bi->blk_));
      } else if (bi->row_id_ != bi->blk_->row_id_) { // defense check
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bi->row_id_ != bi->blk_->row_id_",
            K(ret), K(row_id), KPC(bi), K(bi->blk_), KPC(bi->blk_));
      }
    } else {
      if (OB_FAIL(ensure_reader_buffer(reader, reader.buf_, bi->length_))) {
        LOG_WARN("ensure reader buffer failed", K(ret));
      } else if (OB_FAIL(read_file(reader.buf_.data(), bi->length_, bi->offset_))) {
        LOG_WARN("read block from file failed", K(ret), KPC(bi));
      } else {
        reader.blk_ = reinterpret_cast<Block *>(reader.buf_.data());
      }
    }
  }
  return ret;
}

int ObRADatumStore::get_store_row(Reader &reader, const int64_t row_id, const StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(row_id < 0) || OB_UNLIKELY(row_id >= row_cnt_)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("invalid of row_id", K(ret), K(row_id), K_(row_cnt));
  } else {
    if (reader.file_size_ != file_size_) { // reset_cursor after dump
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
        LOG_WARN("null block", K(ret), K(row_id), K(save_row_cnt_), K(row_cnt_), K(*this));
      } else if (OB_FAIL(reader.blk_->get_store_row(row_id, sr))) {
        LOG_WARN("get row from block failed",
            K(ret), K(row_id), K(save_row_cnt_), K(row_cnt_), K(*reader.blk_));
      }
    }
  }
  return ret;
}

void ObRADatumStore::Reader::reset()
{
  const int64_t file_size = 0;
  reset_cursor(file_size);
  store_.free_blk_mem(buf_.data(), buf_.capacity());
  buf_.reset();
  free_all_blks();
  store_.free_blk_mem(idx_buf_.data(), idx_buf_.capacity());
  idx_buf_.reset();
}

void ObRADatumStore::Reader::reuse()
{
  reset_cursor(0);
  free_all_blks();
  buf_.reset();
  idx_buf_.reset();
}

void ObRADatumStore::Reader::reset_cursor(const int64_t file_size)
{
  file_size_ = file_size;
  idx_blk_ = NULL;
  ib_pos_ = 0;
  blk_ = NULL;
}

void ObRADatumStore::Reader::free_all_blks()
{
  while (NULL != try_free_list_) {
    auto next = try_free_list_->next_;
    store_.free_blk_mem(try_free_list_, try_free_list_->size_);
    try_free_list_ = next;
  }
}

int ObRADatumStore::Reader::get_row(const int64_t row_id, const StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_id < 0) || OB_UNLIKELY(row_id >= get_row_cnt())) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("invalid row_id", K(ret), K(row_id), K(get_row_cnt()));
  } else if (OB_FAIL(store_.get_store_row(*this, row_id, sr))) {
    LOG_WARN("get store row failed", K(ret), K(row_id));
  } else if (OB_ISNULL(sr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL store row returned", K(ret));
  }
  return ret;
}

int ObRADatumStore::get_timeout(int64_t &timeout_ms)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_us = THIS_WORKER.get_timeout_remain();
  if (OB_UNLIKELY(timeout_us / 1000 <= 0)) {
    ret = OB_TIMEOUT;
    LOG_WARN("query is timeout", K(ret), K(timeout_us));
  } else {
    timeout_ms = timeout_us / 1000;
  }
  return ret;
}

int ObRADatumStore::write_file(BlockIndex &bi, void *buf, int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t timeout_ms = 0;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(size < 0) || OB_UNLIKELY(size > 0 && NULL == buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(size), KP(buf));
  } else if (OB_FAIL(get_timeout(timeout_ms))) {
    LOG_WARN("get timeout failed", K(ret));
  } else {
    if (!is_file_open()) {
      if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(tenant_id_, dir_id_))) {
        LOG_WARN("alloc file directory failed", K(ret));
      } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.open(tenant_id_, fd_, dir_id_))) {
        LOG_WARN("open file failed", K(ret));
      } else {
        file_size_ = 0;
        LOG_INFO("open file success", K_(fd), K_(dir_id));
      }
    }
    ret = OB_E(EventTable::EN_8) ret;
  }
  if (OB_SUCC(ret) && size > 0) {
    if (NULL != mem_stat_) {
      mem_stat_->dumped(size);
    }
    tmp_file::ObTmpFileIOInfo io;
    io.fd_ = fd_;
    io.buf_ = static_cast<char *>(buf);
    io.size_ = size;
    io.io_desc_.set_wait_event(ObWaitEventIds::ROW_STORE_DISK_WRITE);
    io.io_timeout_ms_ = timeout_ms;
    const uint64_t start = rdtsc();
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.write(tenant_id_, io))) {
      LOG_WARN("write to file failed", K(ret), K(io), K(timeout_ms));
    }
    if (NULL != io_observer_) {
      io_observer_->on_write_io(rdtsc() - start);
    }
  }
  if (OB_SUCC(ret)) {
    bi.on_disk_ = true;
    bi.offset_ = file_size_;
    file_size_ += size;
  }
  return ret;
}

int ObRADatumStore::read_file(void *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_SUCCESS;
  int64_t timeout_ms = 0;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(offset < 0)
             || OB_UNLIKELY(size < 0)
             || OB_UNLIKELY(size > 0 && NULL == buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(size), K(offset), KP(buf));
  } else if (OB_FAIL(get_timeout(timeout_ms))) {
    LOG_WARN("get timeout failed", K(ret));
  }

  if (OB_SUCC(ret) && size > 0) {
    tmp_file::ObTmpFileIOInfo io;
    io.fd_ = fd_;
    io.dir_id_ = dir_id_;
    io.buf_ = static_cast<char *>(buf);
    io.size_ = size;
    io.io_desc_.set_wait_event(ObWaitEventIds::ROW_STORE_DISK_READ);
    io.io_timeout_ms_ = timeout_ms;
    const uint64_t start = rdtsc();
    tmp_file::ObTmpFileIOHandle handle;
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.pread(tenant_id_, io, offset, handle))) {
      LOG_WARN("read form file failed", K(ret), K(io), K(offset), K(timeout_ms));
    } else if (OB_UNLIKELY(handle.get_done_size() != size)) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("read data less than expected",
          K(ret), K(io), "read_size", handle.get_done_size());
    }
    if (NULL != io_observer_) {
      io_observer_->on_read_io(rdtsc() - start);
    }
  }
  return ret;
}

int ObRADatumStore::ensure_reader_buffer(Reader &reader, ShrinkBuffer &buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    // try free expired blocks
    if (NULL != reader.try_free_list_) {
      TryFreeMemBlk *cur = reader.try_free_list_;
      TryFreeMemBlk **p_cur = &reader.try_free_list_;
      while (NULL != cur && (NULL != reader.age_) && cur->age_ >= reader.age_->get()) {
        p_cur = &cur->next_;
        cur = cur->next_;
      }
      if (NULL != cur) {
        *p_cur = NULL;
        while (NULL != cur) {
          auto p = cur->next_;
          free_blk_mem(cur, cur->size_);
          cur = p;
        }
      }
    }
    // add used block to try free list if in iteration age control.
    if (NULL != reader.age_ && buf.is_inited()) {
      TryFreeMemBlk *p = reinterpret_cast<TryFreeMemBlk *>(buf.data());
      p->next_ = reader.try_free_list_;
      p->age_ = reader.age_->get();
      p->size_ = buf.capacity();
      reader.try_free_list_ = p;
      buf.reset();
    }

    if (buf.is_inited() && buf.capacity() < size) {
      free_blk_mem(buf.data(), buf.capacity());
      buf.reset();
    }
    if (!buf.is_inited()) {
      const int64_t alloc_size = next_pow2(size);
      char *mem = static_cast<char *>(alloc_blk_mem(alloc_size, false /* link_mem_list */));
      if (OB_UNLIKELY(NULL == mem)) {
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

int ObRADatumStore::dump_block_if_need(const int64_t extra_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(need_dump(extra_size))) {
    int64_t target_dump_size = extra_size + mem_hold_ - mem_limit_;
    // Check whether an IndexBlock will be added, and pre-allocate the corresponding mem size
    if ((NULL == idx_blk_ && blocks_.count() >= DEFAULT_BLOCK_CNT - 1) ||
         (NULL != idx_blk_ && idx_blk_->is_full())) {
      target_dump_size += IndexBlock::INDEX_BLOCK_SIZE;
    }
    if (OB_FAIL(dump(false, std::max(target_dump_size, BIG_BLOCK_SIZE)))) {
      LOG_WARN("fail to dump block", K(ret), K(mem_hold_), K(mem_limit_));
    }
  }
  return ret;
}

bool ObRADatumStore::need_dump(const int64_t extra_size)
{
  bool need_to_dump = false;
  if (!GCONF.is_sql_operator_dump_enabled()) { // no dump
  } else if (mem_limit_ > 0) {
    if (mem_hold_ + extra_size > mem_limit_) {
      need_to_dump = true;
      LOG_TRACE("need dump", K(mem_hold_), K(mem_limit_));
    }
  } else {
    const int64_t mem_ctx_pct_trigger = 80;
    lib::ObMallocAllocator *instance = lib::ObMallocAllocator::get_instance();
    lib::ObTenantCtxAllocatorGuard allocator = NULL;
    if (NULL == instance) {
      int ret = common::OB_ERR_SYS;
      LOG_ERROR("NULL allocator", K(ret));
    } else if (OB_ISNULL(allocator = instance->get_tenant_ctx_allocator(
        tenant_id_, ctx_id_))) {
      // no tenant allocator, do nothing
    } else {
      const int64_t limit = allocator->get_limit();
      const int64_t hold = allocator->get_hold();
      int64_t mod_hold = 0;
      if (limit / 100 * mem_ctx_pct_trigger <= hold) {
        need_to_dump = true;
      }
      if (need_to_dump) {
        LOG_TRACE("check need dump", K(limit), K(hold), K(mod_hold));
      }
    }
  }
  return need_to_dump;
}

int ObRADatumStore::finish_add_row()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!blkbuf_.buf_.is_inited()) {
    // do nothing if ObRADatumStore is empty or has called finish_add_row already
  } else {
    if (OB_FAIL(switch_block(0 /*finish_add_row*/))) {
      LOG_WARN("write last block to file failed", K(ret));
    } else if (has_index_block() && OB_FAIL(switch_idx_block(true /* finish_add */))) {
      LOG_WARN("write last index block to file failed", K(ret));
    } else if (blkbuf_.buf_.is_inited()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sync file failed", K(ret));
    } else if (NULL != idx_blk_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("idx_blk_ is not nullptr", K(ret));
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
