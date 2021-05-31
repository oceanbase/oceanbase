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

#include "sql/engine/basic/ob_chunk_datum_store.h"
// for ObChunkStoreUtil
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/config/ob_server_config.h"

namespace oceanbase {
using namespace common;

namespace sql {
int ObChunkDatumStore::BlockBuffer::init(char* buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    data_ = buf;
    cur_pos_ = 0;
    cap_ = buf_size - sizeof(BlockBuffer);
  }
  return ret;
}

namespace chunk_datum_store {

template <typename T, typename B>
void pointer2off(T*& pointer, B* base)
{
  pointer = reinterpret_cast<T*>(reinterpret_cast<const char*>(pointer) - reinterpret_cast<const char*>(base));
}

template <typename T, typename B>
void off2pointer(T*& pointer, B* base)
{
  pointer = reinterpret_cast<T*>(reinterpret_cast<intptr_t>(pointer) + reinterpret_cast<char*>(base));
}

template <typename T, typename B>
void point2pointer(T*& dst_pointer, B* dst_base, T* src_pointer, const B* src_base)
{
  dst_pointer = reinterpret_cast<T*>(reinterpret_cast<char*>(dst_base) + reinterpret_cast<intptr_t>(src_pointer) -
                                     reinterpret_cast<const char*>(src_base));
}

}  // namespace chunk_datum_store

int ObChunkDatumStore::StoredRow::copy_datums(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx, char* buf,
    const int64_t size, const int64_t row_size, const uint32_t row_extend_size)
{
  int ret = OB_SUCCESS;
  if (payload_ != buf || size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(payload_), KP(buf), K(size));
  } else {
    cnt_ = static_cast<uint32_t>(exprs.count());
    int64_t pos = sizeof(ObDatum) * cnt_ + row_extend_size;
    row_size_ = static_cast<int32_t>(row_size);
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; ++i) {
      ObDatum& in_datum = static_cast<ObDatum&>(exprs.at(i)->locate_expr_datum(ctx));
      ObDatum* datum = new (&cells()[i]) ObDatum();
      if (OB_FAIL(datum->deep_copy(in_datum, buf, size, pos))) {
        LOG_WARN("failed to copy datum", K(ret), K(i), K(pos), K(size), K(row_size), K(in_datum), K(*datum));
      } else {
        LOG_DEBUG("succ to copy_datums", K(cnt_), K(i), K(size), K(row_size), K(in_datum), K(*datum));
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::StoredRow::copy_datums(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx, int64_t& row_size,
    char* buf, const int64_t max_buf_size, const uint32_t row_extend_size)
{
  int ret = OB_SUCCESS;
  if (max_buf_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(payload_), KP(buf), K(max_buf_size));
  } else {
    cnt_ = static_cast<uint32_t>(exprs.count());
    int64_t pos = sizeof(ObDatum) * cnt_ + row_extend_size + sizeof(StoredRow);
    ObDatum* datums = cells();
    if (pos > max_buf_size) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; ++i) {
        ObDatum* in_datum = nullptr;
        ObDatum* datum = (ObDatum*)datums;
        if (OB_FAIL(exprs.at(i)->eval(ctx, in_datum))) {
          LOG_WARN("failed to eval datum", K(ret));
        } else if (OB_FAIL(datum->deep_copy(*in_datum, buf, max_buf_size, pos))) {
          if (OB_BUF_NOT_ENOUGH != ret) {
            LOG_WARN("failed to copy datum", K(ret), K(i), K(pos), K(max_buf_size), KP(in_datum), K(*datum));
          }
        } else {
          LOG_DEBUG("succ to copy_datums", K(cnt_), K(i), K(max_buf_size), KP(in_datum), K(*datum));
        }
        ++datums;
      }
      row_size_ = static_cast<int32_t>(pos);
      row_size = row_size_;
    }
  }
  return ret;
}

int ObChunkDatumStore::StoredRow::copy_datums(common::ObDatum** datums, const int64_t cnt, char* buf,
    const int64_t size, const int64_t row_size, const uint32_t row_extend_size)
{
  int ret = OB_SUCCESS;
  if (payload_ != buf || size < 0 || nullptr == datums) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(payload_), KP(buf), K(size), K(datums));
  } else {
    cnt_ = static_cast<uint32_t>(cnt);
    int64_t pos = sizeof(ObDatum) * cnt_ + row_extend_size;
    row_size_ = static_cast<int32_t>(row_size);
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; ++i) {
      if (nullptr == datums[i]) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), KP(payload_), KP(buf), K(size), K(datums));
      } else {
        ObDatum* datum = new (&cells()[i]) ObDatum();
        if (OB_FAIL(datum->deep_copy(*datums[i], buf, size, pos))) {
          LOG_WARN(
              "failed to copy datum", K(ret), K(i), K(pos), K(size), K(row_size), K(*datums[i]), K(datums[i]->len_));
        }
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::StoredRow::copy_datums(common::ObDatum* datums, const int64_t cnt, char* buf, const int64_t size,
    const int64_t row_size, const uint32_t row_extend_size)
{
  int ret = OB_SUCCESS;
  if (payload_ != buf || size < 0 || nullptr == datums) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(payload_), KP(buf), K(size), K(datums));
  } else {
    cnt_ = static_cast<uint32_t>(cnt);
    row_size_ = static_cast<int32_t>(row_size);
    MEMCPY(buf, static_cast<const void*>(datums), size);
    int64_t pos = sizeof(ObDatum) * cnt_ + row_extend_size;
    for (int64_t i = 0; i < cnt_; ++i) {
      cells()[i].ptr_ = buf + pos;
      pos += cells()[i].len_;
    }
  }
  return ret;
}

int ObChunkDatumStore::StoredRow::to_expr(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cnt_ != exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datum count mismatch", K(ret), K(cnt_), K(exprs.count()));
  } else {
    for (uint32_t i = 0; i < cnt_; ++i) {
      exprs.at(i)->locate_expr_datum(ctx) = cells()[i];
      exprs.at(i)->get_eval_info(ctx).evaluated_ = true;
      LOG_DEBUG("succ to_expr", K(cnt_), K(exprs.count()), KPC(exprs.at(i)), K(cells()[i]), K(lbt()));
    }
  }
  return ret;
}

int ObChunkDatumStore::StoredRow::to_expr(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx, int64_t count) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cnt_ < count || exprs.count() < count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datum count mismatch", K(ret), K(cnt_), K(exprs.count()), K(count));
  } else {
    for (uint32_t i = 0; i < count; ++i) {
      exprs.at(i)->locate_expr_datum(ctx) = cells()[i];
      exprs.at(i)->get_eval_info(ctx).evaluated_ = true;
    }
  }
  return ret;
}

int ObChunkDatumStore::StoredRow::assign(const StoredRow* sr)
{
  int ret = OB_SUCCESS;
  MEMCPY(this, static_cast<const void*>(sr), sr->row_size_);
  ObDatum* src_cells = const_cast<ObDatum*>(sr->cells());
  for (int64_t i = 0; i < cnt_; ++i) {
    chunk_datum_store::point2pointer(*(const char**)&cells()[i].ptr_, this, *(const char**)&src_cells[i].ptr_, sr);
  }
  LOG_DEBUG("trace unswizzling", K(ret), K(this));
  return ret;
}

void ObChunkDatumStore::StoredRow::unswizzling(char* base /*= NULL*/)
{
  if (NULL == base) {
    base = (char*)this;
  }
  for (int64_t i = 0; i < cnt_; ++i) {
    chunk_datum_store::pointer2off(*(const char**)&cells()[i].ptr_, base);
  }
  LOG_DEBUG("trace unswizzling", K(this));
}

void ObChunkDatumStore::StoredRow::swizzling(char* base /*= NULL*/)
{
  if (NULL == base) {
    base = (char*)this;
  }
  for (int64_t i = 0; i < cnt_; ++i) {
    chunk_datum_store::off2pointer(*(const char**)&cells()[i].ptr_, base);
  }
}

int ObChunkDatumStore::Block::add_row(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx, const int64_t row_size,
    uint32_t row_extend_size, StoredRow** stored_row)
{
  int ret = OB_SUCCESS;
  BlockBuffer* buf = get_buffer();
  if (!buf->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(row_size));
  } else if (row_size > buf->remain()) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer not enough", K(row_size), "remain", buf->remain());
  } else {
    StoredRow* sr = (StoredRow*)buf->head();
    if (OB_FAIL(sr->copy_datums(
            exprs, ctx, buf->head() + ROW_HEAD_SIZE, row_size - ROW_HEAD_SIZE, row_size, row_extend_size))) {
      LOG_WARN("copy row failed", K(ret), K(row_size));
    } else {
      if (OB_FAIL(buf->advance(row_size))) {
        LOG_WARN("fill buffer head failed", K(ret), K(buf), K(row_size));
      } else {
        rows_++;
        if (NULL != stored_row) {
          *stored_row = sr;
        }
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::Block::append_row(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx* ctx, BlockBuffer* buf,
    int64_t row_extend_size, StoredRow** stored_row)
{
  int ret = OB_SUCCESS;
  if (!buf->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf));
  } else {
    int64_t max_size = buf->remain();
    if (ROW_HEAD_SIZE > max_size) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      StoredRow* sr = (StoredRow*)buf->head();
      int64_t row_size = 0;
      if (OB_FAIL(sr->copy_datums(exprs, *ctx, row_size, buf->head(), max_size, row_extend_size))) {
        if (OB_BUF_NOT_ENOUGH != ret) {
          LOG_WARN("copy row failed", K(ret), K(max_size));
        }
      } else {
        if (OB_FAIL(buf->advance(row_size))) {
          LOG_WARN("fill buffer head failed", K(ret), K(buf));
        } else {
          ++rows_;
          if (NULL != stored_row) {
            *stored_row = sr;
          }
        }
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::Block::copy_stored_row(const StoredRow& stored_row, StoredRow** dst_sr)
{
  int ret = OB_SUCCESS;
  BlockBuffer* buf = get_buffer();
  int64_t row_size = stored_row.row_size_;
  if (!buf->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(row_size));
  } else {
    StoredRow* sr = new (buf->head()) StoredRow;
    sr->assign(&stored_row);
    if (OB_FAIL(buf->advance(row_size))) {
      LOG_WARN("fill buffer head failed", K(ret), K(buf), K(row_size));
    } else {
      rows_++;
      if (nullptr != dst_sr) {
        *dst_sr = sr;
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::Block::get_store_row(int64_t& cur_pos, const StoredRow*& sr)
{
  int ret = OB_SUCCESS;
  if (cur_pos >= blk_size_) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("invalid index", K(ret), K(cur_pos), K_(rows));
  } else {
    StoredRow* row = reinterpret_cast<StoredRow*>(&payload_[cur_pos]);
    cur_pos += row->row_size_;
    sr = row;
  }
  return ret;
}

int ObChunkDatumStore::Block::gen_unswizzling_payload(char* unswizzling_payload, uint32_t size)
{
  int ret = OB_SUCCESS;
  int64_t cur_pos = 0;
  uint32_t i = 0;
  const int64_t payload_size = get_buffer()->head() - payload_;
  if (OB_ISNULL(unswizzling_payload) || size < payload_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(size), K(payload_size), KP(unswizzling_payload));
  } else {
    MEMCPY(unswizzling_payload, payload_, payload_size);
  }
  while (OB_SUCC(ret) && cur_pos < payload_size && i < rows_) {
    StoredRow* sr = reinterpret_cast<StoredRow*>(unswizzling_payload + cur_pos);
    sr->unswizzling(payload_ + cur_pos);
    cur_pos += sr->row_size_;
    i++;
  }
  return ret;
}

int ObChunkDatumStore::Block::unswizzling()
{
  int ret = OB_SUCCESS;
  int64_t cur_pos = 0;
  uint32_t i = 0;
  while (OB_SUCC(ret) && cur_pos < blk_size_ && i < rows_) {
    StoredRow* sr = reinterpret_cast<StoredRow*>(payload_ + cur_pos);
    sr->unswizzling();
    cur_pos += sr->row_size_;
    i++;
  }
  return ret;
}

int ObChunkDatumStore::Block::swizzling(int64_t* col_cnt)
{
  int ret = OB_SUCCESS;
  int64_t cur_pos = 0;
  uint32_t i = 0;
  StoredRow* sr = NULL;
  while (OB_SUCC(ret) && cur_pos < blk_size_ && i < rows_) {
    sr = reinterpret_cast<StoredRow*>(&payload_[cur_pos]);
    sr->swizzling();
    cur_pos += sr->row_size_;
    i++;
  }

  if (OB_SUCC(ret) && NULL != sr && NULL != col_cnt) {
    *col_cnt = sr->cnt_;
  }
  return ret;
}

ObChunkDatumStore::ObChunkDatumStore(common::ObIAllocator* alloc /* = NULL */)
    : inited_(false),
      tenant_id_(0),
      label_(common::ObModIds::OB_SQL_CHUNK_ROW_STORE),
      ctx_id_(0),
      mem_limit_(0),
      cur_blk_(NULL),
      cur_blk_buffer_(nullptr),
      max_blk_size_(0),
      min_blk_size_(INT64_MAX),
      default_block_size_(BLOCK_SIZE),
      n_blocks_(0),
      row_cnt_(0),
      col_count_(-1),
      enable_dump_(true),
      has_dumped_(false),
      dumped_row_cnt_(0),
      file_size_(0),
      n_block_in_file_(0),
      mem_hold_(0),
      mem_used_(0),
      allocator_(NULL == alloc ? &inner_allocator_ : alloc),
      row_extend_size_(0),
      callback_(nullptr)
{
  io_.fd_ = -1;
  io_.dir_id_ = -1;
}

int ObChunkDatumStore::init(int64_t mem_limit, uint64_t tenant_id /* = common::OB_SERVER_TENANT_ID */,
    int64_t mem_ctx_id /* = common::ObCtxIds::DEFAULT_CTX_ID */,
    const char* label /* = common::ObModIds::OB_SQL_CHUNK_ROW_STORE) */, bool enable_dump /* = true */,
    uint32_t row_extend_size /* = 0 */
)
{
  int ret = OB_SUCCESS;
  enable_dump_ = enable_dump;
  tenant_id_ = tenant_id;
  ctx_id_ = mem_ctx_id;
  label_ = label;
  if (0 == GCONF._chunk_row_store_mem_limit) {
    mem_limit_ = mem_limit;
  } else {
    mem_limit_ = GCONF._chunk_row_store_mem_limit;
  }
  inited_ = true;
  default_block_size_ = BLOCK_SIZE;
  max_blk_size_ = default_block_size_;
  min_blk_size_ = INT64_MAX;
  io_.fd_ = -1;
  row_extend_size_ = row_extend_size;
  return ret;
}

void ObChunkDatumStore::reset()
{
  int ret = OB_SUCCESS;
  if (is_file_open()) {
    aio_write_handle_.reset();
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.remove(io_.fd_))) {
      LOG_WARN("remove file failed", K(ret), K_(io_.fd));
    } else {
      LOG_INFO("close file success", K(ret), K_(io_.fd));
    }
    io_.fd_ = -1;
  }
  file_size_ = 0;
  n_block_in_file_ = 0;

  while (!blocks_.is_empty()) {
    Block* item = blocks_.remove_first();
    mem_hold_ -= item->get_buffer()->mem_size();
    mem_used_ -= item->get_buffer()->mem_size();
    if (nullptr != callback_) {
      callback_->free(item->get_buffer()->mem_size());
    }
    if (NULL != item) {
      allocator_->free(item);
    }
  }
  blocks_.reset();
  cur_blk_ = NULL;
  cur_blk_buffer_ = nullptr;
  while (!free_list_.is_empty()) {
    Block* item = free_list_.remove_first();
    mem_hold_ -= item->get_buffer()->mem_size();
    if (nullptr != callback_) {
      callback_->free(item->get_buffer()->mem_size());
    }
    if (NULL != item) {
      allocator_->free(item);
    }
  }

  LOG_DEBUG("mem usage after free", K(mem_hold_), K(mem_used_), K(blocks_.get_size()));
  mem_hold_ = 0;
  mem_used_ = mem_hold_;
  max_blk_size_ = default_block_size_;
  n_blocks_ = 0;
  row_cnt_ = 0;
}

void* ObChunkDatumStore::alloc_blk_mem(const int64_t size, const bool for_iterator)
{
  void* blk = NULL;
  int ret = OB_SUCCESS;
  if (size < 0) {
    LOG_WARN("invalid argument", K(size));
  } else {
    ObMemAttr attr(tenant_id_, label_, ctx_id_);
    void* mem = allocator_->alloc(size, attr);
    if (NULL == mem) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), K(size), KP(mem), K(label_), K(ctx_id_), K(mem_limit_), K(enable_dump_));
    } else {
      blk = static_cast<char*>(mem);
      if (!for_iterator) {
        mem_hold_ += size;
      }
      if (nullptr != callback_) {
        callback_->alloc(size);
      }
    }
  }
  return blk;
}

void ObChunkDatumStore::free_blk_mem(void* mem, const int64_t size /* = 0 */)
{
  if (NULL != mem) {
    LOG_DEBUG("free blk memory", K(size), KP(mem));
    allocator_->free(mem);
    mem_hold_ -= size;
    if (nullptr != callback_) {
      callback_->free(size);
    }
  }
}

void ObChunkDatumStore::free_block(Block* item)
{
  if (NULL != item) {
    BlockBuffer* buffer = item->get_buffer();
    if (!buffer->is_empty()) {
      mem_used_ -= buffer->mem_size();
    }
    mem_hold_ -= buffer->mem_size();
    if (nullptr != callback_) {
      callback_->free(buffer->mem_size());
    }
    allocator_->free(item);
  }
}

void ObChunkDatumStore::free_blk_list()
{
  Block* item = blocks_.remove_first();
  while (item != NULL) {
    free_block(item);
    item = blocks_.remove_first();
  }
  blocks_.reset();
}

// free mem of specified size
bool ObChunkDatumStore::shrink_block(int64_t size)
{
  int64_t freed_size = 0;
  bool succ = false;
  int ret = OB_SUCCESS;
  if ((0 == blocks_.get_size() && 0 == free_list_.get_size()) || 0 == size) {
    LOG_DEBUG("RowStore no need to shrink", K(size), K(blocks_.get_size()));
  } else {
    Block* item = free_list_.remove_first();
    // free those blocks haven't been used yet
    while (NULL != item) {
      freed_size += item->get_buffer()->mem_size();
      LOG_DEBUG("RowStore shrink free empty", K(size), K(freed_size), K_(item->blk_size));
      free_block(item);
      item = free_list_.remove_first();
    }

    item = blocks_.remove_first();
    while (OB_SUCC(ret) && NULL != item) {
      if (OB_FAIL(dump_one_block(item->get_buffer()))) {
        LOG_WARN("dump failed when shrink blk",
            K(ret),
            K(item),
            K(item->get_buffer()->data_size()),
            K(item->get_buffer()->mem_size()),
            K(size));
      }
      freed_size += item->get_buffer()->mem_size();
      LOG_DEBUG("RowStore shrink dump and free", K(size), K(freed_size), K(item->get_buffer()->mem_size()), K(item));
      free_block(item);
      item = blocks_.remove_first();
    }
  }
  LOG_DEBUG("RowStore shrink_block", K(ret), K(freed_size), K(size));
  if (freed_size >= size) {
    succ = true;
  }
  return succ;
}

int ObChunkDatumStore::init_block_buffer(void* mem, const int64_t size, Block*& block)
{
  int ret = OB_SUCCESS;
  block = new (mem) Block;
  BlockBuffer* blkbuf = new (static_cast<char*>(mem) + size - sizeof(BlockBuffer)) BlockBuffer;
  if (OB_FAIL(blkbuf->init(static_cast<char*>(mem), size))) {
    LOG_WARN("init shrink buffer failed", K(ret));
  } else {
    if (OB_FAIL(blkbuf->advance(sizeof(Block)))) {
      LOG_WARN("fill buffer head failed", K(ret), K(static_cast<void*>(blkbuf->data())), K(sizeof(Block)));
    } else {
      block->set_block_size(static_cast<uint32_t>(blkbuf->capacity()));
      block->next_ = NULL;
      block->rows_ = 0;
    }
  }
  return ret;
}

int ObChunkDatumStore::alloc_block_buffer(
    Block*& block, const int64_t data_size, const int64_t min_size, const bool for_iterator)
{
  int ret = OB_SUCCESS;
  int64_t size = std::max(min_size, data_size);
  size = next_pow2(size);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    void* mem = alloc_blk_mem(size, for_iterator);
    if (OB_ISNULL(mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), K(size));
    } else if (OB_FAIL(ObChunkDatumStore::init_block_buffer(mem, size, block))) {
      free_blk_mem(mem, size);
    } else if (!for_iterator) {
      if (size > max_blk_size_) {
        max_blk_size_ = size;
      }
      if (size < min_blk_size_) {
        min_blk_size_ = size;
      }
    }
    LOG_DEBUG("RowStore alloc block", K(ret), K_(max_blk_size), K(data_size), K(min_size), K(for_iterator));
  }
  return ret;
}

int ObChunkDatumStore::alloc_block_buffer(Block*& block, const int64_t data_size, const bool for_iterator)
{
  int64_t min_size = std::max(static_cast<int64_t>(BLOCK_SIZE), default_block_size_);
  return alloc_block_buffer(block, data_size, min_size, for_iterator);
}

inline int ObChunkDatumStore::dump_one_block(BlockBuffer* item)
{
  int ret = OB_SUCCESS;
  if (item->cur_pos_ <= 0) {
    LOG_WARN("unexpected: dump zero", K(item), K(item->cur_pos_));
  }
  item->block->magic_ = Block::MAGIC;
  if (OB_FAIL(item->get_block()->unswizzling())) {
    LOG_WARN("convert block to copyable failed", K(ret));
  } else if (OB_FAIL(write_file(item->data(), item->capacity()))) {
    LOG_WARN("write block to file failed");
  } else {
    n_block_in_file_++;
    LOG_DEBUG("RowStore Dumpped block", K_(item->block->rows), K_(item->cur_pos), K(item->capacity()));
  }
  return ret;
}

int ObChunkDatumStore::clean_block(Block* clean_block)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(clean_block)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("clean block is null", K(ret));
  } else if (blocks_.get_first() != clean_block) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("clean block is not first block", K(ret));
  } else {
    Block* cur = blocks_.remove_first();
    BlockBuffer* buf = NULL;
    buf = cur->get_buffer();
    if (!buf->is_empty()) {
      mem_used_ -= buf->mem_size();
      row_cnt_ -= cur->rows();
    }
    mem_hold_ -= buf->mem_size();
    if (nullptr != callback_) {
      callback_->free(buf->mem_size());
    }
    allocator_->free(cur);
    --n_blocks_;
  }
  return ret;
}

// only clean memory data
int ObChunkDatumStore::clean_memory_data(bool reuse)
{
  int ret = OB_SUCCESS;
  Block* cur = blocks_.remove_first();
  BlockBuffer* buf = NULL;
  while (OB_SUCC(ret) && NULL != cur) {
    buf = cur->get_buffer();
    if (!buf->is_empty()) {
      mem_used_ -= buf->mem_size();
      row_cnt_ -= cur->rows();
    }
    if (reuse && buf->mem_size() <= default_block_size_) {
      buf->reuse();
      free_list_.add_last(cur);
    } else {
      mem_hold_ -= buf->mem_size();
      if (nullptr != callback_) {
        callback_->free(buf->mem_size());
      }
      allocator_->free(cur);
    }
    cur = blocks_.remove_first();
  }
  if (mem_used_ != 0 || (!reuse && mem_hold_ != 0) || blocks_.get_size() != 0) {
    SQL_ENG_LOG(WARN,
        "hold mem after clean_memory_data",
        K_(mem_used),
        K(reuse),
        K_(mem_hold),
        K(blocks_.get_size()),
        K(free_list_.get_size()));
  }
  if (OB_SUCC(ret)) {
    cur_blk_ = NULL;
    cur_blk_buffer_ = nullptr;
  }
  return ret;
}

// dump all, dump all but skip last.
int ObChunkDatumStore::dump(bool reuse, bool all_dump)
{
  int ret = OB_SUCCESS;
  BlockBuffer* buf = NULL;
  if (!enable_dump_) {
    ret = OB_EXCEED_MEM_LIMIT;
    LOG_DEBUG("ChunkRowStore exceed mem limit and dump is disabled");
  } else {
    int64_t n_block = blocks_.get_size();
    const int64_t org_n_block = n_block;
    Block* cur = nullptr;
    while (OB_SUCC(ret) && 0 < n_block && (all_dump || 1 < n_block)) {
      cur = blocks_.remove_first();
      --n_block;
      if (OB_ISNULL(cur)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur block is null", K(ret));
      } else {
        buf = cur->get_buffer();
        LOG_DEBUG("dumping", K(cur), K(*cur), K(*buf));
        if (buf->is_empty() || OB_FAIL(dump_one_block(buf))) {
          LOG_WARN("failed to dump block", K(ret));
        }
        if (!buf->is_empty()) {
          mem_used_ -= buf->mem_size();
          dumped_row_cnt_ += cur->rows();
        }
        if (reuse && buf->mem_size() <= default_block_size_) {
          buf->reuse();
          free_list_.add_last(cur);
        } else {
          mem_hold_ -= buf->mem_size();
          if (nullptr != callback_) {
            callback_->free(buf->mem_size());
          }
          allocator_->free(cur);
        }
      }
    }
    if (all_dump && (mem_used_ != 0 || (!reuse && mem_hold_ != 0) || blocks_.get_size() != 0)) {
      LOG_WARN(
          "hold mem after dump", K_(mem_used), K(reuse), K_(mem_hold), K(blocks_.get_size()), K(free_list_.get_size()));
    }
    if (OB_SUCC(ret)) {
      if (all_dump) {
        cur_blk_ = NULL;
        cur_blk_buffer_ = nullptr;
      }
      if (!has_dumped_) {
        has_dumped_ = (org_n_block != blocks_.get_size());
      }
    }
    LOG_DEBUG("dumped block", K(n_block), K(org_n_block), K(blocks_.get_size()));
  }
  return ret;
}

bool ObChunkDatumStore::find_block_can_hold(const int64_t size, bool& need_shrink)
{
  bool found = false;
  need_shrink = false;
  if (NULL != cur_blk_ && size <= cur_blk_->get_buffer()->remain()) {
    found = true;
  } else if (free_list_.get_size() > 0 && default_block_size_ >= size) {
    Block* next = free_list_.remove_first();
    LOG_DEBUG("reuse block", K(next), K(*next), K(next->get_buffer()), K(*next->get_buffer()));
    found = true;
    use_block(next);
    blocks_.add_last(next);
    n_blocks_++;
  } else if (mem_limit_ > 0 && mem_hold_ > mem_used_ && mem_hold_ + size > mem_limit_) {
    need_shrink = true;
    LOG_DEBUG("RowStore need shrink", K(size), K(mem_hold_));
  }
  return found;
}

int ObChunkDatumStore::switch_block(const int64_t min_size)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (min_size <= 0 || OB_ISNULL(cur_blk_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(min_size));
  } else if (need_dump(min_size) && OB_FAIL(dump(true, true))) {
    if (OB_EXCEED_MEM_LIMIT != ret) {
      LOG_WARN("got error when dump blocks", K(ret));
    }
  } else {
    LOG_DEBUG("RowStore switch block", K(min_size));
    Block* new_block = NULL;
    bool need_shrink = false;
    bool can_find = find_block_can_hold(min_size, need_shrink);
    LOG_DEBUG("RowStore switch block", K(can_find), K(need_shrink), K(min_size));
    if (need_shrink) {
      if (shrink_block(min_size)) {
        LOG_DEBUG("RowStore shrink succ", K(min_size));
      }
    }
    if (!can_find) {  // need alloc new block
      if (OB_FAIL(alloc_block_buffer(new_block, min_size, false))) {
        LOG_WARN("alloc block failed", K(ret), K(min_size), K(new_block));
      }
      if (!can_find && OB_SUCC(ret)) {
        blocks_.add_last(new_block);
        n_blocks_++;
        use_block(new_block);
        LOG_DEBUG("RowStore switch block",
            K(new_block),
            K(*new_block),
            K(new_block->get_buffer()),
            K(n_blocks_),
            K(min_size));
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::add_row(
    const common::ObIArray<ObExpr*>& exprs, ObEvalCtx* ctx, const int64_t row_size, StoredRow** stored_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t min_buf_size = Block::min_buf_size(row_size);
    if (OB_SUCC(ret)) {
      if (NULL == cur_blk_) {
        Block* new_blk = nullptr;
        if (OB_FAIL(alloc_block_buffer(new_blk, min_buf_size, false))) {
          LOG_WARN("alloc block failed", K(ret));
        } else {
          use_block(new_blk);
          blocks_.add_last(cur_blk_);
          n_blocks_++;
        }
      }
      if (OB_SUCC(ret) && row_size > cur_blk_->get_buffer()->remain()) {
        if (OB_FAIL(switch_block(min_buf_size)) && OB_EXCEED_MEM_LIMIT != ret) {
          LOG_WARN("switch block failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(cur_blk_->add_row(exprs, *ctx, row_size, row_extend_size_, stored_row))) {
          LOG_WARN("add row to block failed", K(ret), K(row_size));
        } else {
          row_cnt_++;
          if (OB_UNLIKELY(0 > col_count_)) {
            col_count_ = exprs.count();
          }
        }
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::add_row(const common::ObIArray<ObExpr*>& exprs, ObEvalCtx* ctx, StoredRow** stored_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (NULL == cur_blk_) {
      int64_t min_buf_size = 0;
      Block* new_blk = nullptr;
      if (OB_FAIL(Block::min_buf_size(exprs, *ctx, min_buf_size))) {
      } else if (OB_FAIL(alloc_block_buffer(new_blk, min_buf_size, false))) {
        LOG_WARN("alloc block failed", K(ret));
      } else {
        use_block(new_blk);
        blocks_.add_last(cur_blk_);
        n_blocks_++;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(cur_blk_->append_row(exprs, ctx, cur_blk_buffer_, row_extend_size_, stored_row))) {
        if (OB_BUF_NOT_ENOUGH == ret) {
          int64_t min_buf_size = 0;
          if (OB_FAIL(Block::min_buf_size(exprs, *ctx, min_buf_size))) {
          } else if (OB_FAIL(switch_block(min_buf_size))) {
            if (OB_EXCEED_MEM_LIMIT != ret) {
              LOG_WARN("switch block failed", K(ret));
            }
          } else if (OB_FAIL(cur_blk_->append_row(exprs, ctx, cur_blk_buffer_, row_extend_size_, stored_row))) {
          } else {
            row_cnt_++;
            if (OB_UNLIKELY(0 > col_count_)) {
              col_count_ = exprs.count();
            }
          }
        } else {
          LOG_WARN("add row to block failed", K(ret));
        }
      } else {
        row_cnt_++;
        if (OB_UNLIKELY(0 > col_count_)) {
          col_count_ = exprs.count();
        }
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::try_add_row(
    const common::ObIArray<ObExpr*>& exprs, ObEvalCtx* ctx, const int64_t memory_limit, bool& row_added)
{
  int ret = OB_SUCCESS;
  int64_t row_size = 0;
  if (OB_FAIL(Block::row_store_size(exprs, *ctx, row_size, row_extend_size_))) {
    LOG_WARN("failed to calc store size");
  } else if (row_size + get_mem_used() > memory_limit) {
    row_added = false;
  } else if (OB_FAIL(add_row(exprs, ctx, row_size, NULL))) {
    LOG_WARN("add row failed", K(ret));
  } else {
    row_added = true;
  }
  return ret;
}

int ObChunkDatumStore::add_row(const StoredRow& src_stored_row, ObEvalCtx* eval_ctx, StoredRow** stored_row)
{
  UNUSED(eval_ctx);
  return add_row(src_stored_row, stored_row);
}

int ObChunkDatumStore::add_row(const StoredRow& src_stored_row, StoredRow** stored_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t row_size = src_stored_row.row_size_;
    const int64_t min_buf_size = Block::min_buf_size(row_size);
    if (OB_SUCC(ret)) {
      if (NULL == cur_blk_) {
        Block* new_blk = nullptr;
        if (OB_FAIL(alloc_block_buffer(new_blk, min_buf_size, false))) {
          LOG_WARN("alloc block failed", K(ret));
        } else {
          use_block(new_blk);
          blocks_.add_last(cur_blk_);
          n_blocks_++;
        }
      }
      if (OB_SUCC(ret) && row_size > cur_blk_->get_buffer()->remain()) {
        if (OB_FAIL(switch_block(min_buf_size)) && OB_EXCEED_MEM_LIMIT != ret) {
          LOG_WARN("switch block failed", K(ret), K(src_stored_row));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(cur_blk_->copy_stored_row(src_stored_row, stored_row))) {
          LOG_WARN("add row to block failed", K(ret), K(src_stored_row), K(row_size));
        } else {
          row_cnt_++;
          if (col_count_ < 0) {
            col_count_ = src_stored_row.cnt_;
          }
        }
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::finish_add_row(bool need_dump)
{
  int ret = OB_SUCCESS;
  int64_t timeout_ms = 0;
  if (free_list_.get_size() > 0) {
    Block* block = free_list_.remove_first();
    while (NULL != block) {
      mem_hold_ -= block->get_buffer()->mem_size();
      if (nullptr != callback_) {
        callback_->free(block->get_buffer()->mem_size());
      }
      allocator_->free(block);
      block = free_list_.remove_first();
    }
    free_list_.reset();
  }
  if (is_file_open()) {
    if (need_dump && OB_FAIL(dump(false, true)) && OB_EXCEED_MEM_LIMIT != ret) {
      LOG_WARN("finish_add_row dump error", K(ret));
    } else if (OB_FAIL(get_timeout(timeout_ms))) {
      LOG_WARN("get timeout failed", K(ret));
    } else if (OB_FAIL(aio_write_handle_.wait(timeout_ms))) {  // last buffer
      LOG_WARN("failed to wait write", K(ret));
    } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.sync(io_.fd_, timeout_ms))) {
      LOG_WARN("sync file failed", K(ret), K_(io_.fd), K(timeout_ms));
    }
  } else {
    LOG_DEBUG("finish_add_row no need to dump", K(ret));
  }

  return ret;
}

// add block manually, this block must be initialized by init_block_buffer()
// and must be removed by remove_added_block before ObChunkDatumStore<>::reset()
int ObChunkDatumStore::add_block(Block* block, bool need_swizzling, bool* added)
{
  int ret = OB_SUCCESS;
  int64_t col_cnt = 0;
  bool need_add = true;

  if (need_swizzling) {
    // for iterate
    if (block->rows_ <= 0) {
      need_add = false;
    } else if (OB_FAIL(block->swizzling(&col_cnt))) {
      LOG_WARN("add block failed", K(block), K(need_swizzling));
    } else if (-1 != this->col_count_ && this->col_count_ != col_cnt) {
      LOG_WARN("add block failed col cnt not match", K(block), K(need_swizzling), K(col_cnt), K_(col_count));
    }
  }

  if (need_add) {
    this->row_cnt_ += block->rows_;
    this->n_blocks_++;
    if (nullptr != added) {
      *added = true;
    }
    blocks_.add_last(block);
    if (NULL == cur_blk_) {
      // cur_blk_ point to the first added block
      cur_blk_ = block;
      cur_blk_buffer_ = cur_blk_->get_buffer();
    }
  }
  return ret;
}

int ObChunkDatumStore::append_block(char* buf, int size, bool need_swizzling)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id_, label_, ctx_id_);
  Block* src_block = reinterpret_cast<Block*>(buf);
  int64_t block_data_size = size;
  Block* new_block = nullptr;
  bool added = false;
  int64_t actual_size = block_data_size + sizeof(BlockBuffer);
  if (OB_FAIL(alloc_block_buffer(new_block, actual_size, actual_size, false))) {
    LOG_WARN("failed to alloc block buffer", K(ret));
  } else {
    MEMCPY(new_block->payload_, src_block->payload_, block_data_size - sizeof(Block));
    new_block->rows_ = src_block->rows_;
    BlockBuffer* block_buffer = new_block->get_buffer();
    use_block(new_block);
    if (block_data_size == sizeof(Block)) {
      // skip empty block.
      free_blk_mem(new_block, block_buffer->mem_size());
    } else if (OB_FAIL(block_buffer->advance(block_data_size - sizeof(Block)))) {
      LOG_WARN("failed to advanced buffer", K(ret));
    } else if (block_buffer->data_size() != block_data_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: data size is not match", K(ret), K(block_buffer->data_size()), K(block_data_size));
    } else if (OB_FAIL(add_block(new_block, need_swizzling, &added))) {
      LOG_WARN("fail to add block", K(ret));
    } else {
      LOG_TRACE("trace append block", K(src_block->rows_), K(size));
    }
    if (OB_FAIL(ret) && !added) {
      free_blk_mem(new_block, block_buffer->mem_size());
    }
  }
  return ret;
}

void ObChunkDatumStore::remove_added_blocks()
{
  blocks_.reset();
  n_blocks_ = 0;
  row_cnt_ = 0;
}

int ObChunkDatumStore::load_next_chunk_blocks(ChunkIterator& it)
{
  int ret = OB_SUCCESS;
  int64_t read_off = 0;
  if (NULL == it.chunk_mem_) {
    if (it.chunk_read_size_ > file_size_) {
      it.chunk_read_size_ = file_size_;
    }
    it.chunk_mem_ = static_cast<char*>(alloc_blk_mem(sizeof(char) * it.chunk_read_size_, true));
    if (OB_ISNULL(it.chunk_mem_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), K(it.chunk_read_size_));
    }
  } else {
    /* when the last block haven't been read fully,
       copy the head half to the start of chunk-mem and read next chunk after it */
    char* last_blk_end = static_cast<char*>(static_cast<void*>(it.cur_iter_blk_)) + it.cur_iter_blk_->blk_size_;
    char* chunk_end = it.chunk_mem_ + it.chunk_read_size_;
    LOG_DEBUG(
        "load chunk", KP(last_blk_end), K_(it.cur_iter_blk_->blk_size), KP(chunk_end), K(chunk_end - last_blk_end));
    if (chunk_end > last_blk_end) {
      MEMCPY(it.chunk_mem_, last_blk_end, chunk_end - last_blk_end);
      read_off += chunk_end - last_blk_end;
    }
  }

  if (OB_SUCC(ret)) {
    int64_t read_n_blocks = 0;
    int64_t read_size = it.chunk_read_size_ - read_off;
    int64_t chunk_size = it.chunk_read_size_;
    int64_t tmp_read_size = 0;
    it.chunk_n_rows_ = 0;
    if (read_size > (it.file_size_ - it.cur_iter_pos_)) {
      read_size = it.file_size_ - it.cur_iter_pos_;
      tmp_read_size = read_size;
      chunk_size = read_size + read_off;
    }

    if (0 == read_size) {
      // ret end
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS !=
          (tmp_ret = read_file(it.chunk_mem_ + read_off, tmp_read_size, it.cur_iter_pos_, it.aio_read_handle_))) {
        LOG_WARN("read blk info from file failed", K(tmp_ret), K_(it.cur_iter_pos));
      }
      if (OB_ITER_END != tmp_ret) {
        LOG_WARN("unexpected status", K(ret), K(tmp_ret));
      } else {
        ret = OB_ITER_END;
      }
    } else if (OB_FAIL(read_file(it.chunk_mem_ + read_off, read_size, it.cur_iter_pos_, it.aio_read_handle_))) {
      LOG_WARN("read blk info from file failed", K(ret), K_(it.cur_iter_pos));
    } else {
      int64_t cur_pos = 0;
      Block* block = reinterpret_cast<Block*>(it.chunk_mem_);
      Block* prev_block = block;
      do {
        if (!block->magic_check()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("StoreRow load block magic check failed", K(ret), K(it), K(cur_pos), K(chunk_size), K(io_));
        } else if (block->blk_size_ <= chunk_size - cur_pos) {
          prev_block->next_ = block;
          cur_pos += block->blk_size_;
          prev_block = block;
          block = reinterpret_cast<Block*>(it.chunk_mem_ + cur_pos);
          read_n_blocks++;
          if (prev_block->blk_size_ == 0 || prev_block->rows_ == 0) {
            ret = OB_INNER_STAT_ERROR;
            LOG_WARN("read file failed", K(ret), K(prev_block->blk_size_), K(read_n_blocks), K(cur_pos));
          } else if (OB_FAIL(prev_block->swizzling(NULL))) {
            LOG_WARN("swizzling failed after read block from file", K(ret), K(it), K(read_n_blocks));
          } else {
            it.chunk_n_rows_ += prev_block->rows_;
          }
        } else {
          break;
        }
      } while (OB_SUCC(ret) && cur_pos < (chunk_size - BlockBuffer::HEAD_SIZE));

      if (OB_SUCC(ret)) {
        prev_block->next_ = NULL;
        it.cur_iter_blk_ = reinterpret_cast<Block*>(it.chunk_mem_);
        it.cur_iter_pos_ += read_size;
        it.cur_chunk_n_blocks_ = read_n_blocks;
        it.cur_nth_blk_ += read_n_blocks;
        if (0 == read_n_blocks) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to chunk read blocks",
              K(read_n_blocks),
              K(it),
              K(read_off),
              K(it.cur_iter_pos_),
              K(read_size),
              K(chunk_size),
              K(cur_pos),
              K(ret));
        } else {
          LOG_TRACE("chunk read blocks succ:",
              K(read_n_blocks),
              K(it),
              K(read_off),
              K(it.cur_iter_pos_),
              K(read_size),
              K(chunk_size),
              K(cur_pos),
              K(it.cur_nth_blk_),
              K(it.cur_iter_blk_->next_));
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    it.set_read_file_iter_end();
    if (nullptr != it.chunk_mem_) {
      callback_free(it.chunk_read_size_);
      allocator_->free(it.chunk_mem_);
    }
    it.chunk_mem_ = nullptr;
    it.cur_iter_blk_ = nullptr;
  }
  return ret;
}

int ObChunkDatumStore::aio_read_file(ChunkIterator& it, int64_t read_size)
{
  int ret = OB_SUCCESS;
  int64_t timeout_ms = 0;
  if (OB_FAIL(get_timeout(timeout_ms))) {
    LOG_WARN("get timeout failed", K(ret));
  } else if (!it.aio_read_handle_.is_valid()) {
    // first time to read
    if (OB_FAIL(aio_write_handle_.wait(timeout_ms))) {
      LOG_WARN("failed to wait", K(ret));
    } else if (OB_FAIL(aio_read_file(it.cur_iter_blk_, read_size, it.cur_iter_pos_, it.aio_read_handle_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("read blk info from file failed", K(ret), K_(it.cur_iter_pos));
      } else if (OB_FAIL(it.aio_read_handle_.wait(timeout_ms))) {
        LOG_WARN("fail to wait io finish", K(ret), K(timeout_ms));
      } else {
        ret = OB_ITER_END;
      }
    } else if (OB_FAIL(it.aio_read_handle_.wait(timeout_ms))) {
      LOG_WARN("fail to wait io finish", K(ret), K(timeout_ms));
    } else {
      if (OB_FAIL(
              aio_read_file(it.swap_iter_blk_, read_size, it.cur_iter_pos_ + read_size, it.swap_aio_read_handle_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("read blk info from file failed", K(ret), K_(it.cur_iter_pos));
        } else {
          ret = OB_SUCCESS;
          it.next_iter_end_ = true;
        }
      }
    }
  } else if (OB_FAIL(it.next_aio_read_handle_->wait(timeout_ms))) {
    LOG_WARN("fail to wait io finish", K(ret), K(timeout_ms));
  } else if (it.next_iter_end_) {
    ret = OB_ITER_END;
  } else {
    // swap
    it.cur_iter_blk_buf_->reuse();
    Block* tmp_blk = it.cur_iter_blk_;
    it.cur_iter_blk_ = it.swap_iter_blk_;
    it.swap_iter_blk_ = tmp_blk;
    it.cur_iter_blk_buf_ = it.cur_iter_blk_->get_buffer();
    blocksstable::ObTmpFileIOHandle* tmp_handle = it.cur_aio_read_handle_;
    it.cur_aio_read_handle_ = it.next_aio_read_handle_;
    it.next_aio_read_handle_ = tmp_handle;
    if (OB_FAIL(aio_read_file(it.swap_iter_blk_, read_size, it.cur_iter_pos_ + read_size, *it.next_aio_read_handle_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("read blk info from file failed", K(ret), K_(it.cur_iter_pos));
      } else {
        ret = OB_SUCCESS;
        it.next_iter_end_ = true;
      }
    }
  }
  return ret;
}

/* get next block from BlockItemList(when all rows in mem) or read from file
 * and let it.cur_iter_blk_ point to the new block
 */
int ObChunkDatumStore::load_next_block(ChunkIterator& it)
{
  int ret = OB_SUCCESS;
  if (it.file_size_ != file_size_) {
    it.reset_cursor(file_size_);
  }
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (it.cur_nth_blk_ < -1 || it.cur_nth_blk_ >= n_blocks_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row should be saved", K(ret), K_(it.cur_nth_blk), K_(n_blocks));
  } else if (is_file_open() && !it.read_file_iter_end()) {
    LOG_DEBUG("debug read size", K(it.chunk_read_size_), K(this->max_blk_size_));
    bool enable_aio = false;
    if (it.chunk_read_size_ > 0 && it.chunk_read_size_ >= this->max_blk_size_) {
      if (OB_FAIL(load_next_chunk_blocks(it))) {
        LOG_WARN("RowStore iter load next chunk blocks failed", K(ret));
      }
    } else {
      /* read from file:
       * read a block of min_size first, if the actual size is lager, then alloc a new block and copy
       * the previous block to the head of it, then read the rest from file
       * */
      // when chunk read size is same as max blk size, then enable aio
      // every buffer is CHUNK_SIZE(64K)
      enable_aio = (BLOCK_SIZE == this->max_blk_size_ && BLOCK_SIZE == this->min_blk_size_);
      int64_t block_size = INT64_MAX == min_blk_size_
                               ? it.chunk_read_size_
                               : (it.chunk_read_size_ > min_blk_size_ ? it.chunk_read_size_ : min_blk_size_);
      int64_t read_size = block_size - sizeof(BlockBuffer);
      if (it.chunk_mem_ != NULL) {
        allocator_->free(it.chunk_mem_);
        it.chunk_mem_ = NULL;
        it.cur_iter_blk_ = NULL;
        callback_free(it.chunk_read_size_);
      }
      if (NULL == it.cur_iter_blk_) {
        // for the first time, it.cur_iter_blk_ need to be freed by Iterator
        if (OB_FAIL(alloc_block_buffer(it.cur_iter_blk_, block_size, true))) {
          LOG_WARN("alloc block failed", K(ret));
        } else if (enable_aio && OB_FAIL(alloc_block_buffer(it.swap_iter_blk_, block_size, true))) {
          LOG_WARN("alloc block failed", K(ret));
        } else {
          it.cur_iter_blk_buf_ = it.cur_iter_blk_->get_buffer();
          LOG_DEBUG("alloc a block for reading",
              K(ret),
              K(it.cur_iter_blk_),
              K(it.cur_iter_blk_->get_buffer()->capacity()),
              K(block_size));
        }
      } else {
        if (it.cur_iter_blk_buf_ == NULL) {
          ret = OB_INNER_STAT_ERROR;
          LOG_WARN("it.cur_iter_blk_buf_ is NULL", K(ret), K_(it.cur_iter_blk_buf));
        } else {
          it.cur_iter_blk_buf_->reuse();
          LOG_DEBUG("StoreRow reuse block",
              K(it.cur_iter_blk_buf_->capacity()),
              K(it.cur_iter_blk_->get_buffer()),
              K_(it.cur_iter_blk_buf),
              K(block_size),
              K(read_size));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t blk_cap = it.cur_iter_blk_buf_->capacity();

        LOG_DEBUG("RowStore need read file",
            K(it.cur_iter_blk_buf_->capacity()),
            K(it.cur_iter_blk_->blk_size_),
            K(block_size),
            K_(file_size),
            K_(it.cur_iter_pos),
            K_(it.cur_iter_blk));
        // read a normal size of Block first
        if (enable_aio && OB_FAIL(aio_read_file(it, read_size))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to aio read", K(ret));
          }
        } else if (!enable_aio &&
                   OB_FAIL(read_file(it.cur_iter_blk_, read_size, it.cur_iter_pos_, it.aio_read_handle_))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("read blk info from file failed", K(ret), K_(it.cur_iter_pos));
          }
        } else if (!it.cur_iter_blk_->magic_check()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("StoreRow load block magic check failed", K(ret), K(it.cur_iter_blk_), K(io_));
        } else if (it.cur_iter_blk_->blk_size_ > read_size) {
          LOG_DEBUG("RowStore blk size larger than min", K(read_size), K(it.cur_iter_blk_->blk_size_));
          Block* blk_head = it.cur_iter_blk_;
          int64_t ac_size = it.cur_iter_blk_->blk_size_;
          int64_t pre_size = it.cur_iter_blk_buf_->mem_size();
          LOG_DEBUG("RowStore need read file", K(ac_size), K(blk_head), K(block_size));
          if (enable_aio) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected status: enable aio", K(ret), K(max_blk_size_), K(it.chunk_read_size_));
          } else if (read_size + it.cur_iter_pos_ > file_size_) {
            ret = OB_ITER_END;
            LOG_WARN("RowStore iter end unexpected", K(ret), K(it.cur_iter_pos_), K(file_size_), K(read_size));
          } else if (blk_cap < ac_size) {
            // need to alloc new blk to hold data
            it.cur_iter_blk_ = nullptr;
            if (OB_FAIL(alloc_block_buffer(it.cur_iter_blk_, ac_size + sizeof(BlockBuffer), true)) ||
                ac_size != it.cur_iter_blk_->get_buffer()->capacity()) {
              LOG_WARN("alloc block failed", K(ret), K(ac_size), K(it.cur_iter_blk_));
              allocator_->free(blk_head);
              if (nullptr != callback_) {
                callback_->free(pre_size);
              }
            } else {
              it.cur_iter_blk_buf_ = it.cur_iter_blk_->get_buffer();
              MEMCPY(it.cur_iter_blk_, blk_head, read_size);
              allocator_->free(blk_head);
              if (nullptr != callback_) {
                callback_->free(pre_size);
              }
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(read_file(static_cast<void*>(it.cur_iter_blk_->payload_ + read_size - sizeof(Block)),
                         ac_size - read_size,
                         it.cur_iter_pos_ + read_size,
                         it.aio_read_handle_))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("read blk info from file failed", K(ret), K_(it.cur_iter_pos));
            }
          } else {
            read_size = ac_size;
          }
        }

        if (OB_FAIL(ret)) {
          if (OB_ITER_END == ret) {
            it.set_read_file_iter_end();
          }
        } else if (OB_FAIL(it.cur_iter_blk_->swizzling(NULL))) {
          LOG_WARN("swizzling failed after read block from file", K(ret), K(it));
        } else if (read_size <= 0 || it.cur_iter_blk_->blk_size_ == 0 || it.cur_iter_blk_->rows_ == 0) {
          ret = OB_INNER_STAT_ERROR;
          LOG_WARN("read file failed", K(ret), K(read_size), K(block_size), K(it.cur_iter_blk_->blk_size_));
        }
      }
      if (OB_SUCC(ret)) {
        it.cur_iter_pos_ += read_size;
        it.cur_nth_blk_++;
        it.cur_chunk_n_blocks_ = 1;
        it.cur_iter_blk_->next_ = NULL;
        it.chunk_n_rows_ = it.cur_iter_blk_->rows_;
        LOG_TRACE(
            "StoreRow read file succ", K(read_size), K_(it.cur_iter_blk), K_(*it.cur_iter_blk), K_(it.cur_iter_pos));
      } else {
        // first read disk data then read memory data, so it must free cur_iter_blk_
        if (NULL != it.cur_iter_blk_) {
          callback_free(it.cur_iter_blk_->get_buffer()->mem_size());
          allocator_->free(it.cur_iter_blk_);
          it.cur_iter_blk_ = NULL;
          it.cur_iter_blk_buf_ = nullptr;
        }
        if (NULL != it.swap_iter_blk_) {
          callback_free(it.swap_iter_blk_->get_buffer()->mem_size());
          allocator_->free(it.swap_iter_blk_);
          it.swap_iter_blk_ = NULL;
        }
      }
    }
  } else {
    it.set_read_file_iter_end();
  }
  // when it's iter_end from disk or disk is no data
  // then read data from in memory
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
  } else if (!is_file_open() || it.read_file_iter_end()) {
    ret = OB_SUCCESS;
    if (OB_ISNULL(blocks_.get_first())) {
      it.set_read_mem_iter_end();
      ret = OB_ITER_END;
      LOG_TRACE("RowStore no more blocks",
          K(ret),
          K_(it.cur_nth_blk),
          K_(n_blocks),
          K(n_block_in_file_),
          K(io_.fd_),
          K(it.iter_end_flag_));
    } else if (nullptr == blocks_.get_first()) {
      ret = OB_ITER_END;
    } else {
      // all in mem or read blocks in mem at first
      it.cur_iter_blk_ = blocks_.get_first();
      LOG_DEBUG("RowStore got block in mem", K_(it.cur_iter_blk), K_(*it.cur_iter_blk));
      it.cur_chunk_n_blocks_ = blocks_.get_size();
      it.cur_nth_blk_ += it.cur_chunk_n_blocks_;
      it.chunk_n_rows_ = this->get_row_cnt_in_memory();
      LOG_TRACE("trace read in memoery data", K(ret));
      if (it.cur_nth_blk_ != n_blocks_ - 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: last chunk data", K(it.cur_nth_blk_), K(n_blocks_), K(blocks_.get_size()), K(ret));
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::get_store_row(RowIterator& it, const StoredRow*& sr)
{
  int ret = OB_SUCCESS;
  if (!is_inited() || NULL == it.cur_iter_blk_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K_(it.cur_iter_blk));
  } else {
    if (OB_UNLIKELY(!it.cur_blk_has_next())) {
      if (Block::MAGIC == it.cur_iter_blk_->magic_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid next ptr", K(ret), K(common::lbt()));
      } else if (it.cur_iter_blk_->get_next() != NULL) {
        it.cur_iter_blk_ = it.cur_iter_blk_->get_next();
        it.cur_row_in_blk_ = 0;
        it.cur_pos_in_blk_ = 0;
        it.cur_nth_block_++;
      } else if (it.cur_nth_block_ != it.n_blocks_ - 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("load block failed",
            K(ret),
            K_(it.cur_row_in_blk),
            K_(it.cur_pos_in_blk),
            K_(it.cur_nth_block),
            K_(it.n_blocks));
      } else {
        ret = OB_ITER_END;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(it.cur_iter_blk_->get_store_row(it.cur_pos_in_blk_, sr))) {
        LOG_WARN("get row from block failed", K(ret), K_(it.cur_row_in_blk), K(*it.cur_iter_blk_));
      } else {
        it.cur_row_in_blk_++;
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObChunkDatumStore::RowIterator::RowIterator()
    : store_(NULL), cur_iter_blk_(NULL), cur_row_in_blk_(0), cur_pos_in_blk_(0), n_blocks_(0), cur_nth_block_(0)
{}

ObChunkDatumStore::RowIterator::RowIterator(ObChunkDatumStore* row_store)
    : store_(row_store), cur_iter_blk_(NULL), cur_row_in_blk_(0), cur_pos_in_blk_(0), n_blocks_(0), cur_nth_block_(0)
{}

int ObChunkDatumStore::RowIterator::init(ChunkIterator* chunk_it)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(chunk_it) || !chunk_it->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(chunk_it));
  } else {
    store_ = chunk_it->store_;
  }
  return ret;
}

ObChunkDatumStore::ChunkIterator::ChunkIterator()
    : store_(NULL),
      cur_iter_blk_(NULL),
      cur_iter_blk_buf_(NULL),
      swap_iter_blk_(nullptr),
      aio_read_handle_(),
      swap_aio_read_handle_(),
      cur_aio_read_handle_(&aio_read_handle_),
      next_aio_read_handle_(&swap_aio_read_handle_),
      next_iter_end_(false),
      cur_nth_blk_(-1),
      cur_chunk_n_blocks_(0),
      cur_iter_pos_(0),
      file_size_(0),
      chunk_read_size_(0),
      chunk_mem_(NULL),
      chunk_n_rows_(0),
      iter_end_flag_(IterEndState::PROCESSING)
{}

int ObChunkDatumStore::ChunkIterator::init(ObChunkDatumStore* store, int64_t chunk_read_size)
{
  int ret = OB_SUCCESS;
  store_ = store;
  chunk_read_size_ = chunk_read_size;
  return ret;
}

ObChunkDatumStore::ChunkIterator::~ChunkIterator()
{
  reset_cursor(0);
}

void ObChunkDatumStore::ChunkIterator::reset_cursor(const int64_t file_size)
{
  file_size_ = file_size;

  if (chunk_mem_ != NULL) {
    store_->allocator_->free(chunk_mem_);
    chunk_mem_ = NULL;
    cur_iter_blk_ = NULL;
    store_->callback_free(chunk_read_size_);
    if (read_file_iter_end()) {
      LOG_ERROR("unexpect status: chunk mem is allocated, but don't free");
    }
  }

  next_iter_end_ = false;
  aio_read_handle_.reset();
  swap_aio_read_handle_.reset();
  cur_aio_read_handle_ = &aio_read_handle_;
  next_aio_read_handle_ = &swap_aio_read_handle_;

  if (!read_file_iter_end()) {
    if (cur_iter_pos_ > 0 && NULL != store_) {
      if (cur_iter_blk_ != NULL) {
        if (nullptr != cur_iter_blk_buf_) {
          store_->callback_free(cur_iter_blk_buf_->mem_size());
        }
        store_->allocator_->free(cur_iter_blk_);
      }
      if (nullptr != swap_iter_blk_) {
        store_->callback_free(swap_iter_blk_->get_buffer()->mem_size());
        store_->allocator_->free(swap_iter_blk_);
      }
    } else {
      if (nullptr != swap_iter_blk_) {
        LOG_ERROR("unexpect status: swap iter blk is not null", K(lbt()));
      }
    }
  }
  cur_iter_blk_buf_ = nullptr;
  cur_iter_blk_ = nullptr;
  swap_iter_blk_ = nullptr;
  cur_nth_blk_ = -1;
  cur_iter_pos_ = 0;
  iter_end_flag_ = IterEndState::PROCESSING;
}

void ObChunkDatumStore::ChunkIterator::reset()
{
  reset_cursor(0);
  chunk_read_size_ = 0;
  chunk_n_rows_ = 0;
}

int ObChunkDatumStore::ChunkIterator::load_next_chunk(RowIterator& it)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("ChunkIterator not init", K(ret));
  } else if (!has_next_chunk()) {
    ret = OB_ITER_END;
  } else {
    if (chunk_read_size_ > 0) {
      cur_iter_blk_ = it.cur_iter_blk_;
    }
    if (OB_FAIL(store_->load_next_block(*this))) {
      LOG_WARN("load block failed", K(ret), K(*this));
    } else {
      it.store_ = store_;
      it.cur_iter_blk_ = cur_iter_blk_;
      it.cur_pos_in_blk_ = 0;
      it.cur_row_in_blk_ = 0;
      it.n_blocks_ = cur_chunk_n_blocks_;
      it.cur_nth_block_ = 0;
    }
  }
  return ret;
}

int ObChunkDatumStore::Iterator::init(ObChunkDatumStore* store, int64_t chunk_read_size)
{
  reset();
  int ret = OB_SUCCESS;
  if (OB_FAIL(chunk_it_.init(store, chunk_read_size)) || OB_FAIL(row_it_.init(&chunk_it_))) {
    LOG_WARN("chunk iterator or row iterator init failed", K(ret));
  }
  return ret;
}

int ObChunkDatumStore::Iterator::convert_to_row(const StoredRow* sr, common::ObDatum** datums)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: store row is null", K(ret));
  } else {
    for (uint32_t i = 0; i < sr->cnt_; ++i) {
      *datums[i] = sr->cells()[i];
    }
  }
  return ret;
}

int ObChunkDatumStore::Iterator::get_next_row(
    const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx, const StoredRow** sr /*NULL*/)
{
  int ret = OB_SUCCESS;
  const StoredRow* tmp_sr = NULL;
  if (OB_FAIL(get_next_row(tmp_sr))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next stored row failed", K(ret));
    }
  } else if (OB_FAIL(convert_to_row(tmp_sr, exprs, ctx))) {
    LOG_WARN("convert row failed", K(ret), K_(chunk_it), K_(row_it));
  } else if (NULL != sr) {
    *sr = tmp_sr;
  }
  return ret;
}

int ObChunkDatumStore::Iterator::get_next_row_skip_const(ObEvalCtx& ctx, const common::ObIArray<ObExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow* sr = NULL;
  if (OB_FAIL(get_next_row(sr))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next stored row failed", K(ret));
    }
  } else if (OB_ISNULL(sr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("returned stored row is NULL", K(ret));
  } else if (exprs.count() != sr->cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stored row cell count mismatch", K(ret), K(sr->cnt_), K(exprs.count()));
  } else {
    // Ignore const exprs to avoid const value overwrite problem, the const expr may be overwrite
    // by local mini task execution.
    // In main plan the const value overwrite problem is avoided by ad remove_const() expr,
    // but in subplan of mini task the subplan is generated in CG time, has no corresponding
    // logical operator, we can not add expr in CG.
    for (int64_t i = 0; i < sr->cnt_; i++) {
      const ObExpr* expr = exprs.at(i);
      if (IS_CONST_TYPE(expr->type_)) {  // T_QUESTIONMARK is included in IS_CONST_TYPE()
        continue;
      } else {
        expr->locate_expr_datum(ctx) = sr->cells()[i];
        expr->get_eval_info(ctx).evaluated_ = true;
      }
    }
  }

  return ret;
}

int ObChunkDatumStore::Iterator::get_next_row(common::ObDatum** datums)
{
  int ret = OB_SUCCESS;
  const StoredRow* sr = NULL;
  if (nullptr == datums) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argments: datums is null", K(datums), K(ret));
  } else if (OB_FAIL(get_next_row(sr))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next stored row failed", K(ret));
    }
  } else if (OB_FAIL(convert_to_row(sr, datums))) {
    LOG_WARN("convert row failed", K(ret), K_(chunk_it), K_(row_it));
  }
  return ret;
}

int ObChunkDatumStore::Iterator::get_next_row(const StoredRow*& sr)
{
  int ret = OB_SUCCESS;
  if (!start_iter_) {
    if (OB_FAIL(chunk_it_.load_next_chunk(row_it_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("Iterator load chunk failed", K(ret));
      }
    } else {
      start_iter_ = true;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(row_it_.get_next_row(sr))) {
    if (OB_ITER_END == ret) {
      if (OB_FAIL(chunk_it_.load_next_chunk(row_it_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("Iterator load chunk failed", K(ret));
        }
      } else if (OB_FAIL(row_it_.get_next_row(sr))) {
        LOG_WARN("get next row failed", K(ret));
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::RowIterator::convert_to_row(
    const StoredRow* sr, const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: store row is null", K(ret));
  } else if (OB_FAIL(sr->to_expr(exprs, ctx))) {
    LOG_WARN("convert store row to expr value failed", K(ret));
  }
  return ret;
}

int ObChunkDatumStore::RowIterator::get_next_row(const StoredRow*& sr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(store_->get_store_row(*this, sr))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get store row failed", K(ret), K_(cur_nth_block), K_(cur_pos_in_blk), K_(cur_row_in_blk));
    }
  }
  return ret;
}

// one block one iter end
int ObChunkDatumStore::RowIterator::get_next_block_row(const StoredRow*& sr)
{
  int ret = OB_SUCCESS;
  if (!cur_blk_has_next()) {
    if (OB_FAIL(store_->clean_block(cur_iter_blk_))) {
      LOG_WARN("failed to clean block", K(ret));
    } else {
      ret = OB_ITER_END;
    }
  } else if (OB_FAIL(store_->get_store_row(*this, sr))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get store row failed", K(ret), K_(cur_nth_block), K_(cur_pos_in_blk), K_(cur_row_in_blk));
    }
  }
  return ret;
}

int ObChunkDatumStore::get_timeout(int64_t& timeout_ms)
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

int ObChunkDatumStore::alloc_dir_id()
{
  int ret = OB_SUCCESS;
  if (-1 == io_.dir_id_ && OB_FAIL(ObChunkStoreUtil::alloc_dir_id(io_.dir_id_))) {
    LOG_WARN("allocate file directory failed", K(ret));
  }
  return ret;
}

int ObChunkDatumStore::write_file(void* buf, int64_t size)
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
      if (-1 == io_.dir_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("temp file dir id is not init", K(ret), K(io_.dir_id_));
      } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.open(io_.fd_, io_.dir_id_))) {
        LOG_WARN("open file failed", K(ret));
      } else {
        file_size_ = 0;
        io_.tenant_id_ = tenant_id_;
        io_.io_desc_.category_ = common::USER_IO;
        io_.io_desc_.wait_event_no_ = ObWaitEventIds::ROW_STORE_DISK_WRITE;
        LOG_INFO("open file success", K_(io_.fd), K_(io_.dir_id));
      }
    }
    ret = E(EventTable::EN_8) ret;
  }
  if (OB_SUCC(ret) && size > 0) {
    set_io(size, static_cast<char*>(buf));
    if (aio_write_handle_.is_valid() && OB_FAIL(aio_write_handle_.wait(timeout_ms))) {
      LOG_WARN("failed to wait write", K(ret));
    } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.aio_write(io_, aio_write_handle_))) {
      LOG_WARN("write to file failed", K(ret), K_(io), K(timeout_ms));
    }
  }
  if (OB_SUCC(ret)) {
    file_size_ += size;
    if (nullptr != callback_) {
      callback_->dumped(size);
    }
  }
  return ret;
}

int ObChunkDatumStore::read_file(
    void* buf, const int64_t size, const int64_t offset, blocksstable::ObTmpFileIOHandle& handle)
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
  } else if (!handle.is_valid()) {
    if (OB_FAIL(aio_write_handle_.wait(timeout_ms))) {
      LOG_WARN("failed to wait write", K(ret));
    }
  }

  if (OB_SUCC(ret) && size > 0) {
    this->set_io(size, static_cast<char*>(buf));
    io_.io_desc_.category_ = common::USER_IO;
    io_.io_desc_.wait_event_no_ = ObWaitEventIds::ROW_STORE_DISK_READ;
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.pread(io_, offset, timeout_ms, handle))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("read form file failed", K(ret), K(io_), K(offset), K(timeout_ms));
      }
    } else if (handle.get_data_size() != size) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("read data less than expected", K(ret), K(io_), "read_size", handle.get_data_size());
    }
  }
  return ret;
}

int ObChunkDatumStore::aio_read_file(
    void* buf, const int64_t size, const int64_t offset, blocksstable::ObTmpFileIOHandle& handle)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (offset < 0 || size < 0 || (size > 0 && NULL == buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(size), K(offset), KP(buf));
  } else if (size > 0) {
    this->set_io(size, static_cast<char*>(buf));
    io_.io_desc_.category_ = common::USER_IO;
    io_.io_desc_.wait_event_no_ = ObWaitEventIds::ROW_STORE_DISK_READ;
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.aio_pread(io_, offset, handle))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("read form file failed", K(ret), K(io_), K(offset));
      }
    }
  }
  return ret;
}

bool ObChunkDatumStore::need_dump(int64_t extra_size)
{
  bool dump = false;
  if (!GCONF.is_sql_operator_dump_enabled()) {
    // no dump
  } else if (mem_limit_ > 0) {
    if (mem_used_ + extra_size > mem_limit_) {
      dump = true;
    }
  }
  return dump;
}

int ObChunkDatumStore::append_datum_store(const ObChunkDatumStore& other_store)
{
  int ret = OB_SUCCESS;
  if (other_store.get_row_cnt() > 0) {
    ObChunkDatumStore::Iterator store_iter;
    if (OB_FAIL(const_cast<ObChunkDatumStore&>(other_store).begin(store_iter))) {
      SQL_ENG_LOG(WARN, "fail to get store_iter", K(ret));
    } else {
      const StoredRow* store_row = NULL;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(store_iter.get_next_row(store_row))) {
          if (OB_ITER_END == ret) {
            // do nothing
          } else {
            SQL_ENG_LOG(WARN, "fail to get next row", K(ret));
          }
        } else if (OB_ISNULL(store_row)) {
          ret = OB_INVALID_ARGUMENT;
          SQL_ENG_LOG(WARN, "fail to get next row", K(ret));
        } else if (OB_FAIL(add_row(*store_row))) {
          SQL_ENG_LOG(WARN, "fail to add row", K(ret));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::update_iterator(Iterator& org_it)
{
  int ret = OB_SUCCESS;
  if (!org_it.start_iter_) {
    // do nothing.
  } else if (is_file_open()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: row store has dump file", K(ret));
  } else if (org_it.row_it_.store_ != this) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: row store is not match", K(ret));
  } else {
    if (blocks_.get_size() != org_it.chunk_it_.cur_chunk_n_blocks_) {
      // update chunk iterator
      // it.cur_iter_blk_ dont' need to reset
      org_it.chunk_it_.cur_chunk_n_blocks_ = blocks_.get_size();
      org_it.chunk_it_.cur_nth_blk_ = org_it.chunk_it_.cur_chunk_n_blocks_;

      // update iterator
      // org_it.row_it_.cur_iter_blk_ = cur_iter_blk_ don't need to reset
      org_it.row_it_.n_blocks_ = org_it.chunk_it_.cur_chunk_n_blocks_;
      org_it.chunk_it_.chunk_n_rows_ = get_row_cnt_in_memory();
    }
    LOG_TRACE("trace update iterator", K(ret), K(org_it.chunk_it_), K(org_it.row_it_));
  }
  return ret;
}

int ObChunkDatumStore::assign(const ObChunkDatumStore& other_store)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(append_datum_store(other_store))) {
    LOG_WARN("fail to append datum store", K(ret));
  }

  return ret;
}

OB_DEF_SERIALIZE(ObChunkDatumStore)
{
  int ret = OB_SUCCESS;
  if (enable_dump_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("chunk datum store not support serialize if enable dump", K(ret));
  }
  LST_DO_CODE(
      OB_UNIS_ENCODE, tenant_id_, ctx_id_, mem_limit_, default_block_size_, col_count_, enable_dump_, row_extend_size_);
  int64_t count = blocks_.get_size();
  OB_UNIS_ENCODE(count);
  if (OB_SUCC(ret)) {
    Block* block = blocks_.get_first();
    while (NULL != block && OB_SUCC(ret)) {
      // serialize data_buf_size
      const int64_t payload_size = block->get_buffer()->head() - block->payload_;
      OB_UNIS_ENCODE(payload_size);
      OB_UNIS_ENCODE(block->rows_);
      if (OB_SUCC(ret)) {
        // serialize block data
        if (buf_len - pos < payload_size) {
          ret = OB_SIZE_OVERFLOW;
        } else if (OB_FAIL(block->gen_unswizzling_payload(buf + pos, payload_size))) {
          LOG_WARN("fail to unswizzling", K(ret));
        } else {
          pos += payload_size;
          // serialize next block
          block = block->get_next();
        }
      }
    }  // end while
  }

  return ret;
}

OB_DEF_DESERIALIZE(ObChunkDatumStore)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, tenant_id_, ctx_id_, mem_limit_);
  if (!is_inited()) {
    if (OB_FAIL(init(mem_limit_, tenant_id_, ctx_id_, "ObChunkRowDE", false /*enable_dump*/))) {
      LOG_WARN("fail to init chunk row store", K(ret));
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE, default_block_size_, col_count_, enable_dump_, row_extend_size_);
  if (OB_SUCC(ret)) {
    Block* block = NULL;
    int64_t payload_size = 0;
    int64_t row_cnt = 0;
    int64_t blk_cnt = 0;
    OB_UNIS_DECODE(blk_cnt);
    for (int64_t i = 0; i < blk_cnt && OB_SUCC(ret); ++i) {
      OB_UNIS_DECODE(payload_size);
      OB_UNIS_DECODE(row_cnt);
      int64_t blk_size = Block::min_buf_size(payload_size);
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(alloc_block_buffer(block, blk_size, false /*for_iterator*/))) {
        LOG_WARN("alloc block failed", K(ret), K(blk_size), KP(block));
      } else {
        MEMCPY(block->payload_, buf + pos, payload_size);
        block->rows_ = row_cnt;
        block->get_buffer()->advance(payload_size);
        pos += payload_size;
        if (OB_FAIL(add_block(block, true /*+need_swizzling*/))) {
          LOG_WARN("fail to add block", K(ret));
        }
      }
    }
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObChunkDatumStore)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      tenant_id_,
      ctx_id_,
      mem_limit_,
      default_block_size_,
      col_count_,
      enable_dump_,
      row_extend_size_);
  int64_t count = blocks_.get_size();
  OB_UNIS_ADD_LEN(count);
  Block* block = blocks_.get_first();
  while (NULL != block) {
    const int64_t payload_size = block->get_buffer()->head() - block->payload_;
    OB_UNIS_ADD_LEN(payload_size);
    OB_UNIS_ADD_LEN(block->rows_);
    len += payload_size;
    block = block->get_next();
  }  // end while

  return len;
}

}  // end namespace sql
}  // end namespace oceanbase
