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
#include "sql/engine/ob_exec_context.h"
// for ObChunkStoreUtil
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

int ObChunkDatumStore::BlockBuffer::init(char *buf, const int64_t buf_size)
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
void point2pointer(T *&dst_pointer, B *dst_base, T *src_pointer, const B *src_base, int64_t len)
{
  dst_pointer = reinterpret_cast<T *>(reinterpret_cast<char *>(dst_base) +
      reinterpret_cast<intptr_t>(src_pointer) - reinterpret_cast<const char *>(src_base) - len);
}

}

int ObChunkDatumStore::StoredRow::to_expr(const common::ObIArray<ObExpr*> &exprs,
                                          ObEvalCtx &ctx,
                                          int64_t count) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cnt_ < count || exprs.count() < count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datum count mismatch", K(ret), K(cnt_), K(exprs.count()), K(count));
  } else {
    for (uint32_t i = 0; i < count; ++i) {
      if (exprs.at(i)->is_const_expr()) {
        continue;
      } else {
        exprs.at(i)->locate_expr_datum(ctx) = cells()[i];
        exprs.at(i)->set_evaluated_projected(ctx);
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::StoredRow::assign(const StoredRow *sr)
{
  int ret = OB_SUCCESS;
  MEMCPY(this, static_cast<const void*>(sr), sr->row_size_);
  ObDatum* src_cells = const_cast<ObDatum*>(sr->cells());
  for (int64_t i = 0; i < cnt_; ++i) {
    chunk_datum_store::point2pointer(*(const char **)&cells()[i].ptr_,
                  this,
                  *(const char **)&src_cells[i].ptr_, sr, 0);
  }
  LOG_DEBUG("trace unswizzling", K(ret), K(this));
  return ret;
}

// only for LastStoredRow to use
// set null for the nth_col column
int ObChunkDatumStore::StoredRow::set_null(int64_t nth_col)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  int64_t move_len = 0;
  for (int64_t i = 0; i < cnt_; ++i) {
    if (i == nth_col) {
      len = cells()[i].len_;
      if (0 < len) {
        move_len = row_size_ - (reinterpret_cast<const char *>(cells()[i].ptr_) - reinterpret_cast<const char *>(payload_)) - len;
        LOG_DEBUG("chunk store mem move", K(move_len));
        if (0 < move_len) {
          MEMMOVE(const_cast<char *>(cells()[i].ptr_),
                const_cast<char *>(cells()[i].ptr_) + len,
                move_len);
        }
      }
      cells()[i].set_null();
    } else {
      chunk_datum_store::point2pointer(*(const char **)&cells()[i].ptr_,
                  this,
                  *(const char **)&cells()[i].ptr_, this, len);
    }
  }
  row_size_ -= len;
  LOG_DEBUG("trace unswizzling", K(ret), K(this));
  return ret;
}

void ObChunkDatumStore::StoredRow::unswizzling(char *base/*= NULL*/)
{
  if (NULL == base) {
    base = (char *)this;
  }
  unswizzling_datum(cells(), cnt_, base);
  LOG_DEBUG("trace unswizzling", K(this));
}

void ObChunkDatumStore::StoredRow::unswizzling_datum(ObDatum *datum, uint32_t cnt, char *base)
{
  for (int64_t i = 0; i < cnt; ++i) {
    chunk_datum_store::pointer2off(*(const char **)(&(datum[i].ptr_)), base);
  }
}

void ObChunkDatumStore::StoredRow::swizzling(char *base/*= NULL*/)
{
  if (NULL == base) {
    base = (char *)this;
  }
  for (int64_t i = 0; i < cnt_; ++i) {
    chunk_datum_store::off2pointer(*(const char **)&cells()[i].ptr_, base);
  }
}

template <bool UNSWIZZLING>
int ObChunkDatumStore::StoredRow::do_build(StoredRow *&sr,
                                           const ObExprPtrIArray &exprs,
                                           ObEvalCtx &ctx,
                                           char *buf,
                                           const int64_t buf_len,
                                           const uint32_t extra_size)
{
  int ret = OB_SUCCESS;
  sr = reinterpret_cast<StoredRow *>(buf);
  int64_t pos = sizeof(*sr) + sizeof(ObDatum) * exprs.count() + extra_size;
  if (pos > buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    sr->cnt_ = exprs.count();
    ObDatum *datums = sr->cells();
    for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); i++) {
      ObExpr *expr = exprs.at(i);
      ObDatum *in_datum = NULL;
      if (OB_UNLIKELY(NULL == expr)) {
        // Set datum to NULL for NULL expr
        datums[i].set_null();
      } else if (OB_FAIL(expr->eval(ctx, in_datum))) {
        LOG_WARN("expression evaluate failed", K(ret));
      } else {
        ret = UNSWIZZLING
            ? deep_copy_unswizzling(*in_datum, &datums[i], buf, buf_len, pos)
            : datums[i].deep_copy(*in_datum, buf, buf_len, pos);
      }
    }
    if (OB_SUCC(ret)) {
      sr->row_size_ = static_cast<int32_t>(pos);
    }
  }

  return ret;
}

int ObChunkDatumStore::StoredRow::build(StoredRow *&sr,
                                        const ObExprPtrIArray &exprs,
                                        ObEvalCtx &ctx,
                                        char *buf,
                                        const int64_t buf_len,
                                        const uint32_t extra_size, /* = 0 */
                                        const bool unswizzling /* = false */)
{
  return unswizzling
      ? do_build<true>(sr, exprs, ctx, buf, buf_len, extra_size)
      : do_build<false>(sr, exprs, ctx, buf, buf_len, extra_size);
}

int ObChunkDatumStore::StoredRow::build(StoredRow *&sr,
                     const ObExprPtrIArray &exprs,
                     ObEvalCtx &ctx,
                     common::ObIAllocator &alloc,
                     const uint32_t extra_size /* = 0 */)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  char *buf = NULL;
  if (OB_FAIL(Block::row_store_size(exprs, ctx, size, extra_size))) {
    LOG_WARN("get row store size failed", K(ret));
  } else if (NULL == (buf = static_cast<char *>(alloc.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(size));
  } else if (OB_FAIL(build(sr, exprs, ctx, buf, size, extra_size))) {
    LOG_WARN("build stored row failed", K(ret));
  }
  return ret;
}

int ObChunkDatumStore::Block::add_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx,
  const int64_t row_size, uint32_t row_extend_size, StoredRow **stored_row)
{
  int ret = OB_SUCCESS;
  BlockBuffer *buf = get_buffer();
  if (!buf->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(row_size));
  } else if (row_size > buf->remain()) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer not enough", K(row_size), "remain", buf->remain());
  } else {
    StoredRow *sr = NULL;
    if (OB_FAIL(StoredRow::build(sr, exprs, ctx, buf->head(), row_size, row_extend_size))) {
      LOG_WARN("build stored row failed", K(ret));
    } else if (OB_FAIL(buf->advance(sr->row_size_))) {
      LOG_WARN("fill buffer head failed", K(ret), K(buf), K(row_size));
    } else {
      rows_++;
      if (NULL != stored_row) {
        *stored_row = sr;
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::BlockBufferWrap::append_row(
  const common::ObIArray<ObExpr*> &exprs, ObEvalCtx *ctx, int64_t row_extend_size)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(is_inited());
  int64_t max_size = remain();
  int64_t pos = sizeof(ObDatum) * exprs.count() + row_extend_size + sizeof(StoredRow);
  if (pos > max_size) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    StoredRow *sr = (StoredRow*)head();
    sr->cnt_ = static_cast<uint32_t>(exprs.count());
    for (int64_t i = 0; OB_SUCC(ret) && i < sr->cnt_; ++i) {
      ObDatum &in_datum = static_cast<ObDatum&>(exprs.at(i)->locate_expr_datum(*ctx));
      ObDatum *datum = new (&sr->cells()[i])ObDatum();
      // Attension : can't print dst datum after deep_copy_unswizzling
      if (OB_FAIL(deep_copy_unswizzling(in_datum, datum, head(), max_size, pos))) {
        if (OB_BUF_NOT_ENOUGH != ret) {
          LOG_WARN("failed to copy datum", K(ret), K(i), K(pos),
            K(max_size), K(in_datum));
        }
      } else {
        LOG_DEBUG("succ to copy_datums", K(sr->cnt_), K(i), K(max_size), K(pos), K(in_datum));
      }
    }
    if (OB_SUCC(ret)) {
      sr->row_size_ = static_cast<int32_t>(pos);
      fast_advance(pos);
      rows_++;
    }
  }

  return ret;
}

int ObChunkDatumStore::Block::append_row(
  const common::ObIArray<ObExpr*> &exprs, ObEvalCtx *ctx,
  BlockBuffer *buf, int64_t row_extend_size, StoredRow **stored_row, const bool unswizzling)
{
  int ret = OB_SUCCESS;
  if (!buf->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf));
  } else {
    StoredRow *sr = NULL;
    if (OB_FAIL(StoredRow::build(sr, exprs, *ctx, buf->head(), buf->remain(),
                                 row_extend_size, unswizzling))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("build stored row failed", K(ret));
      }
    } else if (OB_FAIL(buf->advance(sr->row_size_))) {
      LOG_WARN("buffer advance failed", K(ret));
    } else {
      ++rows_;
      if (NULL != stored_row) {
        *stored_row = sr;
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::Block::copy_stored_row(const StoredRow &stored_row, StoredRow **dst_sr)
{
  int ret = OB_SUCCESS;
  BlockBuffer *buf = get_buffer();
  int64_t row_size =  stored_row.row_size_;
  if (!buf->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(row_size));
  } else {
    StoredRow *sr = new (buf->head())StoredRow;
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

int ObChunkDatumStore::Block::copy_datums(const ObDatum *datums, const int64_t cnt,
                                          const int64_t extra_size, StoredRow **dst_sr)
{
  int ret = OB_SUCCESS;
  BlockBuffer *buf = get_buffer();
  int64_t head_size = sizeof(StoredRow);
  int64_t datum_size = sizeof(ObDatum) * cnt;
  int64_t row_size = head_size + sizeof(ObDatum) * cnt + extra_size;
  if (!buf->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(row_size));
  } else {
    StoredRow *sr = new (buf->head())StoredRow;
    sr->cnt_ = cnt;
    MEMCPY(sr->payload_, static_cast<const void*>(datums), datum_size);
    char* data_start = sr->payload_ + datum_size + extra_size;
    int64_t pos = 0;
    for (int64_t i = 0; i < cnt; ++i) {
      MEMCPY(data_start + pos, datums[i].ptr_, datums[i].len_);
      sr->cells()[i].ptr_ = data_start + pos;
      pos += datums[i].len_;
      row_size += datums[i].len_;
    }
    sr->row_size_ = row_size;
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

//the memory of shadow stored row is not continuous,
//so you cannot directly copy the memory of the entire stored row,
//and you should make a deep copy of each datum in turn
int ObChunkDatumStore::Block::add_shadow_stored_row(const StoredRow &stored_row,
                                                    const uint32_t row_extend_size,
                                                    StoredRow **dst_sr)
{
  int ret = OB_SUCCESS;
  BlockBuffer *buf = get_buffer();
  int64_t row_size = stored_row.row_size_ + row_extend_size;
  if (!buf->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(row_size));
  } else {
    StoredRow *sr = new (buf->head()) StoredRow;
    char *datum_buf = sr->payload_;
    int64_t buf_size = row_size - ROW_HEAD_SIZE;
    if (OB_FAIL(sr->copy_shadow_datums(stored_row.cells(), stored_row.cnt_,
                                       datum_buf, buf_size, row_size, row_extend_size))) {
      LOG_WARN("copy shadow datums failed", K(ret));
    } else if (OB_FAIL(buf->advance(row_size))) {
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

int ObChunkDatumStore::Block::get_store_row(int64_t &cur_pos, const StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  if (cur_pos >= blk_size_) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("invalid index", K(ret), K(cur_pos), K_(rows));
  } else {
    StoredRow *row = reinterpret_cast<StoredRow *>(&payload_[cur_pos]);
    cur_pos += row->row_size_;
    sr = row;
  }
  return ret;
}

int ObChunkDatumStore::Block::gen_unswizzling_payload(char *unswizzling_payload, uint32_t size)
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
    StoredRow *sr = reinterpret_cast<StoredRow *>(unswizzling_payload + cur_pos);
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
    StoredRow *sr = reinterpret_cast<StoredRow *>(payload_ + cur_pos);
    sr->unswizzling();
    cur_pos += sr->row_size_;
    i++;
  }
  return ret;
}

int ObChunkDatumStore::Block::swizzling(int64_t *col_cnt)
{
  int ret = OB_SUCCESS;
  int64_t cur_pos = 0;
  uint32_t i = 0;
  StoredRow *sr = NULL;
  while (OB_SUCC(ret) && cur_pos < blk_size_ && i < rows_) {
    sr = reinterpret_cast<StoredRow *>(&payload_[cur_pos]);
    sr->swizzling();
    cur_pos += sr->row_size_;
    i++;
  }

  if (OB_SUCC(ret) && NULL != sr && NULL != col_cnt) {
    *col_cnt = sr->cnt_;
  }
  return ret;
}

ObChunkDatumStore::ObChunkDatumStore(const ObLabel &label, common::ObIAllocator *alloc /* = NULL */)
  : inited_(false), tenant_id_(0), label_(label),
    ctx_id_(0), mem_limit_(0), cur_blk_(NULL), cur_blk_buffer_(nullptr),
    max_blk_size_(0), min_blk_size_(INT64_MAX),
    default_block_size_(BLOCK_SIZE),
    n_blocks_(0), row_cnt_(0), col_count_(-1),
    enable_dump_(true), has_dumped_(false), dumped_row_cnt_(0),
    io_event_observer_(nullptr), file_size_(0), n_block_in_file_(0),
    mem_hold_(0), mem_used_(0), max_hold_mem_(0),
    allocator_(NULL == alloc ? &inner_allocator_ : alloc),
    row_extend_size_(0), callback_(nullptr), batch_ctx_(NULL),
    tmp_dump_blk_(nullptr)
{
  io_.fd_ = -1;
  io_.dir_id_ = -1;
}

int ObChunkDatumStore::init(int64_t mem_limit,
    uint64_t tenant_id /* = common::OB_SERVER_TENANT_ID */,
    int64_t mem_ctx_id /* = common::ObCtxIds::DEFAULT_CTX_ID */,
    const char *label /* = common::ObModIds::OB_SQL_CHUNK_ROW_STORE) */,
    bool enable_dump /* = true */,
    uint32_t row_extend_size /* = 0 */,
    int64_t default_block_size /* = BLOCK_SIZE */
)
{
  int ret = OB_SUCCESS;
  enable_dump_ = enable_dump;
  tenant_id_ = tenant_id;
  ctx_id_ = mem_ctx_id;
  UNUSED(label_);
  if (0 == GCONF._chunk_row_store_mem_limit) {
    mem_limit_ = mem_limit;
  } else {
    mem_limit_ = GCONF._chunk_row_store_mem_limit;
  }
  inited_ = true;
  default_block_size_ = std::max(static_cast<int64_t>(MIN_BLOCK_SIZE), default_block_size);
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
    Block *item = blocks_.remove_first();
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
  free_tmp_dump_blk(); // just in case, not necessary. tmp block always freed instantly after use
  while (!free_list_.is_empty()) {
    Block *item = free_list_.remove_first();
    mem_hold_ -= item->get_buffer()->mem_size();
    if (nullptr != callback_) {
      callback_->free(item->get_buffer()->mem_size());
    }
    if (NULL != item) {
      allocator_->free(item);
    }
  }

  if (NULL != batch_ctx_) {
    allocator_->free(batch_ctx_);
    batch_ctx_ = NULL;
  }

  LOG_DEBUG("mem usage after free", K(mem_hold_), K(mem_used_), K(blocks_.get_size()));
  mem_hold_ = 0;
  mem_used_ = mem_hold_;
  max_blk_size_ = default_block_size_;
  n_blocks_ = 0;
  row_cnt_ = 0;
}

void *ObChunkDatumStore::alloc_blk_mem(const int64_t size, const bool for_iterator)
{
  void *blk = NULL;
  int ret = OB_SUCCESS;
  if (size < 0) {
    LOG_WARN("invalid argument", K(size));
  } else {
    ObMemAttr attr(tenant_id_, label_, ctx_id_);
    void *mem = allocator_->alloc(size, attr);
    if (NULL == mem) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), K(size), KP(mem),
        K(label_), K(ctx_id_), K(mem_limit_), K(enable_dump_), K(mem_hold_), K(mem_used_));
    } else {
      blk = static_cast<char *>(mem);
      if (!for_iterator) {
        mem_hold_ += size;
        max_hold_mem_ = max(max_hold_mem_, mem_hold_);
      }
      if (nullptr != callback_) {
        callback_->alloc(size);
      }
    }
  }
  return blk;
}

void ObChunkDatumStore::free_blk_mem(void *mem, const int64_t size /* = 0 */)
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

void ObChunkDatumStore::free_block(Block *item)
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

//free mem of specified size
bool ObChunkDatumStore::shrink_block(int64_t size)
{
  int64_t freed_size = 0;
  bool succ = false;
  int ret = OB_SUCCESS;
  if ((0 == blocks_.get_size() && 0 == free_list_.get_size()) || 0 == size) {
    LOG_DEBUG("RowStore no need to shrink", K(size), K(blocks_.get_size()));
  } else {
    Block* item = free_list_.remove_first();
    //free those blocks haven't been used yet
    while (NULL != item) {
      freed_size += item->get_buffer()->mem_size();
      LOG_DEBUG("RowStore shrink free empty", K(size), K(freed_size), K_(item->blk_size));
      free_block(item);
      item = free_list_.remove_first();
    }

    item = blocks_.remove_first();
    while (OB_SUCC(ret) && NULL != item) {
      if (OB_FAIL(dump_one_block(item->get_buffer()))) {
        LOG_WARN("dump failed when shrink blk", K(ret), K(item),
            K(item->get_buffer()->data_size()), K(item->get_buffer()->mem_size()), K(size));
      }
      dumped_row_cnt_ += item->rows();
      // 不论成功与否，都需要释放内存
      freed_size += item->get_buffer()->mem_size();
      LOG_DEBUG("RowStore shrink dump and free", K(size), K(freed_size),
          K(item->get_buffer()->mem_size()), K(item));
      free_block(item);
      item = blocks_.remove_first();
    }
    free_tmp_dump_blk();
  }
  LOG_DEBUG("RowStore shrink_block", K(ret), K(freed_size), K(size));
  if (freed_size >= size) {
    succ = true;
  }
  return succ;
}

int ObChunkDatumStore::init_block_buffer(void* mem, const int64_t size, Block *&block)
{
  int ret = OB_SUCCESS;
  block = new(mem)Block;
  BlockBuffer* blkbuf = new(static_cast<char *>(mem) + size - sizeof(BlockBuffer))BlockBuffer;
  if (OB_FAIL(blkbuf->init(static_cast<char *>(mem), size))) {
    LOG_WARN("init shrink buffer failed", K(ret));
  } else {
    if (OB_FAIL(blkbuf->advance(sizeof(Block)))) {
      LOG_WARN("fill buffer head failed", K(ret), K(static_cast<void*>(blkbuf->data())),
          K(sizeof(Block)));
    } else {
      block->set_block_size(static_cast<uint32_t>(blkbuf->capacity()));
      block->next_ = NULL;
      block->rows_ = 0;
    }
  }
  return ret;
}

int ObChunkDatumStore::alloc_block_buffer(Block *&block, const int64_t data_size,
                                          const int64_t min_size, const bool for_iterator)
{
  int ret = OB_SUCCESS;
  int64_t size = std::max(min_size, data_size);
  size = next_pow2(size);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    void *mem = alloc_blk_mem(size, for_iterator);
    if (OB_ISNULL(mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), K(size), K(mem_hold_), K(mem_used_));
    } else if (OB_FAIL(ObChunkDatumStore::init_block_buffer(mem, size, block))){
      free_blk_mem(mem, size);
    } else if (!for_iterator) {
      if (size > max_blk_size_) {
        max_blk_size_ = size;
      }//这里需要同时设置max_blk_size_和min_blk_size
      //否则当chunk只add一行的时候min_blk_size是个未初始化的值
      if (size < min_blk_size_) {
        min_blk_size_ = size;
      }
    }
    LOG_DEBUG("RowStore alloc block",
              K(ret), K_(max_blk_size), K(data_size), K(min_size), K(for_iterator));
  }
  return ret;
}

int ObChunkDatumStore::alloc_block_buffer(Block *&block, const int64_t data_size,
    const bool for_iterator)
{
  return alloc_block_buffer(block, data_size, default_block_size_, for_iterator);
}


inline int ObChunkDatumStore::dump_one_block(BlockBuffer *item)
{
  int ret = OB_SUCCESS;
  uint64_t begin_io_dump_time = rdtsc();
  int64_t min_block_size = default_block_size_ - sizeof(BlockBuffer);
  if (item->cur_pos_ <= 0) {
    LOG_WARN("unexpected: dump zero", K(item), K(item->cur_pos_));
  }
  item->block->magic_ = Block::MAGIC;
  if (OB_FAIL(item->get_block()->unswizzling())) {
    LOG_WARN("convert block to copyable failed", K(ret));
  } else if (item->capacity() < min_block_size) {
    if (OB_ISNULL(tmp_dump_blk_)) {
      if (OB_FAIL(alloc_block_buffer(tmp_dump_blk_, default_block_size_, false))) {
        LOG_WARN("failed to alloc block buffer", K(ret));
      }
    }
    CK(min_block_size == tmp_dump_blk_->get_buffer()->capacity());
    if (OB_SUCC(ret)) {
      tmp_dump_blk_->get_buffer()->reuse();
      tmp_dump_blk_->magic_ = Block::MAGIC;
      MEMCPY(tmp_dump_blk_->payload_, item->get_block()->payload_,
                                      item->get_block()->blk_size_);
      tmp_dump_blk_->rows_ = item->get_block()->rows_;
      tmp_dump_blk_->get_buffer()->fast_advance(item->data_size() - BlockBuffer::HEAD_SIZE);
      if (OB_FAIL(write_file(tmp_dump_blk_->get_buffer()->data(),
                             tmp_dump_blk_->get_buffer()->capacity()))) {
        LOG_WARN("write block to file failed");
      }
    }
  } else if (OB_FAIL(write_file(item->data(), item->capacity()))) {
    LOG_WARN("write block to file failed");
  }
  if (OB_SUCC(ret)) {
    n_block_in_file_++;
    LOG_DEBUG("RowStore Dumpped block", K_(item->block->rows),
      K_(item->cur_pos), K(item->capacity()));
  }
  if (OB_LIKELY(nullptr != io_event_observer_)) {
    io_event_observer_->on_write_io(rdtsc() - begin_io_dump_time);
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
    SQL_ENG_LOG(WARN, "hold mem after clean_memory_data", K_(mem_used), K(reuse), K_(mem_hold),
        K(blocks_.get_size()), K(free_list_.get_size()));
  }
  if (OB_SUCC(ret)) {
    cur_blk_ = NULL;
    cur_blk_buffer_ = nullptr;
  }
  return ret;
}

// 添加一种模式，dump时候只dump已经写满的block,剩下最后一个block
int ObChunkDatumStore::dump(bool reuse, bool all_dump, int64_t dumped_size)
{
  int ret = OB_SUCCESS;
  BlockBuffer* buf = NULL;
  if (!enable_dump_) {
    ret = OB_EXCEED_MEM_LIMIT;
    LOG_DEBUG("ChunkRowStore exceed mem limit and dump is disabled");
  // } else if (OB_FAIL(blocks_.prefetch())) {
  //   LOG_WARN("failed to prefetch", K(ret));
  } else {
    dumped_size = all_dump ? INT64_MAX : dumped_size;
    int64_t n_block = blocks_.get_size();
    int64_t tmp_dumped_size = 0;
    const int64_t org_n_block = n_block;
    Block* cur = nullptr;
    while (OB_SUCC(ret) && 0 < n_block && (all_dump || 1 < n_block) && tmp_dumped_size < dumped_size) {
      cur = blocks_.remove_first();
      --n_block;
      if (OB_ISNULL(cur)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur block is null", K(ret));
      } else {
        buf = cur->get_buffer();
        int64_t tmp_size = buf->mem_size();
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
        tmp_dumped_size += tmp_size;
      }
    }
    free_tmp_dump_blk();
    if (all_dump && (mem_used_ != 0 || (!reuse && mem_hold_ != 0) || blocks_.get_size() != 0)) {
      LOG_WARN("hold mem after dump", K_(mem_used), K(reuse), K_(mem_hold),
          K(blocks_.get_size()), K(free_list_.get_size()));
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

bool ObChunkDatumStore::find_block_can_hold(const int64_t size, bool &need_shrink)
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
  } else if (need_dump(min_size) && OB_FAIL(dump(true, false, default_block_size_))) {
    if (OB_EXCEED_MEM_LIMIT != ret) {
      LOG_WARN("got error when dump blocks", K(ret));
    }
  } else {
    LOG_DEBUG("RowStore switch block", K(min_size));
    Block *new_block = NULL;
    bool need_shrink = false;
    bool can_find = find_block_can_hold(min_size, need_shrink);
    LOG_DEBUG("RowStore switch block", K(can_find), K(need_shrink), K(min_size));
    if (need_shrink) {
      if (shrink_block(min_size)) {
        LOG_DEBUG("RowStore shrink succ", K(min_size));
      }
    }
    if (!can_find) { // need alloc new block
      if (OB_FAIL(alloc_block_buffer(new_block, min_size, false))) {
        LOG_WARN("alloc block failed", K(ret), K(min_size), K(new_block));
      }
      if (!can_find && OB_SUCC(ret)){
        blocks_.add_last(new_block);
        n_blocks_++;
        use_block(new_block);
        LOG_DEBUG("RowStore switch block", K(new_block), K(*new_block), K(new_block->get_buffer()),
            K(n_blocks_), K(min_size));
      }
    }
  }
  return ret;
}

inline int ObChunkDatumStore::ensure_write_blk(const int64_t row_size)
{
  int ret = OB_SUCCESS;
  if (NULL == cur_blk_) {
    Block *new_blk = nullptr;
    if (OB_FAIL(alloc_block_buffer(new_blk, Block::min_buf_size(row_size), false))) {
      LOG_WARN("alloc block failed", K(ret));
    } else {
      use_block(new_blk);
      blocks_.add_last(cur_blk_);
      n_blocks_++;
    }
  } else if (row_size > cur_blk_buffer_->remain()) {
    if (OB_FAIL(switch_block(Block::min_buf_size(row_size))) && OB_EXCEED_MEM_LIMIT != ret) {
      LOG_WARN("switch block failed", K(ret), K(row_size));
    }
  }
  return ret;
}

int ObChunkDatumStore::add_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx *ctx,
                               const int64_t row_size, StoredRow **stored_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ensure_write_blk(row_size))) {
    LOG_WARN("ensure write blk failed", K(ret));
  } else if (OB_FAIL(cur_blk_->add_row(exprs, *ctx, row_size, row_extend_size_, stored_row))) {
          LOG_WARN("add row to block failed", K(ret), K(row_size));
  } else {
    row_cnt_++;
    if (OB_UNLIKELY(0 > col_count_)) {
      col_count_ = exprs.count();
    }
  }
  return ret;
}

int ObChunkDatumStore::add_row(
  const common::ObIArray<ObExpr*> &exprs, ObEvalCtx *ctx, StoredRow **stored_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (NULL == cur_blk_) {
      int64_t min_buf_size = 0;
      Block *new_blk = nullptr;
      if (OB_FAIL(Block::min_buf_size(exprs, row_extend_size_, *ctx, min_buf_size))) {
      } else if (OB_FAIL(alloc_block_buffer(new_blk, min_buf_size, false))) {
        LOG_WARN("alloc block failed", K(ret));
      } else {
        use_block(new_blk);
        blocks_.add_last(cur_blk_);
        n_blocks_++;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(cur_blk_->append_row(
          exprs, ctx, cur_blk_buffer_, row_extend_size_, stored_row, false))) {
        if (OB_BUF_NOT_ENOUGH == ret) {
          int64_t min_buf_size = 0;
          if (OB_FAIL(Block::min_buf_size(exprs, row_extend_size_, *ctx, min_buf_size))) {
          } else if (OB_FAIL(switch_block(min_buf_size))) {
            if (OB_EXCEED_MEM_LIMIT != ret) {
              LOG_WARN("switch block failed", K(ret));
            }
          } else if (OB_FAIL(cur_blk_->append_row(
              exprs, ctx, cur_blk_buffer_, row_extend_size_, stored_row, false))) {
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

int ObChunkDatumStore::try_add_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx *ctx,
                                   const int64_t memory_limit, bool &row_added)
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

int ObChunkDatumStore::try_add_row(const ShadowStoredRow &sr,
                                   const int64_t memory_limit,
                                   bool &row_added,
                                   StoredRow **stored_sr)
{
  int ret = OB_SUCCESS;
  const StoredRow *real_sr = sr.get_store_row();
  int64_t row_size = sr.get_store_row()->row_size_;
  if (row_size + get_mem_used() > memory_limit) {
    row_added = false;
  } else if (OB_FAIL(add_row(sr, stored_sr))) {
    LOG_WARN("add row failed", K(ret));
  } else {
    row_added = true;
  }
  return ret;
}

int ObChunkDatumStore::try_add_row(const StoredRow &sr, const int64_t memory_limit, bool &row_added)
{
  int ret = OB_SUCCESS;
  int64_t row_size = sr.row_size_;
  if (row_size + get_mem_used() > memory_limit) {
    row_added = false;
  } else if (OB_FAIL(add_row(sr))) {
    LOG_WARN("add row failed", K(ret));
  } else {
    row_added = true;
  }
  return ret;
}

int ObChunkDatumStore::try_add_batch(const StoredRow ** stored_rows,
                                     const int64_t batch_size,
                                     const int64_t memory_limit,
                                     bool &batch_added)
{
  int ret = OB_SUCCESS;
  int64_t rows_size = 0;
  batch_added = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
    rows_size += stored_rows[i]->row_size_;
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (rows_size + get_mem_used() > memory_limit) {
    batch_added = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (OB_FAIL(add_row(*stored_rows[i]))) {
        LOG_WARN("add row failed", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      batch_added = true;
    }
  }
  return ret;
}

int ObChunkDatumStore::try_add_batch(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx *ctx,
                                     const int64_t batch_size, const int64_t memory_limit,
                                     bool &batch_added)
{
  int ret = OB_SUCCESS;
  int64_t rows_size = 0;
  {
    ObEvalCtx::BatchInfoScopeGuard guard(*ctx);
    guard.set_batch_idx(0);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      int64_t curr_size = 0;
      guard.set_batch_idx(i);
      if (OB_FAIL(Block::row_store_size(exprs, *ctx, curr_size, row_extend_size_))) {
        LOG_WARN("failed to calc store size", K(ret), K(i));
      } else {
        rows_size += curr_size;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (rows_size + get_mem_used() > memory_limit) {
    batch_added = false;
  } else {
    int64_t count = 0;
    ObEvalCtx::TempAllocGuard alloc_guard(*ctx);
    ObIAllocator &alloc = alloc_guard.get_allocator();
    void *mem = nullptr;
    if (OB_ISNULL(mem = alloc.alloc(ObBitVector::memory_size(batch_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for skip", K(ret), K(batch_size), K(mem_hold_), K(mem_used_));
    } else {
      ObBitVector *skip = to_bit_vector(mem);
      skip->reset(batch_size);
      if (OB_FAIL(add_batch(exprs, *ctx, *skip, batch_size, count))) {
        LOG_WARN("failed to add batch", K(ret));
      } else {
        batch_added = true;
      }
    }
  }
  return ret;
}

// 该函数用于适配ObSortOpImpl::add_row()这个模板函数
int ObChunkDatumStore::add_row(
  const StoredRow &src_stored_row, ObEvalCtx *eval_ctx, StoredRow **stored_row)
{
  UNUSED(eval_ctx);
  return add_row(src_stored_row, stored_row);
}

/*
 * 从ObChunkDatumStore读出数据，然后再写入ObChunkDatumStore时，使用copy_row
 * 从operator的ObExpr的ObDatum中写入到ObChunkDatumStore时，使用add_row
 * 理论上只有这两个接口
 */
int ObChunkDatumStore::add_row(
  const StoredRow &src_stored_row, StoredRow **stored_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t row_size = src_stored_row.row_size_;
    if (OB_FAIL(ensure_write_blk(row_size))) {
      LOG_WARN("ensure write block failed", K(ret));
    } else if (OB_FAIL(cur_blk_->copy_stored_row(src_stored_row, stored_row))) {
      LOG_WARN("add row to block failed", K(ret), K(src_stored_row), K(row_size));
    } else {
      row_cnt_++;
      if (col_count_ < 0) {
        col_count_ = src_stored_row.cnt_;
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::add_row(const ObDatum *datums, const int64_t cnt,
                               const int64_t extra_size, StoredRow **stored_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int64_t head_size = sizeof(StoredRow);
    int64_t datum_size = sizeof(ObDatum) * cnt;
    int64_t data_size = 0;
    for (int64_t i = 0; i < cnt; ++i) {
      data_size += datums[i].len_;
    }
    const int64_t row_size = head_size + datum_size + extra_size + data_size;
    if (OB_FAIL(ensure_write_blk(row_size))) {
      LOG_WARN("ensure write block failed", K(ret));
    } else if (OB_FAIL(cur_blk_->copy_datums(datums, cnt, extra_size, stored_row))) {
      LOG_WARN("add row to block failed", K(ret), K(datums), K(cnt), K(extra_size), K(row_size));
    } else {
      row_cnt_++;
      if (col_count_ < 0) {
        col_count_ = cnt;
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::add_row(const ShadowStoredRow &sr, StoredRow **stored_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const StoredRow *lsr = sr.get_store_row();
    const int64_t row_size = lsr->row_size_ + row_extend_size_;
    if (OB_FAIL(ensure_write_blk(row_size))) {
      LOG_WARN("ensure write block failed", K(ret));
    } else if (OB_FAIL(cur_blk_->add_shadow_stored_row(*lsr, row_extend_size_, stored_row))) {
      LOG_WARN("add row to block failed", K(ret), KPC(lsr), K(row_size));
    } else {
      row_cnt_++;
      if (col_count_ < 0) {
        col_count_ = lsr->cnt_;
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::add_batch(
    const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
    const ObBitVector &skip, const int64_t batch_size,
    int64_t &stored_rows_count,
    StoredRow **stored_rows,
    const int64_t start_pos /* 0 */)
{
  int ret = OB_SUCCESS;
  // FIXME bin.lb: adapt to selector version currently,
  // may be need an optimized implementation for no skip.
  CK(is_inited());
  OZ(init_batch_ctx(exprs.count(), ctx.max_batch_size_));
  int64_t size = 0;
  if (OB_SUCC(ret)) {
    for (int64_t i = start_pos; i < batch_size; i++) {
      if (skip.at(i)) {
        continue;
      } else {
        batch_ctx_->selector_[size++] = i;
      }
    }
  }
  if (OB_SUCC(ret)) {
    stored_rows_count = size;
    if (OB_FAIL(add_batch(exprs, ctx, skip, batch_size,
                          batch_ctx_->selector_, size, stored_rows))) {
      LOG_WARN("add batch failed");
    }
  }

  return ret;
}

int ObChunkDatumStore::add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                                 const ObBitVector &skip, const int64_t batch_size,
                                 const uint16_t selector[], const int64_t size,
                                 StoredRow **stored_rows)
{
  int ret = OB_SUCCESS;
  CK(is_inited());
  OZ(init_batch_ctx(exprs.count(), ctx.max_batch_size_));
  const bool reuse_block = true;
  const bool dump_last_block = false;
  if (OB_SUCC(ret) && need_dump(0) && OB_FAIL(dump(reuse_block, dump_last_block))) {
    LOG_WARN("dump failed", K(ret));
  }
  DisableDumpGuard disable_dump(*this);
  bool all_batch_res = true;
  for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); i++) {
    ObExpr *e = exprs.at(i);
    if (OB_ISNULL(e)) {
      batch_ctx_->datums_[i] = nullptr;
    } else if (OB_FAIL(e->eval_batch(ctx, skip, batch_size))) {
      LOG_WARN("evaluate batch failed", K(ret));
    } else {
      if (!e->is_batch_result()) {
        all_batch_res = false;
        break;
      } else {
        batch_ctx_->datums_[i] = e->locate_batch_datums(ctx);
      }
    }
  }
  if (OB_SUCC(ret) && !all_batch_res) {
    if (OB_FAIL(add_batch_fallback(exprs, ctx, skip, batch_size, selector, size, stored_rows))) {
      LOG_WARN("add batch fallback failed", K(batch_size), K(size));
    }
  }

  if (OB_SUCC(ret) && all_batch_res) {
    if (OB_FAIL(inner_add_batch(batch_ctx_->datums_, exprs, selector, size,
                                NULL == stored_rows ? batch_ctx_->stored_rows_ : stored_rows))) {
      LOG_WARN("inner add batch failed", K(ret), K(batch_size), K(size));
    }
  }
  return ret;
}

template <uint32_t LEN>
struct AssignFixedLenDatumValue
{
  static void assign_datum_value(void *dst, const char *src, uint32_t len)
  {
    UNUSED(len);
    MEMCPY(dst, src, LEN);
  }
};

struct AssignNumberDatumValue
{
  static void assign_datum_value(void *dst, const char *src, uint32_t len)
  {
    if (4 == len) {
      MEMCPY(dst, src, 4);
    } else if (8 == len) {
      MEMCPY(dst, src, 8);
    } else if (12 == len){
      MEMCPY(dst, src, 12);
    } else {
      MEMCPY(dst, src, len);
    }
  }
};

struct AssignDefaultDatumValue
{
  static void assign_datum_value(void *dst, const char *src, uint32_t len)
  {
    MEMCPY(dst, src, len);
  }
};

template <typename T>
static void assign_datums(const ObDatum **datums, const uint16_t selector[], const int64_t size,
    ObChunkDatumStore::StoredRow **stored_rows, int64_t col_idx)
{
  const ObDatum *cur_datums = datums[col_idx];
  for (int64_t i = 0; i < size; i++) {
    ObChunkDatumStore::StoredRow *srow = stored_rows[i];
    const ObDatum &src = cur_datums[selector[i]];
    ObDatum &dst = srow->cells()[col_idx];
    dst.pack_ = src.pack_;
    dst.ptr_ = reinterpret_cast<char *>(srow) + srow->row_size_;
    if (!src.is_null()) {
      T::assign_datum_value((void *)dst.ptr_, src.ptr_, src.len_);
    }
    srow->row_size_ += src.len_;
  }
}

int ObChunkDatumStore::inner_add_batch(const ObDatum **datums,
                                       const common::ObIArray<ObExpr *> &exprs,
                                       const uint16_t selector[],
                                       const int64_t size, StoredRow **stored_rows)
{
  int ret = OB_SUCCESS;
  // calc row size
  uint32_t *size_array = batch_ctx_->row_size_array_;
  int64_t col_cnt = exprs.count();
  const int64_t base_row_size = Block::ROW_HEAD_SIZE
      + sizeof(ObDatum) * col_cnt + row_extend_size_;
  for (int64_t i = 0; i < size; i++) {
    size_array[i] = base_row_size;
  }
  for (int64_t col_idx = 0; col_idx < col_cnt; col_idx++) {
    if (OB_ISNULL(datums[col_idx])) {
      continue;
    }
    const ObDatum *cur_datums = datums[col_idx];
    for (int64_t i = 0; i < size; i++) {
      size_array[i] += cur_datums[selector[i]].len_;
    }
  }

  // allocate block and assign stored rows
  int64_t idx = 0;
  while (idx < size && OB_SUCC(ret)) {
    if (OB_FAIL(ensure_write_blk(size_array[idx]))) {
      LOG_WARN("ensure write block failed", K(ret), K(size_array[idx]), K(col_cnt), K(size));
      for (int64_t col_idx = 0; col_idx < col_cnt; col_idx++) {
        if (OB_ISNULL(datums[col_idx])) {
          continue;
        }
        const ObDatum *cur_datums = datums[col_idx];
      }
    } else {
      int64_t data_size = 0;
      const int64_t remain = cur_blk_buffer_->remain();
      char *buf = cur_blk_buffer_->head();
      int64_t rows = 0;
      for (int64_t i = idx; i < size; i++) {
        if (data_size + size_array[i] <= remain) {
          StoredRow *srow = reinterpret_cast<StoredRow *>(buf + data_size);
          stored_rows[i] = srow;
          srow->cnt_ = col_cnt;
          srow->row_size_ = base_row_size;
          rows += 1;
          data_size += size_array[i];
        } else {
          break;
        }
      }
      cur_blk_buffer_->fast_advance(data_size);
      cur_blk_->rows_ += rows;
      idx += rows;
    }
  }

  // copy data
  // FIXME bin.lb:
  // 1. try row style loop?
  if (OB_SUCC(ret)) {
    for (int64_t col_idx = 0; col_idx < col_cnt; col_idx++) {
      if (OB_ISNULL(exprs.at(col_idx))) {
        for (int64_t i = 0; i < size; i++) {
          stored_rows[i]->cells()[col_idx].set_null();
        }
        continue;
      }
      ObObjType meta_type = exprs.at(col_idx)->datum_meta_.type_;
      const ObObjDatumMapType datum_map_type = ObDatum::get_obj_datum_map_type(meta_type);
      switch (datum_map_type) {
      case OBJ_DATUM_NUMBER:
        assign_datums<AssignNumberDatumValue>(datums, selector, size, stored_rows, col_idx);
        break;
      case OBJ_DATUM_8BYTE_DATA:
        assign_datums<AssignFixedLenDatumValue<8>>(datums, selector, size, stored_rows, col_idx);
        break;
      case OBJ_DATUM_4BYTE_DATA:
        assign_datums<AssignFixedLenDatumValue<4>>(datums, selector, size, stored_rows, col_idx);
        break;
      case OBJ_DATUM_1BYTE_DATA:
        assign_datums<AssignFixedLenDatumValue<1>>(datums, selector, size, stored_rows, col_idx);
        break;
      default:
        assign_datums<AssignDefaultDatumValue>(datums, selector, size, stored_rows, col_idx);
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row_cnt_ += size;
  }

#ifndef NDEBUG
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < size; i++) {
      if (size_array[i] != stored_rows[i]->row_size_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row size not match", K(ret), K(i),
                 K(size_array[i]), K(stored_rows[i]->row_size_));
      }
    }
  }
#endif
  return ret;
}

int ObChunkDatumStore::add_batch_fallback(
    const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
    const ObBitVector &, const int64_t batch_size,
    const uint16_t selector[], const int64_t size,
    StoredRow **stored_rows)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_size(batch_size);
  for (int64_t i = 0; i < size && OB_SUCC(ret); i++) {
    int64_t idx = selector[i];
    batch_info_guard.set_batch_idx(idx);
    StoredRow *srow = NULL;
    if (OB_FAIL(add_row(exprs, &ctx, &srow))) {
      LOG_WARN("add row failed", K(ret), K(i), K(idx));
    } else {
      if (NULL != stored_rows) {
        stored_rows[i] = srow;
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
    Block *block = free_list_.remove_first();
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
    } else {
      uint64_t begin_io_dump_time = rdtsc();
      if (OB_FAIL(get_timeout(timeout_ms))) {
        LOG_WARN("get timeout failed", K(ret));
      } else if (OB_FAIL(aio_write_handle_.wait(timeout_ms))) { // last buffer
        LOG_WARN("failed to wait write", K(ret));
      } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.sync(io_.fd_, timeout_ms))) {
        LOG_WARN("sync file failed", K(ret), K_(io_.fd), K(timeout_ms));
      }
      if (OB_LIKELY(nullptr != get_io_event_observer())) {
        get_io_event_observer()->on_write_io(rdtsc() - begin_io_dump_time);
      }
    }
  } else {
    LOG_DEBUG("finish_add_row no need to dump", K(ret));
  }
  return ret;
}

//add block manually, this block must be initialized by init_block_buffer()
//and must be removed by remove_added_block before ObChunkDatumStore<>::reset()
int ObChunkDatumStore::add_block(Block* block, bool need_swizzling, bool *added)
{
  int ret = OB_SUCCESS;
  int64_t col_cnt = 0;
  bool need_add = true;

  if (need_swizzling) {
    //for iterate
    if (block->rows_ <= 0) {
      need_add = false;
    } else if (OB_FAIL(block->swizzling(&col_cnt))) {
      LOG_WARN("add block failed", K(block), K(need_swizzling));
    } else if (-1 != this->col_count_ && this->col_count_ != col_cnt) {
      LOG_WARN("add block failed col cnt not match", K(block), K(need_swizzling),
          K(col_cnt), K_(col_count));
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
      //cur_blk_ point to the first added block
      cur_blk_ = block;
      cur_blk_buffer_ = cur_blk_->get_buffer();
    }
  }
  return ret;
}

int ObChunkDatumStore::append_block(char *buf, int size,  bool need_swizzling)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id_, label_, ctx_id_);
  Block *src_block = reinterpret_cast<Block*>(buf);
  int64_t block_data_size = size;
  Block *new_block = nullptr;
  bool added = false;
  int64_t actual_size = block_data_size + sizeof(BlockBuffer);
  if (OB_FAIL(alloc_block_buffer(new_block, actual_size, actual_size, false))) {
    LOG_WARN("failed to alloc block buffer", K(ret));
  } else {
    MEMCPY(new_block->payload_, src_block->payload_, block_data_size - sizeof(Block));
    new_block->rows_ = src_block->rows_;
    BlockBuffer *block_buffer = new_block->get_buffer();
    use_block(new_block);
    if (block_data_size == sizeof(Block)) {
      // 空block，忽略
      free_blk_mem(new_block, block_buffer->mem_size());
    } else if (OB_FAIL(block_buffer->advance(block_data_size - sizeof(Block)))) {
      LOG_WARN("failed to advanced buffer", K(ret));
    } else if (block_buffer->data_size() != block_data_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: data size is not match", K(ret),
        K(block_buffer->data_size()), K(block_data_size));
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

// Append part of a block. Only payload is given, without Block header.
int ObChunkDatumStore::append_block_payload(char *payload, int size, int rows, bool need_swizzling)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id_, label_, ctx_id_);
  int64_t block_data_size = size + sizeof(Block);
  Block *new_block = nullptr;
  bool added = false;
  int64_t actual_size = block_data_size + sizeof(BlockBuffer);
  if (OB_FAIL(alloc_block_buffer(new_block, actual_size, actual_size, false))) {
    LOG_WARN("failed to alloc block buffer", K(ret));
  } else {
    MEMCPY(new_block->payload_, payload, block_data_size - sizeof(Block));
    new_block->rows_ = rows;
    BlockBuffer *block_buffer = new_block->get_buffer();
    use_block(new_block);
    if (block_data_size == sizeof(Block)) {
      // 空block，忽略
      free_blk_mem(new_block, block_buffer->mem_size());
    } else if (OB_FAIL(block_buffer->advance(block_data_size - sizeof(Block)))) {
      LOG_WARN("failed to advanced buffer", K(ret));
    } else if (block_buffer->data_size() != block_data_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: data size is not match", K(ret),
        K(block_buffer->data_size()), K(block_data_size));
    } else if (OB_FAIL(add_block(new_block, need_swizzling, &added))) {
      LOG_WARN("fail to add block", K(ret));
    } else {
      LOG_TRACE("trace append block", K(rows), K(size));
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

int ObChunkDatumStore::get_store_row(RowIterator &it, const StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  if (!is_inited() || NULL == it.cur_iter_blk_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K_(it.cur_iter_blk));
  } else {
    if (OB_UNLIKELY(!it.cur_blk_has_next())) {
      if (OB_UNLIKELY(Block::MAGIC == it.cur_iter_blk_->magic_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid next ptr", K(ret), K(common::lbt()));
      } else if (it.cur_iter_blk_->get_next() != NULL) {
        it.cur_iter_blk_ = it.cur_iter_blk_->get_next();
        it.cur_row_in_blk_ = 0;
        it.cur_pos_in_blk_ = 0;
        it.cur_nth_block_++;
      } else if (it.cur_nth_block_ != it.n_blocks_ - 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("load block failed", K(ret), K_(it.cur_row_in_blk), K_(it.cur_pos_in_blk),
            K_(it.cur_nth_block), K_(it.n_blocks));
      } else {
        ret = OB_ITER_END;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(it.cur_iter_blk_->get_store_row(it.cur_pos_in_blk_, sr))) {
          LOG_WARN("get row from block failed", K(ret), K_(it.cur_row_in_blk),
            K(*it.cur_iter_blk_));
      } else {
        it.cur_row_in_blk_++;
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObChunkDatumStore::RowIterator::RowIterator()
  : store_(NULL),
    cur_iter_blk_(NULL),
    cur_row_in_blk_(0),
    cur_pos_in_blk_(0),
    n_blocks_(0),
    cur_nth_block_(0)
{

}

ObChunkDatumStore::RowIterator::RowIterator(ObChunkDatumStore *row_store)
  : store_(row_store),
    cur_iter_blk_(NULL),
    cur_row_in_blk_(0),
    cur_pos_in_blk_(0),
    n_blocks_(0),
    cur_nth_block_(0)
{
}

int ObChunkDatumStore::RowIterator::init(ObChunkDatumStore *store)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    store_ = store;
  }
  return ret;
}

int ObChunkDatumStore::Iterator::init(ObChunkDatumStore *store,
                                      const IterationAge *age /* = NULL */)
{
  reset();
  int ret = OB_SUCCESS;
  store_ = store;
  age_ = age;
  default_block_size_ = store->default_block_size_;
  if (OB_FAIL(row_it_.init(store))) {
    LOG_WARN("row iterator init failed", K(ret));
  }
  return ret;
}

/*
 * from StoredRow to NewRow
 * 这里暂时没有对datums的有效性做检查，理论上需要传入datums的个数cnt
 */
int ObChunkDatumStore::Iterator::convert_to_row(const StoredRow *sr, common::ObDatum **datums)
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

/*
 * 上层operator直接传入需要计算的ObExpr来获取ObDatum
 */
int ObChunkDatumStore::Iterator::get_next_row(const common::ObIArray<ObExpr*> &exprs,
                                              ObEvalCtx &ctx,
                                              const StoredRow **sr/*NULL*/)
{
  int ret = OB_SUCCESS;
  const StoredRow *tmp_sr = NULL;
  if (OB_FAIL(get_next_row(tmp_sr))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next stored row failed", K(ret));
    }
  } else if (OB_FAIL(convert_to_row(tmp_sr, exprs, ctx))) {
    LOG_WARN("convert row failed", K(ret), K_(row_it));
  } else if (NULL != sr) {
    *sr = tmp_sr;
  }
  return ret;
}

/*
 * 上层operator等，通过绑定ObExpr的ObDatum或者构造ObDatum的数组
 * 当获取到一行时，直接写入datums，这样上层就可以直接拿到数据，对于operator由于已经指向了真是地址
 * 所以相当于写入了数据，不需要额外再做处理
 */
int ObChunkDatumStore::Iterator::get_next_row(common::ObDatum **datums)
{
  int ret = OB_SUCCESS;
  const StoredRow *sr = NULL;
  if (nullptr == datums) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argments: datums is null", K(datums), K(ret));
  } else if (OB_FAIL(get_next_row(sr))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next stored row failed", K(ret));
    }
  } else if (OB_FAIL(convert_to_row(sr, datums))) {
    LOG_WARN("convert row failed", K(ret), K_(row_it));
  }
  return ret;
}

/*
 * 直接获取这行数据的原始datum，没有添加任何修饰
 * 可以通过convert_to_row将 StoredRow 转化成 Datums 数据
 */
int ObChunkDatumStore::Iterator::get_next_row(const StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  if (!start_iter_) {
    if (OB_FAIL(load_next_block(row_it_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("Iterator load chunk failed", K(ret));
      }
    } else {
      start_iter_ = true;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(row_it_.get_next_row(sr))) {
    if (OB_ITER_END == ret) {
      if (OB_FAIL(load_next_block(row_it_))) {
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

int ObChunkDatumStore::Iterator::get_next_batch(const StoredRow **rows,
                                                const int64_t max_rows,
                                                int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  if (NULL == rows || max_rows <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(rows), K(read_rows));
  } else {
    if (!start_iter_) {
      if (OB_FAIL(load_next_block(row_it_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("Iterator load chunk failed", K(ret));
        }
      } else {
        start_iter_ = true;
      }
    }
  }
  if (OB_SUCC(ret)) {
    // free cached blocks first, which are cached for previous batch
    begin_new_batch();
    for (read_rows = 0; read_rows < max_rows && OB_SUCC(ret); ) {
      int64_t tmp_read_rows = 0;
      if (OB_FAIL(row_it_.get_next_batch(rows + read_rows,
                                         max_rows - read_rows,
                                         tmp_read_rows))) {
        if (OB_ITER_END == ret) {
          read_rows += tmp_read_rows;
          if (NULL == chunk_mem_ || 0 == read_rows + tmp_read_rows) {
            // read next block if not in chunk iterate and already got row.
            if (OB_FAIL(load_next_block(row_it_))) {
              if (OB_ITER_END != ret) {
                LOG_WARN("Iterator load chunk failed", K(ret));
              }
            }
          }
        } else {
          LOG_WARN("read row failed", K(ret));
        }
      } else {
        read_rows += tmp_read_rows;
      }
    }
    // return success if got row
    if (OB_ITER_END == ret && read_rows != 0) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
};

int ObChunkDatumStore::RowIterator::convert_to_row(
  const StoredRow *sr, const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx)
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

int ObChunkDatumStore::RowIterator::get_next_row(const StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(store_->get_store_row(*this, sr))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get store row failed", K(ret), K_(cur_nth_block), K_(cur_pos_in_blk),
          K_(cur_row_in_blk));
    }
  }
  return ret;
}

int ObChunkDatumStore::RowIterator::get_next_batch(const StoredRow **rows,
                             const int64_t max_rows, int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  read_rows = 0;
  if (NULL == rows || max_rows <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(rows), K(read_rows));
  } else {
    while (read_rows < max_rows && OB_SUCC(ret)) {
      if (OB_FAIL(get_next_row(rows[read_rows]))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("read row failed", K(ret));
        }
      } else {
        read_rows += 1;
      }
    }
  }
  if (OB_ITER_END == ret && read_rows != 0) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObChunkDatumStore::RowIterator::get_next_batch(const common::ObIArray<ObExpr*> &exprs,
         ObEvalCtx &ctx, const int64_t max_rows, int64_t &read_rows, const StoredRow **rows)
{
  int ret = OB_SUCCESS;
  read_rows = 0;
  if (OB_FAIL(get_next_batch(rows, max_rows, read_rows))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next batch failed", K(ret), K(max_rows));
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t col_idx = 0; col_idx < exprs.count(); col_idx++) {
      ObExpr *e = exprs.at(col_idx);
      if (e->is_const_expr()) {
        continue;
      } else {
        ObDatum *datums = e->locate_batch_datums(ctx);
        if (!e->is_batch_result()) {
          datums[0] = rows[0]->cells()[col_idx];
        } else {
          for (int64_t i = 0; i < read_rows; i++) {
            datums[i] = rows[i]->cells()[col_idx];
          }
        }
        e->set_evaluated_projected(ctx);
        ObEvalInfo &info = e->get_eval_info(ctx);
        info.notnull_ = false;
        info.point_to_frame_ = false;
      }
    }
  }

  return ret;
}

int ObChunkDatumStore::get_timeout(int64_t &timeout_ms)
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

int ObChunkDatumStore::write_file(void *buf, int64_t size)
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
        io_.io_desc_.set_wait_event(ObWaitEventIds::ROW_STORE_DISK_WRITE);
        LOG_INFO("open file success", K_(io_.fd), K_(io_.dir_id));
      }
    }
    ret = OB_E(EventTable::EN_8) ret;
  }
  if (OB_SUCC(ret) && size > 0) {
    set_io(size, static_cast<char *>(buf));
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
  void *buf,
  const int64_t size,
  const int64_t offset,
  blocksstable::ObTmpFileIOHandle &handle,
  const int64_t file_size,
  const int64_t cur_pos,
  int64_t &tmp_file_size)
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
  int64_t read_size = file_size - cur_pos;
  if (OB_FAIL(ret)) {
  } else if (0 >= size) {
    CK (cur_pos >= file_size);
    OX (ret = OB_ITER_END);
  } else {
    blocksstable::ObTmpFileIOInfo tmp_io = io_;
    set_io(size, static_cast<char *>(buf), tmp_io);
    tmp_io.io_desc_.set_wait_event(ObWaitEventIds::ROW_STORE_DISK_READ);

    if (0 == read_size
        && OB_FAIL(FILE_MANAGER_INSTANCE_V2.get_tmp_file_size(tmp_io.fd_, tmp_file_size))) {
      LOG_WARN("failed to get tmp file size", K(ret));
    } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.pread(tmp_io, offset, timeout_ms, handle))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("read form file failed", K(ret), K(tmp_io), K(offset), K(timeout_ms));
      }
    } else if (handle.get_data_size() != size) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("read data less than expected",
          K(ret), K(tmp_io), "read_size", handle.get_data_size());
    }
  }
  return ret;
}

int ObChunkDatumStore::aio_read_file(
  void *buf,
  const int64_t size,
  const int64_t offset,
  blocksstable::ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (offset < 0 || size < 0 || (size > 0 && NULL == buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(size), K(offset), KP(buf));
  } else if (size > 0) {
    blocksstable::ObTmpFileIOInfo tmp_io = io_;
    set_io(size, static_cast<char *>(buf), tmp_io);
    tmp_io.io_desc_.set_wait_event(ObWaitEventIds::ROW_STORE_DISK_READ);
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.aio_pread(tmp_io, offset, handle))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("read form file failed", K(ret), K(tmp_io), K(offset));
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

int ObChunkDatumStore::append_datum_store(const ObChunkDatumStore &other_store)
{
  int ret = OB_SUCCESS;
  if (other_store.get_row_cnt() > 0) {
    ObChunkDatumStore::Iterator store_iter;
    if (OB_FAIL(const_cast<ObChunkDatumStore &>(other_store).begin(store_iter))) {
      SQL_ENG_LOG(WARN, "fail to get store_iter", K(ret));
    } else {
      const StoredRow *store_row = NULL;
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

int ObChunkDatumStore::assign(const ObChunkDatumStore &other_store)
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
  int64_t ser_ctx_id = ctx_id_;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_0_0) {
    if (ObCtxIds::DEFAULT_CTX_ID == ser_ctx_id) {
      // do nothing
    } else if (ObCtxIds::WORK_AREA == ser_ctx_id) {
      ser_ctx_id = OLD_WORK_AREA_ID;
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected ctx id", K(ser_ctx_id), K(lbt()));
    }
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              ser_ctx_id,
              mem_limit_,
              default_block_size_,
              col_count_,
              enable_dump_,
              row_extend_size_);
  int64_t count = blocks_.get_size();
  OB_UNIS_ENCODE(count);
  if (OB_SUCC(ret)) {
    Block *block = blocks_.get_first();
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
    } //end while
  }

  return ret;
}


OB_DEF_DESERIALIZE(ObChunkDatumStore)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              ctx_id_,
              mem_limit_);
  if (ObCtxIds::DEFAULT_CTX_ID == ctx_id_
      || ObCtxIds::WORK_AREA == ctx_id_) {
    // do nothing
  } else if (OLD_WORK_AREA_ID == ctx_id_) {
    ctx_id_ = ObCtxIds::WORK_AREA;
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected ctx id", K(ctx_id_), K(lbt()));
  }
  if (!is_inited()) {
    if (OB_FAIL(init(mem_limit_, tenant_id_,
                     ctx_id_, label_, false/*enable_dump*/))) {
      LOG_WARN("fail to init chunk row store", K(ret));
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              default_block_size_,
              col_count_,
              enable_dump_,
              row_extend_size_);
  if (OB_SUCC(ret)) {
    Block *block = NULL;
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
  Block *block = blocks_.get_first();
  while (NULL != block) {
    const int64_t payload_size = block->get_buffer()->head() - block->payload_;
    OB_UNIS_ADD_LEN(payload_size);
    OB_UNIS_ADD_LEN(block->rows_);
    len += payload_size;
    block = block->get_next();
  } //end while

  return len;
}

int ObChunkDatumStore::init_batch_ctx(const int64_t col_cnt, const int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == batch_ctx_)) {
    const int64_t size = sizeof(*batch_ctx_)
        + sizeof(ObDatum *) * col_cnt
        + sizeof(*batch_ctx_->row_size_array_) * max_batch_size
        + sizeof(*batch_ctx_->selector_) * max_batch_size
        + sizeof(*batch_ctx_->stored_rows_) * max_batch_size;
    char *mem = static_cast<char *>(allocator_->alloc(size));
    if (OB_UNLIKELY(max_batch_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("max batch size is not positive when init batch ctx", K(ret), K(max_batch_size));
    } else if (NULL == mem) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(size), K(col_cnt), K(max_batch_size),
        K(mem_hold_), K(mem_used_));
    } else {
      auto begin = mem;
      batch_ctx_ = reinterpret_cast<BatchCtx *>(mem);
      mem += sizeof(*batch_ctx_);
#define SET_BATCH_CTX_FIELD(X, N) \
      batch_ctx_->X = reinterpret_cast<typeof(batch_ctx_->X)>(mem); \
      mem += sizeof(*batch_ctx_->X) * N;

      SET_BATCH_CTX_FIELD(datums_, col_cnt);
      SET_BATCH_CTX_FIELD(stored_rows_, max_batch_size);
      SET_BATCH_CTX_FIELD(row_size_array_, max_batch_size);
      SET_BATCH_CTX_FIELD(selector_, max_batch_size);
#undef SET_BATCH_CTX_FIELD

      if (mem - begin != size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("size mismatch", K(ret), K(mem - begin), K(size), K(col_cnt), K(max_batch_size));
      }
    }
  }
  return ret;
}

void ObChunkDatumStore::free_tmp_dump_blk()
{
  if (NULL != tmp_dump_blk_) {
    free_block(tmp_dump_blk_);
    tmp_dump_blk_ = nullptr;
  }
}


void ObChunkDatumStore::Iterator::reset_cursor(const int64_t file_size)
{
  file_size_ = file_size;

  if (chunk_mem_ != NULL) {
    store_->allocator_->free(chunk_mem_);
    chunk_mem_ = NULL;
    cur_iter_blk_ = NULL;
    store_->callback_free(0);
    if (read_file_iter_end()) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpect status: chunk mem is allocated, but don't free");
    }
  }

  aio_read_handle_.reset();

  const bool force_free = true;
  if (NULL != aio_blk_) {
    free_block(aio_blk_, aio_blk_buf_->mem_size(), force_free);
  }
  if (NULL != read_blk_) {
    free_block(read_blk_, read_blk_buf_->mem_size(), force_free);
  }
  aio_blk_ = NULL;
  aio_blk_buf_ = NULL;
  read_blk_ = NULL;
  read_blk_buf_ = NULL;

  while (NULL != icached_.get_first()) {
    free_block(icached_.remove_first(), default_block_size_, force_free);
  }

  while (NULL != ifree_list_.get_first()) {
    free_block(ifree_list_.remove_first(), default_block_size_, force_free);
  }

  if (nullptr != blk_holder_ptr_) {
    if (blk_holder_ptr_->block_list_.get_size() > 0) {
      blk_holder_ptr_->release();
      blk_holder_ptr_ = nullptr;
    }
  }

  cur_iter_blk_ = nullptr;
  cur_nth_blk_ = -1;
  cur_iter_pos_ = 0;
  iter_end_flag_ = IterEndState::PROCESSING;
}

int ObChunkDatumStore::Iterator::load_next_block(RowIterator& it)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("chunk store not init", K(ret));
  } else if (store_->n_blocks_ <= 0 || cur_nth_blk_ >= store_->n_blocks_ - 1) {
    ret = OB_ITER_END;
  } else {
    CK(store_->is_inited());
    if (OB_SUCC(ret) && file_size_ != store_->file_size_) {
      reset_cursor(store_->file_size_);
    }
    if (OB_FAIL(ret)) {
    } else if (cur_nth_blk_ < -1 || cur_nth_blk_ >= store_->n_blocks_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("row should be saved", K(ret), K_(cur_nth_blk), K_(store_->n_blocks));
    } else if (store_->is_file_open() && !read_file_iter_end()) {
      uint64_t begin_io_read_time = rdtsc();
      // return at least one block when read file not end (!read_file_iter_end())
      if (OB_FAIL(read_next_blk())) {
        LOG_WARN("read next blk failed", K(ret));
      } else {
        if (cur_iter_pos_ >= file_size_) {
          set_read_file_iter_end();
        } else {
          if (OB_FAIL(prefetch_next_blk())) {
            LOG_WARN("prefetch next blk failed", K(ret));
          }
        }
      }
      if (OB_LIKELY(nullptr != store_->get_io_event_observer())) {
        store_->get_io_event_observer()->on_read_io(rdtsc() - begin_io_read_time);
      }
    } else {
      ret = OB_ITER_END;
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      set_read_file_iter_end();
      if (NULL == store_->blocks_.get_first()) {
        set_read_mem_iter_end();
        ret = OB_ITER_END;
      } else {
        cur_iter_blk_ = store_->blocks_.get_first();
        cur_chunk_n_blocks_ = store_->blocks_.get_size();
        cur_nth_blk_ += cur_chunk_n_blocks_;
        chunk_n_rows_ = store_->get_row_cnt_in_memory();
        if (cur_nth_blk_ != store_->n_blocks_ - 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status",
                  K(ret), K(cur_nth_blk_), K(store_->n_blocks_), K(store_->blocks_.get_size()));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    it.store_ = store_;
    it.cur_iter_blk_ = cur_iter_blk_;
    it.cur_pos_in_blk_ = 0;
    it.cur_row_in_blk_ = 0;
    it.n_blocks_ = cur_chunk_n_blocks_;
    it.cur_nth_block_ = 0;
  }
  return ret;
}


int ObChunkDatumStore::Iterator::read_next_blk()
{
  int ret = OB_SUCCESS;
  if (NULL == aio_blk_) {
    if (OB_FAIL(prefetch_next_blk())) {
      LOG_WARN("prefetch next blk failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(aio_wait())) {
      LOG_WARN("aio wait failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && !aio_blk_->magic_check()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read corrupt data", K(ret), K(aio_blk_->magic_),
             K(store_->file_size_), K(cur_iter_pos_));
  }
  if (OB_SUCC(ret)) {
    // data block is larger than min block
    const int64_t loaded_len = aio_blk_buf_->capacity();
    if (aio_blk_->blk_size_ > loaded_len) {
      Block *blk= NULL;
      if (OB_FAIL(alloc_block(blk, aio_blk_->blk_size_ + sizeof(BlockBuffer)))) {
        LOG_WARN("alloc block failed", K(ret), K(aio_blk_->blk_size_));
      } else {
        BlockBuffer *blk_buf = blk->get_buffer();
        MEMCPY(blk, aio_blk_, loaded_len);
        free_block(aio_blk_, aio_blk_buf_->mem_size());
        aio_blk_ = blk;
        aio_blk_buf_ = blk_buf;
        if (OB_FAIL(aio_read((char *)aio_blk_ + loaded_len,
                             aio_blk_->blk_size_ - loaded_len))) {
          LOG_WARN("aio read failed", K(ret));
        } else if (OB_FAIL(aio_wait())) {
          LOG_WARN("aio wait failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    // move aio block to read block
    if (NULL != read_blk_) {
      free_block(read_blk_, read_blk_buf_->mem_size());
    }
    read_blk_ = aio_blk_;
    read_blk_buf_ = aio_blk_buf_;
    aio_blk_ = NULL;
    aio_blk_buf_ = NULL;
    if (OB_FAIL(read_blk_->swizzling(NULL))) {
      LOG_WARN("swizzling failed", K(ret));
    } else {
      cur_chunk_n_blocks_ = 1;
      cur_nth_blk_ += 1;
      read_blk_->next_ = NULL;
      cur_iter_blk_ = read_blk_;
      chunk_n_rows_ = cur_iter_blk_->rows_;
    }
  }
  return ret;
}

int ObChunkDatumStore::Iterator::prefetch_next_blk()
{
  int ret = OB_SUCCESS;
  CK(NULL == aio_blk_);
  const int64_t block_size = store_->min_blk_size_;
  if (OB_FAIL(alloc_block(aio_blk_, block_size))) {
    LOG_WARN("allocate block buffer failed", K(ret));
  } else {
    aio_blk_buf_ = aio_blk_->get_buffer();
    if (OB_FAIL(aio_read((char *)aio_blk_, aio_blk_buf_->capacity()))) {
      LOG_WARN("aio read failed", K(ret));
    }
  }
  return ret;
}

int ObChunkDatumStore::Iterator::alloc_block(Block *&blk, const int64_t size)
{
  int ret = OB_SUCCESS;
  try_free_cached_blocks();
  if (size == default_block_size_ && NULL != ifree_list_.get_first()) {
    blk = ifree_list_.remove_first();
    ObChunkDatumStore::init_block_buffer(blk, size, blk);
  } else if (OB_FAIL(store_->alloc_block_buffer(blk, size, true))) {
    LOG_WARN("alloc block buffer failed", K(ret), K(size));
  }
  return ret;
}

void ObChunkDatumStore::Iterator::free_block(Block *blk, const int64_t size,
                                                  const bool force_free)
{
  if (NULL != blk) {
    bool do_phy_free = force_free;
    if (!force_free) {
      try_free_cached_blocks();
      if (NULL != blk_holder_ptr_) {
         // fill iter ptr at pos of age, since we do not use age in shared cache
        *((int64_t *)((char *)blk + size - sizeof(int64_t))) = reinterpret_cast<int64_t> (this);
        blk_holder_ptr_->block_list_.add_last(blk);
        blk->blk_size_ = size;
      } else if (NULL != age_) {
        STATIC_ASSERT(sizeof(BlockBuffer) >= sizeof(int64_t), "unexpected block buffer size");
        // Save age to the tail of the block, we always allocate one BlockBuffer in tail of block,
        // it's safe to write it here.
        *((int64_t *)((char *)blk + size - sizeof(int64_t))) = age_->get();
        // Save memory size to %blk_size_
        blk->blk_size_ = size;
        icached_.add_last(blk);
      } else {
        if (size == default_block_size_ && !force_free) {
#ifndef NDEBUG
          memset((char *)blk + sizeof(*blk), 0xAA, size - sizeof(*blk));
#endif
          ifree_list_.add_last(blk);
        } else {
          do_phy_free = true;
        }
      }
    }

    if (do_phy_free) {
#ifndef NDEBUG
      memset(blk, 0xAA, size);
#endif
      store_->allocator_->free(blk);
      store_->callback_free(size);
    }
  }
}

void ObChunkDatumStore::Iterator::try_free_cached_blocks()
{
  const int64_t read_age = NULL == age_ ? INT64_MAX : age_->get();
  // try free age expired blocks
  while (NULL != icached_.get_first()) {
    Block *b = icached_.get_first();
    // age is stored in tail of block, see free_block()
    const int64_t age = *((int64_t *)((char *)b + b->blk_size_ - sizeof(int64_t)));
    if (age < read_age) {
      b = icached_.remove_first();
      if (b->blk_size_ == default_block_size_) {
#ifndef NDEBUG
        memset((char *)b + sizeof(*b), 0xAA, b->blk_size_ - sizeof(*b));
#endif
        ifree_list_.add_last(b);
      } else {
        const bool force_free = true;
        free_block(b, b->blk_size_, force_free);
      }
    } else {
      break;
    }
  }
}

int ObChunkDatumStore::Iterator::aio_read(char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (!aio_read_handle_.is_valid()) {
    // first read, wait write finish
    int64_t timeout_ms = 0;
    OZ(store_->get_timeout(timeout_ms));
    OZ(store_->aio_write_handle_.wait(timeout_ms));
  }
  if (OB_SUCC(ret)) {
    if (size <= 0 || cur_iter_pos_ >= file_size_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected read offset", K(ret), K(size), K(cur_iter_pos_), K(file_size_));
    } else {
      int64_t read_size = std::min(file_size_ - cur_iter_pos_, size);
      if (OB_FAIL(store_->aio_read_file(buf, read_size, cur_iter_pos_, aio_read_handle_))) {
        LOG_WARN("aio read file failed", K(ret), K(read_size));
      } else {
        cur_iter_pos_ += size;
      }
    }
  }
  return ret;
}

int ObChunkDatumStore::Iterator::aio_wait()
{
  int ret = OB_SUCCESS;
  int64_t timeout_ms = 0;
  OZ(store_->get_timeout(timeout_ms));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(aio_read_handle_.wait(timeout_ms))) {
      LOG_WARN("aio wait failed", K(ret), K(timeout_ms));
    }
  }
  return ret;
}

void ObChunkDatumStore::IteratedBlockHolder::release()
{
  while (block_list_.get_size() > 0) {
    Block *blk = block_list_.remove_first();
    Iterator *iter = reinterpret_cast<Iterator *> (*((int64_t *)((char *)blk + blk->blk_size_ - sizeof(int64_t))));
    if (OB_NOT_NULL(blk) && OB_NOT_NULL(iter)) {
      iter->free_block(blk, blk->blk_size_, true);
    } else {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "get unexpected block pair", KP(iter), KP(blk));
    }
  }
}

} // end namespace sql
} // end namespace oceanbase
