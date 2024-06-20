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

#ifndef OCEANBASE_BASIC_OB_CHUNK_ROW_STORE_CPP_
#define OCEANBASE_BASIC_OB_CHUNK_ROW_STORE_CPP_

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_chunk_row_store.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/config/ob_server_config.h"


namespace oceanbase
{
using namespace common;

namespace sql
{
int ObChunkRowStore::BlockBuffer::init(char *buf, const int64_t buf_size)
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

namespace chunk_row_store {

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

inline int ObChunkRowStore::StoredRow::copy_row(const ObNewRow &r, char *buf,
    const int64_t size, const int64_t row_size, const uint32_t row_extend_size)
{
  int ret = OB_SUCCESS;
  if (payload_ != buf || size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(payload_), KP(buf), K(size));
  } else {
    cnt_ = static_cast<uint32_t>(r.get_count());
    //LOG_DEBUG("Molly copy row", K(r.get_count()), K(r.count_), K(r.projector_size_), K(cnt_));
    int64_t pos = sizeof(ObObj) * cnt_ + row_extend_size;
    row_size_ = static_cast<int32_t>(row_size);
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; ++i) {
      ObObj *obj = new (&cells()[i])ObObj();
      if (OB_FAIL(obj->deep_copy(r.get_cell(i), buf, size, pos))) {
        LOG_WARN("copy cell failed", K(ret));
      }
    }
  }

  return ret;
}

int ObChunkRowStore::StoredRow::assign(const StoredRow *sr)
{
  int ret = OB_SUCCESS;
  MEMCPY(this, static_cast<const void*>(sr), sr->row_size_);
  ObObj* src_cells = const_cast<ObObj*>(sr->cells());
  for (int64_t i = 0; i < cnt_; ++i) {
    if (cells()[i].need_deep_copy()) {
      chunk_row_store::point2pointer(cells()[i].v_.string_, this, src_cells[i].v_.string_, sr);
    }
  }

  return ret;
}

void ObChunkRowStore::StoredRow::unswizzling(char *base/*= NULL*/)
{
  if (NULL == base) {
    base = (char *)this;
  }
  for (int64_t i = 0; i < cnt_; ++i) {
    if (cells()[i].need_deep_copy()) {
      chunk_row_store::pointer2off(cells()[i].v_.string_, base);
    }
  }
  LOG_DEBUG("trace unswizzling", K(this));
}

void ObChunkRowStore::StoredRow::swizzling()
{
  for (int64_t i = 0; i < cnt_; ++i) {
    if (cells()[i].need_deep_copy()) {
      chunk_row_store::off2pointer(cells()[i].v_.string_, this);
    }
  }
}

int ObChunkRowStore::Block::add_row(const ObNewRow &row, const int64_t row_size,
    uint32_t row_extend_size, StoredRow **stored_row)
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
    StoredRow *sr = new (buf->head())StoredRow;
    if (OB_FAIL(sr->copy_row(
        row, buf->head() + ROW_HEAD_SIZE, row_size - ROW_HEAD_SIZE, row_size, row_extend_size))) {
      LOG_WARN("copy row failed", K(ret), K(row), K(row_size));
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
    //LOG_DEBUG("RowStore blk add_row", K_(sr->cnt), K_(rows), K(row_size), K(buf->data_size()));
  }

  return ret;
}


int ObChunkRowStore::Block::copy_row(const StoredRow *stored_row)
{
  int ret = OB_SUCCESS;
  BlockBuffer *buf = get_buffer();
  int64_t row_size =  stored_row->row_size_;
  if (!buf->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(row_size));
  } else {
    StoredRow *sr = new (buf->head())StoredRow;
    sr->assign(stored_row);
    if (OB_FAIL(buf->advance(row_size))) {
      LOG_WARN("fill buffer head failed", K(ret), K(buf), K(row_size));
    } else {
      rows_++;
    }
  }

  return ret;
}

int ObChunkRowStore::Block::get_store_row(int64_t &cur_pos, const StoredRow *&sr)
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

int ObChunkRowStore::Block::unswizzling()
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

int ObChunkRowStore::Block::swizzling(int64_t *col_cnt)
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

ObChunkRowStore::ObChunkRowStore(common::ObIAllocator *alloc /* = NULL */)
  : inited_(false), tenant_id_(0), label_(nullptr), ctx_id_(0), mem_limit_(0),
    cur_blk_(NULL), max_blk_size_(0), min_blk_size_(INT64_MAX), default_block_size_(BLOCK_SIZE),
    n_blocks_(0), col_count_(0),
    row_cnt_(0), enable_dump_(true), has_dumped_(false), dumped_row_cnt_(0),
    file_size_(0), n_block_in_file_(0),
    mem_hold_(0), mem_used_(0), max_hold_mem_(0),
    allocator_(NULL == alloc ? &inner_allocator_ : alloc),
    row_store_mode_(STORE_MODE::WITHOUT_PROJECTOR), projector_(NULL), projector_size_(0),
    row_extend_size_(0), alloc_size_(0), free_size_(0), callback_(nullptr)
{
  io_.fd_ = -1;
  io_.dir_id_ = -1;
}

int ObChunkRowStore::init(int64_t mem_limit,
    uint64_t tenant_id /* = common::OB_SERVER_TENANT_ID */,
    int64_t mem_ctx_id /* = common::ObCtxIds::DEFAULT_CTX_ID */,
    const char *label /* = common::ObNewModIds::OB_SQL_CHUNK_ROW_STORE) */,
    bool enable_dump /* = true */,
    STORE_MODE row_store_mode /* WITHOUT_PROJECTOR */,
    uint32_t row_extend_size /* = 0 */
)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
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
    row_store_mode_ = row_store_mode;
    row_extend_size_ = row_extend_size;
  }
  return ret;
}

void ObChunkRowStore::reset()
{
  int ret = OB_SUCCESS;
  tenant_id_ = common::OB_SERVER_TENANT_ID;
  label_ = common::ObModIds::OB_SQL_ROW_STORE;
  ctx_id_ = common::ObCtxIds::DEFAULT_CTX_ID;

  mem_limit_ = 0;
  row_cnt_ = 0;

  if (is_file_open()) {
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.remove(io_.fd_))) {
      LOG_WARN("remove file failed", K(ret), K_(io_.fd));
    } else {
      LOG_TRACE("close file success", K(ret), K_(io_.fd));
    }
    io_.fd_ = -1;
  }
  n_block_in_file_ = 0;

  while (!blocks_.is_empty()) {
    Block *item = blocks_.remove_first();
    mem_hold_ -= item->get_buffer()->mem_size();
    mem_used_ -= item->get_buffer()->mem_size();
    callback_free(item->get_buffer()->mem_size());
    if (NULL != item) {
      allocator_->free(item);
    }
  }
  blocks_.reset();
  cur_blk_ = NULL;
  while (!free_list_.is_empty()) {
    Block *item = free_list_.remove_first();
    mem_hold_ -= item->get_buffer()->mem_size();
    callback_free(item->get_buffer()->mem_size());
    if (NULL != item) {
      allocator_->free(item);
    }
  }
  free_list_.reset();

  LOG_DEBUG("mem usage after free", K(mem_hold_), K(mem_used_));
  max_blk_size_ = 0;
  n_blocks_ = 0;
  row_cnt_ = 0;

  if(OB_SUCC(ret)) {
    if (nullptr != callback_) {
      callback_->dumped(-file_size_);
    }
  }
  file_size_ = 0;

  if (NULL != projector_) {
    allocator_->free(projector_);
  }
  projector_ = NULL;
  projector_size_ = 0;
  if (alloc_size_ != free_size_) {
    LOG_ERROR("It may has memory no free", K(alloc_size_), K(free_size_), K(lbt()));
  }
  alloc_size_ = 0;
  free_size_ = 0;
  callback_ = nullptr;

  inited_ = false;
}


void ObChunkRowStore::reuse()
{
  int ret = OB_SUCCESS;
  if (is_file_open()) {
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.remove(io_.fd_))) {
      LOG_WARN("remove file failed", K(ret), K_(io_.fd));
    } else {
      LOG_TRACE("close file success", K(ret), K_(io_.fd));
    }
    io_.fd_ = -1;
  }
  file_size_ = 0;
  n_block_in_file_ = 0;

  while (!blocks_.is_empty()) {
    Block *item = blocks_.remove_first();
    mem_hold_ -= item->get_buffer()->mem_size();
    mem_used_ -= item->get_buffer()->mem_size();
    callback_free(item->get_buffer()->mem_size());
    if (NULL != item) {
      allocator_->free(item);
    }
  }
  blocks_.reset();
  cur_blk_ = NULL;
  while (!free_list_.is_empty()) {
    Block *item = free_list_.remove_first();
    mem_hold_ -= item->get_buffer()->mem_size();
    callback_free(item->get_buffer()->mem_size());
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

  if (NULL != projector_) {
    allocator_->free(projector_);
  }
  projector_ = NULL;
  projector_size_ = 0;
}

void *ObChunkRowStore::alloc_blk_mem(const int64_t size, const bool for_iterator)
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
      LOG_WARN("alloc memory failed", K(ret), K(size), KP(mem), K(label_), K(ctx_id_), K(mem_limit_), K(enable_dump_));
    } else {
      blk = static_cast<char *>(mem);
      if (!for_iterator) {
        mem_hold_ += size;
        max_hold_mem_ = max(max_hold_mem_, mem_hold_);
      }
      callback_alloc(size);
    }
  }
  return blk;
}

void ObChunkRowStore::free_blk_mem(void *mem, const int64_t size /* = 0 */)
{
  if (NULL != mem) {
    LOG_DEBUG("free blk memory", K(size), KP(mem));
    allocator_->free(mem);
    mem_hold_ -= size;
    callback_free(size);
  }
}

void ObChunkRowStore::free_block(Block *item)
{
  if (NULL != item) {
    BlockBuffer* buffer = item->get_buffer();
    if (!buffer->is_empty()) {
      mem_used_ -= buffer->mem_size();
    }
    mem_hold_ -= buffer->mem_size();
    callback_free(buffer->mem_size());
    allocator_->free(item);
  }
}

void ObChunkRowStore::free_blk_list()
{
  Block* item = blocks_.remove_first();
  while (item != NULL) {
    free_block(item);
    item = blocks_.remove_first();
  }
  blocks_.reset();
}

//free mem of specified size
bool ObChunkRowStore::shrink_block(int64_t size)
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
  }
  LOG_DEBUG("RowStore shrink_block", K(ret), K(freed_size), K(size));
  if (freed_size >= size) {
    succ = true;
  }
  return succ;
}

int ObChunkRowStore::init_block_buffer(void* mem, const int64_t size, Block *&block)
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

int ObChunkRowStore::alloc_block_buffer(Block *&block, const int64_t data_size,
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
      LOG_WARN("alloc memory failed", K(ret), K(size));
    } else if (OB_FAIL(init_block_buffer(mem, size, block))){
      free_blk_mem(mem, size);
    } else if (!for_iterator) {
      if (size > max_blk_size_) {
        max_blk_size_ = size;
      } else if (size < min_blk_size_) {
        min_blk_size_ = size;
      }
    }
    LOG_DEBUG("RowStore alloc block", K(ret), K_(max_blk_size),
      K(data_size), K(min_size), K(for_iterator));
  }
  return ret;
}

int ObChunkRowStore::alloc_block_buffer(Block *&block, const int64_t data_size,
    const bool for_iterator)
{
  int64_t min_size = std::max(static_cast<int64_t>(BLOCK_SIZE), default_block_size_);
  return alloc_block_buffer(block, data_size, min_size, for_iterator);
}

inline int ObChunkRowStore::dump_one_block(BlockBuffer *item)
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

// only clean memory data
int ObChunkRowStore::clean_memory_data(bool reuse)
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
      callback_free(buf->mem_size());
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
  }
  return ret;
}

// 添加一种模式，dump时候只dump已经写满的block,剩下最后一个block
int ObChunkRowStore::dump(bool reuse, bool all_dump)
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
        if (!buf->is_empty() && OB_FAIL(dump_one_block(buf))) {
          LOG_WARN("failed to dump block", K(ret));
        }
        // 不论成功与否，都需要释放内存
        if (!buf->is_empty()) {
          mem_used_ -= buf->mem_size();
          dumped_row_cnt_ += cur->rows();
        }
        if (reuse && buf->mem_size() <= default_block_size_) {
          buf->reuse();
          free_list_.add_last(cur);
        } else {
          mem_hold_ -= buf->mem_size();
          callback_free(buf->mem_size());
          allocator_->free(cur);
        }
      }
    }
    if (all_dump && (mem_used_ != 0 || (!reuse && mem_hold_ != 0) || blocks_.get_size() != 0)) {
      LOG_WARN("hold mem after dump", K_(mem_used), K(reuse), K_(mem_hold),
          K(blocks_.get_size()), K(free_list_.get_size()), K(n_block), K(org_n_block), K(ret));
    }
    if (OB_SUCC(ret)) {
      if (all_dump) {
        cur_blk_ = NULL;
      }
      if (!has_dumped_) {
        has_dumped_ = (org_n_block != blocks_.get_size());
      }
    }
  }
  return ret;
}

bool ObChunkRowStore::find_block_can_hold(const int64_t size, bool &need_shrink)
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

int ObChunkRowStore::switch_block(const int64_t min_size)
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

int ObChunkRowStore::add_row(const common::ObNewRow &row, StoredRow **stored_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int64_t bak_projector_size_ = 0;
    int32_t *bak_projector_ = NULL;
    ObNewRow &r = const_cast<ObNewRow &>(row);
    //store project
    if (STORE_MODE::FULL == row_store_mode_ && NULL != row.projector_ && row.projector_size_ > 0) {
      LOG_DEBUG("has projector", K(row.projector_size_), K(row.count_));
      if (OB_ISNULL(projector_)) {
        ObMemAttr attr(tenant_id_, label_, ctx_id_);
        projector_size_ = row.projector_size_;
        const int64_t size = projector_size_ * sizeof(*projector_);
        projector_ = static_cast<int32_t *>(allocator_->alloc(size));
        if (OB_ISNULL(projector_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          MEMCPY(projector_, row.projector_, size);
        }
      } else if (projector_size_ != row.projector_size_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("projector size mismatch A", K(ret), K(projector_size_), K(row.projector_size_));
      }
      if (OB_SUCC(ret)) {
        bak_projector_ = r.projector_;
        bak_projector_size_ = r.projector_size_;
        r.projector_size_ = 0;
        r.projector_ = NULL;
      }
    }
    const int64_t row_size = Block::row_store_size(row, row_extend_size_);
    const int64_t min_buf_size = Block::min_buf_size(row_size);
    if (OB_SUCC(ret)) {
      if (NULL == cur_blk_) {
        Block *new_blk = nullptr;
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
          LOG_WARN("switch block failed", K(ret), K(row));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(cur_blk_->add_row(row, row_size, row_extend_size_, stored_row))) {
          LOG_WARN("add row to block failed", K(ret), K(row), K(row_size));
        } else {
          row_cnt_++;
          if (0 >= col_count_) {
            col_count_ = row.get_count();
          }
        }
      }
    }
    // always keep row unchanged.
    if (NULL != bak_projector_) {
      r.projector_size_ = bak_projector_size_;
      r.projector_ = bak_projector_;
    }
   // if (stored_row != NULL) {
   //   stored_type_ = (*stored_row)->TYPE;
  //  }
  }

  return ret;
}

int ObChunkRowStore::copy_row(const StoredRow *stored_row, ObChunkRowStore* crs)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t row_size = stored_row->row_size_;
    const int64_t min_buf_size = Block::min_buf_size(row_size);
    if (OB_NOT_NULL(crs) && OB_NOT_NULL(crs->projector_) && nullptr == projector_) {
      projector_size_ = crs->projector_size_;
      const int64_t size = projector_size_ * sizeof(*projector_);
      projector_ = static_cast<int32_t *>(allocator_->alloc(size));
      if (OB_ISNULL(projector_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        MEMCPY(projector_, crs->projector_, size);
      }
    } else if (OB_NOT_NULL(crs) && projector_size_ != crs->projector_size_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("projector size mismatch", K(ret), K(projector_size_), K(crs->projector_size_));
    }
    if (OB_SUCC(ret)) {
      if (NULL == cur_blk_) {
        Block *new_blk = nullptr;
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
          LOG_WARN("switch block failed", K(ret), K(*stored_row));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(cur_blk_->copy_row(stored_row))) {
          LOG_WARN("add row to block failed", K(ret), K(*stored_row), K(row_size));
        } else {
          row_cnt_++;
          if (col_count_ <= 0) {
            col_count_ = stored_row->cnt_;
          }
        }
      }
    }
  }

  return ret;
}

int ObChunkRowStore::finish_add_row(bool need_dump)
{
  int ret = OB_SUCCESS;
  int64_t timeout_ms = 0;
  if (free_list_.get_size() > 0) {
    Block *block = free_list_.remove_first();
    while (NULL != block) {
      mem_hold_ -= block->get_buffer()->mem_size();
      callback_free(block->get_buffer()->mem_size());
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
    } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.sync(io_.fd_, timeout_ms))) {
      LOG_WARN("sync file failed", K(ret), K_(io_.fd), K(timeout_ms));
    }
  } else {
    LOG_DEBUG("finish_add_row no need to dump", K(ret));
  }

  return ret;
}

//add block manually, this block must be initialized by init_block_buffer()
//and must be removed by remove_added_block before ObChunkRowStore<>::reset()
int ObChunkRowStore::add_block(Block* block, bool need_swizzling, bool *added)
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
    } else if (this->col_count_ != 0 && this->col_count_ != col_cnt) {
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
    }
  }
  return ret;
}

int ObChunkRowStore::append_block(char *buf, int size,  bool need_swizzling)
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
      LOG_TRACE("trace append block", K(src_block->rows_), K(size), K(mem_used_), K(mem_hold_));
    }
    if (OB_FAIL(ret) && !added) {
      free_blk_mem(new_block, block_buffer->mem_size());
    }
  }
  // dump data if mem used > 16MB
  const int64_t dump_threshold = 1 << 24;
  if (OB_SUCC(ret) && mem_used_ > dump_threshold) {
    if (OB_FAIL(dump(false /* reuse */, true /* all_dump */))) {
      LOG_WARN("dump failed", K(ret));
    }
  }
  return ret;
}

void ObChunkRowStore::remove_added_blocks()
{
  blocks_.reset();
  n_blocks_ = 0;
  row_cnt_ = 0;
}

int ObChunkRowStore::load_next_chunk_blocks(ChunkIterator &it)
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
    LOG_DEBUG("load chunk", KP(last_blk_end), K_(it.cur_iter_blk_->blk_size),
        KP(chunk_end), K(chunk_end - last_blk_end));
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
      tmp_read_size = read_size;
      read_size = it.file_size_ - it.cur_iter_pos_;
      chunk_size = read_size + read_off;
    }
    int64_t tmp_file_size = -1;
    if (0 == read_size) {
      // ret end
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = read_file(
          it.chunk_mem_ + read_off, tmp_read_size, it.cur_iter_pos_,
          it.file_size_, it.cur_iter_pos_, tmp_file_size))) {
        LOG_WARN("read blk info from file failed", K(tmp_ret), K_(it.cur_iter_pos));
      }
      if (OB_ITER_END != tmp_ret) {
        if (OB_UNLIKELY(OB_SUCCESS == tmp_ret)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected read succ", K(read_size), K(tmp_read_size), K(it), K(read_off));
        } else {
          ret = tmp_ret;
          LOG_WARN("unexpected status", K(ret), K(tmp_ret));
        }
      } else {
        ret = OB_ITER_END;
      }
    } else if (OB_FAIL(read_file(it.chunk_mem_ + read_off, read_size, it.cur_iter_pos_,
                                 it.file_size_, it.cur_iter_pos_, tmp_file_size))) {
      LOG_WARN("read blk info from file failed", K(ret), K_(it.cur_iter_pos));
    } else {
      int64_t cur_pos = 0;
      Block* block = reinterpret_cast<Block *>(it.chunk_mem_);
      Block* prev_block = block;
      do {
        if (!block->magic_check()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("StoreRow load block magic check failed", K(ret), K(it), K(cur_pos), K(chunk_size));
        } else if (block->blk_size_ <= chunk_size - cur_pos) {
          prev_block->next_ = block;
          cur_pos += block->blk_size_;
          prev_block = block;
          block = reinterpret_cast<Block*>(it.chunk_mem_ + cur_pos);
          read_n_blocks++;
          if (prev_block->blk_size_ == 0 || prev_block->rows_ == 0) {
            ret = OB_INNER_STAT_ERROR;
            LOG_WARN("read file failed", K(ret), K(prev_block->blk_size_), K(read_n_blocks), K(cur_pos));
          } else if (OB_FAIL(prev_block->swizzling(NULL))){
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
        it.cur_iter_blk_ = reinterpret_cast<Block *>(it.chunk_mem_);
        it.cur_iter_pos_ += read_size;
        it.cur_chunk_n_blocks_ = read_n_blocks;
        it.cur_nth_blk_ += read_n_blocks;
        if (0 == read_n_blocks) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to chunk read blocks", K(read_n_blocks), K(it), K(read_off),
            K(it.cur_iter_pos_), K(read_size), K(chunk_size), K(cur_pos), K(ret));
        } else {
          LOG_TRACE("chunk read blocks succ:", K(read_n_blocks), K(it), K(read_off),
            K(it.cur_iter_pos_), K(read_size), K(chunk_size), K(cur_pos),
            K(it.cur_nth_blk_));
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

/* get next block from BlockItemList(when all rows in mem) or read from file
 * and let it.cur_iter_blk_ point to the new block
 */
int ObChunkRowStore::load_next_block(ChunkIterator &it)
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
    if (it.chunk_read_size_ > 0 && it.chunk_read_size_ >= this->max_blk_size_) {
      if (OB_FAIL(load_next_chunk_blocks(it))) {
        LOG_WARN("RowStore iter load next chunk blocks failed", K(ret));
      }
    } else {
      /* read from file:
      * read a block of min_size first, if the actual size is lager, then alloc a new block and copy
      * the previous block to the head of it, then read the rest from file
      * */
      int64_t block_size = INT64_MAX == min_blk_size_ ? it.chunk_read_size_ :
                      (it.chunk_read_size_ > min_blk_size_ ? it.chunk_read_size_ : min_blk_size_);
      int64_t read_size = block_size - sizeof(BlockBuffer);
      if (it.chunk_mem_ != NULL) {
        // 一般情况下，写入磁盘的数据都会大于一个block size，但当测试时，如强制写入N行就写入磁盘
        // 会导致读数据时，由于整个数据比较小，小于blk size，这个时候会申请内存和写入数据的大小一样大（节省内存）
        // 这种情况下才会走到这里，主要用来测试代码
        allocator_->free(it.chunk_mem_);
        it.chunk_mem_ = NULL;
        it.cur_iter_blk_ = NULL;
        callback_free(it.chunk_read_size_);
      }
      if (NULL == it.cur_iter_blk_) {
        //for the first time, it.cur_iter_blk_ need to be freed by Iterator
        if (OB_FAIL(alloc_block_buffer(it.cur_iter_blk_, block_size, true))) {
          LOG_WARN("alloc block failed", K(ret));
        } else {
          it.cur_iter_blk_buf_ = it.cur_iter_blk_->get_buffer();
          LOG_DEBUG("alloc a block for reading", K(ret), K(it.cur_iter_blk_),
              K(it.cur_iter_blk_->get_buffer()->capacity()), K(block_size));
        }
      } else {
        if (it.cur_iter_blk_buf_ == NULL) {
          ret = OB_INNER_STAT_ERROR;
          LOG_WARN("it.cur_iter_blk_buf_ is NULL", K(ret), K_(it.cur_iter_blk_buf));
        } else {
          it.cur_iter_blk_buf_->reuse();
          LOG_DEBUG("StoreRow reuse block", K(it.cur_iter_blk_buf_->capacity()),
              K(it.cur_iter_blk_->get_buffer()), K_(it.cur_iter_blk_buf), K(block_size), K(read_size));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t blk_cap = it.cur_iter_blk_buf_->capacity();
        int64_t tmp_file_size = -1;
        LOG_DEBUG("RowStore need read file", K(it.cur_iter_blk_buf_->capacity()),
            K(it.cur_iter_blk_->blk_size_), K(block_size), K_(file_size), K_(it.cur_iter_pos),
            K_(it.cur_iter_blk));
        //read a normal size of Block first
        if (OB_FAIL(read_file(it.cur_iter_blk_, read_size, it.cur_iter_pos_,
                              it.file_size_, it.cur_iter_pos_, tmp_file_size))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("read blk info from file failed", K(ret), K_(it.cur_iter_pos));
          }
        } else if (!it.cur_iter_blk_->magic_check()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("StoreRow load block magic check failed", K(ret), K(it.cur_iter_blk_));
        } else if (it.cur_iter_blk_->blk_size_ > read_size) {
          LOG_DEBUG("RowStore blk size larger than min", K(read_size),
              K(it.cur_iter_blk_->blk_size_));
          Block* blk_head = it.cur_iter_blk_;
          int64_t ac_size = it.cur_iter_blk_->blk_size_;
          int64_t pre_size = it.cur_iter_blk_buf_->mem_size();
          LOG_DEBUG("RowStore need read file", K(ac_size), K(blk_head), K(block_size));
          if (read_size + it.cur_iter_pos_ > file_size_) {
            ret = OB_ITER_END;
            LOG_WARN("RowStore iter end unexpected", K(ret), K(it.cur_iter_pos_),
                K(file_size_), K(read_size));
          } else if (blk_cap < ac_size) {
            //need to alloc new blk to hold data
            it.cur_iter_blk_ = nullptr;
            if (OB_FAIL(alloc_block_buffer(it.cur_iter_blk_, ac_size + sizeof(BlockBuffer), true))
                || ac_size != it.cur_iter_blk_->get_buffer()->capacity()) {
              LOG_WARN("alloc block failed", K(ret), K(ac_size), K(it.cur_iter_blk_));
              allocator_->free(blk_head);
              callback_free(pre_size);
            } else {
              it.cur_iter_blk_buf_ = it.cur_iter_blk_->get_buffer();
              MEMCPY(it.cur_iter_blk_, blk_head, read_size);
              allocator_->free(blk_head);
              callback_free(pre_size);
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(read_file(static_cast<void*>(it.cur_iter_blk_->payload_ + read_size - sizeof(Block)),
                                ac_size - read_size,
                                it.cur_iter_pos_ + read_size,
                                it.file_size_,
                                it.cur_iter_pos_,
                                tmp_file_size))) {
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
        } else if (OB_FAIL(it.cur_iter_blk_->swizzling(NULL))){
          LOG_WARN("swizzling failed after read block from file", K(ret), K(it));
        } else if (read_size <= 0
                  || it.cur_iter_blk_->blk_size_ == 0
                  || it.cur_iter_blk_->rows_ == 0) {
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
        LOG_TRACE("StoreRow read file succ", K(read_size), K_(it.cur_iter_blk),
          K_(*it.cur_iter_blk), K_(it.cur_iter_pos));
      } else {
        // first read disk data then read memory data, so it must free cur_iter_blk_
        if (NULL != it.cur_iter_blk_) {
          callback_free(it.cur_iter_blk_->get_buffer()->mem_size());
          allocator_->free(it.cur_iter_blk_);
          it.cur_iter_blk_ = NULL;
          it.cur_iter_blk_buf_ = nullptr;
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
      LOG_TRACE("RowStore no more blocks", K(ret), K_(it.cur_nth_blk), K_(n_blocks),
        K(n_block_in_file_), K(io_.fd_), K(it.iter_end_flag_));
    } else if (nullptr == blocks_.get_first()) {
      ret = OB_ITER_END;
    } else {
      //all in mem or read blocks in mem at first
      it.cur_iter_blk_ = blocks_.get_first();
      LOG_DEBUG("RowStore got block in mem", K_(it.cur_iter_blk), K_(*it.cur_iter_blk));
      it.cur_chunk_n_blocks_ = blocks_.get_size();
      it.cur_nth_blk_ += it.cur_chunk_n_blocks_;
      it.chunk_n_rows_ = this->get_row_cnt_in_memory();
      LOG_TRACE("trace read in memoery data", K(ret));
      if (it.cur_nth_blk_ != n_blocks_ - 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: last chunk data", K(it.cur_nth_blk_), K(n_blocks_),
          K(blocks_.get_size()), K(ret));
      }
    }
  }
  return ret;
}

int ObChunkRowStore::get_store_row(RowIterator &it, const StoredRow *&sr)
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
            K_(it.cur_nth_block), K_(it.n_blocks), K(lbt()));
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
ObChunkRowStore::RowIterator::RowIterator()
  : store_(NULL),
    cur_iter_blk_(NULL),
    cur_row_in_blk_(0),
    cur_pos_in_blk_(0),
    n_blocks_(0),
    cur_nth_block_(0)
{

}

ObChunkRowStore::RowIterator::RowIterator(ObChunkRowStore *row_store)
  : store_(row_store),
    cur_iter_blk_(NULL),
    cur_row_in_blk_(0),
    cur_pos_in_blk_(0),
    n_blocks_(0),
    cur_nth_block_(0)
{
}

int ObChunkRowStore::RowIterator::init(ChunkIterator *chunk_it)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(chunk_it) || !chunk_it->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(chunk_it));
  } else {
    store_ = chunk_it->store_;
    row_.projector_ = NULL;
    row_.projector_size_ = 0;
  }
  return ret;
}

ObChunkRowStore::ChunkIterator::ChunkIterator()
  : store_(NULL),
    cur_iter_blk_(NULL),
    cur_iter_blk_buf_(NULL),
    cur_nth_blk_(-1),
    cur_chunk_n_blocks_(0),
    cur_iter_pos_(0),
    file_size_(0),
    chunk_read_size_(0),
    chunk_mem_(NULL),
    chunk_n_rows_(0),
    iter_end_flag_(IterEndState::PROCESSING)
{

}

int ObChunkRowStore::ChunkIterator::init(ObChunkRowStore *store, int64_t chunk_read_size)
{
  int ret = OB_SUCCESS;
  store_ = store;
  chunk_read_size_ = chunk_read_size;
  CK (chunk_read_size >= 0);
  return ret;
}

ObChunkRowStore::ChunkIterator::~ChunkIterator()
{
  reset_cursor(0);
}

void ObChunkRowStore::ChunkIterator::reset_cursor(const int64_t file_size)
{
  file_size_ = file_size;

  if (chunk_mem_ != NULL) {
    store_->allocator_->free(chunk_mem_);
    chunk_mem_ = NULL;
    cur_iter_blk_ = NULL;
    store_->callback_free(chunk_read_size_);
    if (read_file_iter_end()) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpect status: chunk mem is allocated, but don't free");
    }
  }
  if (!read_file_iter_end() && cur_iter_blk_ != NULL) {
    if (cur_iter_pos_ > 0 && NULL != store_) {
      if (nullptr != cur_iter_blk_buf_) {
        store_->callback_free(cur_iter_blk_buf_->mem_size());
      }
      store_->allocator_->free(cur_iter_blk_);
    }
  }
  cur_iter_blk_buf_ = nullptr;
  cur_iter_blk_ = nullptr;
  cur_nth_blk_ = -1;
  cur_iter_pos_ = 0;
  iter_end_flag_ = IterEndState::PROCESSING;
}

void ObChunkRowStore::ChunkIterator::reset()
{
  reset_cursor(0);
  chunk_read_size_ = 0;
  chunk_n_rows_ = 0;
}

int ObChunkRowStore::ChunkIterator::load_next_chunk(RowIterator& it)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("ChunkIterator not init", K(ret));
  } else if (!has_next_chunk()){
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

int ObChunkRowStore::Iterator::init(ObChunkRowStore *store, int64_t chunk_read_size)
{
  reset();
  switch (store->row_store_mode_) {
  case STORE_MODE::WITHOUT_PROJECTOR:
    (Iterator::convert_row_with_obj_fun_) = &Iterator::convert_to_row_with_obj;
    Iterator::convert_row_fun_ = &Iterator::convert_to_row;
    break;
  case STORE_MODE::FULL:
    Iterator::convert_row_with_obj_fun_ = &Iterator::convert_to_row_full_with_obj;
    Iterator::convert_row_fun_ = &Iterator::convert_to_row_full;
    break;
  }
  int ret = OB_SUCCESS;
  if (OB_FAIL(chunk_it_.init(store, chunk_read_size))
      || OB_FAIL(row_it_.init(&chunk_it_))) {
    LOG_WARN("chunk iterator or row iterator init failed", K(ret));
  }
  return ret;
}

int ObChunkRowStore::Iterator::get_next_row(ObNewRow &row)
{
  int ret = OB_SUCCESS;
  const StoredRow *sr = NULL;
  if (OB_FAIL(get_next_row(sr))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next stored row failed", K(ret));
    }
  } else if (OB_FAIL((this->*convert_row_with_obj_fun_)(row_it_, row, sr))) {
    LOG_WARN("convert row failed", K(ret), K_(chunk_it), K_(row_it));
  }
  return ret;
}

int ObChunkRowStore::Iterator::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  const StoredRow *sr = NULL;
  if (OB_FAIL(get_next_row(sr))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next stored row failed", K(ret));
    }
  } else if (OB_FAIL((this->*convert_row_fun_)(row_it_, row, sr))) {
    LOG_WARN("convert row failed", K(ret), K_(chunk_it), K_(row_it));
  }
  return ret;
}

int ObChunkRowStore::Iterator::get_next_row(const StoredRow *&sr)
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

int ObChunkRowStore::RowIterator::get_next_row(const StoredRow *&sr)
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

/* from StoredRow to NewRow */
int ObChunkRowStore::RowIterator::convert_to_row(common::ObNewRow &row, const StoredRow *sr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sr)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = store_row2new_row(row, *sr);
  }
  return ret;
}


int ObChunkRowStore::RowIterator::store_row2new_row(common::ObNewRow &row, const StoredRow &sr)
{
  int ret = OB_SUCCESS;
  if (row.count_ < sr.cnt_){
    ret = OB_BUF_NOT_ENOUGH;
    OB_LOG(WARN, "column buffer count is not enough", K_(row.count), K_(sr.cnt));
  } else {
    //do not overwite row.count_
    //row.count_ = sr.cnt_;
    MEMCPY(row.cells_, sr.cells(), sizeof(ObObj) * sr.cnt_);
  }
  return ret;
}

/* from StoredRow to NewRow */
int ObChunkRowStore::RowIterator::convert_to_row_full(common::ObNewRow &row, const StoredRow *sr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL store row returned", K(ret));
  } else if (row.count_ < sr->cnt_){
    ret = OB_BUF_NOT_ENOUGH;
    OB_LOG(WARN, "column buffer count is not enough", K_(row.count), K(store_->get_col_cnt()));
  } else {
    //do not overwite row.count_
    //row.count_ = sr->cnt_;
    MEMCPY(row.cells_, sr->cells(), sizeof(ObObj) * sr->cnt_);
    MEMCPY(row.projector_, store_->projector_, sizeof(*store_->projector_) * sr->cnt_);
    row.projector_size_ = store_->projector_size_;
  }
  return ret;
}

/* from StoredRow to NewRow */
int ObChunkRowStore::RowIterator::convert_to_row(common::ObNewRow *&row, const StoredRow *sr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL store row returned", K(ret));
  } else {
    //do not overwite row.count_
    //row.count_ = sr->cnt_;
    row_.projector_ = NULL;
    row_.projector_size_ = 0;
    row_.count_ = sr->cnt_;
    row_.cells_ = const_cast<ObObj *>(sr->cells());
    row = &row_;
  }
  return ret;
}

/* from StoredRow to NewRow */
int ObChunkRowStore::RowIterator::convert_to_row_full(common::ObNewRow *&row, const StoredRow *sr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL store row returned", K(ret));
  } else {
    row_.projector_ = store_->projector_;
    row_.projector_size_ = store_->projector_size_;
    row_.count_ = sr->cnt_;
    row_.cells_ = const_cast<ObObj *>(sr->cells());
    row = &row_;
  }
  return ret;
}

int ObChunkRowStore::get_timeout(int64_t &timeout_ms)
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

int ObChunkRowStore::alloc_dir_id()
{
  int ret = OB_SUCCESS;
  if (-1 == io_.dir_id_ && OB_FAIL(ObChunkStoreUtil::alloc_dir_id(io_.dir_id_))) {
    LOG_WARN("allocate file directory failed", K(ret));
  }
  return ret;
}

int ObChunkRowStore::write_file(void *buf, int64_t size)
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
        io_.io_timeout_ms_ = timeout_ms;
        LOG_TRACE("open file success", K_(io_.fd), K_(io_.dir_id));
      }
    }
    ret = OB_E(EventTable::EN_8) ret;
  }
  if (OB_SUCC(ret) && size > 0) {
    set_io(size, static_cast<char *>(buf));
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.write(io_))) {
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

int ObChunkRowStore::read_file(void *buf, const int64_t size, const int64_t offset,
                               const int64_t file_size, const int64_t cur_pos,
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
  }
  int64_t read_size = file_size - cur_pos;
  if (OB_FAIL(ret)) {
  } else if (0 >= size) {
    CK (cur_pos >= file_size);
    OX (ret = OB_ITER_END);
  } else {
    this->set_io(size, static_cast<char *>(buf));
    io_.io_desc_.set_wait_event(ObWaitEventIds::ROW_STORE_DISK_READ);
    io_.io_timeout_ms_ = timeout_ms;
    blocksstable::ObTmpFileIOHandle handle;
    if (0 == read_size
        && OB_FAIL(FILE_MANAGER_INSTANCE_V2.get_tmp_file_size(io_.fd_, tmp_file_size))) {
      LOG_WARN("failed to get tmp file size", K(ret));
    } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.pread(io_, offset, handle))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("read form file failed", K(ret), K(io_), K(offset), K(timeout_ms));
      }
    } else if (handle.get_data_size() != size) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("read data less than expected",
          K(ret), K(io_), "read_size", handle.get_data_size());
    }
  }
  return ret;
}

bool ObChunkRowStore::need_dump(int64_t extra_size)
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

int ObChunkStoreUtil::alloc_dir_id(int64_t &dir_id)
{
  int ret = OB_SUCCESS;
  dir_id = 0;
  if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.alloc_dir(dir_id))) {
    LOG_WARN("allocate file directory failed", K(ret));
  }
  return ret;
}

int ObChunkRowStore::Block::gen_unswizzling_payload(char *unswizzling_payload, uint32 size)
{
  int ret = OB_SUCCESS;
  int64_t cur_pos = 0;
  uint32_t i = 0;
  if (OB_ISNULL(unswizzling_payload) || size < blk_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(size), K(blk_size_), KP(unswizzling_payload));
  } else {
    MEMCPY(unswizzling_payload, payload_, blk_size_);
  }
  while (OB_SUCC(ret) && cur_pos < blk_size_ && i < rows_) {
    StoredRow *sr = reinterpret_cast<StoredRow *>(unswizzling_payload + cur_pos);
    sr->unswizzling(payload_ + cur_pos);
    cur_pos += sr->row_size_;
    i++;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObChunkRowStore)
{
  int ret = OB_SUCCESS;
  if (enable_dump_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("chunk row store not support serialize if enable dump", K(ret));
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              ctx_id_,
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
      OB_UNIS_ENCODE(block->blk_size_);
      OB_UNIS_ENCODE(block->rows_);
      if (OB_SUCC(ret)) {
        if (buf_len - pos < block->blk_size_) {
          ret = OB_SIZE_OVERFLOW;
        } else if (OB_FAIL(block->gen_unswizzling_payload(buf + pos, block->blk_size_))) {
          LOG_WARN("fail to unswizzling", K(ret));
        } else {
          pos += block->blk_size_;
          block = block->get_next();
        }
      }
    } 
  }

  return ret;
}


OB_DEF_DESERIALIZE(ObChunkRowStore)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              ctx_id_,
              mem_limit_);
  if (!is_inited()) {
    if (OB_FAIL(init(mem_limit_, tenant_id_,
                     ctx_id_, "ChunkRowDE", false/*enable_dump*/))) {
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
    int64_t blk_size = 0;
    int64_t row_cnt = 0;
    int64_t blk_cnt = 0;
    OB_UNIS_DECODE(blk_cnt);
    for (int64_t i = 0; i < blk_cnt && OB_SUCC(ret); ++i) {
      OB_UNIS_DECODE(blk_size);
      OB_UNIS_DECODE(row_cnt);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(alloc_block_buffer(block, blk_size, false /*for_iterator*/))) {
        LOG_WARN("alloc block failed", K(ret), K(blk_size), KP(block));
      } else {
        MEMCPY(block->payload_, buf + pos, blk_size);
        block->blk_size_ = blk_size;
        block->rows_ = row_cnt;
        pos += blk_size;
        if (OB_FAIL(add_block(block, true /*+need_swizzling*/))) {
          LOG_WARN("fail to add block", K(ret));
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObChunkRowStore)
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
  const Block *block = blocks_.get_first();
  while (NULL != block) {
    OB_UNIS_ADD_LEN(block->blk_size_);
    OB_UNIS_ADD_LEN(block->rows_);
    len += block->blk_size_;
    block = block->get_next();
  } 

  return len;
}



} // end namespace sql
} // end namespace oceanbase
#endif // OCEANBASE_BASIC_OB_CHUNK_ROW_STORE_CPP_
