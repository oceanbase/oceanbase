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

#include "ob_temp_block_store.h"
#include "lib/container/ob_se_array_iterator.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/config/ob_server_config.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/ob_io_event_observer.h"


namespace oceanbase
{
using namespace common;

namespace sql
{

const int64_t ObTempBlockStore::IndexBlock::INDEX_BLOCK_SIZE;
const int64_t ObTempBlockStore::BIG_BLOCK_SIZE;

int ObTempBlockStore::ShrinkBuffer::init(char *buf, const int64_t buf_size)
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

ObTempBlockStore::ObTempBlockStore(common::ObIAllocator *alloc /* = NULL */)
  : inited_(false), allocator_(NULL == alloc ? &inner_allocator_ : alloc), blk_(NULL), blk_buf_(),
    block_id_cnt_(0), saved_block_id_cnt_(0), dumped_block_id_cnt_(0), enable_dump_(true),
    enable_trunc_(false), last_trunc_offset_(0),
    tenant_id_(0), label_(), ctx_id_(0), mem_limit_(0), mem_hold_(0), mem_used_(0),
    file_size_(0), block_cnt_(0), index_block_cnt_(0), block_cnt_on_disk_(0),
    alloced_mem_size_(0), max_block_size_(0), max_hold_mem_(0), idx_blk_(NULL), mem_stat_(NULL),
    io_observer_(NULL), last_block_on_disk_(false), cur_file_offset_(0)
{
  label_[0] = '\0';
  io_.dir_id_ = -1;
  io_.fd_ = -1;
}

int ObTempBlockStore::init(int64_t mem_limit,
                           bool enable_dump,
                           uint64_t tenant_id,
                           int64_t mem_ctx_id,
                           const char *label,
                           common::ObCompressorType compress_type,
                           const bool enable_trunc)
{
  int ret = OB_SUCCESS;
  mem_limit_ = mem_limit;
  enable_dump_ = enable_dump;
  tenant_id_ = tenant_id;
  ctx_id_ = mem_ctx_id;
  const int label_len = MIN(lib::AOBJECT_LABEL_SIZE, strlen(label));
  MEMCPY(label_, label, label_len);
  label_[label_len] = '\0';
  io_.fd_ = -1;
  inner_reader_.init(this);
  inited_ = true;
  compressor_.init(compress_type);
  enable_trunc_ = enable_trunc;
  return ret;
}

void ObTempBlockStore::reset()
{
  int ret = OB_SUCCESS;
  LOG_TRACE("reset temp block store", KP(this), K(*this));

  blk_ = NULL;
  blk_buf_.reset();
  // the last index block may not be linked to `blk_mem_list_` and needs to be released manually
  if (NULL != idx_blk_) {
    free_blk_mem(idx_blk_);
    idx_blk_ = NULL;
  }

  reset_block_cnt();
  inner_reader_.reset();

  if (is_file_open()) {
    write_io_handle_.reset();
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.remove(io_.fd_))) {
      LOG_WARN("remove file failed", K(ret), K_(io_.fd));
    } else {
      LOG_INFO("close file success", K(ret), K_(io_.fd), K_(file_size));
    }
    io_.fd_ = -1;
  }
  file_size_ = 0;

  free_mem_list(blk_mem_list_);
  free_mem_list(alloced_mem_list_);
  blocks_.reset();
  set_mem_hold(0);
  set_mem_used(0);
}

void ObTempBlockStore::reuse()
{
  int ret = OB_SUCCESS;
  reset_block_cnt();
  inner_reader_.reset();
  if (is_file_open()) {
    write_io_handle_.reset();
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.remove(io_.fd_))) {
      LOG_WARN("remove file failed", K(ret), K_(io_.fd));
    } else {
      LOG_INFO("close file success", K(ret), K_(io_.fd), K_(file_size));
    }
    io_.fd_ = -1;
  }
  file_size_ = 0;
  if (NULL != idx_blk_) {
    free_blk_mem(idx_blk_);
    idx_blk_ = NULL;
  }
  free_mem_list(alloced_mem_list_);
  DLIST_FOREACH_REMOVESAFE_NORET(node, blk_mem_list_) {
    if (&(*node) + 1 != static_cast<LinkNode *>(static_cast<void *>(blk_buf_.data()))) {
      node->unlink();
      node->~LinkNode();
      allocator_->free(node);
    }
  }
  set_mem_hold(0);
  set_mem_used(0);
  if (NULL != blk_) {
    if (OB_FAIL(setup_block(blk_buf_, blk_))) {
      LOG_WARN("setup block failed", K(ret));
    }
    block_cnt_ = 1;
    set_mem_hold(blk_buf_.capacity() + sizeof(LinkNode));
    max_block_size_ = blk_buf_.capacity();
    max_hold_mem_ = mem_hold_;
  }
  blocks_.reset();
}

void ObTempBlockStore::reset_block_cnt()
{
  block_cnt_ = 0;
  index_block_cnt_ = 0;
  block_cnt_on_disk_ = 0;
  block_id_cnt_ = 0;
  saved_block_id_cnt_ = 0;
  dumped_block_id_cnt_ = 0;
  alloced_mem_size_ = 0;
  max_block_size_ = 0;
  max_hold_mem_ = 0;
}

int ObTempBlockStore::alloc_dir_id()
{
  int ret = OB_SUCCESS;
  if (-1 == io_.dir_id_) {
    io_.dir_id_ = 0;
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.alloc_dir(io_.dir_id_))) {
      LOG_WARN("allocate file directory failed", K(ret));
    }
  }
  return ret;
}

int ObTempBlockStore::finish_add_row(bool need_dump /*true*/)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("start finish add row", KP(this), K(need_dump));
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(finish_write())) {
    LOG_WARN("fail to flush write buffer", K(ret));
  } else if (OB_ISNULL(blk_)) {
    // do nothing if store is empty or has called finish_add_row already
  } else if (is_file_open()) {
    if (need_dump && OB_FAIL(dump(true)) && OB_EXCEED_MEM_LIMIT != ret) {
      LOG_WARN("fail to dump all when finish add row", K(ret));
    } else {
      int64_t timeout_ms = 0;
      const uint64_t begin_io_dump_time = rdtsc();
      if (OB_FAIL(get_timeout(timeout_ms))) {
        LOG_WARN("get timeout failed", K(ret));
      } else if (write_io_handle_.is_valid() && OB_FAIL(write_io_handle_.wait())) {
        LOG_WARN("fail to wait write", K(ret), K(write_io_handle_));
      } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.sync(io_.fd_, timeout_ms))) {
        LOG_WARN("sync file failed", K(ret), K(io_.fd_), K(timeout_ms));
      }
      if (OB_LIKELY(nullptr != io_observer_)) {
        io_observer_->on_write_io(rdtsc() - begin_io_dump_time);
      }
    }
  }
  return ret;
}

int ObTempBlockStore::init_dtl_block_buffer(void* mem, const int64_t size, Block *&block)
{
  int ret = OB_SUCCESS;
  ShrinkBuffer *buf = NULL;
  if (OB_ISNULL(mem)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mem is null", KP(mem));
  } else if (OB_UNLIKELY(size <= Block::min_blk_size<true>(0))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer is not enough", K(size));
  } else if (OB_ISNULL(buf = new (Block::buffer_position(mem, size))ShrinkBuffer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc buffer failed", K(ret));
  } else if (OB_FAIL(buf->init(static_cast<char *>(mem), size))) {
    LOG_WARN("init shrink buffer failed", K(ret));
  } else {
    block = new (buf->head()) Block;
    if (OB_FAIL(buf->fill_head(sizeof(Block)))) {
      LOG_WARN("fill buffer head failed", K(ret), K(buf), K(sizeof(Block)));
    } else if (OB_FAIL(buf->fill_tail(sizeof(ShrinkBuffer)))) {
      LOG_WARN("fill buffer tail failed", K(ret), K(buf), K(sizeof(ShrinkBuffer)));
    } else {
      block->block_id_ = 0; // unused
      block->cnt_ = 0;
      block->buf_off_ = buf->capacity() - buf->tail_size();
    }
  }
  return ret;
}

/*
 * Append block to store.
 * The `buf` is the pointer to block, and the `size` is the size of the block header and
 * payload size
 */
int ObTempBlockStore::append_block(const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  const Block *src_block = reinterpret_cast<const Block*>(buf);
  const int64_t payload_size = size - sizeof(Block);
  if (OB_ISNULL(buf) || OB_UNLIKELY(payload_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret), KP(buf), K(size), K(payload_size));
  } else if (OB_UNLIKELY(!is_block(buf))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block magic is mismatch", K(ret), K(block_magic(buf)));
  } else if (OB_FAIL(append_block_payload(src_block->payload_, payload_size, src_block->cnt_))) {
    LOG_WARN("fail to append block payload", K(ret));
  }
  return ret;
}

/*
 * Append block payload to store.
 * The `buf` is the pointer to block payload, and the `size` is payload size.
 */
int ObTempBlockStore::append_block_payload(const char *buf, const int64_t size, const int64_t cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0) || cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret), KP(buf), K(size), K(cnt));
  } else if (OB_FAIL(new_block(size, blk_, true))) {
    LOG_WARN("fail to new block", K(ret));
  } else if (OB_UNLIKELY(size > blk_buf_.remain())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("blk size is not enough", K(ret), K(size), K(blk_buf_.remain()));
  } else {
    blk_->cnt_ = static_cast<uint32_t>(cnt);
    MEMCPY(blk_->payload_, buf, size);
    block_id_cnt_ = blk_->end();
    blk_buf_.fast_advance(size);
    LOG_DEBUG("append block payload", K(*this), K(*blk_), K(mem_used_), K(mem_hold_));
  }
  // dump data if mem used > 16MB
  const int64_t dump_threshold = 1 << 24;
  if (OB_SUCC(ret) && mem_used_ > dump_threshold) {
    if (OB_FAIL(dump(true /* all_dump */))) {
      LOG_WARN("dump failed", K(ret));
    }
  }
  return ret;
}

int ObTempBlockStore::new_block(const int64_t mem_size,
                                Block *&blk,
                                const bool strict_mem_size /* false*/)
{
  int ret = OB_SUCCESS;
  const int64_t min_blk_size = Block::min_blk_size<false>(mem_size);
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == blk_)) {
    if (OB_FAIL(alloc_block(blk_, min_blk_size, strict_mem_size))) {
      LOG_WARN("alloc block failed", K(ret), KPC(this));
    }
  } else if (OB_FAIL(dump_block_if_need(min_blk_size))) {
    LOG_WARN("fail to dump block if need", K(ret), K(min_blk_size));
  } else if (OB_FAIL(switch_block(min_blk_size, strict_mem_size))) {
    LOG_WARN("switch block failed", K(ret), K(mem_size), K(min_blk_size));
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(blk_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to new block", K(ret), KP(blk_));
    } else {
      blk = blk_;
    }
  }
  return ret;
}

int ObTempBlockStore::get_block(BlockReader &reader, const int64_t block_id, const Block *&blk)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(block_id < 0) || OB_UNLIKELY(block_id >= block_id_cnt_)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("invalid of row_id", K(ret), K(block_id), K_(block_id_cnt));
  } else {
    if (reader.file_size_ != file_size_) {
      reader.reset_cursor(file_size_);
      blk = NULL;
    }
    if (NULL != blk && blk->contain(block_id)) {
      // found in previous visited block
    } else if (block_id >= saved_block_id_cnt_) {
      // found in write block
      blk = blk_;
    } else {
      blk = NULL;
      bool blk_on_disk = true;
      if (OB_FAIL(inner_get_block(reader, block_id, blk, blk_on_disk))) {
        LOG_WARN("fail to get next block", K(ret));
      } else if (blk_on_disk) {
        if (need_compress() && OB_FAIL(decompr_block(reader, blk))) {
          LOG_WARN("fail to decompress block", K(ret), K(last_block_on_disk_));
        } else {
          Block *tmp_blk = const_cast<Block *>(blk);
          if (OB_FAIL(prepare_blk_for_read(tmp_blk))) {
            LOG_WARN("fail to prepare blk", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && reader.is_async() && OB_NOT_NULL(blk)) {
        // 1. prefetch next block, if do not need prefetch, the aio_blk is null;
        // 2. should prefetch after decompress, since we need the info in read_io_handler_
        int64_t next_block_id = block_id + blk->cnt_;
        if (OB_LIKELY(next_block_id >= 0) && OB_LIKELY(next_block_id < saved_block_id_cnt_)) {
          // if still have next block, prefetch next block
          reader.aio_buf_idx_ = (reader.aio_buf_idx_ + 1) % BlockReader::AIO_BUF_CNT;
          last_block_on_disk_ = true;
          if (OB_FAIL(load_block(reader, next_block_id, reader.aio_blk_, last_block_on_disk_))) {
            LOG_WARN("fail to prefetch next block", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(blk)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Null block returned", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // truncate file, if enable_trunc_
    if (enable_trunc_ && (last_trunc_offset_ + TRUNCATE_THRESHOLD < cur_file_offset_)) {
      if (OB_FAIL(truncate_file(cur_file_offset_))) {
        LOG_WARN("fail to truncate file", K(ret));
      } else {
        last_trunc_offset_ = cur_file_offset_;
      }
    }
  }

  return ret;
}

/* the compressed block is in reader.buf_.data(); we need to decompr it.
 * blk->raw_size_ is the decompressed size.  reader.read_io_handle.size is the compressd_size
 *  1. alloc a buf (decompressd size) to decompr_buf_.
 *  2. decompress( from reader.buf_.data(), to decompre_buf_.data())
 *  3. release the compressed_buf's space.
 *  4. set buf.data_, point to decompressed_buf_
 */

int ObTempBlockStore::decompr_block(BlockReader &reader, const Block *&blk)
{
  int ret = OB_SUCCESS;
  // need decompress here, the compressed data is in reader.buf_
  if (OB_ISNULL(reader.buf_.data()) || OB_ISNULL(blk)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpeteced null pointer", K(ret), KP(blk), KP(reader.buf_.data()));
  } else {
    int64_t comp_size = reader.read_io_handle_.get_data_size() - sizeof(Block);
    int64_t decomp_size = blk->raw_size_ - sizeof(Block);
    int64_t actual_uncomp_size = 0;
    if (OB_FAIL(ensure_reader_buffer(reader, reader.decompr_buf_, blk->raw_size_))) {
      LOG_WARN("fail to alloc decomp_buf", K(ret));
    } else if (FALSE_IT(MEMCPY(reader.decompr_buf_.data(), blk, sizeof(Block)))) {
    } else {
      if (OB_FAIL(compressor_.decompress(blk->payload_,  comp_size,
                                         decomp_size, reader.decompr_buf_.data() + sizeof(Block),
                                         actual_uncomp_size))) {
        LOG_WARN("fail to decompress block", K(ret), KPC(this), K(blk->block_id_), K(blk->cnt_));
      } else if (reader.is_async()) {
        if (OB_FAIL(reader.buf_.init(reader.decompr_buf_.data(), reader.decompr_buf_.capacity()))) {
          LOG_WARN("fail to init reader buf ", K(ret), K(reader.buf_), K(reader.decompr_buf_));
        } else {
          blk = reinterpret_cast<const Block*>(reader.buf_.data());
        }
      } else {
        free_blk_mem(reader.buf_.data(), reader.buf_.capacity());
        if (OB_FAIL(reader.buf_.init(reader.decompr_buf_.data(), reader.decompr_buf_.capacity()))) {
          LOG_WARN("fail to init reader buf ", K(ret), K(reader.buf_), K(reader.decompr_buf_));
        } else {
          blk = reinterpret_cast<const Block*>(reader.buf_.data());
          reader.decompr_buf_.reset();
        }
      }
      if (OB_FAIL(ret)) {
        free_blk_mem(reader.decompr_buf_.data(), reader.decompr_buf_.capacity());
      }
    }
  }
  return ret;
}

// get block async or sync
int ObTempBlockStore::inner_get_block(BlockReader &reader, const int64_t block_id,
                                      const Block *&blk, bool &blk_on_disk)
{
  int ret = OB_SUCCESS;
  blk = nullptr;
  blk_on_disk = true;
  if (reader.is_async()) {
    int aio_buf_idx = reader.aio_buf_idx_ % BlockReader::AIO_BUF_CNT;
    bool need_sync_read = false;
    if (OB_NOT_NULL(reader.aio_blk_)) {
      // the blk is in memory, do not need wait.
      if (reader.aio_blk_->magic_ == Block::MAGIC && reader.aio_blk_->contain(block_id)) {
        blk_on_disk = false;
        blk = reader.aio_blk_;
        reader.aio_blk_ = nullptr;
      } else {
        need_sync_read = true;
      }
    } else {
      if (OB_ISNULL(reader.aio_buf_[aio_buf_idx].data())) {
        // no prefetch
        need_sync_read = true;
      } else if (OB_FAIL(reader.aio_wait())) {
        LOG_WARN("fail to wait read", K(ret), K(reader));
      } else {
        const Block *tmp_blk = reinterpret_cast<const Block*>(reader.aio_buf_[aio_buf_idx].data());
        if (tmp_blk->magic_ == Block::MAGIC && tmp_blk->contain(block_id)) {
          // using the prefetch blk
          if (OB_FAIL(reader.buf_.init(reader.aio_buf_[aio_buf_idx].data(),
                                       reader.aio_buf_[aio_buf_idx].capacity()))) {
            LOG_WARN("fail to init buf with aio_buf", K(ret));
          } else {
            blk = reinterpret_cast<Block*>(reader.buf_.data());
          }
        } else {
          // check fail, shouldn't use the prefetch block, need to load block
          need_sync_read = true;
        }
      }
    }

    if (OB_SUCC(ret) && need_sync_read) {
      // fail to prefetch, read using block_id
      if (OB_FAIL(load_block(reader, block_id, reader.aio_blk_, blk_on_disk))) {
        LOG_WARN("fail to load block", K(ret));
      } else if (OB_NOT_NULL(reader.aio_blk_)) {
        // the blk is in memory, do not need wait.
        blk_on_disk = false;
        blk = reader.aio_blk_;
        reader.aio_blk_ = nullptr;
      } else if (OB_FAIL(reader.aio_wait())) {
        LOG_WARN("fail to wait read", K(ret), K(reader));
      } else {
        if (OB_FAIL(reader.buf_.init(reader.aio_buf_[aio_buf_idx].data(),
                    reader.aio_buf_[aio_buf_idx].capacity()))) {
          LOG_WARN("fail to init buf with aio_buf", K(ret));
        } else {
          blk = reinterpret_cast<Block*>(reader.buf_.data());
        }
      }
    }
    if (OB_SUCC(ret)) {
      blk_on_disk = (need_sync_read && blk_on_disk) || (!need_sync_read && last_block_on_disk_);
    }
  } else {
    if(OB_FAIL(load_block(reader, block_id, blk, blk_on_disk))) {
      LOG_WARN("fail to load block", K(ret));
    }
  }
  return ret;
}

int ObTempBlockStore::get_timeout(int64_t &timeout_ms)
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

int ObTempBlockStore::alloc_block(Block *&blk, const int64_t min_size, const bool strict_mem_size)
{
  int ret = OB_SUCCESS;
  int64_t size = min_size;
  if (!strict_mem_size) {
    size = std::max(static_cast<int64_t>(BLOCK_SIZE), min_size);
    size += sizeof(LinkNode);
    size = next_pow2(size);
    size -= sizeof(LinkNode);
  }
  void *mem = alloc_blk_mem(size, &blk_mem_list_);
  if (OB_ISNULL(mem)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret), K(size));
  } else if (OB_FAIL(blk_buf_.init(static_cast<char *>(mem), size))) {
    LOG_WARN("init shrink buffer failed", K(ret));
  } else if (OB_FAIL(setup_block(blk_buf_, blk))) {
    LOG_WARN("setup block buffer fail", K(ret));
  } else {
    ++block_cnt_;
    LOG_TRACE("succ to alloc new block", KP(this), KP(mem), K_(blk_buf), K(*blk));
  }
  if (OB_FAIL(ret) && !OB_ISNULL(mem)) {
    free_blk_mem(mem, size);
  }
  return ret;
}

void *ObTempBlockStore::alloc_blk_mem(const int64_t size, ObDList<LinkNode> *list)
{
  void *blk = NULL;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(size));
  } else {
    ObMemAttr attr(tenant_id_, label_, ctx_id_);
    void *mem = allocator_->alloc(size + sizeof(LinkNode), attr);
    if (OB_UNLIKELY(NULL == mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), KP(mem), K(size + sizeof(LinkNode)),
        K(label_), K(ctx_id_), K(mem_limit_), K(enable_dump_), K(mem_hold_), K(mem_used_));
    } else {
      LinkNode *node = new (mem) LinkNode;
      if (NULL != list && OB_UNLIKELY(!list->add_last(node))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("add node to list failed", K(ret));
        node->~LinkNode();
        allocator_->free(mem);
      } else {
        blk = static_cast<char *>(mem) + sizeof(LinkNode);
        inc_mem_hold(size + sizeof(LinkNode));
        max_block_size_ = MAX(max_block_size_, size);
        max_hold_mem_ = MAX(mem_hold_, max_hold_mem_);
      }
    }
  }
  return blk;
}

int ObTempBlockStore::setup_block(ShrinkBuffer &buf, Block *&blk)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!buf.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("block buffer not inited", K(ret));
  } else {
    buf.reuse();
    blk = new (buf.head()) Block;
    blk->block_id_ = block_id_cnt_;
    blk->raw_size_ = buf.capacity();
    if (OB_FAIL(buf.fill_head(sizeof(Block)))) {
      LOG_WARN("fill buffer head failed", K(ret), K(buf), K(sizeof(Block)));
    } else {
      inc_mem_used(sizeof(Block));
    }
    if (OB_SUCC(ret) && OB_FAIL(prepare_setup_blk(blk))) {
      LOG_WARN("fail to prepare setup blk", K(ret));
    }
  }
  return ret;
}

int ObTempBlockStore::switch_block(const int64_t min_size, const bool strict_mem_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(blk_)) {
    // do nothing
  } else if (OB_UNLIKELY(min_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(min_size));
  } else if (OB_FAIL(prepare_blk_for_switch(blk_))) {
    LOG_WARN("block compact failed", K(ret));
  } else {
    const bool finish_add = (0 == min_size);
    Block *new_blk = NULL;
    BlockIndex bi;
    bi.is_idx_block_ = false;
    bi.on_disk_ = false;
    bi.block_id_ = ~(0b11UL << 62) & saved_block_id_cnt_;
    bi.blk_ = blk_;
    bi.length_ = static_cast<int32_t>(blk_buf_.head_size());
    bi.capacity_ = static_cast<int32_t>(blk_buf_.capacity());
    blk_->raw_size_ = bi.length_;
    blk_buf_.reset();
    if (OB_FAIL(add_block_idx(bi))) {
      LOG_WARN("add block index failed", K(ret));
    } else if (!finish_add && OB_FAIL(alloc_block(new_blk, min_size, strict_mem_size))) {
      LOG_WARN("alloc block failed", K(ret), K(min_size));
    } else {
      LOG_DEBUG("switch block", KP(blk_), K(*blk_), K(blk_->checksum()));
      saved_block_id_cnt_ = block_id_cnt_;
      blk_ = new_blk;
    }
    if (OB_FAIL(ret) && NULL != new_blk) {
      free_blk_mem(new_blk, blk_buf_.capacity());
    }
  }
  return ret;
}

int ObTempBlockStore::add_block_idx(const BlockIndex &bi)
{
  int ret = OB_SUCCESS;
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
      inc_mem_used(sizeof(BlockIndex));
    }
  }
  return ret;
}

int ObTempBlockStore::alloc_idx_block(IndexBlock *&ib)
{
  int ret = OB_SUCCESS;
  void *mem = alloc_blk_mem(IndexBlock::INDEX_BLOCK_SIZE, NULL);
  if (OB_ISNULL(mem)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret));
  } else {
    ib = new (mem) IndexBlock;
    ++index_block_cnt_;
    inc_mem_used(sizeof(IndexBlock));
  }
  return ret;
}

int ObTempBlockStore::build_idx_block()
{
  STATIC_ASSERT(IndexBlock::capacity() > DEFAULT_BLOCK_CNT,
      "DEFAULT_BLOCK_CNT block indexes must fit in one index block");
  int ret = OB_SUCCESS;
  if (OB_FAIL(alloc_idx_block(idx_blk_))) {
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

int ObTempBlockStore::switch_idx_block(bool finish_add)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == idx_blk_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index block should not be null");
  } else if (OB_FAIL(link_idx_block(idx_blk_))) {
    LOG_WARN("fail to link index block", K(ret));
  } else {
    IndexBlock *ib = NULL;
    BlockIndex bi;
    bi.is_idx_block_ = true;
    bi.on_disk_ = false;
    bi.block_id_ = idx_blk_->block_id();
    bi.idx_blk_ = idx_blk_;
    bi.length_ = static_cast<int32_t>(idx_blk_->buffer_size());
    bi.capacity_ = static_cast<int32_t>(IndexBlock::INDEX_BLOCK_SIZE);
    if (!finish_add) {
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
      if (NULL != ib) {
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

int ObTempBlockStore::link_idx_block(IndexBlock *idx_blk)
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

void ObTempBlockStore::set_mem_hold(int64_t hold)
{
  inc_mem_hold(hold - mem_hold_);
}

void ObTempBlockStore::inc_mem_hold(int64_t hold)
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

void ObTempBlockStore::free_blk_mem(void *mem, const int64_t size /* = 0 */)
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

int ObTempBlockStore::load_block(BlockReader &reader, const int64_t block_id,
                                 const Block *&blk, bool &on_disk)
{
  int ret = OB_SUCCESS;
  BlockIndex *bi;
  blk = nullptr;
  on_disk = true;
  if (OB_UNLIKELY(block_id < 0) || OB_UNLIKELY(block_id >= saved_block_id_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row should be saved", K(ret), K(block_id), K_(saved_block_id_cnt));
  } else if (OB_FAIL(find_block_idx(reader, block_id, bi))) {
    LOG_WARN("find block index failed", K(ret), K(block_id));
  } else {
    if (!bi->on_disk_) {
      blk = bi->blk_;
      on_disk = false;
    } else {
      if (reader.is_async()) {
        int aio_buf_idx = reader.aio_buf_idx_ % BlockReader::AIO_BUF_CNT;
        if (OB_FAIL(ensure_reader_buffer(reader, reader.aio_buf_[aio_buf_idx],
                                         bi->length_))) {
          LOG_WARN("ensure reader buffer failed", K(ret));
        } else if (OB_FAIL(read_file(reader.aio_buf_[aio_buf_idx].data(), bi->length_, bi->offset_,
                            reader.get_read_io_handler(), reader.is_async()))) {
          LOG_WARN("read block from file failed", K(ret), K(bi));
        }
      } else {
        if (OB_FAIL(ensure_reader_buffer(reader, reader.buf_, bi->length_))) {
          LOG_WARN("ensure reader buffer failed", K(ret));
        } else if (OB_FAIL(read_file(reader.buf_.data(), bi->length_, bi->offset_,
                            reader.get_read_io_handler(), reader.is_async()))) {
          LOG_WARN("read block from file failed", K(ret), K(bi));
        }
      }
    }
    if (OB_SUCC(ret) && bi->on_disk_) {
      if (reader.is_async()) {
        cur_file_offset_ = bi->offset_ > 0 ? bi->offset_ - 1 : 0;
      } else {
        blk = reinterpret_cast<const Block *>(reader.buf_.data());
        cur_file_offset_ = bi->offset_ + bi->length_;
      }
    }
  }
  return ret;
}

int ObTempBlockStore::find_block_idx(BlockReader &reader, const int64_t block_id, BlockIndex *&bi)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(block_id < 0) || OB_UNLIKELY(block_id >= saved_block_id_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row should be saved", K(ret), K(block_id), K_(saved_block_id_cnt));
  } else {
    bool found = false;
    if (NULL != reader.idx_blk_) {
      if (OB_UNLIKELY(reader.ib_pos_ < 0)
          || OB_UNLIKELY(reader.ib_pos_ >= reader.idx_blk_->cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ib_pos out of range", K(ret), K(reader.ib_pos_), K(*reader.idx_blk_));
      } else {
        int64_t pos = reader.ib_pos_;
        if (block_id > reader.idx_blk_->block_indexes_[pos].block_id_) {
          pos += 1;
          if (reader.idx_blk_->blk_in_pos(block_id, pos)) {
            found = true;
            reader.ib_pos_ = pos;
          }
        } else {
          pos -= 1;
          if (reader.idx_blk_->blk_in_pos(block_id, pos)) {
            found = true;
            reader.ib_pos_ = pos;
          }
        }
      }
      if (!found) {
        reader.reset_cursor(file_size_, false);
      } else {
        bi = &(reader.idx_blk_->block_indexes_[reader.ib_pos_]);
      }
    }
    if (OB_FAIL(ret) || found) {
    } else {
      IndexBlock *ib = NULL;
      if (NULL != idx_blk_ && !idx_blk_->is_empty() && block_id >= idx_blk_->block_id()) {
        ib = idx_blk_;
      }

      if (NULL == ib && blocks_.count() > 0) {
        ObSEArray<BlockIndex, DEFAULT_BLOCK_CNT>::iterator it =
          std::lower_bound(blocks_.begin(), blocks_.end(), block_id, &BlockIndex::compare);
        if (it == blocks_.end() || it->block_id_ != block_id) {
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
        BlockIndex *it = std::lower_bound(&ib->block_indexes_[0], &ib->block_indexes_[ib->cnt_],
            block_id, &BlockIndex::compare);
        if (it == ib->block_indexes_ + ib->cnt_ || it->block_id_ != block_id) {
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

int ObTempBlockStore::load_idx_block(BlockReader &reader, IndexBlock *&ib, const BlockIndex &bi)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!bi.is_idx_block_)) {
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
          reader.idx_buf_.data(), bi.length_, bi.offset_, reader.get_read_io_handler(), false))) {
        LOG_WARN("read block index from file failed", K(ret), K(bi));
      } else {
        ib = reinterpret_cast<IndexBlock *>(reader.idx_buf_.data());
      }
    }
  }
  return ret;
}

int ObTempBlockStore::ensure_reader_buffer(BlockReader &reader, ShrinkBuffer &buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    TryFreeMemBlk *try_reuse_blk = NULL;
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
          TryFreeMemBlk *p = cur->next_;
          if (NULL == try_reuse_blk && cur->size_ >= size) {
            try_reuse_blk = cur;
          } else {
            free_blk_mem(cur, cur->size_);
          }
          cur = p;
        }
      }
    }
    // add used block to try free list if in iteration age control.
    if ((NULL != reader.blk_holder_ptr_ || NULL != reader.age_) && buf.is_inited()) {
      TryFreeMemBlk *p = reinterpret_cast<TryFreeMemBlk *>(buf.data());
      p->size_ = buf.capacity();
      if (NULL != reader.blk_holder_ptr_) {
        p->reader_ = &reader;
        p->next_ = reader.blk_holder_ptr_->blocks_;
        reader.blk_holder_ptr_->blocks_ = p;
      } else if (NULL != reader.age_) {
        p->age_ = reader.age_->get();
        p->next_ = reader.try_free_list_;
        reader.try_free_list_ = p;
      }
      if (buf.data() == reader.buf_.data()) {
        // reset `buf_` of reader if the buffers share common memory ptr.
        reader.buf_.reset();
      }
      buf.reset();
    }

    if (buf.is_inited() && buf.capacity() < size) {
      free_blk_mem(buf.data(), buf.capacity());
      buf.reset();
    }
    if (!buf.is_inited()) {
      if (NULL == try_reuse_blk) {
        // alloc new memory block for reader
        const int64_t alloc_size = next_pow2(size);
        char *mem = static_cast<char *>(alloc_blk_mem(alloc_size, &alloced_mem_list_));
        if (OB_UNLIKELY(NULL == mem)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed", K(ret), K(alloc_size));
        } else if (OB_FAIL(buf.init(mem, alloc_size))) {
          LOG_WARN("init buffer failed", K(ret));
          free_blk_mem(mem);
          mem = NULL;
        }
        LOG_TRACE("alloc new blk for reader", KP(this), KP(mem));
      } else if (OB_FAIL(buf.init(reinterpret_cast<char *>(try_reuse_blk), try_reuse_blk->size_))) {
        LOG_WARN("fail to init reused block buf", K(ret), K_(try_reuse_blk->size));
        free_blk_mem(try_reuse_blk);
        try_reuse_blk = NULL;
      }
    } else if (try_reuse_blk != NULL) {
      free_blk_mem(try_reuse_blk);
      try_reuse_blk = NULL;
    }
  }
  return ret;
}

int ObTempBlockStore::BlockReader::aio_wait()
{
  int ret = OB_SUCCESS;
  int64_t timeout_ms = 0;
  OZ(get_timeout(timeout_ms));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(read_io_handle_.wait())) {
      LOG_WARN("aio wait failed", K(ret), K(timeout_ms));
    }
  }
  return ret;
}

int ObTempBlockStore::BlockReader::get_block(const int64_t block_id, const Block *&blk)
{
  return store_->get_block(*this, block_id, blk);
}

int ObTempBlockStore::write_file(BlockIndex &bi, void *buf, int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t timeout_ms = 0;
  if (OB_UNLIKELY(size < 0) || OB_UNLIKELY(size > 0 && NULL == buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(size), KP(buf));
  } else if (OB_FAIL(get_timeout(timeout_ms))) {
    LOG_WARN("get timeout failed", K(ret));
  } else {
    if (!is_file_open()) {
      if (OB_FAIL(alloc_dir_id())) {
        LOG_WARN("alloc file directory failed", K(ret));
      } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.open(io_.fd_, io_.dir_id_))) {
        LOG_WARN("open file failed", K(ret));
      } else {
        file_size_ = 0;
        io_.tenant_id_ = tenant_id_;
        io_.io_desc_.set_wait_event(ObWaitEventIds::ROW_STORE_DISK_WRITE);
        io_.io_timeout_ms_ = timeout_ms;
        LOG_INFO("open file success", K_(io_.fd), K_(io_.dir_id), K(get_compressor_type()));
      }
    }
    ret = OB_E(EventTable::EN_8) ret;
  }
  if (OB_SUCC(ret) && size > 0) {
    io_.buf_ = static_cast<char *>(buf);
    io_.size_ = size;
    const uint64_t start = rdtsc();
    if (write_io_handle_.is_valid() && OB_FAIL(write_io_handle_.wait())) {
      LOG_WARN("fail to wait write", K(ret), K(write_io_handle_));
    } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.aio_write(io_, write_io_handle_))) {
      LOG_WARN("write to file failed", K(ret), K_(io), K(timeout_ms));
    }
    if (NULL != io_observer_) {
      io_observer_->on_write_io(rdtsc() - start);
    }
  }
  if (OB_SUCC(ret)) {
    bi.on_disk_ = true;
    bi.offset_ = file_size_;
    file_size_ += size;
    if (NULL != mem_stat_) {
      mem_stat_->dumped(size);
    }
  }
  return ret;
}

int ObTempBlockStore::read_file(void *buf, const int64_t size, const int64_t offset,
                                blocksstable::ObTmpFileIOHandle &handle, const bool is_async)
{
  int ret = OB_SUCCESS;
  int64_t timeout_ms = 0;
  if (OB_UNLIKELY(offset < 0) || OB_UNLIKELY(size < 0) || OB_UNLIKELY(size > 0 && NULL == buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(size), K(offset), KP(buf));
  } else if (OB_FAIL(get_timeout(timeout_ms))) {
    LOG_WARN("get timeout failed", K(ret));
  } else if (!handle.is_valid() && OB_FAIL(write_io_handle_.wait())) {
    LOG_WARN("fail to wait write", K(ret));
  }

  if (OB_SUCC(ret) && size > 0) {
    blocksstable::ObTmpFileIOInfo tmp_read_id = io_;
    tmp_read_id.buf_ = static_cast<char *>(buf);
    tmp_read_id.size_ = size;
    tmp_read_id.io_desc_.set_wait_event(ObWaitEventIds::ROW_STORE_DISK_READ);
    tmp_read_id.io_timeout_ms_ = timeout_ms;
    const uint64_t start = rdtsc();
    if (is_async) {
      if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.aio_pread(tmp_read_id, offset, handle))) {
        LOG_WARN("read form file failed", K(ret), K(tmp_read_id), K(offset), K(timeout_ms));
      }
    } else {
      if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.pread(tmp_read_id, offset, handle))) {
        LOG_WARN("read form file failed", K(ret), K(tmp_read_id), K(offset), K(timeout_ms));
      } else if (OB_UNLIKELY(handle.get_data_size() != size)) {
        ret = OB_INNER_STAT_ERROR;
        LOG_WARN("read data less than expected", K(ret), K(tmp_read_id),
                                                 "read_size", handle.get_data_size());
      }
    }
    if (NULL != io_observer_) {
      io_observer_->on_read_io(rdtsc() - start);
    }
  }
  return ret;
}


int ObTempBlockStore::dump_block_if_need(const int64_t extra_size)
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

bool ObTempBlockStore::need_dump(const int64_t extra_size)
{
  bool need_to_dump = false;
  if (!GCONF.is_sql_operator_dump_enabled() || !enable_dump_) { // no dump
  } else if (mem_limit_ > 0) {
    if (mem_hold_ + extra_size > mem_limit_) {
      need_to_dump = true;
      LOG_TRACE("need dump", K(mem_hold_), K(mem_limit_));
    }
  }
  return need_to_dump;
}

int ObTempBlockStore::dump(const bool all_dump, const int64_t target_dump_size /*INT64_MAX*/)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("start to dump store", KP(this), K(*this), K(all_dump), K(target_dump_size),
             K(blk_mem_list_.get_size()));
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!enable_dump_) {
    ret = OB_EXCEED_MEM_LIMIT;
    LOG_INFO("BlockStore exceed mem limit and dump is disabled", K(ret));
  } else {
    if (all_dump) {
      // If need to dump all, first switch block and index block to ensure
      // that all block indexes are established.
      if (OB_FAIL(switch_block(0 /*finish_add */, false))) {
        LOG_WARN("fail to dump last block", K(ret));
      } else if (NULL != idx_blk_ && OB_FAIL(switch_idx_block(true /* finish_add */))) {
        LOG_WARN("fail to dump last index block", K(ret));
      }
      LOG_TRACE("dump all blocks", K(blk_mem_list_.get_size()));
    }
    int64_t total_dumped_size = 0;
    LinkNode *node = blk_mem_list_.get_first();
    LinkNode *next_node = NULL;
    void *mem = nullptr;
    while (OB_SUCC(ret) && node != blk_mem_list_.get_header() &&
        (all_dump || total_dumped_size < target_dump_size)) {
      next_node = node->get_next();
      if (OB_ISNULL(mem = static_cast<void *>(node + 1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur block is null", K(ret));
      } else {
        int64_t dumped_size = 0;
        if (is_last_block(mem)) {
          // skip the last block or index block
        } else if (is_block(mem)) {
          if (OB_FAIL(dump_block(static_cast<Block *>(mem), dumped_size))) {
            LOG_WARN("fail to dump block", K(ret), KP(mem));
          }
        } else if (is_index_block(mem)) {
          if (OB_FAIL(dump_index_block(static_cast<IndexBlock *>(mem), dumped_size))) {
            LOG_WARN("fail to dump index block", K(ret), KP(this), KP(mem));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("magic of cur block is unexpected", K(ret), K(block_magic(mem)),
                                                        K(blk_mem_list_.get_size()));
        }
        if (OB_SUCC(ret) && dumped_size > 0) {
          if (OB_ISNULL(blk_mem_list_.remove(node))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("remove node failed", K(ret), K(blk_mem_list_.get_size()));
          } else {
            node->~LinkNode();
            allocator_->free(node);
            total_dumped_size += dumped_size;
          }
        }
      }
      if (OB_SUCC(ret)) {
        node = next_node;
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(all_dump && !blk_mem_list_.is_empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("all_dump mode blk_mem_list_ is non-empty", K(ret), K(blk_mem_list_.get_size()));
    }
  }
  LOG_TRACE("after dump", K(ret), KP(this), K(*this), K(blk_mem_list_.get_size()),
                          KP(blk_), KP(idx_blk_));
  return ret;
}

int ObTempBlockStore::write_compressed_block(Block *blk, BlockIndex *bi)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(blk) || OB_ISNULL(bi)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null pointer", K(ret));
  } else {
    // only compress payload_ in block.
    int64_t need_size = 0;
    int64_t data_size = blk->raw_size_ - sizeof(Block);
    char *comp_buf = nullptr;
    int64_t comp_size = 0;
    if (OB_FAIL(compressor_.calc_need_size(data_size, need_size))) {
      LOG_WARN("fail to calc need size", K(ret));
    } else if (OB_ISNULL(comp_buf = (char *)allocator_->alloc(need_size + sizeof(Block)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), KP(comp_buf));
    } else if (FALSE_IT(MEMCPY(comp_buf, blk, sizeof(Block)))) { // copy the head
    } else if (OB_FAIL(compressor_.compress(blk->payload_, data_size, need_size, comp_buf + sizeof(Block), comp_size))) {
      LOG_WARN("fail to compress block", K(ret));
    } else if (OB_FAIL(write_file(*bi, static_cast<void *>(comp_buf), comp_size + sizeof(Block)))) {
      LOG_WARN("fail to write compressed block to file", K(ret));
    } else {
      bi->length_ = comp_size + sizeof(Block);
    }
    if (OB_NOT_NULL(comp_buf)) {
      allocator_->free(comp_buf);
    }
  }

  return ret;
}

int ObTempBlockStore::dump_block(Block *blk, int64_t &dumped_size)
{
  int ret = OB_SUCCESS;
  BlockIndex *bi;
  if (OB_FAIL(find_block_idx(inner_reader_, blk->block_id_, bi))) {
    LOG_WARN("fail to find_block_index", K(ret), K(blk));
  } else if (OB_UNLIKELY(bi->on_disk_ || bi->is_idx_block_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block on disk is unexpected", K(ret), K(bi), K(*bi), K(*blk));
  } else if (FALSE_IT(dumped_size = bi->length_)) {
  } else if (OB_FAIL(prepare_blk_for_write(blk))) {
    LOG_WARN("fail to prepare blk for write", K(ret));
  } else if (need_compress()) {
    if (OB_FAIL(write_compressed_block(blk, bi))) {
      LOG_WARN("fail to write compressed block", K(ret), K(bi));
    }
  } else {
    if (OB_FAIL(write_file(*bi, static_cast<void *>(blk), bi->length_))) {
      LOG_WARN("write block to file failed", K(ret), K(bi));
    }
  }

  if (OB_SUCC(ret)) {
    ++block_cnt_on_disk_;
    dumped_block_id_cnt_ += blk->cnt_;
    inc_mem_hold(-(bi->capacity_ + sizeof(LinkNode)));
    inc_mem_used(-(dumped_size));
    LOG_TRACE("succ to dump block", KP(this), K(*blk), K(*bi), K(dumped_size));
  }
  return ret;
}

int ObTempBlockStore::dump_index_block(IndexBlock *idx_blk, int64_t &dumped_size)
{
  int ret = OB_SUCCESS;
  dumped_size = 0;
  if (OB_ISNULL(idx_blk)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Index block is null", K(ret), KP(idx_blk), KP(idx_blk_));
  } else if (OB_UNLIKELY(idx_blk->is_empty() || blocks_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("idx_blk or blocks is empty", K(ret), K(idx_blk), K(blocks_));
  } else {
    BlockIndex *bi = NULL;
    const int64_t block_id = idx_blk->block_id();
    ObSEArray<BlockIndex, DEFAULT_BLOCK_CNT>::iterator it =
        std::lower_bound(blocks_.begin(), blocks_.end(), block_id, &BlockIndex::compare);
    if (it == blocks_.end() || it->block_id_ != block_id) {
      it--;
    }
    bi = &(*it);
    if (OB_ISNULL(bi)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bi is null", K(ret), K(blocks_.count()));
    } else if (OB_UNLIKELY(!bi->is_idx_block_ || bi->on_disk_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block index is not idx block or memory index", K(ret), KP(bi), K(*bi));
    } else if (OB_FAIL(write_file(*bi, static_cast<void *>(idx_blk), bi->length_))) { // write file and update bi
      LOG_WARN("write index block to file failed", K(ret), K(bi));
    } else {
      dumped_size = bi->length_;
      inc_mem_hold(-(bi->capacity_ + sizeof(LinkNode)));
      inc_mem_used(-(dumped_size));
      LOG_TRACE("succ to dump idx_bk", KP(this), K(*idx_blk), K(dumped_size));
    }
  }
  return ret;
}

void ObTempBlockStore::free_mem_list(ObDList<LinkNode> &list)
{
  while (!list.is_empty()) {
    LinkNode *node = list.remove_first();
    if (NULL != node) {
      node->~LinkNode();
      allocator_->free(node);
    }
  }
}

int ObTempBlockStore::BlockReader::init(ObTempBlockStore *store, const bool async)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), KP(store));
  } else {
    store_ = store;
    is_async_ = async;
  }
  return ret;
}

void ObTempBlockStore::BlockReader::reset()
{
  reset_cursor(0);
  if (NULL != store_) {
    free_all_blks();
    store_->free_blk_mem(idx_buf_.data(), idx_buf_.capacity());
    idx_buf_.reset();
    for (int64_t i = 0; i < AIO_BUF_CNT; i++) {
      if (aio_buf_[i].data() != buf_.data()) {
        store_->free_blk_mem(aio_buf_[i].data(), aio_buf_[i].capacity());
      }
      aio_buf_[i].reset();
    }
    store_->free_blk_mem(buf_.data(), buf_.capacity());
    buf_.reset();
    decompr_buf_.reset();
    /*
     * 1. do not need to free decompr_buf_, since it's data_ is same as buf.
     * 2. aio_buf_[N].data() may have same ptr as buf_.data(); shoudn't free twice
     */
  }
  read_io_handle_.reset();
}

void ObTempBlockStore::BlockReader::reuse()
{
  reset_cursor(0);
  if (NULL != store_) {
    free_all_blks();
    store_->free_blk_mem(idx_buf_.data(), idx_buf_.capacity());
    idx_buf_.reset();
    for (int64_t i = 0; i < AIO_BUF_CNT; i++) {
      if (aio_buf_[i].data() != buf_.data()) {
        store_->free_blk_mem(aio_buf_[i].data(), aio_buf_[i].capacity());
      }
      aio_buf_[i].reset();
    }
    store_->free_blk_mem(buf_.data(), buf_.capacity());
    buf_.reset();
    decompr_buf_.reset();
  }
  read_io_handle_.set_last_extent_id(0);
}

void ObTempBlockStore::BlockReader::reset_cursor(const int64_t file_size, const bool need_release)
{
  file_size_ = file_size;
  idx_blk_ = NULL;
  aio_blk_ = NULL;
  ib_pos_ = 0;
  if (need_release && nullptr != blk_holder_ptr_) {
    blk_holder_ptr_->release();
    blk_holder_ptr_ = nullptr;
  }
}

void ObTempBlockStore::BlockReader::free_all_blks()
{
  while (NULL != try_free_list_ && NULL != store_) {
    TryFreeMemBlk *next = try_free_list_->next_;
    store_->free_blk_mem(try_free_list_, try_free_list_->size_);
    try_free_list_ = next;
  }
}

void ObTempBlockStore::BlockHolder::release()
{
  while (NULL != blocks_) {
    TryFreeMemBlk *next = blocks_->next_;
    if (OB_NOT_NULL(blocks_) && OB_NOT_NULL(blocks_->reader_)) {
      blocks_->reader_->free_blk_mem(blocks_, blocks_->size_);
    } else {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "get unexpected block pair", KP(blocks_), KP(blocks_->reader_));
    }
    blocks_ = next;
  }
}

OB_DEF_SERIALIZE(ObTempBlockStore)
{
  int ret = OB_SUCCESS;
  if (inited_ && (enable_dump_ || need_compress())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block store not support serialize if enable dump", K(ret), K_(enable_dump),
             K(get_compressor_type()));
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              ctx_id_,
              mem_limit_,
              label_);
  const int64_t count = get_block_cnt();
  OB_UNIS_ENCODE(count);
  if (OB_SUCC(ret)) {
    const LinkNode *node = blk_mem_list_.get_first();
    const LinkNode *next_node = NULL;
    const void *mem = nullptr;
    while (OB_SUCC(ret) && node != blk_mem_list_.get_header()) {
      next_node = node->get_next();
      if (OB_ISNULL(mem = static_cast<const void *>(node + 1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur block is null", K(ret));
      } else if (is_index_block(mem)) {
        // skip serialize index block
      } else if (OB_UNLIKELY(!is_block(mem))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("block magic mismatch", K(ret), K(block_magic(mem)));
      } else {
        // serialize data_buf_size
        const Block* blk = static_cast<const Block *>(mem);
        LOG_DEBUG("serialize block", K(*blk));
        const int64_t raw_size = get_block_raw_size(blk);
        OB_UNIS_ENCODE(raw_size);
        if (OB_SUCC(ret)) {
          // serialize block data
          if (buf_len - pos < raw_size) {
            ret = OB_SIZE_OVERFLOW;
          } else {
            MEMCPY(buf + pos, mem, raw_size);
            pos += raw_size;
          }
        }
      }
      if (OB_SUCC(ret)) {
        node = next_node;
      }
    } //end while
  }
  return ret;
}


OB_DEF_DESERIALIZE(ObTempBlockStore)
{
  int ret = OB_SUCCESS;
  char label[lib::AOBJECT_LABEL_SIZE + 1];
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              ctx_id_,
              mem_limit_,
              label);
  if (!is_inited()) {
    if (OB_FAIL(init(mem_limit_, false/*enable_dump*/, tenant_id_, ctx_id_, label,
                     NONE_COMPRESSOR))) {
      LOG_WARN("fail to init Block row store", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    Block *block = NULL;
    int64_t raw_size = 0;
    int64_t blk_cnt = 0;
    OB_UNIS_DECODE(blk_cnt);
    for (int64_t i = 0; i < blk_cnt && OB_SUCC(ret); ++i) {
      OB_UNIS_DECODE(raw_size);
      if (OB_SUCC(ret) && OB_FAIL(append_block(buf + pos, raw_size))) {
        LOG_WARN("fail to append block", K(ret));
      } else {
        pos += raw_size;
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTempBlockStore)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_,
              ctx_id_,
              mem_limit_,
              label_);
  const int64_t count = get_block_cnt();
  OB_UNIS_ADD_LEN(count);
  const LinkNode *node = blk_mem_list_.get_first();
  const LinkNode *next_node = NULL;
  const void *mem = nullptr;
  while (node != blk_mem_list_.get_header()) {
    next_node = node->get_next();
    if (OB_ISNULL(mem = static_cast<const void *>(node + 1))) {
      break;
    } else if (is_block(mem)) {
      const Block* blk = static_cast<const Block *>(mem);
      const int64_t payload_size = get_block_raw_size(blk);
      OB_UNIS_ADD_LEN(payload_size);
      len += payload_size;
    }
    node = next_node;
  } //end while

  return len;
}

int ObTempBlockStore::truncate_file(int64_t offset)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.truncate(get_file_fd(), offset))) {
    LOG_WARN("truncate failed", K(ret), K(get_file_fd()), K(offset));
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
