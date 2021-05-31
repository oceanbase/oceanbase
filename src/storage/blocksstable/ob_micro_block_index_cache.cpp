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

#define USING_LOG_PREFIX STORAGE
#include "ob_micro_block_index_cache.h"
#include "lib/stat/ob_diagnose_info.h"
#include "ob_macro_block_meta_mgr.h"
#include "ob_store_file.h"
#include "storage/ob_sstable.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_file_system_util.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace blocksstable {
ObMicroBlockIndexInfo::ObMicroBlockIndexInfo() : block_id_(), table_id_(0), file_id_(OB_INVALID_DATA_FILE_ID)
{}

ObMicroBlockIndexInfo::ObMicroBlockIndexInfo(
    const MacroBlockId& block_id, const uint64_t table_id, const int64_t file_id)
    : block_id_(block_id), table_id_(table_id), file_id_(file_id)
{}

ObMicroBlockIndexInfo::~ObMicroBlockIndexInfo()
{}

uint64_t ObMicroBlockIndexInfo::hash() const
{
  return murmurhash(this, sizeof(ObMicroBlockIndexInfo), 0);
}

bool ObMicroBlockIndexInfo::operator==(const ObIKVCacheKey& other) const
{
  const ObMicroBlockIndexInfo& other_info = reinterpret_cast<const ObMicroBlockIndexInfo&>(other);
  return (block_id_ == other_info.block_id_ && table_id_ == other_info.table_id_ && file_id_ == other_info.file_id_);
}

uint64_t ObMicroBlockIndexInfo::get_tenant_id() const
{
  return extract_tenant_id(table_id_);
}

int64_t ObMicroBlockIndexInfo::size() const
{
  return sizeof(*this);
}

int ObMicroBlockIndexInfo::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey*& key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", KP(buf), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The micro block index info is not valid, ", K(*this), K(ret));
  } else {
    ObMicroBlockIndexInfo* info = new (buf) ObMicroBlockIndexInfo();
    info->block_id_ = block_id_;
    info->table_id_ = table_id_;
    info->file_id_ = file_id_;
    key = info;
  }
  return ret;
}

bool ObMicroBlockIndexInfo::is_valid() const
{
  return OB_INVALID_ID != table_id_ && block_id_.is_valid() && OB_INVALID_DATA_FILE_ID != file_id_;
}

void ObMicroBlockIndexInfo::set(const MacroBlockId& block_id, const uint64_t table_id, const int64_t file_id)
{
  block_id_ = block_id;
  table_id_ = table_id;
  file_id_ = file_id;
}

ObMicroBlockIndexCache::ObMicroBlockIndexCache()
{}

ObMicroBlockIndexCache::~ObMicroBlockIndexCache()
{}

int ObMicroBlockIndexCache::init(const char* cache_name, const int64_t priority)
{
  int ret = OB_SUCCESS;
  const int64_t mem_limit = 1024 * 1024 * 1024LL;
  if (OB_SUCCESS !=
      (ret = common::ObKVCache<ObMicroBlockIndexInfo, ObMicroBlockIndexMgr>::init(cache_name, priority))) {
    STORAGE_LOG(WARN, "Fail to init kv cache, ", K(ret));
  } else if (OB_FAIL(allocator_.init(mem_limit, OB_MALLOC_BIG_BLOCK_SIZE, OB_MALLOC_BIG_BLOCK_SIZE))) {
    STORAGE_LOG(WARN, "Fail to init io allocator, ", K(ret));
  } else {
    allocator_.set_label(ObModIds::OB_SSTABLE_INDEX);
  }
  return ret;
}

void ObMicroBlockIndexCache::destroy()
{
  common::ObKVCache<ObMicroBlockIndexInfo, ObMicroBlockIndexMgr>::destroy();
  allocator_.destroy();
}

int ObMicroBlockIndexCache::get_micro_infos(const uint64_t table_id, const ObMacroBlockCtx& macro_block_ctx,
    const ObStoreRange& range, const bool is_left_border, const bool is_right_border,
    common::ObIArray<ObMicroBlockInfo>& micro_infos, const bool is_prewarm)
{
  int ret = OB_SUCCESS;
  const ObMicroBlockIndexMgr* micro_index_mgr = NULL;
  ObKVCacheHandle handle;
  ObMicroBlockIndexTransformer* transformer = GET_TSI_MULT(ObMicroBlockIndexTransformer, 1);

  if (OB_UNLIKELY(OB_INVALID_ID == table_id || !macro_block_ctx.is_valid() || !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), K(table_id), K(macro_block_ctx), K(range));
  } else if (OB_UNLIKELY(NULL == transformer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "Fail to allocate transformer, ", K(ret));
  } else if (OB_FAIL(read_micro_index(table_id, macro_block_ctx, *transformer, micro_index_mgr, handle, is_prewarm))) {
    STORAGE_LOG(WARN, "Fail to read micro block index, ", K(ret), K(macro_block_ctx));
  } else if (OB_ISNULL(micro_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The micro_index_mgr is NULL, ", K(ret));
  } else if (OB_FAIL(micro_index_mgr->search_blocks(range, is_left_border, is_right_border, micro_infos))) {
    if (OB_BEYOND_THE_RANGE != ret) {
      STORAGE_LOG(WARN, "block index mgr fail to search blocks by range.", K(ret), K(range));
    }
  }
  return ret;
}

int ObMicroBlockIndexCache::get_micro_infos(const uint64_t table_id, const ObMacroBlockCtx& macro_block_ctx,
    const ObStoreRowkey& rowkey, ObMicroBlockInfo& micro_info, const bool is_prewarm)
{
  int ret = OB_SUCCESS;
  const ObMicroBlockIndexMgr* micro_index_mgr = NULL;
  ObKVCacheHandle handle;
  ObMicroBlockIndexTransformer* transformer = GET_TSI_MULT(ObMicroBlockIndexTransformer, 1);

  if (OB_UNLIKELY(OB_INVALID_ID == table_id || !macro_block_ctx.is_valid() || NULL == rowkey.get_obj_ptr() ||
                  rowkey.get_obj_cnt() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(table_id), K(macro_block_ctx), K(rowkey), K(ret));
  } else if (OB_UNLIKELY(NULL == transformer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "Fail to allocate transformer, ", K(ret));
  } else if (OB_FAIL(read_micro_index(table_id, macro_block_ctx, *transformer, micro_index_mgr, handle, is_prewarm))) {
    STORAGE_LOG(WARN, "Fail to read micro block index, ", K(ret), K(macro_block_ctx));
  } else if (OB_ISNULL(micro_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The micro_index_mgr is NULL, ", K(ret));
  } else if (OB_FAIL(micro_index_mgr->search_blocks(rowkey, micro_info))) {
    if (OB_BEYOND_THE_RANGE != ret) {
      STORAGE_LOG(WARN, "micro index mgr fail to search micro blocks.", K(ret), K(rowkey));
    }
  }
  return ret;
}

int ObMicroBlockIndexCache::get_micro_endkey(const uint64_t table_id, const ObMacroBlockCtx& macro_block_ctx,
    const int32_t micro_block_index, ObIAllocator& alloctor, ObStoreRowkey& endkey)
{
  int ret = OB_SUCCESS;
  const ObMicroBlockIndexMgr* micro_index_mgr = NULL;
  ObKVCacheHandle handle;
  ObMicroBlockIndexTransformer* transformer = GET_TSI_MULT(ObMicroBlockIndexTransformer, 1);
  const bool is_prewarm = false;

  if (OB_UNLIKELY(OB_INVALID_ID == table_id || !macro_block_ctx.is_valid() || micro_block_index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(table_id), K(macro_block_ctx), K(micro_block_index), K(ret));
  } else if (OB_UNLIKELY(NULL == transformer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "Fail to allocate transformer, ", K(ret));
  } else if (OB_FAIL(read_micro_index(table_id, macro_block_ctx, *transformer, micro_index_mgr, handle, is_prewarm))) {
    STORAGE_LOG(WARN, "Fail to read micro block index, ", K(ret));
  } else if (OB_ISNULL(micro_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The micro_index_mgr is NULL, ", K(ret));
  } else if (OB_FAIL(micro_index_mgr->get_endkey(micro_block_index, alloctor, endkey))) {
    if (OB_BEYOND_THE_RANGE != ret) {
      STORAGE_LOG(WARN, "micro index mgr fail to get endkey.", K(ret), K(macro_block_ctx), K(micro_block_index));
    }
  }
  return ret;
}

int ObMicroBlockIndexCache::get_micro_endkeys(const uint64_t table_id, const ObMacroBlockCtx& macro_block_ctx,
    ObIAllocator& alloctor, ObIArray<ObStoreRowkey>& endkeys)
{
  int ret = OB_SUCCESS;
  const ObMicroBlockIndexMgr* micro_index_mgr = NULL;
  ObKVCacheHandle handle;
  ObMicroBlockIndexTransformer* transformer = GET_TSI_MULT(ObMicroBlockIndexTransformer, 1);
  const bool is_prewarm = false;

  if (OB_UNLIKELY(OB_INVALID_ID == table_id || !macro_block_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(table_id), K(macro_block_ctx), K(ret));
  } else if (OB_UNLIKELY(NULL == transformer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "Fail to allocate transformer, ", K(ret));
  } else if (OB_FAIL(read_micro_index(table_id, macro_block_ctx, *transformer, micro_index_mgr, handle, is_prewarm))) {
    STORAGE_LOG(WARN, "Fail to read micro block index, ", K(ret));
  } else if (OB_ISNULL(micro_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The micro_index_mgr is NULL, ", K(ret));
  } else if (OB_FAIL(micro_index_mgr->get_endkeys(alloctor, endkeys))) {
    if (OB_BEYOND_THE_RANGE != ret) {
      STORAGE_LOG(WARN, "micro index mgr fail to get endkey.", K(ret), K(macro_block_ctx));
    }
  }
  return ret;
}

int ObMicroBlockIndexCache::read_micro_index(const uint64_t table_id, const ObMacroBlockCtx& macro_block_ctx,
    ObMicroBlockIndexTransformer& transformer, const ObMicroBlockIndexMgr*& index_mgr, ObKVCacheHandle& handle,
    const bool is_prewarm)
{
  int ret = OB_SUCCESS;
  const char* buf = NULL;
  ObIOHandle io_handle;
  ObFullMacroBlockMeta full_meta;
  ObMacroBlockReadInfo read_info;
  ObStorageFileHandle file_handle;
  ObStorageFile* file = nullptr;
  ObMacroBlockHandle macro_handle;

  if (OB_UNLIKELY(OB_INVALID_ID == table_id || !macro_block_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(table_id), K(macro_block_ctx), K(ret));
  } else if (OB_FAIL(
                 macro_block_ctx.sstable_->get_meta(macro_block_ctx.sstable_block_id_.macro_block_id_, full_meta))) {
    STORAGE_LOG(WARN, "fail to get meta", K(ret));
  } else if (!full_meta.is_valid()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "error sys, macro block meta must not be null", K(ret), K(full_meta));
  } else if (OB_FAIL(file_handle.assign(macro_block_ctx.sstable_->get_storage_file_handle()))) {
    STORAGE_LOG(WARN, "fail to get file handle", K(ret), K(macro_block_ctx.sstable_->get_storage_file_handle()));
  } else if (OB_ISNULL(file = file_handle.get_storage_file())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get pg file", K(ret), K(file_handle));
  } else {
    ObMicroBlockIndexInfo index_info(macro_block_ctx.get_macro_block_id(), table_id, file->get_file_id());

    if (OB_UNLIKELY(!index_info.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument, ", K(index_info), K(ret));
    } else if (OB_SUCC(get(index_info, index_mgr, handle))) {
      // Success get from cache
      EVENT_INC(ObStatEventIds::BLOCK_INDEX_CACHE_HIT);
    } else {
      EVENT_INC(ObStatEventIds::BLOCK_INDEX_CACHE_MISS);
      const ObMacroBlockMetaV2* meta = full_meta.meta_;
      read_info.macro_block_ctx_ = &macro_block_ctx;
      read_info.offset_ = meta->micro_block_index_offset_;
      read_info.size_ = meta->get_block_index_size();
      read_info.io_desc_.category_ = is_prewarm ? PREWARM_IO : USER_IO;
      read_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_DATA_INDEX_READ;
      macro_handle.set_file(file);
      if (OB_FAIL(file->read_block(read_info, macro_handle))) {
        STORAGE_LOG(WARN, "Fail to read macro block, ", K(ret));
      } else if (OB_ISNULL(buf = macro_handle.get_buffer())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected error, the io buffer is NULL, ", K(ret));
      } else if (OB_FAIL(transformer.transform(buf, full_meta, index_mgr))) {
        STORAGE_LOG(WARN, "transformer fail to transform, ", K(*meta), K(ret));
      } else if (OB_ISNULL(index_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "transformer fail to get block index mgr, ", K(ret));
      } else {
        // ignore ret, allow put fail
        int tmp_ret = OB_SUCCESS;
        bool overwrite = false;
        if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = put(index_info, *index_mgr, overwrite)))) {
          if (OB_ENTRY_EXIST != tmp_ret) {
            STORAGE_LOG(WARN, "Fail to put micro block index to cache, ", K(tmp_ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObMicroBlockIndexCache::get_cache_block_index(
    const uint64_t table_id, const MacroBlockId block_id, const int64_t file_id, ObMicroBlockIndexBufferHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id || !block_id.is_valid() || OB_INVALID_DATA_FILE_ID == file_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(table_id), K(block_id), K(file_id));
  } else {
    ObMicroBlockIndexInfo index_info(block_id, table_id, file_id);
    if (OB_UNLIKELY(!index_info.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument, ", K(index_info), K(ret));
    } else if (OB_SUCC(get(index_info, handle.index_mgr_, handle.handle_))) {
      // Success get from cache
      EVENT_INC(ObStatEventIds::BLOCK_INDEX_CACHE_HIT);
    } else {
      EVENT_INC(ObStatEventIds::BLOCK_INDEX_CACHE_MISS);
    }
  }
  return ret;
}

int ObMicroBlockIndexCache::prefetch(const uint64_t table_id, const blocksstable::ObMacroBlockCtx& macro_block_ctx,
    blocksstable::ObStorageFile* pg_file, ObMacroBlockHandle& macro_handle, const ObQueryFlag& flag)
{
  int ret = OB_SUCCESS;
  ObFullMacroBlockMeta full_meta;

  if (OB_UNLIKELY(0 == table_id || OB_INVALID_ID == table_id || !macro_block_ctx.is_valid() || NULL == pg_file)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(table_id), K(macro_block_ctx), K(pg_file));
  } else if (OB_FAIL(
                 macro_block_ctx.sstable_->get_meta(macro_block_ctx.sstable_block_id_.macro_block_id_, full_meta))) {
    STORAGE_LOG(WARN, "fail to get meta", K(ret));
  } else if (!full_meta.is_valid()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "error sys, meta must not be null", K(ret), K(macro_block_ctx));
  } else {
    const ObMacroBlockMetaV2* meta = full_meta.meta_;
    const int64_t data_offset = meta->micro_block_index_offset_;
    const int64_t data_size = meta->get_block_index_size();

    ObMacroBlockReadInfo read_info;
    ObMicroBlockIndexIOCallback callback;
    // fill read info
    read_info.io_desc_.category_ = flag.is_prewarm() ? PREWARM_IO
                                   : flag.is_large_query() && GCONF._large_query_io_percentage.get_value() > 0
                                       ? LARGE_QUERY_IO
                                       : USER_IO;
    read_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_DATA_INDEX_READ;
    read_info.macro_block_ctx_ = &macro_block_ctx;
    // fill callback
    common::align_offset_size(data_offset, data_size, read_info.offset_, read_info.size_);
    read_info.io_callback_ = &callback;

    callback.cache_ = this;
    callback.key_.set(macro_block_ctx.get_macro_block_id(), table_id, pg_file->get_file_id());
    callback.offset_ = std::abs(data_offset - read_info.offset_);
    callback.buf_size_ = read_info.size_ + DIO_READ_ALIGN_SIZE;
    callback.allocator_ = &allocator_;
    callback.use_index_cache_ = flag.is_use_block_index_cache();
    callback.io_buf_size_ = read_info.size_;
    callback.aligned_offset_ = read_info.offset_;

    macro_handle.set_file(pg_file);
    if (OB_FAIL(callback.table_handle_.set_table(macro_block_ctx.sstable_))) {
      STORAGE_LOG(WARN, "fail to set table", K(ret));
    } else if (OB_FAIL(pg_file->async_read_block(read_info, macro_handle))) {
      STORAGE_LOG(WARN, "Fail to async read block, ", K(ret));
    }
  }
  return ret;
}

int ObMicroBlockIndexCache::cal_border_row_count(const uint64_t table_id, const ObMacroBlockCtx& macro_block_ctx,
    const common::ObStoreRange& range, const bool is_left_border, const bool is_right_border,
    int64_t& logical_row_count, int64_t& physical_row_count, bool& need_check_micro_block)
{
  int ret = OB_SUCCESS;
  const ObMicroBlockIndexMgr* micro_index_mgr = NULL;
  ObKVCacheHandle handle;
  ObMicroBlockIndexTransformer* transformer = GET_TSI_MULT(ObMicroBlockIndexTransformer, 1);
  bool is_prewarm = false;

  if (OB_UNLIKELY(OB_INVALID_ID == table_id || !macro_block_ctx.is_valid() || !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(table_id), K(macro_block_ctx), K(range), K(ret));
  } else if (OB_UNLIKELY(NULL == transformer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "Fail to allocate transformer, ", K(ret));
  } else if (OB_FAIL(read_micro_index(table_id, macro_block_ctx, *transformer, micro_index_mgr, handle, is_prewarm))) {
    STORAGE_LOG(WARN, "Fail to read micro block index, ", K(ret));
  } else if (OB_ISNULL(micro_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The micro_index_mgr is NULL, ", K(ret));
  } else if (OB_FAIL(micro_index_mgr->cal_border_row_count(range,
                 is_left_border,
                 is_right_border,
                 logical_row_count,
                 physical_row_count,
                 need_check_micro_block))) {
    if (OB_BEYOND_THE_RANGE != ret) {
      STORAGE_LOG(WARN, "block index mgr fail to search blocks by range.", K(ret), K(range));
    }
  }
  return ret;
}

int ObMicroBlockIndexCache::cal_macro_purged_row_count(
    const uint64_t table_id, const ObMacroBlockCtx& macro_block_ctx, int64_t& purged_row_count)
{
  int ret = OB_SUCCESS;
  const ObMicroBlockIndexMgr* micro_index_mgr = NULL;
  ObKVCacheHandle handle;
  ObMicroBlockIndexTransformer* transformer = GET_TSI_MULT(ObMicroBlockIndexTransformer, 1);
  bool is_prewarm = false;

  if (OB_UNLIKELY(OB_INVALID_ID == table_id || !macro_block_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(table_id), K(macro_block_ctx), K(ret));
  } else if (OB_UNLIKELY(NULL == transformer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "Fail to allocate transformer, ", K(ret));
  } else if (OB_FAIL(read_micro_index(table_id, macro_block_ctx, *transformer, micro_index_mgr, handle, is_prewarm))) {
    STORAGE_LOG(WARN, "Fail to read micro block index, ", K(ret));
  } else if (OB_ISNULL(micro_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The micro_index_mgr is NULL, ", K(ret));
  } else if (OB_FAIL(micro_index_mgr->cal_macro_purged_row_count(purged_row_count))) {
    STORAGE_LOG(WARN, "block index mgr fail to get mark delete flags", K(ret), K(macro_block_ctx));
  }
  return ret;
}

ObMicroBlockIndexCache::ObMicroBlockIndexIOCallback::ObMicroBlockIndexIOCallback()
    : key_(),
      buffer_(NULL),
      offset_(0),
      buf_size_(0),
      cache_(NULL),
      allocator_(NULL),
      idx_mgr_(NULL),
      use_index_cache_(true),
      io_buf_size_(0),
      aligned_offset_(0),
      table_handle_()
{
  static_assert(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObMicroBlockIndexCache::ObMicroBlockIndexIOCallback::~ObMicroBlockIndexIOCallback()
{
  if (NULL != allocator_) {
    if (NULL != buffer_) {
      // free buffer_
      allocator_->free(buffer_);
      buffer_ = NULL;
    }
    if (!handle_.is_valid() && NULL != idx_mgr_) {
      allocator_->free(const_cast<ObMicroBlockIndexMgr*>(idx_mgr_));
      idx_mgr_ = NULL;
    }
  }
}

int64_t ObMicroBlockIndexCache::ObMicroBlockIndexIOCallback::size() const
{
  return sizeof(ObMicroBlockIndexIOCallback);
}

int ObMicroBlockIndexCache::ObMicroBlockIndexIOCallback::inner_deep_copy(
    char* buf, const int64_t buf_len, ObIOCallback*& callback) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", KP(buf), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(NULL == cache_)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The micro block io callback is not valid, ", KP_(cache), K(ret));
  } else {
    ObMicroBlockIndexIOCallback* pcallback = new (buf) ObMicroBlockIndexIOCallback();
    pcallback->key_ = key_;
    pcallback->buffer_ = buffer_;
    pcallback->offset_ = offset_;
    pcallback->buf_size_ = buf_size_;
    pcallback->cache_ = cache_;
    pcallback->allocator_ = allocator_;
    pcallback->idx_mgr_ = idx_mgr_;
    pcallback->handle_ = handle_;
    pcallback->use_index_cache_ = use_index_cache_;
    pcallback->io_buf_size_ = io_buf_size_;
    pcallback->aligned_offset_ = aligned_offset_;
    if (OB_FAIL(pcallback->table_handle_.assign(table_handle_))) {
      STORAGE_LOG(WARN, "fail to assign table handle", K(ret));
    } else {
      callback = pcallback;
    }
  }
  return ret;
}

int ObMicroBlockIndexCache::ObMicroBlockIndexIOCallback::alloc_io_buf(
    char*& io_buf, int64_t& io_buf_size, int64_t& aligned_offset)
{
  int ret = OB_SUCCESS;
  io_buf_size = 0;
  aligned_offset = 0;
  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "Invalid data, the allocator is NULL, ", K(ret));
  } else if (OB_UNLIKELY(NULL == (buffer_ = (char*)(allocator_->alloc(buf_size_))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "Fail to allocate memory, ", K(ret));
  } else {
    io_buf = reinterpret_cast<char*>(upper_align(reinterpret_cast<int64_t>(buffer_), DIO_READ_ALIGN_SIZE));
    io_buf_size = io_buf_size_;
    aligned_offset = aligned_offset_;
  }
  return ret;
}

int ObMicroBlockIndexCache::ObMicroBlockIndexIOCallback::put_cache_and_fetch(
    const ObFullMacroBlockMeta& meta, ObMicroBlockIndexTransformer& transformer)
{
  int ret = OB_SUCCESS;
  ObKVCachePair* kvpair = nullptr;
  ObKVCacheInstHandle inst_handle;
  const bool overwrite = false;
  const ObMicroBlockIndexMgr* idx_mgr = nullptr;
  const int64_t size = transformer.get_transformer_size();
  if (OB_UNLIKELY(OB_SUCCESS == (ret = cache_->get(key_, idx_mgr_, handle_)))) {
    // entry exist, no need to put
  } else if (OB_FAIL(cache_->alloc(key_.get_tenant_id(), key_.size(), size, kvpair, handle_, inst_handle))) {
    LOG_WARN("failed to alloc kvcache buf", K(ret));
  } else {
    if (OB_FAIL(key_.deep_copy(reinterpret_cast<char*>(kvpair->key_), key_.size(), kvpair->key_))) {
      LOG_WARN("failed to deep copy key", K(ret), K_(key));
    } else if (OB_FAIL(
                   transformer.create_block_index_mgr(meta, reinterpret_cast<char*>(kvpair->value_), size, idx_mgr))) {
      LOG_WARN("failed to create block index mgr", K(ret));
    } else {
      idx_mgr_ = idx_mgr;
      if (OB_FAIL(cache_->put_kvpair(inst_handle, kvpair, handle_, overwrite))) {
        if (OB_ENTRY_EXIST != ret) {
          LOG_WARN("failed to put micro block index mgr to cache", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_FAIL(ret)) {
      handle_.reset();
      idx_mgr_ = nullptr;
    }
  }
  return ret;
}

int ObMicroBlockIndexCache::ObMicroBlockIndexIOCallback::inner_process(const bool is_success)
{
  int ret = OB_SUCCESS;
  ObMicroBlockIndexTransformer* transformer = GET_TSI_MULT(ObMicroBlockIndexTransformer, 1);
  ObFullMacroBlockMeta full_meta;
  ObTableHandle table_handle;
  ObSSTable* sstable = nullptr;

  if (NULL == cache_ || NULL == buffer_ || NULL == allocator_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The cache is null, ", KP_(cache), KP_(buffer), K(ret));
  } else if (is_success) {
    char* index_buf =
        reinterpret_cast<char*>(upper_align(reinterpret_cast<int64_t>(buffer_), DIO_READ_ALIGN_SIZE)) + offset_;
    if (OB_FAIL(table_handle_.get_sstable(sstable))) {
      STORAGE_LOG(WARN, "fail to get sstable", K(ret));
    } else if (OB_ISNULL(sstable)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "error sys, sstable must not be null", K(ret));
    } else if (OB_FAIL(sstable->get_meta(key_.get_block_id(), full_meta))) {
      STORAGE_LOG(WARN, "fail to get meta", K(ret), K(key_));
    } else if (!full_meta.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro meta is null", K(ret), K_(key), K(full_meta));
    } else if (OB_FAIL(transformer->transform(index_buf, full_meta))) {
      STORAGE_LOG(WARN, "transformer fail to transform, ", K(ret));
    } else {
      if (OB_UNLIKELY(!use_index_cache_) || OB_FAIL(put_cache_and_fetch(full_meta, *transformer))) {
        char* buf = nullptr;
        const int64_t buf_len = transformer->get_transformer_size();
        handle_.reset();
        if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to allocate index mgr", K(ret), K(buf_len));
        } else if (OB_FAIL(transformer->create_block_index_mgr(full_meta, buf, buf_len, idx_mgr_))) {
          STORAGE_LOG(WARN, "failed to deep copy index mgr", K(ret));
          allocator_->free(buf);
        }
      }
    }
  }

  if (NULL != allocator_ && NULL != buffer_) {
    // free buffer_
    allocator_->free(buffer_);
    buffer_ = NULL;
  }
  return ret;
}

const char* ObMicroBlockIndexCache::ObMicroBlockIndexIOCallback::get_data()
{
  const char* data = NULL;
  if (NULL != idx_mgr_) {
    data = reinterpret_cast<const char*>(idx_mgr_);
  }
  return data;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
