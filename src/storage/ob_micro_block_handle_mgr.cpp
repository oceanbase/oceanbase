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
#include "blocksstable/ob_micro_block_info.h"
#include "blocksstable/ob_storage_cache_suite.h"
#include "ob_micro_block_handle_mgr.h"


using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace storage {
/**
 * --------------------------------------------------------------ObMicroBlockDataHandle-----------------------------------------------------------------
 */
ObMicroBlockDataHandle::ObMicroBlockDataHandle()
  : tenant_id_(0),
    macro_block_id_(),
    block_state_(ObSSTableMicroBlockState::UNKNOWN_STATE),
    block_index_(-1),
    micro_info_(),
    des_meta_(),
    encrypt_key_(),
    cache_handle_(),
    io_handle_(),
    allocator_(nullptr),
    loaded_index_block_data_(),
    is_loaded_index_block_(false)
{
  des_meta_.encrypt_key_ = encrypt_key_;
}

ObMicroBlockDataHandle::~ObMicroBlockDataHandle()
{
  reset();
}

void ObMicroBlockDataHandle::reset()
{
  block_state_ = ObSSTableMicroBlockState::UNKNOWN_STATE;
  tenant_id_ = 0;
  macro_block_id_.reset();
  block_index_ = -1;
  micro_info_.reset();
  cache_handle_.reset();
  io_handle_.reset();
  try_release_loaded_index_block();
  allocator_ = nullptr;
}

int ObMicroBlockDataHandle::get_data_block_data(
    ObMacroBlockReader &block_reader,
    ObMicroBlockData &block_data)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_loaded_block_data(block_data))) {
    if (THIS_WORKER.get_timeout_remain() <= 0) {
      // already timeout, don't retry
      LOG_WARN("get data block data already timeout", K(ret), K(THIS_WORKER.get_timeout_remain()));
    } else {
      //try sync io
      ObMicroBlockId micro_block_id;
      micro_block_id.macro_id_ = macro_block_id_;
      micro_block_id.offset_ = micro_info_.offset_;
      micro_block_id.size_ = micro_info_.size_;
      if (OB_FAIL(ObStorageCacheSuite::get_instance().get_block_cache().load_block(
          micro_block_id,
          des_meta_,
          &block_reader,
          block_data,
          nullptr))) {
        LOG_WARN("Fail to load micro block, ", K(ret), K_(tenant_id), K_(macro_block_id), K_(micro_info));
      }
    }
  }
  return ret;
}

int ObMicroBlockDataHandle::get_index_block_data(ObMicroBlockData &index_block)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_loaded_block_data(index_block))) {
    try_release_loaded_index_block();
    if (THIS_WORKER.get_timeout_remain() <= 0) {
      LOG_WARN("get index block data already timeout", K(ret), K(THIS_WORKER.get_timeout_remain()));
      // already timeout, don't retry
    } else {
      //try sync io
      ObMicroBlockId micro_block_id;
      micro_block_id.macro_id_ = macro_block_id_;
      micro_block_id.offset_ = micro_info_.offset_;
      micro_block_id.size_ = micro_info_.size_;
      is_loaded_index_block_ = true;
      if (OB_FAIL(ObStorageCacheSuite::get_instance().get_index_block_cache().load_block(
          micro_block_id,
          des_meta_,
          nullptr,
          loaded_index_block_data_,
          allocator_))) {
        LOG_WARN("Fail to load index micro block", K(ret), K_(macro_block_id), K(micro_block_id));
        try_release_loaded_index_block();
      } else {
        index_block = loaded_index_block_data_;
      }
    }
  }
  return ret;
}

int ObMicroBlockDataHandle::get_cached_index_block_data(ObMicroBlockData &index_block)
{
  int ret = OB_SUCCESS;
  if (ObSSTableMicroBlockState::IN_BLOCK_CACHE != block_state_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to get block data, unexpected block state", K(ret), K(block_state_));
  } else {
    const ObMicroBlockData *pblock = NULL;
    if (NULL == (pblock = cache_handle_.get_block_data())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Fail to get cache block, ", K(ret));
    } else {
      index_block = *pblock;
    }
  }
  return ret;
}

int ObMicroBlockDataHandle::get_loaded_block_data(ObMicroBlockData &block_data)
{
  int ret = OB_SUCCESS;
  const ObMicroBlockData *pblock = NULL;
  const char *io_buf = NULL;

  if (ObSSTableMicroBlockState::IN_BLOCK_CACHE == block_state_) {
    if (NULL == (pblock = cache_handle_.get_block_data())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Fail to get cache block, ", K(ret));
    } else {
      block_data = *pblock;
    }
  } else if (ObSSTableMicroBlockState::IN_BLOCK_IO == block_state_) {
    const int64_t timeout_ms = max(THIS_WORKER.get_timeout_remain() / 1000, 0);
    if (is_loaded_index_block_ && loaded_index_block_data_.is_valid()) {
      LOG_DEBUG("Use sync loaded index block data", K_(macro_block_id),
          K(loaded_index_block_data_), K_(io_handle));
      block_data = loaded_index_block_data_;
    } else if (OB_FAIL(io_handle_.wait(timeout_ms))) {
      LOG_WARN("Fail to wait micro block io, ", K(ret));
    } else if (NULL == (io_buf = io_handle_.get_buffer())) {
      ret = OB_INVALID_IO_BUFFER;
      LOG_WARN("Fail to get block data, io may be failed, ", K(ret));
    } else {
      if (-1 == block_index_) {
        //single block io
        pblock = reinterpret_cast<const ObMicroBlockData *>(io_buf);
        block_data = *pblock;
      } else {
        //multi block io
        const ObMultiBlockIOResult *io_result = reinterpret_cast<const ObMultiBlockIOResult *>(io_buf);
        if (OB_FAIL(io_result->get_block_data(block_index_, block_data))) {
          LOG_WARN("get_block_data failed", K(ret), K_(block_index));
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected block state, ", K(ret), K_(block_state));
  }
  return ret;
}

void ObMicroBlockDataHandle::try_release_loaded_index_block()
{
  if (is_loaded_index_block_ && nullptr != allocator_ && loaded_index_block_data_.is_valid()) {
    char *data_buf = const_cast<char *>(loaded_index_block_data_.get_buf());
    char *extra_buf = const_cast<char *>(loaded_index_block_data_.get_extra_buf());
    if (OB_NOT_NULL(data_buf)) {
      allocator_->free(data_buf);
    }
    if (OB_NOT_NULL(extra_buf)) {
      allocator_->free(extra_buf);
    }
  }
  loaded_index_block_data_.reset();
  is_loaded_index_block_ = false;
}

/**
 * -------------------------------------------------------------------ObMicroBlockHandleMgr----------------------------------------------------------------------
 */
ObMicroBlockHandleMgr::ObMicroBlockHandleMgr() : allocator_() {}

void ObMicroBlockHandleMgr::reset()
{
  ObHandleMgr::reset();
  allocator_.reset();
}

int ObMicroBlockHandleMgr::init(
    const bool is_multi,
    const bool is_ordered,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr mem_attr(MTL_ID(), "MicroHandleMgr");
  if (OB_FAIL(ObHandleMgr::init(is_multi, is_ordered, allocator))) {
    LOG_WARN("Fail to init base handle mgr", K(ret));
  } else if (OB_FAIL(allocator_.init(
      lib::ObMallocAllocator::get_instance(),
      OB_MALLOC_MIDDLE_BLOCK_SIZE,
      mem_attr))) {
    LOG_WARN("Fail to init inner allocator", K(ret));
  }
  return ret;
}

int ObMicroBlockHandleMgr::get_micro_block_handle(
    const uint64_t tenant_id,
    const ObMicroIndexInfo &index_block_info,
    const bool is_data_block,
    ObMicroBlockDataHandle &micro_block_handle)
{
  int ret = OB_SUCCESS;
  const bool deep_copy_key = true;
  bool found = false;
  const MacroBlockId &macro_id = index_block_info.get_macro_id();
  const int64_t offset = index_block_info.get_block_offset();
  const int64_t size = index_block_info.get_block_size();
  const ObIndexBlockRowHeader *idx_header = index_block_info.row_header_;
  micro_block_handle.reset();
  micro_block_handle.allocator_ = &allocator_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Block handle manager is not inited", K(ret));
  } else if (OB_ISNULL(idx_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpect null index header", K(ret));
  } else {
    if (is_multi_) {
      if (is_ordered_) {
        if (tenant_id == last_handle_->tenant_id_
            && macro_id == last_handle_->macro_block_id_
            && offset == last_handle_->micro_info_.offset_
            && size == last_handle_->micro_info_.size_) {
          micro_block_handle = *last_handle_;
          found = true;
        }
      } else {
        ObMicroBlockCacheKey key(tenant_id, macro_id, offset, size);
        if (OB_FAIL(handle_cache_->get_handle(key, micro_block_handle))) {
          if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
            LOG_WARN("failed to get from handle cache", K(ret));
          }
          ret = OB_SUCCESS; // cache invalid, read block through retrograde path
        } else {
          found = true;
        }
      }
    }

    if (OB_FAIL(ret) || found) {
    } else if (OB_FAIL(idx_header->fill_micro_des_meta(deep_copy_key, micro_block_handle.des_meta_))) {
      LOG_WARN("Fail to fill micro block deserialize meta", K(ret));
    } else {
      micro_block_handle.tenant_id_ = tenant_id;
      micro_block_handle.macro_block_id_ = macro_id;
      micro_block_handle.micro_info_.set(static_cast<int32_t>(offset), static_cast<int32_t>(size));
      ObIMicroBlockCache *cache = nullptr;
      if (is_data_block) {
        cache = &ObStorageCacheSuite::get_instance().get_block_cache();
      } else {
        cache = &ObStorageCacheSuite::get_instance().get_index_block_cache();
      }

      if (OB_ISNULL(cache)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null block cache", K(ret), KP(cache));
      } else if (OB_FAIL(cache->get_cache_block(
          tenant_id, macro_id, offset, size, micro_block_handle.cache_handle_))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          LOG_WARN("Fail to get cache block", K(ret));
        }
      } else if (FALSE_IT(micro_block_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_CACHE)) {
      } else if (is_multi_) {
        if (is_ordered_) {
          *last_handle_ = micro_block_handle;
        } else {
          ObMicroBlockCacheKey key(tenant_id, macro_id, offset, size);
          if (OB_FAIL(handle_cache_->put_handle(key, micro_block_handle))) {
            LOG_WARN("Fail to put handle cache", K(ret), K(key));
          }
        }
      }
    }
  }
  return ret;
}

int ObMicroBlockHandleMgr::put_micro_block_handle(
    const uint64_t tenant_id,
    const blocksstable::MacroBlockId &macro_id,
    const blocksstable::ObIndexBlockRowHeader &idx_header,
    ObMicroBlockDataHandle &micro_block_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "block handle mgr is not inited", K(ret));
  } else if (is_multi_) {
    if (is_ordered_) {
      *last_handle_ = micro_block_handle;
    } else {
      const int64_t offset = idx_header.get_block_offset();
      const int64_t size = idx_header.get_block_size();
      ObMicroBlockCacheKey key(tenant_id, macro_id, offset, size);
      if (OB_FAIL(handle_cache_->put_handle(key, micro_block_handle))) {
        STORAGE_LOG(WARN, "failed to put handle cache", K(ret), K(key));
      }
    }
  }
  return ret;
}

int ObMicroBlockHandleMgr::reset_handle_cache()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "block handle mgr is not inited", K(ret));
  } else if (is_multi_) {
    if (is_ordered_) {
      last_handle_->reset();
    } else {
      handle_cache_->reset_handles();
    }
  }
  return ret;
}

}
}
