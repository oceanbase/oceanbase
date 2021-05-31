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

#include "ob_micro_block_handle_mgr.h"
#include "blocksstable/ob_storage_cache_suite.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace storage {
/**
 * --------------------------------------------------------------ObMicroBlockDataHandle-----------------------------------------------------------------
 */
ObMicroBlockDataHandle::ObMicroBlockDataHandle()
    : table_id_(0),
      block_ctx_(),
      block_state_(ObSSTableMicroBlockState::UNKNOWN_STATE),
      block_index_(-1),
      micro_info_(),
      cache_handle_(),
      io_handle_()
{}

ObMicroBlockDataHandle::~ObMicroBlockDataHandle()
{}

void ObMicroBlockDataHandle::reset()
{
  block_state_ = ObSSTableMicroBlockState::UNKNOWN_STATE;
  table_id_ = 0;
  block_ctx_.reset();
  block_index_ = -1;
  micro_info_.reset();
  cache_handle_.reset();
  io_handle_.reset();
}

int ObMicroBlockDataHandle::get_block_data(
    ObMacroBlockReader& block_reader, ObStorageFile* storage_file, ObMicroBlockData& block_data)
{
  int ret = OB_SUCCESS;
  const ObMicroBlockData* pblock = NULL;
  const char* io_buf = NULL;

  if (ObSSTableMicroBlockState::IN_BLOCK_CACHE == block_state_) {
    if (NULL == (pblock = cache_handle_.get_block_data())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Fail to get cache block, ", K(ret));
    } else {
      block_data = *pblock;
    }
  } else if (ObSSTableMicroBlockState::IN_BLOCK_IO == block_state_) {
    const int64_t timeout_ms = max(THIS_WORKER.get_timeout_remain() / 1000, 0);
    if (OB_FAIL(io_handle_.wait(timeout_ms))) {
      STORAGE_LOG(WARN, "Fail to wait micro block io, ", K(ret));
    } else if (NULL == (io_buf = io_handle_.get_buffer())) {
      ret = OB_INVALID_IO_BUFFER;
      STORAGE_LOG(WARN, "Fail to get block data, io may be failed, ", K(ret));
    } else {
      if (-1 == block_index_) {
        // single block io
        pblock = reinterpret_cast<const ObMicroBlockData*>(io_buf);
        block_data = *pblock;
      } else {
        // multi block io
        const ObMultiBlockIOResult* io_result = reinterpret_cast<const ObMultiBlockIOResult*>(io_buf);
        if (OB_FAIL(io_result->get_block_data(block_index_, block_data))) {
          STORAGE_LOG(WARN, "get_block_data failed", K(ret), K_(block_index));
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected block state, ", K(ret), K_(block_state));
  }

  if (OB_FAIL(ret)) {
    // try sync io
    if (OB_FAIL(ObStorageCacheSuite::get_instance().get_block_cache().load_cache_block(
            block_reader, table_id_, block_ctx_, micro_info_.offset_, micro_info_.size_, storage_file, block_data))) {
      STORAGE_LOG(
          WARN, "Fail to load micro block, ", K(ret), K_(table_id), K_(block_ctx), K_(micro_info), KP(storage_file));
    }
  }
  return ret;
}

/**
 * -------------------------------------------------------------------ObMicroBlockHandleMgr----------------------------------------------------------------------
 */
int ObMicroBlockHandleMgr::get_micro_block_handle(const uint64_t table_id,
    const blocksstable::ObMacroBlockCtx& block_ctx, const int64_t file_id, const int64_t offset, const int64_t size,
    const int64_t index, ObMicroBlockDataHandle& micro_block_handle)
{
  int ret = OB_SUCCESS;
  bool found = false;
  micro_block_handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "block handle mgr is not inited", K(ret));
  } else if (is_multi_) {
    if (is_ordered_) {
      if (table_id == last_handle_->table_id_ &&
          block_ctx.get_macro_block_id() == last_handle_->block_ctx_.get_macro_block_id() &&
          offset == last_handle_->micro_info_.offset_ && size == last_handle_->micro_info_.size_) {
        micro_block_handle = *last_handle_;
        found = true;
      }
    } else {
      ObMicroBlockCacheKey key(table_id, block_ctx.get_macro_block_id(), file_id, offset, size);
      if (OB_FAIL(handle_cache_->get_handle(key, micro_block_handle))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          STORAGE_LOG(WARN, "failed to get from handle cache", K(ret));
        }
      } else {
        found = true;
      }
    }
  }
  if (!found) {
    if (OB_FAIL(ObStorageCacheSuite::get_instance().get_block_cache().get_cache_block(
            table_id, block_ctx.get_macro_block_id(), file_id, offset, size, micro_block_handle.cache_handle_))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "Fail to get cache block, ", K(ret));
      }
    } else {
      micro_block_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_CACHE;
      micro_block_handle.table_id_ = table_id;
      micro_block_handle.block_ctx_ = block_ctx;
      micro_block_handle.micro_info_.set(static_cast<int32_t>(offset), static_cast<int32_t>(size), index);
      if (is_multi_) {
        if (is_ordered_) {
          *last_handle_ = micro_block_handle;
        } else {
          ObMicroBlockCacheKey key(table_id, block_ctx.get_macro_block_id(), file_id, offset, size);
          if (OB_FAIL(handle_cache_->put_handle(key, micro_block_handle))) {
            STORAGE_LOG(WARN, "failed to put handle cache", K(ret), K(key));
          }
        }
      }
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
