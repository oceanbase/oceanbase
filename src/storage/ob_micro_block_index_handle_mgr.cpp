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

#include "ob_micro_block_index_handle_mgr.h"
#include "blocksstable/ob_storage_cache_suite.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace storage {
/**
 * ---------------------------------------------------------ObMicroBlockIndexHandle---------------------------------------------------
 */
ObMicroBlockIndexHandle::ObMicroBlockIndexHandle()
    : table_id_(0), block_ctx_(), block_index_mgr_(NULL), cache_handle_(), io_handle_()
{}

ObMicroBlockIndexHandle::~ObMicroBlockIndexHandle()
{}

void ObMicroBlockIndexHandle::reset()
{
  table_id_ = 0;
  block_ctx_.reset();
  block_index_mgr_ = NULL;
  cache_handle_.reset();
  io_handle_.reset();
}

int ObMicroBlockIndexHandle::search_blocks(const ObStoreRange& range, const bool is_left_border,
    const bool is_right_border, ObIArray<ObMicroBlockInfo>& infos, const ObIArray<ObRowkeyObjComparer*>* cmp_funcs)
{
  int ret = OB_SUCCESS;
  const ObMicroBlockIndexMgr* block_idx_mgr = NULL;
  if (OB_FAIL(get_block_index_mgr(block_idx_mgr))) {
    if (OB_UNLIKELY(0 == table_id_)) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "The ObMicroBlockIndexHandle has not been inited, ", K(ret), K_(table_id), K_(block_ctx));
    } else if (OB_FAIL(ObStorageCacheSuite::get_instance().get_micro_index_cache().get_micro_infos(
                   table_id_, block_ctx_, range, is_left_border, is_right_border, infos))) {
      STORAGE_LOG(WARN, "failed to get micro infos", K(ret));
    }
  } else if (OB_FAIL(block_idx_mgr->search_blocks(range, is_left_border, is_right_border, infos, cmp_funcs))) {
    if (OB_BEYOND_THE_RANGE != ret) {
      STORAGE_LOG(WARN, "Fail to search blocks, ", K(ret), K(range));
    }
  }
  return ret;
}

int ObMicroBlockIndexHandle::search_blocks(
    const ObStoreRowkey& rowkey, ObMicroBlockInfo& info, const ObIArray<ObRowkeyObjComparer*>* cmp_funcs)
{
  int ret = OB_SUCCESS;
  const ObMicroBlockIndexMgr* block_idx_mgr = NULL;
  if (OB_FAIL(get_block_index_mgr(block_idx_mgr))) {
    if (OB_UNLIKELY(0 == table_id_)) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "The ObMicroBlockIndexHandle has not been inited, ", K(ret), K_(table_id), K_(block_ctx));
    } else if (OB_FAIL(ObStorageCacheSuite::get_instance().get_micro_index_cache().get_micro_infos(
                   table_id_, block_ctx_, rowkey, info))) {
      if (OB_BEYOND_THE_RANGE != ret) {
        STORAGE_LOG(WARN, "Fail to get micro infos, ", K(ret));
      }
    }
  } else if (OB_FAIL(block_idx_mgr->search_blocks(rowkey, info, cmp_funcs))) {
    STORAGE_LOG(WARN, "failed to search blocks", K(ret), K(rowkey));
  }
  return ret;
}

int ObMicroBlockIndexHandle::get_block_index_mgr(const ObMicroBlockIndexMgr*& block_idx_mgr)
{
  int ret = OB_SUCCESS;
  if (NULL == block_index_mgr_) {
    if (OB_UNLIKELY(NULL == (block_index_mgr_ = cache_handle_.get_index_mgr()))) {
      const int64_t timeout_ms = max(THIS_WORKER.get_timeout_remain() / 1000, 0);
      if (OB_FAIL(io_handle_.wait(timeout_ms))) {
        STORAGE_LOG(WARN, "Fail to wait io complete, ", K(ret));
      } else if (NULL == (block_index_mgr_ = reinterpret_cast<const ObMicroBlockIndexMgr*>(io_handle_.get_buffer()))) {
        ret = OB_IO_ERROR;
        STORAGE_LOG(WARN, "Fail to get micro block index from io handle, ", K(ret), K_(io_handle));
      }
    }
  }
  block_idx_mgr = block_index_mgr_;
  return ret;
}

/**
 * -----------------------------------------------------------ObMicroBlockIndexMgr-----------------------------------------------------------
 */

int ObMicroBlockIndexHandleMgr::get_block_index_handle(const uint64_t table_id,
    const blocksstable::ObMacroBlockCtx& block_ctx, const int64_t file_id, const common::ObQueryFlag& flag,
    blocksstable::ObStorageFile* pg_file, ObMicroBlockIndexHandle& block_idx_handle)
{
  int ret = OB_SUCCESS;
  bool found = false;
  block_idx_handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "index handle mgr is not inited", K(ret));
  } else if (is_multi_) {
    if (is_ordered_) {
      if (table_id == last_handle_->table_id_ &&
          block_ctx.get_macro_block_id() == last_handle_->block_ctx_.get_macro_block_id()) {
        block_idx_handle = *last_handle_;
        found = true;
      }
    } else if (OB_FAIL(handle_cache_->get_handle(block_ctx.get_macro_block_id(), block_idx_handle))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        STORAGE_LOG(WARN, "failed to get from IndexHandleCache", K(ret));
      }
    } else {
      found = true;
    }
  }
  if (!found) {
    if (OB_FAIL(ObStorageCacheSuite::get_instance().get_micro_index_cache().get_cache_block_index(
            table_id, block_ctx.get_macro_block_id(), file_id, block_idx_handle.cache_handle_))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "Fail to get cache block index, ", K(ret));
      }
      ret = OB_SUCCESS;
      if (FALSE_IT(block_idx_handle.io_handle_.set_file(pg_file))) {
      } else if (OB_FAIL(ObStorageCacheSuite::get_instance().get_micro_index_cache().prefetch(
                     table_id, block_ctx, pg_file, block_idx_handle.io_handle_, flag))) {
        STORAGE_LOG(WARN, "Fail to prefetch micro block index, ", K(ret));
      }
    } else {
      found = true;
    }

    if (OB_SUCC(ret)) {
      block_idx_handle.table_id_ = table_id;
      block_idx_handle.block_ctx_ = block_ctx;
      if (is_multi_) {
        if (is_ordered_) {
          *last_handle_ = block_idx_handle;
        } else if (found && OB_FAIL(handle_cache_->put_handle(block_ctx.get_macro_block_id(), block_idx_handle))) {
          STORAGE_LOG(WARN, "failed to put handle", K(ret), K(block_ctx));
        }
      }
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
