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

#include "storage/blocksstable/ob_micro_block_info.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "share/cache/ob_kvcache_pointer_swizzle.h"
#include "ob_micro_block_handle_mgr.h"


using namespace oceanbase::common;
using namespace oceanbase::blocksstable;


namespace oceanbase {
namespace storage {
/**
 * --------------------------------------------------------------ObMicroBlockDataHandle-----------------------------------------------------------------
 */
ObMicroBlockDataHandle::ObMicroBlockDataHandle()
  : tenant_id_(OB_INVALID_TENANT_ID),
    macro_block_id_(),
    block_state_(ObSSTableMicroBlockState::UNKNOWN_STATE),
    block_index_(-1),
    micro_info_(),
    des_meta_(),
    encrypt_key_(),
    cache_handle_(),
    io_handle_(),
    handle_mgr_(nullptr),
    allocator_(nullptr),
    loaded_block_data_(),
    is_loaded_block_(false)
{
  des_meta_.encrypt_key_ = encrypt_key_;
}

ObMicroBlockDataHandle::~ObMicroBlockDataHandle()
{
  reset();
}

void ObMicroBlockDataHandle::init(
  const uint64_t tenant_id,
  const MacroBlockId &macro_id,
  const int64_t offset,
  const int64_t size,
  ObMicroBlockHandleMgr *handle_mgr)
{
  tenant_id_ = tenant_id;
  macro_block_id_ = macro_id;
  micro_info_.set(offset, size);
  handle_mgr_ = handle_mgr;
}

void ObMicroBlockDataHandle::reset()
{
  if (nullptr != handle_mgr_) {
    handle_mgr_->dec_hold_size(*this);
    handle_mgr_ = nullptr;
  }
  block_state_ = ObSSTableMicroBlockState::UNKNOWN_STATE;
  tenant_id_ = OB_INVALID_TENANT_ID;
  macro_block_id_.reset();
  block_index_ = -1;
  micro_info_.reset();
  cache_handle_.reset();
  io_handle_.reset();
  try_release_loaded_block();
  allocator_ = nullptr;
}

bool ObMicroBlockDataHandle::match(const blocksstable::MacroBlockId &macro_id,
                                   const int32_t offset,
                                   const int32_t size) const
{
  return offset == micro_info_.offset_
      && macro_id == macro_block_id_
      && size == micro_info_.size_
      && MTL_ID() == tenant_id_;
}

int ObMicroBlockDataHandle::get_micro_block_data(
    ObMacroBlockReader *macro_reader,
    ObMicroBlockData &block_data,
    const bool is_data_block)
{
  int ret = OB_SUCCESS;
  if (ObSSTableMicroBlockState::NEED_SYNC_IO == block_state_ || OB_FAIL(get_loaded_block_data(block_data))) {
    if (is_loaded_block_ && loaded_block_data_.is_valid()) {
      LOG_DEBUG("Use sync loaded index block data", K(is_data_block), K_(macro_block_id),
                K(loaded_block_data_), K_(io_handle));
      block_data = loaded_block_data_;
    } else {
      try_release_loaded_block();
      if (THIS_WORKER.get_timeout_remain() <= 0) {
        // already timeout, don't retry
        LOG_INFO("get data block data already timeout", K(ret), K(THIS_WORKER.get_timeout_remain()));
      } else {
        //try sync io
        ObMicroBlockId micro_block_id;
        micro_block_id.macro_id_ = macro_block_id_;
        micro_block_id.offset_ = micro_info_.offset_;
        micro_block_id.size_ = micro_info_.size_;
        is_loaded_block_ = true;
        if (OB_FAIL(ObStorageCacheSuite::get_instance().get_micro_block_cache(is_data_block).load_block(
                    micro_block_id,
                    des_meta_,
                    macro_reader,
                    loaded_block_data_,
                    allocator_))) {
          LOG_WARN("Fail to load micro block", K(ret), K_(tenant_id), K_(macro_block_id), K_(micro_info));
          try_release_loaded_block();
        } else {
          io_handle_.reset();
          block_state_ = ObSSTableMicroBlockState::NEED_SYNC_IO;
          block_data = loaded_block_data_;
        }
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
      LOG_WARN("Fail to get cache block", K(ret));
    } else {
      index_block = *pblock;
    }
  }
  return ret;
}

int64_t ObMicroBlockDataHandle::get_handle_size() const
{
  int64_t size = 0;

  if (ObSSTableMicroBlockState::IN_BLOCK_CACHE == block_state_) {
    size = cache_handle_.get_block_size();
  } else if (ObSSTableMicroBlockState::IN_BLOCK_IO == block_state_) {
    size = micro_info_.size_;
  }

  return size;
}

ObMicroBlockDataHandle & ObMicroBlockDataHandle::operator=(const ObMicroBlockDataHandle &other)
{
  if (this != &other) {
    reset();
    tenant_id_ = other.tenant_id_;
    macro_block_id_ = other.macro_block_id_;
    block_state_ = other.block_state_;
    block_index_ = other.block_index_;
    micro_info_ = other.micro_info_;
    des_meta_ = other.des_meta_;
    des_meta_.encrypt_key_ = encrypt_key_;
    MEMCPY(encrypt_key_, other.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
    cache_handle_ = other.cache_handle_;
    io_handle_ = other.io_handle_;
    // TODO @lvling : do not copy handle_mgr_
    allocator_ = other.allocator_;
    loaded_block_data_ = other.loaded_block_data_;
    is_loaded_block_ = other.is_loaded_block_;
  }
  return *this;
}

int ObMicroBlockDataHandle::get_loaded_block_data(ObMicroBlockData &block_data)
{
  int ret = OB_SUCCESS;
  const ObMicroBlockData *pblock = NULL;
  const char *io_buf = NULL;
  if (ObSSTableMicroBlockState::IN_BLOCK_CACHE == block_state_) {
    if (NULL == (pblock = cache_handle_.get_block_data())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Fail to get cache block", K(ret));
    } else {
      block_data = *pblock;
    }
  } else if (ObSSTableMicroBlockState::IN_BLOCK_IO == block_state_) {
    if (OB_FAIL(io_handle_.wait())) {
      LOG_WARN("Fail to wait micro block io", K(ret));
    } else if (NULL == (io_buf = io_handle_.get_buffer())) {
      ret = OB_INVALID_IO_BUFFER;
      LOG_WARN("Fail to get block data, io may be failed", K(ret));
    } else {
      if (-1 == block_index_) {
        //single block io
        pblock = &reinterpret_cast<const ObMicroBlockCacheValue *>(io_buf)->get_block_data();
        block_data = *pblock;
      } else {
        //multi block io
        const ObMultiBlockIOResult *io_result = reinterpret_cast<const ObMultiBlockIOResult *>(io_buf);
        if (OB_FAIL(io_result->get_block_data(block_index_, micro_info_, block_data))) {
          LOG_WARN("get_block_data failed", K(ret), K_(block_index), K_(micro_info));
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected block state", K(ret), K_(block_state));
  }
  return ret;
}

void ObMicroBlockDataHandle::try_release_loaded_block()
{
  if (is_loaded_block_ && nullptr != allocator_ && loaded_block_data_.is_valid()) {
    char *data_buf = const_cast<char *>(loaded_block_data_.get_buf());
    if (OB_NOT_NULL(data_buf)) {
      allocator_->free(data_buf);
    }
  }
  loaded_block_data_.reset();
  is_loaded_block_ = false;
}

/**
 * -------------------------------------------------------------------ObCacheMemController----------------------------------------------------------------------
 */
ObCacheMemController::ObCacheMemController()
  : update_limit_count_(0),
    data_block_submit_io_size_(0),
    data_block_use_cache_limit_(DEFAULT_DATA_BLOCK_USE_CACHE_LIMIT),
    hold_limit_(HOLD_LIMIT_BASE),
    current_hold_size_(0),
    use_data_block_cache_(true)
{}

ObCacheMemController::~ObCacheMemController()
{}

void ObCacheMemController::init(const bool enable_limit)
{
  if (!enable_limit) {
    need_sync_io_func = &ObCacheMemController::need_sync_io_nlimit;
    reach_hold_limit_func = &ObCacheMemController::reach_hold_limit_nlimit;
    update_data_block_io_size_func = &ObCacheMemController::update_data_block_io_size_nlimit;
  } else {
    need_sync_io_func = &ObCacheMemController::need_sync_io_limit;
    reach_hold_limit_func = &ObCacheMemController::reach_hold_limit_limit;
    update_data_block_io_size_func = &ObCacheMemController::update_data_block_io_size_limit;
    LOG_INFO("Start cache memory controller", K(enable_limit), KPC(this));
  }
}

void ObCacheMemController::reset()
{
  update_limit_count_ = 0;
  data_block_submit_io_size_ = 0;
  data_block_use_cache_limit_ = DEFAULT_DATA_BLOCK_USE_CACHE_LIMIT;
  hold_limit_ = HOLD_LIMIT_BASE;
  current_hold_size_ = 0;
  use_data_block_cache_ = true;
}

void ObCacheMemController::add_hold_size(const int64_t handle_size)
{
  current_hold_size_ += handle_size;
}

void ObCacheMemController::dec_hold_size(const int64_t handle_size)
{
  current_hold_size_ -= handle_size;
}

bool ObCacheMemController::need_sync_io_limit(
    const ObQueryFlag &query_flag,
    ObMicroBlockDataHandle &micro_block_handle,
    blocksstable::ObIMicroBlockCache *cache,
    ObFIFOAllocator &block_io_allocator)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_limit(query_flag))) {
    LOG_WARN("Fail to update limit", K(ret));
  }
  if (current_hold_size_ > hold_limit_) {
    LOG_DEBUG("Reach hold limit, submit sync io", K(current_hold_size_), K(hold_limit_));
    micro_block_handle.block_state_ = ObSSTableMicroBlockState::NEED_SYNC_IO;
    micro_block_handle.allocator_ = &block_io_allocator;
    cache->cache_bypass();
    bret = true;
  }
  return bret;
}

bool ObCacheMemController::reach_hold_limit_limit() const
{
  return current_hold_size_ >= hold_limit_;
}

void ObCacheMemController::update_data_block_io_size_limit(
    const int64_t block_size,
    const bool is_data_block,
    const bool use_cache)
{
  if (is_data_block && use_cache) {
    data_block_submit_io_size_ += block_size;
    use_data_block_cache_ = data_block_submit_io_size_ <= data_block_use_cache_limit_;
  }
}

int ObCacheMemController::update_limit(const ObQueryFlag &query_flag)
{
  int ret = OB_SUCCESS;
  if (0 == (++update_limit_count_ % UPDATE_INTERVAL)) {
    uint64_t tenant_id = MTL_ID();
    int64_t tenant_free_memory = lib::get_tenant_memory_limit(tenant_id) - lib::get_tenant_memory_hold(tenant_id);
    hold_limit_ = HOLD_LIMIT_BASE + tenant_free_memory / 1024 ;
    int64_t cache_washable_size = 0;
    if (query_flag.is_use_block_cache()) {
      if (OB_FAIL(ObKVGlobalCache::get_instance().get_washable_size(tenant_id, cache_washable_size))) {
        LOG_WARN("Fail to get kvcache washable size", K(ret));
      } else {
        data_block_use_cache_limit_ = tenant_free_memory / 5 + cache_washable_size / 10;
        use_data_block_cache_ = data_block_submit_io_size_ <= data_block_use_cache_limit_;
      }
    }
    LOG_DEBUG("Update limit details", K(tenant_id), K(tenant_free_memory), K(cache_washable_size),
                                 K(query_flag.is_use_block_cache()), KPC(this));
  }
  return ret;
}

/**
 * -------------------------------------------------------------------ObMicroBlockHandleMgr----------------------------------------------------------------------
 */
ObMicroBlockHandleMgr::ObMicroBlockHandleMgr()
  : data_block_cache_(nullptr),
    index_block_cache_(nullptr),
    table_store_stat_(nullptr),
    query_flag_(nullptr),
    block_io_allocator_(),
    cache_mem_ctrl_(),
    is_inited_(false)
{
}

ObMicroBlockHandleMgr::~ObMicroBlockHandleMgr()
{
  reset();
}

void ObMicroBlockHandleMgr::reset()
{
  if (is_inited_) {
    is_inited_ = false;
    block_io_allocator_.reset();
    cache_mem_ctrl_.reset();
  }
  data_block_cache_ = nullptr;
  index_block_cache_ = nullptr;
  table_store_stat_ = nullptr;
  query_flag_ = nullptr;
}

int ObMicroBlockHandleMgr::init(const bool enable_prefetch_limiting, ObTableScanStoreStat &stat, ObQueryFlag &query_flag)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr mem_attr(MTL_ID(), "MicroBlockIO");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The micro block handle mgr has been inited", K(ret));
  } else if (OB_FAIL(block_io_allocator_.init(nullptr, OB_MALLOC_MIDDLE_BLOCK_SIZE, mem_attr))) {
    LOG_WARN("Fail to init data block io allocator", K(ret));
  } else {
    data_block_cache_ = &(OB_STORE_CACHE.get_block_cache());
    index_block_cache_ = &(OB_STORE_CACHE.get_index_block_cache());
    table_store_stat_ = &stat;
    query_flag_ = &query_flag;
    cache_mem_ctrl_.init(enable_prefetch_limiting);
    is_inited_ = true;
  }
  return ret;
}

int ObMicroBlockHandleMgr::get_micro_block_handle(
    ObMicroIndexInfo &index_block_info,
    const bool is_data_block,
    const bool need_submit_io,
    const bool use_multi_block_prefetch,
    ObMicroBlockDataHandle &micro_block_handle,
    int16_t cur_level)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const MacroBlockId &macro_id = index_block_info.get_macro_id();
  const int64_t offset = index_block_info.get_block_offset();
  const int64_t size = index_block_info.get_block_size();
  const ObIndexBlockRowHeader *idx_header = index_block_info.row_header_;
  ObPointerSwizzleNode *ps_node = index_block_info.ps_node_;
  micro_block_handle.reset();
  ObIMicroBlockCache *cache = is_data_block ? data_block_cache_ : index_block_cache_;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Block handle manager is not inited", K(ret));
  } else if (OB_ISNULL(idx_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpect null index header", K(ret), KP(idx_header));
  } else if (OB_FAIL(idx_header->fill_micro_des_meta(true /* deep_copy_key */, micro_block_handle.des_meta_))) {
    LOG_WARN("Fail to fill micro block deserialize meta", K(ret));
  } else if (FALSE_IT(micro_block_handle.init(tenant_id, macro_id, offset, size, this))) {
  } else if (OB_LIKELY(nullptr != ps_node)
      && OB_SUCC(ps_node->access_mem_ptr(micro_block_handle.cache_handle_))) {
    // get data / index block cache with direct memory pointer
    micro_block_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_CACHE;
    cache->cache_hit(table_store_stat_->block_cache_hit_cnt_);
    LOG_DEBUG("Access memory pointer successfully", K(tenant_id), K(macro_id), K(offset), KPC(ps_node),
                                                    K(micro_block_handle.cache_handle_), K(cur_level));
  } else if (OB_FAIL(cache->get_cache_block(tenant_id, macro_id, offset, size, micro_block_handle.cache_handle_))) {
    // get data / index block cache from disk
    if (!need_submit_io) {
    } else if (cache_mem_ctrl_.need_sync_io(*query_flag_, micro_block_handle, cache, block_io_allocator_)) {
    } else if (OB_FAIL(submit_async_io(cache,
                                       tenant_id,
                                       index_block_info,
                                       is_data_block,
                                       use_multi_block_prefetch,
                                       micro_block_handle))) {
      LOG_WARN("Fail to submit async io for prefetch", K(ret), K(index_block_info), K(micro_block_handle));
    }
  } else {
    // get data / index block cache from cache
    LOG_DEBUG("block cache hit", K(is_data_block), K(tenant_id), K(macro_id), K(offset), K(size), K(cur_level));
    micro_block_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_CACHE;
    cache_mem_ctrl_.add_hold_size(micro_block_handle.get_handle_size());
    cache->cache_hit(table_store_stat_->block_cache_hit_cnt_);
    if (nullptr == ps_node) {
    } else if (OB_FAIL(ps_node->swizzle(micro_block_handle.cache_handle_))) {
      LOG_WARN("Fail to swizzle", K(is_data_block), K(tenant_id), K(macro_id), K(offset), K(size), K(cur_level),
                                  K(micro_block_handle), KP(ps_node), KPC(ps_node));
    }
  }
  return ret;
}

int ObMicroBlockHandleMgr::prefetch_multi_data_block(
    const ObMicroIndexInfo *micro_data_infos,
    ObMicroBlockDataHandle *micro_data_handles,
    const int64_t max_micro_handle_cnt,
    const int64_t max_prefetch_idx,
    const ObMultiBlockIOParam &multi_io_params)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObMacroBlockHandle macro_handle;
  if (OB_UNLIKELY(!multi_io_params.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected io params", K(ret), K(multi_io_params));
  } else {
    const MacroBlockId &macro_id = micro_data_infos[multi_io_params.prefetch_idx_[0] % max_micro_handle_cnt].get_macro_id();
    if (1 == multi_io_params.count()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < multi_io_params.count(); i++) {
        const ObMicroIndexInfo &index_info = micro_data_infos[multi_io_params.prefetch_idx_[i] % max_micro_handle_cnt];
        if (OB_FAIL(data_block_cache_->prefetch(tenant_id, macro_id, index_info, true,
                                                macro_handle, &block_io_allocator_))) {
          LOG_WARN("Fail to prefetch micro block", K(ret), K(index_info), K(macro_handle));
        } else {
          ObMicroBlockDataHandle &micro_handle = micro_data_handles[multi_io_params.prefetch_idx_[i] % max_micro_handle_cnt];
          micro_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_IO;
          cache_mem_ctrl_.add_hold_size(micro_handle.get_handle_size());
          micro_handle.io_handle_ = macro_handle;
          micro_handle.allocator_ = &block_io_allocator_;
          cache_mem_ctrl_.update_data_block_io_size(index_info.get_block_size(), true, true);
        }
      }
    } else if (OB_FAIL(data_block_cache_->prefetch_multi_block(
                tenant_id,
                macro_id,
                multi_io_params,
                true, /* use_cache */
                macro_handle))) {
      LOG_WARN("Fail to prefetch multi blocks", K(ret), K(multi_io_params));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < multi_io_params.count(); i++) {
        if (multi_io_params.prefetch_idx_[i] >= max_prefetch_idx) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected prefetch idx", K(ret), K(i), K(multi_io_params.prefetch_idx_[i]), K(max_prefetch_idx));
        } else {
          ObMicroBlockDataHandle &micro_handle = micro_data_handles[multi_io_params.prefetch_idx_[i] % max_micro_handle_cnt];
          micro_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_IO;
          micro_handle.io_handle_ = macro_handle;
          micro_handle.allocator_ = &block_io_allocator_;
          micro_handle.block_index_ = i;
        }
      }
      if (OB_SUCC(ret)) {
        cache_mem_ctrl_.add_hold_size(multi_io_params.get_data_cache_size());
        cache_mem_ctrl_.update_data_block_io_size(multi_io_params.get_data_cache_size(), true, true);
      }
    }
  }
  return ret;
}

int ObMicroBlockHandleMgr::submit_async_io(
    blocksstable::ObIMicroBlockCache *cache,
    const uint64_t tenant_id,
    const ObMicroIndexInfo &index_block_info,
    const bool is_data_block,
    const bool use_multi_block_prefetch,
    ObMicroBlockDataHandle &micro_block_handle)
{
  int ret = OB_SUCCESS;
  const MacroBlockId &macro_id = index_block_info.get_macro_id();
  const int64_t size = index_block_info.get_block_size();
  cache->cache_miss(table_store_stat_->block_cache_miss_cnt_);
  ObMacroBlockHandle macro_handle;
  bool is_use_block_cache = query_flag_->is_use_block_cache();
  bool use_cache = is_data_block ? is_use_block_cache && cache_mem_ctrl_.get_cache_use_flag()
                                    : is_use_block_cache;
  if (use_cache && is_data_block && use_multi_block_prefetch) {
    micro_block_handle.block_state_ = ObSSTableMicroBlockState::NEED_MULTI_IO;
    ret = OB_SUCCESS;
    // continue and use prefetch in batch later
  } else if (OB_FAIL(cache->prefetch(tenant_id, macro_id, index_block_info, use_cache,
                              macro_handle, &block_io_allocator_))) {
    LOG_WARN("Fail to prefetch micro block", K(ret), K(index_block_info), K(macro_handle),
                                              K(micro_block_handle));
  } else {
    micro_block_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_IO;
    cache_mem_ctrl_.add_hold_size(micro_block_handle.get_handle_size());
    micro_block_handle.io_handle_ = macro_handle;
    micro_block_handle.allocator_ = &block_io_allocator_;
    cache_mem_ctrl_.update_data_block_io_size(size, is_data_block, use_cache);
  }
  return ret;
}

void ObMicroBlockHandleMgr::dec_hold_size(ObMicroBlockDataHandle &handle)
{
  cache_mem_ctrl_.dec_hold_size(handle.get_handle_size());
}

bool ObMicroBlockHandleMgr::reach_hold_limit() const
{
  return cache_mem_ctrl_.reach_hold_limit();
}

}
}
