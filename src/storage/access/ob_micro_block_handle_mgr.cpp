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
    need_release_data_buf_(false),
    is_loaded_block_(false)
{
  des_meta_.encrypt_key_ = encrypt_key_;
}

ObMicroBlockDataHandle::~ObMicroBlockDataHandle()
{
  reset();
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
      LOG_DEBUG("Use sync loaded index block data", K_(macro_block_id),
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
          LOG_WARN("Fail to load micro block, ", K(ret), K_(tenant_id), K_(macro_block_id), K_(micro_info));
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
      LOG_WARN("Fail to get cache block, ", K(ret));
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
      LOG_WARN("Fail to wait micro block io, ", K(ret));
    } else if (NULL == (io_buf = io_handle_.get_buffer())) {
      ret = OB_INVALID_IO_BUFFER;
      LOG_WARN("Fail to get block data, io may be failed, ", K(ret));
    } else {
      if (-1 == block_index_) {
        //single block io
        pblock = &reinterpret_cast<const ObMicroBlockCacheValue *>(io_buf)->get_block_data();
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
 * -------------------------------------------------------------------ObMicroBlockHandleMgr----------------------------------------------------------------------
 */
ObMicroBlockHandleMgr::ObMicroBlockHandleMgr()
  : data_block_cache_(nullptr),
    index_block_cache_(nullptr),
    table_store_stat_(nullptr),
    query_flag_(nullptr),
    block_io_allocator_(),
    update_limit_count_(0),
    data_block_submit_io_size_(0),
    data_block_use_cache_limit_(DEFAULT_DATA_BLOCK_USE_CACHE_LIMIT),
    hold_limit_(HOLD_LIMIT_BASE),
    current_hold_size_(0),
    use_data_block_cache_(true),
    enable_limit_(true),
    is_inited_(false)
{
}

ObMicroBlockHandleMgr::~ObMicroBlockHandleMgr()
{
  reset();
}

void ObMicroBlockHandleMgr::reset()
{
  is_inited_ = false;
  data_block_cache_ = nullptr;
  index_block_cache_ = nullptr;
  table_store_stat_ = nullptr;
  query_flag_ = nullptr;
  update_limit_count_ = 0;
  data_block_submit_io_size_ = 0;
  data_block_use_cache_limit_ = DEFAULT_DATA_BLOCK_USE_CACHE_LIMIT;
  hold_limit_ = HOLD_LIMIT_BASE;
  current_hold_size_ = 0;
  use_data_block_cache_ = true;
  enable_limit_ = true;
  block_io_allocator_.reset();
}

int ObMicroBlockHandleMgr::init(const bool enable_prefetch_limiting, ObTableStoreStat &stat, ObQueryFlag &query_flag)
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
    enable_limit_ = enable_prefetch_limiting;
    is_inited_ = true;
  }
  LOG_DEBUG("micro block handle mgr init details", K(ret), K(is_inited_), K(enable_limit_));
  return ret;
}

int ObMicroBlockHandleMgr::get_micro_block_handle(
    const ObMicroIndexInfo &index_block_info,
    const bool is_data_block,
    const bool need_submit_io,
    ObMicroBlockDataHandle &micro_block_handle)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const MacroBlockId &macro_id = index_block_info.get_macro_id();
  const int64_t offset = index_block_info.get_block_offset();
  const int64_t size = index_block_info.get_block_size();
  const ObIndexBlockRowHeader *idx_header = index_block_info.row_header_;
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
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(update_limit())) {
      LOG_WARN("Fail to update limit", K(tmp_ret));
    }

    micro_block_handle.tenant_id_ = tenant_id;
    micro_block_handle.macro_block_id_ = macro_id;
    micro_block_handle.micro_info_.set(offset, size);
    micro_block_handle.handle_mgr_ = this;

    if (enable_limit_ && need_submit_io && current_hold_size_ > hold_limit_) {
      // reach hold limit, need sync io
      LOG_DEBUG("reach hold limit, submit sync io", K(enable_limit_), K(current_hold_size_), K(hold_limit_),
                                                    K(macro_id), K(offset), K(size));
      micro_block_handle.block_state_ = ObSSTableMicroBlockState::NEED_SYNC_IO;
      micro_block_handle.allocator_ = &block_io_allocator_;
      cache_bypass(is_data_block);
    } else if (OB_FAIL(cache->get_cache_block(tenant_id, macro_id, offset, size, micro_block_handle.cache_handle_))) {
      // submit io
      LOG_DEBUG("try submit io", K(ret), K(enable_limit_), K(query_flag_->is_use_block_cache()),
          K(is_data_block), K(need_submit_io), K(tenant_id), K(macro_id), K(offset), K(size));
      if (need_submit_io) {
        cache_miss(is_data_block);
        ObMacroBlockHandle macro_handle;
        bool use_cache = is_data_block ? query_flag_->is_use_block_cache() && use_data_block_cache_
                                       : query_flag_->is_use_block_cache();
        if (OB_FAIL(cache->prefetch(tenant_id, macro_id, index_block_info, use_cache,
                                    macro_handle, &block_io_allocator_))) {
          LOG_WARN("Fail to prefetch micro block", K(ret), K(index_block_info), K(macro_handle),
                                                   K(micro_block_handle));
        } else {
          micro_block_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_IO;
          current_hold_size_ += micro_block_handle.get_handle_size();
          micro_block_handle.io_handle_ = macro_handle;
          micro_block_handle.allocator_ = &block_io_allocator_;
          micro_block_handle.need_release_data_buf_ = true;
          if (use_cache && is_data_block) {
            update_data_block_io_size(size);
          }
        }
      }
    } else {
      // hit in data / index block cache
      LOG_DEBUG("block cache hit", K(enable_limit_), K(is_data_block), K(tenant_id), K(macro_id), K(offset), K(size));
      micro_block_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_CACHE;
      current_hold_size_ += micro_block_handle.get_handle_size();
      cache_hit(is_data_block);
    }
  }

  return ret;
}

void ObMicroBlockHandleMgr::dec_hold_size(ObMicroBlockDataHandle &handle)
{
  current_hold_size_ -= handle.get_handle_size();
}

bool ObMicroBlockHandleMgr::reach_hold_limit() const
{
  return enable_limit_ && current_hold_size_ >= hold_limit_;
}

int ObMicroBlockHandleMgr::update_limit()
{
  int ret = OB_SUCCESS;
  if (enable_limit_ && 0 == (++update_limit_count_ % UPDATE_INTERVAL)) {
    uint64_t tenant_id = MTL_ID();
    int64_t tenant_free_memory = lib::get_tenant_memory_limit(tenant_id) - lib::get_tenant_memory_hold(tenant_id);
    hold_limit_ = HOLD_LIMIT_BASE + tenant_free_memory / 1024 ;
    int64_t cache_washable_size = 0;
    if (query_flag_->is_use_block_cache()) {
      if (OB_FAIL(ObKVGlobalCache::get_instance().get_washable_size(tenant_id, cache_washable_size))) {
        LOG_WARN("Fail to get kvcache washable size", K(ret));
      } else {
        data_block_use_cache_limit_ = tenant_free_memory / 5 + cache_washable_size / 10;
        use_data_block_cache_ = data_block_submit_io_size_ <= data_block_use_cache_limit_;
      }
    }
    LOG_DEBUG("calculate limit", K(tenant_id), K(tenant_free_memory), K(cache_washable_size),
                                 K(query_flag_->is_use_block_cache()));
  }
  LOG_DEBUG("update limit details", K(enable_limit_), K(update_limit_count_), K(current_hold_size_),
      K(hold_limit_), K(data_block_submit_io_size_), K(data_block_use_cache_limit_), K(use_data_block_cache_));
  return ret;
}

void ObMicroBlockHandleMgr::update_data_block_io_size(const int64_t block_size)
{
  if (enable_limit_) {
    data_block_submit_io_size_ += block_size;
    use_data_block_cache_ = data_block_submit_io_size_ <= data_block_use_cache_limit_;
  }
  LOG_DEBUG("update data block io size", K(enable_limit_), K(block_size), K(use_data_block_cache_),
                                         K(data_block_submit_io_size_), K(data_block_use_cache_limit_));
}

void ObMicroBlockHandleMgr::cache_bypass(const bool is_data_block)
{
  if (is_data_block) {
    EVENT_INC(ObStatEventIds::DATA_BLOCK_READ_CNT);
  } else {
    EVENT_INC(ObStatEventIds::INDEX_BLOCK_READ_CNT);
  }
}

void ObMicroBlockHandleMgr::cache_hit(const bool is_data_block)
{
  ++table_store_stat_->block_cache_hit_cnt_;
  if (is_data_block) {
    EVENT_INC(ObStatEventIds::DATA_BLOCK_CACHE_HIT);
  } else {
    EVENT_INC(ObStatEventIds::INDEX_BLOCK_CACHE_HIT);
    ++table_store_stat_->index_block_cache_hit_cnt_;
  }
}

void ObMicroBlockHandleMgr::cache_miss(const bool is_data_block)
{
  ++table_store_stat_->block_cache_miss_cnt_;
  if (is_data_block) {
    EVENT_INC(ObStatEventIds::DATA_BLOCK_READ_CNT);
  } else {
    EVENT_INC(ObStatEventIds::INDEX_BLOCK_READ_CNT);
    ++table_store_stat_->index_block_cache_miss_cnt_;
  }
}


}
}
