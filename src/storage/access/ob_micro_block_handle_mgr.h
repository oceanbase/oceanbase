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

#ifndef OB_MICRO_BLOCK_HANDLE_MGR_H_
#define OB_MICRO_BLOCK_HANDLE_MGR_H_

#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/blocksstable/ob_macro_block_reader.h"
#include "storage/blocksstable/ob_micro_block_cache.h"
#include "storage/blocksstable/index_block/ob_index_block_row_scanner.h"
#include "storage/ob_handle_mgr.h"

namespace oceanbase {
using namespace blocksstable;
namespace storage {

struct ObSSTableMicroBlockState {
  enum ObSSTableMicroBlockStateEnum {
    UNKNOWN_STATE = 0,
    IN_BLOCK_CACHE,
    IN_BLOCK_IO,
    NEED_SYNC_IO,
    NEED_MULTI_IO
  };
};

struct ObTableScanStoreStat;
class ObMicroBlockHandleMgr;
struct ObMicroBlockDataHandle {
  ObMicroBlockDataHandle();
  virtual ~ObMicroBlockDataHandle();
  void init(
      const uint64_t tenant_id,
      const blocksstable::MacroBlockId &macro_id,
      const int64_t offset,
      const int64_t size,
      ObMicroBlockHandleMgr *handle_mgr);
  void reset();
  bool match(const blocksstable::MacroBlockId &macro_id,
             const int32_t offset,
             const int32_t size) const;
  int get_micro_block_data(
      blocksstable::ObMacroBlockReader *macro_reader,
      blocksstable::ObMicroBlockData &block_data,
      const bool is_data_block = true);
  int get_cached_index_block_data(blocksstable::ObMicroBlockData &index_block);
  int64_t get_handle_size() const;
  ObMicroBlockDataHandle & operator=(const ObMicroBlockDataHandle &other);
  OB_INLINE bool in_block_state() const
  { return ObSSTableMicroBlockState::IN_BLOCK_CACHE == block_state_ || ObSSTableMicroBlockState::IN_BLOCK_IO == block_state_; }
  OB_INLINE bool need_multi_io() const
  { return ObSSTableMicroBlockState::NEED_MULTI_IO == block_state_; }
  TO_STRING_KV(K_(tenant_id), K_(macro_block_id), K_(micro_info), K_(is_loaded_block),
               K_(block_state), K_(block_index), K_(cache_handle), K_(io_handle), K_(loaded_block_data), KP_(allocator));
  uint64_t tenant_id_;
  blocksstable::MacroBlockId macro_block_id_;
  int32_t block_state_;
  int32_t block_index_;
  blocksstable::ObMicroBlockInfo micro_info_;
  blocksstable::ObMicroBlockDesMeta des_meta_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  blocksstable::ObMicroBlockBufferHandle cache_handle_;
  blocksstable::ObMacroBlockHandle io_handle_;
  ObMicroBlockHandleMgr *handle_mgr_;
  ObIAllocator *allocator_;
  blocksstable::ObMicroBlockData loaded_block_data_;
  bool is_loaded_block_;

private:
  int get_loaded_block_data(blocksstable::ObMicroBlockData &block_data);
  void try_release_loaded_block();
};

class ObCacheMemController final
{
  typedef bool (ObCacheMemController::*need_sync_io_func_ptr)(
      const ObQueryFlag &query_flag,
      ObMicroBlockDataHandle &micro_block_handle,
      blocksstable::ObIMicroBlockCache *cache,
      ObFIFOAllocator &block_io_allocator);
  typedef bool (ObCacheMemController::*reach_hold_limit_func_ptr)() const;
  typedef void (ObCacheMemController::*update_data_block_io_size_func_ptr)(
      const int64_t block_size,
      const bool is_data_block,
      const bool use_cache);
public:
  ObCacheMemController();
  ~ObCacheMemController();
  OB_INLINE void init(const bool enable_limit);
  OB_INLINE void reset();
  OB_INLINE bool get_cache_use_flag() { return use_data_block_cache_; }
  OB_INLINE void add_hold_size(const int64_t handle_size);
  OB_INLINE void dec_hold_size(const int64_t handle_size);
public:
  OB_INLINE bool need_sync_io(
      const ObQueryFlag &query_flag,
      ObMicroBlockDataHandle &micro_block_handle,
      blocksstable::ObIMicroBlockCache *cache,
      ObFIFOAllocator &block_io_allocator)
  {
    return (this->*need_sync_io_func)(query_flag, micro_block_handle, cache, block_io_allocator);
  }
  OB_INLINE bool reach_hold_limit() const { return (this->*reach_hold_limit_func)(); }
  OB_INLINE void update_data_block_io_size(
      const int64_t block_size,
      const bool is_data_block,
      const bool use_cache)
  {
    return (this->*update_data_block_io_size_func)(block_size, is_data_block, use_cache);
  }
  TO_STRING_KV(K_(update_limit_count),
                K_(data_block_submit_io_size), K_(data_block_use_cache_limit),
                K_(hold_limit), K_(current_hold_size), K_(use_data_block_cache));
private:
  OB_INLINE bool need_sync_io_nlimit(
      const ObQueryFlag &query_flag,
      ObMicroBlockDataHandle &micro_block_handle,
      blocksstable::ObIMicroBlockCache *cache,
      ObFIFOAllocator &block_io_allocator) { return false; }
  OB_INLINE bool reach_hold_limit_nlimit() const { return false; }
  OB_INLINE void update_data_block_io_size_nlimit(
      const int64_t block_size,
      const bool is_data_block,
      const bool use_cache) {}
private:
  OB_INLINE bool need_sync_io_limit(
      const ObQueryFlag &query_flag,
      ObMicroBlockDataHandle &micro_block_handle,
      blocksstable::ObIMicroBlockCache *cache,
      ObFIFOAllocator &block_io_allocator);
  OB_INLINE bool reach_hold_limit_limit() const;
  OB_INLINE void update_data_block_io_size_limit(
      const int64_t block_size,
      const bool is_data_block,
      const bool use_cache);
  int update_limit(const ObQueryFlag &query_flag);
private:
  need_sync_io_func_ptr need_sync_io_func;
  reach_hold_limit_func_ptr reach_hold_limit_func;
  update_data_block_io_size_func_ptr update_data_block_io_size_func;
private:
  int64_t update_limit_count_;
  int64_t data_block_submit_io_size_;
  int64_t data_block_use_cache_limit_;
  int64_t hold_limit_;
  int64_t current_hold_size_;
  bool use_data_block_cache_;
private:
  static const int64_t HOLD_LIMIT_BASE = 10L << 20;  // 10M
  static const int64_t UPDATE_INTERVAL = 100;
  static const int64_t DEFAULT_DATA_BLOCK_USE_CACHE_LIMIT = 2L << 20;  // 2M
};

class ObMicroBlockHandleMgr
{
public:
  ObMicroBlockHandleMgr();
  ~ObMicroBlockHandleMgr();
  void reset();
  int init(const bool enable_prefetch_limiting, ObTableScanStoreStat &stat, ObQueryFlag &query_flag);
  int get_micro_block_handle(
      blocksstable::ObMicroIndexInfo &index_block_info,
      const bool is_data_block,
      const bool need_submit_io,
      const bool use_multi_block_prefetch,
      ObMicroBlockDataHandle &micro_block_handle,
      int16_t cur_level);
  int prefetch_multi_data_block(
      const ObMicroIndexInfo *micro_data_infos,
      ObMicroBlockDataHandle *micro_data_handles,
      const int64_t max_micro_handle_cnt,
      const int64_t max_prefetch_idx,
      const ObMultiBlockIOParam &multi_io_params);

  int submit_async_io(
      blocksstable::ObIMicroBlockCache *cache,
      const uint64_t tenant_id,
      const blocksstable::ObMicroIndexInfo &index_block_info,
      const bool is_data_block,
      const bool use_multi_block_prefetch,
      ObMicroBlockDataHandle &micro_block_handle);
  void dec_hold_size(ObMicroBlockDataHandle &handle);
  bool reach_hold_limit() const;
  OB_INLINE bool is_valid() const { return is_inited_; }
  TO_STRING_KV(K_(is_inited), KP_(table_store_stat), KPC_(query_flag),
               K_(cache_mem_ctrl), KP_(data_block_cache), KP_(index_block_cache));
private:
  blocksstable::ObDataMicroBlockCache *data_block_cache_;
  blocksstable::ObIndexMicroBlockCache *index_block_cache_;
  ObTableScanStoreStat *table_store_stat_;
  ObQueryFlag *query_flag_;
  ObFIFOAllocator block_io_allocator_;
  ObCacheMemController cache_mem_ctrl_;
  bool is_inited_;
};

} //storage
} //oceanbase

#endif /* OB_MICRO_BLOCK_HANDLE_MGR_H_ */
