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
namespace storage {


struct ObSSTableMicroBlockState {
  enum ObSSTableMicroBlockStateEnum {
    UNKNOWN_STATE = 0,
    IN_BLOCK_CACHE,
    IN_BLOCK_IO,
    NEED_SYNC_IO
  };
};


class ObMicroBlockHandleMgr;
struct ObMicroBlockDataHandle {
  ObMicroBlockDataHandle();
  virtual ~ObMicroBlockDataHandle();
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
  TO_STRING_KV(K_(tenant_id), K_(macro_block_id), K_(micro_info), K_(need_release_data_buf), K_(is_loaded_block),
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
  bool need_release_data_buf_;  // TODO : @lvling to be removed
  bool is_loaded_block_;

private:
  int get_loaded_block_data(blocksstable::ObMicroBlockData &block_data);
  void try_release_loaded_block();
};

class ObMicroBlockHandleMgr
{
public:
  ObMicroBlockHandleMgr();
  ~ObMicroBlockHandleMgr();
  void reset();
  int init(const bool enable_prefetch_limiting, ObTableStoreStat &stat, ObQueryFlag &query_flag);
  int get_micro_block_handle(
      const blocksstable::ObMicroIndexInfo &index_block_info,
      const bool is_data_block,
      const bool need_submit_io,
      ObMicroBlockDataHandle &micro_block_handle);

  void dec_hold_size(ObMicroBlockDataHandle &handle);
  bool reach_hold_limit() const;
  OB_INLINE bool is_valid() const { return is_inited_; }
  TO_STRING_KV(K_(is_inited), K_(enable_limit), K_(current_hold_size), K_(hold_limit),
               K_(data_block_submit_io_size), K_(data_block_use_cache_limit), K_(update_limit_count),
               KPC_(query_flag), KPC_(table_store_stat), KP_(data_block_cache), KP_(index_block_cache));
private:
  int update_limit();
  void update_data_block_io_size(const int64_t block_size);
  void cache_bypass(const bool is_data_block);
  void cache_hit(const bool is_data_block);
  void cache_miss(const bool is_data_block);
private:
  static const int64_t HOLD_LIMIT_BASE = 10L << 20;  // 10M
  static const int64_t UPDATE_INTERVAL = 100;
  static const int64_t DEFAULT_DATA_BLOCK_USE_CACHE_LIMIT = 2L << 20;  // 2M

  blocksstable::ObDataMicroBlockCache *data_block_cache_;
  blocksstable::ObIndexMicroBlockCache *index_block_cache_;
  ObTableStoreStat *table_store_stat_;
  ObQueryFlag *query_flag_;
  ObFIFOAllocator block_io_allocator_;
  int64_t update_limit_count_;
  int64_t data_block_submit_io_size_;
  int64_t data_block_use_cache_limit_;
  int64_t hold_limit_;
  int64_t current_hold_size_;
  bool use_data_block_cache_;
  bool enable_limit_;
  bool is_inited_;
};


}
}

#endif /* OB_MICRO_BLOCK_HANDLE_MGR_H_ */
