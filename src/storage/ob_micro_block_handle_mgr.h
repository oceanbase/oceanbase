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

#include "blocksstable/ob_block_manager.h"
#include "blocksstable/ob_block_sstable_struct.h"
#include "blocksstable/ob_imicro_block_reader.h"
#include "blocksstable/ob_index_block_row_scanner.h"
#include "blocksstable/ob_macro_block_reader.h"
#include "blocksstable/ob_micro_block_cache.h"
#include "storage/ob_handle_mgr.h"

namespace oceanbase {
namespace storage {

struct ObSSTableMicroBlockState {
  enum ObSSTableMicroBlockStateEnum {
    UNKNOWN_STATE = 0,
    IN_BLOCK_CACHE,
    IN_BLOCK_IO
  };
};


struct ObMicroBlockDataHandle {
  ObMicroBlockDataHandle();
  virtual ~ObMicroBlockDataHandle();
  void reset();

  int get_data_block_data(
      blocksstable::ObMacroBlockReader &block_reader,
      blocksstable::ObMicroBlockData &block_data);
  int get_index_block_data(blocksstable::ObMicroBlockData &index_block);
  int get_cached_index_block_data(blocksstable::ObMicroBlockData &index_block);
  TO_STRING_KV(K_(tenant_id), K_(macro_block_id), K_(micro_info),
               K_(block_state), K_(block_index), K_(cache_handle), K_(io_handle));
  uint64_t tenant_id_;
  blocksstable::MacroBlockId macro_block_id_;
  int32_t block_state_;
  int32_t block_index_;
  blocksstable::ObMicroBlockInfo micro_info_;
  blocksstable::ObMicroBlockDesMeta des_meta_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  blocksstable::ObMicroBlockBufferHandle cache_handle_;
  blocksstable::ObMacroBlockHandle io_handle_;
  ObIAllocator *allocator_;
  blocksstable::ObMicroBlockData loaded_index_block_data_;
  bool is_loaded_index_block_;

private:
  int get_loaded_block_data(blocksstable::ObMicroBlockData &block_data);
  void try_release_loaded_index_block();
};

class ObMicroBlockHandleMgr : public ObHandleMgr<ObMicroBlockDataHandle,
                                                 blocksstable::ObMicroBlockCacheKey,
                                                 64>
{
public:
  ObMicroBlockHandleMgr();
  virtual ~ObMicroBlockHandleMgr() = default;
  void reset();

  int init(const bool is_multi, const bool is_ordered, common::ObIAllocator &allocator);
  int get_micro_block_handle(
      const uint64_t tenant_id,
      const blocksstable::ObMicroIndexInfo &index_block_info,
      const bool is_data_block,
      ObMicroBlockDataHandle &micro_block_handle);
  int put_micro_block_handle(
      const uint64_t tenant_id,
      const blocksstable::MacroBlockId &macro_id,
      const blocksstable::ObIndexBlockRowHeader &idx_header,
      ObMicroBlockDataHandle &micro_block_handle);
  int reset_handle_cache();
private:
  // allocator for index micro block prefetch failed and async io
  common::ObFIFOAllocator allocator_;
};

}
}

#endif /* OB_MICRO_BLOCK_HANDLE_MGR_H_ */
