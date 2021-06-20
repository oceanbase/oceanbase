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

#include "blocksstable/ob_macro_block_reader.h"
#include "blocksstable/ob_imicro_block_reader.h"
#include "blocksstable/ob_block_sstable_struct.h"
#include "blocksstable/ob_micro_block_cache.h"
#include "storage/ob_handle_mgr.h"

namespace oceanbase {
namespace storage {

struct ObSSTableMicroBlockState {
  enum ObSSTableMicroBlockStateEnum { UNKNOWN_STATE = 0, IN_BLOCK_CACHE, IN_BLOCK_IO };
};

struct ObMicroBlockDataHandle {
  ObMicroBlockDataHandle();
  virtual ~ObMicroBlockDataHandle();
  void reset();
  int get_block_data(blocksstable::ObMacroBlockReader& block_reader, blocksstable::ObStorageFile* storage_file,
      blocksstable::ObMicroBlockData& block_data);
  TO_STRING_KV(
      K_(table_id), K_(block_ctx), K_(micro_info), K_(block_state), K_(block_index), K_(cache_handle), K_(io_handle));
  uint64_t table_id_;
  blocksstable::ObMacroBlockCtx block_ctx_;
  int32_t block_state_;
  int32_t block_index_;
  blocksstable::ObMicroBlockInfo micro_info_;
  blocksstable::ObMicroBlockBufferHandle cache_handle_;
  blocksstable::ObMacroBlockHandle io_handle_;
};

class ObMicroBlockHandleMgr : public ObHandleMgr<ObMicroBlockDataHandle, blocksstable::ObMicroBlockCacheKey, 64> {
public:
  ObMicroBlockHandleMgr() = default;
  virtual ~ObMicroBlockHandleMgr() = default;
  int get_micro_block_handle(const uint64_t table_id, const blocksstable::ObMacroBlockCtx& block_ctx,
      const int64_t file_id, const int64_t offset, const int64_t size, const int64_t index,
      ObMicroBlockDataHandle& micro_block_handle);
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OB_MICRO_BLOCK_HANDLE_MGR_H_ */
