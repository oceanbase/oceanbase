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

#ifndef OB_MICRO_BLOCK_INDEX_HANDLE_MGR_H_
#define OB_MICRO_BLOCK_INDEX_HANDLE_MGR_H_

#include "blocksstable/ob_micro_block_index_cache.h"
#include "storage/ob_handle_mgr.h"

namespace oceanbase {
namespace storage {

struct ObMicroBlockIndexHandle {
  ObMicroBlockIndexHandle();
  virtual ~ObMicroBlockIndexHandle();
  void reset();
  int search_blocks(const common::ObStoreRange& range, const bool is_left_border, const bool is_right_border,
      common::ObIArray<blocksstable::ObMicroBlockInfo>& infos,
      const common::ObIArray<ObRowkeyObjComparer*>* cmp_funcs = nullptr);
  int search_blocks(const common::ObStoreRowkey& rowkey, blocksstable::ObMicroBlockInfo& info,
      const common::ObIArray<ObRowkeyObjComparer*>* cmp_funcs = nullptr);
  int get_block_index_mgr(const blocksstable::ObMicroBlockIndexMgr*& block_idx_mgr);
  TO_STRING_KV(K_(table_id), K_(block_ctx));
  uint64_t table_id_;
  blocksstable::ObMacroBlockCtx block_ctx_;
  const blocksstable::ObMicroBlockIndexMgr* block_index_mgr_;
  blocksstable::ObMicroBlockIndexBufferHandle cache_handle_;
  blocksstable::ObMacroBlockHandle io_handle_;
};

class ObMicroBlockIndexHandleMgr : public ObHandleMgr<ObMicroBlockIndexHandle, blocksstable::MacroBlockId, 64> {
public:
  ObMicroBlockIndexHandleMgr() = default;
  virtual ~ObMicroBlockIndexHandleMgr() = default;
  int get_block_index_handle(const uint64_t table_id, const blocksstable::ObMacroBlockCtx& block_ctx,
      const int64_t file_id, const common::ObQueryFlag& flag, blocksstable::ObStorageFile* pg_file,
      ObMicroBlockIndexHandle& block_idx_handle);
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OB_MICRO_BLOCK_INDEX_HANDLE_MGR_H_ */
