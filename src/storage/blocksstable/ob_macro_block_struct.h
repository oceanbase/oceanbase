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

#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_store_file_system.h"

#ifndef OB_MACRO_BLOCK_STRUCT_H_
#define OB_MACRO_BLOCK_STRUCT_H_

namespace oceanbase {
namespace blocksstable {
struct ObMacroBlocksWriteCtx final {
  static const int64_t DEFAULT_READ_BLOCK_NUM = 16;
  ObMacroBlocksWriteCtx();
  ~ObMacroBlocksWriteCtx();
  common::ObArenaAllocator allocator_;
  blocksstable::ObStoreFileCtx file_ctx_;
  common::ObSEArray<blocksstable::MacroBlockId, DEFAULT_READ_BLOCK_NUM> macro_block_list_;
  common::ObSEArray<blocksstable::ObFullMacroBlockMeta, DEFAULT_READ_BLOCK_NUM> macro_block_meta_list_;
  ObStorageFileHandle file_handle_;
  ObStorageFile* file_;

  bool is_valid() const;
  void reset();
  void clear();
  int set(ObMacroBlocksWriteCtx& src);
  int add_macro_block_id(const blocksstable::MacroBlockId& macro_block_id);
  int add_macro_block_meta(const blocksstable::ObFullMacroBlockMeta& macro_block_meta);
  int add_macro_block(
      const blocksstable::MacroBlockId& macro_block_id, const blocksstable::ObFullMacroBlockMeta& macro_block_meta);
  int64_t get_macro_block_count() const
  {
    return macro_block_list_.count();
  }
  bool is_empty() const
  {
    return macro_block_list_.empty();
  }
  common::ObIArray<blocksstable::MacroBlockId>& get_macro_block_list()
  {
    return macro_block_list_;
  }
  TO_STRING_KV(K_(file_ctx), K_(macro_block_list), K_(file_handle));
};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif /* OB_MACRO_BLOCK_STRUCT_H_ */
