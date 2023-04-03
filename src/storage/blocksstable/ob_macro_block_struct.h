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

#ifndef OB_MACRO_BLOCK_STRUCT_H_
#define OB_MACRO_BLOCK_STRUCT_H_

namespace oceanbase
{
namespace blocksstable
{
struct ObDataMacroBlockMeta;
struct ObMacroBlocksWriteCtx final
{
public:
  static const int64_t DEFAULT_READ_BLOCK_NUM = 8;
  ObMacroBlocksWriteCtx();
  ~ObMacroBlocksWriteCtx();
  void reset();
  void clear();
  int set(ObMacroBlocksWriteCtx &src);
  int deep_copy(ObMacroBlocksWriteCtx *&dst, ObIAllocator &allocator);
  int get_macro_id_array(common::ObIArray<blocksstable::MacroBlockId> &block_ids);
  int add_macro_block_id(const blocksstable::MacroBlockId &macro_block_id);
  OB_INLINE void increment_old_block_count() { ++use_old_macro_block_count_; }
  OB_INLINE int64_t get_macro_block_count() const { return macro_block_list_.count(); }
  OB_INLINE bool is_empty() const { return macro_block_list_.empty(); }
  OB_INLINE common::ObIArray<blocksstable::MacroBlockId> &get_macro_block_list() { return macro_block_list_; }
  TO_STRING_KV(K(macro_block_list_.count()), K_(use_old_macro_block_count));
public:
  common::ObArenaAllocator allocator_;
  common::ObSEArray<blocksstable::MacroBlockId, DEFAULT_READ_BLOCK_NUM> macro_block_list_;
  int64_t use_old_macro_block_count_;
  DISALLOW_COPY_AND_ASSIGN(ObMacroBlocksWriteCtx);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif /* OB_MACRO_BLOCK_STRUCT_H_ */
