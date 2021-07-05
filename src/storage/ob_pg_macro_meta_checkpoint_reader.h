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

#ifndef OB_PG_MACRO_META_CHECKPOINT_READER_H_
#define OB_PG_MACRO_META_CHECKPOINT_READER_H_

#include "lib/container/ob_array.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/hash/ob_hashmap.h"
#include "blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_macro_meta_replay_map.h"
#include "storage/ob_pg_meta_block_reader.h"
#include "storage/ob_pg_macro_meta_checkpoint_writer.h"
#include "storage/ob_i_table.h"

namespace oceanbase {
namespace storage {

struct ObPGMacroMetaPair final {
public:
  ObPGMacroMetaPair() : meta_(nullptr), block_index_(0)
  {}
  ~ObPGMacroMetaPair() = default;
  blocksstable::ObMacroBlockMetaV2* meta_;
  int64_t block_index_;
};

class ObPGMacroMetaCheckpointReader final {
public:
  ObPGMacroMetaCheckpointReader();
  ~ObPGMacroMetaCheckpointReader() = default;
  int init(const blocksstable::MacroBlockId& entry_block, blocksstable::ObStorageFileHandle& file_handle,
      ObMacroMetaReplayMap* replay_map);
  int read_checkpoint();
  void reset();
  common::ObIArray<blocksstable::MacroBlockId>& get_meta_block_list();

private:
  int read_next_entry(ObPGMacroBlockMetaCheckpointEntry& entry);

private:
  ObPGMetaItemReader reader_;
  ObMacroMetaReplayMap* replay_map_;
  bool is_inited_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_PG_MACRO_META_CHECKPOINT_READER_H_
