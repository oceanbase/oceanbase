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

#ifndef OB_PG_ALL_META_CHECKPOINT_READER_H_
#define OB_PG_ALL_META_CHECKPOINT_READER_H_

#include "storage/ob_pg_macro_meta_checkpoint_reader.h"
#include "storage/ob_pg_meta_checkpoint_reader.h"

namespace oceanbase {
namespace storage {

class ObPGAllMetaCheckpointReader final {
public:
  ObPGAllMetaCheckpointReader();
  ~ObPGAllMetaCheckpointReader() = default;
  int init(const blocksstable::MacroBlockId& macro_entry_block, const blocksstable::MacroBlockId& pg_entry_block,
      blocksstable::ObStorageFileHandle& file_handle, ObPartitionMetaRedoModule& pg_mgr,
      ObMacroMetaReplayMap* replay_map);
  int read_checkpoint();
  void reset();
  int get_meta_block_list(common::ObIArray<blocksstable::MacroBlockId>& meta_block_ids);

private:
  bool is_inited_;
  ObPGMacroMetaCheckpointReader macro_meta_reader_;
  ObPGMetaCheckpointReader pg_meta_reader_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_PG_ALL_META_CHECKPOINT_READER_H_
