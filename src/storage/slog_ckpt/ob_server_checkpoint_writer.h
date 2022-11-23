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

#ifndef OB_STORAGE_CKPT_SERVER_CHECKPOINT_WRITER_H
#define OB_STORAGE_CKPT_SERVER_CHECKPOINT_WRITER_H

#include "storage/slog_ckpt/ob_linked_macro_block_writer.h"

namespace oceanbase
{

namespace common
{
class ObLogCursor;
}

namespace storage
{

class ObServerCheckpointWriter final
{
public:
  ObServerCheckpointWriter() : is_inited_(false) {}
  ~ObServerCheckpointWriter() = default;
  ObServerCheckpointWriter(const ObServerCheckpointWriter &) = delete;
  ObServerCheckpointWriter &operator=(const ObServerCheckpointWriter &) = delete;

  int init();
  int write_checkpoint(const common::ObLogCursor &log_cursor);
  common::ObIArray<blocksstable::MacroBlockId> &get_meta_block_list();

private:
  int write_tenant_meta_checkpoint(blocksstable::MacroBlockId &block_entry);

private:
  bool is_inited_;
  common::ObConcurrentFIFOAllocator allocator_;
  ObLinkedMacroBlockItemWriter tenant_meta_item_writer_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_STORAGE_CKPT_SERVER_CHECKPOINT_WRITER_H
