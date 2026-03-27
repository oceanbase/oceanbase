/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

class ObStorageLogger;

class ObServerCheckpointWriter final
{
public:
  ObServerCheckpointWriter() : is_inited_(false), server_slogger_(nullptr) {}
  ~ObServerCheckpointWriter() = default;
  ObServerCheckpointWriter(const ObServerCheckpointWriter &) = delete;
  ObServerCheckpointWriter &operator=(const ObServerCheckpointWriter &) = delete;

  int init(ObStorageLogger *server_slogger);
  int write_checkpoint(const common::ObLogCursor &log_cursor);
  common::ObIArray<blocksstable::MacroBlockId> &get_meta_block_list();

private:
  int write_tenant_meta_checkpoint(blocksstable::MacroBlockId &block_entry);

private:
  bool is_inited_;
  ObStorageLogger *server_slogger_;
  common::ObConcurrentFIFOAllocator allocator_;
  ObSlogCheckpointFdDispenser fd_dispenser_;
  ObLinkedMacroBlockItemWriter tenant_meta_item_writer_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_STORAGE_CKPT_SERVER_CHECKPOINT_WRITER_H
