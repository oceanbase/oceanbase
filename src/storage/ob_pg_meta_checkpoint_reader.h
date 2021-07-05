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

#ifndef OB_PG_META_CHECKPOINT_READER_H_
#define OB_PG_META_CHECKPOINT_READER_H_

#include "storage/ob_partition_meta_redo_module.h"
#include "storage/ob_pg_meta_block_reader.h"
#include "storage/ob_pg_meta_checkpoint_writer.h"

namespace oceanbase {
namespace storage {

class ObPGMetaCheckpointReader final {
public:
  ObPGMetaCheckpointReader();
  ~ObPGMetaCheckpointReader() = default;
  int init(const blocksstable::MacroBlockId& entry_block, blocksstable::ObStorageFileHandle& file_handle,
      ObPartitionMetaRedoModule& pg_mgr);
  int read_checkpoint();
  void reset();
  common::ObIArray<blocksstable::MacroBlockId>& get_meta_block_list();

private:
  int read_item(ObPGMetaItemBuffer& item);

private:
  bool is_inited_;
  ObPGMetaItemReader reader_;
  blocksstable::ObStorageFileHandle file_handle_;
  ObPartitionMetaRedoModule* pg_mgr_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_PG_META_CHECKPOINT_READER_H_
