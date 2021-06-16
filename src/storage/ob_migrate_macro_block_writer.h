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

#ifndef OCEANBASE_STORAGE_OB_MIGRATE_MACRO_BLOCK_WRITER_H_
#define OCEANBASE_STORAGE_OB_MIGRATE_MACRO_BLOCK_WRITER_H_
#include "storage/blocksstable/ob_store_file.h"
#include "blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_checker.h"
namespace oceanbase {
namespace storage {
class ObIPartitionMacroBlockReader;
class ObPartitionMacroBlockOfsReader;
class ObIMigrateMacroBlockWriter {
public:
  ObIMigrateMacroBlockWriter()
  {}
  virtual ~ObIMigrateMacroBlockWriter()
  {}
  virtual int process(blocksstable::ObMacroBlocksWriteCtx& copied_ctx) = 0;
};

class ObMigrateMacroBlockWriter : public ObIMigrateMacroBlockWriter {
public:
  ObMigrateMacroBlockWriter();
  virtual ~ObMigrateMacroBlockWriter()
  {}
  int init(ObIPartitionMacroBlockReader* reader, const uint64_t tenant_id, blocksstable::ObStorageFile* pg_file);

  virtual int process(blocksstable::ObMacroBlocksWriteCtx& copied_ctx);

private:
  int check_macro_block(const blocksstable::ObFullMacroBlockMeta& meta, const blocksstable::ObBufferReader& data);
  bool is_inited_;
  uint64_t tenant_id_;
  ObIPartitionMacroBlockReader* reader_;
  blocksstable::ObStorageFileHandle file_handle_;
  blocksstable::ObSSTableMacroBlockChecker macro_checker_;
};
}  // namespace storage
}  // namespace oceanbase
#endif /* OCEANBASE_STORAGE_OB_MIGRATE_MACRO_BLOCK_WRITER_H_ */
