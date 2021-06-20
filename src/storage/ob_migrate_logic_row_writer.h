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

#ifndef OCEANBASE_STORAGE_OB_MIGRATE_LOGIC_ROW_WRITER_H_
#define OCEANBASE_STORAGE_OB_MIGRATE_LOGIC_ROW_WRITER_H_
#include "storage/ob_partition_base_data_ob_reader.h"
#include "blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_tenant_file_struct.h"

namespace oceanbase {
namespace storage {
class ObMigrateLogicRowWriter {
public:
  ObMigrateLogicRowWriter();
  virtual ~ObMigrateLogicRowWriter()
  {}

  int init(ObILogicRowIterator* iterator, const common::ObPGKey& pg_key,
      const blocksstable::ObStorageFileHandle& file_handle);

  virtual int process(blocksstable::ObMacroBlocksWriteCtx& macro_block_writectx,
      blocksstable::ObMacroBlocksWriteCtx& lob_block_writectx);
  void reset();
  int64_t get_data_checksum() const
  {
    return data_checksum_;
  }

private:
  bool is_inited_;
  ObILogicRowIterator* iterator_;
  int64_t data_checksum_;
  common::ObPGKey pg_key_;
  blocksstable::ObStorageFileHandle file_handle_;
  DISALLOW_COPY_AND_ASSIGN(ObMigrateLogicRowWriter);
};

}  // end namespace storage
}  // end namespace oceanbase
#endif /* OCEANBASE_STORAGE_OB_MIGRATE_LOGIC_ROW_WRITER_H_ */
