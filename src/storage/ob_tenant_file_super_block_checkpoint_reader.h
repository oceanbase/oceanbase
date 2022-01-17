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

#ifndef OB_TENANT_FILE_SUPER_BLOCK_CHECKPOINT_READER_H_
#define OB_TENANT_FILE_SUPER_BLOCK_CHECKPOINT_READER_H_

#include "storage/ob_pg_meta_block_reader.h"
#include "storage/ob_tenant_file_super_block_checkpoint_writer.h"
#include "storage/ob_tenant_file_mgr.h"

namespace oceanbase {
namespace storage {

class ObTenantFileSuperBlockCheckpointReader final {
public:
  ObTenantFileSuperBlockCheckpointReader();
  ~ObTenantFileSuperBlockCheckpointReader() = default;
  void reset()
  {
    reader_.reset();
  }
  int read_checkpoint(const blocksstable::ObSuperBlockMetaEntry& entry, ObBaseFileMgr& file_mgr,
      blocksstable::ObStorageFileHandle& file_handle);
  common::ObIArray<blocksstable::MacroBlockId>& get_meta_block_list();

private:
  ObPGMetaItemReader reader_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_TENANT_FILE_SUPER_BLOCK_CHECKPOINT_READER_H_
