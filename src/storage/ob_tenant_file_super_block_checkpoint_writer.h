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

#ifndef OB_TENANT_FILE_SUPER_BLOCK_CHECKPOINT_WRITER_H_
#define OB_TENANT_FILE_SUPER_BLOCK_CHECKPOINT_WRITER_H_

#include "storage/ob_tenant_file_struct.h"
#include "storage/ob_tenant_file_mgr.h"
#include "storage/ob_pg_meta_block_writer.h"

namespace oceanbase {
namespace storage {
struct ObTenantFileSuperBlockCheckpointEntry final {
public:
  static const int64_t TENANT_FILE_ENTRY_VERSION = 1;
  explicit ObTenantFileSuperBlockCheckpointEntry(ObTenantFileInfo& file_info) : file_info_(file_info)
  {}
  ~ObTenantFileSuperBlockCheckpointEntry() = default;
  TO_STRING_KV(K_(file_info));
  OB_UNIS_VERSION(TENANT_FILE_ENTRY_VERSION);
  ObTenantFileInfo& file_info_;
};

class ObTenantFileSuperBlockItem : public ObIPGMetaItem {
public:
  ObTenantFileSuperBlockItem();
  virtual ~ObTenantFileSuperBlockItem();
  void set_tenant_file_entry(ObTenantFileSuperBlockCheckpointEntry& entry);
  virtual int serialize(const char*& buf, int64_t& buf_size) override;
  virtual int16_t get_item_type() const override
  {
    return TENANT_FILE_INFO;
  }
  TO_STRING_KV(KP_(buf), K_(buf_size), KP_(entry));

private:
  int extend_buf(const int64_t request_size);

public:
  common::ObArenaAllocator allocator_;
  char* buf_;
  int64_t buf_size_;
  ObTenantFileSuperBlockCheckpointEntry* entry_;
};

class ObTenantFileSuperBlockCheckpointWriter final {
public:
  ObTenantFileSuperBlockCheckpointWriter();
  ~ObTenantFileSuperBlockCheckpointWriter() = default;
  int write_checkpoint(blocksstable::ObStorageFileHandle& server_root_handle, ObBaseFileMgr& file_mgr,
      common::hash::ObHashMap<ObTenantFileKey, ObTenantFileCheckpointEntry>& file_checkpoint_map,
      blocksstable::ObSuperBlockMetaEntry& entry);
  ObIArray<blocksstable::MacroBlockId>& get_meta_block_list();
  void reset();

private:
  ObPGMetaItemWriter writer_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_TENANT_FILE_SUPER_BLOCK_CHECKPOINT_WRITER_H_
