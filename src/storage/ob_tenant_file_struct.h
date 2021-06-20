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

#ifndef OB_TENANT_FILE_STRUCT_H_
#define OB_TENANT_FILE_STRUCT_H_

#include "blocksstable/ob_block_sstable_struct.h"
#include "blocksstable/ob_store_file_system.h"
#include "blocksstable/ob_store_file.h"
#include "storage/ob_macro_meta_replay_map.h"

namespace oceanbase {
namespace blocksstable {
class ObStorageFile;
}
namespace storage {

struct ObTenantFileKey final {
public:
  ObTenantFileKey() : tenant_id_(common::OB_INVALID_ID), file_id_(0)
  {}
  ObTenantFileKey(const uint64_t tenant_id, const int64_t file_id) : tenant_id_(tenant_id), file_id_(file_id)
  {}
  ~ObTenantFileKey() = default;
  void reset();
  bool is_valid() const
  {
    return common::OB_INVALID_ID != tenant_id_ && file_id_ >= -1;
  }
  uint64_t hash() const;
  bool operator==(const ObTenantFileKey& other) const;
  bool operator!=(const ObTenantFileKey& other) const;
  TO_STRING_KV(K_(tenant_id), K_(file_id));
  uint64_t tenant_id_;
  int64_t file_id_;
  OB_UNIS_VERSION(1);
};

enum ObTenantFileStatus {
  TENANT_FILE_INVALID = 0,
  TENANT_FILE_INIT,
  TENANT_FILE_NORMAL,
  TENANT_FILE_DELETING,
  TENANT_FILE_MAX
};

struct ObTenantFileSuperBlock final {
public:
  static const int64_t TENANT_FILE_SUPER_BLOCK_VERSION_V1 = 1;
  ObTenantFileSuperBlock();
  ~ObTenantFileSuperBlock() = default;
  void reset();
  bool is_valid() const;
  bool is_normal_status() const
  {
    return TENANT_FILE_NORMAL == status_;
  }
  bool is_init_status() const
  {
    return TENANT_FILE_INIT == status_;
  }
  TO_STRING_KV(K_(macro_meta_entry), K_(pg_meta_entry), K_(status));
  OB_UNIS_VERSION(TENANT_FILE_SUPER_BLOCK_VERSION_V1);

public:
  blocksstable::ObSuperBlockMetaEntry macro_meta_entry_;
  blocksstable::ObSuperBlockMetaEntry pg_meta_entry_;
  ObTenantFileStatus status_;
  bool is_sys_table_file_;
};

struct ObTenantFileInfo : public blocksstable::ObISuperBlock {
public:
  ObTenantFileInfo() : tenant_key_(), tenant_file_super_block_(), pg_map_()
  {}
  virtual ~ObTenantFileInfo()
  {
    reset();
  }
  void reset()
  {
    tenant_key_.reset();
    tenant_file_super_block_.reset();
    if (pg_map_.created()) {
      pg_map_.destroy();
    }
  }
  virtual int64_t get_timestamp() const
  {
    return 0;
  }
  virtual bool is_valid() const
  {
    return tenant_key_.is_valid() && tenant_file_super_block_.is_valid();
  }
  bool is_normal_status() const
  {
    return tenant_file_super_block_.is_normal_status();
  }
  bool is_init_status() const
  {
    return tenant_file_super_block_.is_init_status();
  }
  int add_pg(const common::ObPGKey& pg_key);
  int exist_pg(const common::ObPGKey& pg_key);
  int remove_pg(const common::ObPGKey& pg_key);
  bool is_empty_file() const
  {
    return 0 == pg_map_.size();
  }
  int64_t get_pg_cnt() const
  {
    return pg_map_.size();
  }
  int deep_copy(ObTenantFileInfo& dst_info);
  int serialize_pg_map(char* buf, int64_t buf_len, int64_t& pos) const;
  int deserialize_pg_map(const char* buf, int64_t buf_len, int64_t& pos);
  int64_t get_pg_map_serialize_size() const;
  VIRTUAL_TO_STRING_KV(K_(tenant_key), K_(tenant_file_super_block), K(pg_map_.size()));
  typedef common::hash::ObHashMap<common::ObPGKey, bool> PG_MAP;
  ObTenantFileKey tenant_key_;
  ObTenantFileSuperBlock tenant_file_super_block_;
  PG_MAP pg_map_;
  VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
  DISALLOW_COPY_AND_ASSIGN(ObTenantFileInfo);
};

class ObMetaBlockListHandle final {
public:
  ObMetaBlockListHandle();
  ~ObMetaBlockListHandle();
  int add_macro_blocks(const common::ObIArray<blocksstable::MacroBlockId>& block_list, const bool need_switch_handle);
  void reset();
  int reserve(const int64_t block_count);
  const common::ObIArray<blocksstable::MacroBlockId>& get_meta_block_list() const;
  blocksstable::ObStorageFile* get_storage_file()
  {
    return meta_handles_[0].get_storage_file();
  }
  void set_storage_file(blocksstable::ObStorageFile* file);

private:
  void switch_handle();
  void reset_new_handle();

private:
  static const int64_t META_BLOCK_HANDLE_CNT = 2;
  blocksstable::ObMacroBlocksHandle meta_handles_[META_BLOCK_HANDLE_CNT];
  int64_t cur_handle_pos_;
};

struct ObTenantFileValue final {
public:
  static const int64_t MAX_REF_CNT_PER_FILE = 1000;
  static const int64_t META_HANDLE_CNT = 2;
  ObTenantFileValue()
      : file_info_(), storage_file_(), info_file_(nullptr), meta_block_handle_(), is_owner_(true), from_svr_ckpt_(false)
  {}
  ~ObTenantFileValue()
  {
    reset();
  }
  void reset();
  TO_STRING_KV(K_(file_info), K_(storage_file), KP_(info_file), K_(is_owner), K_(from_svr_ckpt));
  ObTenantFileInfo file_info_;
  blocksstable::ObStorageFileWithRef storage_file_;
  blocksstable::ObStorageFile* info_file_;
  ObMetaBlockListHandle meta_block_handle_;
  bool is_owner_;
  bool from_svr_ckpt_;  // only used during observer restart
};

class ObTenantFileFilter final {
public:
  ObTenantFileFilter();
  ~ObTenantFileFilter() = default;
  void reset();
  void set_filter_tenant_file_key(const ObTenantFileKey& tenant_file_key);
  int is_filtered(const ObTenantFileKey& tenant_file_key, bool& is_filtered);
  ObTenantFileKey tenant_file_key_;
};

class ObTenantFileCheckpointEntry final {
public:
  ObTenantFileCheckpointEntry();
  ~ObTenantFileCheckpointEntry();
  void reset();
  int assign(const ObTenantFileCheckpointEntry& entry);
  ObTenantFileKey tenant_file_key_;
  ObTenantFileSuperBlock super_block_;
  ObMetaBlockListHandle meta_block_handle_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_TENANT_FILE_STRUCT_H_
