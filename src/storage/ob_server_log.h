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

#ifndef OB_SERVER_LOG_H_
#define OB_SERVER_LOG_H_

#include "blocksstable/slog/ob_base_storage_logger.h"
#include "storage/ob_tenant_file_struct.h"

namespace oceanbase {
namespace storage {
enum ObServerRedoLogSubcmd {
  REDO_LOG_REMOVE_TENANT_FILE_SUPER_BLOCK = 1,
  REDO_LOG_UPDATE_TENANT_FILE_SUPER_BLOCK = 2,
  REDO_LOG_ADD_PG_TO_TENANT_FILE = 3,
  REDO_LOG_REMOVE_PG_FROM_TENANT_FILE = 4,
  REDO_LOG_UPDATE_TENANT_FILE_INFO = 5,
};

struct ObUpdateTenantFileSuperBlockLogEntry : public blocksstable::ObIBaseStorageLogEntry {
public:
  static const int64_t UPDATE_TENANT_FILE_SUPER_BLOCK_VERSION = 1;
  ObUpdateTenantFileSuperBlockLogEntry();
  virtual ~ObUpdateTenantFileSuperBlockLogEntry() = default;
  bool is_valid() const
  {
    return file_key_.is_valid() && super_block_.is_valid();
  }
  ObTenantFileKey file_key_;
  ObTenantFileSuperBlock super_block_;
  TO_STRING_KV(K_(file_key), K_(super_block));
  OB_UNIS_VERSION_V(UPDATE_TENANT_FILE_SUPER_BLOCK_VERSION);
};

struct ObRemoveTenantFileSuperBlockLogEntry : public blocksstable::ObIBaseStorageLogEntry {
public:
  static const int64_t REMOVE_TENANT_FILE_SUPER_BLOCK_VERSION = 1;
  ObRemoveTenantFileSuperBlockLogEntry() : key_(), delete_file_(false)
  {}
  ObRemoveTenantFileSuperBlockLogEntry(const ObTenantFileKey& key, const bool delete_file)
      : key_(key), delete_file_(delete_file)
  {}
  virtual ~ObRemoveTenantFileSuperBlockLogEntry() = default;
  bool is_valid() const
  {
    return key_.is_valid();
  }
  ObTenantFileKey key_;
  bool delete_file_;
  TO_STRING_KV(K_(key));
  OB_UNIS_VERSION_V(REMOVE_TENANT_FILE_SUPER_BLOCK_VERSION);
};

struct ObAddPGToTenantFileLogEntry : public blocksstable::ObIBaseStorageLogEntry {
public:
  static const int64_t ADD_PG_TO_TENANT_FILE_VERSION = 1;
  ObAddPGToTenantFileLogEntry() : file_key_(), pg_key_()
  {}
  ObAddPGToTenantFileLogEntry(const ObTenantFileKey& file_key, const common::ObPGKey& pg_key)
      : file_key_(file_key), pg_key_(pg_key)
  {}
  virtual ~ObAddPGToTenantFileLogEntry() = default;
  bool is_valid() const
  {
    return file_key_.is_valid() && pg_key_.is_valid();
  }
  TO_STRING_KV(K_(file_key), K_(pg_key));
  ObTenantFileKey file_key_;
  common::ObPGKey pg_key_;
  OB_UNIS_VERSION_V(ADD_PG_TO_TENANT_FILE_VERSION);
};

struct ObRemovePGFromTenantFileLogEntry : public blocksstable::ObIBaseStorageLogEntry {
public:
  static const int64_t REMOVE_PG_FROM_TENANT_FILE_VERSION = 1;
  ObRemovePGFromTenantFileLogEntry() : file_key_(), pg_key_()
  {}
  ObRemovePGFromTenantFileLogEntry(const ObTenantFileKey& file_key, const common::ObPGKey& pg_key)
      : file_key_(file_key), pg_key_(pg_key)
  {}
  virtual ~ObRemovePGFromTenantFileLogEntry() = default;
  bool is_valid() const
  {
    return file_key_.is_valid() && pg_key_.is_valid();
  }
  TO_STRING_KV(K_(file_key), K_(pg_key));
  ObTenantFileKey file_key_;
  common::ObPGKey pg_key_;
  OB_UNIS_VERSION_V(REMOVE_PG_FROM_TENANT_FILE_VERSION);
};

struct ObUpdateTenantFileInfoLogEntry : public blocksstable::ObIBaseStorageLogEntry {
public:
  static const int64_t UPDATE_TENANT_FILE_INFO_VERSION = 1;
  ObUpdateTenantFileInfoLogEntry() : file_info_()
  {}
  virtual ~ObUpdateTenantFileInfoLogEntry() = default;
  bool is_valid() const
  {
    return file_info_.is_valid();
  }
  ObTenantFileInfo file_info_;
  TO_STRING_KV(K_(file_info));
  OB_UNIS_VERSION_V(UPDATE_TENANT_FILE_INFO_VERSION);
};
}  // end namespace storage
}  // end namespace oceanbase
#endif  // OB_SERVER_LOG_H_
