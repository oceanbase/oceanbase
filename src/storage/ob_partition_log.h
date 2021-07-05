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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_LOG_H_
#define OCEANBASE_STORAGE_OB_PARTITION_LOG_H_

#include "common/ob_partition_key.h"
#include "common/ob_range.h"

#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/slog/ob_base_storage_logger.h"
#include "storage/ob_storage_struct.h"
#include "storage/ob_partition_split.h"
#include "storage/ob_table_store.h"
#include "share/ob_unit_getter.h"
#include "storage/ob_old_sstable.h"
#include "storage/ob_reserved_data_mgr.h"

namespace oceanbase {
namespace storage {
enum ObPartitionRedoLogSubcmd {
  REDO_LOG_ADD_STORE = 0,     // deprecated in 2.0
  REDO_LOG_REMOVE_STORE = 1,  // deprecated in 2.0
  REDO_LOG_ADD_PARTITION = 2,
  REDO_LOG_REMOVE_PARTITION = 3,
  REDO_LOG_SET_MIGRATE_IN_STATUS = 4,  // deprecated in 3.0
  REDO_LOG_SET_REPLICA_STATUS = 5,     // deprecated in 2.0
  REDO_LOG_SET_REPLICA_TYPE = 6,

  // for ob_table_mgr
  REDO_LOG_CREATE_SSTABLE = 7,    // added in 2.0
  REDO_LOG_COMPELTE_SSTABLE = 8,  // added in 2.0
  REDO_LOG_DELETE_SSTABLE = 9,    // added in 2.0

  // for partition store
  REDO_LOG_CREATE_PARTITION_STORE = 10,       // deprecated in 3.0
  REDO_LOG_MODIFY_TABLE_STORE = 11,           // added in 2.0
  REDO_LOG_DROP_INDEX_SSTABLE_OF_STORE = 12,  // added in 2.0
  REDO_LOG_UPDATE_PARTITION_META = 13,        // deprecated in 3.0

  // for partition split
  REDO_LOG_SET_PARTITION_SPLIT_STATE = 20,  // added in 2.0
  REDO_LOG_SET_PARTITION_SPLIT_INFO = 21,   // added in 2.0

  // for tenant config
  REDO_LOG_UPDATE_TENANT_CONFIG = 22,  // added in 2.0

  // for partition group
  REDO_LOG_CREATE_PARTITION_GROUP = 23,
  REDO_LOG_ADD_PARTITION_TO_PG = 24,
  REDO_LOG_UPDATE_PARTITION_GROUP_META = 25,
  REDO_LOG_REMOVE_PARTITION_FROM_PG = 26,
  REDO_LOG_CREATE_PG_PARTITION_STORE = 27,
  REDO_LOG_UPDATE_PG_PARTITION_META = 28,
  REDO_LOG_ADD_PARTITION_GROUP = 29,

  // for new sstable log
  REDO_LOG_ADD_SSTABLE = 31,
  REDO_LOG_REMOVE_SSTABLE = 32,
  REDO_LOG_CHANGE_MACRO_META = 33,
  REDO_LOG_SET_START_LOG_TS_AFTER_MAJOR = 34,

  // for backup meta data
  REDO_LOG_ADD_BACKUP_META_DATA = 35,
  REDO_LOG_REMOVE_BACKUP_META_DATA = 36,

  // for load file checkpoint
  REDO_LOG_WRITE_FILE_CHECKPOINT = 37,

  // for recovery point data
  // DATE: 2020-09-21 (since 2.2.70)
  REDO_LOG_ADD_RECOVERY_POINT_DATA = 38,
  REDO_LOG_REMOVE_RECOVERY_POINT_DATA = 39,
};

struct ObBeginTransLogEntry : public blocksstable::ObIBaseStorageLogEntry {
  // For compatibility, the variables in this struct MUST NOT be deleted or moved.
  // You should ONLY add variables at the end.
  // Note that if you use complex structure as variables, the complex structure should also keep compatibility.

  ObBeginTransLogEntry();
  virtual ~ObBeginTransLogEntry();
  virtual bool is_valid() const override;
  virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const override;
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos) override;
  virtual int64_t get_serialize_size() const override;
  virtual int64_t to_string(char* buf, const int64_t buf_len) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBeginTransLogEntry);
};

struct ObChangePartitionLogEntry : public blocksstable::ObIBaseStorageLogEntry {
  // For compatibility, the variables in this struct MUST NOT be deleted or moved.
  // You should ONLY add variables at the end.
  // Note that if you use complex structure as variables, the complex structure should also keep compatibility.
public:
  static const int64_t CHANGE_PARTITION_LOG_VERSION = 1;
  common::ObPartitionKey partition_key_;
  common::ObReplicaType replica_type_;
  common::ObPGKey pg_key_;
  uint64_t log_id_;

  ObChangePartitionLogEntry();
  virtual ~ObChangePartitionLogEntry();
  VIRTUAL_TO_STRING_KV(K_(partition_key), K_(replica_type), K_(pg_key), K_(log_id));
  virtual bool is_valid() const;
  OB_UNIS_VERSION_V(CHANGE_PARTITION_LOG_VERSION);

private:
  DISALLOW_COPY_AND_ASSIGN(ObChangePartitionLogEntry);
};

struct ObChangePartitionStorageLogEntry : public blocksstable::ObIBaseStorageLogEntry {
  // For compatibility, the variables in this struct MUST NOT be deleted or moved.
  // You should ONLY add variables at the end.
  // Note that if you use complex structure as variables, the complex structure should also keep compatibility.
public:
  static const int64_t CHANGE_PARTITION_STORAGE_LOG_VERSION = 1;
  common::ObPartitionKey partition_key_;

  ObChangePartitionStorageLogEntry();
  virtual ~ObChangePartitionStorageLogEntry();
  VIRTUAL_TO_STRING_KV(K_(partition_key));
  virtual bool is_valid() const;
  OB_UNIS_VERSION_V(CHANGE_PARTITION_STORAGE_LOG_VERSION);

private:
  DISALLOW_COPY_AND_ASSIGN(ObChangePartitionStorageLogEntry);
};

// new logs for 2.x
struct ObCreateSSTableLogEntry : public blocksstable::ObIBaseStorageLogEntry {
  ObSSTable& sstable_;
  static const int64_t CREATE_SSTABLE_LOG_VERSION = 1;
  ObCreateSSTableLogEntry(ObSSTable& sstable);
  bool is_valid() const;
  TO_STRING_KV(K_(sstable));
  OB_UNIS_VERSION_V(CREATE_SSTABLE_LOG_VERSION);
};

struct ObCompleteSSTableLogEntry : public blocksstable::ObIBaseStorageLogEntry {
  ObOldSSTable& sstable_;
  static const int64_t COMPLETE_SSTABLE_LOG_VERSION = 1;
  ObCompleteSSTableLogEntry(ObOldSSTable& sstable);
  bool is_valid() const;
  TO_STRING_KV(K_(sstable));
  OB_UNIS_VERSION_V(COMPLETE_SSTABLE_LOG_VERSION);
};
struct ObDeleteSSTableLogEntry : public blocksstable::ObIBaseStorageLogEntry {
  ObITable::TableKey table_key_;
  static const int64_t DELETE_SSTABLE_LOG_VERSION = 1;
  ObDeleteSSTableLogEntry();
  bool is_valid() const;
  TO_STRING_KV(K_(table_key));
  OB_UNIS_VERSION_V(DELETE_SSTABLE_LOG_VERSION);
};

struct ObAddSSTableLogEntry : public blocksstable::ObIBaseStorageLogEntry {
public:
  static const int64_t ADD_SSTABLE_LOG_VERSION = 1;
  ObAddSSTableLogEntry(const common::ObPGKey& pg_key, ObSSTable& sstable);
  explicit ObAddSSTableLogEntry(ObSSTable& sstable);
  virtual ~ObAddSSTableLogEntry() = default;
  bool is_valid() const;
  TO_STRING_KV(K_(pg_key), K_(sstable));
  OB_UNIS_VERSION_V(ADD_SSTABLE_LOG_VERSION);

public:
  common::ObPGKey pg_key_;
  ObSSTable& sstable_;
};

struct ObRemoveSSTableLogEntry : public blocksstable::ObIBaseStorageLogEntry {
public:
  static const int64_t REMOVE_SSTABLE_LOG_VERSION = 1;
  ObRemoveSSTableLogEntry();
  ObRemoveSSTableLogEntry(const ObPGKey& pg_key, const ObITable::TableKey& table_key);
  virtual ~ObRemoveSSTableLogEntry() = default;
  bool is_valid() const;
  TO_STRING_KV(K_(pg_key), K_(table_key));
  OB_UNIS_VERSION_V(REMOVE_SSTABLE_LOG_VERSION);

public:
  common::ObPGKey pg_key_;
  ObITable::TableKey table_key_;
};

struct ObCreatePartitionStoreLogEntry : public blocksstable::ObIBaseStorageLogEntry {
  ObPartitionStoreMeta meta_;
  static const int64_t ADD_SSTABLE_LOG_VERSION = 1;
  ObCreatePartitionStoreLogEntry();
  bool is_valid() const;
  TO_STRING_KV(K_(meta));
  OB_UNIS_VERSION_V(ADD_SSTABLE_LOG_VERSION);
};

struct ObModifyTableStoreLogEntry : public blocksstable::ObIBaseStorageLogEntry {
  int64_t kept_multi_version_start_;
  ObTableStore& table_store_;
  common::ObPGKey pg_key_;
  static const int64_t MODIFY_TABLE_STORE_LOG_VERSION = 1;
  ObModifyTableStoreLogEntry(ObTableStore& table_store);
  bool is_valid() const;
  TO_STRING_KV(K_(kept_multi_version_start), K_(table_store), K_(pg_key));
  OB_UNIS_VERSION_V(MODIFY_TABLE_STORE_LOG_VERSION);
};

struct ObDropIndexSSTableLogEntry : public blocksstable::ObIBaseStorageLogEntry {
  common::ObPartitionKey pkey_;
  uint64_t index_id_;
  common::ObPGKey pg_key_;
  static const int64_t DROP_INDEX_SSTABLE_LOG_VERSION = 1;
  ObDropIndexSSTableLogEntry();
  bool is_valid() const;
  TO_STRING_KV(K_(pkey), K_(index_id), K_(pg_key));
  OB_UNIS_VERSION_V(DROP_INDEX_SSTABLE_LOG_VERSION);
};

struct ObUpdatePartitionMetaLogEntry : public blocksstable::ObIBaseStorageLogEntry {
  ObPartitionStoreMeta meta_;
  static const int64_t UPDATE_PARTITION_META_LOG_VERSION = 1;
  ObUpdatePartitionMetaLogEntry();
  bool is_valid() const;
  TO_STRING_KV(K_(meta));
  OB_UNIS_VERSION_V(UPDATE_PARTITION_META_LOG_VERSION);
};

struct ObSplitPartitionStateLogEntry : public blocksstable::ObIBaseStorageLogEntry {
  OB_UNIS_VERSION_V(1);

public:
  ObSplitPartitionStateLogEntry() : pkey_(), state_(UNKNOWN_SPLIT_STATE)
  {}
  ~ObSplitPartitionStateLogEntry()
  {}
  int init(const common::ObPartitionKey& pkey, const int64_t state);
  ObPartitionSplitStateEnum get_state() const
  {
    return static_cast<ObPartitionSplitStateEnum>(state_);
  }
  const common::ObPartitionKey get_pkey() const
  {
    return pkey_;
  }
  bool is_valid() const
  {
    return pkey_.is_valid() && is_valid_split_state(static_cast<ObPartitionSplitStateEnum>(state_));
  }
  TO_STRING_KV(K_(pkey), "state", to_state_str(static_cast<ObPartitionSplitStateEnum>(state_)));

private:
  common::ObPartitionKey pkey_;
  int64_t state_;
};

struct ObSplitPartitionInfoLogEntry : public blocksstable::ObIBaseStorageLogEntry {
  OB_UNIS_VERSION_V(1);

public:
  ObSplitPartitionInfoLogEntry() : pkey_(), split_info_()
  {}
  ~ObSplitPartitionInfoLogEntry()
  {}
  int init(const common::ObPartitionKey& pkey, const ObPartitionSplitInfo& split_info);
  const common::ObPartitionKey get_pkey() const
  {
    return pkey_;
  }
  const ObPartitionSplitInfo& get_split_info() const
  {
    return split_info_;
  }
  bool is_valid() const
  {
    return pkey_.is_valid() && split_info_.is_valid();
  }
  TO_STRING_KV(K_(pkey), K_(split_info));

private:
  common::ObPartitionKey pkey_;
  ObPartitionSplitInfo split_info_;
};

struct ObUpdateTenantConfigLogEntry : public blocksstable::ObIBaseStorageLogEntry {
  OB_UNIS_VERSION_V(1);

public:
  explicit ObUpdateTenantConfigLogEntry(share::TenantUnits& units);
  virtual ~ObUpdateTenantConfigLogEntry()
  {}
  bool is_valid() const
  {
    return units_.count() >= 0;
  }
  TO_STRING_KV(K_(units));

private:
  share::TenantUnits& units_;
};

struct ObCreatePartitionGroupLogEntry : public blocksstable::ObIBaseStorageLogEntry {
  ObPartitionGroupMeta meta_;
  static const int64_t ADD_PARTITION_GROUP_VERSION = 1;
  ObCreatePartitionGroupLogEntry();
  bool is_valid() const;
  TO_STRING_KV(K_(meta));
  OB_UNIS_VERSION_V(ADD_PARTITION_GROUP_VERSION);
};

struct ObUpdatePartitionGroupMetaLogEntry : public blocksstable::ObIBaseStorageLogEntry {
  ObPartitionGroupMeta meta_;
  static const int64_t UPDATE_PARTITION_GROUP_META_LOG_VERSION = 1;
  ObUpdatePartitionGroupMetaLogEntry();
  bool is_valid() const;
  TO_STRING_KV(K_(meta));
  OB_UNIS_VERSION_V(UPDATE_PARTITION_GROUP_META_LOG_VERSION);
};

struct ObCreatePGPartitionStoreLogEntry : public blocksstable::ObIBaseStorageLogEntry {
  ObPGPartitionStoreMeta meta_;
  common::ObPGKey pg_key_;
  static const int64_t CREATE_PG_PARTITION_STORE_LOG_VERSION = 1;
  ObCreatePGPartitionStoreLogEntry();
  bool is_valid() const;
  TO_STRING_KV(K_(meta), K_(pg_key));
  OB_UNIS_VERSION_V(CREATE_PG_PARTITION_STORE_LOG_VERSION);
};

struct ObUpdatePGPartitionMetaLogEntry : public blocksstable::ObIBaseStorageLogEntry {
  ObPGPartitionStoreMeta meta_;
  common::ObPGKey pg_key_;
  static const int64_t UPDATE_PG_PARTITION_META_LOG_VERSION = 1;
  ObUpdatePGPartitionMetaLogEntry();
  bool is_valid() const;
  TO_STRING_KV(K_(meta), K_(pg_key));
  OB_UNIS_VERSION_V(UPDATE_PG_PARTITION_META_LOG_VERSION);
};

struct ObPGMacroBlockMetaLogEntry : public blocksstable::ObIBaseStorageLogEntry {
public:
  static const int64_t PG_MACRO_BLOCK_META_VERSION = 1;
  ObPGMacroBlockMetaLogEntry(const common::ObPGKey& pg_key, const ObITable::TableKey& table_key,
      const int64_t data_file_id, const int64_t disk_no, const blocksstable::MacroBlockId& macro_block_id,
      blocksstable::ObMacroBlockMetaV2& meta);
  ObPGMacroBlockMetaLogEntry(blocksstable::ObMacroBlockMetaV2& meta);
  virtual ~ObPGMacroBlockMetaLogEntry() = default;
  bool is_valid() const
  {
    return pg_key_.is_valid() && table_key_.is_valid() && macro_block_id_.is_valid() && meta_.is_valid();
  }
  TO_STRING_KV(K_(pg_key), K_(table_key), K_(data_file_id), K_(disk_no), K_(macro_block_id), K_(meta));
  OB_UNIS_VERSION_V(PG_MACRO_BLOCK_META_VERSION);

public:
  ObPGKey pg_key_;
  ObITable::TableKey table_key_;
  int64_t data_file_id_;
  int64_t disk_no_;
  blocksstable::MacroBlockId macro_block_id_;
  blocksstable::ObMacroBlockMetaV2& meta_;
  DISALLOW_COPY_AND_ASSIGN(ObPGMacroBlockMetaLogEntry);
};

struct ObSetStartLogTsAfterMajorLogEntry : public blocksstable::ObIBaseStorageLogEntry {
public:
  static const int64_t SET_START_LOG_TS_AFTER_MAJOR_VERSION = 1;
  explicit ObSetStartLogTsAfterMajorLogEntry(
      const common::ObPGKey& pg_key, const ObITable::TableKey& table_key, const int64_t start_log_ts_after_major);
  ObSetStartLogTsAfterMajorLogEntry() : start_log_ts_after_major_(0)
  {}
  virtual ~ObSetStartLogTsAfterMajorLogEntry() = default;
  bool is_valid() const;
  TO_STRING_KV(K_(pg_key), K_(table_key), K_(start_log_ts_after_major));
  OB_UNIS_VERSION(SET_START_LOG_TS_AFTER_MAJOR_VERSION);

public:
  ObPGKey pg_key_;
  ObITable::TableKey table_key_;
  int64_t start_log_ts_after_major_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSetStartLogTsAfterMajorLogEntry);
};

struct ObAddRecoveryPointDataLogEntry : public blocksstable::ObIBaseStorageLogEntry {
public:
  static const int64_t ADD_RECOVERY_POINT_DATA_LOG_VERSION = 1;
  explicit ObAddRecoveryPointDataLogEntry(const ObRecoveryPointType point_type, ObRecoveryPointData& point_data);
  virtual ~ObAddRecoveryPointDataLogEntry() = default;
  bool is_valid() const;
  TO_STRING_KV(K_(point_type), K_(point_data));
  OB_UNIS_VERSION(ADD_RECOVERY_POINT_DATA_LOG_VERSION);

public:
  ObRecoveryPointType point_type_;
  ObRecoveryPointData& point_data_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAddRecoveryPointDataLogEntry);
};

struct ObRemoveRecoveryPointDataLogEntry : public blocksstable::ObIBaseStorageLogEntry {
public:
  static const int64_t REMOVE_RECOVERY_POINT_DATA_LOG_VERSION = 1;
  ObRemoveRecoveryPointDataLogEntry();
  explicit ObRemoveRecoveryPointDataLogEntry(
      const ObRecoveryPointType point_type, const ObRecoveryPointData& meta_data);
  virtual ~ObRemoveRecoveryPointDataLogEntry() = default;
  bool is_valid() const;
  TO_STRING_KV(K_(point_type), K_(pg_key), K_(snapshot_version));
  OB_UNIS_VERSION(REMOVE_RECOVERY_POINT_DATA_LOG_VERSION);

public:
  ObRecoveryPointType point_type_;
  ObPGKey pg_key_;
  int64_t snapshot_version_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoveRecoveryPointDataLogEntry);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_I_PARTITION_STORAGE_H_
