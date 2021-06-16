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

#ifndef OB_BACKUP_META_DATA_MGR_H
#define OB_BACKUP_META_DATA_MGR_H

#include "lib/allocator/ob_malloc.h"
#include "lib/list/ob_dlist.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "storage/ob_i_table.h"
#include "storage/ob_storage_struct.h"
#include "storage/blocksstable/ob_store_file_system.h"

namespace oceanbase {
namespace storage {
class ObPGSSTableMgr;
class ObRecoveryPointInfo;

class ObRecoveryTableData {
public:
  static const int64_t OB_RECOVERY_TABLE_DATA_VERSION = 1;
  ObRecoveryTableData();
  virtual ~ObRecoveryTableData();
  int init(const uint64_t tenant_id, const ObTablesHandle& tables_handle);
  void reset();
  int get_tables(ObTablesHandle& handle);
  int get_tables(const ObPartitionKey& pkey, ObTablesHandle& handle);
  int get_table(const ObITable::TableKey& table_key, ObTableHandle& handle);
  int replay(
      const uint64_t tenant_id, const common::ObIArray<ObITable::TableKey>& table_keys, ObPGSSTableMgr& sstable_mgr);
  bool is_inited() const
  {
    return is_inited_;
  }
  bool is_valid() const;
  int is_below_given_snapshot(const int64_t snapshot_version, bool& is_below);
  TO_STRING_KV(K(tables_.count()));
  OB_UNIS_VERSION(OB_RECOVERY_TABLE_DATA_VERSION);

private:
  // members won't be serialized
  bool is_inited_;
  common::ObFixedArray<ObITable*> tables_;
  common::ObMalloc allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObRecoveryTableData);
};

class ObRecoveryPointData : public ObDLinkBase<ObRecoveryPointData> {
public:
  typedef common::ObSEArray<ObITable::TableKey, 1> TableKeyArray;
  static const int64_t OB_RECOVERY_POINT_DATA_VERSION = 1;
  static const int64_t MAX_TABLE_CNT_IN_BUCKET = 10;
  ObRecoveryPointData();
  virtual ~ObRecoveryPointData();
  bool is_inited() const
  {
    return is_inited_;
  }
  uint64_t get_tenant_id() const
  {
    return pg_meta_.pg_key_.get_tenant_id();
  }
  int64_t get_snapshot_version() const
  {
    return snapshot_version_;
  };
  bool is_valid() const;
  int init(const int64_t snapshot_version, const ObPartitionGroupMeta& pg_meta,
      const ObIArray<ObPGPartitionStoreMeta>& partition_store_metas);
  void reset();
  int check_table_exist(const int64_t table_id, bool& exist);
  int add_table_sstables(const int64_t table_id, const ObTablesHandle& handle);
  int get_table_sstables(const int64_t table_id, ObTablesHandle& handle) const;
  int clear_table_sstables(const int64_t table_id);
  int add_sstables(const ObTablesHandle& handle);
  int get_all_tables(ObTablesHandle& handle) const;
  ObPGKey get_pg_key() const
  {
    return pg_meta_.pg_key_;
  }
  // for replay
  int finish_replay(ObPGSSTableMgr& sstable_mgr);
  int assign(const ObRecoveryPointData& other);
  int serialize(char* buf, const int64_t serialize_size, int64_t& pos) const;
  int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
  int deserialize_assign(const ObRecoveryPointData& other);
  int64_t get_serialize_size() const;
  int get_table_keys_map(common::hash::ObHashMap<int64_t, TableKeyArray>& table_keys_map) const;
  // for backup
  const ObPartitionGroupMeta& get_pg_meta() const
  {
    return pg_meta_;
  }
  int is_below_given_snapshot(const int64_t snapshot_version, bool& is_below);
  int64_t get_last_replay_log_ts() const
  {
    return pg_meta_.storage_info_.get_data_info().get_last_replay_log_ts();
  }
  int get_partition_store_meta(const ObPartitionKey& pkey, ObPGPartitionStoreMeta& partition_store_meta);
  int get_sstable(const ObITable::TableKey& table_key, ObTableHandle& handle);
  int get_partition_tables(const ObPartitionKey& pkey, ObTablesHandle& handle);

  DECLARE_VIRTUAL_TO_STRING;

private:
  int add_table_(const int64_t table_id, const TableKeyArray& table_keys, const ObTablesHandle& tables_handle);
  void clear_table_map_();
  void clear_table_keys_map_();
  int64_t get_table_id(ObITable* table)
  {
    ObITable::TableKey table_key = table->get_key();
    return table_key.table_id_;
  }
  void free_table_data_(ObRecoveryTableData* table_data);

private:
  // members won't be serialized
  bool is_inited_;
  common::hash::ObHashMap<int64_t, ObRecoveryTableData*> table_map_;
  // TODO: Fix me: the allocation is not FIFO, pages may not be released because of small piece memory in use.
  common::ObMalloc allocator_;

  // members to be serialized
  int64_t snapshot_version_;  // the restore point snapshot version
  ObPartitionGroupMeta pg_meta_;
  common::ObSEArray<ObPGPartitionStoreMeta, 1> partition_store_metas_;
  common::hash::ObHashMap<int64_t, TableKeyArray> table_keys_map_;

  DISALLOW_COPY_AND_ASSIGN(ObRecoveryPointData);
};

// WARNING: not thread safe, must be protected
class ObRecoveryData {
public:
  ObRecoveryData();
  virtual ~ObRecoveryData();
  void destroy();
  int init(const ObPartitionKey& pg_key);
  ObRecoveryPointData* get_header()
  {
    return recover_point_list_.get_header();
  }
  ObRecoveryPointData* get_first()
  {
    return recover_point_list_.get_first();
  }
  // create a recovery point but not insert into recover_point_list_
  int create_recovery_point(const int64_t snapshot_version, const ObPartitionGroupMeta& pg_meta,
      const ObIArray<ObPGPartitionStoreMeta>& partition_store_metas, const ObTablesHandle& tables_handle,
      ObRecoveryPointData*& new_data);
  void free_recovery_point(ObRecoveryPointData* new_data);
  int insert_recovery_point(ObRecoveryPointData* new_data);
  int remove_recovery_point(ObRecoveryPointData* del_data);
  int check_recovery_point_exist(const int64_t snapshot_version, bool& is_exist) const;
  int check_recovery_point_exist(const int64_t table_id, const int64_t snapshot_version, bool& is_exist);
  int get_recovery_point(const int64_t snapshot_version, ObRecoveryPointData*& point_data);
  int get_recovery_point_below_given_snapshot(const int64_t snapshot_version, ObRecoveryPointData*& point_data);
  int get_unneed_recovery_points(const ObIArray<int64_t>& versions_needs, const int64_t snapshot_gc_ts,
      ObIArray<ObRecoveryPointData*>& points_data);
  int get_unneed_recovery_points(
      const int64_t smaller_snapshot, const int64_t larger_snapshot, ObIArray<ObRecoveryPointData*>& points_data);
  int get_recovery_point_data(const int64_t snapshot_version,  // maybe deleted
      ObTablesHandle& handle);
  int get_recovery_point_table_data(
      const int64_t table_id, const int64_t snapshot_version, ObTablesHandle& handle) const;
  int update_recovery_point(const int64_t table_id, const int64_t snapshot_version, ObTablesHandle& handle);
  int64_t get_serialize_size();
  // for replay
  int serialize(common::ObIAllocator& allocator, char*& buf, int64_t& serialize_size);
  int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
  int replay_add_recovery_point(const ObRecoveryPointData& point_data);
  int replay_remove_recovery_point(ObRecoveryPointData* del_data);
  int64_t get_recovery_point_cnt()
  {
    return recover_point_list_.get_size();
  }
  void set_pg_key(ObPGKey& pg_key)
  {
    pg_key_ = pg_key;
  }
  int finish_replay(ObPGSSTableMgr& pg_sstable_mgr);
  int remove_recovery_points(const ObIArray<ObRecoveryPointData*>& point_list);
  DECLARE_VIRTUAL_TO_STRING;

private:
  static const int64_t OB_RECOVERY_DATA_VERSION = 1;
  ObPartitionKey pg_key_;
  ObDList<ObRecoveryPointData> recover_point_list_;
  common::ObMalloc allocator_;

  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObRecoveryData);
};

enum ObRecoveryPointType {
  UNKNOWN_TYPE = 0,
  RESTORE_POINT = 1,
  BACKUP = 2,
};
class ObRecoveryDataMgr {
public:
  struct SerializePair {
    SerializePair(char* buf, int64_t size) : buf_(buf), size_(size)
    {}
    SerializePair() : buf_(nullptr), size_(0)
    {}
    TO_STRING_KV(KP_(buf), K_(size));
    char* buf_;
    int64_t size_;
  };
  ObRecoveryDataMgr();
  virtual ~ObRecoveryDataMgr();
  void destroy();
  int init(const ObPartitionKey& pg_key);
  int enable_write_slog(ObPGSSTableMgr& pg_sstable_mgr);
  int serialize(common::ObIAllocator& allocator, char*& buf, int64_t& serialize_size);
  int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
  int replay_add_recovery_point(const ObRecoveryPointType point_type, const ObRecoveryPointData& point_data);
  int replay_remove_recovery_point(const ObRecoveryPointType point_type, const int64_t snapshot_version);
  OB_INLINE int set_storage_file_handle(blocksstable::ObStorageFileHandle& file_handle)
  {
    return file_handle_.assign(file_handle);
  }
  void set_pg_key(ObPGKey& pg_key)
  {
    pg_key_ = pg_key;
    restore_point_data_.set_pg_key(pg_key);
    backup_point_data_.set_pg_key(pg_key);
  }
  int64_t get_recovery_point_cnt()
  {
    TCRLockGuard lock_guard(lock_);
    return restore_point_data_.get_recovery_point_cnt() + backup_point_data_.get_recovery_point_cnt();
  }
  int get_all_points_info(ObIArray<ObRecoveryPointInfo>& points_info);
  // for restore point
  int add_restore_point(const int64_t snapshot_version, const ObPartitionGroupMeta& pg_meta,
      const ObIArray<ObPGPartitionStoreMeta>& partition_store_metas, const ObTablesHandle& tables_handle);
  int check_restore_point_exist(const int64_t snapshot_version, bool& is_exist);
  int get_restore_point_read_tables(const int64_t table_id, const int64_t snapshot_version, ObTablesHandle& handle);
  int remove_unneed_restore_point(const ObIArray<int64_t>& versions_need_left, const int64_t snapshot_gc_ts);
  int get_restore_point_start_version(int64_t& start_version);
  int set_restore_point_start_version(const int64_t start_version);

  // for backup
  int add_backup_point(const int64_t snapshot_version, const ObPartitionGroupMeta& pg_meta,
      const ObIArray<ObPGPartitionStoreMeta>& partition_store_metas, const ObTablesHandle& tables_handle);
  int get_backup_point_data(const int64_t snapshot_version, ObPartitionGroupMeta& pg_meta, ObTablesHandle& handle);
  int remove_unneed_backup_point(const ObIArray<int64_t>& versions_need_left, const int64_t snapshot_gc_ts);
  int get_backup_pg_meta_data(const int64_t snapshot_version, ObPartitionGroupMeta& pg_meta);
  int get_backup_partition_meta_data(const ObPartitionKey& pkey, const int64_t snapshot_version,
      ObPGPartitionStoreMeta& partition_store_meta, ObTablesHandle& handle);
  int get_backup_sstable(const int64_t snapshot_version, const ObITable::TableKey& table_key, ObTableHandle& handle);
  int check_backup_point_exist(const int64_t snapshot_version, bool& is_exist);
  int get_all_backup_tables(const int64_t snapshot_version, ObTablesHandle& handle);

  TO_STRING_KV(K_(pg_key), K_(restore_point_data), K_(backup_point_data));

private:
  int add_recovery_point_(const ObRecoveryPointType point_type, const int64_t snapshot_version,
      const ObPartitionGroupMeta& pg_meta, const ObIArray<ObPGPartitionStoreMeta>& partition_store_metas,
      const ObTablesHandle& tables_handle, ObRecoveryData& recovery_data);
  int write_add_data_slog_(const ObRecoveryPointType point_type, ObRecoveryPointData& point_data);
  int write_remove_data_slogs_(const ObRecoveryPointType point_type, ObIArray<ObRecoveryPointData*>& points_data);
  // for restore point
  int replay_add_restore_point_(const ObRecoveryPointData& point_data);
  int replay_remove_restore_point_(const int64_t snapshot_version);
  // for backup
  int replay_add_backup_point_(const ObRecoveryPointData& point_data);
  int replay_remove_backup_point_(const int64_t replay_log_ts);

private:
  static const int64_t OLD_MAGIC_NUM = -0xABCD;
  static const int64_t MAGIC_NUM = -0xABCE;
  static const int64_t OB_RECOVERY_DATA_MGR_VERSION = 1;
  common::TCRWLock lock_;  // used to protect restore_point_data_ and backup_point_data_
  bool enable_write_slog_;
  int64_t log_seq_num_;
  ObPartitionKey pg_key_;
  blocksstable::ObStorageFileHandle file_handle_;
  bool is_inited_;

  ObRecoveryData restore_point_data_;
  ObRecoveryData backup_point_data_;

  // no need serialize
  int64_t restore_point_start_version_;
  DISALLOW_COPY_AND_ASSIGN(ObRecoveryDataMgr);
};

class ObRecoveryPointInfo {
public:
  ObRecoveryPointInfo() : type_(ObRecoveryPointType::UNKNOWN_TYPE), tables_handle_(), snapshot_version_(0)
  {}
  virtual ~ObRecoveryPointInfo()
  {
    reset();
  }
  void reset()
  {
    type_ = ObRecoveryPointType::UNKNOWN_TYPE;
    tables_handle_.reset();
    snapshot_version_ = 0;
  }
  int init(const ObRecoveryPointType type, ObRecoveryPointData* point);
  int64_t get_snapshot_version() const
  {
    return snapshot_version_;
  }
  void get_type(ObRecoveryPointType& type) const
  {
    type = type_;
  }
  int get_tables(ObTablesHandle& handle) const;
  int assign(const ObRecoveryPointInfo& other);
  TO_STRING_KV(K_(type), K(tables_handle_.get_count()), K_(snapshot_version));

private:
  ObRecoveryPointType type_;
  ObTablesHandle tables_handle_;
  int64_t snapshot_version_;
};
// Iterate all the recovery points in one PG
class ObIRecoveryPointIterator {
public:
  ObIRecoveryPointIterator()
  {}
  virtual ~ObIRecoveryPointIterator()
  {}
  virtual int get_next(ObRecoveryPointInfo*& point_info) = 0;
};

class ObRecoveryPointIterator : public ObIRecoveryPointIterator {
public:
  ObRecoveryPointIterator() : points_info_(), array_idx_(0), data_mgr_(NULL)
  {}
  virtual ~ObRecoveryPointIterator()
  {
    reset();
  }
  virtual int get_next(ObRecoveryPointInfo*& point_info);
  void reset()
  {
    points_info_.reuse();
    array_idx_ = 0;
    data_mgr_ = NULL;
  }
  int init(ObRecoveryDataMgr& data_mgr);
  bool is_valid()
  {
    return data_mgr_ != NULL;
  }

private:
  common::ObArray<ObRecoveryPointInfo> points_info_;
  int64_t array_idx_;
  ObRecoveryDataMgr* data_mgr_;
};

}  // namespace storage
}  // namespace oceanbase
#endif
