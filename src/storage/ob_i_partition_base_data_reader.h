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

#ifndef OCEANBASE_STORAGE_OB_I_PARTITION_BASE_DATA_READER_H_
#define OCEANBASE_STORAGE_OB_I_PARTITION_BASE_DATA_READER_H_

#include "lib/utility/ob_macro_utils.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "storage/ob_partition_service_rpc.h"
#include "storage/ob_i_store.h"
#include "ob_i_table.h"
#include "ob_storage_struct.h"

namespace oceanbase {
namespace storage {

struct ObMigrateArgMacroBlockInfo {
  ObMigrateArgMacroBlockInfo() : fetch_arg_(), macro_block_id_()
  {}
  void reset();
  TO_STRING_KV(K_(fetch_arg), K_(macro_block_id));

  obrpc::ObFetchMacroBlockArg fetch_arg_;
  blocksstable::MacroBlockId macro_block_id_;
};

struct ObMigrateMacroBlockInfo {
  ObMigrateMacroBlockInfo() : pair_(), macro_block_id_(), full_meta_(), need_copy_(true)
  {}
  TO_STRING_KV(K_(pair), K_(macro_block_id), K_(need_copy));
  blocksstable::ObSSTablePair pair_;
  blocksstable::MacroBlockId macro_block_id_;
  blocksstable::ObFullMacroBlockMeta full_meta_;
  bool need_copy_;
};

struct ObMigrateSSTableInfo {
  ObMigrateSSTableInfo();
  TO_STRING_KV(K_(is_inited), K_(pending_idx), K_(sstable_meta), K_(macro_block_array));
  int assign(const ObMigrateSSTableInfo& sstable_info);

  bool is_inited_;
  int64_t pending_idx_;
  ObITable::TableKey table_key_;
  blocksstable::ObSSTableBaseMeta sstable_meta_;
  common::ObArray<ObMigrateMacroBlockInfo> macro_block_array_;
};

// ObPartitionMacroBlockObReader (storage/ob_partition_base_data_ob_reader.cpp): data comes from remote ob
// ObPartitionMacroBlockOSSReader (not implemented): data comes from backup oss
class ObIPartitionMacroBlockReader {
public:
  enum Type {
    MACRO_BLOCK_OB_READER = 0,
    MACRO_BLOCK_RESTORE_READER = 1,
    MACRO_BLOCK_BACKUP_READER = 2,
    MACRO_BLOCK_RESTORE_READER_V1 = 3,  // not used any more
    MACRO_BLOCK_RESTORE_READER_V2 = 4,
    MAX_READER_TYPE
  };
  // macro block list is set in the init func
  ObIPartitionMacroBlockReader()
  {}
  virtual ~ObIPartitionMacroBlockReader()
  {}
  virtual int get_next_macro_block(blocksstable::ObFullMacroBlockMeta& meta, blocksstable::ObBufferReader& data,
      blocksstable::MacroBlockId& src_macro_id) = 0;
  virtual Type get_type() const = 0;
  virtual int64_t get_data_size() const = 0;
  int deserialize_macro_meta(char* buf, int64_t data_len, int64_t& pos, common::ObIAllocator& allocator,
      blocksstable::ObFullMacroBlockMeta& full_meta);

private:
  DISALLOW_COPY_AND_ASSIGN(ObIPartitionMacroBlockReader);
};

// Represents the meta information needed to build the target sstable meta
struct ObLogicTableMeta {
  ObLogicTableMeta() : schema_version_(-1)
  {}
  ~ObLogicTableMeta()
  {}
  void reset()
  {
    schema_version_ = -1;
  }
  bool is_valid() const
  {
    return schema_version_ >= 0;
  }
  TO_STRING_KV(K_(schema_version));

  int64_t schema_version_;
  OB_UNIS_VERSION(1);
};

class ObILogicBaseMetaReader {
public:
  ObILogicBaseMetaReader()
  {}
  virtual ~ObILogicBaseMetaReader()
  {}

  virtual int fetch_end_key_list(common::ObIArray<common::ObStoreRowkey>& end_key_list) = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObILogicBaseMetaReader);
};

class ObIPhysicalBaseMetaReader {
public:
  enum Type {
    BASE_DATA_META_OB_READER,
    BASE_DATA_META_RESTORE_READER,
    BASE_DATA_META_OB_COMPAT_READER,
    BASE_DATA_META_BACKUP_READER,
    BASE_DATA_META_RESTORE_READER_V1,
    BASE_DATA_META_RESTORE_READER_V2,
  };
  ObIPhysicalBaseMetaReader()
  {}
  virtual ~ObIPhysicalBaseMetaReader()
  {}

  virtual int fetch_sstable_meta(blocksstable::ObSSTableBaseMeta& sstable_meta) = 0;
  virtual int fetch_macro_block_list(common::ObIArray<blocksstable::ObSSTablePair>& macro_block_list) = 0;
  virtual Type get_type() const = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIPhysicalBaseMetaReader);
};

class ObIPGPartitionBaseDataMetaObReader {
public:
  enum Type {
    BASE_DATA_META_OB_READER = 0,
    BASE_DATA_META_OB_COMPAT_READER = 1,
    BASE_DATA_META_OB_RESTORE_READER = 2,
    BASE_DATA_META_OB_BACKUP_READER = 3,
    BASE_DATA_META_OB_RESTORE_READER_V1 = 4,
    BASE_DATA_META_OB_RESTORE_READER_V2 = 5,
  };
  ObIPGPartitionBaseDataMetaObReader()
  {}
  virtual ~ObIPGPartitionBaseDataMetaObReader()
  {}
  virtual int fetch_pg_partition_meta_info(obrpc::ObPGPartitionMetaInfo& partition_meta_info) = 0;
  virtual Type get_type() const = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIPGPartitionBaseDataMetaObReader);
};

struct ObSameVersionIncTable {
  ObArray<ObITable::TableKey> src_table_keys_;
  ObPartitionKey pkey_;
  ObVersion version_;
  int64_t table_id_;
  ObSameVersionIncTable();
  bool is_valid() const;
  void reset();
  int assign(const ObSameVersionIncTable& same_version_inc_table);
  int64_t get_max_snapshot_version();
  int64_t get_min_base_version();

  TO_STRING_KV(K_(src_table_keys), K_(pkey), K_(version), K_(table_id));
  DISALLOW_COPY_AND_ASSIGN(ObSameVersionIncTable);
};

struct ObMigrateTableInfo {
  struct SSTableInfo {
    ObITable::TableKey src_table_key_;
    int64_t dest_base_version_;               // for compatible 2.1 logic migrate
    common::ObLogTsRange dest_log_ts_range_;  // TODO  generate new log ts range
    SSTableInfo();
    bool is_valid() const;
    void reset();
    TO_STRING_KV(K_(src_table_key), K_(dest_base_version), K_(dest_log_ts_range));
  };

  uint64_t table_id_;
  // major sstables
  ObArray<SSTableInfo> major_sstables_;
  // include same version minor sstable,frozen memtable,active memtable
  ObArray<SSTableInfo> minor_sstables_;
  int64_t multi_version_start_;
  bool ready_for_read_;

  ObMigrateTableInfo();
  ~ObMigrateTableInfo();
  int assign(const ObMigrateTableInfo& info);
  void reuse();
  TO_STRING_KV(K_(table_id), K_(multi_version_start), K_(ready_for_read), K_(major_sstables), K_(minor_sstables));

private:
  DISALLOW_COPY_AND_ASSIGN(ObMigrateTableInfo);
};

struct ObMigratePartitionInfo final {
  ObPGPartitionStoreMeta meta_;
  ObArray<ObMigrateTableInfo> table_infos_;
  ObArray<uint64_t> table_id_list_;
  common::ObAddr src_;
  bool is_restore_;

  ObMigratePartitionInfo();
  ~ObMigratePartitionInfo();
  int assign(const ObMigratePartitionInfo& info);
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(src), K_(meta), K_(table_id_list), K_(table_infos), K_(is_restore));

private:
  DISALLOW_COPY_AND_ASSIGN(ObMigratePartitionInfo);
};

class ObIPartitionGroupMetaRestoreReader {
public:
  enum Type {
    PG_META_RESTORE_READER,
    PG_META_RESTORE_READER_V1,
    PG_META_RESTORE_READER_V2,
  };
  ObIPartitionGroupMetaRestoreReader()
  {}
  virtual ~ObIPartitionGroupMetaRestoreReader()
  {}
  virtual Type get_type() const = 0;
  virtual int fetch_partition_group_meta(ObPartitionGroupMeta& pg_meta) = 0;
  virtual int64_t get_data_size() const = 0;
  virtual int fetch_sstable_meta(
      const ObITable::TableKey& table_key, blocksstable::ObSSTableBaseMeta& sstable_meta) = 0;
  virtual int fetch_sstable_pair_list(
      const ObITable::TableKey& table_key, common::ObIArray<blocksstable::ObSSTablePair>& pair_list) = 0;
};

class ObIPhyRestoreMacroIndexStore {
public:
  enum Type {
    PHY_RESTORE_MACRO_INDEX_STORE_V1 = 1,
    PHY_RESTORE_MACRO_INDEX_STORE_V2 = 2,
  };
  ObIPhyRestoreMacroIndexStore()
  {}
  virtual ~ObIPhyRestoreMacroIndexStore()
  {}
  virtual Type get_type() const = 0;
  virtual bool is_inited() const = 0;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_I_PARTITION_BASE_DATA_READER_H_ */
