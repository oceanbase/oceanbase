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

#ifndef SRC_STORAGE_BACKUP_OB_PARTITION_BASE_DATA_PHYSICAL_RESTORE_V2_H_
#define SRC_STORAGE_BACKUP_OB_PARTITION_BASE_DATA_PHYSICAL_RESTORE_V2_H_

#include "share/ob_define.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/ob_i_partition_base_data_reader.h"
#include "storage/ob_partition_base_data_oss_reader.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/backup/ob_partition_backup_struct.h"
#include "share/backup/ob_backup_path.h"
#include "storage/ob_partition_base_data_physical_restore.h"

namespace oceanbase {
using namespace share;
namespace storage {
class ObPhyRestoreMacroIndexStoreV2 : public ObIPhyRestoreMacroIndexStore {
public:
  ObPhyRestoreMacroIndexStoreV2();
  virtual ~ObPhyRestoreMacroIndexStoreV2();
  void reset();
  int init(const int64_t backup_task_id, const share::ObPhysicalRestoreArg& arg,
      const ObReplicaRestoreStatus& restore_status);
  int init(const int64_t backup_task_id, const common::ObPartitionKey& pkey, const share::ObPhysicalBackupArg& arg,
      const ObBackupDataType& backup_data_type);
  int get_macro_index_array(
      const ObITable::TableKey& table_key, const common::ObArray<ObBackupTableMacroIndex>*& index_list) const;
  int get_macro_index(
      const ObITable::TableKey& table_key, const int64_t sstable_idx, ObBackupTableMacroIndex& macro_index) const;
  int get_sstable_pair_list(
      const ObITable::TableKey& table_key, common::ObIArray<blocksstable::ObSSTablePair>& pair_list) const;
  int get_major_macro_index_array(
      const uint64_t index_table_id, const common::ObArray<ObBackupTableMacroIndex>*& index_list) const;
  int64_t get_backup_task_id() const
  {
    return backup_task_id_;
  }
  virtual Type get_type() const
  {
    return PHY_RESTORE_MACRO_INDEX_STORE_V2;
  }
  virtual bool is_inited() const;

  TO_STRING_KV(K_(is_inited));

private:
  int init_major_macro_index(const common::ObPartitionKey& backup_pg_key, const ObBackupBaseDataPathInfo& path_info,
      const ObReplicaRestoreStatus& restore_status);
  int init_minor_macro_index(const int64_t backup_task_id, const common::ObPartitionKey& backup_pg_key,
      const ObBackupBaseDataPathInfo& path_info, const ObReplicaRestoreStatus& restore_status);
  int add_sstable_index(
      const ObITable::TableKey& table_key, const common::ObIArray<ObBackupTableMacroIndex>& index_list);
  int init_one_file(const ObString& path, const ObString& storage_info);
  int get_table_key_ptr(const ObITable::TableKey& table_key, const ObITable::TableKey*& table_key_ptr);

private:
  static const int64_t BUCKET_SIZE = 100000;  // 10w
  typedef common::hash::ObHashMap<ObITable::TableKey, common::ObArray<ObBackupTableMacroIndex>*> MacroIndexMap;
  bool is_inited_;
  common::ObArenaAllocator allocator_;
  MacroIndexMap index_map_;
  int64_t backup_task_id_;
  ObArray<ObITable::TableKey*> table_keys_ptr_;
  DISALLOW_COPY_AND_ASSIGN(ObPhyRestoreMacroIndexStoreV2);
};

class ObPartitionBaseDataMetaRestoreReaderV2 {
public:
  ObPartitionBaseDataMetaRestoreReaderV2();
  virtual ~ObPartitionBaseDataMetaRestoreReaderV2();
  void reset();
  int init(const common::ObPartitionKey& pkey, const ObPhysicalRestoreArg& restore_info,
      const ObPhyRestoreMacroIndexStoreV2& macro_indexs, const ObBackupPartitionStoreMetaInfo& backup_pg_meta_info,
      const int64_t schema_version);
  int fetch_partition_meta(ObPGPartitionStoreMeta& partition_store_meta);
  int fetch_sstable_meta(const ObITable::TableKey& table_key, blocksstable::ObSSTableBaseMeta& sstable_meta);
  int fetch_sstable_pair_list(
      const ObITable::TableKey& table_key, common::ObIArray<blocksstable::ObSSTablePair>& pair_list);
  int fetch_all_table_ids(common::ObIArray<uint64_t>& table_id_array);
  int fetch_table_keys(const uint64_t index_id, obrpc::ObFetchTableInfoResult& table_res);
  TO_STRING_KV(
      K_(pkey), K_(restore_info), K_(last_read_size), K_(partition_store_meta), K_(data_version), K_(schema_version));

private:
  int prepare(const common::ObPartitionKey& pkey);
  int trans_table_key(const ObITable::TableKey& table_Key, ObITable::TableKey& backup_table_key);
  int get_backup_sstable_meta_info(
      const ObITable::TableKey& backup_table_key, const ObBackupSSTableMetaInfo*& backup_sstable_meta_info);
  int get_backup_table_keys(const uint64_t backup_index_id, common::ObIArray<ObITable::TableKey>& table_keys);
  int do_filter_tables(common::ObIArray<uint64_t>& table_ids);

private:
  bool is_inited_;
  common::ObPartitionKey pkey_;
  const ObPhysicalRestoreArg* restore_info_;
  const ObPhyRestoreMacroIndexStoreV2* macro_indexs_;
  const ObBackupPartitionStoreMetaInfo* partition_store_meta_info_;
  int64_t last_read_size_;
  ObPGPartitionStoreMeta partition_store_meta_;
  int64_t data_version_;
  int64_t schema_version_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionBaseDataMetaRestoreReaderV2);
};

class ObPartitionGroupMetaRestoreReaderV2;
class ObPhysicalBaseMetaRestoreReaderV2 : public ObIPhysicalBaseMetaReader {
public:
  ObPhysicalBaseMetaRestoreReaderV2();
  virtual ~ObPhysicalBaseMetaRestoreReaderV2()
  {}
  int init(common::ObInOutBandwidthThrottle& bandwidth_throttle, const ObPhysicalRestoreArg& restore_info,
      const ObITable::TableKey& table_key, ObIPartitionGroupMetaRestoreReader& reader);
  virtual int fetch_sstable_meta(blocksstable::ObSSTableBaseMeta& sstable_meta);
  virtual int fetch_macro_block_list(common::ObIArray<blocksstable::ObSSTablePair>& macro_block_list);
  virtual Type get_type() const
  {
    return BASE_DATA_META_RESTORE_READER_V1;
  }

private:
  bool is_inited_;
  const ObPhysicalRestoreArg* restore_info_;
  ObIPartitionGroupMetaRestoreReader* reader_;
  common::ObArenaAllocator allocator_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  ObITable::TableKey table_key_;
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalBaseMetaRestoreReaderV2);
};

class ObPartitionMacroBlockRestoreReaderV2 : public ObIPartitionMacroBlockReader {
public:
  ObPartitionMacroBlockRestoreReaderV2();
  virtual ~ObPartitionMacroBlockRestoreReaderV2();
  void reset();
  int init(common::ObInOutBandwidthThrottle& bandwidth_throttle, common::ObIArray<ObMigrateArgMacroBlockInfo>& list,
      const ObPhysicalRestoreArg& restore_info, const ObPhyRestoreMacroIndexStoreV2& macro_indexs,
      const ObITable::TableKey& table_key);
  virtual int get_next_macro_block(blocksstable::ObFullMacroBlockMeta& meta, blocksstable::ObBufferReader& data,
      blocksstable::MacroBlockId& src_macro_id);
  virtual Type get_type() const
  {
    return MACRO_BLOCK_RESTORE_READER_V2;
  }
  virtual int64_t get_data_size() const
  {
    return read_size_;
  }

private:
  int trans_macro_block(
      const uint64_t table_id, blocksstable::ObMacroBlockMetaV2& meta, blocksstable::ObBufferReader& data);
  int get_macro_block_path(const ObBackupTableMacroIndex& macro_index, share::ObBackupPath& path);

private:
  bool is_inited_;
  common::ObArray<obrpc::ObFetchMacroBlockArg> macro_list_;
  int64_t macro_idx_;
  int64_t read_size_;
  uint64_t table_id_;
  ObBackupBaseDataPathInfo backup_path_info_;
  const ObPhyRestoreMacroIndexStoreV2* macro_indexs_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  ObPartitionKey backup_pgkey_;
  ObITable::TableKey backup_table_key_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionMacroBlockRestoreReaderV2);
};

class ObPartitionGroupMetaRestoreReaderV2 : public ObIPartitionGroupMetaRestoreReader {
public:
  ObPartitionGroupMetaRestoreReaderV2();
  virtual ~ObPartitionGroupMetaRestoreReaderV2();
  void reset();
  int init(common::ObInOutBandwidthThrottle& bandwidth_throttle, const ObPhysicalRestoreArg& restore_info,
      const ObPhyRestoreMetaIndexStore& meta_indexs, const ObPhyRestoreMacroIndexStoreV2& macro_indexs);
  virtual int fetch_partition_group_meta(ObPartitionGroupMeta& pg_meta);
  virtual int64_t get_data_size() const
  {
    return reader_.get_data_size();
  }
  virtual int fetch_sstable_meta(const ObITable::TableKey& table_key, blocksstable::ObSSTableBaseMeta& sstable_meta);
  virtual int fetch_sstable_pair_list(
      const ObITable::TableKey& table_key, common::ObIArray<blocksstable::ObSSTablePair>& pair_list);
  virtual Type get_type() const
  {
    return PG_META_RESTORE_READER_V2;
  };
  int get_partition_readers(const ObPartitionArray& partitions,
      common::ObIArray<ObPartitionBaseDataMetaRestoreReaderV2*>& partition_reader_array);
  int get_restore_tenant_id(uint64_t& tenant_id);
  int get_restore_schema_version(int64_t& schema_version);

private:
  int prepare(const ObPhysicalRestoreArg& restore_info, const ObPhyRestoreMacroIndexStoreV2& macro_indexs);
  int prepare_pg_meta(const ObPhysicalRestoreArg& restore_info);
  int create_pg_partition_if_need(const ObPhysicalRestoreArg& restore_info, const ObPartitionGroupMeta& backup_pg_meta);
  int read_partition_meta(const ObPartitionKey& pkey, const ObPhysicalRestoreArg& restore_info,
      ObPGPartitionStoreMeta& partition_store_meta);
  int trans_backup_pgmeta(const ObPhysicalRestoreArg& restore_info, ObPartitionGroupMeta& backup_pg_meta);
  int check_backup_partitions_in_pg(const ObPhysicalRestoreArg& restore_info, ObPartitionGroupMeta& backup_pg_meta);
  int get_backup_partition_meta_info(const ObPartitionKey& pkey, const ObPhysicalRestoreArg& restore_info,
      const ObBackupPartitionStoreMetaInfo*& backup_partition_meta_info);
  int trans_from_backup_partitions(const ObPhysicalRestoreArg& restore_info,
      const common::ObPartitionArray& backup_partitions, common::ObPartitionArray& current_partitions);
  int trans_to_backup_partitions(const ObPhysicalRestoreArg& restore_info,
      const common::ObPartitionArray& current_partitions, common::ObPartitionArray& backup_partitions);
  int do_filter_pg_partitions(const ObPGKey& pg_key, ObPartitionArray& partitions);

private:
  typedef hash::ObHashMap<ObPartitionKey, ObPartitionBaseDataMetaRestoreReaderV2*> MetaReaderMap;
  bool is_inited_;
  const ObPhysicalRestoreArg* restore_info_;
  const ObPhyRestoreMetaIndexStore* meta_indexs_;
  ObPGMetaPhysicalReader reader_;
  ObPartitionGroupMeta pg_meta_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  int64_t last_read_size_;
  MetaReaderMap partition_reader_map_;
  common::ObArenaAllocator allocator_;
  ObBackupPGMetaInfo backup_pg_meta_info_;
  int64_t schema_version_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionGroupMetaRestoreReaderV2);
};

// 3.1 and later version use this class
class ObPGPartitionBaseDataMetaRestoreReaderV2 : public ObIPGPartitionBaseDataMetaObReader {
public:
  ObPGPartitionBaseDataMetaRestoreReaderV2();
  virtual ~ObPGPartitionBaseDataMetaRestoreReaderV2();
  void reset();
  int init(const ObPartitionArray& partitions, ObPartitionGroupMetaRestoreReaderV2* reader);
  int fetch_pg_partition_meta_info(obrpc::ObPGPartitionMetaInfo& partition_meta_info);
  virtual Type get_type() const
  {
    return BASE_DATA_META_OB_RESTORE_READER_V2;
  }

private:
  int check_sstable_table_ids_in_table(const ObPartitionKey& pkey, const common::ObIArray<uint64_t>& table_ids);
  int check_sstable_ids_contain_schema_table_id(const hash::ObHashSet<uint64_t>& table_id_set,
      const uint64_t schema_table_id, schema::ObSchemaGetterGuard& schema_guard);

private:
  bool is_inited_;
  int64_t reader_index_;
  common::ObArray<ObPartitionBaseDataMetaRestoreReaderV2*> partition_reader_array_;
  int64_t schema_version_;
  DISALLOW_COPY_AND_ASSIGN(ObPGPartitionBaseDataMetaRestoreReaderV2);
};

}  // namespace storage
}  // namespace oceanbase

#endif
