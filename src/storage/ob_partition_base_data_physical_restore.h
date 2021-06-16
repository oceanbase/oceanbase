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

#ifndef SRC_STORAGE_OB_PARTITION_BASE_DATA_PHYSICAL_RESTORE_H_
#define SRC_STORAGE_OB_PARTITION_BASE_DATA_PHYSICAL_RESTORE_H_

#include "share/ob_define.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/ob_i_partition_base_data_reader.h"
#include "storage/ob_partition_base_data_oss_reader.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/backup/ob_partition_backup_struct.h"
#include "share/backup/ob_backup_path.h"

namespace oceanbase {
using namespace share;
namespace storage {

class ObRestoreFileUtil {
public:
  static const int64_t MAX_IDLE_TIME = 10 * 1000LL * 1000LL;  // 10s
public:
  static int read_one_file(
      const ObString& path, const ObString& storage_info, ObIAllocator& allocator, char*& buf, int64_t& read_size);
  static int pread_file(
      const ObString& path, const ObString& storage_info, const int64_t offset, const int64_t read_size, char* buf);

  static int read_partition_group_meta(const ObString& path, const ObString& storage_info,
      const ObBackupMetaIndex& meta_index, ObPartitionGroupMeta& pg_meta);

  static int read_partition_meta(const ObString& path, const ObString& storage_info,
      const ObBackupMetaIndex& meta_index, const int64_t& compatible, storage::ObPGPartitionStoreMeta& partition_meta);

  static int read_sstable_metas(const ObString& path, const ObString& storage_info, const ObBackupMetaIndex& meta_index,
      common::ObArenaAllocator& meta_allocator, common::ObIArray<blocksstable::ObSSTableBaseMeta>& sstable_info_array);

  static int read_table_keys(const ObString& path, const ObString& storage_info, const ObBackupMetaIndex& meta_index,
      common::ObIArray<storage::ObITable::TableKey>& table_keys);

  static int read_macroblock_data(const ObString& path, const ObString& storage_info,
      const ObBackupMacroIndex& meta_index, common::ObArenaAllocator& allocator,
      blocksstable::ObMacroBlockSchemaInfo*& new_schema, blocksstable::ObMacroBlockMetaV2*& new_meta,
      blocksstable::ObBufferReader& macro_data);

  static int read_macroblock_data(const ObString& path, const ObString& storage_info,
      const ObBackupTableMacroIndex& meta_index, common::ObArenaAllocator& allocator,
      blocksstable::ObMacroBlockSchemaInfo*& new_schema, blocksstable::ObMacroBlockMetaV2*& new_meta,
      blocksstable::ObBufferReader& macro_data);

  static int fetch_max_backup_file_id(const ObString& path, const ObString& storage_info, const int64_t& backup_set_id,
      int64_t& max_index_id, int64_t& max_data_id);
  static int get_file_id(const ObString& file_name, int64_t& file_id);
  static int limit_bandwidth_and_sleep(
      ObInOutBandwidthThrottle& throttle, const int64_t cur_data_size, int64_t& last_read_size);

  static int parse_backup_data(
      blocksstable::ObBufferReader& buff_reader, share::ObBackupCommonHeader& header, char*& buf, int64_t& buf_size);
  static int get_backup_data(
      blocksstable::ObBufferReader& buff_reader, share::ObBackupCommonHeader& header, char*& buf, int64_t& buf_size);

  static int read_backup_pg_meta_info(const ObString& path, const ObString& storage_info,
      const ObBackupMetaIndex& meta_index, ObBackupPGMetaInfo& backup_pg_meta_info);
};

class ObPhyRestoreMetaIndexStore final {
public:
  ObPhyRestoreMetaIndexStore();
  virtual ~ObPhyRestoreMetaIndexStore();
  void reset();
  int init(const ObBackupBaseDataPathInfo& path_info, const int64_t compatible, const bool need_check_compeleted);
  bool is_inited() const;
  int get_meta_index(const ObPartitionKey& part_key, const ObBackupMetaType& type, ObBackupMetaIndex& meta_index) const;
  int check_meta_index_completed(const int64_t compatible, const ObBackupBaseDataPathInfo& path_info);

public:
  typedef common::hash::ObHashMap<share::ObMetaIndexKey, share::ObBackupMetaIndex> MetaIndexMap;
  const MetaIndexMap& get_meta_index_map()
  {
    return index_map_;
  }
  TO_STRING_KV(K_(is_inited));

private:
  static const int64_t BUCKET_SIZE = 1024;
  int init_one_file(const ObString& path, const ObString& storage_info, int64_t& file_length, int64_t& total_length);
  bool is_inited_;
  MetaIndexMap index_map_;
  DISALLOW_COPY_AND_ASSIGN(ObPhyRestoreMetaIndexStore);
};

// TODO():fetch MacroIndex on demand
class ObPhyRestoreMacroIndexStore : public ObIPhyRestoreMacroIndexStore {
public:
  ObPhyRestoreMacroIndexStore();
  virtual ~ObPhyRestoreMacroIndexStore();
  void reset();
  int init(const share::ObPhysicalRestoreArg& arg, const ObReplicaRestoreStatus& restore_status);
  int init(const common::ObPartitionKey& pkey, const share::ObPhysicalBackupArg& arg);

  int get_macro_index_array(const uint64_t index_id, const common::ObArray<ObBackupMacroIndex>*& index_list) const;
  int get_macro_index(const uint64_t index_id, const int64_t sstable_idx, ObBackupMacroIndex& macro_index) const;
  int get_sstable_pair_list(const uint64_t index_id, common::ObIArray<blocksstable::ObSSTablePair>& pair_list) const;
  virtual Type get_type() const
  {
    return PHY_RESTORE_MACRO_INDEX_STORE_V1;
  }
  virtual bool is_inited() const;
  TO_STRING_KV(K_(is_inited));

private:
  static const int64_t BUCKET_SIZE = 100000;  // 10w
  typedef common::hash::ObHashMap<uint64_t, common::ObArray<ObBackupMacroIndex>*> MacroIndexMap;
  int init_one_file(const ObString& path, const ObString& storage_info, int64_t& file_length, int64_t& total_length);
  int add_sstable_index(const uint64_t index_id, const common::ObIArray<ObBackupMacroIndex>& index_list);
  bool is_inited_;
  common::ObArenaAllocator allocator_;
  MacroIndexMap index_map_;
  DISALLOW_COPY_AND_ASSIGN(ObPhyRestoreMacroIndexStore);
};

class ObPartitionMetaPhysicalReader final {
public:
  ObPartitionMetaPhysicalReader();
  virtual ~ObPartitionMetaPhysicalReader();
  void reset();
  int init(const ObPhysicalRestoreArg& arg, const ObPhyRestoreMetaIndexStore& meta_indexs,
      const ObPhyRestoreMacroIndexStore& macro_indexs, const ObPartitionKey& pkey);
  int read_partition_meta(ObPGPartitionStoreMeta& partition_store_meta);
  int read_sstable_pair_list(const uint64_t index_tid, common::ObIArray<blocksstable::ObSSTablePair>& pair_list);
  int read_sstable_meta(const uint64_t backup_index, blocksstable::ObSSTableBaseMeta& sstable_meta);
  int64_t get_sstable_count() const
  {
    return sstable_meta_array_.count();
  }
  int64_t get_data_size() const
  {
    return data_size_;
  }
  int read_table_ids(common::ObIArray<uint64_t>& table_id_array);
  int read_table_keys_by_table_id(const uint64_t table_id, ObIArray<ObITable::TableKey>& table_keys_array);
  bool is_inited() const
  {
    return is_inited_;
  }
  TO_STRING_KV(K_(is_inited), K_(sstable_index), K_(data_size), K_(pkey));

private:
  int read_all_sstable_meta();
  int read_table_keys();

private:
  bool is_inited_;
  int64_t sstable_index_;
  int64_t data_size_;
  common::ObArray<blocksstable::ObSSTableBaseMeta> sstable_meta_array_;
  common::ObArray<ObITable::TableKey> table_keys_array_;
  common::ObArenaAllocator allocator_;
  const ObPhysicalRestoreArg* arg_;
  const ObPhyRestoreMetaIndexStore* meta_indexs_;
  const ObPhyRestoreMacroIndexStore* macro_indexs_;
  int64_t table_count_;
  ObPartitionKey pkey_;

  DISALLOW_COPY_AND_ASSIGN(ObPartitionMetaPhysicalReader);
};

class ObPGMetaPhysicalReader final {
public:
  ObPGMetaPhysicalReader();
  virtual ~ObPGMetaPhysicalReader();
  void reset();
  int init(const share::ObPhysicalRestoreArg& arg, const ObPhyRestoreMetaIndexStore& meta_indexs);
  int read_partition_group_meta(ObPartitionGroupMeta& pg_meta);
  // 3.1 and later version use this interface
  int read_backup_pg_meta_info(ObBackupPGMetaInfo& backup_pg_meta_info);

  int64_t get_data_size() const
  {
    return data_size_;
  }
  bool is_inited() const
  {
    return is_inited_;
  }

private:
  bool is_inited_;
  int64_t data_size_;
  const share::ObPhysicalRestoreArg* arg_;
  const ObPhyRestoreMetaIndexStore* meta_indexs_;
  DISALLOW_COPY_AND_ASSIGN(ObPGMetaPhysicalReader);
};

class ObPartitionBaseDataMetaRestoreReaderV1 {
public:
  ObPartitionBaseDataMetaRestoreReaderV1();
  virtual ~ObPartitionBaseDataMetaRestoreReaderV1();
  void reset();
  int init(common::ObInOutBandwidthThrottle& bandwidth_throttle, const common::ObPartitionKey& pkey,
      const ObPhysicalRestoreArg& restore_info, const ObPhyRestoreMetaIndexStore& meta_indexs,
      const ObPhyRestoreMacroIndexStore& macro_indexs);
  int fetch_partition_meta(ObPGPartitionStoreMeta& partition_store_meta);
  int fetch_sstable_meta(const uint64_t index_id, blocksstable::ObSSTableBaseMeta& sstable_meta);
  int64_t get_data_size() const
  {
    return reader_.get_data_size();
  }
  int fetch_sstable_pair_list(const uint64_t index_id, common::ObIArray<blocksstable::ObSSTablePair>& pair_list);
  int fetch_all_table_ids(common::ObIArray<uint64_t>& table_id_array);
  int fetch_table_keys(const uint64_t index_id, obrpc::ObFetchTableInfoResult& table_res);
  TO_STRING_KV(K_(pkey), K_(restore_info), K_(last_read_size), K_(partition_store_meta), K_(data_version));

private:
  int prepare(const common::ObPartitionKey& pkey);

private:
  bool is_inited_;
  common::ObPartitionKey pkey_;
  const ObPhysicalRestoreArg* restore_info_;
  ObPartitionMetaPhysicalReader reader_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  int64_t last_read_size_;
  ObPGPartitionStoreMeta partition_store_meta_;
  int64_t data_version_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionBaseDataMetaRestoreReaderV1);
};

class ObPartitionGroupMetaRestoreReaderV1;
class ObPhysicalBaseMetaRestoreReaderV1 : public ObIPhysicalBaseMetaReader {
public:
  ObPhysicalBaseMetaRestoreReaderV1();
  virtual ~ObPhysicalBaseMetaRestoreReaderV1()
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
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalBaseMetaRestoreReaderV1);
};

// used for backup since 2.2.6
class ObPartitionMacroBlockRestoreReaderV1 : public ObIPartitionMacroBlockReader {
public:
  ObPartitionMacroBlockRestoreReaderV1();
  virtual ~ObPartitionMacroBlockRestoreReaderV1();
  void reset();
  int init(common::ObInOutBandwidthThrottle& bandwidth_throttle, common::ObIArray<ObMigrateArgMacroBlockInfo>& list,
      const ObPhysicalRestoreArg& restore_info, const ObPhyRestoreMacroIndexStore& macro_indexs,
      const ObITable::TableKey& table_key);
  virtual int get_next_macro_block(blocksstable::ObFullMacroBlockMeta& meta, blocksstable::ObBufferReader& data,
      blocksstable::MacroBlockId& src_macro_id);
  virtual Type get_type() const
  {
    return MACRO_BLOCK_RESTORE_READER_V1;
  }
  virtual int64_t get_data_size() const
  {
    return read_size_;
  }

private:
  int trans_macro_block(
      const uint64_t table_id, blocksstable::ObMacroBlockMetaV2& meta, blocksstable::ObBufferReader& data);

private:
  bool is_inited_;
  common::ObArray<obrpc::ObFetchMacroBlockArg> macro_list_;
  int64_t macro_idx_;
  int64_t read_size_;
  uint64_t table_id_;
  ObBackupBaseDataPathInfo backup_path_info_;
  const ObPhyRestoreMacroIndexStore* macro_indexs_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  uint64_t backup_index_id_;
  ObPartitionKey backup_pgkey_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionMacroBlockRestoreReaderV1);
};

class ObPartitionGroupMetaRestoreReaderV1 : public ObIPartitionGroupMetaRestoreReader {
public:
  ObPartitionGroupMetaRestoreReaderV1();
  virtual ~ObPartitionGroupMetaRestoreReaderV1();
  void reset();
  int init(common::ObInOutBandwidthThrottle& bandwidth_throttle, const ObPhysicalRestoreArg& restore_info,
      const ObPhyRestoreMetaIndexStore& meta_indexs, const ObPhyRestoreMacroIndexStore& macro_indexs);
  virtual int fetch_partition_group_meta(ObPartitionGroupMeta& pg_meta);
  virtual int64_t get_data_size() const
  {
    return reader_.get_data_size();
  }
  virtual int fetch_sstable_meta(const ObITable::TableKey& table_key, blocksstable::ObSSTableBaseMeta& sstable_meta);
  virtual int fetch_sstable_pair_list(
      const ObITable::TableKey& table_key, common::ObIArray<blocksstable::ObSSTablePair>& pair_list);
  int get_partition_readers(const ObPartitionArray& partitions,
      common::ObIArray<ObPartitionBaseDataMetaRestoreReaderV1*>& partition_reader_array);
  virtual Type get_type() const
  {
    return PG_META_RESTORE_READER_V1;
  }
  int get_restore_tenant_id(uint64_t& tenant_id);

private:
  int prepare(common::ObInOutBandwidthThrottle& bandwidth_throttle, const ObPhysicalRestoreArg& restore_info,
      const ObPhyRestoreMetaIndexStore& meta_indexs, const ObPhyRestoreMacroIndexStore& macro_indexs);
  int prepare_pg_meta(common::ObInOutBandwidthThrottle& bandwidth_throttle, const ObPhysicalRestoreArg& restore_info);
  int create_pg_partition_if_need(const ObPhysicalRestoreArg& restore_info, const ObPartitionGroupMeta& backup_pg_meta);
  int read_partition_meta(const ObPartitionKey& pkey, const ObPhysicalRestoreArg& restore_info,
      ObPGPartitionStoreMeta& partition_store_meta);
  int trans_backup_pgmeta(ObPartitionGroupMeta& backup_pg_meta);
  int check_backup_partitions_in_pg(const ObPhysicalRestoreArg& restore_info, ObPartitionGroupMeta& backup_pg_meta);

private:
  typedef hash::ObHashMap<ObPartitionKey, ObPartitionBaseDataMetaRestoreReaderV1*> MetaReaderMap;
  bool is_inited_;
  const ObPhysicalRestoreArg* restore_info_;
  const ObPhyRestoreMetaIndexStore* meta_indexs_;
  ObPGMetaPhysicalReader reader_;
  ObPartitionGroupMeta pg_meta_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  int64_t last_read_size_;
  MetaReaderMap partition_reader_map_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionGroupMetaRestoreReaderV1);
};

// used for 2.2
class ObPGPartitionBaseDataMetaRestoreReaderV1 : public ObIPGPartitionBaseDataMetaObReader {
public:
  ObPGPartitionBaseDataMetaRestoreReaderV1();
  virtual ~ObPGPartitionBaseDataMetaRestoreReaderV1();
  void reset();
  int init(const ObPartitionArray& partitions, ObPartitionGroupMetaRestoreReaderV1* reader);
  int fetch_pg_partition_meta_info(obrpc::ObPGPartitionMetaInfo& partition_meta_info);
  virtual Type get_type() const
  {
    return BASE_DATA_META_OB_RESTORE_READER_V1;
  }

private:
  int check_sstable_table_ids_in_table(const ObPartitionKey& pkey, const common::ObIArray<uint64_t>& table_ids);
  int check_sstable_ids_contain_schema_table_id(const hash::ObHashSet<uint64_t>& table_id_set,
      const uint64_t schema_table_id, schema::ObSchemaGetterGuard& schema_guard);

private:
  bool is_inited_;
  int64_t reader_index_;
  common::ObArray<ObPartitionBaseDataMetaRestoreReaderV1*> partition_reader_array_;
  int64_t schema_version_;
  DISALLOW_COPY_AND_ASSIGN(ObPGPartitionBaseDataMetaRestoreReaderV1);
};

}  // namespace storage
}  // namespace oceanbase

#endif
