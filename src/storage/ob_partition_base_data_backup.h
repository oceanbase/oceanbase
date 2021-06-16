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

#ifndef STORAGE_OB_PARTITION_BASE_DATA_BACKUP_H_
#define STORAGE_OB_PARTITION_BASE_DATA_BACKUP_H_

#include "share/ob_define.h"
#include "share/restore/ob_restore_args.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/ob_i_partition_base_data_reader.h"
#include "lib/restore/ob_storage_path.h"
#include "lib/thread/ob_dynamic_thread_pool.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/queue/ob_fixed_queue.h"
#include "storage/ob_partition_service_rpc.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_path.h"
#include "lib/restore/ob_storage.h"
#include "storage/ob_partition_base_data_physical_restore.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "storage/backup/ob_partition_backup_struct.h"
#include "storage/backup/ob_partition_base_data_physical_restore_v2.h"

namespace oceanbase {
// TODO(): remove using
using namespace common;
using namespace common::hash;
using namespace share;
using namespace blocksstable;
using namespace obrpc;
using namespace schema;

namespace storage {
class ObRestoreInfo;
class ObPGStorage;
class ObPartMigrationTask;
class ObMigrateCtx;

struct ObBackupSSTableInfo {
  ObBackupSSTableInfo() : sstable_meta_(), part_list_()
  {}
  ~ObBackupSSTableInfo()
  {}
  TO_STRING_KV(K_(sstable_meta), K_(part_list));

  void reset();
  bool is_valid() const;
  int assign(const ObBackupSSTableInfo& result);
  blocksstable::ObSSTableBaseMeta sstable_meta_;
  ObSArray<ObSSTablePair> part_list_;
};

// macro block backup arg
struct ObBackupMacroBlockArg final {
  ObBackupMacroBlockArg();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(fetch_arg), KP_(table_key_ptr), K_(need_copy));

  obrpc::ObFetchMacroBlockArg fetch_arg_;
  const ObITable::TableKey* table_key_ptr_;
  bool need_copy_;
};

class ObPartitionMetaBackupReader {

public:
  ObPartitionMetaBackupReader();
  virtual ~ObPartitionMetaBackupReader()
  {}

  int init(const share::ObPhysicalBackupArg& arg, const ObPartitionKey& pkey);
  int read_partition_meta(ObPartitionStoreMeta& partition_store_meta);
  int read_sstable_pair_list(const uint64_t backup_index, common::ObIArray<blocksstable::ObSSTablePair>& pair_list);
  int read_sstable_meta(const uint64_t backup_index, blocksstable::ObSSTableBaseMeta& sstable_meta);
  int64_t get_data_size() const
  {
    return data_size_;
  }
  int read_table_ids(common::ObIArray<uint64_t>& table_id_array);
  int read_table_keys_by_table_id(const uint64_t table_id, ObIArray<ObITable::TableKey>& table_keys_array);

  int read_backup_metas(ObPGPartitionStoreMeta*& partition_store_meta,
      common::ObIArray<ObBackupSSTableInfo>*& sstable_info_array,
      common::ObArray<ObITable::TableKey>*& table_key_array);
  int fetch_table_ids(hash::ObHashSet<uint64_t>& tables_ids);
  bool is_inited() const
  {
    return is_inited_;
  }

private:
  int read_partition_meta_info(const ObPartitionKey& pkey, const int64_t backup_snapshot_version);

  int read_table_info(const ObPartitionKey& pkey, const uint64_t table_id, const int64_t multi_version_start,
      const int64_t version, ObPGStorage* pg_storage, ObFetchTableInfoResult& table_info);
  int read_all_sstable_metas();
  int read_sstable_meta(ObITable::TableKey& table_key, ObBackupSSTableInfo& sstable_info);
  int prepare_partition_store_meta_info(
      const ObPGPartitionStoreMeta& partition_store_meta, const ObTablesHandle& handle);
  int build_backup_sstable_info(const ObSSTable* sstable, ObBackupSSTableInfo& sstable_info);

private:
  bool is_inited_;
  int64_t data_size_;
  ObPGPartitionMetaInfo meta_info_;
  common::ObArray<ObBackupSSTableInfo> sstable_info_array_;
  common::ObArray<ObITable::TableKey> table_key_array_;
  const share::ObPhysicalBackupArg* arg_;
  common::ObArenaAllocator meta_allocator_;
  int64_t table_count_;
  ObPartitionKey pkey_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionMetaBackupReader);
};

class ObMacroBlockBackupSyncReader final {
public:
  ObMacroBlockBackupSyncReader();
  virtual ~ObMacroBlockBackupSyncReader();

  int init(const share::ObPhysicalBackupArg& args, const ObITable::TableKey& table_key,
      const obrpc::ObFetchMacroBlockArg& macro_arg);
  int64_t get_data_size() const
  {
    return data_size_;
  }
  int get_macro_block_meta(blocksstable::ObFullMacroBlockMeta& meta, blocksstable::ObBufferReader& data);
  void reset();
  TO_STRING_KV(K_(is_inited), K_(args), K_(data_size), K_(result_code), K_(is_data_ready), K_(macro_arg),
      K_(backup_index_tid), K_(full_meta), K_(data));

private:
  int process();
  int get_macro_read_info(const obrpc::ObFetchMacroBlockArg& arg, blocksstable::ObMacroBlockCtx& macro_block_ctx,
      blocksstable::ObMacroBlockReadInfo& read_info);

private:
  bool is_inited_;
  const share::ObPhysicalBackupArg* args_;
  common::ObArenaAllocator allocator_;
  int64_t data_size_;
  int32_t result_code_;
  bool is_data_ready_;
  obrpc::ObFetchMacroBlockArg macro_arg_;
  uint64_t backup_index_tid_;
  blocksstable::ObMacroBlockMetaHandle meta_handle_;
  blocksstable::ObFullMacroBlockMeta full_meta_;
  ObMacroBlockHandle macro_handle_;
  blocksstable::ObBufferReader data_;
  ObPartitionKey pkey_;
  ObTableHandle store_handle_;
  ObSSTable* sstable_;
  DISALLOW_COPY_AND_ASSIGN(ObMacroBlockBackupSyncReader);
};

class ObPartitionBaseDataMetaBackupReader {
public:
  ObPartitionBaseDataMetaBackupReader();
  virtual ~ObPartitionBaseDataMetaBackupReader();
  int init(
      const common::ObPartitionKey& pkey, const ObDataStorageInfo& data_info, const ObPhysicalBackupArg& backup_arg);
  int fetch_partition_meta(ObPGPartitionStoreMeta& partition_store_meta);
  int fetch_sstable_meta(const uint64_t index_id, blocksstable::ObSSTableBaseMeta& sstable_meta);
  int64_t get_data_size() const
  {
    return reader_.get_data_size();
  }
  int fetch_sstable_pair_list(const uint64_t index_id, common::ObIArray<blocksstable::ObSSTablePair>& pair_list);
  int fetch_all_table_ids(common::ObIArray<uint64_t>& table_id_array);
  int fetch_table_keys(const uint64_t index_id, obrpc::ObFetchTableInfoResult& table_res);
  TO_STRING_KV(K_(pkey), KP(backup_arg_), K_(last_read_size), K_(partition_store_meta), K_(snapshot_version),
      K_(schema_version), K_(data_version));

private:
  int prepare(const common::ObPartitionKey& pkey, const ObDataStorageInfo& data_info);

private:
  bool is_inited_;
  common::ObPartitionKey pkey_;
  const ObPhysicalBackupArg* backup_arg_;
  ObPartitionMetaBackupReader reader_;
  common::ObArenaAllocator allocator_;
  int64_t last_read_size_;
  ObPGPartitionStoreMeta partition_store_meta_;
  int64_t snapshot_version_;
  int64_t schema_version_;
  int64_t data_version_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionBaseDataMetaBackupReader);
};

class ObPartitionGroupMetaBackupReader;
class ObPhysicalBaseMetaBackupReader : public ObIPhysicalBaseMetaReader {
public:
  ObPhysicalBaseMetaBackupReader();
  virtual ~ObPhysicalBaseMetaBackupReader()
  {}
  int init(ObRestoreInfo& restore_info, const ObPartitionKey& pkey, const uint64_t table_id,
      ObPartitionGroupMetaBackupReader& reader);
  virtual int fetch_sstable_meta(blocksstable::ObSSTableBaseMeta& sstable_meta);
  virtual int fetch_macro_block_list(common::ObIArray<blocksstable::ObSSTablePair>& macro_block_list);
  virtual Type get_type() const
  {
    return BASE_DATA_META_BACKUP_READER;
  }

private:
  bool is_inited_;
  ObRestoreInfo* restore_info_;
  ObPartitionGroupMetaBackupReader* reader_;
  common::ObArenaAllocator allocator_;
  ObPartitionKey pkey_;
  uint64_t table_id_;
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalBaseMetaBackupReader);
};

class ObPartitionMacroBlockBackupReader : public ObIPartitionMacroBlockReader {
public:
  ObPartitionMacroBlockBackupReader();
  virtual ~ObPartitionMacroBlockBackupReader();
  int init(const ObPhysicalBackupArg& backup_arg, const ObIArray<ObBackupMacroBlockArg>& list);
  virtual int get_next_macro_block(blocksstable::ObFullMacroBlockMeta& meta, blocksstable::ObBufferReader& data,
      blocksstable::MacroBlockId& src_macro_id);
  virtual Type get_type() const
  {
    return MACRO_BLOCK_BACKUP_READER;
  }
  virtual int64_t get_data_size() const
  {
    return read_size_;
  }
  int64_t get_block_count() const
  {
    return readers_.count();
  }
  bool is_inited()
  {
    return is_inited_;
  }

private:
  int schedule_macro_block_task(const ObPhysicalBackupArg& backup_arg, const obrpc::ObFetchMacroBlockArg& arg,
      const ObITable::TableKey& table_key, ObMacroBlockBackupSyncReader& reader);

private:
  bool is_inited_;
  common::ObArray<obrpc::ObFetchMacroBlockArg> macro_list_;
  int64_t macro_idx_;
  common::ObArenaAllocator allocator_;
  common::ObArray<ObMacroBlockBackupSyncReader*> readers_;
  int64_t read_size_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionMacroBlockBackupReader);
};

class ObPartitionGroupMetaBackupReader {
public:
  ObPartitionGroupMetaBackupReader();
  virtual ~ObPartitionGroupMetaBackupReader();
  int init(const ObPartitionGroupMeta& pg_meta, const ObPhysicalBackupArg& backup_arg);
  int get_partition_readers(const ObPartitionArray& partitions,
      common::ObIArray<ObPartitionBaseDataMetaBackupReader*>& partition_reader_array);
  int fetch_sstable_meta(
      const uint64_t index_id, const ObPartitionKey& pkey, blocksstable::ObSSTableBaseMeta& sstable_meta);
  int fetch_sstable_pair_list(
      const uint64_t index_id, const ObPartitionKey& pkey, common::ObIArray<blocksstable::ObSSTablePair>& pair_list);

private:
  int prepare(const ObPartitionGroupMeta& pg_meta, const ObPhysicalBackupArg& backup_arg);

private:
  bool is_inited_;
  common::ObPGKey pg_key_;
  const ObPhysicalBackupArg* backup_arg_;
  int64_t last_read_size_;
  hash::ObHashMap<ObPartitionKey, ObPartitionBaseDataMetaBackupReader*> partition_reader_map_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionGroupMetaBackupReader);
};

class ObPGPartitionBaseDataMetaBackupReader : public ObIPGPartitionBaseDataMetaObReader {
public:
  ObPGPartitionBaseDataMetaBackupReader();
  virtual ~ObPGPartitionBaseDataMetaBackupReader();

  int init(const ObPartitionArray& partitions, ObPartitionGroupMetaBackupReader* reader);
  int fetch_pg_partition_meta_info(obrpc::ObPGPartitionMetaInfo& partition_meta_info);
  virtual Type get_type() const
  {
    return BASE_DATA_META_OB_BACKUP_READER;
  }

private:
  bool is_inited_;
  int64_t reader_index_;
  common::ObArray<ObPartitionBaseDataMetaBackupReader*> partition_reader_array_;
  DISALLOW_COPY_AND_ASSIGN(ObPGPartitionBaseDataMetaBackupReader);
};

class ObBackupFileAppender final {
public:
  ObBackupFileAppender();
  virtual ~ObBackupFileAppender();
  int open(common::ObInOutBandwidthThrottle& bandwidth_throttle, const share::ObPhysicalBackupArg& backup_arg,
      const common::ObString& path, const ObBackupFileType type);
  int close();
  bool is_valid() const;
  int append_meta_index(const common::ObIArray<ObBackupMetaIndex>& meta_index_array);
  int append_macro_index(
      const common::ObIArray<ObBackupTableMacroIndex>& macro_index_array, ObITable::TableKey& table_key);
  int append_partition_group_meta(
      const ObPartitionGroupMeta& pg_meta, ObBackupMetaIndex& meta_index, bool& is_uploaded);
  int append_partition_meta(
      const storage::ObPGPartitionStoreMeta& partition_meta, ObBackupMetaIndex& meta_index, bool& is_uploaded);
  int append_sstable_metas(const common::ObIArray<ObBackupSSTableInfo>& sstable_info_array,
      ObBackupMetaIndex& meta_index, bool& is_uploaded);
  int append_table_keys(const common::ObIArray<storage::ObITable::TableKey>& table_keys, ObBackupMetaIndex& meta_index,
      bool& is_uploaded);
  int append_macroblock_data(blocksstable::ObFullMacroBlockMeta& macro_meta, blocksstable::ObBufferReader& macro_data,
      ObBackupTableMacroIndex& macro_index);
  int append_backup_pg_meta_info(
      const ObBackupPGMetaInfo& pg_meta_info, ObBackupMetaIndex& meta_index, bool& is_uploaded);
  int sync_upload();
  int64_t get_upload_size()
  {
    return file_offset_ + data_buffer_.pos();
  }

  TO_STRING_KV(K_(is_opened), K_(file_offset), K_(max_buf_size), KP(backup_arg_));

private:
  int get_data_version(const ObBackupFileType data_type, uint16& data_version);
  int is_exist(const common::ObString& uri, bool& exist);
  template <class T>
  int write(const T& backup_base, int64_t& write_size, bool& is_uploaded);
  int write_tail();
  int open(const common::ObString path);

private:
  const static int64_t MAX_DATA_BUF_LENGTH = 1LL << 22;       // 4M
  const static int64_t MAX_INDEX_BUF_LENGTH = 1LL << 18;      // 256KB
  const static int64_t MAX_IDLE_TIME = 10 * 1000LL * 1000LL;  // 10s
  bool is_opened_;
  uint64_t file_offset_;
  int64_t max_buf_size_;
  ObBackupFileType file_type_;
  ObSelfBufferWriter tmp_buffer_;
  ObSelfBufferWriter data_buffer_;
  ObStorageAppender storage_appender_;
  ObBackupCommonHeader* common_header_;
  const share::ObPhysicalBackupArg* backup_arg_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupFileAppender);
};

class ObBackupMetaWriter final {
public:
  ObBackupMetaWriter();
  virtual ~ObBackupMetaWriter();
  int open(common::ObInOutBandwidthThrottle& bandwidth_throttle, ObIArray<ObPartMigrationTask>& task_list);
  int process();
  int close();
  TO_STRING_KV(K_(is_inited), K_(task_id), KP_(cp_fty), K_(meta_appender), K_(index_appender));

private:
  int prepare_appender(common::ObInOutBandwidthThrottle& bandwidth_throttle, const share::ObPhysicalBackupArg& arg);
  int check_task(const ObIArray<ObPartMigrationTask>& task_list) const;
  int get_min_snapshot_version(const common::ObIArray<ObITable::TableKey>& table_key_array, int64_t& snapshot_version);
  int append_meta_index(const bool need_upload, ObBackupMetaIndex& meta_index);
  void set_meta_index(const ObPartitionKey& pkey, ObBackupMetaIndex& meta_index);
  int write_pg_meta(const ObPartitionKey& pgkey, const ObPartitionGroupMeta& pg_meta);
  int write_partition_meta(const ObPartitionKey& pkey, const ObPGPartitionStoreMeta& partition_meta);
  int write_sstable_metas(const ObPartitionKey& pkey, const common::ObIArray<ObBackupSSTableInfo>& sstable_info_array);
  int write_table_keys(const ObPartitionKey& pkey, const common::ObArray<ObITable::TableKey>& table_keys);
  int create_partition_store_meta_info(const ObPGPartitionStoreMeta& partition_store_meta,
      const common::ObIArray<ObBackupSSTableInfo>& sstable_info_array,
      const common::ObIArray<ObITable::TableKey>& table_keys,
      ObBackupPartitionStoreMetaInfo& partition_store_meta_info);
  int write_backup_pg_meta_info(const ObBackupPGMetaInfo& pg_meta_info);

private:
  bool is_inited_;
  int64_t task_id_;
  ObIArray<ObPartMigrationTask>* task_list_;
  ObIPartitionComponentFactory* cp_fty_;
  ObBackupFileAppender meta_appender_;
  ObBackupFileAppender index_appender_;
  ObArray<ObBackupMetaIndex> meta_index_array_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMetaWriter);
};

struct ObBackupMacroBlockInfo final {
  ObBackupMacroBlockInfo();
  bool is_valid() const;
  TO_STRING_KV(K_(table_key), K_(start_index), K_(cur_block_count), K_(total_block_count));
  ObITable::TableKey table_key_;
  int64_t start_index_;
  int64_t cur_block_count_;
  int64_t total_block_count_;
};

class ObBackupPhysicalPGCtx {
public:
  class SubTask final {
  public:
    SubTask();
    virtual ~SubTask();
    void reset();

    TO_STRING_KV(K_(block_info), K_(block_count));
    ObArray<ObBackupMacroBlockInfo> block_info_;
    int64_t block_count_;
  };

  struct MacroIndexMergePoint final {
    MacroIndexMergePoint();
    void reset();
    bool is_valid() const;
    TO_STRING_KV(K_(table_id), K_(sstable_idx), KP_(macro_index_array));
    uint64_t table_id_;
    int64_t sstable_idx_;
    const ObArray<ObBackupTableMacroIndex>* macro_index_array_;
  };

  struct MacroIndexRetryPoint final {
    MacroIndexRetryPoint();
    void reset();
    bool is_valid() const;
    TO_STRING_KV(K_(table_key), K_(last_idx));
    ObITable::TableKey table_key_;
    int64_t last_idx_;
  };

public:
  const static uint64_t DEFAULT_WAIT_TIME = 10 * 1000 * 1000;  // 10s
  const static uint64_t MAX_MACRO_BLOCK_COUNT_PER_TASK = 512;  // 1GB per backup data file
  ObBackupPhysicalPGCtx();
  virtual ~ObBackupPhysicalPGCtx();
  // TODO() delete this interface later
  int open(common::ObInOutBandwidthThrottle& bandwidth_throttle, const share::ObPhysicalBackupArg& arg,
      const common::ObPGKey& pg_key);
  int open(common::ObInOutBandwidthThrottle& bandwidth_throttle, const share::ObPhysicalBackupArg& arg,
      const common::ObPGKey& pg_key, const ObBackupDataType& backup_data_type);
  int init(common::ObInOutBandwidthThrottle& bandwidth_throttle, const share::ObPhysicalBackupArg& arg,
      const common::ObPGKey& pg_key);
  int open();
  int close();
  void reset();
  bool is_valid() const;
  int64_t get_task_count() const;
  void set_result(const int32_t ret);
  int get_result() const;
  int add_backup_macro_block_info(const ObBackupMacroBlockInfo& block_info);
  int wait_for_turn(const int64_t task_idx);
  int finish_task(const int64_t task_idx);
  int fetch_prev_macro_index(const ObPhyRestoreMacroIndexStoreV2& macro_index_store,
      const ObBackupMacroBlockArg& macro_arg, ObBackupTableMacroIndex& macro_index);
  int check_table_exist(
      const ObITable::TableKey& table_key, const ObPhyRestoreMacroIndexStoreV2& macro_index_store, bool& is_exist);
  bool is_opened() const
  {
    return is_opened_;
  }

  TO_STRING_KV(K_(macro_block_count), K_(base_task_id), K_(retry_cnt), K_(task_turn), K_(index_merge_point), K_(result),
      K_(pg_key), K_(table_keys), "task_count", tasks_.count(), K_(macro_index_appender));
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  ObArray<ObITable::TableKey> table_keys_;
  ObArray<SubTask> tasks_;
  ObBackupFileAppender macro_index_appender_;
  int64_t macro_block_count_;
  int64_t block_count_per_task_;
  int64_t base_task_id_;
  int64_t retry_cnt_;

private:
  int fetch_available_sub_task(SubTask*& sub_task);
  int init_macro_index_appender();
  int init_already_backup_data();  // for retry
  int fetch_retry_points(const ObString& path, const ObString& storage_info);
  int reuse_already_backup_data(ObBackupMacroBlockInfo& block_info);  // for retry
  int get_tenant_pg_data_path(const ObBackupBaseDataPathInfo& path_info, ObBackupPath& path);
  int get_macro_block_index_path(
      const ObBackupBaseDataPathInfo& path_info, const int64_t retry_cnt, ObBackupPath& path);

private:
  common::ObThreadCond cond_;
  volatile int64_t task_turn_;
  MacroIndexMergePoint index_merge_point_;
  common::SpinRWLock lock_;
  int32_t result_;
  common::ObPGKey pg_key_;
  const share::ObPhysicalBackupArg* backup_arg_;
  bool find_breakpoint_;  // make sure retry backup only one breakpoint
  ObArray<MacroIndexRetryPoint> retry_points_;
  bool is_opened_;
  ObBackupDataType backup_data_type_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupPhysicalPGCtx);
};

class ObBackupCopyPhysicalTask : public share::ObITask {
public:
  ObBackupCopyPhysicalTask();
  virtual ~ObBackupCopyPhysicalTask();
  virtual int generate_next_task(ObITask*& next_task) override;
  int init(const int64_t task_idx, ObMigrateCtx& ctx);
  int init(const int64_t task_idx, const ObITable::TableKey& already_backup_table_key, ObMigrateCtx& ctx);
  virtual int process() override;

private:
  int get_macro_block_backup_reader(const ObIArray<ObBackupMacroBlockArg>& list, const ObPhysicalBackupArg& backup_arg,
      ObPartitionMacroBlockBackupReader*& reader);
  int fetch_backup_macro_block_arg(const share::ObPhysicalBackupArg& backup_arg, const ObITable::TableKey& table_key,
      const int64_t macro_idx, ObBackupMacroBlockArg& macro_arg);
  int fetch_physical_block_with_retry(
      const common::ObIArray<ObBackupMacroBlockArg>& list, const int64_t copy_count, const int64_t reuse_count);
  int backup_physical_block(
      const common::ObIArray<ObBackupMacroBlockArg>& list, const int64_t copy_count, const int64_t reuse_count);
  int get_datafile_appender(const ObITable::TableType& table_type, const share::ObPhysicalBackupArg& arg,
      const common::ObPGKey& pg_key, ObBackupFileAppender& macro_file);
  int backup_block_data(ObPartitionMacroBlockBackupReader& reader, ObBackupFileAppender& macro_file,
      ObBackupTableMacroIndex& block_index);
  int backup_block_index(const int64_t reuse_count, const ObIArray<ObBackupMacroBlockArg>& list,
      ObIArray<ObBackupTableMacroIndex>& macro_indexs);
  int reuse_block_index(const ObIArray<ObBackupMacroBlockArg>& list, ObIArray<ObBackupTableMacroIndex>& macro_indexs,
      int64_t& reuse_count);
  int calc_migrate_data_statics(const int64_t copy_count, const int64_t reuse_count);

private:
  static const int64_t OB_FETCH_MAJOR_BLOCK_RETRY_INTERVAL = 1 * 1000 * 1000L;  // 1s
  bool is_inited_;
  ObMigrateCtx* ctx_;
  int64_t task_idx_;
  int64_t base_task_id_;  // default=0, base_task_id_ + task_idx_ = data file name;

  ObBackupPhysicalPGCtx* backup_pg_ctx_;
  ObBackupPhysicalPGCtx::SubTask* sub_task_;
  ObIPartitionComponentFactory* cp_fty_;
  ObSSTableMacroBlockChecker checker_;
  ObITable::TableKey already_backup_table_key_;
  int64_t output_macro_data_bytes_;
  int64_t input_macro_data_bytes_;

  DISALLOW_COPY_AND_ASSIGN(ObBackupCopyPhysicalTask);
};

class ObBackupFinishTask : public share::ObITask {
public:
  ObBackupFinishTask();
  virtual ~ObBackupFinishTask();
  int init(ObMigrateCtx& migrate_ctx);
  virtual int process() override;

private:
  bool is_inited_;
  ObMigrateCtx* ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupFinishTask);
};

struct ObBackupMacroData final {
  static const int64_t BACKUP_MARCO_DATA_VERSION = 1;
  OB_UNIS_VERSION(BACKUP_MARCO_DATA_VERSION);

public:
  ObBackupMacroData(blocksstable::ObBufferHolder& meta, blocksstable::ObBufferReader& data);
  TO_STRING_KV(K_(data), K_(meta));

private:
  blocksstable::ObBufferHolder& meta_;
  blocksstable::ObBufferReader& data_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMacroData);
};

template <class T>
int ObBackupFileAppender::write(const T& backup_base, int64_t& write_size, bool& is_uploaded)
{
  int ret = OB_SUCCESS;
  const int64_t need_write_size = backup_base.get_serialize_size();
  write_size = 0;
  is_uploaded = false;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup file appender not init", K(ret), K(is_opened_));
  } else if (0 == need_write_size) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ObBackupFileAppender write nothing", K(ret), K(need_write_size));
  } else if (data_buffer_.length() + need_write_size > max_buf_size_) {
    if (OB_FAIL(sync_upload())) {
      STORAGE_LOG(WARN, "upload buffer fail", K(ret));
    } else {
      is_uploaded = true;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(common_header_)) {
      const int64_t header_len = sizeof(ObBackupCommonHeader);
      if (data_buffer_.length() + header_len + need_write_size > max_buf_size_) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN, "data buffer not enough", K(ret), K(header_len), K(need_write_size));
      } else {
        char* header_buf = data_buffer_.data();
        if (OB_FAIL(data_buffer_.advance_zero(header_len))) {
          STORAGE_LOG(WARN, "advance failed", K(ret), K(header_len));
        } else {
          common_header_ = reinterpret_cast<ObBackupCommonHeader*>(header_buf);
          common_header_->reset();
          common_header_->compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
          common_header_->data_type_ = file_type_;
          if (OB_FAIL(get_data_version(file_type_, common_header_->data_version_))) {
            STORAGE_LOG(WARN, "get common header data version fail", K(ret), K_(file_type));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t last_pos = data_buffer_.pos();
    if (OB_FAIL(data_buffer_.write_serialize(backup_base))) {
      STORAGE_LOG(WARN, "data buffer write fail", K(ret), K(need_write_size));
    } else if (data_buffer_.pos() - last_pos > need_write_size) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR,
          "serialize size must not large than need write size",
          K(ret),
          "cur_pos",
          data_buffer_.pos(),
          K(last_pos),
          K(need_write_size),
          K(backup_base));
    } else {
      write_size = data_buffer_.pos() - last_pos;
      common_header_->data_length_ += write_size;
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase

#endif /* SRC_STORAGE_OB_PARTITION_BASE_DATA_BACKUP_H_ */
