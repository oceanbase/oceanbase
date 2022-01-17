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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_BASE_DATA_VALIDATE_H_
#define OCEANBASE_STORAGE_OB_PARTITION_BASE_DATA_VALIDATE_H_

#include <stdint.h>
#include "lib/container/ob_iarray.h"
#include "archive/ob_archive_log_file_store.h"
#include "clog/ob_log_entry.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_pg_validate_task_updater.h"
#include "storage/ob_i_partition_base_data_reader.h"

namespace oceanbase {
namespace blocksstable {
class ObBufferHolder;
class ObSSTableMacroBlockChecker;
}  // namespace blocksstable
namespace common {
class ObMySQLProxy;
class ObStorageReader;
class ModulePageAllocator;
class ObInOutBandwidthThrottle;
}  // namespace common
namespace obrpc {
class ObSrvRpcProxy;
class ObCommonRpcProxy;
}  // namespace obrpc
namespace share {
class ObBackupPath;
class ObBackupBaseDataPathInfo;
}  // namespace share
namespace storage {
class ObMigrateCtx;
class ObBackupChecksumChecker;

// TODO : extract common interfaces
class ObBackupMetaIndexStore {
public:
  ObBackupMetaIndexStore();
  virtual ~ObBackupMetaIndexStore()
  {}
  int init(const share::ObBackupBaseDataPathInfo& path_info);
  bool is_inited() const
  {
    return is_inited_;
  }
  void reset();
  int get_meta_index(
      const common::ObPartitionKey& pkey, const share::ObBackupMetaType& type, share::ObBackupMetaIndex& meta_index);

private:
  int init_from_remote_file(const common::ObString& path, const common::ObString& storage_info);

private:
  typedef common::hash::ObHashMap<share::ObMetaIndexKey, share::ObBackupMetaIndex> MetaIndexMap;

  bool is_inited_;
  MetaIndexMap meta_index_map_;
  common::ObArenaAllocator allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupMetaIndexStore);
};

// TODO : extract common interfaces
class ObBackupMacroIndexStore {
public:
  ObBackupMacroIndexStore();
  virtual ~ObBackupMacroIndexStore()
  {}
  int init(const share::ObBackupBaseDataPathInfo& path_info);
  bool is_inited() const
  {
    return is_inited_;
  }
  void reset();
  int get_macro_block_index(const common::ObPGKey& pg_key, common::ObArray<share::ObBackupMacroIndex>*& index_list);

private:
  int init_from_remote_file(
      const common::ObPGKey& pg_key, const common::ObString& path, const common::ObString& storage_info);
  int add_sstable_index(const common::ObPGKey& pg_key, const common::ObIArray<share::ObBackupMacroIndex>& index_list);

private:
  typedef common::hash::ObHashMap<common::ObPGKey, common::ObArray<share::ObBackupMacroIndex>*> MacroIndexMap;

  bool is_inited_;
  share::ObBackupBaseDataPathInfo path_info_;
  MacroIndexMap macro_index_map_;
  common::ObArenaAllocator allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupMacroIndexStore);
};

class ObValidateBackupPGCtx {
public:
  struct SubTask final {
    SubTask();
    virtual ~SubTask();
    void reset();

    int64_t macro_block_count_;
    share::ObBackupMacroIndex* macro_block_infos_;
    common::ObPartitionKey pkey_;
  };

public:
  ObValidateBackupPGCtx();
  virtual ~ObValidateBackupPGCtx();
  int init(storage::ObMigrateCtx& migrate_ctx, common::ObInOutBandwidthThrottle& bandwidth_throttle);
  void reset();
  bool is_valid() const;
  int get_result() const
  {
    return result_;
  }
  ObBackupMacroIndexStore& get_macro_index_store()
  {
    return macro_index_store_;
  }
  int64_t get_sub_task_count() const
  {
    return sub_task_cnt_;
  }
  bool is_pg_key_only_in_clog() const
  {
    return only_in_clog_;
  }
  TO_STRING_KV(K_(result), K_(pg_key), K_(sub_task_cnt), K_(path_info));

private:
  int fetch_next_sub_task(SubTask*& sub_task);

public:
  static const int64_t MAX_MACRO_BLOCK_COUNT_PER_SUB_TASK = 1 << 7;
  bool is_inited_;
  bool is_dropped_tenant_;
  bool need_validate_clog_;
  bool only_in_clog_;
  int64_t job_id_;
  int32_t result_;
  int64_t retry_count_;
  int64_t archive_round_;
  int64_t backup_set_id_;
  int64_t current_task_idx_;
  int64_t min_clog_file_id_;
  int64_t max_clog_file_id_;
  int64_t last_replay_log_id_;
  int64_t current_clog_file_id_;
  int64_t start_log_id_;
  int64_t end_log_id_;
  int64_t total_log_size_;
  int64_t clog_end_timestamp_;
  int64_t sub_task_cnt_;
  int64_t total_partition_count_;
  int64_t finish_partition_count_;
  int64_t total_macro_block_count_;
  int64_t finish_macro_block_count_;
  int64_t macro_block_count_per_sub_task_;
  SubTask* sub_tasks_;
  ObBackupMacroIndexStore macro_index_store_;
  archive::ObArchiveLogFileStore archive_file_store_;
  common::ObPGKey pg_key_;
  common::ObArenaAllocator allocator_;
  common::ObMySQLProxy* sql_proxy_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  obrpc::ObCommonRpcProxy* rs_rpc_proxy_;
  share::ObBackupBaseDataPathInfo path_info_;
  storage::ObMigrateCtx* migrate_ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObValidateBackupPGCtx);
};

class ObValidatePrepareTask : public share::ObITask {
public:
  ObValidatePrepareTask();
  virtual ~ObValidatePrepareTask();
  int init(ObMigrateCtx& migrate_ctx, ObValidateBackupPGCtx& validate_pg_ctx);
  virtual int process() override;

private:
  bool is_inited_;
  ObMigrateCtx* migrate_ctx_;
  ObValidateBackupPGCtx* validate_pg_ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObValidatePrepareTask);
};

class ObValidateClogDataTask : public share::ObITask {
public:
  ObValidateClogDataTask();
  virtual ~ObValidateClogDataTask();
  int init(const int64_t clog_id, ObMigrateCtx& migrate_ctx, ObValidateBackupPGCtx& validate_pg_ctx);
  virtual int generate_next_task(share::ObITask*& next_task) override;
  virtual int process() override;

private:
  const uint64_t TIMEOUT = 60 * 1000 * 1000L;
  bool is_inited_;
  int cur_clog_file_id_;
  int64_t clog_end_timestamp_;
  ObMigrateCtx* migrate_ctx_;
  ObValidateBackupPGCtx* validate_pg_ctx_;
  archive::ObArchiveLogFileStore* archive_log_file_store_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObValidateClogDataTask);
};

class ObValidateBaseDataTask : public share::ObITask {
public:
  ObValidateBaseDataTask();
  virtual ~ObValidateBaseDataTask();
  int init(const int64_t task_idx, ObMigrateCtx& migrate_ctx, ObValidateBackupPGCtx& validate_pg_ctx);
  virtual int generate_next_task(share::ObITask*& next_task) override;
  virtual int process() override;

private:
  int check_base_data_valid(bool& is_valid);

  int fetch_macro_block_with_retry(
      const share::ObBackupMacroIndex& macro_index, const int64_t retry_cnt, blocksstable::ObBufferReader& macro_data);
  int fetch_macro_block(const share::ObBackupMacroIndex& macro_index, blocksstable::ObBufferReader& macro_data);
  int read_macro_block_data(const common::ObString& path, const common::ObString& storage_info,
      const share::ObBackupMacroIndex& macro_index, common::ObIAllocator& allocator,
      blocksstable::ObBufferReader& macro_data);
  int checksum_macro_block_data(blocksstable::ObBufferReader& buffer_reader, bool& is_valid);

private:
  static const int64_t FETCH_MACRO_BLOCK_RETRY_INTERVAL = 1 * 1000 * 1000L;
  static const int64_t MAX_RETRY_TIME = 3L;

  bool is_inited_;
  int64_t task_idx_;
  ObMigrateCtx* migrate_ctx_;
  ObValidateBackupPGCtx* validate_pg_ctx_;
  ObValidateBackupPGCtx::SubTask* sub_task_;

  common::ObArenaAllocator* allocator_;
  ObBackupMacroIndexStore* macro_index_store_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObValidateBaseDataTask);
};

class ObValidateFinishTask : public share::ObITask {
public:
  ObValidateFinishTask();
  virtual ~ObValidateFinishTask();
  int init(ObMigrateCtx& migrate_ctx_);
  virtual int process() override;
  ObValidateBackupPGCtx& get_validate_ctx()
  {
    return validate_pg_ctx_;
  }

private:
  bool is_inited_;
  ObMigrateCtx* migrate_ctx_;
  ObValidateBackupPGCtx validate_pg_ctx_;
  share::ObPGValidateTaskUpdater updater_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObValidateFinishTask);
};

class ObValidateBackupUtil {
public:
  static int read_single_file(const common::ObString& path, const common::ObString& storage_info,
      ObIAllocator& allocator, char*& buf, int64_t& read_size);
  static int read_part_file(const common::ObString& path, const common::ObString& storage_info, char* buf,
      const int64_t read_size, const int64_t offset);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif
