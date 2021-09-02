// Copyright 2020 Alibaba Inc. All Rights Reserved
// Author:
//     yanfeng <yangyi.yyy@alibaba-inc.com>
// Normalizer:
//     yanfeng <yangyi.yyy@alibaba-inc.com>

#ifndef OCEANBASE_STORAGE_OB_PARTITION_BASE_DATA_BACKUP_BACKUPSET_H_
#define OCEANBASE_STORAGE_OB_PARTITION_BASE_DATA_BACKUP_BACKUPSET_H_

#include "storage/ob_partition_base_data_backup.h"
#include "storage/blocksstable/ob_macro_block_checker.h"
#include "storage/blocksstable/ob_store_file.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase {
namespace common {
class ObArenaAllocator;
class ObInOutBandwidthThrottle;
}  // namespace common
namespace storage {
class ObMigrateCtx;
class ObBackupFileAppender;

struct ObBackupBackupsetPGCtx {
public:
  struct SubTask {
    SubTask();
    virtual ~SubTask();
    void reset();
    TO_STRING_KV(K_(macro_block_cnt), K_(macro_index_list));

    int64_t macro_block_cnt_;
    share::ObBackupMacroIndex* macro_index_list_;
  };

public:
  ObBackupBackupsetPGCtx();
  virtual ~ObBackupBackupsetPGCtx();

  int open(ObMigrateCtx& mig_ctx, const common::ObPartitionKey& pg_key,
      common::ObInOutBandwidthThrottle& bandwidth_throttle);
  int close();

  int wait_for_task(int64_t task_idx);
  int finish_task(int64_t task_idx);
  int get_result() const
  {
    return result_;
  }

  int init_macro_index_appender();
  int convert_to_backup_arg(const share::ObBackupBackupsetArg& in_arg, share::ObPhysicalBackupArg& out_arg);

  static const int64_t DEFAULT_WAIT_TIME = 10 * 1000 * 1000L;

  TO_STRING_KV(K_(is_opened), K_(sub_task_cnt), K_(pg_key));

  bool is_opened_;
  bool all_macro_block_need_reuse_;
  int64_t retry_cnt_;  // TODO
  int result_;
  SubTask* sub_tasks_;
  int64_t sub_task_cnt_;
  ObMigrateCtx* mig_ctx_;
  common::ObPartitionKey pg_key_;
  common::ObArenaAllocator allocator_;
  common::ObInOutBandwidthThrottle* throttle_;
  share::ObBackupBackupsetArg backup_backupset_arg_;
  storage::ObBackupFileAppender* macro_index_appender_;

  ObPhyRestoreMacroIndexStore reuse_macro_index_store_;

  int64_t partition_cnt_;
  int64_t total_macro_block_cnt_;
  int64_t finish_macro_block_cnt_;

  common::ObThreadCond cond_;
  volatile int64_t sub_task_idx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupBackupsetPGCtx);
};

class ObBackupBackupsetPGFileCtx {
public:
  ObBackupBackupsetPGFileCtx();
  virtual ~ObBackupBackupsetPGFileCtx();
  int open(const share::ObBackupBackupsetArg& arg, const common::ObPGKey& pg_key);
  int close();
  int get_result() const
  {
    return result_;
  }

  share::ObBackupBackupsetArg backup_backupset_arg_;
  common::ObArray<common::ObString> major_files_;
  common::ObArray<common::ObString> minor_files_;
  common::ObArenaAllocator allocator_;  // for files
  common::ObPartitionKey pg_key_;
  int64_t minor_task_id_;
  bool is_opened_;
  int result_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupBackupsetPGFileCtx);
};

class ObBackupBackupsetFileTask : public share::ObITask {
public:
  ObBackupBackupsetFileTask();
  virtual ~ObBackupBackupsetFileTask();
  int init(int task_idx, ObMigrateCtx& mig_ctx, ObBackupBackupsetPGFileCtx& pg_ctx);
  virtual int generate_next_task(share::ObITask*& next_task) override;
  virtual int process() override;

private:
  int get_next_file(common::ObString& file_name, bool& is_minor);
  int check_and_mkdir(const common::ObPGKey& pg_key, const bool is_minor);
  int transfer_file_with_retry(const common::ObPGKey& pg_key, const common::ObString& file_name, const bool is_minor);
  int transfer_file(const common::ObPGKey& pg_key, const common::ObString& file_name, const bool is_minor);
  int inner_transfer_file(
      const common::ObPGKey& pg_key, const common::ObString& file_name, const bool is_minor, int64_t& transfer_len);
  int get_file_backup_path(const common::ObPGKey& pg_key, const common::ObString& file, const bool is_src,
      const bool is_minor, ObBackupPath& path);
  int get_file_length(const common::ObString& file_path, const common::ObString& storage_info, int64_t& length);
  int read_part_file(
      const ObString& file_path, const ObString& storage_info, const int64_t offset, const int64_t len, char*& buf);
  int get_transfer_len(const int64_t delta_len, int64_t& transfer_len);
  void* bb_malloc(const int64_t byte);
  void bb_free(void* ptr);

private:
  static const int64_t RETRY_TIME = 3;
  static const int64_t RETRY_SLEEP_INTERVAL = 1000 * 1000;

private:
  bool is_inited_;
  int task_idx_;
  ObMigrateCtx* mig_ctx_;
  ObBackupBackupsetPGFileCtx* pg_ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupBackupsetFileTask);
};

class ObBackupBackupsetFinishTask : public share::ObITask {
public:
  ObBackupBackupsetFinishTask();
  virtual ~ObBackupBackupsetFinishTask();
  int init(ObMigrateCtx& mig_ctx);
  virtual int process() override;
  ObBackupBackupsetPGCtx& get_backup_backupset_ctx()
  {
    return pg_ctx_;
  }
  ObBackupBackupsetPGFileCtx& get_backup_backupset_file_ctx()
  {
    return pg_file_ctx_;
  }

private:
  int report_pg_task_stat();

private:
  bool is_inited_;
  ObMigrateCtx* mig_ctx_;
  ObBackupBackupsetPGCtx pg_ctx_;
  ObBackupBackupsetPGFileCtx pg_file_ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupBackupsetFinishTask);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif
