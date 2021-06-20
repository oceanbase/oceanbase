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

#ifndef OCEANBASE_ROOTSERVER_OB_BACKUP_ARCHIVELOG_H_
#define OCEANBASE_ROOTSERVER_OB_BACKUP_ARCHIVELOG_H_
#include "archive/ob_log_archive_define.h"
#include "clog/ob_log_reader_interface.h"
#include "lib/allocator/page_arena.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/string/ob_string.h"
#include "lib/hash/ob_link_hashmap.h"
#include "share/ob_thread_pool.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_path.h"
#include "share/scheduler/ob_dag_scheduler.h"
namespace oceanbase {
namespace common {
class ObInOutBandwidthThrottle;
}
namespace archive {
class ObArchiveLogFileStore;
}
namespace storage {
class ObMigrateCtx;

struct ObBackupArchiveLogPGCtx {
public:
  ObBackupArchiveLogPGCtx();
  virtual ~ObBackupArchiveLogPGCtx();

  int open(ObMigrateCtx& mig_ctx, const common::ObPGKey& pg_key, common::ObInOutBandwidthThrottle& throttle);
  int close();

  TO_STRING_KV(K_(pg_key), K_(src_backup_dest), K_(src_storage_info), K_(dst_backup_dest), K_(dst_storage_info));

  bool is_opened_;
  common::ObPGKey pg_key_;
  int64_t log_archive_round_;
  int64_t checkpoint_ts_;
  int64_t rs_checkpoint_ts_;
  char src_backup_dest_[share::OB_MAX_BACKUP_PATH_LENGTH];
  char src_storage_info_[share::OB_MAX_BACKUP_STORAGE_INFO_LENGTH];
  char dst_backup_dest_[share::OB_MAX_BACKUP_PATH_LENGTH];
  char dst_storage_info_[share::OB_MAX_BACKUP_STORAGE_INFO_LENGTH];

  ObMigrateCtx* mig_ctx_;
  common::ObInOutBandwidthThrottle* throttle_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupArchiveLogPGCtx);
};

class ObBackupArchiveLogPGTask : public share::ObITask {
  struct RoundRange {
    RoundRange() : min_round_(OB_INVALID_ID), max_round_(OB_INVALID_ID)
    {}
    bool is_valid() const
    {
      return OB_INVALID_ID != min_round_ && OB_INVALID_ID != max_round_;
    }
    void reset()
    {
      min_round_ = OB_INVALID_ID;
      max_round_ = OB_INVALID_ID;
    }
    TO_STRING_KV(K_(min_round), K_(max_round));
    uint64_t min_round_;
    uint64_t max_round_;
  };
  struct FileRange {
    FileRange() : min_file_id_(OB_INVALID_ID), max_file_id_(OB_INVALID_ID)
    {}
    bool is_valid() const
    {
      return OB_INVALID_ID != min_file_id_ && OB_INVALID_ID != max_file_id_;
    }
    void reset()
    {
      min_file_id_ = OB_INVALID_ID;
      max_file_id_ = OB_INVALID_ID;
    }
    TO_STRING_KV(K_(min_file_id), K_(max_file_id));
    uint64_t min_file_id_;
    uint64_t max_file_id_;
  };
  struct FileInfo {
    FileInfo()
    {}
    TO_STRING_KV(K_(uri), K_(info));
    common::ObString uri_;
    common::ObString info_;
    char dest_path_[share::OB_MAX_BACKUP_PATH_LENGTH];             // under line storage for uri_
    char storage_info_[share::OB_MAX_BACKUP_STORAGE_INFO_LENGTH];  // under line storage for info_
  };

public:
  ObBackupArchiveLogPGTask();
  virtual ~ObBackupArchiveLogPGTask();
  int init(ObMigrateCtx& mig_ctx, ObBackupArchiveLogPGCtx& pg_ctx);
  virtual int process() override;

private:
  int check_nfs_mounted_if_nfs(const ObBackupArchiveLogPGCtx& pg_ctx, bool& mounted);
  int converge_task(
      const archive::LogArchiveFileType file_type, const ObBackupArchiveLogPGCtx& pg_ctx, FileRange& file_range);
  int catchup_archive_log(const archive::LogArchiveFileType file_type, const ObBackupArchiveLogPGCtx& pg_ctx,
      const FileRange& data_file_range);
  int get_file_range(const char* backup_dest, const common::ObString& storage_info,
      const archive::LogArchiveFileType file_type, const int64_t log_archive_round, const common::ObPGKey& pg_key,
      FileRange& file_range);
  int init_archive_file_store_(ObBackupArchiveLogPGCtx& task, const int64_t incarnation, const int64_t round,
      archive::ObArchiveLogFileStore& file_store);
  int get_file_path_info(const char* backup_dest, const char* storage_info, const common::ObPGKey& pg_key,
      const archive::LogArchiveFileType file_type, const int64_t incarnation, const int64_t round,
      const int64_t file_id, FileInfo& file_info);
  int extract_last_log_in_data_file(ObBackupArchiveLogPGCtx& pg_ctx, const int64_t file_id);
  int cal_data_file_range_delta(const common::ObPGKey& pkey, const int64_t log_archive_round,
      const FileRange& src_file_range, const FileRange& dst_file_range, FileRange& delta_file_range);
  int check_archive_log_interrupted(const int64_t log_archive_round, const FileRange& src_file_range,
      const FileRange& dst_file_range, bool& interrupted);
  int check_file_exist(const FileInfo& file_info, bool& file_exist);
  int get_file_length(const FileInfo& file_info, int64_t& file_length);
  int check_and_mkdir(const archive::LogArchiveFileType file_type, const ObBackupArchiveLogPGCtx& pg_ctx);
  int do_file_transfer(const archive::LogArchiveFileType file_type, const ObBackupArchiveLogPGCtx& pg_ctx,
      const FileInfo& src_info, const FileInfo& dst_info, const bool dst_exist);
  int do_single_file_transfer(const archive::LogArchiveFileType file_type, const ObBackupArchiveLogPGCtx& pg_ctx,
      const FileInfo& src_info, const FileInfo& dst_info);
  int do_part_file_transfer(const FileInfo& src_info, const FileInfo& dst_info);
  int read_single_file(const FileInfo& file_info, clog::ObReadBuf& rbuf);
  int write_single_file(const FileInfo& file_info, const clog::ObReadBuf& buf);
  int read_part_file(const FileInfo& file_info, const int64_t offset, clog::ObReadBuf& rbuf);
  int write_part_file(const FileInfo& file_info, const clog::ObReadBuf& buf);
  int build_archive_file_prefix(const char* backup_dest, const common::ObPGKey& pg_key,
      const archive::LogArchiveFileType file_type, const int64_t incarnation, const int64_t round, char* dest_path,
      int64_t& pos);
  int build_archive_file_path(const char* backup_dest, const common::ObPGKey& pg_key,
      const archive::LogArchiveFileType file_type, const int64_t incarnation, const int64_t round,
      const int64_t file_id, char* dest_path);
  int build_file_prefix(const ObPGKey& pg_key, const char* base_path, const archive::LogArchiveFileType file_type,
      char* dest_path_buf, int64_t& pos);
  const char* get_file_prefix_with_type(const archive::LogArchiveFileType file_type);

private:
  int try_touch_archive_key(const ObBackupArchiveLogPGCtx& pg_ctx, const int64_t incarnation, const int64_t round);
  int build_archive_key_prefix(
      const ObBackupArchiveLogPGCtx& pg_ctx, const ObPGKey& pg_key, const int64_t incarnation, const int64_t round);
  int touch_archive_key_file(
      const ObBackupArchiveLogPGCtx& pg_ctx, const int64_t incarnation, const int64_t round, const ObPGKey& pg_key);
  int build_archive_key_path(const ObBackupArchiveLogPGCtx& pg_ctx, const int64_t incarnation, const int64_t round,
      const ObPGKey& pkey, share::ObBackupPath& archive_key_path);

private:
  bool is_inited_;
  common::ObPGKey pg_key_;
  ObMigrateCtx* mig_ctx_;
  ObBackupArchiveLogPGCtx* pg_ctx_;
  common::ObInOutBandwidthThrottle* throttle_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupArchiveLogPGTask);
};

class ObBackupArchiveLogFinishTask : public share::ObITask {
public:
  ObBackupArchiveLogFinishTask();
  virtual ~ObBackupArchiveLogFinishTask();
  int init(ObMigrateCtx& mig_ctx);
  virtual int process() override;
  ObBackupArchiveLogPGCtx& get_backup_archivelog_ctx()
  {
    return pg_ctx_;
  }

private:
  bool is_inited_;
  ObMigrateCtx* mig_ctx_;
  ObBackupArchiveLogPGCtx pg_ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupArchiveLogFinishTask);
};

}  // end namespace storage
}  // end namespace oceanbase
#endif
