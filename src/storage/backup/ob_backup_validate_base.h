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

#ifndef STORAGE_LOG_STREAM_BACKUP_VALIDATE_BASE_H_
#define STORAGE_LOG_STREAM_BACKUP_VALIDATE_BASE_H_

#include "share/ob_common_rpc_proxy.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/backup/ob_backup_validate_struct.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_path.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_sstable_sec_meta_iterator.h"
#include "share/backup/ob_archive_store.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/backup/ob_backup_block_file_reader_writer.h"

namespace oceanbase
{
namespace storage
{
struct ObBackupFileGroup
{
  OB_UNIS_VERSION(1);
public:
  ObBackupFileGroup();
  ~ObBackupFileGroup() {}
  void reset();
  bool is_valid() const;
  int assign(const ObBackupFileGroup &other);
  TO_STRING_KV(K_(group_id), K_(accumulated_file_count), K_(file_list));

  int64_t group_id_;
  int64_t accumulated_file_count_;
  common::ObSArray<backup::ObBackupFileInfo> file_list_;
};

struct ObBackupValidateDagNetInitParam : public share::ObIDagInitParam
{
public:
  ObBackupValidateDagNetInitParam();
  virtual ~ObBackupValidateDagNetInitParam() {}
  void reset();
  bool is_valid() const override;
  int assign(const ObBackupValidateDagNetInitParam &other);
  int set(const obrpc::ObBackupValidateLSArg &args);
  bool operator == (const ObBackupValidateDagNetInitParam &other) const;
  bool operator != (const ObBackupValidateDagNetInitParam &other) const;
  uint64_t hash() const;
  VIRTUAL_TO_STRING_KV(K_(ls_id), K_(trace_id), K_(tenant_id), K_(incarnation),
      K_(task_id), K_(task_type), K_(validate_id), K_(dest_id),
      K_(round_id), K_(validate_level), K_(validate_path));

public:
  share::ObTaskId trace_id_;
  int64_t job_id_;
  uint64_t tenant_id_;
  int64_t incarnation_;
  uint64_t task_id_;
  share::ObLSID ls_id_;
  share::ObBackupValidateType task_type_;
  int64_t validate_id_;
  int64_t dest_id_;
  int64_t round_id_;
  share::ObBackupValidateLevel validate_level_;
  share::ObBackupPathString validate_path_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateDagNetInitParam);
};

struct ObBackupArchivePieceLSNRange
{
  OB_UNIS_VERSION(1);
public:
  ObBackupArchivePieceLSNRange();
  ~ObBackupArchivePieceLSNRange() {}
  void reset();
  bool is_valid() const;
  int assign(const ObBackupArchivePieceLSNRange &other);
  TO_STRING_KV(K_(group_id), K_(start_lsn), K_(end_lsn));

  int64_t group_id_;
  palf::LSN start_lsn_;
  palf::LSN end_lsn_;
};

class ObBackupValidateTaskContext final
{
public:
  ObBackupValidateTaskContext();
  virtual ~ObBackupValidateTaskContext() { running_groups_.destroy(); }
  int init();
  void reset();
  bool is_valid() const;
  int set_dir_queue(const common::ObIArray<ObBackupPathString> &dir_queue);
  int get_dir_path(const int64_t dir_queue_idx, ObBackupPathString &dir_path);
  int64_t get_dir_queue_count() const { return dir_queue_.count(); }
  int set_tablet_array(const common::ObIArray<common::ObTabletID> &tablet_array);
  int get_tablet_id(const int64_t task_id, common::ObTabletID &tablet_id);
  int add_archive_piece_lsn_range(const ObBackupArchivePieceLSNRange &lsn_range);
  int get_archive_piece_lsn_range(const int64_t group_id, ObBackupArchivePieceLSNRange* &lsn_range, bool &is_last_task);
  int set_validate_result(
      const int result,
      const char *error_msg,
      const ObBackupValidateDagNetInitParam &param,
      const common::ObAddr &src_server,
      const ObTaskId &dag_id,
      backup::ObBackupReportCtx &report_ctx);
  int set_next_task_id_and_read_bytes(const int64_t next_task_id, const int64_t read_bytes);
  int get_next_task_id(int64_t &task_id);
  int set_ls_info(const share::ObSingleLSInfoDesc &ls_info);
  const share::ObSingleLSInfoDesc* get_ls_info() const;
  bool has_validate_task() const {return next_task_id_ < total_task_count_;}
  int get_result() const;
  int64_t get_task_count() const { return total_task_count_; }
  int remove_running_task_id(
    const int64_t running_id,
    const int64_t read_bytes,
    bool &need_update_stat,
    int64_t &validate_checkpoint,
    int64_t &delta_read_bytes,
    int64_t &total_read_bytes);

  TO_STRING_KV(K_(running_groups), K_(result), K_(next_task_id), K_(total_task_count), K_(ls_info),
      K_(total_read_bytes), K_(delta_read_bytes), K_(last_reported_checkpoint));

private:
  int add_running_group_(const int64_t running_id);
private:
  common::ObSArray<common::ObTabletID> tablet_array_;
  common::ObSArray<ObBackupPathString> dir_queue_;
  common::ObSArray<ObBackupArchivePieceLSNRange> archive_piece_lsn_ranges_;
  share::ObSingleLSInfoDesc ls_info_;
  hash::ObHashSet<int64_t> running_groups_;
  int result_;
  mutable common::SpinRWLock ctx_lock_;
  int64_t next_task_id_;
  int64_t total_task_count_;
  int64_t total_read_bytes_;
  int64_t delta_read_bytes_;
  int64_t last_reported_checkpoint_;
  bool inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateTaskContext);
};

enum class ObBackupValidateDagType : int64_t
{
  BACKUP_VALIDATE_PREPARE_DAG = 0,
  BACKUP_VALIDATE_BASIC_DAG = 1,
  BACKUP_VALIDATE_BACKUP_SET_PHYSICAL_DAG = 2,
  BACKUP_VALIDATE_ARCHIVE_PIECE_PHYSICAL_DAG = 3,
  BACKUP_VALIDATE_FINISH_DAG = 4,
  MAX_TYPE,
};

class ObBackupValidateObUtils final
{
public:
  explicit ObBackupValidateObUtils() {}
  ~ObBackupValidateObUtils() {}
  static int report_validate_over(const obrpc::ObBackupTaskRes &res, backup::ObBackupReportCtx &report_ctx);
  static int init_meta_index_store(
      const ObExternBackupSetInfoDesc &backup_set_info, const ObBackupValidateDagNetInitParam &param,
      const bool is_sec_meta, ObBackupDataStore &store, backup::ObBackupMetaIndexStoreWrapper &meta_index_store);
  static int get_sec_meta_iterator(
      const backup::ObBackupSSTableMeta &sstable_meta,
      const ObITableReadInfo &read_info,
      const ObExternBackupSetInfoDesc &backup_set_info,
      backup::ObBackupMetaIndexStoreWrapper &meta_index_store,
      const share::ObBackupDest &backup_dest,
      const common::ObStorageIdMod &mod,
      backup::ObBackupSSTableSecMetaIterator &sec_meta_iterator);
  static int get_rowkey_read_info(
      const ObITable::TableKey &table_key,
      const ObMigrationTabletParam &tablet_param,
      ObArenaAllocator &allocator,
      const ObITableReadInfo *&rowkey_read_info);
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateObUtils);
};

}//storage
}//oceanbase

#endif // STORAGE_LOG_STREAM_BACKUP_VALIDATE_BASE_H_