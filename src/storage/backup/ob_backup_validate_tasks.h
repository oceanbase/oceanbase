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

#ifndef STORAGE_LOG_STREAM_BACKUP_VALIDATE_TASKS_H_
#define STORAGE_LOG_STREAM_BACKUP_VALIDATE_TASKS_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_path.h"
#include "storage/backup/ob_backup_validate_base.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_extern_info_mgr.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/backup/ob_backup_sstable_sec_meta_iterator.h"
#include "storage/backup/ob_backup_index_store.h"
#include "share/backup/ob_archive_struct.h"
#include "share/backup/ob_archive_store.h"
#include "logservice/restoreservice/ob_remote_log_source.h"
#include "storage/high_availability/ob_storage_restore_struct.h"
#include "storage/backup/ob_backup_block_file_reader_writer.h"

namespace oceanbase
{
namespace storage
{

class ObBackupValidateBaseDag;

//TODO(xingzhi): Adjust class position
class ObBackupValidateBasicTask: public share::ObITask
{
public:
  ObBackupValidateBasicTask();
  virtual ~ObBackupValidateBasicTask();
  int init(const ObBackupValidateDagNetInitParam &param, const int64_t file_group_id,
      const backup::ObBackupReportCtx &report_ctx, const share::ObBackupStorageInfo &storage_info);
  virtual int process() override;
  virtual int generate_next_task(share::ObITask *&next_task) override;
  bool is_valid() const { return is_inited_; }

private:
  int do_basic_validate_(const ObBackupPathString &dir_path);

private:
  bool is_inited_;
  ObBackupValidateDagNetInitParam param_;
  int64_t task_id_;
  ObBackupValidateTaskContext *ctx_;
  backup::ObBackupReportCtx report_ctx_;
  share::ObBackupStorageInfo storage_info_;
  char error_msg_[share::OB_MAX_VALIDATE_LOG_INFO_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateBasicTask);
};

class ObBackupValidateFinishTask: public share::ObITask
{
public:
  ObBackupValidateFinishTask();
  virtual ~ObBackupValidateFinishTask();
  int init(const ObBackupValidateDagNetInitParam &param, const backup::ObBackupReportCtx &report_ctx);
  virtual int process() override;
  bool is_valid() const { return is_inited_; }

private:
  int report_validate_succ_();

private:
  bool is_inited_;
  ObBackupValidateDagNetInitParam param_;
  ObBackupValidateTaskContext *ctx_;
  backup::ObBackupReportCtx report_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateFinishTask);
};

class ObBackupValidateTabletFinishTask: public share::ObITask
{
public:
  ObBackupValidateTabletFinishTask();
  virtual ~ObBackupValidateTabletFinishTask();
  int init(
      ObBackupValidateBaseDag &base_dag,
      const int64_t task_id);
  virtual int process() override;

public:
  int set_sstable_meta_array(const common::ObIArray<backup::ObBackupSSTableMeta> &sstable_meta_array);
  int get_sstable_meta(const int64_t index, backup::ObBackupSSTableMeta &sstable_meta);
  int64_t get_sstable_meta_count() const { return sstable_meta_array_.count(); }
  int add_read_bytes(const int64_t read_bytes);

private:
  bool is_inited_;
  ObBackupValidateTaskContext *ctx_;
  backup::ObBackupReportCtx report_ctx_;
  ObBackupValidateDagNetInitParam param_;
  ObTabletID tablet_id_;
  int64_t task_id_;
  int64_t read_bytes_;
  common::ObArray<backup::ObBackupSSTableMeta> sstable_meta_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateTabletFinishTask);
};

class ObBackupValidateBackupSetPhysicalTask: public share::ObITask
{
public:
  ObBackupValidateBackupSetPhysicalTask();
  virtual ~ObBackupValidateBackupSetPhysicalTask();
  int init(ObBackupValidateBaseDag &base_dag,
      const int64_t task_id,
      ObExternBackupSetInfoDesc &backup_set_info,
      backup::ObBackupMetaIndexStoreWrapper *meta_index_store);
  virtual int process() override;
  virtual int generate_next_task(share::ObITask *&next_task) override;

private:
  int generate_tablet_finish_task_(
      const int64_t task_id,
      ObBackupValidateTabletFinishTask *&tablet_finish_task);
  int get_backup_data_type_(ObBackupDataType &backup_data_type_array);
  int validate_sstables_(
      const backup::ObBackupTabletMeta &tablet_meta,
      const ObBackupDataType &backup_data_type,
      ObBackupValidateTabletFinishTask *tablet_finish_task);
  int get_sstable_meta_(
      const common::ObTabletID &tablet_id,
      const ObBackupDataType &backup_data_type,
      ObIArray<backup::ObBackupSSTableMeta> &sstable_meta_array);
  int get_tablet_meta_(
      const common::ObTabletID &tablet_id,
      const ObBackupDataType &backup_data_type,
      bool &is_deleted_tablet,
      backup::ObBackupTabletMeta &tablet_meta);
  int generate_sstable_validate_task_(
      const storage::ObMigrationTabletParam &tablet_param,
      ObBackupValidateTabletFinishTask *tablet_finish_task);
  int validate_filled_tx_scn_(const ObIArray<backup::ObBackupSSTableMeta> &sstable_meta_array);
  int get_backup_tx_data_table_filled_tx_scn_(share::SCN &filled_tx_scn);

protected:
  bool is_inited_;
  ObBackupValidateDagNetInitParam param_;
  backup::ObBackupReportCtx report_ctx_;
  share::ObBackupStorageInfo storage_info_;
  ObBackupValidateTaskContext *ctx_;
  int64_t task_id_;
  ObTabletID tablet_id_;
  int64_t validated_meta_files_;
  int64_t validated_macro_blocks_;
  int64_t total_meta_files_;
  int64_t total_macro_blocks_;
  ObExternBackupSetInfoDesc backup_set_info_;
  ObBackupDest backup_dest_;
  int64_t read_bytes_;
  ObBackupPathString error_msg_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateBackupSetPhysicalTask);
};

class ObBackupValidateArchivePiecePhysicalTask: public share::ObITask
{
public:
  ObBackupValidateArchivePiecePhysicalTask();
  virtual ~ObBackupValidateArchivePiecePhysicalTask();
  int init(const ObBackupValidateDagNetInitParam &param,
    const backup::ObBackupReportCtx &report_ctx,
    const share::ObBackupStorageInfo &storage_info,
    const int64_t lsn_group_id);
  virtual int process() override;
  virtual int generate_next_task(share::ObITask *&next_task) override;
  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(ctx));

private:
  int do_validate_ls_range_();

protected:
  bool is_inited_;
  bool is_last_task_;
  ObBackupValidateDagNetInitParam param_;
  backup::ObBackupReportCtx report_ctx_;
  share::ObBackupStorageInfo storage_info_;
  ObBackupValidateTaskContext *ctx_;
  ObBackupArchivePieceLSNRange *lsn_range_;
  int64_t validated_log_bytes_;
  class GetSourceFunctor
  {
  public:
    explicit GetSourceFunctor(logservice::ObRemoteRawPathParent &raw_path_parent);
    ~GetSourceFunctor();
    int operator()(const share::ObLSID &id, logservice::ObRemoteSourceGuard &guard);

  private:
    logservice::ObRemoteRawPathParent &raw_path_parent_;
  };
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateArchivePiecePhysicalTask);
};

class ObBackupValidatePrepareTask: public share::ObITask
{
public:
  ObBackupValidatePrepareTask();
  virtual ~ObBackupValidatePrepareTask();
  int init(const ObBackupValidateDagNetInitParam &param,
      const backup::ObBackupReportCtx &report_ctx,
      const share::ObBackupStorageInfo &storage_info,
      ObBackupValidateTaskContext *ctx);
  virtual int process() override;

private:
// basic validate methods
  int set_task_id_();
  int prepare_basic_validate_();
  int get_and_add_dir_list_(const common::ObArray<ObBackupPath> &path_list);
// physical validate methods
  int prepare_backupset_physical_validate_();
  int report_validate_succ_();
  int get_tablet_list_(common::ObIArray<common::ObTabletID> &all_tablets);
  int prepare_archive_piece_physical_validate_();
  // piece meta ls info collection (align with ob_admin prepare/meta tasks)
  int collect_and_check_piece_ls_info_(
      const share::ObArchiveStore &archive_store,
      const share::ObBackupDest &piece_root_dest,
      const share::ObSinglePieceDesc &single_piece_desc,
      share::ObSingleLSInfoDesc &ls_info);
  int get_active_piece_ls_info_(
      const share::ObBackupDest &piece_root_dest,
      const share::ObSinglePieceDesc &single_piece_desc,
      share::ObSingleLSInfoDesc &ls_info);
  int generate_basic_validate_dag_();
  int generate_backupset_physical_validate_dag_();
  int generate_archive_piece_physical_validate_dag_();

private:
  bool is_inited_;
  ObBackupValidateDagNetInitParam param_;
  ObBackupValidateTaskContext *ctx_;
  backup::ObBackupReportCtx report_ctx_;
  share::ObBackupStorageInfo storage_info_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidatePrepareTask);
};

class ObBackupValidateSSTableTask: public share::ObITask
{
public:
  ObBackupValidateSSTableTask();
  virtual ~ObBackupValidateSSTableTask();
  int init(ObBackupValidateBaseDag &base_dag,
      const int64_t sstable_index,
      ObExternBackupSetInfoDesc &backup_set_info,
      const storage::ObMigrationTabletParam &tablet_param,
      backup::ObBackupMetaIndexStoreWrapper *meta_index_store,
      ObBackupValidateTabletFinishTask *tablet_finish_task);
  virtual int generate_next_task(share::ObITask *&next_task) override;
  virtual int process() override;

private:
  int generate_validate_macro_block_task_();
  int validate_sstable_checksum_();

private:
  bool is_inited_;
  ObBackupValidateDagNetInitParam param_;
  backup::ObBackupReportCtx report_ctx_;
  ObBackupValidateTaskContext *ctx_;
  ObBackupDest backup_dest_;
  ObExternBackupSetInfoDesc backup_set_info_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;
  backup::ObBackupSSTableMeta sstable_meta_;
  int64_t sstable_index_;
  storage::ObMigrationTabletParam tablet_param_;
  ObBackupValidateTabletFinishTask *tablet_finish_task_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateSSTableTask);
};

class ObBackupValidateMacroBlockFinishTask: public share::ObITask
{
public:
  ObBackupValidateMacroBlockFinishTask();
  virtual ~ObBackupValidateMacroBlockFinishTask() {}
  int init(ObBackupValidateBaseDag &base_dag,
      const backup::ObBackupSSTableMeta &sstable_meta,
      const storage::ObMigrationTabletParam &tablet_param,
      const ObExternBackupSetInfoDesc &backup_set_info,
      backup::ObBackupMetaIndexStoreWrapper &meta_index_store,
      ObBackupValidateTabletFinishTask *tablet_finish_task);
  virtual int process() override;
  int get_next_bacth_macro_index_array(ObIArray<backup::ObBackupMacroBlockIndex> &next_macro_index_array);
private:
  bool is_inited_;
  mutable common::ObSpinLock lock_;
  ObBackupValidateTaskContext *ctx_;
  int64_t next_macro_index;
  ObArenaAllocator allocator_;
  backup::ObBackupReportCtx report_ctx_;
  ObBackupValidateDagNetInitParam param_;
  backup::ObBackupSSTableMeta sstable_meta_;
  backup::ObBackupSSTableSecMetaIterator sec_meta_iterator_;
  ObBackupValidateTabletFinishTask *tablet_finish_task_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateMacroBlockFinishTask);
};

class ObBackupValidateMacroBlockTask: public share::ObITask
{
public:
  ObBackupValidateMacroBlockTask();
  virtual ~ObBackupValidateMacroBlockTask();
  int init(ObBackupValidateBaseDag &base_dag,
      const backup::ObBackupSSTableMeta &sstable_meta,
      const ObExternBackupSetInfoDesc &backup_set_info,
      const ObIArray<backup::ObBackupMacroBlockIndex> &macro_index_array,
      ObBackupValidateMacroBlockFinishTask *macro_block_finish_task,
      ObBackupValidateTabletFinishTask *tablet_finish_task);
  virtual int process() override;
  virtual int generate_next_task(share::ObITask *&next_task) override;

public:
  static int get_next_batch_macro_indexs(
      ObRestoreMacroBlockIdMgr &macro_block_id_mgr,
      common::ObArray<blocksstable::ObDataMacroBlockMeta> &macro_metas);

private:
  int read_macro_block_(
      const backup::ObBackupMacroBlockIndex &macro_index,
      const share::ObBackupDataType &data_type,
      blocksstable::ObBufferReader &data_buffer,
      blocksstable::ObBufferReader &read_buffer);
  int alloc_macro_block_data_buffer_(
      common::ObArenaAllocator &allocator,
      blocksstable::ObBufferReader &read_buffer,
      blocksstable::ObBufferReader &data_buffer);

private:
  bool is_inited_;
  ObBackupValidateTaskContext *ctx_;
  ObBackupDest backup_dest_;
  backup::ObBackupReportCtx report_ctx_;
  ObBackupValidateDagNetInitParam param_;
  backup::ObBackupSSTableMeta sstable_meta_;
  ObExternBackupSetInfoDesc backup_set_info_;
  common::ObSArray<backup::ObBackupMacroBlockIndex> macro_index_array_;
  ObBackupValidateMacroBlockFinishTask *macro_block_finish_task_;
  ObBackupValidateTabletFinishTask *tablet_finish_task_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateMacroBlockTask);
};

}//storage
}//oceanbase

#endif // STORAGE_LOG_STREAM_BACKUP_VALIDATE_TASKS_H_