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

#ifndef STORAGE_LOG_STREAM_BACKUP_VALIDATE_SCHEDULER_H_
#define STORAGE_LOG_STREAM_BACKUP_VALIDATE_SCHEDULER_H_

#include "storage/backup/ob_backup_validate_base.h"
#include "storage/backup/ob_backup_validate_tasks.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/backup/ob_backup_validate_struct.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_index_store.h"

namespace oceanbase
{
namespace storage
{
class ObLSBackupValidateScheduler final
{
public:
  ObLSBackupValidateScheduler() {}
  ~ObLSBackupValidateScheduler() {}
  static int schedule_backup_validate_dag(const obrpc::ObBackupValidateLSArg &args);
};

class ObBackupValidateBaseDag : public share::ObIDag
{
public:
  ObBackupValidateBaseDag();
  virtual ~ObBackupValidateBaseDag();

  virtual bool operator == (const ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int init(share::ObIDagNet *dag_net);
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return true; }

  virtual int create_first_task() = 0;
  bool is_inited() const { return is_inited_; }

public:
  virtual ObBackupValidateDagType get_dag_type() const { return ObBackupValidateDagType::MAX_TYPE; }
  ObBackupValidateTaskContext *get_task_context() { return ctx_; }
  const ObBackupValidateDagNetInitParam &get_param() const { return param_; }
  const share::ObBackupStorageInfo &get_storage_info() const { return storage_info_; }
  const backup::ObBackupReportCtx &get_report_ctx() const { return report_ctx_; }
  const ObBackupDest &get_backup_dest() const { return backup_dest_; }
  ObArenaAllocator *get_allocator() { return &allocator_; }

protected:
  bool is_inited_;
  ObBackupValidateDagNetInitParam param_;
  share::ObBackupStorageInfo storage_info_;
  ObBackupDest backup_dest_;
  backup::ObBackupReportCtx report_ctx_;
  ObBackupValidateTaskContext *ctx_;
  ObArenaAllocator allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateBaseDag);
};

class ObBackupValidatePrepareDag: public ObBackupValidateBaseDag
{
public:
  ObBackupValidatePrepareDag();
  virtual ~ObBackupValidatePrepareDag();

  virtual int create_first_task() override;
  virtual ObBackupValidateDagType get_dag_type() const override
  { return ObBackupValidateDagType::BACKUP_VALIDATE_PREPARE_DAG; }

  INHERIT_TO_STRING_KV("ObBackupValidateBaseDag", ObBackupValidateBaseDag, KP(this), K_(param));

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidatePrepareDag);
};

class ObBackupValidateBasicDag: public ObBackupValidateBaseDag
{
public:
  ObBackupValidateBasicDag();
  virtual ~ObBackupValidateBasicDag();

  virtual int create_first_task() override;
  virtual ObBackupValidateDagType get_dag_type() const override
  { return ObBackupValidateDagType::BACKUP_VALIDATE_BASIC_DAG; }

  INHERIT_TO_STRING_KV("ObBackupValidateBaseDag", ObBackupValidateBaseDag, KP(this), K_(param));

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateBasicDag);
};

class ObBackupValidateBackupSetPhysicalDag: public ObBackupValidateBaseDag
{
public:
  ObBackupValidateBackupSetPhysicalDag();
  virtual ~ObBackupValidateBackupSetPhysicalDag();

  virtual int create_first_task() override;
  virtual ObBackupValidateDagType get_dag_type() const override
  { return ObBackupValidateDagType::BACKUP_VALIDATE_BACKUP_SET_PHYSICAL_DAG; }

  INHERIT_TO_STRING_KV("ObBackupValidateBaseDag", ObBackupValidateBaseDag, KP(this), K_(param), K_(succeed_set_dest_info));
private:
  int get_dependency_backup_sets_(
      const share::ObBackupDest &current_set_dest,
      const share::ObBackupSetDesc &current_set_desc,
      common::ObIArray<share::ObBackupSetBriefInfo> &backup_set_list);
private:
  backup::ObBackupMetaIndexStoreWrapper meta_index_store_;
  bool succeed_set_dest_info_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateBackupSetPhysicalDag);
};

class ObBackupValidateArchivePiecePhysicalDag: public ObBackupValidateBaseDag
{
public:
  ObBackupValidateArchivePiecePhysicalDag();
  virtual ~ObBackupValidateArchivePiecePhysicalDag();

  virtual int create_first_task() override;
  virtual ObBackupValidateDagType get_dag_type() const override
  { return ObBackupValidateDagType::BACKUP_VALIDATE_ARCHIVE_PIECE_PHYSICAL_DAG; }

  INHERIT_TO_STRING_KV("ObBackupValidateBaseDag", ObBackupValidateBaseDag, KP(this), K_(param));

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateArchivePiecePhysicalDag);
};

class ObBackupValidateFinishDag: public ObBackupValidateBaseDag
{
public:
  ObBackupValidateFinishDag();
  virtual ~ObBackupValidateFinishDag();

  virtual int create_first_task() override;
  virtual ObBackupValidateDagType get_dag_type() const override
  { return ObBackupValidateDagType::BACKUP_VALIDATE_FINISH_DAG; }

  INHERIT_TO_STRING_KV("ObBackupValidateBaseDag", ObBackupValidateBaseDag, KP(this), K_(param));

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateFinishDag);
};

class ObBackupValidateDagNet : public share::ObIDagNet
{
public:
  ObBackupValidateDagNet();
  virtual ~ObBackupValidateDagNet();
  int init_by_param(const share::ObIDagInitParam *param) override;
  int start_running() override;
  bool operator == (const share::ObIDagNet &other) const override;
  uint64_t hash() const override;
  int fill_comment(char *buf, const int64_t buf_len) const override;
  int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  bool is_valid() const override { return param_.is_valid(); }
  ObBackupValidateDagNetInitParam &get_param() { return param_;}
  ObBackupValidateTaskContext &get_task_context() { return task_context_; }
  const backup::ObBackupReportCtx &get_report_ctx() const { return report_ctx_; }
  const share::ObBackupStorageInfo &get_storage_info() const { return storage_info_; }

  INHERIT_TO_STRING_KV("share::ObIDagNet", share::ObIDagNet, K_(param), K_(storage_info));

private:
  int schedule_prepare_validate_();

protected:
  bool is_inited_;
  ObBackupValidateDagNetInitParam param_;
  backup::ObBackupReportCtx report_ctx_;
  share::ObBackupStorageInfo storage_info_;
  ObBackupValidateTaskContext task_context_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateDagNet);
};

}//storage
}//oceanbase

#endif // STORAGE_LOG_STREAM_BACKUP_VALIDATE_SCHEDULER_H_