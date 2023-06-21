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

#ifndef STORAGE_LOG_STREAM_BACKUP_CLEAN_H_
#define STORAGE_LOG_STREAM_BACKUP_CLEAN_H_
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "share/backup/ob_backup_clean_struct.h"
#include "share/backup/ob_archive_struct.h"

namespace oceanbase
{
namespace storage
{

class ObLSBackupCleanScheduler final
{
public:
  ObLSBackupCleanScheduler() {}
  ~ObLSBackupCleanScheduler() {}
  static int schedule_backup_clean_dag(const obrpc::ObLSBackupCleanArg &args);
};

struct ObLSBackupCleanDagNetInitParam : public share::ObIDagInitParam
{
public:
  ObLSBackupCleanDagNetInitParam();
  virtual ~ObLSBackupCleanDagNetInitParam() {}
  bool is_valid() const override;
  int set(const obrpc::ObLSBackupCleanArg &args);
  bool operator == (const ObLSBackupCleanDagNetInitParam &other) const;
  bool operator != (const ObLSBackupCleanDagNetInitParam &other) const;
  int64_t hash() const;
  VIRTUAL_TO_STRING_KV(K_(ls_id), K_(trace_id), K_(tenant_id), K_(incarnation),
      K_(task_id), K_(task_type), K_(id), K_(dest_id), K_(round_id));

public:
  share::ObTaskId trace_id_;
  int64_t job_id_;
  uint64_t tenant_id_;
  int64_t incarnation_;
  uint64_t task_id_;
  share::ObLSID ls_id_;
  share::ObBackupCleanTaskType::TYPE task_type_;
  uint64_t id_;
  int64_t dest_id_;
  int64_t round_id_;
};

class ObLSBackupCleanDagNet : public share::ObIDagNet
{
public:
  ObLSBackupCleanDagNet();
  virtual ~ObLSBackupCleanDagNet();
  int init_by_param(const share::ObIDagInitParam *param) override;
  int start_running() override;
  bool operator == (const share::ObIDagNet &other) const override;
  int64_t hash() const override;
  int fill_comment(char *buf, const int64_t buf_len) const override;
  int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  bool is_valid() const override { return param_.is_valid(); }
  bool is_ha_dag_net() const override { return true; }
  ObLSBackupCleanDagNetInitParam &get_param() { return param_;} 
  INHERIT_TO_STRING_KV("share::ObIDagNet", share::ObIDagNet, K_(param));
private:

protected:
  bool is_inited_;
  ObLSBackupCleanDagNetInitParam param_;
  common::ObMySQLProxy *sql_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupCleanDagNet);
};

enum class ObLSBackupCleanDagType : int64_t
{
  LS_BACKUP_CLEAN_DAG = 0,
  MAX_TYPE,
};

class ObLSBackupCleanDag: public share::ObIDag
{
public:
  ObLSBackupCleanDag();
  virtual ~ObLSBackupCleanDag();
  virtual bool operator == (const ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int init(share::ObIDagNet *dag_net);
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return true; }
  int create_first_task();

  INHERIT_TO_STRING_KV("ObIDag", ObIDag, KP(this), K_(param), K_(result));
private:
  int report_result(); 

private:
  bool is_inited_;
  ObLSBackupCleanDagNetInitParam param_;
  int32_t result_;
  common::ObMySQLProxy *sql_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupCleanDag);
};

class ObLSBackupCleanTask: public share::ObITask
{
public:
  ObLSBackupCleanTask();
  virtual ~ObLSBackupCleanTask();
  int init(const ObLSBackupCleanDagNetInitParam &param, common::ObMySQLProxy &sql_proxy);
  virtual int process() override;
protected:
  int start_handle_ls_task_();
  int post_rpc_result_(const int64_t result);
  int check_can_do_task_(bool &can);
  int do_ls_task();
  int delete_backup_piece_ls_files_(const share::ObBackupPath &path);
  int delete_backup_set_ls_files_(const share::ObBackupPath &path); 
  int get_set_ls_path_(share::ObBackupPath &path);
  int get_piece_ls_path(share::ObBackupPath &path);
  int delete_piece_ls_meta_files_(const share::ObBackupPath &path);
  int delete_piece_log_files_(const share::ObBackupPath &path);
  int delete_complement_log_(const share::ObBackupPath &path);
  int delete_sys_data_(const share::ObBackupPath &path);
  int delete_major_data_(const share::ObBackupPath &path);
  int delete_minor_data_(const share::ObBackupPath &path);
  int delete_meta_info_(const share::ObBackupPath &path);
  int delete_log_stream_dir_(const share::ObBackupPath &ls_path);
protected:
  bool is_inited_;
  common::ObMySQLProxy *sql_proxy_;
  share::ObBackupSetFileDesc backup_set_desc_;
  share::ObTenantArchivePieceAttr backup_piece_info_;
  share::ObBackupDest backup_dest_;
  uint64_t task_id_;
  int64_t job_id_;
  uint64_t tenant_id_;
  int64_t incarnation_;
  int64_t backup_set_id_;
  int64_t backup_piece_id_;
  int64_t round_id_;
  int64_t dest_id_;
  share::ObLSID ls_id_;
  share::ObBackupCleanTaskType::TYPE task_type_;
  share::ObTaskId trace_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupCleanTask);
};



}
}
#endif
