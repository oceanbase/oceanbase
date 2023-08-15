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

#ifndef OCEANBASE_SHARE_OB_BACKUP_CEALN_STRUCT_H_
#define OCEANBASE_SHARE_OB_BACKUP_CEALN_STRUCT_H_

#include "share/backup/ob_backup_struct.h"
namespace oceanbase
{
namespace share
{

struct ObBackupCleanStatus
{
  enum Status 
  {
    INIT = 0,
    DOING = 1,
    COMPLETED = 2,
    FAILED = 3,
    CANCELING= 4,
    CANCELED = 5,
    MAX_STATUS
  };
  ObBackupCleanStatus(): status_(MAX_STATUS) {}
  virtual ~ObBackupCleanStatus() = default;
  bool is_valid() const;
  const char* get_str() const;
  int set_status(const char *str);
  TO_STRING_KV(K_(status));
  Status status_; 
};

struct ObNewBackupCleanType final
{
  enum TYPE
  {
    EMPTY_TYPE = 0,
    DELETE_BACKUP_SET = 1,
    DELETE_BACKUP_PIECE = 2,
    DELETE_BACKUP_ALL = 3,
    DELETE_OBSOLETE_BACKUP = 4,
    DELETE_OBSOLETE_BACKUP_BACKUP = 5,
    CANCEL_DELETE = 6,
    MAX,
  };
  static const char *get_str(const TYPE &type);
  static TYPE get_type(const char *type_str);
  static OB_INLINE bool is_valid(const TYPE &type) { return type >= 0 && type < MAX; }
};

enum ObPolicyOperatorType
{
  ADD_POLICY = 0,
  DROP_POLICY = 1,
  CHANGE_POLICY = 2,
  MAX,
};

struct ObBackupCleanTaskType final
{
  enum TYPE
  {
    BACKUP_SET = 0,
    BACKUP_PIECE = 1,
    MAX,
  };
  static const char *get_str(const TYPE &type);
  static TYPE get_type(const char *type_str);
  static OB_INLINE bool is_valid(const TYPE &type) { return type >= 0 && type < MAX; }
};

class ObBackupCleanStats final
{
public:
  ObBackupCleanStats() :
      total_bytes_(0),
      delete_bytes_(0),
      total_files_count_(0),
      delete_files_count_(0) 
  {
  }
  ~ObBackupCleanStats() {}
  void reset();
  TO_STRING_KV(K_(total_bytes), K_(delete_bytes), K_(total_files_count), K_(delete_files_count));
  int64_t total_bytes_;
  int64_t delete_bytes_;
  int64_t total_files_count_;
  int64_t delete_files_count_; 
};

struct ObBackupCleanJobAttr final
{
public:
  ObBackupCleanJobAttr();
  ~ObBackupCleanJobAttr() {}
  void reset();
  bool  is_tmplate_valid() const;
  bool is_valid() const;
  int assign(const ObBackupCleanJobAttr &other);
  bool is_clean_copy() const { return dest_id_ > 0; }
  bool is_delete_obsolete_backup() const { return ObNewBackupCleanType::DELETE_OBSOLETE_BACKUP == clean_type_ || ObNewBackupCleanType::DELETE_OBSOLETE_BACKUP_BACKUP == clean_type_; }
  bool is_delete_backup_set() const { return ObNewBackupCleanType::DELETE_BACKUP_SET == clean_type_; }
  bool is_delete_backup_piece() const { return ObNewBackupCleanType::DELETE_BACKUP_PIECE == clean_type_; }
  int get_clean_parameter(int64_t &parameter) const;
  int set_clean_parameter(const int64_t parameter);
  int check_backup_clean_job_match(const ObBackupCleanJobAttr &job_attr) const;
  int set_dest_id(const int64_t dest_id);
  int get_executor_tenant_id_str(share::ObDMLSqlSplicer &dml) const;
  int set_executor_tenant_id(const ObString &str);

  TO_STRING_KV(K_(job_id), K_(tenant_id), K_(incarnation_id), K_(initiator_job_id), K_(executor_tenant_id), K_(initiator_tenant_id),  K_(clean_type),
      K_(expired_time), K_(backup_set_id), K_(backup_piece_id), K_(dest_id), K_(job_level), 
      K_(backup_path), K_(start_ts), K_(end_ts), K_(status), K_(description), K_(result), K_(retry_count), K_(can_retry), K_(task_count),
      K_(success_task_count)); 

  int64_t job_id_; // pk
  uint64_t tenant_id_; // pk
  int64_t incarnation_id_;
  uint64_t initiator_tenant_id_;
  int64_t initiator_job_id_;
  common::ObSArray<uint64_t> executor_tenant_id_;
  ObNewBackupCleanType::TYPE clean_type_;
  int64_t expired_time_;
  int64_t backup_set_id_;
  int64_t backup_piece_id_;
  int64_t dest_id_;
  ObBackupLevel job_level_;
  ObBackupPathString backup_path_;
  int64_t start_ts_;
  int64_t end_ts_;
  ObBackupCleanStatus status_;
  share::ObBackupDescription description_;
  int result_;
  int64_t retry_count_;
  bool can_retry_;
  int64_t task_count_;
  int64_t success_task_count_;
};
  // TODO(wenjinyu.wjy) 4.3 Split the structure of set and piece

struct ObBackupCleanTaskAttr final
{
public:
  ObBackupCleanTaskAttr();
  ~ObBackupCleanTaskAttr() {}
  bool is_valid() const;
  void reset(); 
  int assign(const ObBackupCleanTaskAttr &other);
  bool is_delete_backup_set_task() const { return ObBackupCleanTaskType::BACKUP_SET == task_type_; }
  bool is_delete_backup_piece_task() const { return ObBackupCleanTaskType::BACKUP_PIECE == task_type_; }
  bool is_succeed_task() const { return result_ == OB_SUCCESS ? true : false; }
  int get_backup_clean_id(int64_t &id) const;
  int set_backup_clean_id(const int64_t id);
  TO_STRING_KV(K_(task_id), K_(tenant_id), K_(task_type), K_(job_id),  K_(backup_set_id),
      K_(backup_piece_id), K_(round_id), K_(dest_id), K_(start_ts), K_(end_ts),
      K_(backup_path),  K_(status), K_(result), K_(total_ls_count),
      K_(finish_ls_count), K_(stats)); 
  int64_t task_id_;
  uint64_t tenant_id_;
  int64_t incarnation_id_;
  ObBackupCleanTaskType::TYPE task_type_;
  int64_t job_id_;
  int64_t backup_set_id_;
  int64_t backup_piece_id_;
  int64_t round_id_;
  int64_t dest_id_;
  int64_t start_ts_;
  int64_t end_ts_;
  ObBackupCleanStatus status_;
  ObBackupPathString backup_path_;
  int result_;
  int64_t total_ls_count_;
  int64_t finish_ls_count_;
  ObBackupCleanStats stats_;
};

struct ObBackupCleanLSTaskAttr final
{
  ObBackupCleanLSTaskAttr();
  ~ObBackupCleanLSTaskAttr() {}
  bool is_valid() const;
  void reset(); 
  bool is_delete_backup_set_task() const { return ObBackupCleanTaskType::BACKUP_SET == task_type_; }
  bool is_delete_backup_piece_task() const { return ObBackupCleanTaskType::BACKUP_PIECE == task_type_; }
  int assign(const ObBackupCleanLSTaskAttr &other);
  TO_STRING_KV(K_(task_id), K_(tenant_id), K_(ls_id), K_(job_id),  K_(backup_set_id),
      K_(backup_piece_id), K_(round_id), K_(task_type), K_(status), K_(start_ts), K_(end_ts),
      K_(dst), K_(result), K_(stats)); 
  int64_t task_id_;
  uint64_t tenant_id_;
  ObLSID ls_id_;
  int64_t job_id_;
  int64_t backup_set_id_;
  int64_t backup_piece_id_;
  int64_t round_id_;
  ObBackupCleanTaskType::TYPE task_type_;
  ObTaskId task_trace_id_; 
  ObBackupTaskStatus status_;
  int64_t start_ts_;
  int64_t end_ts_;
  ObAddr dst_;
  int result_;
  int64_t retry_id_;
  ObBackupCleanStats stats_;
};

struct ObDeletePolicyAttr final
{
  ObDeletePolicyAttr();
  ~ObDeletePolicyAttr() {}
  bool is_valid() const;
  void reset(); 
  int assign(const ObDeletePolicyAttr &other);
  TO_STRING_KV(K_(tenant_id), K_(policy_name), K_(recovery_window),  K_(redundancy), K_(backup_copies)); 

  uint64_t tenant_id_;
  char policy_name_[OB_BACKUP_DELETE_POLICY_NAME_LENGTH];
  char recovery_window_[OB_BACKUP_RECOVERY_WINDOW_LENGTH];
  int64_t redundancy_;
  int64_t backup_copies_;
};

}//share
}//oceanbase

#endif /* OCEANBASE_SHARE_OB_BACKUP_CEALN_STRUCT_H_ */