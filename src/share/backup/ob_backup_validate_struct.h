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

 #ifndef OCEANBASE_BACKUP_OB_BACKUP_VALIDATE_STRUCT_H_
#define OCEANBASE_BACKUP_OB_BACKUP_VALIDATE_STRUCT_H_

#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace share
{
struct ObBackupValidateStatus
{
  enum Status
  {
    INIT = 0,
    DOING = 1,
    COMPLETED = 2,
    FAILED = 3,
    CANCELING= 4,
    CANCELED = 5,
    BASIC = 6,
    PHYSICAL = 7,
    MAX_STATUS
  };
  ObBackupValidateStatus(): status_(MAX_STATUS) {}
  ObBackupValidateStatus(const Status &status): status_(status) {}
  virtual ~ObBackupValidateStatus() = default;
  ObBackupValidateStatus &operator=(const Status &status);
  bool is_valid() const;
  void reset() { status_ = MAX_STATUS; }
  bool is_validate_finish() const {return CANCELED == status_ || FAILED == status_ || COMPLETED == status_; }
  const char *get_str() const;
  int set_staust(const char *str);
  TO_STRING_KV(K_(status));
  Status status_;
};

struct ObBackupValidateType final
{
  OB_UNIS_VERSION(1);
public:
  enum ValidateType
  {
    DATABASE = 0,
    BACKUPSET = 1,
    ARCHIVELOG_PIECE = 2,
    MAX_VALIDATION_TYPE
  };
public:
  ObBackupValidateType() : type_(MAX_VALIDATION_TYPE) {}
  virtual ~ObBackupValidateType() = default;
  explicit ObBackupValidateType(const ValidateType &type) : type_(type) {}
  int set(const uint64_t type_value);
  int set(const char *type_str);
  const char *get_str() const;
  bool need_validate_backup_set() const;
  bool need_validate_archive_piece() const;
  bool is_backupset() const {return BACKUPSET == type_;}
  bool is_archivelog() const {return ARCHIVELOG_PIECE == type_;}
  bool is_valid() const {return DATABASE <= type_ && type_ < MAX_VALIDATION_TYPE;}
  void reset() { type_ = MAX_VALIDATION_TYPE; }
  ObBackupValidateType &operator=(const ValidateType &type) { type_ = type; return *this; }
  TO_STRING_KV(K_(type));
public:
  ValidateType type_;
};

struct ObBackupValidateLevel final
{
  OB_UNIS_VERSION(1);
public:
  enum ValidateLevel
  {
    BASIC = 0,
    PHYSICAL = 1,
    MAX_LEVEL
  };
public:
  ObBackupValidateLevel() : level_(MAX_LEVEL) {}
  virtual ~ObBackupValidateLevel() = default;
  int set(const char *level_str);
  const char *get_str() const;
  explicit ObBackupValidateLevel(const ValidateLevel &level) : level_(level) {}
  bool is_valid() const {return ValidateLevel::BASIC <= level_ && level_ < ValidateLevel::MAX_LEVEL;}
  ObBackupValidateLevel &operator=(const ValidateLevel &level) { level_ = level; return *this; }
  bool is_basic() const {return BASIC == level_;}
  bool is_physical() const {return PHYSICAL == level_;}
  void reset() { level_ = MAX_LEVEL; }
  TO_STRING_KV(K_(level));
public:
  ValidateLevel level_;
};

struct ObBackupValidatePathType final
{
  OB_UNIS_VERSION(1);
public:
  enum ValidatePathType
  {
    BACKUP_DEST = 0,
    BACKUP_SET_DEST = 1,
    ARCHIVELOG_DEST = 2,
    ARCHIVELOG_PIECE_DEST = 3,
    MAX_PATH_TYPE
  };
public:
  ObBackupValidatePathType() : type_(MAX_PATH_TYPE) {}
  virtual ~ObBackupValidatePathType() = default;
  int set(const char *type_str);
  const char *get_str() const;
  explicit ObBackupValidatePathType(const ValidatePathType &type) : type_(type) {}
  bool is_valid() const {return ValidatePathType::BACKUP_DEST <= type_ && type_ < ValidatePathType::MAX_PATH_TYPE;}
  ObBackupValidatePathType &operator=(const ValidatePathType &type) { type_ = type; return *this; }
  void reset() { type_ = MAX_PATH_TYPE; }
  bool is_backup_validate_type() const;
  bool is_archivelog_validate_type() const;
  bool is_dest_level_path() const;
  TO_STRING_KV(K_(type));
public:
  ValidatePathType type_;
};

class ObBackupValidateStats final
{
public:
  ObBackupValidateStats() :
      validated_bytes_(0),
      total_object_count_(0),
      finish_object_count_(0)
  {
  }
  ~ObBackupValidateStats() {}
  void reset();
  TO_STRING_KV(K_(validated_bytes), K_(total_object_count), K_(finish_object_count));
  int64_t validated_bytes_;
  int64_t total_object_count_;
  int64_t finish_object_count_;
};

struct ObBackupValidateJobAttr final
{
public:
  ObBackupValidateJobAttr();
  ~ObBackupValidateJobAttr() {}
  void reset();
  bool is_tmplate_valid() const;
  bool is_valid() const;
  int assign(const ObBackupValidateJobAttr &other);
  int set_executor_tenant_id(const char *executor_tenant_id_str);
  int set_validate_ids(const char *set_validate_ids);
  int get_executor_tenant_id_str(share::ObDMLSqlSplicer &dml) const;
  int get_validate_ids_str(share::ObDMLSqlSplicer &dml) const;
  int set_path_type(const ObBackupValidatePathType &path_type);
  bool need_set_backup_dest() const;
  bool need_set_archive_dest() const;
  TO_STRING_KV(K_(job_id), K_(tenant_id), K_(incarnation_id), K_(initiator_tenant_id), K_(initiator_job_id),
      K_(executor_tenant_ids), K_(type), K_(validate_path), K_(path_type), K_(level),
      K_(backup_set_ids), K_(logarchive_piece_ids), K_(start_ts), K_(end_ts), K_(status), K_(result), K_(can_retry),
      K_(retry_count), K_(task_count), K_(success_task_count), K_(comment), K_(description));
  int64_t job_id_;
  uint64_t tenant_id_;
  int64_t incarnation_id_;
  uint64_t initiator_tenant_id_;
  int64_t initiator_job_id_;
  common::ObSArray<uint64_t> executor_tenant_ids_;
  ObBackupValidateType type_;
  ObBackupPathString validate_path_;
  ObBackupValidatePathType path_type_;
  ObBackupValidateLevel level_;
  common::ObSArray<uint64_t> backup_set_ids_;
  common::ObSArray<uint64_t> logarchive_piece_ids_;
  int64_t start_ts_;
  int64_t end_ts_;
  ObBackupValidateStatus status_;
  int result_;
  bool can_retry_;
  int64_t retry_count_;
  int64_t task_count_;
  int64_t success_task_count_;
  ObHAResultInfo::Comment comment_;
  ObBackupDescription description_;
};

struct ObBackupValidateTaskAttr final
{
public:
  ObBackupValidateTaskAttr();
  ~ObBackupValidateTaskAttr() {}
  bool is_valid() const;
  int assign(const ObBackupValidateTaskAttr &other);
  void reset();
  const char *get_plus_archivelog_str() const;
  TO_STRING_KV(K_(task_id), K_(tenant_id), K_(incarnation_id), K_(job_id), K_(type), K_(validate_path), K_(path_type),
      K_(plus_archivelog), K_(initiator_task_id), K_(validate_id), K_(validate_level), K_(round_id), K_(start_ts),
      K_(end_ts), K_(status), K_(result), K_(can_retry), K_(retry_count), K_(total_ls_count), K_(finish_ls_count),
      K_(total_bytes), K_(validate_bytes), K_(dest_id), K_(comment));
  int64_t task_id_;
  uint64_t tenant_id_;
  int64_t incarnation_id_;
  int64_t job_id_;
  ObBackupValidateType type_;
  ObBackupPathString validate_path_;
  ObBackupValidatePathType path_type_;
  bool plus_archivelog_;
  int64_t initiator_task_id_;
  int64_t validate_id_;
  ObBackupValidateLevel validate_level_;
  int64_t round_id_;
  int64_t start_ts_;
  int64_t end_ts_;
  ObBackupValidateStatus status_;
  int result_;
  bool can_retry_;
  int64_t retry_count_;
  int64_t total_ls_count_;
  int64_t finish_ls_count_;
  int64_t total_bytes_;
  int64_t validate_bytes_;
  int64_t dest_id_;
  ObHAResultInfo::Comment comment_;
};

struct ObBackupValidateLSTaskAttr final
{
public:
  ObBackupValidateLSTaskAttr();
  ~ObBackupValidateLSTaskAttr() {}
  bool is_valid() const;
  void reset();
  int assign(const ObBackupValidateLSTaskAttr &other);
  bool is_validate_backup_set_task() const { return ObBackupValidateType::BACKUPSET == task_type_.type_; }
  bool is_validate_archivelog_piece_task() const { return ObBackupValidateType::ARCHIVELOG_PIECE == task_type_.type_; }
  bool is_succeed_task() const { return result_ == OB_SUCCESS ? true : false; }
  int get_validate_id(int64_t &id) const;
  int set_validate_id(const int64_t id);
  TO_STRING_KV(K_(task_id), K_(tenant_id), K_(ls_id), K_(job_id), K_(validate_id),
      K_(round_id), K_(task_type), K_(status), K_(start_ts), K_(end_ts),
      K_(validate_path), K_(result), K_(total_object_count), K_(finish_object_count), K_(stats));
  int64_t task_id_;
  uint64_t tenant_id_;
  ObLSID ls_id_;
  int64_t job_id_;
  int64_t validate_id_;
  int64_t round_id_;
  ObBackupValidateType task_type_;
  ObTaskId task_trace_id_;
  ObBackupTaskStatus status_;
  int64_t start_ts_;
  int64_t end_ts_;
  ObAddr dst_;
  int result_;
  int64_t retry_id_;
  ObBackupValidateStats stats_;
  ObBackupPathString validate_path_;
  int64_t dest_id_;
  ObBackupValidateLevel validate_level_;
  int64_t total_object_count_;
  int64_t finish_object_count_;
  int64_t validated_bytes_;
  ObHAResultInfo::Comment comment_;
};


}//end namespace share
}//end namespace oceanbase

#endif