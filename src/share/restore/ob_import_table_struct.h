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

#ifndef OCEANBASE_SHARE_IMPORT_TABLE_STRUCT_H
#define OCEANBASE_SHARE_IMPORT_TABLE_STRUCT_H

#include "lib/ob_define.h"
#include "share/backup/ob_backup_struct.h"
#include "share/ob_inner_table_operator.h"
#include "share/restore/ob_import_arg.h"

namespace oceanbase
{
namespace share
{
class ObImportResult final
{
public:
  using Comment = common::ObFixedLengthString<OB_COMMENT_LENGTH>;
  ObImportResult(): is_succeed_(true), comment_() {}
  ~ObImportResult() {}
  void set_result(const bool is_succeed, const Comment &comment = "");
  int set_result(const bool is_succeed, const char *buf);
  int set_result(const int err_code, const share::ObTaskId &trace_id, const ObAddr &addr, const ObString &extra_info = ObString());
  void reset();
  const char *get_result_str() const { return is_succeed_ ? "SUCCESS" : "FAILED"; }
  const char *get_comment() const { return comment_.ptr(); }
  const ObString get_comment_str() const { return comment_.str(); }
  bool is_succeed() const { return is_succeed_; }
  bool is_comment_setted() const { return !comment_.is_empty(); }
  ObImportResult &operator=(const ObImportResult &result);
  TO_STRING_KV(K_(is_succeed), K_(comment));
private:
  bool is_succeed_;
  Comment comment_;
};

#ifdef Property_declare_int
#undef Property_declare_int
#endif

#define Property_declare_int(variable_type, variable_name)\
private:\
  variable_type variable_name##_;\
public:\
  variable_type get_##variable_name() const\
  { return variable_name##_;}\
  void set_##variable_name(variable_type other)\
  { variable_name##_ = other;}

#ifdef Property_declare_ObString
#undef Property_declare_ObString
#endif
#define Property_declare_ObString(variable_name)\
private:\
  ObString variable_name##_;\
public:\
  const ObString &get_##variable_name() const\
  { return variable_name##_;}\
  int set_##variable_name(const ObString &str)\
  { return deep_copy_ob_string(allocator_, str, variable_name##_);}

#ifdef Property_declare_struct
#undef Property_declare_struct
#endif

#define Property_declare_struct(variable_type, variable_name)\
private:\
  variable_type variable_name##_;\
public:\
  const variable_type &get_##variable_name() const\
  { return variable_name##_;}\
  variable_type &get_##variable_name() \
  { return variable_name##_;}\
  void set_##variable_name(variable_type other)\
  { variable_name##_ = other;}

class ObImportTableTaskStatus final
{
public:
  enum Status
  {
    INIT = 0,
    DOING = 1,
    FINISH = 2,
    MAX
  };
  ObImportTableTaskStatus(): status_(MAX) {}
  ~ObImportTableTaskStatus() {}
  ObImportTableTaskStatus(const Status &status): status_(status) {}
  ObImportTableTaskStatus(const ObImportTableTaskStatus &status): status_(status.status_) {}
  ObImportTableTaskStatus &operator=(const ObImportTableTaskStatus &status) {
    if (this != &status) {
      status_ = status.status_;
    }
    return *this;
  }
  ObImportTableTaskStatus &operator=(const Status &status) { status_ = status; return *this; }
  bool operator ==(const ObImportTableTaskStatus &other) const { return status_ == other.status_; }
  bool operator !=(const ObImportTableTaskStatus &other) const { return status_ != other.status_; }
  operator Status() const { return status_; }
  bool is_valid() const { return status_ >= INIT && status_ < MAX; }
  bool is_finish() const { return FINISH == status_; }

  Status get_status() const { return status_; }
  ObImportTableTaskStatus get_next_status(const int err_code);
  const char* get_str() const;
  int set_status(const char *str);
  TO_STRING_KV(K_(status))
private:
  Status status_;
};

struct ObImportTableTask final : public ObIInnerTableRow
{
public:
  struct Key final : public ObIInnerTableKey
  {
    Key() : tenant_id_(OB_INVALID_TENANT_ID), task_id_() {}
    ~Key() {}
    void reset() { tenant_id_ = OB_INVALID_TENANT_ID; task_id_ = 0; }
    bool is_pkey_valid() const override { return (is_user_tenant(tenant_id_) || is_sys_tenant(tenant_id_)) && task_id_ != 0; }
    bool operator==(const Key &other) const
    {
      return task_id_ == other.task_id_ && tenant_id_ == other.tenant_id_;
    }
    bool operator!=(const Key &other) const
    {
      return task_id_ != other.task_id_ || tenant_id_ != other.tenant_id_;
    }
    int fill_pkey_dml(share::ObDMLSqlSplicer &dml) const override;
    TO_STRING_KV(K_(tenant_id), K_(task_id));
    uint64_t tenant_id_;
    int64_t task_id_;
  };
public:
  ObImportTableTask() { reset(); }
  virtual ~ObImportTableTask() {}
  bool is_valid() const { return is_pkey_valid(); }
  void reset();
  int assign(const ObImportTableTask &that);
  int get_pkey(Key &key) const;
  bool is_pkey_valid() const override { return (is_user_tenant(tenant_id_) || is_sys_tenant(tenant_id_)) && task_id_ != 0; }
  int fill_pkey_dml(share::ObDMLSqlSplicer &dml) const override;
  int parse_from(common::sqlclient::ObMySQLResult &result) override;
  int fill_dml(share::ObDMLSqlSplicer &dml) const override;

  TO_STRING_KV(K_(tenant_id), K_(task_id), K_(job_id), K_(src_tenant_id), K_(src_tablespace), K_(src_tablegroup),
      K_(src_database), K_(src_table), K_(src_partition), K_(target_tablespace), K_(target_tablegroup), K_(target_database),
      K_(target_table), K_(table_column), K_(status), K_(start_ts), K_(completion_ts), K_(cumulative_ts), K_(total_bytes),
      K_(total_rows), K_(imported_bytes), K_(imported_rows), K_(total_index_count), K_(imported_index_count),
      K_(failed_index_count), K_(total_constraint_count), K_(imported_constraint_count), K_(failed_constraint_count),
      K_(total_ref_constraint_count), K_(imported_ref_constraint_count), K_(failed_ref_constraint_count),
      K_(total_trigger_count), K_(imported_trigger_count), K_(failed_trigger_count), K_(result));

  Property_declare_int(uint64_t, tenant_id)
  Property_declare_int(int64_t, task_id)
  Property_declare_int(int64_t, job_id)
  Property_declare_int(uint64_t, src_tenant_id)
  Property_declare_ObString(src_tablespace)
  Property_declare_ObString(src_tablegroup)
  Property_declare_ObString(src_database)
  Property_declare_ObString(src_table)
  Property_declare_ObString(src_partition)
  Property_declare_ObString(target_tablespace)
  Property_declare_ObString(target_tablegroup)
  Property_declare_ObString(target_database)
  Property_declare_ObString(target_table)
  Property_declare_int(int64_t, table_column)
  Property_declare_int(ObImportTableTaskStatus, status)
  Property_declare_int(int64_t, start_ts)
  Property_declare_int(int64_t, completion_ts)
  Property_declare_int(int64_t, cumulative_ts)
  Property_declare_int(int64_t, total_bytes)
  Property_declare_int(int64_t, total_rows)
  Property_declare_int(int64_t, imported_bytes)
  Property_declare_int(int64_t, imported_rows)
  Property_declare_int(int64_t, total_index_count)
  Property_declare_int(int64_t, imported_index_count)
  Property_declare_int(int64_t, failed_index_count)
  Property_declare_int(int64_t, total_constraint_count)
  Property_declare_int(int64_t, imported_constraint_count)
  Property_declare_int(int64_t, failed_constraint_count)
  Property_declare_int(int64_t, total_ref_constraint_count)
  Property_declare_int(int64_t, imported_ref_constraint_count)
  Property_declare_int(int64_t, failed_ref_constraint_count)
  Property_declare_int(int64_t, total_trigger_count)
  Property_declare_int(int64_t, imported_trigger_count)
  Property_declare_int(int64_t, failed_trigger_count)
  Property_declare_struct(ObImportResult, result)

private:
  common::ObArenaAllocator allocator_;
};

class ObImportTableJobStatus final
{
public:
  enum Status
  {
    INIT = 0,
    IMPORT_TABLE = 1,
    RECONSTRUCT_REF_CONSTRAINT = 2,
    CANCELING = 3,
    IMPORT_FINISH = 4,
    MAX_STATUS
  };
  ObImportTableJobStatus(): status_(MAX_STATUS) {}
  ~ObImportTableJobStatus() {}
  ObImportTableJobStatus(const Status &status): status_(status) {}
  ObImportTableJobStatus(const ObImportTableJobStatus &status): status_(status.status_) {}
  ObImportTableJobStatus &operator=(const ObImportTableJobStatus &status) {
    if (this != &status) {
      status_ = status.status_;
    }
    return *this;
  }
  ObImportTableJobStatus &operator=(const Status &status) { status_ = status; return *this; }
  bool operator ==(const ObImportTableJobStatus &other) const { return status_ == other.status_; }
  bool operator ==(const Status &other) const { return status_ == other; }
  bool operator !=(const ObImportTableJobStatus &other) const { return status_ != other.status_; }
  bool operator !=(const Status &other) const { return status_ != other; }
  operator Status() const { return status_; }
  bool is_valid() const { return status_ >= INIT && status_ < MAX_STATUS; }
  bool is_finish() const { return IMPORT_FINISH == status_; }

  Status get_status() const { return status_; }
  static ObImportTableJobStatus get_next_status(const ObImportTableJobStatus &cur_status);

  const char* get_str() const;
  int set_status(const char *str);

  TO_STRING_KV(K_(status))
private:
  Status status_;
};


struct ObImportTableJob final : public ObIInnerTableRow
{
public:

  struct Key final : public ObIInnerTableKey
  {
    Key() : tenant_id_(OB_INVALID_TENANT_ID), job_id_(-1) {}
    ~Key() {}
    void reset() { tenant_id_ = OB_INVALID_TENANT_ID; job_id_ = -1; }
    bool is_pkey_valid() const override { return (is_user_tenant(tenant_id_) || is_sys_tenant(tenant_id_)) && job_id_ >= 0; }
    bool operator==(const Key &other) const
    {
      return job_id_ == other.job_id_ && tenant_id_ == other.tenant_id_;
    }
    bool operator!=(const Key &other) const
    {
      return job_id_ != other.job_id_ || tenant_id_ != other.tenant_id_;
    }
    int fill_pkey_dml(share::ObDMLSqlSplicer &dml) const override;
    TO_STRING_KV(K_(tenant_id), K_(job_id));
    uint64_t tenant_id_;
    int64_t job_id_;
  };

public:
  ObImportTableJob() { reset(); }
  virtual ~ObImportTableJob() {}
  bool is_valid() const override { return is_pkey_valid(); }
  void reset();
  int get_pkey(Key &key) const;
  bool is_pkey_valid() const override { return (is_user_tenant(tenant_id_) || is_sys_tenant(tenant_id_)) && job_id_ >= 0; }
  int fill_pkey_dml(share::ObDMLSqlSplicer &dml) const override;
  int parse_from(common::sqlclient::ObMySQLResult &result) override;
  int fill_dml(share::ObDMLSqlSplicer &dml) const override;
  int assign(const ObImportTableJob &that);

  TO_STRING_KV(K_(tenant_id), K_(job_id), K_(initiator_tenant_id), K_(initiator_job_id), K_(start_ts), K_(end_ts),
      K_(src_tenant_name), K_(src_tenant_id), K_(status), K_(total_table_count), K_(finished_table_count),
      K_(failed_table_count), K_(total_bytes), K_(finished_bytes), K_(failed_bytes), K_(result), K_(import_arg));

  Property_declare_int(uint64_t, tenant_id)
  Property_declare_int(int64_t, job_id)
  Property_declare_int(uint64_t, initiator_tenant_id)
  Property_declare_int(int64_t, initiator_job_id)
  Property_declare_int(int64_t, start_ts)
  Property_declare_int(int64_t, end_ts)
  Property_declare_ObString(src_tenant_name)
  Property_declare_int(uint64_t, src_tenant_id)
  Property_declare_int(ObImportTableJobStatus, status)
  Property_declare_int(int64_t, total_table_count)
  Property_declare_int(int64_t, finished_table_count)
  Property_declare_int(int64_t, failed_table_count)
  Property_declare_int(int64_t, total_bytes)
  Property_declare_int(int64_t, finished_bytes)
  Property_declare_int(int64_t, failed_bytes)
  Property_declare_struct(ObImportResult, result)
  Property_declare_ObString(description)

  const share::ObImportArg &get_import_arg() const { return import_arg_; }
  share::ObImportArg &get_import_arg() { return import_arg_; }

private:
  share::ObImportArg import_arg_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObImportTableJob);
};

class ObRecoverTableStatus final
{
public:
  enum Status
  {
    PREPARE = 0,
    RECOVERING = 1,
    RESTORE_AUX_TENANT = 2,
    ACTIVE_AUX_TENANT = 3,
    PRECHECK_IMPORT = 4,
    GEN_IMPORT_JOB = 5,
    IMPORTING = 6,
    CANCELING = 7,
    COMPLETED = 8,
    FAILED = 9,
    MAX_STATUS
  };
public:
  ObRecoverTableStatus(): status_(MAX_STATUS) {}
  ~ObRecoverTableStatus() {}
  ObRecoverTableStatus(const Status &status): status_(status) {}
  ObRecoverTableStatus(const ObRecoverTableStatus &status): status_(status.status_) {}
  ObRecoverTableStatus &operator=(const ObRecoverTableStatus &status) {
    if (this != &status) {
      status_ = status.status_;
    }
    return *this;
  }

  ObRecoverTableStatus &operator=(const Status &status) { status_ = status; return *this; }
  bool operator ==(const ObRecoverTableStatus &other) const { return status_ == other.status_; }
  bool operator ==(const ObRecoverTableStatus::Status &other) const { return status_ == other; }
  bool operator !=(const ObRecoverTableStatus::Status &other) const { return status_ != other; }
  bool operator !=(const ObRecoverTableStatus &other) const { return status_ != other.status_; }
  operator Status() const { return status_; }
  bool is_valid() const { return status_ >= PREPARE && status_ < MAX_STATUS; }
  bool is_finish() const { return COMPLETED == status_ || FAILED == status_; }
  bool is_completed() const { return COMPLETED == status_; }

  Status get_status() const { return status_; }
  static ObRecoverTableStatus get_sys_next_status(const ObRecoverTableStatus &cur_status); // used by sys job
  static ObRecoverTableStatus get_user_next_status(const ObRecoverTableStatus &cur_status); // used by user job

  const char* get_str() const;
  int set_status(const char *str);

  TO_STRING_KV(K_(status))

private:
  Status status_;
};

struct ObRecoverTableJob final : public ObIInnerTableRow
{
public:
  struct Key final : public ObIInnerTableKey
  {
    Key() : tenant_id_(OB_INVALID_TENANT_ID), job_id_(-1) {}
    ~Key() {}
    void reset() { tenant_id_ = OB_INVALID_TENANT_ID; job_id_ = -1; }
    bool is_pkey_valid() const override {
         return (is_user_tenant(tenant_id_) || is_sys_tenant(tenant_id_)) && job_id_ >= 0; }

    bool operator==(const Key &other) const
    {
      return job_id_ == other.job_id_ && tenant_id_ == other.tenant_id_;
    }
    bool operator!=(const Key &other) const
    {
      return job_id_ != other.job_id_ || tenant_id_ != other.tenant_id_;
    }
    int fill_pkey_dml(share::ObDMLSqlSplicer &dml) const override;
    TO_STRING_KV(K_(tenant_id), K_(job_id));
    uint64_t tenant_id_;
    int64_t job_id_;
  };

public:
  ObRecoverTableJob() { reset(); }
  virtual ~ObRecoverTableJob() {}
  bool is_valid() const override { return is_pkey_valid(); }
  void reset();
  int assign(const ObRecoverTableJob &that);
  bool is_pkey_valid() const override { return (is_user_tenant(tenant_id_) || is_sys_tenant(tenant_id_)) && job_id_ >= 0; }

  int get_pkey(Key &key) const;
  int fill_pkey_dml(share::ObDMLSqlSplicer &dml) const override;
  int parse_from(common::sqlclient::ObMySQLResult &result) override;
  int fill_dml(share::ObDMLSqlSplicer &dml) const override;

  TO_STRING_KV(K_(tenant_id), K_(job_id), K_(initiator_tenant_id), K_(initiator_job_id), K_(start_ts), K_(end_ts), K_(status),
      K_(aux_tenant_name), K_(target_tenant_name), K_(target_tenant_id), K_(restore_scn), K_(restore_option),
      K_(backup_dest), K_(backup_dest), K_(backup_passwd), K_(external_kms_info), K_(result),
      K_(import_arg), K_(multi_restore_path_list));

  Property_declare_int(uint64_t, tenant_id)
  Property_declare_int(int64_t, job_id)
  Property_declare_int(uint64_t, initiator_tenant_id)
  Property_declare_int(int64_t, initiator_job_id)
  Property_declare_int(int64_t, start_ts)
  Property_declare_int(int64_t, end_ts)
  Property_declare_int(ObRecoverTableStatus, status)
  Property_declare_ObString(aux_tenant_name)
  Property_declare_ObString(target_tenant_name)
  Property_declare_int(uint64_t, target_tenant_id)
  Property_declare_int(share::SCN, restore_scn)
  Property_declare_ObString(restore_option)
  Property_declare_ObString(backup_dest)
  Property_declare_ObString(backup_passwd)
  Property_declare_ObString(external_kms_info)
  Property_declare_struct(ObImportResult, result)
  Property_declare_ObString(description)
  ObPhysicalRestoreBackupDestList& get_multi_restore_path_list() { return multi_restore_path_list_; }
  const ObPhysicalRestoreBackupDestList& get_multi_restore_path_list() const { return multi_restore_path_list_; }
  share::ObImportArg &get_import_arg() { return import_arg_; }
  const share::ObImportArg &get_import_arg() const { return import_arg_; }
private:
  share::ObImportArg import_arg_;
  share::ObPhysicalRestoreBackupDestList multi_restore_path_list_;
  common::ObArenaAllocator allocator_;
};

#undef Property_declare_int
#undef Property_declare_ObString
#undef Property_declare_struct
}
}

#endif