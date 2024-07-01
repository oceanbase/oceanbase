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

#ifndef OCEANBASE_SHARE_OB_TENANT_CLONE_TABLE_OPERATOR_H_
#define OCEANBASE_SHARE_OB_TENANT_CLONE_TABLE_OPERATOR_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "rootserver/tenant_snapshot/ob_tenant_snapshot_util.h" // for ObConflictCaseWithClone
#include "share/tenant_snapshot/ob_tenant_snapshot_id.h"
#include "src/share/scn.h"

namespace oceanbase
{
namespace share
{
class ObDMLSqlSplicer;
class TenantCloneStatusStrPair;

class ObCancelCloneJobReason
{
  OB_UNIS_VERSION(1);
public:
  enum CancelCloneJobReason
  {
    INVALID = -1,
    CANCEL_BY_USER,
    CANCEL_BY_STANDBY_TRANSFER,
    CANCEL_BY_STANDBY_UPGRADE,
    CANCEL_BY_STANDBY_ALTER_LS,
    MAX
  };
public:
  ObCancelCloneJobReason() : reason_(INVALID) {}
  explicit ObCancelCloneJobReason(CancelCloneJobReason reason) : reason_(reason) {}

  int init_by_conflict_case(const rootserver::ObConflictCaseWithClone &case_to_check);
  ObCancelCloneJobReason &operator=(const CancelCloneJobReason reason) { reason_ = reason; return *this; }
  ObCancelCloneJobReason &operator=(const ObCancelCloneJobReason &other) { reason_ = other.reason_; return *this; }
  void reset() { reason_ = INVALID; }
  void assign(const ObCancelCloneJobReason &other) { reason_ = other.reason_; }
  bool operator==(const ObCancelCloneJobReason &other) const { return other.reason_ == reason_; }
  bool operator!=(const ObCancelCloneJobReason &other) const { return other.reason_ != reason_; }
  bool is_valid() const { return INVALID < reason_ && MAX > reason_; }
  bool is_cancel_by_user() const { return CANCEL_BY_USER == reason_; }
  bool is_cancel_by_standby_transfer() const { return CANCEL_BY_STANDBY_TRANSFER == reason_; }
  bool is_cancel_by_standby_upgrade() const { return CANCEL_BY_STANDBY_UPGRADE == reason_; }
  bool is_cancel_by_standby_alter_ls() const { return CANCEL_BY_STANDBY_ALTER_LS == reason_; }
  const CancelCloneJobReason &get_reason() const { return reason_; }
  const char* get_reason_str() const;

  TO_STRING_KV("reason", get_reason_str());
private:
  CancelCloneJobReason reason_;
};

class ObTenantCloneStatus
{
public:
  enum class Status : int64_t
  {
    // Attention!!!
    // If you want to add new status, please be sure to add the corresponding string
    // in ObTenantCloneStatus::TENANT_CLONE_STATUS_ARRAY[]
    CLONE_SYS_LOCK = 0,
    CLONE_SYS_CREATE_INNER_RESOURCE_POOL,
    CLONE_SYS_CREATE_SNAPSHOT, // only for fork tenant job
    CLONE_SYS_WAIT_CREATE_SNAPSHOT, // only for fork tenant job
    CLONE_SYS_CREATE_TENANT,
    CLONE_SYS_WAIT_TENANT_RESTORE_FINISH,
    CLONE_SYS_RELEASE_RESOURCE,

    CLONE_USER_PREPARE = 50,
    CLONE_USER_CREATE_INIT_LS,
    CLONE_USER_WAIT_LS,
    CLONE_USER_POST_CHECK,

    CLONE_USER_SUCCESS = 100,
    CLONE_USER_FAIL,

    CLONE_SYS_SUCCESS = 150,
    CLONE_SYS_LOCK_FAIL,
    CLONE_SYS_CREATE_INNER_RESOURCE_POOL_FAIL,
    CLONE_SYS_CREATE_SNAPSHOT_FAIL,
    CLONE_SYS_WAIT_CREATE_SNAPSHOT_FAIL,
    CLONE_SYS_CREATE_TENANT_FAIL,
    CLONE_SYS_WAIT_TENANT_RESTORE_FINISH_FAIL,
    CLONE_SYS_RELEASE_RESOURCE_FAIL,
    //This status corresponds to the user's command to cancel cloning job
    CLONE_SYS_CANCELING,
    CLONE_SYS_CANCELED,
    CLONE_MAX_STATUS = 200
  };

private:
  struct TenantCloneStatusStrPair
  {
    TenantCloneStatusStrPair(ObTenantCloneStatus::Status status, const char *str) : status_(status), str_(str) {}
    ObTenantCloneStatus::Status status_;
    const char *str_;
  };
  static const TenantCloneStatusStrPair TENANT_CLONE_STATUS_ARRAY[];

public:
  ObTenantCloneStatus() : status_(Status::CLONE_MAX_STATUS) {}
  ~ObTenantCloneStatus() {};
  explicit ObTenantCloneStatus(const Status &status): status_(status) {}
  ObTenantCloneStatus &operator=(const ObTenantCloneStatus &status);
  ObTenantCloneStatus &operator=(const Status &status);
  bool operator ==(const ObTenantCloneStatus &other) const { return status_ == other.status_; }
  bool operator !=(const ObTenantCloneStatus &other) const { return status_ != other.status_; }
  operator Status() const { return status_; }
  static const char *get_clone_status_str(const Status &status);
  static ObTenantCloneStatus get_clone_status(const ObString &status_str);
  Status get_status() const { return status_; }
  bool is_valid() const { return status_ != Status::CLONE_MAX_STATUS; }
  void reset() {status_ = Status::CLONE_MAX_STATUS; }
  bool is_user_status() const;
  bool is_user_success_status() const;
  bool is_sys_status() const;
  bool is_sys_canceling_status() const;
  bool is_sys_failed_status() const;
  bool is_sys_success_status() const;
  bool is_sys_processing_status() const;
  bool is_sys_valid_snapshot_status_for_fork() const;
  bool is_sys_release_resource_status() const;
  bool is_sys_release_clone_resource_status() const;
  TO_STRING_KV("status", get_clone_status_str(status_));

private:
  Status status_;
};

enum class ObTenantCloneJobType : int64_t
{
  RESTORE = 0,
  FORK = 1,
  CLONE_JOB_MAX_TYPE = 200
};

struct TenantCloneJobTypeStrPair
{
  TenantCloneJobTypeStrPair(ObTenantCloneJobType type, const char *str) : type_(type), str_(str) {}
  ObTenantCloneJobType type_;
  const char *str_;
};

struct ObCloneJob {
public:
  ObCloneJob();
public:
  const common::ObCurTraceId::TraceId &get_trace_id() const { return trace_id_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  int64_t get_job_id() const { return job_id_; }
  uint64_t get_source_tenant_id() const { return source_tenant_id_; }
  const ObString &get_source_tenant_name() const { return source_tenant_name_; }
  uint64_t get_clone_tenant_id() const { return clone_tenant_id_; }
  const ObString &get_clone_tenant_name() const { return clone_tenant_name_; }
  void set_clone_tenant_id(const uint64_t clone_tenant_id) { clone_tenant_id_ = clone_tenant_id; }
  const ObTenantSnapshotID &get_tenant_snapshot_id() const { return tenant_snapshot_id_; }
  const ObString &get_tenant_snapshot_name() const { return tenant_snapshot_name_; }
  uint64_t get_resource_pool_id() const { return resource_pool_id_; }
  const ObString &get_resource_pool_name() const { return resource_pool_name_; }
  const ObString &get_unit_config_name() const { return unit_config_name_; }
  const SCN &get_restore_scn() const { return restore_scn_; }
  const ObTenantCloneStatus &get_status() const { return status_; }
  const ObTenantCloneJobType &get_job_type() const { return job_type_; }
  void set_status(const ObTenantCloneStatus::Status &status) { status_ = status; }
  int get_ret_code() const { return ret_code_; }
  uint64_t get_data_version() const { return data_version_; }
  uint64_t get_min_cluster_version() const { return min_cluster_version_; }

  struct ObCloneJobInitArg
  {
    common::ObCurTraceId::TraceId trace_id_;
    uint64_t tenant_id_;
    int64_t job_id_;
    uint64_t source_tenant_id_;
    ObString source_tenant_name_;
    uint64_t clone_tenant_id_;  //new cloned user_tenant_id
    ObString clone_tenant_name_;
    ObTenantSnapshotID tenant_snapshot_id_;
    ObString tenant_snapshot_name_;
    uint64_t resource_pool_id_;
    ObString resource_pool_name_;
    ObString unit_config_name_;
    SCN restore_scn_;
    ObTenantCloneStatus status_;
    ObTenantCloneJobType job_type_;
    int ret_code_;
    uint64_t data_version_;
    uint64_t min_cluster_version_;
    TO_STRING_KV(K_(trace_id), K_(tenant_id), K_(job_id),
                 K_(source_tenant_id), K_(source_tenant_name),
                 K_(clone_tenant_id), K_(clone_tenant_name),
                 K_(tenant_snapshot_id), K_(tenant_snapshot_name),
                 K_(resource_pool_id), K_(resource_pool_name),
                 K_(unit_config_name), K_(restore_scn),
                 K_(status), K_(job_type), K_(ret_code),
                 K_(data_version), K_(min_cluster_version));
  };
  int init(const ObCloneJobInitArg &init_arg);
  int assign(const ObCloneJob &other);
  void reset();
  bool is_valid() const;
  bool is_valid_status_allows_user_tenant_to_do_ls_recovery() const;
  TO_STRING_KV(K_(trace_id), K_(tenant_id), K_(job_id),
               K_(source_tenant_id), K_(source_tenant_name),
               K_(clone_tenant_id), K_(clone_tenant_name),
               K_(tenant_snapshot_id), K_(tenant_snapshot_name),
               K_(resource_pool_id), K_(resource_pool_name),
               K_(unit_config_name), K_(restore_scn),
               K_(status), K_(job_type), K_(ret_code),
               K_(data_version), K_(min_cluster_version));
private:
  common::ObCurTraceId::TraceId trace_id_;
  //in sys tenant space, tenant_id_ is OB_SYS_TENANT_ID
  //in user tenant space, tenant_id_ is new cloned user_tenant_id
  uint64_t tenant_id_;
  int64_t job_id_;
  uint64_t source_tenant_id_;
  ObString source_tenant_name_;
  uint64_t clone_tenant_id_;  //new cloned user_tenant_id
  ObString clone_tenant_name_;
  ObTenantSnapshotID tenant_snapshot_id_;
  ObString tenant_snapshot_name_;
  uint64_t resource_pool_id_;
  ObString resource_pool_name_;
  ObString unit_config_name_;
  SCN restore_scn_;
  ObTenantCloneStatus status_;
  ObTenantCloneJobType job_type_;
  int ret_code_;
  ObArenaAllocator allocator_;
  uint64_t data_version_;
  uint64_t min_cluster_version_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCloneJob);
};

class ObTenantCloneTableOperator
{
public:
  ObTenantCloneTableOperator();
  bool is_inited() const { return is_inited_; }

  int init(const uint64_t user_tenant_id, ObISQLClient *proxy);
  //get clone job according to source_tenant_id
  //a source tenant only has one clone_job in OB_ALL_CLONE_JOB_TNAME
  int get_clone_job_by_source_tenant_id(const uint64_t source_tenant_id,
                                        ObCloneJob &job);
  int get_clone_job_by_job_id(const int64_t job_id, ObCloneJob &job);
  //get clone job according to clone_tenant_name
  int get_clone_job_by_clone_tenant_name(const ObString &clone_tenant_name,
                                         const bool need_lock,
                                         ObCloneJob &job);
  int get_all_clone_jobs(ObArray<ObCloneJob> &jobs);
  int insert_clone_job(const ObCloneJob &job);
  int update_job_status(const int64_t job_id,
                        const ObTenantCloneStatus &old_status,
                        const ObTenantCloneStatus &new_status);
  int update_job_failed_info(const int64_t job_id, const int ret_code, const ObString& err_msg);
  int update_job_resource_pool_id(const int64_t job_id, const uint64_t resource_pool_id);
  int update_job_clone_tenant_id(const int64_t job_id,
                                 const uint64_t clone_tenant_id);
  int update_job_snapshot_info(const int64_t job_id,
                               const ObTenantSnapshotID snapshot_id,
                               const ObString &snapshot_name);
  int update_job_snapshot_scn(const int64_t job_id,
                              const SCN &restore_scn);
  int remove_clone_job(const ObCloneJob &job);
  int insert_clone_job_history(const ObCloneJob &job);
  //get user clone job history
  int get_user_clone_job_history(ObCloneJob &job);
  //get clone job history according to job_id
  int get_sys_clone_job_history(const int64_t job_id,
                                ObCloneJob &job);
  int get_job_failed_message(const int64_t job_id, ObIAllocator &allocator, ObString &err_msg);

public:
  static const char* get_job_type_str(ObTenantCloneJobType job_type);
  static ObTenantCloneJobType get_job_type(const ObString &str);
private:
  //job num <= 1 in a certain tenant
  int read_only_exist_one_job_(const ObSqlString &sql,
                               ObCloneJob &job);
  int read_jobs_(const ObSqlString &sql,
                 ObArray<ObCloneJob> &jobs);
  int build_insert_dml_(const ObCloneJob &job,
                        ObDMLSqlSplicer &dml);
  int fill_job_from_result_(const common::sqlclient::ObMySQLResult *result,
                            ObCloneJob &job);
private:
  static const TenantCloneJobTypeStrPair TENANT_CLONE_JOB_TYPE_ARRAY[];
private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObISQLClient *proxy_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantCloneTableOperator);

};

}
}

#endif /* OCEANBASE_SHARE_OB_TENANT_CLONE_TABLE_OPERATOR_H_ */
