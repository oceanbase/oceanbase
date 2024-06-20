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

#ifndef OCEANBASE_SHARE_OB_TENANT_SNAPSHOT_TABLE_OPERATOR_H_
#define OCEANBASE_SHARE_OB_TENANT_SNAPSHOT_TABLE_OPERATOR_H_

#include "storage/ls/ob_ls_meta_package.h"
#include "share/tenant_snapshot/ob_tenant_snapshot_id.h"

namespace oceanbase
{
namespace share
{
// Attention!!!
// if a tenant_snapshot is created failed, the status of it is DELETING rather than FAILED.
// only if a tmp snapshot for fork tenant (a type of tenant clone job) is created failed,
// the snapshot will be set as FAILED.
// And this type of snapshot will be recycled after user executes the recycle sql for clone_job.
enum class ObTenantSnapStatus : int64_t
{
  CREATING = 0,
  DECIDED,
  NORMAL,
  CLONING,
  DELETING,
  FAILED,
  MAX,
};

enum class ObLSSnapStatus : int64_t
{
  CREATING = 0,
  NORMAL,
  CLONING,
  FAILED,
  MAX,
};

enum class ObTenantSnapType : int64_t
{
  AUTO,
  MANUAL,
  MAX,
};

enum class ObTenantSnapOperation : int64_t
{
  CREATE = 0,
  DELETE,
  MAX,
};

struct ObTenantSnapJobItem
{
public:
  ObTenantSnapJobItem() : tenant_id_(OB_INVALID_TENANT_ID),
                                     tenant_snapshot_id_(),
                                     operation_(ObTenantSnapOperation::MAX),
                                     job_start_time_(OB_INVALID_TIMESTAMP),
                                     trace_id_(),
                                     majority_succ_time_(OB_INVALID_TIMESTAMP) {}
  ObTenantSnapJobItem(uint64_t tenant_id,
                      ObTenantSnapshotID tenant_snapshot_id,
                      ObTenantSnapOperation operation,
                      const common::ObCurTraceId::TraceId& trace_id) :
                      tenant_id_(tenant_id),
                      tenant_snapshot_id_(tenant_snapshot_id),
                      operation_(operation),
                      job_start_time_(OB_INVALID_TIMESTAMP),
                      trace_id_(trace_id),
                      majority_succ_time_(OB_INVALID_TIMESTAMP) {}
  int assign(const ObTenantSnapJobItem &other);
  ObTenantSnapJobItem &operator=(const ObTenantSnapJobItem &other) = delete;
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(tenant_id), K_(tenant_snapshot_id), K_(operation),
               K_(job_start_time), K_(trace_id), K_(majority_succ_time));

public:
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  ObTenantSnapshotID get_tenant_snapshot_id() const { return tenant_snapshot_id_; }
  void set_tenant_snapshot_id(const ObTenantSnapshotID tenant_snapshot_id)
  {
    tenant_snapshot_id_ = tenant_snapshot_id;
  }

  ObTenantSnapOperation get_operation() const { return operation_; }
  void set_operation(ObTenantSnapOperation operation) { operation_ = operation; }

  const common::ObCurTraceId::TraceId& get_trace_id() const { return trace_id_; }
  void set_trace_id(common::ObCurTraceId::TraceId& trace_id) { trace_id_ = trace_id; }

  int64_t get_job_start_time() const { return job_start_time_; }
  void set_job_start_time(int64_t job_start_time) { job_start_time_ = job_start_time; }

  int64_t get_majority_succ_time() const { return majority_succ_time_; }
  void set_majority_succ_time(const int64_t majority_succ_time)
  {
    majority_succ_time_ = majority_succ_time;
  }

private:
  uint64_t tenant_id_;
  ObTenantSnapshotID tenant_snapshot_id_;
  ObTenantSnapOperation operation_;
  int64_t job_start_time_;
  common::ObCurTraceId::TraceId trace_id_;
  int64_t majority_succ_time_;
};

struct ObCreateSnapshotJob
{
public:
  ObCreateSnapshotJob() : create_active_ts_(OB_INVALID_TIMESTAMP),
                          create_expire_ts_(OB_INVALID_TIMESTAMP),
                          paxos_replica_num_(OB_INVALID_COUNT),
                          arbitration_service_status_(),
                          job_item_() {}
  ~ObCreateSnapshotJob() {}

  int init(const int64_t create_active_ts,
           const int64_t create_expire_ts,
           const int64_t paxos_replica_num,
           const ObArbitrationServiceStatus &arbitration_service_status,
           const ObTenantSnapJobItem& job_item);

  void reset();
  int assign(const ObCreateSnapshotJob& other);
  bool is_valid() const { return job_item_.is_valid()
                                 && OB_INVALID_TIMESTAMP != create_active_ts_
                                 && OB_INVALID_TIMESTAMP != create_expire_ts_
                                 && OB_INVALID_COUNT != paxos_replica_num_
                                 && arbitration_service_status_.is_valid(); }

public:
  int64_t get_create_active_ts() const { return create_active_ts_; }
  int64_t get_create_expire_ts() const { return create_expire_ts_; }
  int64_t get_paxos_replica_num() const { return paxos_replica_num_; }
  const ObArbitrationServiceStatus &get_arbitration_service_status() const { return arbitration_service_status_; }
  ObTenantSnapshotID get_tenant_snapshot_id() const { return job_item_.get_tenant_snapshot_id(); }
  int64_t get_majority_succ_time() const { return job_item_.get_majority_succ_time(); }
  const common::ObCurTraceId::TraceId& get_trace_id() const { return job_item_.get_trace_id(); }

  TO_STRING_KV(K_(create_active_ts),
               K_(create_expire_ts),
               K_(paxos_replica_num),
               K_(arbitration_service_status),
               K_(job_item));
private:
  int64_t create_active_ts_;
  int64_t create_expire_ts_;
  int64_t paxos_replica_num_;
  ObArbitrationServiceStatus arbitration_service_status_;
  ObTenantSnapJobItem job_item_;
};

struct ObDeleteSnapshotJob
{
public:
  ObDeleteSnapshotJob() : job_item_() {}
public:
  ObTenantSnapshotID get_tenant_snapshot_id() const { return job_item_.get_tenant_snapshot_id(); }
  const common::ObCurTraceId::TraceId& get_trace_id() const { return job_item_.get_trace_id(); }
  int64_t get_job_start_time() const { return job_item_.get_job_start_time(); }

  int init(const ObTenantSnapJobItem& job_item);
  void reset();
  int assign(const ObDeleteSnapshotJob& other);
  bool is_valid() const { return job_item_.is_valid(); }

  TO_STRING_KV(K_(job_item));
private:
  ObTenantSnapJobItem job_item_;
};

struct ObTenantSnapItem
{
public:
  ObTenantSnapItem();
public:
  uint64_t get_tenant_id() const { return tenant_id_; }
  const ObTenantSnapshotID &get_tenant_snapshot_id() const { return tenant_snapshot_id_; }
  const char *get_snapshot_name() const { return snapshot_name_; }
  const ObTenantSnapStatus &get_status() const { return status_; }
  void set_status(const ObTenantSnapStatus &status) { status_ = status; }
  const SCN &get_snapshot_scn() const { return snapshot_scn_; }
  void set_snapshot_scn(const SCN &snapshot_scn) { snapshot_scn_ = snapshot_scn; }
  const SCN &get_clog_start_scn() const { return clog_start_scn_; }
  const ObTenantSnapType &get_type() const { return type_; }
  int64_t get_create_time() const { return create_time_; }
  void set_create_time(const int64_t create_time) { create_time_ = create_time; }
  uint64_t get_data_version() const { return data_version_; }
  void set_owner_job_id(const int64_t owner_job_id) { owner_job_id_ = owner_job_id; }
  int64_t get_owner_job_id() const { return owner_job_id_; }
  int init(const uint64_t tenant_id,
           const ObTenantSnapshotID &snapshot_id,
           const ObString &snapshot_name,
           const ObTenantSnapStatus &status,
           const SCN &snapshot_scn,
           const SCN &clog_start_scn,
           const ObTenantSnapType &type,
           const int64_t create_time,
           const uint64_t data_version,
           const int64_t owner_job_id);
  int assign(const ObTenantSnapItem &other);
  void reset();
  bool is_valid() const { return OB_INVALID_TENANT_ID != tenant_id_
                                 && tenant_snapshot_id_.is_valid()
                                 && ObTenantSnapStatus::MAX != status_; }
  TO_STRING_KV(K_(tenant_id), K_(tenant_snapshot_id), K_(snapshot_name),
               K_(status), K_(snapshot_scn), K_(clog_start_scn),
               K_(type), K_(create_time), K_(data_version),
               K_(owner_job_id));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantSnapItem);
private:
  uint64_t tenant_id_;
  ObTenantSnapshotID tenant_snapshot_id_;
  char snapshot_name_[common::OB_MAX_TENANT_SNAPSHOT_NAME_LENGTH + 1];
  ObTenantSnapStatus status_;
  SCN snapshot_scn_;
  SCN clog_start_scn_;
  ObTenantSnapType type_;
  int64_t create_time_;
  uint64_t data_version_;
  // when the status_ is CLONING, the clone_job id will be owner_job_id of "global_lock"(snapshot_id == 0)
  // for the other status or the other snapshot, the owner_job_id always be OB_INVALID_ID
  int64_t owner_job_id_;
};

struct ObTenantSnapLSItem {
public:
  ObTenantSnapLSItem() : tenant_id_(OB_INVALID_TENANT_ID),
                         tenant_snapshot_id_(),
                         ls_attr_() {}
public:
  uint64_t get_tenant_id() const { return tenant_id_; }
  const ObTenantSnapshotID &get_tenant_snapshot_id() const { return tenant_snapshot_id_; }
  const ObLSAttr &get_ls_attr() const { return ls_attr_; }
  int init(const uint64_t tenant_id,
           const ObTenantSnapshotID &tenant_snapshot_id,
           const ObLSAttr &ls_attr);
  int assign(const ObTenantSnapLSItem &other);
  void reset();
  bool is_valid() const { return OB_INVALID_TENANT_ID != tenant_id_
                          && tenant_snapshot_id_.is_valid()
                          && ls_attr_.is_valid(); }
  TO_STRING_KV(K_(tenant_id), K_(tenant_snapshot_id), K_(ls_attr));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantSnapLSItem);
private:
  uint64_t tenant_id_;
  ObTenantSnapshotID tenant_snapshot_id_;
  ObLSAttr ls_attr_;
};

struct ObTenantSnapLSReplicaSimpleItem {
public:
  ObTenantSnapLSReplicaSimpleItem();
public:
  uint64_t get_tenant_id() const { return tenant_id_; }
  const ObTenantSnapshotID &get_tenant_snapshot_id() const { return tenant_snapshot_id_; }
  const ObLSID &get_ls_id() const { return ls_id_; }
  const ObAddr &get_addr() const { return addr_; }
  void set_status(const ObLSSnapStatus &status) { status_ = status; }
  const ObLSSnapStatus &get_status() const { return status_; }
  void set_zone(const ObZone &zone) { zone_ = zone; }
  const ObZone &get_zone() const { return zone_; }
  void set_unit_id(const uint64_t unit_id) { unit_id_ = unit_id; }
  uint64_t get_unit_id() const { return unit_id_; }
  void set_begin_interval_scn(const SCN &begin_interval_scn) { begin_interval_scn_ = begin_interval_scn; }
  const SCN &get_begin_interval_scn() const { return begin_interval_scn_; }
  void set_end_interval_scn(const SCN &end_interval_scn) { end_interval_scn_ = end_interval_scn; }
  const SCN &get_end_interval_scn() const { return end_interval_scn_; }
  int init(const uint64_t tenant_id,
           const ObTenantSnapshotID &tenant_snapshot_id,
           const ObLSID &ls_id,
           const common::ObAddr &addr,
           const ObLSSnapStatus &status,
           const common::ObZone &zone,
           const uint64_t unit_id,
           const SCN &begin_interval_scn,
           const SCN &end_interval_scn);
  int assign(const ObTenantSnapLSReplicaSimpleItem &other);
  void reset();
  bool is_valid() const { return OB_INVALID_TENANT_ID != tenant_id_
                                 && tenant_snapshot_id_.is_valid()
                                 && ls_id_.is_valid()
                                 && addr_.is_valid()
                                 && ObLSSnapStatus::MAX != status_; }
  TO_STRING_KV(K_(tenant_id), K_(tenant_snapshot_id), K_(ls_id),
               K_(addr), K_(status), K_(zone), K_(unit_id),
               K_(begin_interval_scn), K_(end_interval_scn));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantSnapLSReplicaSimpleItem);
private:
  uint64_t tenant_id_;
  ObTenantSnapshotID tenant_snapshot_id_;
  ObLSID ls_id_;
  common::ObAddr addr_;
  ObLSSnapStatus status_;
  common::ObZone zone_;
  uint64_t unit_id_;
  SCN begin_interval_scn_;
  SCN end_interval_scn_;
};

struct ObTenantSnapLSReplicaItem {
public:
  ObTenantSnapLSReplicaSimpleItem s_;
  ObLSMetaPackage ls_meta_package_;
public:
  ObTenantSnapLSReplicaItem() : s_(), ls_meta_package_() {}
  int assign(const ObTenantSnapLSReplicaItem &other);
  bool is_valid() const { return s_.is_valid(); }
  void reset() { s_.reset(); ls_meta_package_.reset(); }
  TO_STRING_KV(K_(s), K_(ls_meta_package));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantSnapLSReplicaItem);
};

//TODO: Eliminate redundancy and simplify functions
class ObTenantSnapshotTableOperator
{
public:
  ObTenantSnapshotTableOperator();
  int init(const uint64_t user_tenant_id, ObISQLClient *proxy);
  bool is_inited() const { return is_inited_; }

public:
  ObTenantSnapStatus str_to_tenant_snap_status(const ObString &status_str);
  const char* tenant_snap_status_to_str(const ObTenantSnapStatus &status);
  ObLSSnapStatus str_to_ls_snap_status(const ObString &status_str);
  const char* ls_snap_status_to_str(const ObLSSnapStatus &status);

  ObTenantSnapType str_to_type(const ObString &type_str);
  const char* type_to_str(const ObTenantSnapType &type);

  ObTenantSnapOperation str_to_operation(const ObString &str);
  const char* operation_to_str(const ObTenantSnapOperation &operation);

  //************************OB_ALL_TENANT_SNAPSHOTS_TNAME**********************************
  /*for special record in inner table __all_tenant_snapshots*/
  int insert_tenant_snap_item(const ObTenantSnapItem& item);
  int get_tenant_snap_item(const ObTenantSnapshotID &snapshot_id,
                           const bool need_lock,
                           ObTenantSnapItem &item);
  int get_tenant_snap_item(const ObString &snapshot_name,
                           const bool need_lock,
                           ObTenantSnapItem &item);
  int get_all_user_tenant_snap_items(ObArray<ObTenantSnapItem> &items);
  //for the item (snapshot = OB_TENANT_SNAPSHOT_GLOBAL_STATE)
  //Note: if new_snapshot_scn use default value(invalid scn), we will not update column "snapshot_scn"
  //      if new_create_time use default value(OB_INVALID_TIMESTAMP), we will not update column "create_time"
  int update_special_tenant_snap_item(const ObTenantSnapshotID &tenant_snapshot_id,
                                      const ObTenantSnapStatus &old_status,
                                      const ObTenantSnapStatus &new_status,
                                      const int64_t owner_job_id,
                                      const SCN &new_snapshot_scn = SCN::invalid_scn(),
                                      const int64_t new_create_time = OB_INVALID_TIMESTAMP);
  int update_tenant_snap_item(const ObTenantSnapshotID &tenant_snapshot_id,
                              const ObTenantSnapStatus &old_status,
                              const ObTenantSnapStatus &new_status,
                              const SCN &new_snapshot_scn = SCN::invalid_scn(),
                              const SCN &new_clog_start_scn = SCN::invalid_scn());
  int update_tenant_snap_item(const ObString &snapshot_name,
                              const ObTenantSnapStatus &old_status,
                              const ObTenantSnapStatus &new_status);
  int update_tenant_snap_item(const ObTenantSnapshotID &tenant_snapshot_id,
                              const ObTenantSnapStatus &old_status_1,
                              const ObTenantSnapStatus &old_status_2,
                              const ObTenantSnapStatus &new_status);
  int delete_tenant_snap_item(const ObTenantSnapshotID &snapshot_id);
  //****************************************************************************************

  //**********************OB_ALL_TENANT_SNAPSHOT_LS_TNAME***********************************
  int insert_tenant_snap_ls_items(const ObArray<ObTenantSnapLSItem> &items);
  int delete_tenant_snap_ls_items(const ObTenantSnapshotID &snapshot_id);
  int get_tenant_snap_ls_items(const ObTenantSnapshotID &tenant_snapshot_id,
                               ObArray<ObTenantSnapLSItem> &items);
  int get_tenant_snap_ls_item(const ObTenantSnapshotID &tenant_snapshot_id,
                              const ObLSID &ls_id,
                              ObTenantSnapLSItem &item);
  //****************************************************************************************

  //******************OB_ALL_TENANT_SNAPSHOT_LS_META_TABLE_TNAME****************************
  int get_tenant_snap_related_addrs(const ObTenantSnapshotID &tenant_snapshot_id,
                                    ObArray<ObAddr> &addrs);
  int get_proper_ls_meta_package(const ObTenantSnapshotID &tenant_snapshot_id,
                                 const ObLSID &ls_id,
                                 ObLSMetaPackage& ls_meta_package);
  int get_ls_meta_package(const ObTenantSnapshotID &tenant_snapshot_id,
                          const ObLSID &ls_id,
                          const ObAddr& addr,
                          ObLSMetaPackage& ls_meta_package);
  //get all simple_items by tenant_snapshot_id
  int get_tenant_snap_ls_replica_simple_items(const ObTenantSnapshotID &tenant_snapshot_id,
                                              ObArray<ObTenantSnapLSReplicaSimpleItem> &simple_items);
  //get all simple_items by tenant_snapshot_id and addr
  int get_tenant_snap_ls_replica_simple_items(const ObTenantSnapshotID &tenant_snapshot_id,
                                              const common::ObAddr& addr,
                                              ObArray<ObTenantSnapLSReplicaSimpleItem> &simple_items);
  //get simple_item by tenant_snapshot_id and ls_id and addr
  int get_tenant_snap_ls_replica_simple_item(const ObTenantSnapshotID &tenant_snapshot_id,
                                             const ObLSID &ls_id,
                                             const ObAddr& addr,
                                             const bool need_lock,
                                             ObTenantSnapLSReplicaSimpleItem &simple_item);
  //get all simple_items by tenant_snapshot_id and ls_id
  int get_tenant_snap_ls_replica_simple_items(const ObTenantSnapshotID &tenant_snapshot_id,
                                              const ObLSID &ls_id,
                                              ObArray<ObTenantSnapLSReplicaSimpleItem> &simple_items);
  int get_tenant_snap_ls_replica_simple_items(const ObTenantSnapshotID &tenant_snapshot_id,
                                              const ObLSID &ls_id,
                                              const ObLSSnapStatus &status,
                                              ObArray<ObTenantSnapLSReplicaSimpleItem> &simple_items);

  int delete_tenant_snap_ls_replica_items(const ObTenantSnapshotID &snapshot_id);
  int insert_tenant_snap_ls_replica_simple_items(const ObArray<ObTenantSnapLSReplicaSimpleItem> &simple_items);
  int update_tenant_snap_ls_replica_item(const ObTenantSnapLSReplicaSimpleItem &simple_item,
                                         const ObLSMetaPackage* ls_meta_package);
  //****************************************************************************************

  //******************OB_ALL_TENANT_SNAPSHOT_LS_META_TABLE_TNAME****************************
  int archive_tenant_snap_ls_replica_history(const ObTenantSnapshotID &snapshot_id);
  int eliminate_tenant_snap_ls_replica_history();
  //****************************************************************************************

  //******************OB_ALL_TENANT_SNAPSHOT_CREATE_JOB_TNAME****************************
  int get_tenant_snap_job_item(const ObTenantSnapshotID &tenant_snapshot_id,
                               const ObTenantSnapOperation operation,
                               ObTenantSnapJobItem &item);
  int insert_tenant_snap_job_item(const ObTenantSnapJobItem &item);
  int update_tenant_snap_job_majority_succ_time(const ObTenantSnapshotID &tenant_snapshot_id,
                                                const int64_t majority_succ_time);
  int delete_tenant_snap_job_item(const ObTenantSnapshotID &snapshot_id);

private:
  int get_tenant_snap_ls_replica_simple_items_(const ObSqlString &sql,
                                               ObArray<ObTenantSnapLSReplicaSimpleItem> &simple_items);
  int fill_item_from_result_(const common::sqlclient::ObMySQLResult *result,
                             ObTenantSnapItem &item);
  int fill_item_from_result_(const common::sqlclient::ObMySQLResult *result,
                             ObTenantSnapLSItem &item);
  int fill_item_from_result_(const common::sqlclient::ObMySQLResult *result,
                             ObTenantSnapLSReplicaSimpleItem &simple_item);
  int fill_item_from_result_(const common::sqlclient::ObMySQLResult *result,
                             ObTenantSnapJobItem &item);
  int encode_package_to_hex_string_(const ObLSMetaPackage& ls_meta_package,
                                    ObArenaAllocator& allocator,
                                    ObString& hex_str);
  int decode_hex_string_to_package_(const ObString& hex_str,
                                    ObArenaAllocator& allocator,
                                    ObLSMetaPackage& ls_meta_package);
  //****************************************************************************************
public:
  static const int64_t GLOBAL_STATE_ID = 0;
  static constexpr const char *GLOBAL_STATE_NAME = "_snapshot_global_state";

private:
  static const char* TENANT_SNAP_STATUS_ARRAY[];
  static const char* LS_SNAP_STATUS_ARRAY[];
  static const char* TENANT_SNAP_TYPE_ARRAY[];
  static const char* TENANT_SNAP_OPERATION_ARRAY[];
  static constexpr const char* INVALID_STR = "invalid";

private:
  bool is_inited_;
  uint64_t user_tenant_id_;
  ObISQLClient *proxy_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantSnapshotTableOperator);
};

}
}

#endif /* OCEANBASE_SHARE_OB_TENANT_SNAPSHOT_TABLE_OPERATOR_H_ */
