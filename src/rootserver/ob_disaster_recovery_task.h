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

#ifndef OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_H_
#define OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_H_

#include "lib/hash/ob_hashtable.h"
#include "lib/hash/ob_refered_map.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "lib/net/ob_addr.h"
#include "common/ob_member.h"
#include "common/ob_zone.h"
#include "common/ob_member_list.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace obrpc
{
class ObSrvRpcProxy;
}

namespace share
{
class ObLSTableOperator;
}

namespace rootserver
{

namespace drtask
{
  const static char * const REMOVE_LOCALITY_PAXOS_REPLICA = "remove redundant paxos replica according to locality";
  const static char * const REMOVE_LOCALITY_NON_PAXOS_REPLICA = "remove redundant non-paxos replica according to locality";
  const static char * const ADD_LOCALITY_PAXOS_REPLICA = "add paxos replica according to locality";
  const static char * const ADD_LOCALITY_NON_PAXOS_REPLICA = "add non-paxos replica according to locality";
  const static char * const TRANSFORM_LOCALITY_REPLICA_TYPE = "type transform according to locality";
  const static char * const MODIFY_PAXOS_REPLICA_NUMBER = "modify paxos replica number according to locality";
  const static char * const REMOVE_PERMANENT_OFFLINE_REPLICA = "remove permanent offline replica";
  const static char * const REPLICATE_REPLICA = "replicate to unit task";
  const static char * const CANCEL_MIGRATE_UNIT_WITH_PAXOS_REPLICA = "cancel migrate unit remove paxos replica";
  const static char * const CANCEL_MIGRATE_UNIT_WITH_NON_PAXOS_REPLICA = "cancel migrate unit remove non-paxos replica";
  const static char * const MIGRATE_REPLICA_DUE_TO_UNIT_GROUP_NOT_MATCH = "migrate replica due to unit group not match";
  const static char * const MIGRATE_REPLICA_DUE_TO_UNIT_NOT_MATCH = "migrate replica due to unit not match";
  const static char * const ALTER_SYSTEM_COMMAND_ADD_REPLICA = "add replica by manual";
  const static char * const ALTER_SYSTEM_COMMAND_REMOVE_REPLICA = "remove replica by manual";
  const static char * const ALTER_SYSTEM_COMMAND_MODIFY_REPLICA_TYPE = "modify replica type by manual";
  const static char * const ALTER_SYSTEM_COMMAND_MIGRATE_REPLICA = "migrate replica by manual";
  const static char * const ALTER_SYSTEM_COMMAND_MODIFY_PAXOS_REPLICA_NUM = "modify paxos_replica_num by manual";
};

namespace drtasklog
{
  const static char * const START_MIGRATE_LS_REPLICA_STR = "start_migrate_ls_replica";
  const static char * const FINISH_MIGRATE_LS_REPLICA_STR = "finish_migrate_ls_replica";
  const static char * const START_ADD_LS_REPLICA_STR = "start_add_ls_replica";
  const static char * const FINISH_ADD_LS_REPLICA_STR = "finish_add_ls_replica";
  const static char * const START_TYPE_TRANSFORM_LS_REPLICA_STR = "start_type_transform_ls_replica";
  const static char * const FINISH_TYPE_TRANSFORM_LS_REPLICA_STR = "finish_type_transform_ls_replica";
  const static char * const START_REMOVE_LS_PAXOS_REPLICA_STR = "start_remove_ls_paxos_replica";
  const static char * const FINISH_REMOVE_LS_PAXOS_REPLICA_STR = "finish_remove_ls_paxos_replica";
  const static char * const START_REMOVE_LS_NON_PAXOS_REPLICA_STR = "start_remove_ls_non_paxos_replica";
  const static char * const FINISH_REMOVE_LS_NON_PAXOS_REPLICA_STR = "finish_remove_ls_non_paxos_replica";
  const static char * const START_MODIFY_PAXOS_REPLICA_NUMBER_STR = "start_modify_paxos_replica_number";
  const static char * const FINISH_MODIFY_PAXOS_REPLICA_NUMBER_STR = "finish_modify_paxos_replica_number";
}

class ObDRLSReplicaTaskStatus
{
  OB_UNIS_VERSION(1);
public:
  enum DRLSReplicaTaskStatus
  {
    INPROGRESS = 0,
    COMPLETED,
    FAILED,
    CANCELED,
    WAITING,
    MAX_STATUS,
  };
public:
  ObDRLSReplicaTaskStatus() : status_(MAX_STATUS) {}
  ObDRLSReplicaTaskStatus(DRLSReplicaTaskStatus status) : status_(status) {}

  ObDRLSReplicaTaskStatus &operator=(const DRLSReplicaTaskStatus status) { status_ = status; return *this; }
  ObDRLSReplicaTaskStatus &operator=(const ObDRLSReplicaTaskStatus &other) { status_ = other.status_; return *this; }
  void reset() { status_ = MAX_STATUS; }
  void assign(const ObDRLSReplicaTaskStatus &other);
  bool is_valid() const { return MAX_STATUS != status_; }
  const DRLSReplicaTaskStatus &get_status() const { return status_; }
  int parse_from_string(const ObString &status);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  const char* get_status_str() const;

private:
  DRLSReplicaTaskStatus status_;
};

enum class ObDRTaskType : int64_t;
enum class ObDRTaskPriority : int64_t;

enum ObDRTaskRetComment
{
  RECEIVE_FROM_STORAGE_RPC = 0,
  FAIL_TO_SEND_RPC = 1,
  CLEAN_TASK_DUE_TO_SERVER_NOT_EXIST = 2,
  CLEAN_TASK_DUE_TO_SERVER_PERMANENT_OFFLINE = 3,
  CLEAN_TASK_DUE_TO_TASK_NOT_RUNNING = 4,
  CLEAN_TASK_DUE_TO_TASK_TIMEOUT = 5,
  CANNOT_EXECUTE_DUE_TO_SERVER_NOT_ALIVE = 6,
  CANNOT_EXECUTE_DUE_TO_PAXOS_REPLICA_NUMBER = 7,
  CANNOT_EXECUTE_DUE_TO_REPLICA_NOT_INSERVICE = 8,
  CANNOT_EXECUTE_DUE_TO_SERVER_PERMANENT_OFFLINE = 9,
  CANNOT_PERSIST_TASK_DUE_TO_CLONE_CONFLICT = 10,
  MAX
};

class ObDRTaskQueue;
const char *ob_disaster_recovery_task_type_strs(const rootserver::ObDRTaskType type);
int parse_disaster_recovery_task_type_from_string(const ObString &task_type_str, rootserver::ObDRTaskType& task_type);
const char *ob_disaster_recovery_task_priority_strs(const rootserver::ObDRTaskPriority task_priority);
const char* ob_disaster_recovery_task_ret_comment_strs(const rootserver::ObDRTaskRetComment ret_comment);
bool is_manual_dr_task_data_version_match(uint64_t tenant_data_version);
int build_execute_result(
    const int ret_code,
    const ObDRTaskRetComment &ret_comment,
    const int64_t start_time,
    ObSqlString &execute_result);

class ObDstReplica
{
public:
  ObDstReplica() : unit_id_(common::OB_INVALID_ID),
                   unit_group_id_(common::OB_INVALID_ID),
                   zone_(),
                   member_() {}
public:
  int assign(
      const uint64_t unit_id,
      const uint64_t unit_group_id,
      const common::ObZone &zone,
      const common::ObReplicaMember &member);
  int assign(
      const ObDstReplica &that);
public:
  bool is_valid() const {
    return unit_id_ != OB_INVALID_ID
           && unit_group_id_ != OB_INVALID_ID
           && member_.is_valid();
  }
  TO_STRING_KV(K_(unit_id),
               K_(unit_group_id),
               K_(zone),
               K_(member));
  const common::ObAddr &get_server() const { return member_.get_server(); }
  common::ObReplicaType get_replica_type() const { return member_.get_replica_type(); }
  uint64_t get_unit_id() const { return unit_id_; }
  uint64_t get_unit_group_id() const { return unit_group_id_; }
  const common::ObZone &get_zone() const { return zone_; }
  int64_t get_memstore_percent() const { return member_.get_memstore_percent(); }
  const common::ObReplicaMember &get_member() const { return member_; }
public:
  void reset();
private:
  uint64_t unit_id_;
  uint64_t unit_group_id_;
  common::ObZone zone_;
  common::ObReplicaMember member_;
};

class ObDRTaskMgr;

enum class ObDRTaskType : int64_t
{
  LS_MIGRATE_REPLICA = 0,
  LS_ADD_REPLICA,
  LS_BUILD_ONLY_IN_MEMBER_LIST,
  LS_TYPE_TRANSFORM,
  LS_REMOVE_PAXOS_REPLICA,
  LS_REMOVE_NON_PAXOS_REPLICA,
  LS_MODIFY_PAXOS_REPLICA_NUMBER,
  MAX_TYPE,
};

class ObDRTaskKey
{
public:
  ObDRTaskKey() : tenant_id_(OB_INVALID_TENANT_ID),
                  ls_id_(),
                  task_execute_zone_(),
                  task_type_(ObDRTaskType::MAX_TYPE) {}
  virtual ~ObDRTaskKey() {}
public:
  void reset();
  bool is_valid() const;
  bool operator==(const ObDRTaskKey &that) const;
  int init(const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const common::ObZone &task_execute_zone,
           const ObDRTaskType &task_type);
  int assign(const ObDRTaskKey &that);
  ObDRTaskKey& operator=(const ObDRTaskKey&) = delete;
  TO_STRING_KV(K_(tenant_id),
               K_(ls_id),
               K_(task_execute_zone),
               K_(task_type));

  uint64_t get_tenant_id() const { return tenant_id_; }
  const share::ObLSID &get_ls_id() const { return ls_id_; }
  const common::ObZone &get_zone() const { return task_execute_zone_; }
  const ObDRTaskType &get_task_type() const { return task_type_; }
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObZone task_execute_zone_;
  ObDRTaskType task_type_;
};

enum class ObDRTaskPriority : int64_t 
{
  HIGH_PRI = 0,
  LOW_PRI,
  MAX_PRI,
};

/* this ObDRTask is the base class,
 * derived classes include Migrate/Add/Type transform/Remove and so on
 */
class ObDRTask : public common::ObDLinkBase<ObDRTask>
{

public:
  ObDRTask() : task_key_(),
               tenant_id_(common::OB_INVALID_ID),
               ls_id_(),
               cluster_id_(-1),
               transmit_data_size_(0),
               invoked_source_(obrpc::ObAdminClearDRTaskArg::TaskType::AUTO),
               generate_time_(common::ObTimeUtility::current_time()),
               priority_(ObDRTaskPriority::MAX_PRI),
               comment_("DRTask"),
               schedule_time_(0),
               execute_time_(0),
               task_id_() {}
  virtual ~ObDRTask() {}
public:
  bool is_valid() const {
    return !task_id_.is_invalid()
           && task_key_.is_valid()
           && OB_INVALID_ID != tenant_id_
           && ls_id_.is_valid();
  }
  // operations of task_id_
  const share::ObTaskId &get_task_id() const { return task_id_; }
public:
  int build(
      const ObDRTaskKey &task_key,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const share::ObTaskId &task_id,
      const int64_t schedule_time_us,
      const int64_t generate_time_us,
      const int64_t cluster_id,
      const int64_t transmit_data_size,
      const obrpc::ObAdminClearDRTaskArg::TaskType invoked_source,
      const ObDRTaskPriority priority,
      const ObString &comment);

public:
  virtual const common::ObAddr &get_dst_server() const = 0;

  virtual ObDRTaskType get_disaster_recovery_task_type() const = 0;
  /* rebalance task mgr related virtual interface
   */
  virtual int get_execute_transmit_size(int64_t &execute_transmit_size) const = 0;
  /* rebalance task executor related virtual interface
   */

  /* all_virtual_rebalance_task_stat pure interface
   */
  virtual int get_virtual_disaster_recovery_task_stat(
      common::ObAddr &src,
      common::ObAddr &data_src,
      common::ObAddr &dest,
      common::ObAddr &offline) const = 0;
  /* rootservice_event_history related pure interface
   */
  virtual int log_execute_start() const = 0;
  virtual int log_execute_result(const int ret_code, const ObDRTaskRetComment &ret_comment) const = 0;
  /* execute related
   */

  virtual int check_before_execute(
      share::ObLSTableOperator &lst_operator,
      ObDRTaskRetComment &ret_comment) const = 0;

  virtual int execute(
      obrpc::ObSrvRpcProxy &rpc_proxy,
      int &ret_code,
      ObDRTaskRetComment &ret_comment) const = 0;

  virtual int fill_dml_splicer(
      share::ObDMLSqlSplicer &dml_splicer) const;

  int fill_dml_splicer_for_new_column(
      share::ObDMLSqlSplicer &dml_splicer,
      const common::ObAddr &force_data_src) const;
  // to string
  virtual TO_STRING_KV(K_(task_key),
                       K_(tenant_id),
                       K_(ls_id),
                       K_(cluster_id),
                       K_(transmit_data_size),
                       K_(invoked_source),
                       K_(generate_time),
                       K_(priority),
                       K_(comment),
                       K_(schedule_time),
                       K_(execute_time),
                       K_(task_id));
public:
  bool is_already_timeout() const;
  // operations of task_key_
  const ObDRTaskKey &get_task_key() const { return task_key_; }
  int set_task_key(
      const ObDRTaskKey &task_key);
  // operations of tenant_id
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  // operations of ls_id
  const share::ObLSID &get_ls_id() const { return ls_id_; }
  void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }
  // operations of cluster_id
  int64_t get_cluster_id() const { return cluster_id_; }
  void set_cluster_id(const int64_t cluster_id) { cluster_id_ = cluster_id; }
  // operations of transmit_data_size
  int64_t get_transmit_data_size() const { return transmit_data_size_; }
  void set_transmit_data_size(const int64_t size) { transmit_data_size_ = size; }
  // operations of invoked_source_
  obrpc::ObAdminClearDRTaskArg::TaskType get_invoked_source() const { return invoked_source_; }
  void set_invoked_source(obrpc::ObAdminClearDRTaskArg::TaskType t) { invoked_source_ = t; }
  // operations of generate_time_
  int64_t get_generate_time() const { return generate_time_; }
  void set_generate_time(const int64_t generate_time) { generate_time_ = generate_time; }
  // operations of priority_
  ObDRTaskPriority get_priority() const { return priority_; }
  void set_priority(ObDRTaskPriority priority) { priority_ = priority; }
  bool is_high_priority_task() const { return ObDRTaskPriority::HIGH_PRI == priority_; }
  bool is_low_priority_task() const { return ObDRTaskPriority::LOW_PRI == priority_; }
  // operations of comments
  ObString get_comment() const { return comment_.string(); }
  int set_comment(const ObString comment) { return comment_.assign(comment); }
  virtual const char* get_log_start_str() const = 0;
  virtual const char* get_log_finish_str() const = 0;
  // operations of schedule_time_
  int64_t get_schedule_time() const { return schedule_time_; }
  void set_schedule_time(const int64_t schedule_time) { schedule_time_ = schedule_time; }
  bool in_schedule() const { return schedule_time_ > 0; }
  // operations of execute_time_
  int64_t get_execute_time() const { return execute_time_; }
  void set_execute_time(const int64_t execute_time) { execute_time_ = execute_time; }
public:
  virtual int64_t get_clone_size() const = 0;
  virtual int clone(void *input_ptr, ObDRTask *&output_task) const = 0;
  int deep_copy(const ObDRTask &that);
public:
  void set_schedule();
  bool is_manual_task() const { return obrpc::ObAdminClearDRTaskArg::TaskType::MANUAL == invoked_source_; }
public:
  /* disallow copy constructor and operator= */
  ObDRTask(const ObDRTask &) = delete;
  ObDRTask &operator=(const ObDRTask &) = delete;
protected:
  ObDRTaskKey task_key_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int64_t cluster_id_;
  /* transmit_data_size_ is the data transmission volumn when this task info is executed,
   * when a migrate/add task is executed, transmit_data_size_ is the data size of the replica,
   * when a paxos replica number modification/replica type transform task is executed, no data needs to be
   * transmitted, so the tranmit_data_size_ is set to zero.
   */
  int64_t transmit_data_size_;
  obrpc::ObAdminClearDRTaskArg::TaskType invoked_source_;
  int64_t generate_time_;
  ObDRTaskPriority priority_;
  ObSqlString comment_;
  int64_t schedule_time_;
  int64_t execute_time_;
  share::ObTaskId task_id_;
};

class ObMigrateLSReplicaTask : public ObDRTask
{
public:
  ObMigrateLSReplicaTask() : ObDRTask(),
                             dst_replica_(),
                             src_member_(),
                             data_src_member_(),
                             force_data_src_member_(),
                             paxos_replica_number_(0),
                             prioritize_same_zone_src_(false) {}
  virtual ~ObMigrateLSReplicaTask() {}
public:
  int build(
      const ObDRTaskKey &task_key,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const share::ObTaskId &task_id,
      const int64_t schedule_time_us,
      const int64_t generate_time_us,
      const int64_t cluster_id,
      const int64_t transmit_data_size,
      const obrpc::ObAdminClearDRTaskArg::TaskType invoked_source,
      const ObDRTaskPriority priority,
      const ObString &comment,
      const ObDstReplica &dst_replica,
      const common::ObReplicaMember &src_member,
      const common::ObReplicaMember &data_src_member,
      const common::ObReplicaMember &force_data_src_member,
      const int64_t paxos_replica_number);

  // only use some necessary information build a ObMigrateLSReplicaTask
  // Specifically, this method is only used when manually executing operation and maintenance commands
  int simple_build(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const share::ObTaskId &task_id,
      const ObDstReplica &dst_replica,
      const common::ObReplicaMember &src_member,
      const common::ObReplicaMember &data_src_member,
      const common::ObReplicaMember &force_data_src_member,
      const int64_t paxos_replica_number);
  // build a ObMigrateLSReplicaTask from sql result read from inner table
  // @param [in] res, sql result read from inner table
  int build_task_from_sql_result(const sqlclient::ObMySQLResult &res);
public:
  virtual const common::ObAddr &get_dst_server() const override {
    return dst_replica_.get_server();
  }

  virtual ObDRTaskType get_disaster_recovery_task_type() const override {
    return ObDRTaskType::LS_MIGRATE_REPLICA;
  }

  virtual INHERIT_TO_STRING_KV("ObDRTask", ObDRTask,
                               K(dst_replica_),
                               K(src_member_),
                               K(data_src_member_),
                               K(force_data_src_member_),
                               K(paxos_replica_number_),
                               K(prioritize_same_zone_src_));

  virtual int get_execute_transmit_size(
      int64_t &execute_transmit_size) const override;

  virtual int get_virtual_disaster_recovery_task_stat(
      common::ObAddr &src,
      common::ObAddr &data_src,
      common::ObAddr &dest,
      common::ObAddr &offline) const override;
  
  virtual int log_execute_start() const override;

  virtual int log_execute_result(const int ret_code, const ObDRTaskRetComment &ret_comment) const override;

  virtual int check_before_execute(
      share::ObLSTableOperator &lst_operator,
      ObDRTaskRetComment &ret_comment) const override;

  virtual int execute(
      obrpc::ObSrvRpcProxy &rpc_proxy,
      int &ret_code,
      ObDRTaskRetComment &ret_comment) const override;

  virtual int fill_dml_splicer(
      share::ObDMLSqlSplicer &dml_splicer) const override;

  virtual const char* get_log_start_str() const override { return drtasklog::START_MIGRATE_LS_REPLICA_STR; }
  virtual const char* get_log_finish_str() const override { return drtasklog::FINISH_MIGRATE_LS_REPLICA_STR; }
  virtual int64_t get_clone_size() const override;
  virtual int clone(
      void *input_ptr,
      ObDRTask *&output_task) const override;
public:
  bool get_prioritize_same_zone_src() const { return prioritize_same_zone_src_; };
  void set_prioritize_same_zone_src(bool p) { prioritize_same_zone_src_ = p; };
  // operations of dst_replica_
  int set_dst_replica(
      const ObDstReplica &that);
  int set_dst_replica(
      const uint64_t unit_id,
      const uint64_t unit_group_id,
      const common::ObZone &zone,
      const common::ObReplicaMember &member);
  const ObDstReplica &get_dst_replica() const { return dst_replica_; }
  // operations of src_member_
  void set_src_member(const common::ObReplicaMember &s) { src_member_ = s; }
  const common::ObReplicaMember &get_src_member() const { return src_member_; }
  // operations of data_src_member_;
  void set_data_src_member(const common::ObReplicaMember &s) { data_src_member_ = s; }
  const common::ObReplicaMember &get_data_src_member() const { return data_src_member_; }
  void set_force_data_src_member(const common::ObReplicaMember &s) { force_data_src_member_ = s; }
  const common::ObReplicaMember &get_force_data_src_member() const { return force_data_src_member_; }
  // operations of paxos_replica_number_
  void set_paxos_replica_number(const int64_t paxos_replica_number) { paxos_replica_number_ = paxos_replica_number; }
  int64_t get_paxos_replica_number() const { return paxos_replica_number_; }
private:
  int check_paxos_number(
      const share::ObLSInfo &ls_info,
      ObDRTaskRetComment &ret_comment) const;

  int check_online(
      const share::ObLSInfo &ls_info,
      ObDRTaskRetComment &ret_comment) const;
private:
  ObDstReplica dst_replica_;
  common::ObReplicaMember src_member_;
  common::ObReplicaMember data_src_member_;
  common::ObReplicaMember force_data_src_member_;
  int64_t paxos_replica_number_;
  bool prioritize_same_zone_src_;
};

class ObAddLSReplicaTask : public ObDRTask
{
public:
  ObAddLSReplicaTask() : ObDRTask(),
                         dst_replica_(),
                         data_src_member_(),
                         force_data_src_member_(),
                         orig_paxos_replica_number_(0),
                         paxos_replica_number_(0) {}
  virtual ~ObAddLSReplicaTask() {}
public:
  int build(
      const ObDRTaskKey &task_key,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const share::ObTaskId &task_id,
      const int64_t schedule_time_us,
      const int64_t generate_time_us,
      const int64_t cluster_id,
      const int64_t transmit_data_size,
      const obrpc::ObAdminClearDRTaskArg::TaskType invoked_source,
      const ObDRTaskPriority priority,
      const ObString &comment,
      const ObDstReplica &dst_replica_,
      const common::ObReplicaMember &data_src_member,
      const common::ObReplicaMember &force_data_src_member,
      const int64_t orig_paxos_replica_number,
      const int64_t paxos_replica_number);

  // only use some necessary information build a ObAddLSReplicaTask
  // Specifically, this method is only used when manually executing operation and maintenance commands
  int simple_build(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const share::ObTaskId &task_id,
      const ObDstReplica &dst_replica,
      const common::ObReplicaMember &data_src_member,
      const common::ObReplicaMember &force_data_src_member,
      const int64_t orig_paxos_replica_number,
      const int64_t paxos_replica_number);
  // build a ObAddLSReplicaTask from sql result read from inner table
  // @param [in] res, sql result read from inner table
  int build_task_from_sql_result(const sqlclient::ObMySQLResult &res);
public:
  virtual const common::ObAddr &get_dst_server() const override {
    return dst_replica_.get_server();
  }

  virtual ObDRTaskType get_disaster_recovery_task_type() const override {
    return ObDRTaskType::LS_ADD_REPLICA;
  }

  virtual INHERIT_TO_STRING_KV("ObDRTask", ObDRTask,
                               K(dst_replica_),
                               K(data_src_member_),
                               K(force_data_src_member_),
                               K(orig_paxos_replica_number_),
                               K(paxos_replica_number_));
  virtual int get_execute_transmit_size(
      int64_t &execute_transmit_size) const override;

  virtual int get_virtual_disaster_recovery_task_stat(
      common::ObAddr &src,
      common::ObAddr &data_src,
      common::ObAddr &dest,
      common::ObAddr &offline) const override;
  
  virtual int log_execute_start() const override;

  virtual int log_execute_result(const int ret_code, const ObDRTaskRetComment &ret_comment) const override;

  virtual int check_before_execute(
      share::ObLSTableOperator &lst_operator,
      ObDRTaskRetComment &ret_comment) const override;

  virtual int execute(
      obrpc::ObSrvRpcProxy &rpc_proxy,
      int &ret_code,
      ObDRTaskRetComment &ret_comment) const override;

  virtual int fill_dml_splicer(
      share::ObDMLSqlSplicer &dml_splicer) const override;

  virtual const char* get_log_start_str() const override { return drtasklog::START_ADD_LS_REPLICA_STR; }
  virtual const char* get_log_finish_str() const override { return drtasklog::FINISH_ADD_LS_REPLICA_STR; }
  virtual int64_t get_clone_size() const override;
  virtual int clone(
      void *input_ptr,
      ObDRTask *&output_task) const override;
public:
  // operations of dst_replica_
  int set_dst_replica(
      const ObDstReplica &that);
  int set_dst_replica(
      const uint64_t unit_id,
      const uint64_t unit_group_id,
      const common::ObZone &zone,
      const common::ObReplicaMember &member);
  const ObDstReplica &get_dst_replica() const { return dst_replica_; }
  // operations of data_src_member_;
  void set_data_src_member(const common::ObReplicaMember &s) { data_src_member_ = s; }
  const common::ObReplicaMember &get_data_src_member() const { return data_src_member_; }
  void set_force_data_src_member(const common::ObReplicaMember &s) { force_data_src_member_ = s; }
  const common::ObReplicaMember &get_force_data_src_member() const { return force_data_src_member_; }
  // operations of orig_paxos_replica_number_
  void set_orig_paxos_replica_number(const int64_t paxos_replica_number) { orig_paxos_replica_number_ = paxos_replica_number; }
  int64_t get_orig_paxos_replica_number() const { return orig_paxos_replica_number_; }
  // operations of paxos_replica_number_
  void set_paxos_replica_number(const int64_t paxos_replica_number) { paxos_replica_number_ = paxos_replica_number; }
  int64_t get_paxos_replica_number() const { return paxos_replica_number_; }
private:
  int check_online(
      const share::ObLSInfo &ls_info,
      ObDRTaskRetComment &ret_comment) const;

  int check_paxos_member(
      const share::ObLSInfo &ls_info,
      ObDRTaskRetComment &ret_comment) const;
private:
  ObDstReplica dst_replica_;
  common::ObReplicaMember data_src_member_;
  common::ObReplicaMember force_data_src_member_;
  int64_t orig_paxos_replica_number_;
  int64_t paxos_replica_number_;
};

class ObLSTypeTransformTask : public ObDRTask
{
public:
  ObLSTypeTransformTask() : ObDRTask(),
                            dst_replica_(),
                            src_member_(),
                            data_src_member_(),
                            orig_paxos_replica_number_(0),
                            paxos_replica_number_(0) {}
  virtual ~ObLSTypeTransformTask() {}
public:
  int build(
      const ObDRTaskKey &task_key,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const share::ObTaskId &task_id,
      const int64_t schedule_time_us,
      const int64_t generate_time_us,
      const int64_t cluster_id,
      const int64_t transmit_data_size,
      const obrpc::ObAdminClearDRTaskArg::TaskType invoked_source,
      const ObDRTaskPriority priority,
      const ObString &comment,
      const ObDstReplica &dst_replica_,
      const common::ObReplicaMember &src_member,
      const common::ObReplicaMember &data_src_member,
      const int64_t orig_paxos_replica_number,
      const int64_t paxos_replica_number);

  // only use some necessary information build a ObLSTypeTransformTask
  // Specifically, this method is only used when manually executing operation and maintenance commands
  int simple_build(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const share::ObTaskId &task_id,
      const ObDstReplica &dst_replica,
      const common::ObReplicaMember &src_member,
      const common::ObReplicaMember &data_src_member,
      const int64_t orig_paxos_replica_number,
      const int64_t paxos_replica_number);
  // build a ObLSTypeTransformTask from sql result read from inner table
  // @param [in] res, sql result read from inner table
  int build_task_from_sql_result(const sqlclient::ObMySQLResult &res);
public:
  virtual const common::ObAddr &get_dst_server() const override {
    return dst_replica_.get_server();
  }

  virtual ObDRTaskType get_disaster_recovery_task_type() const override {
    return ObDRTaskType::LS_TYPE_TRANSFORM;
  }
  virtual INHERIT_TO_STRING_KV("ObDRTask", ObDRTask,
                               K(dst_replica_),
                               K(src_member_),
                               K(data_src_member_),
                               K(orig_paxos_replica_number_),
                               K(paxos_replica_number_));

  virtual int get_execute_transmit_size(
      int64_t &execute_transmit_size) const override;

  virtual int get_virtual_disaster_recovery_task_stat(
      common::ObAddr &src,
      common::ObAddr &data_src,
      common::ObAddr &dest,
      common::ObAddr &offline) const override;
  
  virtual int log_execute_start() const override;

  virtual int log_execute_result(const int ret_code, const ObDRTaskRetComment &ret_comment) const override;

  virtual int check_before_execute(
      share::ObLSTableOperator &lst_operator,
      ObDRTaskRetComment &ret_comment) const override;

  virtual int execute(
      obrpc::ObSrvRpcProxy &rpc_proxy,
      int &ret_code,
      ObDRTaskRetComment &ret_comment) const override;

  virtual int fill_dml_splicer(
      share::ObDMLSqlSplicer &dml_splicer) const override;

  virtual const char* get_log_start_str() const override { return drtasklog::START_TYPE_TRANSFORM_LS_REPLICA_STR; }
  virtual const char* get_log_finish_str() const override { return drtasklog::FINISH_TYPE_TRANSFORM_LS_REPLICA_STR; }
  virtual int64_t get_clone_size() const override;
  virtual int clone(
      void *input_ptr,
      ObDRTask *&output_task) const override;
public:
  // operations of dst_replica_
  int set_dst_replica(
      const ObDstReplica &that);
  int set_dst_replica(
      const uint64_t unit_id,
      const uint64_t unit_group_id,
      const common::ObZone &zone,
      const common::ObReplicaMember &member);
  const ObDstReplica &get_dst_replica() const { return dst_replica_; }
  // operations of src_member_
  void set_src_member(const common::ObReplicaMember &s) { src_member_ = s; }
  const common::ObReplicaMember &get_src_member() const { return src_member_; }
  // operations of data_src_member_;
  void set_data_src_member(const common::ObReplicaMember &s) { data_src_member_ = s; }
  const common::ObReplicaMember &get_data_src_member() const { return data_src_member_; }
  // operations of orig_paxos_replica_number_
  void set_orig_paxos_replica_number(const int64_t paxos_replica_number) { orig_paxos_replica_number_ = paxos_replica_number; }
  int64_t get_orig_paxos_replica_number() const { return orig_paxos_replica_number_; }
  // operations of paxos_replica_number_
  void set_paxos_replica_number(const int64_t paxos_replica_number) { paxos_replica_number_ = paxos_replica_number; }
  int64_t get_paxos_replica_number() const { return paxos_replica_number_; }
private:
  int check_online(
      const share::ObLSInfo &ls_info,
      ObDRTaskRetComment &ret_comment) const;

  int check_paxos_member(
      const share::ObLSInfo &ls_info,
      ObDRTaskRetComment &ret_comment) const;
private:
  ObDstReplica dst_replica_;
  common::ObReplicaMember src_member_;
  common::ObReplicaMember data_src_member_;
  int64_t orig_paxos_replica_number_;
  int64_t paxos_replica_number_;
};

class ObRemoveLSReplicaTask : public ObDRTask
{
public:
  ObRemoveLSReplicaTask() : ObDRTask(),
                                 leader_(),
                                 remove_server_(),
                                 orig_paxos_replica_number_(0),
                                 paxos_replica_number_(0),
                                 replica_type_(REPLICA_TYPE_FULL) {}
  virtual ~ObRemoveLSReplicaTask() {}
public:
  int build(
      const ObDRTaskKey &task_key,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const share::ObTaskId &task_id,
      const int64_t schedule_time_us,
      const int64_t generate_time_us,
      const int64_t cluster_id,
      const int64_t transmit_data_size,
      const obrpc::ObAdminClearDRTaskArg::TaskType invoked_source,
      const ObDRTaskPriority priority,
      const ObString &comment,
      const common::ObAddr &leader,
      const common::ObReplicaMember &remove_server,
      const int64_t orig_paxos_replica_number,
      const int64_t paxos_replica_number,
      const ObReplicaType &replica_type);

  // only use some necessary information build a ObRemoveLSReplicaTask
  // Specifically, this method is only used when manually executing operation and maintenance commands
  int simple_build(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const share::ObTaskId &task_id,
      const common::ObAddr &leader,
      const common::ObReplicaMember &remove_server,
      const int64_t orig_paxos_replica_number,
      const int64_t paxos_replica_number,
      const ObReplicaType &replica_type);

  // build a ObRemoveLSReplicaTask from sql result read from inner table
  // @param [in] res, sql result read from inner table
  int build_task_from_sql_result(const sqlclient::ObMySQLResult &res);
public:
  virtual const common::ObAddr &get_dst_server() const override {
    return leader_;
  }

  virtual ObDRTaskType get_disaster_recovery_task_type() const override {
    return ObReplicaTypeCheck::is_paxos_replica_V2(replica_type_)
           ? ObDRTaskType::LS_REMOVE_PAXOS_REPLICA
           : ObDRTaskType::LS_REMOVE_NON_PAXOS_REPLICA;
  }

  virtual INHERIT_TO_STRING_KV("ObDRTask", ObDRTask,
                               K(leader_),
                               K(remove_server_),
                               K(orig_paxos_replica_number_),
                               K(paxos_replica_number_),
                               K(replica_type_));

  virtual int get_execute_transmit_size(
      int64_t &execute_transmit_size) const override;

  virtual int get_virtual_disaster_recovery_task_stat(
      common::ObAddr &src,
      common::ObAddr &data_src,
      common::ObAddr &dest,
      common::ObAddr &offline) const override;

  virtual int log_execute_start() const override;

  virtual int log_execute_result(const int ret_code, const ObDRTaskRetComment &ret_comment) const override;

  virtual int check_before_execute(
      share::ObLSTableOperator &lst_operator,
      ObDRTaskRetComment &ret_comment) const override;

  virtual int execute(
      obrpc::ObSrvRpcProxy &rpc_proxy,
      int &ret_code,
      ObDRTaskRetComment &ret_comment) const override;

  virtual int fill_dml_splicer(
      share::ObDMLSqlSplicer &dml_splicer) const override;

  virtual const char* get_log_start_str() const override
  {
    return  ObDRTaskType::LS_REMOVE_PAXOS_REPLICA == get_disaster_recovery_task_type()
            ? drtasklog::START_REMOVE_LS_PAXOS_REPLICA_STR
            : drtasklog::START_REMOVE_LS_NON_PAXOS_REPLICA_STR;
  }
  virtual const char* get_log_finish_str() const override
  {
    return  ObDRTaskType::LS_REMOVE_PAXOS_REPLICA == get_disaster_recovery_task_type()
            ? drtasklog::FINISH_REMOVE_LS_PAXOS_REPLICA_STR
            : drtasklog::FINISH_REMOVE_LS_NON_PAXOS_REPLICA_STR;
  }

  virtual int64_t get_clone_size() const override;
  virtual int clone(
      void *input_ptr,
      ObDRTask *&output_task) const override;
public:
  // operations of leader_
  void set_leader(const common::ObAddr &l) { leader_ = l; };
  const common::ObAddr &get_leader() const { return leader_; }
  // operations of remove_server_
  void set_remove_server(const common::ObReplicaMember &d) { remove_server_ = d; }
  const common::ObReplicaMember &get_remove_server() const { return remove_server_; }
  // operations of orig_paxos_replica_number_
  void set_orig_paxos_replica_number(const int64_t q) { orig_paxos_replica_number_ = q; }
  int64_t get_orig_paxos_replica_number() const { return orig_paxos_replica_number_; }
  // operations of paxos_replica_number_
  void set_paxos_replica_number(const int64_t q) { paxos_replica_number_ = q; }
  int64_t get_paxos_replica_number() const { return paxos_replica_number_; }
  // operations of replica_type_
  void set_replica_type(const ObReplicaType &replica_type) { replica_type_ = replica_type; }
  const ObReplicaType &get_replica_type() const { return replica_type_; }
private:
  common::ObAddr leader_;
  common::ObReplicaMember remove_server_;
  int64_t orig_paxos_replica_number_;
  int64_t paxos_replica_number_;
  ObReplicaType replica_type_;
};

class ObLSModifyPaxosReplicaNumberTask : public ObDRTask
{
public:
  ObLSModifyPaxosReplicaNumberTask() : ObDRTask(),
                           server_(),
                           orig_paxos_replica_number_(),
                           paxos_replica_number_(),
                           member_list_() {}
  virtual ~ObLSModifyPaxosReplicaNumberTask() {}
public:
  int build(
      const ObDRTaskKey &task_key,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const share::ObTaskId &task_id,
      const int64_t schedule_time_us,
      const int64_t generate_time_us,
      const int64_t cluster_id,
      const int64_t transmit_data_size,
      const obrpc::ObAdminClearDRTaskArg::TaskType invoked_source,
      const ObDRTaskPriority priority,
      const ObString &comment,
      const common::ObAddr &dst_server,
      const int64_t orig_paxos_replica_number,
      const int64_t paxos_replica_number,
      const common::ObMemberList &member_list);

  // only use some necessary information build a ObLSModifyPaxosReplicaNumberTask
  // Specifically, this method is only used when manually executing operation and maintenance commands
  int simple_build(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const share::ObTaskId &task_id,
      const common::ObAddr &dst_server,
      const int64_t orig_paxos_replica_number,
      const int64_t paxos_replica_number,
      const common::ObMemberList &member_list);
  // build a ObLSModifyPaxosReplicaNumberTask from sql result read from inner table
  // @param [in] res, sql result read from inner table
  int build_task_from_sql_result(const sqlclient::ObMySQLResult &res);
public:
  virtual const common::ObAddr &get_dst_server() const override {
    return server_;
  }

  virtual ObDRTaskType get_disaster_recovery_task_type() const override {
    return ObDRTaskType::LS_MODIFY_PAXOS_REPLICA_NUMBER;
  }

  virtual INHERIT_TO_STRING_KV("ObDRTask", ObDRTask,
                               K(server_),
                               K(orig_paxos_replica_number_),
                               K(paxos_replica_number_));

  virtual int get_execute_transmit_size(
      int64_t &execute_transmit_size) const override;

  virtual int get_virtual_disaster_recovery_task_stat(
      common::ObAddr &src,
      common::ObAddr &data_src,
      common::ObAddr &dest,
      common::ObAddr &offline) const override;
  
  virtual int log_execute_start() const override;

  virtual int log_execute_result(const int ret_code, const ObDRTaskRetComment &ret_comment) const override;

  virtual int check_before_execute(
      share::ObLSTableOperator &lst_operator,
      ObDRTaskRetComment &ret_comment) const override;

  virtual int execute(
      obrpc::ObSrvRpcProxy &rpc_proxy,
      int &ret_code,
      ObDRTaskRetComment &ret_comment) const override;

  virtual int fill_dml_splicer(
      share::ObDMLSqlSplicer &dml_splicer) const override;

  virtual const char* get_log_start_str() const override { return drtasklog::START_MODIFY_PAXOS_REPLICA_NUMBER_STR; }
  virtual const char* get_log_finish_str() const override { return drtasklog::FINISH_MODIFY_PAXOS_REPLICA_NUMBER_STR; }
  virtual int64_t get_clone_size() const override;
  virtual int clone(
      void *input_ptr,
      ObDRTask *&output_task) const override;
public:
  // operations of server_
  void set_server(const common::ObAddr &d) { server_ = d; }
  const common::ObAddr &get_server() const { return server_; }
  // operations of orig_paxos_replica_number_
  void set_orig_paxos_replica_number(const int64_t orig_paxos_replica_number) { orig_paxos_replica_number_ = orig_paxos_replica_number; }
  int64_t get_orig_paxos_replica_number() const { return orig_paxos_replica_number_; }
  // operations of paxos_replica_number_
  void set_paxos_replica_number(const int64_t paxos_replica_number) { paxos_replica_number_ = paxos_replica_number; }
  int64_t get_paxos_replica_number() const { return paxos_replica_number_; }
  // operations of member_list_
  void set_member_list(const common::ObMemberList &that) { member_list_ = that; }
  const common::ObMemberList &get_member_list() const { return member_list_; }

private:
  common::ObAddr server_;
  int64_t orig_paxos_replica_number_;
  int64_t paxos_replica_number_;
  common::ObMemberList member_list_;
};
} // end namespace rootserver
} // end namespace oceanbase
#endif // OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_H_
