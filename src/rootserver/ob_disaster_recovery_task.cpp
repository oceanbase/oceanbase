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

#define USING_LOG_PREFIX RS


#include "ob_disaster_recovery_task.h"
#include "observer/ob_server.h"
#include "observer/omt/ob_tenant_timezone_mgr.h" //for OTTZ_MGR

namespace oceanbase
{
using namespace common;
using namespace lib;
using namespace obrpc;
using namespace share;
using namespace share::schema;
namespace rootserver
{

#define READ_TASK_FROM_SQL_RES_FOR_DISASTER_RECOVERY                                                                        \
  uint64_t tenant_id = OB_INVALID_TENANT_ID;                                                                                \
  int64_t ls_id = ObLSID::INVALID_LS_ID;                                                                                    \
  common::ObString task_type_str;                                                                                           \
  common::ObString task_id;                                                                                                 \
  int64_t priority = 2;                                                                                                     \
  common::ObString dest_ip;                                                                                                 \
  int64_t dest_port = OB_INVALID_INDEX;                                                                                     \
  common::ObString src_ip;                                                                                                  \
  int64_t src_port = OB_INVALID_INDEX;                                                                                      \
  int64_t src_paxos_replica_number = OB_INVALID_COUNT;                                                                      \
  int64_t dest_paxos_replica_number = OB_INVALID_COUNT;                                                                     \
  common::ObString src_type_str;                                                                                            \
  common::ObString dest_type_str;                                                                                           \
  ObReplicaType src_type_to_set = REPLICA_TYPE_INVALID;                                                                     \
  ObReplicaType dest_type_to_set = REPLICA_TYPE_INVALID;                                                                    \
  common::ObString execute_ip;                                                                                              \
  int64_t execute_port = OB_INVALID_INDEX;                                                                                  \
  int64_t schedule_time_us = 0;                                                                                             \
  int64_t generate_time_us = 0;                                                                                             \
  common::ObString comment;                                                                                                 \
  int64_t data_source_port = 0;                                                                                             \
  common::ObString data_source_ip;                                                                                          \
  bool is_manual = false;                                                                                                   \
  (void)GET_COL_IGNORE_NULL(res.get_int, "ls_id", ls_id);                                                                   \
  EXTRACT_INT_FIELD_MYSQL(res, "tenant_id", tenant_id, uint64_t);                                                           \
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "task_type", task_type_str);                                                   \
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "task_id", task_id);                                                           \
  (void)GET_COL_IGNORE_NULL(res.get_int, "priority", priority);                                                             \
  (void)GET_COL_IGNORE_NULL(res.get_int, "source_paxos_replica_number", src_paxos_replica_number);                          \
  (void)GET_COL_IGNORE_NULL(res.get_int, "target_paxos_replica_number", dest_paxos_replica_number);                         \
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "source_replica_type", src_type_str);                                          \
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "target_replica_type", dest_type_str);                                         \
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "source_replica_svr_ip", src_ip);                                              \
  (void)GET_COL_IGNORE_NULL(res.get_int, "source_replica_svr_port", src_port);                                              \
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "target_replica_svr_ip", dest_ip);                                             \
  (void)GET_COL_IGNORE_NULL(res.get_int, "target_replica_svr_port", dest_port);                                             \
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "task_exec_svr_ip", execute_ip);                                               \
  (void)GET_COL_IGNORE_NULL(res.get_int, "task_exec_svr_port", execute_port);                                               \
  {                                                                                                                         \
    ObTimeZoneInfoWrap tz_info_wrap;                                                                                        \
    ObTZMapWrap tz_map_wrap;                                                                                                \
    OZ(OTTZ_MGR.get_tenant_tz(tenant_id, tz_map_wrap));                                                                     \
    tz_info_wrap.set_tz_info_map(tz_map_wrap.get_tz_map());                                                                 \
    (void)GET_COL_IGNORE_NULL(res.get_timestamp, "generate_time", tz_info_wrap.get_time_zone_info(), generate_time_us);     \
    (void)GET_COL_IGNORE_NULL(res.get_timestamp, "schedule_time", tz_info_wrap.get_time_zone_info(), schedule_time_us);     \
  }                                                                                                                         \
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "comment", comment);                                                           \
  EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(res, "data_source_svr_port", data_source_port,                                 \
    int64_t, true/*skip null error*/, true/*skip column error*/, 0);                                                        \
  EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(res, "data_source_svr_ip", data_source_ip,                                 \
    true/*skip null error*/, true/*skip column error*/, "0.0.0.0");                                                         \
  EXTRACT_BOOL_FIELD_MYSQL_SKIP_RET(res, "is_manual", is_manual);                                                           \
  ObDRTaskKey task_key;                                                                                                     \
  common::ObAddr execute_server_key;                                                                                        \
  common::ObZone execute_zone;                                                                                              \
  rootserver::ObDRTaskType task_type = rootserver::ObDRTaskType::MAX_TYPE;                                                  \
  ObSqlString comment_to_set;                                                                                               \
  ObSqlString task_id_sqlstring_format;                                                                                     \
  share::ObTaskId task_id_to_set;                                                                                           \
  rootserver::ObDRTaskPriority priority_to_set;                                                                             \
  if (OB_FAIL(ret)) {                                                                                                       \
  } else if (OB_FAIL(parse_disaster_recovery_task_type_from_string(task_type_str, task_type))) {                            \
    LOG_WARN("fail to parse task type", KR(ret), K(task_type_str));                                                         \
  } else if (false == execute_server_key.set_ip_addr(execute_ip, static_cast<uint32_t>(execute_port))) {                    \
    ret = OB_ERR_UNEXPECTED;                                                                                                \
    LOG_WARN("invalid server address", K(execute_ip), K(execute_port));                                                     \
  } else if (OB_FAIL(SVR_TRACER.get_server_zone(execute_server_key, execute_zone))) {                                       \
    LOG_WARN("get server zone failed", KR(ret), K(execute_server_key));                                                     \
  } else if (OB_FAIL(task_key.init(tenant_id, share::ObLSID(ls_id), execute_zone, task_type))) {                            \
    LOG_WARN("fail to init a ObDRTaskKey", KR(ret), K(tenant_id), K(ls_id), K(execute_zone), K(task_type));                 \
  } else if (OB_FAIL(comment_to_set.assign(comment))) {                                                                     \
    LOG_WARN("fai to assign a ObString to ObSqlString", KR(ret), K(comment));                                               \
  } else if (OB_FAIL(task_id_sqlstring_format.assign(task_id))) {                                                           \
    LOG_WARN("fail to assign task id to ObSqlString format", KR(ret), K(task_id));                                          \
  } else if (OB_FAIL(task_id_to_set.set(task_id_sqlstring_format.ptr()))) {                                                 \
    LOG_WARN("fail to init a task_id", KR(ret), K(task_id_sqlstring_format));                                               \
  } else {                                                                                                                  \
    if (priority == 0) {                                                                                                    \
      priority_to_set = ObDRTaskPriority::HIGH_PRI;                                                                         \
    } else if (priority == 1) {                                                                                             \
      priority_to_set = ObDRTaskPriority::LOW_PRI;                                                                          \
    } else {                                                                                                                \
      priority_to_set = ObDRTaskPriority::MAX_PRI;                                                                          \
    }                                                                                                                       \
    src_type_to_set = ObShareUtil::string_to_replica_type(src_type_str);                                                    \
    dest_type_to_set = ObShareUtil::string_to_replica_type(dest_type_str);                                                  \
  }                                                                                                                         \

OB_SERIALIZE_MEMBER(
    ObDRLSReplicaTaskStatus,
    status_);

static const char* dr_ls_replica_task_status_strs[] = {
  "INPROGRESS",
  "COMPLETED",
  "FAILED",
  "CANCELED",
  "WAITING",
};

const char* ObDRLSReplicaTaskStatus::get_status_str() const {
  STATIC_ASSERT(ARRAYSIZEOF(dr_ls_replica_task_status_strs) == (int64_t)MAX_STATUS,
                "dr_ls_replica_task_status_strs string array size mismatch enum DRLSReplicaTaskStatus count");
  const char *str = NULL;
  if (status_ >= INPROGRESS && status_ < MAX_STATUS) {
    str = dr_ls_replica_task_status_strs[status_];
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid DRLSReplicaTaskStatus", K_(status));
  }
  return str;
}

int64_t ObDRLSReplicaTaskStatus::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(status), "status", get_status_str());
  J_OBJ_END();
  return pos;
}

void ObDRLSReplicaTaskStatus::assign(const ObDRLSReplicaTaskStatus &other)
{
  if (this != &other) {
    status_ = other.status_;
  }
}

int ObDRLSReplicaTaskStatus::parse_from_string(const ObString &status)
{
  int ret = OB_SUCCESS;
  bool found = false;
  STATIC_ASSERT(ARRAYSIZEOF(dr_ls_replica_task_status_strs) == (int64_t)MAX_STATUS,
                "dr_ls_replica_task_status_strs string array size mismatch enum DRLSReplicaTaskStatus count");
  for (int64_t i = 0; i < ARRAYSIZEOF(dr_ls_replica_task_status_strs) && !found; i++) {
    if (0 == status.case_compare(dr_ls_replica_task_status_strs[i])) {
      status_ = static_cast<DRLSReplicaTaskStatus>(i);
      found = true;
      break;
    }
  }
  if (!found) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to parse status from string", KR(ret), K(status), K_(status));
  }
  return ret;
}

int build_execute_result(
    const int ret_code,
    const ObDRTaskRetComment &ret_comment,
    const int64_t start_time,
    ObSqlString &execute_result)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t elapsed = now - start_time;
  execute_result.reset();
  if (OB_UNLIKELY(start_time <= 0)) {
    // not validate ret_comment, it may be an invalid value
    // for example :
    //   see ERRSIM_DISASTER_RECOVERY_EXECUTE_TASK_ERROR in ObDRTaskExecutor::execute,
    //   it will cause the task execution failed, but no valid ret_comment is constructed.
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_time));
  } else if (OB_FAIL(execute_result.append_fmt(
              "ret:%d, %s; elapsed:%ld;", ret_code, common::ob_error_name(ret_code), elapsed))) {
    LOG_WARN("fail to append to execute_result", KR(ret), K(ret_code), K(elapsed));
  } else if (OB_SUCCESS != ret_code
             && OB_FAIL(execute_result.append_fmt(" comment:%s;",
                        ob_disaster_recovery_task_ret_comment_strs(ret_comment)))) {
    LOG_WARN("fail to append ret comment to execute result", KR(ret), K(ret_comment));
  }
  return ret;
}

bool is_manual_dr_task_data_version_match(uint64_t tenant_data_version)
{
  return ((tenant_data_version >= DATA_VERSION_4_3_3_0)
       || (tenant_data_version >= MOCK_DATA_VERSION_4_2_3_0 && tenant_data_version < DATA_VERSION_4_3_0_0)
       || (tenant_data_version >= MOCK_DATA_VERSION_4_2_1_8 && tenant_data_version < DATA_VERSION_4_2_2_0));
}

int ObDstReplica::assign(
    const uint64_t unit_id,
    const uint64_t unit_group_id,
    const common::ObZone &zone,
    const common::ObReplicaMember &member)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == unit_id
                  || OB_INVALID_ID == unit_group_id
                  || !member.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
             K(unit_id),
             K(unit_group_id),
             K(zone),
             K(member));
  } else {
    unit_id_ = unit_id;
    unit_group_id_ = unit_group_id;
    zone_ = zone;
    member_ = member;
  }
  return ret;
}

int ObDstReplica::assign(
    const ObDstReplica &that)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!that.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(that));
  } else {
    unit_id_ = that.unit_id_;
    unit_group_id_ = that.unit_group_id_;
    zone_ = that.zone_;
    member_ = that.member_;
  }
  return ret;
}

void ObDstReplica::reset()
{
  unit_id_ = OB_INVALID_ID;
  unit_group_id_ = OB_INVALID_ID;
  zone_.reset();
  member_.reset();
}

static const char* disaster_recovery_task_ret_comment_strs[] = {
  "[storage] receive task reply from storage rpc",
  "[storage] fail to send execution rpc",
  "[rs] rs need to clean this task because server not exist",
  "[rs] rs need to clean this task because server permanent offline",
  "[rs] rs need to clean this task because task not running",
  "[rs] rs need to clean this task because task is timeout",
  "[rs] task can not execute because server is not alive",
  "[rs] task can not execute because fail to check paxos replica number",
  "[rs] task can not execute because replica is not in service",
  "[rs] task can not execute because server is permanent offline",
  "[rs] task can not persist because conflict with clone operation",
  ""/*default max*/
};

const char* ob_disaster_recovery_task_ret_comment_strs(const rootserver::ObDRTaskRetComment ret_comment)
{
  STATIC_ASSERT(ARRAYSIZEOF(disaster_recovery_task_ret_comment_strs) == (int64_t)rootserver::ObDRTaskRetComment::MAX + 1,
                "ret_comment string array size mismatch enum ObDRTaskRetComment count");
  const char *str = NULL;
  if (ret_comment >= rootserver::ObDRTaskRetComment::RECEIVE_FROM_STORAGE_RPC && ret_comment <= rootserver::ObDRTaskRetComment::MAX) {
    str = disaster_recovery_task_ret_comment_strs[static_cast<int64_t>(ret_comment)];
  } else {
    str = disaster_recovery_task_ret_comment_strs[static_cast<int64_t>(rootserver::ObDRTaskRetComment::MAX)];
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid ObDRTaskRetComment", K(ret_comment));
  }
  return str;
}

static const char* disaster_recovery_task_priority_strs[] = {
  "HIGH",
  "LOW",
  "MAX"
};

const char* ob_disaster_recovery_task_priority_strs(const rootserver::ObDRTaskPriority task_priority)
{
  STATIC_ASSERT(ARRAYSIZEOF(disaster_recovery_task_priority_strs) == (int64_t)rootserver::ObDRTaskPriority::MAX_PRI + 1,
                "type string array size mismatch with enum ObDRTaskPriority count");
  const char *str = NULL;
  if (task_priority >= rootserver::ObDRTaskPriority::HIGH_PRI && task_priority < rootserver::ObDRTaskPriority::MAX_PRI) {
    str = disaster_recovery_task_priority_strs[static_cast<int64_t>(task_priority)];
  } else {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid ObDRTask priority", K(task_priority));
  }
  return str;
}

static const char* disaster_recovery_task_type_strs[] = {
  "MIGRATE REPLICA",
  "ADD REPLICA",
  "BUILD ONLY IN MEMBER LIST",
  "TYPE TRANSFORM",
  "REMOVE PAXOS REPLICA",
  "REMOVE NON PAXOS REPLICA",
  "MODIFY PAXOS REPLICA NUMBER",
  "MAX_TYPE"
};

const char *ob_disaster_recovery_task_type_strs(const rootserver::ObDRTaskType type)
{
  STATIC_ASSERT(ARRAYSIZEOF(disaster_recovery_task_type_strs) == (int64_t)rootserver::ObDRTaskType::MAX_TYPE + 1,
                "type string array size mismatch with enum ObDRTaskType count");
  const char *str = NULL;
  if (type >= rootserver::ObDRTaskType::LS_MIGRATE_REPLICA && type < rootserver::ObDRTaskType::MAX_TYPE) {
    str = disaster_recovery_task_type_strs[static_cast<int64_t>(type)];
  } else {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid ObDRTask type", K(type));
  }
  return str;
}

int parse_disaster_recovery_task_type_from_string(
    const ObString &task_type_str,
    rootserver::ObDRTaskType& task_type)
{
  int ret = OB_SUCCESS;
  bool found = false;
  STATIC_ASSERT(ARRAYSIZEOF(disaster_recovery_task_type_strs) == (int64_t)rootserver::ObDRTaskType::MAX_TYPE + 1,
                "disaster_recovery_task_type_strs string array size mismatch enum ObDRTaskType count");
  for (int64_t i = 0; i < ARRAYSIZEOF(disaster_recovery_task_type_strs) && !found; i++) {
    if (0 == task_type_str.case_compare(disaster_recovery_task_type_strs[i])) {
      task_type = static_cast<rootserver::ObDRTaskType>(i);
      found = true;
    }
  }
  if (!found) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to parse task_type from string", KR(ret), K(task_type), K(task_type_str));
  }
  return ret;
}

void ObDRTaskKey::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  task_execute_zone_.reset();
  task_type_ = ObDRTaskType::MAX_TYPE;
}

bool ObDRTaskKey::is_valid() const
{
  return is_valid_tenant_id(tenant_id_)
      && ls_id_.is_valid_with_tenant(tenant_id_)
      && !task_execute_zone_.is_empty()
      && ObDRTaskType::MAX_TYPE != task_type_;
}

bool ObDRTaskKey::operator==(const ObDRTaskKey &that) const
{
  return tenant_id_ == that.tenant_id_
      && ls_id_ == that.ls_id_
      && task_execute_zone_ == that.task_execute_zone_
      && task_type_ == that.task_type_;
}

int ObDRTaskKey::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObZone &task_execute_zone,
    const ObDRTaskType &task_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
               || !ls_id.is_valid_with_tenant(tenant_id)
               || task_execute_zone.is_empty()
               || ObDRTaskType::MAX_TYPE == task_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id), K(task_execute_zone), K(task_type));
  } else if (OB_FAIL(task_execute_zone_.assign(task_execute_zone))) {
    LOG_WARN("task_execute_zone_ assign failed", KR(ret), K(task_execute_zone));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    task_type_ = task_type;
  }
  return ret;
}

int ObDRTaskKey::assign(
    const ObDRTaskKey &that)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!that.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(task_execute_zone_.assign(that.task_execute_zone_))) {
    LOG_WARN("task_execute_zone_ assign failed", KR(ret), K(that.task_execute_zone_));
  } else {
    tenant_id_ = that.tenant_id_;
    ls_id_ = that.ls_id_;
    task_type_ = that.task_type_;
  }
  return ret;
}

int ObDRTask::fill_dml_splicer(
    share::ObDMLSqlSplicer &dml_splicer) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", tenant_id_))) {
    LOG_WARN("add column failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(dml_splicer.add_pk_column("ls_id", ls_id_.id()))) {
    LOG_WARN("add column failed", KR(ret), K(ls_id_));
  } else if (OB_FAIL(dml_splicer.add_pk_column("task_id", task_id_))) {
    LOG_WARN("add column failed", KR(ret), K(task_id_));
  } else if (OB_FAIL(dml_splicer.add_column("task_status", ObDRLSReplicaTaskStatus(ObDRLSReplicaTaskStatus::INPROGRESS).get_status_str()))) {
    // it will only be called when the task is started
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml_splicer.add_column("priority", static_cast<int64_t>(priority_)))) {
    LOG_WARN("add column failed", KR(ret), K(priority_));
  } else if (OB_FAIL(dml_splicer.add_time_column("generate_time", generate_time_))) {
    LOG_WARN("add column failed", KR(ret), K(generate_time_));
  } else if (OB_FAIL(dml_splicer.add_time_column("schedule_time", schedule_time_))) {
    LOG_WARN("add column failed", KR(ret), K(schedule_time_));
  } else if (OB_FAIL(dml_splicer.add_column("comment", comment_.ptr()))) {
    LOG_WARN("add column failed", KR(ret), K(comment_));
  }
  return ret;
}

int ObDRTask::fill_dml_splicer_for_new_column(
    share::ObDMLSqlSplicer &dml_splicer,
    const common::ObAddr &force_data_src) const
{
  // force_data_src may be invalid
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  char force_data_source_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret));
  } else if (ObDRTaskType::MAX_TYPE == get_disaster_recovery_task_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected task type", KR(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(tenant_id_), tenant_data_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K(tenant_id_));
  } else if (!is_manual_dr_task_data_version_match(tenant_data_version)
           && (is_manual_task() || force_data_src.is_valid())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("manual operation is not suppported when tenant's data version is not match",
      KR(ret), K(tenant_data_version), K(is_manual_task()), K(force_data_src));
  } else if (is_manual_dr_task_data_version_match(tenant_data_version)) {
    if (!is_manual_task() && force_data_src.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task invoke and data_source is not match", KR(ret), K(is_manual_task()), K(force_data_src));
    } else if (OB_FAIL(dml_splicer.add_column("is_manual", is_manual_task()))) {
      LOG_WARN("add column failed", KR(ret), K(is_manual_task()));
    } else if (ObDRTaskType::LS_ADD_REPLICA == get_disaster_recovery_task_type()
            || ObDRTaskType::LS_MIGRATE_REPLICA == get_disaster_recovery_task_type()) {
      if (false == force_data_src.ip_to_string(force_data_source_ip, sizeof(force_data_source_ip))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("convert data_src_server ip to string failed", KR(ret), K(force_data_src));
      } else if (OB_FAIL(dml_splicer.add_column("data_source_svr_ip", force_data_source_ip))) {
        LOG_WARN("add column failed", KR(ret), K(force_data_source_ip));
      } else if (OB_FAIL(dml_splicer.add_column("data_source_svr_port", force_data_src.get_port()))) {
        LOG_WARN("add column failed", KR(ret), K(force_data_src));
      }
    }
  }
  return ret;
}

bool ObDRTask::is_already_timeout() const
{
  int64_t now = ObTimeUtility::current_time();
  return schedule_time_ + GCONF.balancer_task_timeout < now;
}

int ObDRTask::set_task_key(
    const ObDRTaskKey &task_key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_key_.assign(task_key))) {
    LOG_WARN("fail to init task", KR(ret), K(task_key));
  }
  return ret;
}

void ObDRTask::set_schedule()
{
  set_schedule_time(ObTimeUtility::current_time());
}

int ObDRTask::deep_copy(const ObDRTask &that)
{
  int ret = OB_SUCCESS;
  tenant_id_ = that.tenant_id_;
  ls_id_ = that.ls_id_;
  cluster_id_ = that.cluster_id_;
  transmit_data_size_ = that.transmit_data_size_;
  invoked_source_ = that.invoked_source_;
  /* generated_time_ shall not be copied,
   * the generated_time_ is automatically set in the constructor func
   */
  priority_ = that.priority_;
  schedule_time_ = that.schedule_time_;
  execute_time_ = that.execute_time_;
  task_id_ = that.task_id_;
  if (OB_FAIL(set_comment(that.comment_.string()))) {
    LOG_WARN("fail to assign comment", KR(ret), K_(comment), K(that));
  } else if (OB_FAIL(task_key_.assign(that.task_key_))) {
    LOG_WARN("fail to set task_key", KR(ret), K_(task_key), K(that));
  }
  return ret;
}

int ObDRTask::build(
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
    const ObString &comment)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!task_key.is_valid()
                  || OB_INVALID_ID == tenant_id
                  || !ls_id.is_valid()
                  || task_id.is_invalid()
                  || transmit_data_size < 0
                  || obrpc::ObAdminClearDRTaskArg::TaskType::MAX_TYPE == invoked_source
                  || ObDRTaskPriority::MAX_PRI == priority
                  || nullptr == comment)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(task_key),
             K(tenant_id),
             K(ls_id),
             K(task_id),
             K(transmit_data_size),
             K(invoked_source),
             K(priority),
             K(comment));
  } else {
    if (OB_FAIL(set_task_key(task_key))) {
      LOG_WARN("fail to set task key", KR(ret), K(task_key));
    } else if (OB_FAIL(set_comment(comment))) {
      LOG_WARN("fail to set comment", KR(ret), K(comment));
    } else {
      tenant_id_ = tenant_id;
      set_ls_id(ls_id);
      task_id_ = task_id;
      schedule_time_ = schedule_time_us;
      generate_time_ = generate_time_us;
      cluster_id_ = cluster_id;
      transmit_data_size_ = transmit_data_size;
      invoked_source_ = invoked_source;
      set_priority(priority);
    }
  }
  return ret;
}

// ===================== ObMigrateLSReplicaTask ========================

int ObMigrateLSReplicaTask::get_execute_transmit_size(
    int64_t &execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  execute_transmit_size = 0;
  ObReplicaType dst_replica_type = dst_replica_.get_replica_type();
  if (ObReplicaTypeCheck::is_replica_type_valid(dst_replica_type)) {
    execute_transmit_size = transmit_data_size_;
  } else if (REPLICA_TYPE_LOGONLY == dst_replica_type
      || REPLICA_TYPE_ENCRYPTION_LOGONLY == dst_replica_type) {
    execute_transmit_size = 0;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dst unexpected replica type", KR(ret), "task", *this);
  }
  return ret;
}

int ObMigrateLSReplicaTask::get_virtual_disaster_recovery_task_stat(
    common::ObAddr &src,
    common::ObAddr &data_src,
    common::ObAddr &dst,
    common::ObAddr &offline) const
{
  int ret = OB_SUCCESS;
  src = src_member_.get_server();
  data_src = data_src_member_.get_server();
  dst = dst_replica_.get_server();
  offline = src_member_.get_server();
  return ret;
}

int ObMigrateLSReplicaTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  ObSqlString source;
  char src_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char data_src_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (false == src_member_.get_server().ip_to_string(src_ip, sizeof(src_ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert src_server ip to string failed", KR(ret), "src_member", src_member_.get_server());
  } else if (false == data_src_member_.get_server().ip_to_string(data_src_ip, sizeof(data_src_ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert data_src_server ip to string failed", KR(ret), "data_src_member", data_src_member_.get_server());
  } else if (OB_FAIL(source.append_fmt(
              "source_replica:%s:%d data_source_replica:%s:%d",
              src_ip, src_member_.get_server().get_port(),
              data_src_ip, data_src_member_.get_server().get_port()))) {
    LOG_WARN("fail to append to source", KR(ret),
             "src_member", src_member_.get_server(),
             "data_src_member", data_src_member_.get_server());
  } else {
    ROOTSERVICE_EVENT_ADD("disaster_recovery", get_log_start_str(),
                          "tenant_id", get_tenant_id(),
                          "ls_id", get_ls_id().id(),
                          "task_id", get_task_id(),
                          "source", source.ptr(),
                          "destination", dst_replica_.get_server(),
                          "comment", get_comment().ptr());
  }
  return ret;
}


int ObMigrateLSReplicaTask::log_execute_result(
    const int ret_code,
    const ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  ObSqlString execute_result;
  int64_t start_time = execute_time_ > 0 ? execute_time_ : schedule_time_;
  // when rs change leader, execute_time_ is 0, only schedule_time_ is valid
  if (OB_FAIL(build_execute_result(ret_code, ret_comment, start_time, execute_result))) {
    LOG_WARN("fail to build execute result", KR(ret), K(ret_code), K(ret_comment));
  } else {
    ROOTSERVICE_EVENT_ADD("disaster_recovery", get_log_finish_str(),
                          "tenant_id", get_tenant_id(),
                          "ls_id", get_ls_id().id(),
                          "task_id", get_task_id(),
                          "source", src_member_.get_server(),
                          "destination", dst_replica_.get_server(),
                          "execute_result", execute_result,
                          get_comment().ptr());
  }
  return ret;
}

int ObMigrateLSReplicaTask::check_before_execute(
    share::ObLSTableOperator &lst_operator,
    ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  share::ObLSInfo ls_info;
  if (OB_UNLIKELY(lst_operator.get(
          GCONF.cluster_id,
          get_tenant_id(),
          get_ls_id(),
          share::ObLSTable::COMPOSITE_MODE,
          ls_info))) {
    LOG_WARN("fail to get log stream info", KR(ret),
             "tenant_id", get_tenant_id(),
             "ls_id", get_ls_id());
  } else if (OB_FAIL(check_paxos_number(ls_info, ret_comment))) {
    LOG_WARN("fail to check paxos replica number", KR(ret), K(ls_info));
  } else if (OB_FAIL(check_online(ls_info, ret_comment))) {
    LOG_WARN("fail to check online", KR(ret), K(ls_info));
  }
  return ret;
}

int ObMigrateLSReplicaTask::execute(
    obrpc::ObSrvRpcProxy &rpc_proxy,
    int &ret_code,
    ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;

  DEBUG_SYNC(BEFORE_SEND_MIGRATE_REPLICA_DRTASK);
  ObLSMigrateReplicaArg arg;
  if (OB_FAIL(arg.init(
          get_task_id(),
          get_tenant_id(),
          get_ls_id(),
          get_src_member(),
          get_dst_replica().get_member(),
          get_data_src_member(),
          get_paxos_replica_number(),
          false/*skip_change_member_list(not used)*/,
          get_force_data_src_member(),
          get_prioritize_same_zone_src()))) {
    /*
     During parallel migration, the same zone data source is preferred.
     If multiple replica select the same server as the data source,
     it will cause greater IO pressure on the corresponding server and affect normal function.
     In addition, high IO pressure will also affect the speed of copying data and lose the advantage of parallel migration.
    */
    LOG_WARN("fail to init arg", KR(ret));
  } else if (OB_FAIL(rpc_proxy.to(get_dst_server())
        .by(get_tenant_id()).ls_migrate_replica(arg))) {
    ret_code = ret;
    ret_comment = ObDRTaskRetComment::FAIL_TO_SEND_RPC;
    LOG_WARN("fail to send ls migrate replica rpc", KR(ret), K(arg));
  } else {
    LOG_INFO("start to execute ls migrate replica", K(arg));
  }
  return ret;
}

int ObMigrateLSReplicaTask::fill_dml_splicer(
    ObDMLSqlSplicer &dml_splicer) const
{
  int ret = OB_SUCCESS;
  char src_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char dest_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char task_id[OB_TRACE_STAT_BUFFER_SIZE] = "";
  char task_type[MAX_DISASTER_RECOVERY_TASK_TYPE_LENGTH] = "MIGRATE REPLICA";
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret));
  } else if (false == get_src_member().get_server().ip_to_string(src_ip, sizeof(src_ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert src_server ip to string failed", KR(ret), "src_server", get_src_member().get_server());
  } else if (false == get_dst_server().ip_to_string(dest_ip, sizeof(dest_ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert dest_server ip to string failed", KR(ret), "dest_server", get_dst_server());
  } else if (OB_FAIL(ObDRTask::fill_dml_splicer(dml_splicer))) {
    LOG_WARN("ObDRTask fill dml splicer failed", KR(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column("task_type", task_type))
          || OB_FAIL(dml_splicer.add_column("target_replica_svr_ip", dest_ip))
          || OB_FAIL(dml_splicer.add_column("target_replica_svr_port", get_dst_server().get_port()))
          || OB_FAIL(dml_splicer.add_column("target_paxos_replica_number", get_paxos_replica_number()))
          || OB_FAIL(dml_splicer.add_column("target_replica_type", ObShareUtil::replica_type_to_string(get_dst_replica().get_member().get_replica_type())))
          || OB_FAIL(dml_splicer.add_column("source_replica_svr_ip", src_ip))
          || OB_FAIL(dml_splicer.add_column("source_replica_svr_port", get_src_member().get_server().get_port()))
          || OB_FAIL(dml_splicer.add_column("source_paxos_replica_number", get_paxos_replica_number()))
          || OB_FAIL(dml_splicer.add_column("source_replica_type", ObShareUtil::replica_type_to_string(get_src_member().get_replica_type())))
          || OB_FAIL(dml_splicer.add_column("task_exec_svr_ip", dest_ip))
          || OB_FAIL(dml_splicer.add_column("task_exec_svr_port", get_dst_server().get_port()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(fill_dml_splicer_for_new_column(dml_splicer, get_force_data_src_member().get_server()))) {
    LOG_WARN("fill dml_splicer for new column failed", KR(ret));
  }
  return ret;
}

int ObMigrateLSReplicaTask::set_dst_replica(
    const ObDstReplica &that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dst_replica_.assign(that))) {
    LOG_WARN("fail to assign dst replica", KR(ret), K(that));
  }
  return ret;
}

int ObMigrateLSReplicaTask::set_dst_replica(
    const uint64_t unit_id,
    const uint64_t unit_group_id,
    const common::ObZone &zone,
    const common::ObReplicaMember &member)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dst_replica_.assign(
          unit_id,
          unit_group_id,
          zone,
          member))) {
    LOG_WARN("fail to assign dst replica", KR(ret),
             K(unit_id), K(unit_group_id), K(zone), K(member));
  }
  return ret;
}

int ObMigrateLSReplicaTask::check_paxos_number(
    const share::ObLSInfo &ls_info,
    ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  const ObLSReplica *leader = nullptr;
  if (OB_FAIL(ls_info.find_leader(leader))) {
    LOG_WARN("fail to get leader", KR(ret), K(ls_info));
  } else if (OB_ISNULL(leader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader replica is null", KR(ret));
  } else if (leader->get_paxos_replica_number() <= 0) {
    ret = OB_REBALANCE_TASK_CANT_EXEC;
    LOG_WARN("paxos replica number not report", KR(ret), KPC(leader));
  } else if (leader->get_paxos_replica_number() != paxos_replica_number_) {
    ret = OB_REBALANCE_TASK_CANT_EXEC;
    LOG_WARN("paxos replica number not match", KR(ret),
             "paxos_replica_number", leader->get_paxos_replica_number(),
             "this_task", *this);
  }
  if (OB_FAIL(ret)) {
    ret_comment = ObDRTaskRetComment::CANNOT_EXECUTE_DUE_TO_PAXOS_REPLICA_NUMBER;
  }
  return ret;
}

int ObMigrateLSReplicaTask::check_online(
    const share::ObLSInfo &ls_info,
    ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  const ObLSReplica *replica = nullptr;
  int tmp_ret = ls_info.find(dst_replica_.get_server(), replica);
  if (OB_ENTRY_NOT_EXIST == tmp_ret) {
    // good
  } else if (OB_SUCCESS != tmp_ret) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to find replica", K(ret));
  } else if (nullptr == replica) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to find replica", K(ret));
  } else if (replica->is_in_service()) {
    ret = OB_REBALANCE_TASK_CANT_EXEC;
    LOG_WARN("cannot online normal replica already exist", KR(ret), K(ls_info),
             "dst_server", dst_replica_.get_server());
    ret_comment = ObDRTaskRetComment::CANNOT_EXECUTE_DUE_TO_REPLICA_NOT_INSERVICE;
  }
  return ret;
}

int64_t ObMigrateLSReplicaTask::get_clone_size() const
{
  return sizeof(*this);
}

int ObMigrateLSReplicaTask::clone(
    void *input_ptr,
    ObDRTask *&output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    ObMigrateLSReplicaTask *my_task = new (input_ptr) ObMigrateLSReplicaTask();
    if (OB_UNLIKELY(nullptr == my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to construct", KR(ret));
    } else if (OB_FAIL(my_task->deep_copy(*this))) {
      LOG_WARN("fail to deep copy", KR(ret));
    } else if (OB_FAIL(my_task->set_dst_replica(get_dst_replica()))) {
      LOG_WARN("fail to set dst replica", KR(ret));
    } else {
      my_task->set_src_member(get_src_member());
      my_task->set_data_src_member(get_data_src_member());
      my_task->set_force_data_src_member(get_force_data_src_member());
      my_task->set_paxos_replica_number(get_paxos_replica_number());
      my_task->set_prioritize_same_zone_src(get_prioritize_same_zone_src());
      output_task = my_task;
    }
  }
  return ret;
}

int ObMigrateLSReplicaTask::build(
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
    const int64_t paxos_replica_number)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!dst_replica.is_valid()
                  || !src_member.is_valid()
                  || !data_src_member.is_valid()
                  || paxos_replica_number <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(dst_replica),
             K(src_member),
             K(data_src_member),
             K(paxos_replica_number));
  } else if (OB_FAIL(ObDRTask::build(
          task_key,
          tenant_id,
          ls_id,
          task_id,
          schedule_time_us,
          generate_time_us,
          cluster_id,
          transmit_data_size,
          invoked_source,
          priority,
          comment))) {
    LOG_WARN("fail to build ObDRTask", KR(ret),
             K(task_key),
             K(tenant_id),
             K(ls_id),
             K(task_id),
             K(schedule_time_us),
             K(generate_time_us),
             K(transmit_data_size),
             K(invoked_source),
             K(priority));
  } else {
    if (OB_FAIL(dst_replica_.assign(dst_replica))) {
      LOG_WARN("fail to assign dst replica", KR(ret), K(dst_replica));
    } else {
      set_src_member(src_member);
      set_data_src_member(data_src_member);
      set_force_data_src_member(force_data_src_member);
      paxos_replica_number_ = paxos_replica_number;
    }
  }
  return ret;
}

int ObMigrateLSReplicaTask::simple_build(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const share::ObTaskId &task_id,
    const ObDstReplica &dst_replica,
    const common::ObReplicaMember &src_member,
    const common::ObReplicaMember &data_src_member,
    const common::ObReplicaMember &force_data_src_member,
    const int64_t paxos_replica_number)
{
  int ret = OB_SUCCESS;
  ObDRTaskKey task_key;
  common::ObZone zone;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
               || !ls_id.is_valid_with_tenant(tenant_id)
               || !task_id.is_valid()
               || !dst_replica.is_valid()
               || !src_member.is_valid()
               || paxos_replica_number <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(task_id), K(dst_replica),
              K(src_member), K(paxos_replica_number));
  } else if (OB_FAIL(SVR_TRACER.get_server_zone(dst_replica.get_server(), zone))) {
    LOG_WARN("get server zone failed", KR(ret), K(dst_replica));
  } else if (OB_FAIL(task_key.init(tenant_id, ls_id, zone, ObDRTaskType::LS_MIGRATE_REPLICA))) {
    LOG_WARN("fail to init task key", KR(ret), K(tenant_id), K(ls_id), K(zone));
  } else if (OB_FAIL(ObDRTask::build(
                        task_key,
                        tenant_id,
                        ls_id,
                        task_id,
                        0,/*schedule_time_us*/ 0,/*generate_time_us*/
                        GCONF.cluster_id, 0,/*transmit_data_size*/
                        obrpc::ObAdminClearDRTaskArg::TaskType::MANUAL,
                        ObDRTaskPriority::HIGH_PRI,
                        ObString(drtask::ALTER_SYSTEM_COMMAND_MIGRATE_REPLICA)))) {
    LOG_WARN("fail to build ObDRTask", KR(ret), K(task_key), K(tenant_id), K(ls_id), K(task_id));
  } else if (OB_FAIL(dst_replica_.assign(dst_replica))) {
    LOG_WARN("fail to assign dst replica", KR(ret), K(dst_replica));
  } else {
    src_member_ = src_member;
    data_src_member_ = data_src_member;
    force_data_src_member_ = force_data_src_member;
    paxos_replica_number_ = paxos_replica_number;
  }
  return ret;
}

int ObMigrateLSReplicaTask::build_task_from_sql_result(
    const sqlclient::ObMySQLResult &res)
{
  int ret = OB_SUCCESS;
  READ_TASK_FROM_SQL_RES_FOR_DISASTER_RECOVERY
  common::ObAddr src_server;
  common::ObAddr dest_server;
  ObDstReplica dst_replica;
  common::ObAddr force_data_source;
  if (OB_FAIL(ret)) {
  } else if (false == src_server.set_ip_addr(src_ip, static_cast<uint32_t>(src_port))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid server address", K(src_ip), K(src_port));
  } else if (false == dest_server.set_ip_addr(dest_ip, static_cast<uint32_t>(dest_port))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid server address", K(dest_ip), K(dest_port));
  } else if (false == force_data_source.set_ip_addr(data_source_ip, static_cast<uint32_t>(data_source_port))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid server address", K(data_source_ip), K(data_source_port));
  } else if (OB_FAIL(dst_replica.assign(0/*unit id*/, 0/*unit group id*/, task_key.get_zone(), ObReplicaMember(dest_server, 0)))) {
    LOG_WARN("fail to init a ObDstReplica", KR(ret));
  } else if (OB_FAIL(build(
                    task_key,                       //(in used)
                    task_key.get_tenant_id(),       //(in used)
                    task_key.get_ls_id(),           //(in used)
                    task_id_to_set,                 //(in used)
                    schedule_time_us,               //(in used)
                    generate_time_us,               //(in used)
                    GCONF.cluster_id,               //(not used)cluster_id
                    0/*transmit_data_size*/,             //(not used)
                    is_manual ? obrpc::ObAdminClearDRTaskArg::TaskType::MANUAL : obrpc::ObAdminClearDRTaskArg::TaskType::AUTO,//(not used)invoked_source
                    priority_to_set,                //(not used)
                    comment_to_set.ptr(),           //comment
                    dst_replica,                    //(in used)dest_server
                    ObReplicaMember(src_server, 0), //(in used)src_server
                    ObReplicaMember(src_server, 0), //(not used)data_src_member
                    ObReplicaMember(force_data_source, 0), //(not used)force_data_source
                    src_paxos_replica_number))) {                 //(not used)
    LOG_WARN("fail to build a ObMigrateLSReplicaTask", KR(ret));
  } else {
    LOG_INFO("success to build a ObMigrateLSReplicaTask", KPC(this), K(task_id), K(task_id_sqlstring_format), K(task_id_to_set));
  }
  return ret;
}

// ================================== ObAddLSReplicaTask ==================================

int ObAddLSReplicaTask::get_execute_transmit_size(
    int64_t &execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  execute_transmit_size = 0;
  ObReplicaType dst_replica_type = dst_replica_.get_replica_type();
  if (ObReplicaTypeCheck::is_replica_type_valid(dst_replica_type)) {
    execute_transmit_size = transmit_data_size_;
  } else if (REPLICA_TYPE_LOGONLY == dst_replica_type
      || REPLICA_TYPE_ENCRYPTION_LOGONLY == dst_replica_type) {
    execute_transmit_size = 0;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dst unexpected replica type", KR(ret), "task", *this);
  }
  return ret;
}

int ObAddLSReplicaTask::get_virtual_disaster_recovery_task_stat(
    common::ObAddr &src,
    common::ObAddr &data_src,
    common::ObAddr &dst,
    common::ObAddr &offline) const
{
  int ret = OB_SUCCESS;
  src = data_src_member_.get_server();
  data_src = data_src_member_.get_server();
  dst = dst_replica_.get_server();
  UNUSED(offline);
  return ret;
}

int ObAddLSReplicaTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  ROOTSERVICE_EVENT_ADD("disaster_recovery", get_log_start_str(),
                        "tenant_id", get_tenant_id(),
                        "ls_id", get_ls_id().id(),
                        "task_id", get_task_id(),
                        "destination", dst_replica_.get_server(),
                        "data_source", data_src_member_.get_server(),
                        "comment", get_comment().ptr());
  return ret;
}


int ObAddLSReplicaTask::log_execute_result(
    const int ret_code,
    const ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  ObSqlString execute_result;
  int64_t start_time = execute_time_ > 0 ? execute_time_ : schedule_time_;
  if (OB_FAIL(build_execute_result(ret_code, ret_comment, start_time, execute_result))) {
    LOG_WARN("fail to build execute result", KR(ret), K(ret_code), K(ret_comment));
  } else {
    ROOTSERVICE_EVENT_ADD("disaster_recovery", get_log_finish_str(),
                          "tenant_id", get_tenant_id(),
                          "ls_id", get_ls_id().id(),
                          "task_id", get_task_id(),
                          "destination", dst_replica_.get_server(),
                          "data_source", data_src_member_.get_server(),
                          "execute_result", execute_result,
                          get_comment().ptr());
  }
  return ret;
}

int ObAddLSReplicaTask::check_before_execute(
    share::ObLSTableOperator &lst_operator,
    ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  share::ObLSInfo ls_info;
  if (OB_UNLIKELY(lst_operator.get(
          GCONF.cluster_id,
          get_tenant_id(),
          get_ls_id(),
          share::ObLSTable::COMPOSITE_MODE,
          ls_info))) {
    LOG_WARN("fail to get log stream info", KR(ret),
             "tenant_id", get_tenant_id(),
             "ls_id", get_ls_id());
  } else if (OB_FAIL(check_online(ls_info, ret_comment))) {
    LOG_WARN("fail to check online", KR(ret), K(ls_info));
  } else if (OB_FAIL(check_paxos_member(ls_info, ret_comment))) {
    LOG_WARN("fail to check paxos member", KR(ret), K(ls_info));
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_EXECUTE_ADD_REPLICA_ERROR);
int ObAddLSReplicaTask::execute(
    obrpc::ObSrvRpcProxy &rpc_proxy,
    int &ret_code,
    ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;

  DEBUG_SYNC(BEFORE_SEND_ADD_REPLICA_DRTASK);
  ObLSAddReplicaArg arg;
  if (OB_UNLIKELY(ERRSIM_EXECUTE_ADD_REPLICA_ERROR)) {
    ret = ERRSIM_EXECUTE_ADD_REPLICA_ERROR;
  } else if (OB_FAIL(arg.init(
          get_task_id(),
          get_tenant_id(),
          get_ls_id(),
          get_dst_replica().get_member(),
          get_data_src_member(),
          get_orig_paxos_replica_number(),
          get_paxos_replica_number(),
          false/*skip_change_member_list(not used)*/,
          get_force_data_src_member()))) {
    LOG_WARN("fail to init arg", KR(ret));
  } else if (OB_FAIL(rpc_proxy.to(get_dst_server())
        .by(get_tenant_id()).ls_add_replica(arg))) {
    ret_code = ret;
    ret_comment = ObDRTaskRetComment::FAIL_TO_SEND_RPC;
    LOG_WARN("fail to send ls add replica rpc", KR(ret), K(arg));
  } else {
    LOG_INFO("start to execute ls add replica", K(arg));
  }
  return ret;
}

int ObAddLSReplicaTask::fill_dml_splicer(
    ObDMLSqlSplicer &dml_splicer) const
{
  int ret = OB_SUCCESS;
  char src_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char dest_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char task_id[OB_TRACE_STAT_BUFFER_SIZE] = "";
  char task_type[MAX_DISASTER_RECOVERY_TASK_TYPE_LENGTH] = "ADD REPLICA";
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret));
  } else if (false == get_data_src_member().get_server().ip_to_string(src_ip, sizeof(src_ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert src_server ip to string failed", KR(ret), "src_server", get_data_src_member().get_server());
  } else if (false == get_dst_server().ip_to_string(dest_ip, sizeof(dest_ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert dest_server ip to string failed", KR(ret), "dest_server", get_dst_server());
  } else if (OB_FAIL(ObDRTask::fill_dml_splicer(dml_splicer))) {
    LOG_WARN("ObDRTask fill dml splicer failed", KR(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column("task_type", task_type))
          || OB_FAIL(dml_splicer.add_column("target_replica_svr_ip", dest_ip))
          || OB_FAIL(dml_splicer.add_column("target_replica_svr_port", get_dst_server().get_port()))
          || OB_FAIL(dml_splicer.add_column("target_paxos_replica_number", get_paxos_replica_number()))
          || OB_FAIL(dml_splicer.add_column("target_replica_type", ObShareUtil::replica_type_to_string(get_dst_replica().get_member().get_replica_type())))
          || OB_FAIL(dml_splicer.add_column("source_replica_svr_ip", src_ip))
          || OB_FAIL(dml_splicer.add_column("source_replica_svr_port", get_data_src_member().get_server().get_port()))
          || OB_FAIL(dml_splicer.add_column("source_paxos_replica_number", get_orig_paxos_replica_number()))
          || OB_FAIL(dml_splicer.add_column("source_replica_type", ObShareUtil::replica_type_to_string(get_dst_replica().get_member().get_replica_type())))
          || OB_FAIL(dml_splicer.add_column("task_exec_svr_ip", dest_ip))
          || OB_FAIL(dml_splicer.add_column("task_exec_svr_port", get_dst_server().get_port()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(fill_dml_splicer_for_new_column(dml_splicer, get_force_data_src_member().get_server()))) {
    LOG_WARN("fill dml_splicer for new column failed", KR(ret));
  }
  return ret;
}

int ObAddLSReplicaTask::set_dst_replica(
    const ObDstReplica &that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dst_replica_.assign(that))) {
    LOG_WARN("fail to assign dst replica", KR(ret), K(that));
  }
  return ret;
}

int ObAddLSReplicaTask::set_dst_replica(
    const uint64_t unit_id,
    const uint64_t unit_group_id,
    const common::ObZone &zone,
    const common::ObReplicaMember &member)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dst_replica_.assign(
          unit_id,
          unit_group_id,
          zone,
          member))) {
    LOG_WARN("fail to assign dst replica", KR(ret),
             K(unit_id), K(unit_group_id), K(zone), K(member));
  }
  return ret;
}

int ObAddLSReplicaTask::check_online(
    const share::ObLSInfo &ls_info,
    ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  const ObLSReplica *replica = nullptr;
  int tmp_ret = ls_info.find(dst_replica_.get_server(), replica);
  if (OB_ENTRY_NOT_EXIST == tmp_ret) {
    // good
  } else if (OB_SUCCESS != tmp_ret) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to find replica", K(ret));
  } else if (nullptr == replica) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to find replica", K(ret));
  } else if (replica->is_in_service()) {
    ret = OB_REBALANCE_TASK_CANT_EXEC;
    LOG_WARN("cannot online normal replica already exist", KR(ret), K(ls_info),
             "dst_server", dst_replica_.get_server());
    ret_comment = ObDRTaskRetComment::CANNOT_EXECUTE_DUE_TO_REPLICA_NOT_INSERVICE;
  }
  return ret;
}

int ObAddLSReplicaTask::check_paxos_member(
    const share::ObLSInfo &ls_info,
    ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  if (!ObReplicaTypeCheck::is_paxos_replica_V2(dst_replica_.get_replica_type())) {
    // no need to check non paxos replica
  } else {
    const ObZone &dst_zone = dst_replica_.get_zone();
    FOREACH_CNT_X(r, ls_info.get_replicas(), OB_SUCC(ret)) {
      if (OB_UNLIKELY(nullptr == r)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid replica", K(ret), K(ls_info));
      } else if (r->get_server() == dst_replica_.get_server()) {
        // already check in check online
      } else if ((!is_manual_task() && r->get_zone() == dst_zone)
                 // manual operation allowed mutiple replica in same zone
                 && r->is_in_service()
                 && ObReplicaTypeCheck::is_paxos_replica_V2(r->get_replica_type())
                 && ObReplicaTypeCheck::is_paxos_replica_V2(dst_replica_.get_replica_type())) {
        ret = OB_REBALANCE_TASK_CANT_EXEC;
        LOG_WARN("only one paxos member allowed in a single zone", K(ret),
                 "zone", dst_zone, "task", *this);
      } else {} // no more to do
    }
  }
  if (OB_FAIL(ret)) {
    ret_comment = ObDRTaskRetComment::CANNOT_EXECUTE_DUE_TO_PAXOS_REPLICA_NUMBER;
  }
  return ret;
}

int64_t ObAddLSReplicaTask::get_clone_size() const
{
  return sizeof(*this);
}

int ObAddLSReplicaTask::clone(
    void *input_ptr,
    ObDRTask *&output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    ObAddLSReplicaTask *my_task = new (input_ptr) ObAddLSReplicaTask();
    if (OB_UNLIKELY(nullptr == my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to construct", KR(ret));
    } else if (OB_FAIL(my_task->deep_copy(*this))) {
      LOG_WARN("fail to deep copy", KR(ret));
    } else if (OB_FAIL(my_task->set_dst_replica(get_dst_replica()))) {
      LOG_WARN("fail to set dst replica", KR(ret));
    } else {
      my_task->set_data_src_member(get_data_src_member());
      my_task->set_force_data_src_member(get_force_data_src_member());
      my_task->set_orig_paxos_replica_number(get_orig_paxos_replica_number());
      my_task->set_paxos_replica_number(get_paxos_replica_number());
      output_task = my_task;
    }
  }
  return ret;
}

int ObAddLSReplicaTask::build(
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
    const common::ObReplicaMember &data_src_member,
    const common::ObReplicaMember &force_data_src_member,
    const int64_t orig_paxos_replica_number,
    const int64_t paxos_replica_number)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!dst_replica.is_valid()
                  || !data_src_member.is_valid()
                  || paxos_replica_number <= 0
                  || orig_paxos_replica_number <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(dst_replica),
             K(data_src_member),
             K(orig_paxos_replica_number),
             K(paxos_replica_number));
  } else if (OB_FAIL(ObDRTask::build(
          task_key,
          tenant_id,
          ls_id,
          task_id,
          schedule_time_us,
          generate_time_us,
          cluster_id,
          transmit_data_size,
          invoked_source,
          priority,
          comment))) {
    LOG_WARN("fail to build ObDRTask", KR(ret),
             K(task_key),
             K(tenant_id),
             K(ls_id),
             K(task_id),
             K(transmit_data_size),
             K(invoked_source),
             K(priority));
  } else {
    if (OB_FAIL(dst_replica_.assign(dst_replica))) {
      LOG_WARN("fail to assign dst replica", KR(ret), K(dst_replica));
    } else {
      set_data_src_member(data_src_member);
      set_force_data_src_member(force_data_src_member);
      orig_paxos_replica_number_ = orig_paxos_replica_number;
      paxos_replica_number_ = paxos_replica_number;
    }
  }
  return ret;
}

int ObAddLSReplicaTask::simple_build(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const share::ObTaskId &task_id,
    const ObDstReplica &dst_replica,
    const common::ObReplicaMember &data_src_member,
    const common::ObReplicaMember &force_data_src_member,
    const int64_t orig_paxos_replica_number,
    const int64_t paxos_replica_number)
{
  int ret = OB_SUCCESS;
  ObDRTaskKey task_key;
  common::ObZone zone;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
               || !ls_id.is_valid_with_tenant(tenant_id)
               || !task_id.is_valid()
               || !dst_replica.is_valid()
               || paxos_replica_number <= 0
               || orig_paxos_replica_number <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(task_id), K(dst_replica),
            K(orig_paxos_replica_number), K(paxos_replica_number));
  } else if (OB_FAIL(SVR_TRACER.get_server_zone(dst_replica.get_server(), zone))) {
    LOG_WARN("get server zone failed", KR(ret), K(dst_replica));
  } else if (OB_FAIL(task_key.init(tenant_id, ls_id, zone, ObDRTaskType::LS_ADD_REPLICA))) {
    LOG_WARN("fail to init task key", KR(ret), K(tenant_id), K(ls_id), K(zone));
  } else if (OB_FAIL(ObDRTask::build(
                        task_key,
                        tenant_id,
                        ls_id,
                        task_id,
                        0,/*schedule_time_us*/ 0,/*generate_time_us*/
                        GCONF.cluster_id, 0,/*transmit_data_size*/
                        obrpc::ObAdminClearDRTaskArg::TaskType::MANUAL,
                        ObDRTaskPriority::HIGH_PRI,
                        ObString(drtask::ALTER_SYSTEM_COMMAND_ADD_REPLICA)))) {
    LOG_WARN("fail to build ObDRTask", KR(ret), K(task_key), K(tenant_id), K(ls_id), K(task_id));
  } else if (OB_FAIL(dst_replica_.assign(dst_replica))) {
    LOG_WARN("fail to assign dst replica", KR(ret), K(dst_replica));
  } else {
    data_src_member_ = data_src_member;
    force_data_src_member_ = force_data_src_member;
    orig_paxos_replica_number_ = orig_paxos_replica_number;
    paxos_replica_number_ = paxos_replica_number;
  }
  return ret;
}


int ObAddLSReplicaTask::build_task_from_sql_result(
    const sqlclient::ObMySQLResult &res)
{
  int ret = OB_SUCCESS;
  READ_TASK_FROM_SQL_RES_FOR_DISASTER_RECOVERY
  common::ObAddr src_server;
  common::ObAddr dest_server;
  ObDstReplica dst_replica;
  common::ObAddr force_data_source;
  if (OB_FAIL(ret)) {
  } else if (false == src_server.set_ip_addr(src_ip, static_cast<uint32_t>(src_port))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid server address", K(src_ip), K(src_port));
  } else if (false == dest_server.set_ip_addr(dest_ip, static_cast<uint32_t>(dest_port))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid server address", K(dest_ip), K(dest_port));
  } else if (false == force_data_source.set_ip_addr(data_source_ip, static_cast<uint32_t>(data_source_port))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid server address", K(data_source_ip), K(data_source_port));
  } else if (OB_FAIL(dst_replica.assign(0/*unit id*/, 0/*unit group id*/, task_key.get_zone(), ObReplicaMember(dest_server, 0)))) {
    LOG_WARN("fail to init a ObDstReplica", KR(ret));
  } else if (OB_FAIL(build(
                    task_key,                       //(in used)
                    task_key.get_tenant_id(),       //(in used)
                    task_key.get_ls_id(),           //(in used)
                    task_id_to_set,                 //(in used)
                    schedule_time_us,
                    generate_time_us,
                    GCONF.cluster_id,               //(not used)cluster_id
                    0/*transmit_data_size*/,        //(not used)
                    is_manual ? obrpc::ObAdminClearDRTaskArg::TaskType::MANUAL : obrpc::ObAdminClearDRTaskArg::TaskType::AUTO,//(not used)invoked_source
                    priority_to_set,                //(not used)
                    comment_to_set.ptr(),           //comments
                    dst_replica,                    //(in used)dest_server
                    ObReplicaMember(src_server, 0), //(in used)src_server
                    ObReplicaMember(force_data_source, 0), //(in used)force_data_source
                    src_paxos_replica_number,                     //(in used)
                    dest_paxos_replica_number))) {                //(in used)
    LOG_WARN("fail to build a ObAddLSReplicaTask", KR(ret));
  } else {
    LOG_INFO("success to build a ObAddLSReplicaTask", KPC(this), K(task_id), K(task_id_to_set), K(task_id_sqlstring_format));
  }
  return ret;
}

// ================================== ObLSTypeTransformTask ==================================
int ObLSTypeTransformTask::get_execute_transmit_size(
    int64_t &execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  execute_transmit_size = 0;
  ObReplicaType dst_replica_type = dst_replica_.get_replica_type();
  ObReplicaType src_replica_type = src_member_.get_replica_type();
  if (REPLICA_TYPE_LOGONLY == dst_replica_type
      || REPLICA_TYPE_ENCRYPTION_LOGONLY == dst_replica_type) {
    execute_transmit_size = 0;
  } else if (REPLICA_TYPE_FULL == dst_replica_type) {
    if (REPLICA_TYPE_READONLY == src_replica_type) {
      execute_transmit_size = 0;
    } else if (REPLICA_TYPE_LOGONLY == src_replica_type
        || REPLICA_TYPE_ENCRYPTION_LOGONLY == src_replica_type) {
      execute_transmit_size = transmit_data_size_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid type", KR(ret), "task", *this);
    }
  } else if (REPLICA_TYPE_READONLY == dst_replica_type) {
    if (REPLICA_TYPE_FULL == src_replica_type) {
      execute_transmit_size = 0;
    } else if (REPLICA_TYPE_LOGONLY == src_replica_type
        || REPLICA_TYPE_ENCRYPTION_LOGONLY == src_replica_type) {
      execute_transmit_size = transmit_data_size_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid type", KR(ret), "task", *this);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid type", KR(ret), "task", *this);
  }
  return ret;
}

int ObLSTypeTransformTask::get_virtual_disaster_recovery_task_stat(
    common::ObAddr &src,
    common::ObAddr &data_src,
    common::ObAddr &dst,
    common::ObAddr &offline) const
{
  int ret = OB_SUCCESS;
  src = data_src_member_.get_server();
  data_src = data_src_member_.get_server();
  dst = dst_replica_.get_server();
  UNUSED(offline);
  return ret;
}

int ObLSTypeTransformTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  ROOTSERVICE_EVENT_ADD("disaster_recovery", get_log_start_str(),
                        "tenant_id", get_tenant_id(),
                        "ls_id", get_ls_id().id(),
                        "task_id", get_task_id(),
                        "destination", dst_replica_.get_server(),
                        "data_source", src_member_,
                        "comment", get_comment().ptr());
  return ret;
}

int ObLSTypeTransformTask::log_execute_result(
    const int ret_code,
    const ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  ObSqlString execute_result;
  int64_t start_time = execute_time_ > 0 ? execute_time_ : schedule_time_;
  if (OB_FAIL(build_execute_result(ret_code, ret_comment, start_time, execute_result))) {
    LOG_WARN("fail to build execute result", KR(ret), K(ret_code), K(ret_comment));
  } else {
    ROOTSERVICE_EVENT_ADD("disaster_recovery", get_log_finish_str(),
                          "tenant_id", get_tenant_id(),
                          "ls_id", get_ls_id().id(),
                          "task_id", get_task_id(),
                          "destination", dst_replica_.get_server(),
                          "data_source", src_member_,
                          "execute_result", execute_result,
                          get_comment().ptr());
  }
  return ret;
}

int ObLSTypeTransformTask::check_before_execute(
    share::ObLSTableOperator &lst_operator,
    ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  share::ObLSInfo ls_info;
  if (OB_UNLIKELY(lst_operator.get(
          GCONF.cluster_id,
          get_tenant_id(),
          get_ls_id(),
          share::ObLSTable::COMPOSITE_MODE,
          ls_info))) {
    LOG_WARN("fail to get log stream info", KR(ret),
             "tenant_id", get_tenant_id(),
             "ls_id", get_ls_id());
  } else if (OB_FAIL(check_online(ls_info, ret_comment))) {
    LOG_WARN("fail to check online", KR(ret), K(ls_info));
  } else if (OB_FAIL(check_paxos_member(ls_info, ret_comment))) {
    LOG_WARN("fail to check paxos member", KR(ret), K(ls_info));
  }
  return ret;
}

int ObLSTypeTransformTask::execute(
    obrpc::ObSrvRpcProxy &rpc_proxy,
    int &ret_code,
    ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  
  ObLSChangeReplicaArg arg;
  if (OB_FAIL(arg.init(
          get_task_id(),
          get_tenant_id(),
          get_ls_id(),
          get_src_member(),
          get_dst_replica().get_member(),
          get_data_src_member(),
          get_orig_paxos_replica_number(),
          get_paxos_replica_number(),
          false/*skip_change_member_list(not used)*/))) {
    LOG_WARN("fail to init arg", KR(ret));
  } else if (OB_FAIL(rpc_proxy.to(get_dst_server())
        .by(get_tenant_id()).ls_type_transform(arg))) {
    ret_code = ret;
    ret_comment = ObDRTaskRetComment::FAIL_TO_SEND_RPC;
    LOG_WARN("fail to send ls type transform rpc", KR(ret), K(arg));
  } else {
    LOG_INFO("start to execute ls type transform", K(arg));
  }
  return ret;
}

int ObLSTypeTransformTask::fill_dml_splicer(
    ObDMLSqlSplicer &dml_splicer) const
{
  int ret = OB_SUCCESS;
  char src_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char dest_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char target_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char task_id[OB_TRACE_STAT_BUFFER_SIZE] = "";
  char task_type[MAX_DISASTER_RECOVERY_TASK_TYPE_LENGTH] = "TYPE TRANSFORM";
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret));
  } else if (false == get_src_member().get_server().ip_to_string(src_ip, sizeof(src_ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert src_server ip to string failed", KR(ret), "src_server", get_src_member().get_server());
  } else if (false == get_dst_server().ip_to_string(dest_ip, sizeof(dest_ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert dest_server ip to string failed", KR(ret), "dest_server", get_dst_server());
  } else if (OB_FAIL(ObDRTask::fill_dml_splicer(dml_splicer))) {
    LOG_WARN("ObDRTask fill dml splicer failed", KR(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column("task_type", task_type))
          || OB_FAIL(dml_splicer.add_column("target_replica_svr_ip", dest_ip))
          || OB_FAIL(dml_splicer.add_column("target_replica_svr_port", get_dst_server().get_port()))
          || OB_FAIL(dml_splicer.add_column("target_paxos_replica_number", get_paxos_replica_number()))
          || OB_FAIL(dml_splicer.add_column("target_replica_type", ObShareUtil::replica_type_to_string(get_dst_replica().get_member().get_replica_type())))
          || OB_FAIL(dml_splicer.add_column("source_replica_svr_ip", src_ip))
          || OB_FAIL(dml_splicer.add_column("source_replica_svr_port", get_src_member().get_server().get_port()))
          || OB_FAIL(dml_splicer.add_column("source_paxos_replica_number", get_orig_paxos_replica_number()))
          || OB_FAIL(dml_splicer.add_column("source_replica_type", ObShareUtil::replica_type_to_string(get_src_member().get_replica_type())))
          || OB_FAIL(dml_splicer.add_column("task_exec_svr_ip", dest_ip))
          || OB_FAIL(dml_splicer.add_column("task_exec_svr_port", get_dst_server().get_port()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(fill_dml_splicer_for_new_column(dml_splicer, common::ObAddr()))) {
    LOG_WARN("fill dml_splicer for new column failed", KR(ret));
  }
  return ret;
}

int ObLSTypeTransformTask::set_dst_replica(
    const ObDstReplica &that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dst_replica_.assign(that))) {
    LOG_WARN("fail to assign dst replica", KR(ret), K(that));
  }
  return ret;
}

int ObLSTypeTransformTask::set_dst_replica(
    const uint64_t unit_id,
    const uint64_t unit_group_id,
    const common::ObZone &zone,
    const common::ObReplicaMember &member)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dst_replica_.assign(
          unit_id,
          unit_group_id,
          zone,
          member))) {
    LOG_WARN("fail to assign dst replica", KR(ret),
             K(unit_id), K(unit_group_id), K(zone), K(member));
  }
  return ret;
}

int ObLSTypeTransformTask::check_online(
    const share::ObLSInfo &ls_info,
    ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  const ObLSReplica *replica = nullptr;
  int tmp_ret = ls_info.find(dst_replica_.get_server(), replica);
  if (OB_ENTRY_NOT_EXIST == tmp_ret) {
    // good
  } else if (OB_SUCCESS != tmp_ret) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to find replica", K(ret));
  } else if (nullptr == replica) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to find replica", K(ret));
  } else if (replica->is_paxos_replica() && !replica->is_in_service()) {
    ret = OB_REBALANCE_TASK_CANT_EXEC;
    LOG_WARN("cannot online normal replica already exist", KR(ret), K(ls_info),
             "dst_server", dst_replica_.get_server());
    ret_comment = ObDRTaskRetComment::CANNOT_EXECUTE_DUE_TO_REPLICA_NOT_INSERVICE;
  }
  return ret;
}

int ObLSTypeTransformTask::check_paxos_member(
    const share::ObLSInfo &ls_info,
    ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  // no need to make sure only one F-replica in one zone.
  // Because shrink unit number may shrink unit with F-replica on it,
  // thus making another R type transform to F, then 2F in one zone is expected
  return ret;
}

int64_t ObLSTypeTransformTask::get_clone_size() const
{
  return sizeof(*this);
}

int ObLSTypeTransformTask::clone(
    void *input_ptr,
    ObDRTask *&output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    ObLSTypeTransformTask *my_task = new (input_ptr) ObLSTypeTransformTask();
    if (OB_UNLIKELY(nullptr == my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to construct", KR(ret));
    } else if (OB_FAIL(my_task->deep_copy(*this))) {
      LOG_WARN("fail to deep copy", KR(ret));
    } else if (OB_FAIL(my_task->set_dst_replica(get_dst_replica()))) {
      LOG_WARN("fail to set dst replica", KR(ret));
    } else {
      my_task->set_src_member(get_src_member());
      my_task->set_data_src_member(get_data_src_member());
      my_task->set_orig_paxos_replica_number(get_orig_paxos_replica_number());
      my_task->set_paxos_replica_number(get_paxos_replica_number());
      output_task = my_task;
    }
  }
  return ret;
}

int ObLSTypeTransformTask::build(
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
    const int64_t orig_paxos_replica_number,
    const int64_t paxos_replica_number)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!dst_replica.is_valid()
                  || !data_src_member.is_valid()
                  || paxos_replica_number <= 0
                  || orig_paxos_replica_number <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(dst_replica),
             K(data_src_member),
             K(orig_paxos_replica_number),
             K(paxos_replica_number));
  } else if (OB_FAIL(ObDRTask::build(
          task_key,
          tenant_id,
          ls_id,
          task_id,
          schedule_time_us,
          generate_time_us,
          cluster_id,
          transmit_data_size,
          invoked_source,
          priority,
          comment))) {
    LOG_WARN("fail to build ObDRTask", KR(ret),
             K(task_key),
             K(tenant_id),
             K(ls_id),
             K(task_id),
             K(transmit_data_size),
             K(invoked_source),
             K(priority));
  } else {
    if (OB_FAIL(dst_replica_.assign(dst_replica))) {
      LOG_WARN("fail to assign dst replica", KR(ret), K(dst_replica));
    } else {
      set_src_member(src_member);
      set_data_src_member(data_src_member);
      orig_paxos_replica_number_ = orig_paxos_replica_number;
      paxos_replica_number_ = paxos_replica_number;
    }
  }
  return ret;
}

int ObLSTypeTransformTask::simple_build(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const share::ObTaskId &task_id,
    const ObDstReplica &dst_replica,
    const common::ObReplicaMember &src_member,
    const common::ObReplicaMember &data_src_member,
    const int64_t orig_paxos_replica_number,
    const int64_t paxos_replica_number)
{
  int ret = OB_SUCCESS;
  ObDRTaskKey task_key;
  common::ObZone zone;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
               || !ls_id.is_valid_with_tenant(tenant_id)
               || !task_id.is_valid()
               || !dst_replica.is_valid()
               || !src_member.is_valid()
               || paxos_replica_number <= 0
               || orig_paxos_replica_number <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(task_id), K(dst_replica), K(src_member),
              K(orig_paxos_replica_number), K(paxos_replica_number));
  } else if (OB_FAIL(SVR_TRACER.get_server_zone(dst_replica.get_server(), zone))) {
    LOG_WARN("get server zone failed", KR(ret), K(dst_replica));
  } else if (OB_FAIL(task_key.init(tenant_id, ls_id, zone, ObDRTaskType::LS_TYPE_TRANSFORM))) {
    LOG_WARN("fail to init task key", KR(ret), K(tenant_id), K(ls_id), K(zone));
  } else if (OB_FAIL(ObDRTask::build(
                        task_key,
                        tenant_id,
                        ls_id,
                        task_id,
                        0,/*schedule_time_us*/ 0,/*generate_time_us*/
                        GCONF.cluster_id, 0,/*transmit_data_size*/
                        obrpc::ObAdminClearDRTaskArg::TaskType::MANUAL,
                        ObDRTaskPriority::HIGH_PRI,
                        ObString(drtask::ALTER_SYSTEM_COMMAND_MODIFY_REPLICA_TYPE)))) {
    LOG_WARN("fail to build ObDRTask", KR(ret), K(task_key), K(tenant_id), K(ls_id), K(task_id));
  } else if (OB_FAIL(dst_replica_.assign(dst_replica))) {
    LOG_WARN("fail to assign dst replica", KR(ret), K(dst_replica));
  } else {
    src_member_ = src_member;
    data_src_member_ = data_src_member;
    orig_paxos_replica_number_ = orig_paxos_replica_number;
    paxos_replica_number_ = paxos_replica_number;
  }
  return ret;
}

int ObLSTypeTransformTask::build_task_from_sql_result(
    const sqlclient::ObMySQLResult &res)
{
  int ret = OB_SUCCESS;
  READ_TASK_FROM_SQL_RES_FOR_DISASTER_RECOVERY
  common::ObAddr src_server;
  common::ObAddr dest_server;
  ObDstReplica dst_replica;
  if (OB_FAIL(ret)) {
  } else if (false == src_server.set_ip_addr(src_ip, static_cast<uint32_t>(src_port))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid server address", K(src_ip), K(src_port));
  } else if (false == dest_server.set_ip_addr(dest_ip, static_cast<uint32_t>(dest_port))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid server address", K(dest_ip), K(dest_port));
  } else {
    ObReplicaMember src_member;
    ObReplicaMember dest_member;
    if (OB_FAIL(src_member.init(src_server, 0, src_type_to_set))) {
      LOG_WARN("failed to init src_member", KR(ret), K(src_server), K(src_type_str), K(src_type_to_set));
    } else if (OB_FAIL(dest_member.init(dest_server, 0, dest_type_to_set))) {
      LOG_WARN("failed to init dest_member", KR(ret), K(dest_server), K(dest_type_str), K(dest_type_to_set));
    } else if (OB_FAIL(dst_replica.assign(0/*unit id*/, 0/*unit group id*/, task_key.get_zone(), dest_member))) {
      LOG_WARN("fail to init a ObDstReplica", KR(ret), K(task_key), K(dest_member));
    }
    //STEP3_0: to build a task
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(build(
                    task_key,                    //(in used)
                    task_key.get_tenant_id(),    //(in used)
                    task_key.get_ls_id(),        //(in used)
                    task_id_to_set,              //(in used)
                    schedule_time_us,
                    generate_time_us,
                    GCONF.cluster_id,            //(not used)cluster_id
                    0/*transmit_data_size*/,     //(not used)
                    is_manual ? obrpc::ObAdminClearDRTaskArg::TaskType::MANUAL : obrpc::ObAdminClearDRTaskArg::TaskType::AUTO,//(not used)invoked_source
                    priority_to_set,             //(not used)
                    comment_to_set.ptr(),        //comment
                    dst_replica,                 //(in used)dest_server
                    src_member,                  //(in used)src_server
                    src_member,                  //(not used)data_src_server
                    src_paxos_replica_number,                  //(in used)
                    dest_paxos_replica_number))) {             //(in used)
      LOG_WARN("fail to build a ObLSTypeTransformTask", KR(ret), K(task_key),
               K(task_id_to_set), K(schedule_time_us), K(generate_time_us),
               K(priority_to_set), K(dst_replica), K(src_member), K(comment_to_set), K(src_paxos_replica_number),
               K(dest_paxos_replica_number));
    } else {
      LOG_INFO("success to build a ObLSTypeTransformTask", KPC(this), K(task_id), K(task_id_to_set), K(task_id_sqlstring_format));
    }
  }
  return ret;
}

// ======================================== ObRemoveLSReplicaTask ======================================
int ObRemoveLSReplicaTask::get_execute_transmit_size(
    int64_t &execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  execute_transmit_size = 0;
  return ret;
}

int ObRemoveLSReplicaTask::get_virtual_disaster_recovery_task_stat(
    common::ObAddr &src,
    common::ObAddr &data_src,
    common::ObAddr &dst,
    common::ObAddr &offline) const
{
  int ret = OB_SUCCESS;
  UNUSED(src);
  UNUSED(data_src);
  dst = leader_;
  offline = remove_server_.get_server();
  return ret;
}

int ObRemoveLSReplicaTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  ROOTSERVICE_EVENT_ADD("disaster_recovery", get_log_start_str(),
                        "tenant_id", get_tenant_id(),
                        "ls_id", get_ls_id().id(),
                        "task_id", get_task_id(),
                        "leader", leader_,
                        "remove_server", remove_server_,
                        "comment", get_comment().ptr());
  return ret;
}


int ObRemoveLSReplicaTask::log_execute_result(
    const int ret_code,
    const ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  ObSqlString execute_result;
  int64_t start_time = execute_time_ > 0 ? execute_time_ : schedule_time_;
  if (OB_FAIL(build_execute_result(ret_code, ret_comment, start_time, execute_result))) {
    LOG_WARN("fail to build execute result", KR(ret), K(ret_code), K(ret_comment));
  } else {
    ROOTSERVICE_EVENT_ADD("disaster_recovery", get_log_finish_str(),
                          "tenant_id", get_tenant_id(),
                          "ls_id", get_ls_id().id(),
                          "task_id", get_task_id(),
                          "leader", leader_,
                          "remove_server", remove_server_,
                          "execute_result", execute_result,
                          get_comment().ptr());
  }
  return ret;
}

int ObRemoveLSReplicaTask::check_before_execute(
    share::ObLSTableOperator &lst_operator,
    ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  UNUSED(lst_operator);
  return ret;
}

int ObRemoveLSReplicaTask::execute(
    obrpc::ObSrvRpcProxy &rpc_proxy,
    int &ret_code,
    ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  if (ObDRTaskType::LS_REMOVE_PAXOS_REPLICA == get_disaster_recovery_task_type()) {
    ObLSDropPaxosReplicaArg arg;
    if (OB_FAIL(arg.init(
            get_task_id(),
            get_tenant_id(),
            get_ls_id(),
            get_remove_server(),
            get_orig_paxos_replica_number(),
            get_paxos_replica_number()))) {
      LOG_WARN("fail to init arg", KR(ret));
    } else if (OB_FAIL(rpc_proxy.to(get_dst_server())
          .by(get_tenant_id()).ls_remove_paxos_replica(arg))) {
      ret_code = ret;
      ret_comment = ObDRTaskRetComment::FAIL_TO_SEND_RPC;
      LOG_WARN("fail to send ls remove paxos replica rpc", KR(ret), K(arg));
    } else {
      LOG_INFO("start to execute ls remove paxos replica", K(arg));
    }
  } else if (ObDRTaskType::LS_REMOVE_NON_PAXOS_REPLICA == get_disaster_recovery_task_type()) {
    ObLSDropNonPaxosReplicaArg arg;
    if (OB_FAIL(arg.init(
            get_task_id(),
            get_tenant_id(),
            get_ls_id(),
            get_remove_server()))) {
      LOG_WARN("fail to init arg", KR(ret));
    } else if (OB_FAIL(rpc_proxy.to(get_dst_server())
          .by(get_tenant_id()).ls_remove_nonpaxos_replica(arg))) {
      ret_code = ret;
      ret_comment = ObDRTaskRetComment::FAIL_TO_SEND_RPC;
      LOG_WARN("fail to send ls remove nonpaxos replica", KR(ret), K(arg));
    } else {
      LOG_INFO("start to execute ls remove nonpaxos replica", K(arg));
    }
  } else {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task type not expected", KR(ret), "task_type", get_disaster_recovery_task_type());
  }
  return ret;
}

int ObRemoveLSReplicaTask::fill_dml_splicer(
    ObDMLSqlSplicer &dml_splicer) const
{
  int ret = OB_SUCCESS;
  char src_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char dest_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char target_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char task_id[OB_TRACE_STAT_BUFFER_SIZE] = "";
  const char *task_type_to_set = ob_disaster_recovery_task_type_strs(get_disaster_recovery_task_type());

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret));
  } else if (false == get_leader().ip_to_string(dest_ip, sizeof(dest_ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert dest_server ip to string failed", KR(ret), "dest_server", get_dst_server());
  } else if (false == get_remove_server().get_server().ip_to_string(target_ip, sizeof(target_ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert target_server ip to string failed", KR(ret), "target_server", get_remove_server().get_server());
  } else if (OB_FAIL(ObDRTask::fill_dml_splicer(dml_splicer))) {
    LOG_WARN("ObDRTask fill dml splicer failed", KR(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column("task_type", task_type_to_set))
          || OB_FAIL(dml_splicer.add_column("target_replica_svr_ip", target_ip))
          || OB_FAIL(dml_splicer.add_column("target_replica_svr_port", get_remove_server().get_server().get_port()))
          || OB_FAIL(dml_splicer.add_column("target_paxos_replica_number", get_paxos_replica_number()))
          || OB_FAIL(dml_splicer.add_column("target_replica_type", ObShareUtil::replica_type_to_string(get_remove_server().get_replica_type())))
          || OB_FAIL(dml_splicer.add_column("source_replica_svr_ip", src_ip))
          || OB_FAIL(dml_splicer.add_column("source_replica_svr_port", 0))
          || OB_FAIL(dml_splicer.add_column("source_paxos_replica_number", get_orig_paxos_replica_number()))
          || OB_FAIL(dml_splicer.add_column("source_replica_type", ""))
          || OB_FAIL(dml_splicer.add_column("task_exec_svr_ip", dest_ip))
          || OB_FAIL(dml_splicer.add_column("task_exec_svr_port", get_leader().get_port()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(fill_dml_splicer_for_new_column(dml_splicer, common::ObAddr()))) {
    LOG_WARN("fill dml_splicer for new column failed", KR(ret));
  }
  return ret;
}

int64_t ObRemoveLSReplicaTask::get_clone_size() const
{
  return sizeof(*this);
}

int ObRemoveLSReplicaTask::clone(
    void *input_ptr,
    ObDRTask *&output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    ObRemoveLSReplicaTask *my_task = new (input_ptr) ObRemoveLSReplicaTask();
    if (OB_ISNULL(my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to construct", KR(ret));
    } else if (OB_FAIL(my_task->deep_copy(*this))) {
      LOG_WARN("fail to deep copy", KR(ret));
    } else {
      my_task->set_leader(get_leader());
      my_task->set_remove_server(get_remove_server());
      my_task->set_orig_paxos_replica_number(get_orig_paxos_replica_number());
      my_task->set_paxos_replica_number(get_paxos_replica_number());
      my_task->set_replica_type(get_replica_type());
      output_task = my_task;
    }
  }
  return ret;
}

int ObRemoveLSReplicaTask::build(
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
    const ObReplicaType &replica_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!leader.is_valid()
                  || !remove_server.is_valid()
                  || orig_paxos_replica_number <= 0
                  || paxos_replica_number <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(leader),
             K(remove_server),
             K(orig_paxos_replica_number),
             K(paxos_replica_number));
  } else if (OB_FAIL(ObDRTask::build(
          task_key,
          tenant_id,
          ls_id,
          task_id,
          schedule_time_us,
          generate_time_us,
          cluster_id,
          transmit_data_size,
          invoked_source,
          priority,
          comment))) {
    LOG_WARN("fail to build ObDRTask", KR(ret),
             K(task_key),
             K(tenant_id),
             K(ls_id),
             K(transmit_data_size),
             K(invoked_source),
             K(priority));
  } else {
    set_leader(leader);
    set_remove_server(remove_server);
    orig_paxos_replica_number_ = orig_paxos_replica_number;
    paxos_replica_number_ = paxos_replica_number;
    replica_type_ = replica_type;
  }
  return ret;
}

int ObRemoveLSReplicaTask::simple_build(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const share::ObTaskId &task_id,
    const common::ObAddr &leader,
    const common::ObReplicaMember &remove_server,
    const int64_t orig_paxos_replica_number,
    const int64_t paxos_replica_number,
    const ObReplicaType &replica_type)
{
  int ret = OB_SUCCESS;
  ObDRTaskKey task_key;
  ObDRTaskType task_type = ObDRTaskType::MAX_TYPE;
  common::ObZone zone;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
               || !ls_id.is_valid_with_tenant(tenant_id)
               || !task_id.is_valid()
               || !leader.is_valid()
               || !remove_server.is_valid()
               || orig_paxos_replica_number <= 0
               || paxos_replica_number <= 0
               || !ObReplicaTypeCheck::is_replica_type_valid(replica_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(task_id),
             K(leader), K(remove_server), K(orig_paxos_replica_number),
             K(paxos_replica_number), K(replica_type));
  } else if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_type)) {
    task_type = ObDRTaskType::LS_REMOVE_PAXOS_REPLICA;
  } else {
    task_type = ObDRTaskType::LS_REMOVE_NON_PAXOS_REPLICA;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(SVR_TRACER.get_server_zone(leader, zone))) {
    LOG_WARN("get server zone failed", KR(ret), K(leader));
  } else if (OB_FAIL(task_key.init(tenant_id, ls_id, zone, task_type))) {
    LOG_WARN("fail to init task key", KR(ret), K(tenant_id), K(ls_id), K(zone));
  } else if (OB_FAIL(ObDRTask::build(
                        task_key,
                        tenant_id,
                        ls_id,
                        task_id,
                        0,/*schedule_time_us*/ 0,/*generate_time_us*/
                        GCONF.cluster_id, 0,/*transmit_data_size*/
                        obrpc::ObAdminClearDRTaskArg::TaskType::MANUAL,
                        ObDRTaskPriority::HIGH_PRI,
                        ObString(drtask::ALTER_SYSTEM_COMMAND_REMOVE_REPLICA)))) {
    LOG_WARN("fail to build ObDRTask", KR(ret), K(task_key), K(tenant_id), K(ls_id), K(task_id));
  } else {
    leader_ = leader;
    remove_server_ = remove_server;
    orig_paxos_replica_number_ = orig_paxos_replica_number;
    paxos_replica_number_ = paxos_replica_number;
    replica_type_ = replica_type;
  }
  return ret;
}

int ObRemoveLSReplicaTask::build_task_from_sql_result(
    const sqlclient::ObMySQLResult &res)
{
  int ret = OB_SUCCESS;
  READ_TASK_FROM_SQL_RES_FOR_DISASTER_RECOVERY
  common::ObAddr dest_server;
  common::ObAddr execute_server;
  if (OB_FAIL(ret)) {
  } else if (false == dest_server.set_ip_addr(dest_ip, static_cast<uint32_t>(dest_port))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid server address", K(dest_ip), K(dest_port));
  } else if (false == execute_server.set_ip_addr(execute_ip, static_cast<uint32_t>(execute_port))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid server address", K(execute_ip), K(execute_port));
  } else {
    if (0 == task_type_str.case_compare(ob_disaster_recovery_task_type_strs(ObDRTaskType::LS_REMOVE_PAXOS_REPLICA))) {
      if (OB_UNLIKELY(REPLICA_TYPE_FULL != dest_type_to_set)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task_type and replica_type do not match", KR(ret), K(task_type_str), K(dest_type_to_set));
      }
    } else if (0 == task_type_str.case_compare(ob_disaster_recovery_task_type_strs(ObDRTaskType::LS_REMOVE_NON_PAXOS_REPLICA))) {
      if (OB_UNLIKELY(! ObReplicaTypeCheck::is_non_paxos_replica(dest_type_to_set))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task_type and replica_type do not match", KR(ret), K(task_type_str), K(dest_type_to_set));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected task_type", KR(ret), K(task_type_str));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(build(
                    task_key,                    //(in used)
                    task_key.get_tenant_id(),    //(in used)
                    task_key.get_ls_id(),        //(in used)
                    task_id_to_set,              //(in used)
                    schedule_time_us,
                    generate_time_us,
                    GCONF.cluster_id,            //(not used)cluster_id
                    0/*transmit_data_size*/,     //(not used)
                    is_manual ? obrpc::ObAdminClearDRTaskArg::TaskType::MANUAL : obrpc::ObAdminClearDRTaskArg::TaskType::AUTO,//(not used)invoked_source
                    priority_to_set,             //(not used)
                    comment_to_set.ptr(),        //comment
                    execute_server,                 //(in used)leader
                    ObReplicaMember(dest_server, 0), //(in used)dest_server
                    src_paxos_replica_number,                  //(in used)
                    dest_paxos_replica_number,                 //(in used)
                    dest_type_to_set))) {            //(in used)
    LOG_WARN("fail to build a ObRemoveLSReplicaTask", KR(ret));
  } else {
    LOG_INFO("success to build a ObRemoveLSReplicaTask", KPC(this), K(task_id), K(task_id_to_set), K(task_id_sqlstring_format));
  }
  return ret;
}

// ================================== ObLSModifyPaxosReplicaNumberTask ==================================
int ObLSModifyPaxosReplicaNumberTask::get_execute_transmit_size(
    int64_t &execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  execute_transmit_size = 0;
  return ret;
}

int ObLSModifyPaxosReplicaNumberTask::get_virtual_disaster_recovery_task_stat(
    common::ObAddr &src,
    common::ObAddr &data_src,
    common::ObAddr &dst,
    common::ObAddr &offline) const
{
  int ret = OB_SUCCESS;
  UNUSED(src);
  UNUSED(data_src);
  dst = server_;
  UNUSED(offline);
  return ret;
}

int ObLSModifyPaxosReplicaNumberTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  ObSqlString paxos_replica_number;
  if (OB_FAIL(paxos_replica_number.append_fmt(
              "orig_paxos_replica_number:%ld target_paxos_replica_number:%ld",
              orig_paxos_replica_number_, paxos_replica_number_ ))) {
    LOG_WARN("fail to append to paxos_replica_number", KR(ret),
             K(orig_paxos_replica_number_), K(paxos_replica_number_));
  } else {
    ROOTSERVICE_EVENT_ADD("disaster_recovery", get_log_start_str(),
                          "tenant_id", get_tenant_id(),
                          "ls_id", get_ls_id().id(),
                          "task_id", get_task_id(),
                          "destination", server_,
                          "change_of_paxos_replica_number", paxos_replica_number.ptr(),
                          "comment", get_comment().ptr());
  }
  return ret;
}


int ObLSModifyPaxosReplicaNumberTask::log_execute_result(
    const int ret_code,
    const ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  ObSqlString execute_result;
  int64_t start_time = execute_time_ > 0 ? execute_time_ : schedule_time_;
  if (OB_FAIL(build_execute_result(ret_code, ret_comment, start_time, execute_result))) {
    LOG_WARN("fail to build execute result", KR(ret), K(ret_code), K(ret_comment));
  } else {
    ROOTSERVICE_EVENT_ADD("disaster_recovery", get_log_finish_str(),
                          "tenant_id", get_tenant_id(),
                          "ls_id", get_ls_id().id(),
                          "task_id", get_task_id(),
                          "execute_result", execute_result,
                          "orig_paxos_replica_number", orig_paxos_replica_number_,
                          "paxos_replica_number", paxos_replica_number_);
  }
  return ret;
}

int ObLSModifyPaxosReplicaNumberTask::check_before_execute(
    share::ObLSTableOperator &lst_operator,
    ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;
  UNUSED(lst_operator);
  return ret;
}

int ObLSModifyPaxosReplicaNumberTask::execute(
    obrpc::ObSrvRpcProxy &rpc_proxy,
    int &ret_code,
    ObDRTaskRetComment &ret_comment) const
{
  int ret = OB_SUCCESS;

  ObLSModifyPaxosReplicaNumberArg arg;
  if (OB_FAIL(arg.init(
          get_task_id(),
          get_tenant_id(),
          get_ls_id(),
          get_orig_paxos_replica_number(),
          get_paxos_replica_number(),
          get_member_list()))) {
    LOG_WARN("fail to init arg", KR(ret));
  } else if (OB_FAIL(rpc_proxy.to(get_dst_server())
        .by(get_tenant_id()).ls_modify_paxos_replica_number(arg))) {
    ret_code = ret;
    ret_comment = ObDRTaskRetComment::FAIL_TO_SEND_RPC;
    LOG_WARN("fail to send ls modify paxos replica number rpc", KR(ret), K(arg));
  } else {
    LOG_INFO("start to execute ls modify paxos replica number", K(arg));
  }
  return ret;
}

int ObLSModifyPaxosReplicaNumberTask::fill_dml_splicer(
    ObDMLSqlSplicer &dml_splicer) const
{
  int ret = OB_SUCCESS;
  char src_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char dest_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char target_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char task_id[OB_TRACE_STAT_BUFFER_SIZE] = "";
  char task_type[MAX_DISASTER_RECOVERY_TASK_TYPE_LENGTH] = "MODIFY PAXOS REPLICA NUMBER";
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret));
  } else if (false == get_dst_server().ip_to_string(dest_ip, sizeof(dest_ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert dest_server ip to string failed", KR(ret), "dest_server", get_dst_server());
  } else if (OB_FAIL(ObDRTask::fill_dml_splicer(dml_splicer))) {
    LOG_WARN("ObDRTask fill dml splicer failed", KR(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column("task_type", task_type))
          || OB_FAIL(dml_splicer.add_column("target_replica_svr_ip", dest_ip))
          || OB_FAIL(dml_splicer.add_column("target_replica_svr_port", get_dst_server().get_port()))
          || OB_FAIL(dml_splicer.add_column("target_paxos_replica_number", get_paxos_replica_number()))
          || OB_FAIL(dml_splicer.add_column("target_replica_type", ""))
          || OB_FAIL(dml_splicer.add_column("source_replica_svr_ip", src_ip))
          || OB_FAIL(dml_splicer.add_column("source_replica_svr_port", 0))
          || OB_FAIL(dml_splicer.add_column("source_paxos_replica_number", get_orig_paxos_replica_number()))
          || OB_FAIL(dml_splicer.add_column("source_replica_type", ""))
          || OB_FAIL(dml_splicer.add_column("task_exec_svr_ip", dest_ip))
          || OB_FAIL(dml_splicer.add_column("task_exec_svr_port", get_dst_server().get_port()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(fill_dml_splicer_for_new_column(dml_splicer, common::ObAddr()))) {
    LOG_WARN("fill dml_splicer for new column failed", KR(ret));
  }
  return ret;
}

int64_t ObLSModifyPaxosReplicaNumberTask::get_clone_size() const
{
  return sizeof(*this);
}

int ObLSModifyPaxosReplicaNumberTask::clone(
    void *input_ptr,
    ObDRTask *&output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    ObLSModifyPaxosReplicaNumberTask *my_task = new (input_ptr) ObLSModifyPaxosReplicaNumberTask();
    if (OB_UNLIKELY(nullptr == my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to construct", KR(ret));
    } else if (OB_FAIL(my_task->deep_copy(*this))) {
      LOG_WARN("fail to deep copy", KR(ret));
    } else {
      my_task->set_server(get_server());
      my_task->set_orig_paxos_replica_number(get_orig_paxos_replica_number());
      my_task->set_paxos_replica_number(get_paxos_replica_number());
      my_task->set_member_list(get_member_list());
      output_task = my_task;
    }
  }
  return ret;
}

int ObLSModifyPaxosReplicaNumberTask::build(
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
    const common::ObMemberList &member_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!dst_server.is_valid()
                  || orig_paxos_replica_number <= 0
                  || paxos_replica_number <= 0
                  || !member_list.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(dst_server),
             K(orig_paxos_replica_number),
             K(paxos_replica_number),
             K(member_list));
  } else if (OB_FAIL(ObDRTask::build(
          task_key,
          tenant_id,
          ls_id,
          task_id,
          schedule_time_us,
          generate_time_us,
          cluster_id,
          transmit_data_size,
          invoked_source,
          priority,
          comment))) {
    LOG_WARN("fail to build ObDRTask", KR(ret),
             K(task_key),
             K(tenant_id),
             K(ls_id),
             K(transmit_data_size),
             K(invoked_source),
             K(priority));
  } else {
    set_server(dst_server);
    set_orig_paxos_replica_number(orig_paxos_replica_number);
    set_paxos_replica_number(paxos_replica_number);
    set_member_list(member_list);
  }
  return ret;
}

int ObLSModifyPaxosReplicaNumberTask::simple_build(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const share::ObTaskId &task_id,
    const common::ObAddr &dst_server,
    const int64_t orig_paxos_replica_number,
    const int64_t paxos_replica_number,
    const common::ObMemberList &member_list)
{
  int ret = OB_SUCCESS;
  ObDRTaskKey task_key;
  common::ObZone zone;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
               || !ls_id.is_valid_with_tenant(tenant_id)
               || !task_id.is_valid()
               || !dst_server.is_valid()
               || orig_paxos_replica_number <= 0
               || paxos_replica_number <= 0
               || !member_list.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(task_id),
             K(dst_server), K(orig_paxos_replica_number),
             K(paxos_replica_number), K(member_list));
  } else if (OB_FAIL(SVR_TRACER.get_server_zone(dst_server, zone))) {
    LOG_WARN("get server zone failed", KR(ret), K(dst_server));
  } else if (OB_FAIL(task_key.init(tenant_id, ls_id, zone,
                                   ObDRTaskType::LS_MODIFY_PAXOS_REPLICA_NUMBER))) {
    LOG_WARN("fail to init task key", KR(ret), K(tenant_id), K(ls_id), K(zone));
  } else if (OB_FAIL(ObDRTask::build(
          task_key,
          tenant_id,
          ls_id,
          task_id,
          0,/*schedule_time_us*/ 0,/*generate_time_us*/
          GCONF.cluster_id, 0,/*transmit_data_size*/
          obrpc::ObAdminClearDRTaskArg::TaskType::MANUAL,
          ObDRTaskPriority::HIGH_PRI,
          ObString(drtask::ALTER_SYSTEM_COMMAND_MODIFY_PAXOS_REPLICA_NUM)))) {
    LOG_WARN("fail to build ObDRTask", KR(ret),
              K(task_key), K(tenant_id), K(ls_id), K(task_id));
  } else {
    orig_paxos_replica_number_ = orig_paxos_replica_number;
    paxos_replica_number_ = paxos_replica_number;
    server_ = dst_server;
    member_list_ = member_list;
  }
  return ret;
}

int ObLSModifyPaxosReplicaNumberTask::build_task_from_sql_result(
    const sqlclient::ObMySQLResult &res)
{
  int ret = OB_SUCCESS;
  READ_TASK_FROM_SQL_RES_FOR_DISASTER_RECOVERY
  common::ObAddr execute_server;
  common::ObMemberList member_list;
  if (OB_FAIL(ret)) {
  } else if (false == execute_server.set_ip_addr(execute_ip, static_cast<uint32_t>(execute_port))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid server address", K(execute_ip), K(execute_port));
  } else if (OB_FAIL(member_list.add_member(ObMember(execute_server, 0)))) {
    // this field is not accurate and will not be used by tasks that are reloaded into memory.
    // it just ensures that the task build is valid.
    LOG_WARN("fail to add server to member list", KR(ret), K(execute_server));
  } else if (OB_FAIL(build(
                    task_key,                    //(in used)
                    task_key.get_tenant_id(),    //(in used)
                    task_key.get_ls_id(),        //(in used)
                    task_id_to_set,              //(in used)
                    schedule_time_us,
                    generate_time_us,
                    GCONF.cluster_id,            //(not used)cluster_id
                    0/*transmit_data_size*/,     //(not used)
                    is_manual ? obrpc::ObAdminClearDRTaskArg::TaskType::MANUAL : obrpc::ObAdminClearDRTaskArg::TaskType::AUTO,//(not used)invoked_source
                    priority_to_set,             //(not used)
                    comment_to_set.ptr(),        //comment
                    execute_server,                 //(in used)leader
                    src_paxos_replica_number,                  //(in used)
                    dest_paxos_replica_number,                 //(in used)
                    member_list))) {
    LOG_WARN("fail to build a ObLSModifyPaxosReplicaNumberTask", KR(ret));
  } else {
    LOG_INFO("success to build a ObLSModifyPaxosReplicaNumberTask", KPC(this), K(task_id), K(task_id_to_set), K(task_id_sqlstring_format));
  }
  return ret;
}
} // end namespace rootserver
} // end namespace oceanbase
