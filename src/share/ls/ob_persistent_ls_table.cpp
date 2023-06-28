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

#define USING_LOG_PREFIX SHARE_PT

#include "ob_persistent_ls_table.h"               // for ObPersistentLSTable's functions
#include "share/config/ob_server_config.h"        // for ObServerConfig
#include "observer/omt/ob_tenant_timezone_mgr.h"  // for OTTZ_MGR.get_tenant_tz
#include "lib/ob_define.h"                        // for pure tenant related
#include "share/ls/ob_ls_table.h"                 // for OB_ALL_LS_META_TABLE_TNAME
#include "lib/mysqlclient/ob_isql_client.h"       // for ObISQLClient
#include "logservice/palf/log_define.h"           // for INVALID_PROPOSAL_ID
#include "observer/ob_server_struct.h"            // for GCTX
#include "share/inner_table/ob_inner_table_schema_constants.h" // for xxx_TNAME

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace share;

common::ObISQLClient& ObPersistentLSTable::AutoTransProxy::get_sql_client()
{
  return trans_.is_started()
      ? static_cast<ObISQLClient &>(trans_)
      : static_cast<ObISQLClient &>(sql_proxy_);
}

int ObPersistentLSTable::AutoTransProxy::start_trans(
    uint64_t sql_tenant_id,
    bool with_snapshot)
{
  int ret = OB_SUCCESS;
  if (trans_.is_started()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("transaction already started", KR(ret));
  } else if (OB_FAIL(trans_.start(
                       &sql_proxy_,
                       sql_tenant_id,
                       with_snapshot))) {
    LOG_WARN("start transaction failed", KR(ret), K(sql_tenant_id),
             K(with_snapshot));
  }
  return ret;
}

int ObPersistentLSTable::AutoTransProxy::end_trans(const bool is_succ)
{
  int ret = OB_SUCCESS;
  if (trans_.is_started()) {
    if (OB_FAIL(trans_.end(is_succ))) {
      LOG_WARN("end transaction failed", KR(ret), K(is_succ));
    }
  }
  return ret;
}

ObPersistentLSTable::ObPersistentLSTable()
  : ObLSTable(),
    inited_(false),
    sql_proxy_(nullptr),
    config_(nullptr)
{
}

ObPersistentLSTable::~ObPersistentLSTable()
{
}

int ObPersistentLSTable::init(ObISQLClient &sql_proxy, ObServerConfig *config)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("int twice", KR(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    config_ = config;
    inited_ = true;
  }
  return ret;
}

int ObPersistentLSTable::get(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObLSTable::Mode mode,
    ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  const bool filter_flag_replica = true;
  const bool lock_replica = false;
  UNUSED(mode);
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPersistentLSTable not init", KR(ret));
  } else if (!is_valid_key(tenant_id, ls_id)
            || OB_ISNULL(sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KT(tenant_id), K(ls_id), KP_(sql_proxy));
  } else if (OB_FAIL(fetch_ls_info_(lock_replica, filter_flag_replica, cluster_id,
                                    tenant_id, ls_id, *sql_proxy_, ls_info))) {
    LOG_WARN("get log stream table failed", KR(ret), K(cluster_id), KT(tenant_id),
             K(ls_id), K(filter_flag_replica));
  }
  return ret;
}

int ObPersistentLSTable::fetch_ls_info_(
    const bool lock_replica,
    const bool filter_flag_replica,
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObISQLClient &sql_client,
    ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  const char *table_name_meta = OB_ALL_LS_META_TABLE_TNAME;
  const uint64_t sql_tenant_id = get_private_table_exec_tenant_id(tenant_id);
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPersistentLSTable not init", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
      || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KT(tenant_id), K(ls_id));
  } else if (OB_INVALID_TENANT_ID == sql_tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sql_tenant_id", KR(ret), K(sql_tenant_id));
  } else if (OB_FAIL(ls_info.init(tenant_id, ls_id))) {
    LOG_WARN("fail to init ls info", KR(ret), K(tenant_id), K(ls_id));
  } else {
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql.assign_fmt(
          "SELECT * FROM %s"
          " WHERE tenant_id = %lu AND ls_id = %lu"
          " ORDER BY tenant_id, ls_id, svr_ip, svr_port ASC%s",
          table_name_meta, tenant_id, ls_id.id(),
          lock_replica ? " FOR UPDATE" : ""))) {
        LOG_WARN("assign sql string failed", KR(ret), K(tenant_id),
                K(ls_id), K(lock_replica));
      } else if (OB_FAIL(sql_client.read(result, cluster_id, sql_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(cluster_id), K(sql_tenant_id), "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed");
      } else if (OB_FAIL(construct_ls_info(
          *result.get_result(),
          filter_flag_replica,
          ls_info))) {
        LOG_WARN("construct log stream info failed",
            KR(ret), K(filter_flag_replica), K(ls_info));
      }
    }
  }
  return ret;
}

int ObPersistentLSTable::construct_ls_replica(
    common::sqlclient::ObMySQLResult &res,
    ObLSReplica &replica)
{
  int ret = OB_SUCCESS;
  // location related
  int64_t create_time_us = 0;
  int64_t modify_time_us = 0;
  int64_t tenant_id = 0;
  int64_t ls_id = ObLSID::INVALID_LS_ID;
  ObString ip;
  common::ObAddr server;
  int64_t port = 0;
  int64_t sql_port = 0;
  int64_t role = 0;    // INVALID_ROLE
  ObString member_list;
  ObLSReplica::MemberList member_list_to_set;
  ObReplicaStatus status;
  int64_t replica_type = 0;
  int64_t proposal_id = palf::INVALID_PROPOSAL_ID;
  ObString status_str;
  ObString default_status("NORMAL");
  int64_t restore_status = 0;
  int64_t memstore_percent = 100;
  // meta related
  int64_t unit_id = 0;
  ObString zone;
  int64_t paxos_replica_number = OB_INVALID_COUNT;
  int64_t data_size = 0;
  int64_t required_size = 0;
  ObString learner_list;
  GlobalLearnerList learner_list_to_set;
  int64_t rebuild_flag = 0;
  // TODO: try to fetch coulmn_value by column_name
  // column select order defined in LSTableColNames::LSTableColNames
  // location related
  (void)GET_COL_IGNORE_NULL(res.get_int, "tenant_id", tenant_id);
  {
    ObTimeZoneInfoWrap tz_info_wrap;
    ObTZMapWrap tz_map_wrap;
    OZ(OTTZ_MGR.get_tenant_tz(tenant_id, tz_map_wrap));
    tz_info_wrap.set_tz_info_map(tz_map_wrap.get_tz_map());
    (void)GET_COL_IGNORE_NULL(res.get_timestamp, "gmt_create", tz_info_wrap.get_time_zone_info(), create_time_us);
    (void)GET_COL_IGNORE_NULL(res.get_timestamp, "gmt_modified", tz_info_wrap.get_time_zone_info(), modify_time_us);
  }
  (void)GET_COL_IGNORE_NULL(res.get_int, "ls_id", ls_id);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "svr_ip", ip);
  (void)GET_COL_IGNORE_NULL(res.get_int, "svr_port", port);
  (void)GET_COL_IGNORE_NULL(res.get_int, "sql_port", sql_port);
  (void)GET_COL_IGNORE_NULL(res.get_int, "role", role);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "member_list", member_list);
  (void)GET_COL_IGNORE_NULL_WITH_DEFAULT_VALUE(res.get_int, "proposal_id", proposal_id, 0);
  (void)GET_COL_IGNORE_NULL(res.get_int, "replica_type", replica_type);
  (void)GET_COL_IGNORE_NULL_WITH_DEFAULT_VALUE(res.get_varchar, "replica_status", status_str, default_status);
  (void)GET_COL_IGNORE_NULL_WITH_DEFAULT_VALUE(res.get_int, "restore_status", restore_status, 0);
  (void)GET_COL_IGNORE_NULL_WITH_DEFAULT_VALUE(res.get_int, "memstore_percent", memstore_percent, 100/* default 100 */);
  // meta related
  (void)GET_COL_IGNORE_NULL(res.get_int, "unit_id", unit_id);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "zone", zone);
  (void)GET_COL_IGNORE_NULL_WITH_DEFAULT_VALUE(res.get_int, "paxos_replica_number", paxos_replica_number, OB_INVALID_COUNT);
  (void)GET_COL_IGNORE_NULL(res.get_int, "data_size", data_size);
  (void)GET_COL_IGNORE_NULL_WITH_DEFAULT_VALUE(res.get_int, "required_size", required_size, 0);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(res, "learner_list", learner_list);
  EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(res, "rebuild", rebuild_flag, int64_t, true, true, 0);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObLSReplica::text2member_list(
                to_cstring(member_list),
                member_list_to_set))) {
    LOG_WARN("text2member_list failed", KR(ret));
  } else if (OB_FAIL(ObLSReplica::text2learner_list(
                to_cstring(learner_list),
                learner_list_to_set))) {
    LOG_WARN("text2member_list for learner_list failed", KR(ret));
  } else if (false == server.set_ip_addr(ip, static_cast<uint32_t>(port))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid server address", K(ip), K(port));
  } else {
    char buf[MAX_REPLICA_STATUS_LENGTH];
    int64_t pos = status_str.to_string(buf, MAX_REPLICA_STATUS_LENGTH);
    if (pos != status_str.length()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to write string to buf", KR(ret), K(status_str));
    } else if (OB_FAIL(get_replica_status(buf, status))) {
      LOG_WARN("fail to get replica status", KR(ret), K(status_str));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(replica.init(
      create_time_us,
      modify_time_us,
      tenant_id,
      ObLSID(ls_id),
      server,
      sql_port,
      static_cast<ObRole>(role),
      (ObReplicaType)replica_type,
      proposal_id,
      status,
      ObLSRestoreStatus((ObLSRestoreStatus::Status)restore_status),
      memstore_percent,
      static_cast<uint64_t>(unit_id),
      zone,
      paxos_replica_number,
      data_size,
      required_size,
      member_list_to_set,
      learner_list_to_set,
      0 != rebuild_flag))) {
    LOG_WARN("fail to init a ls replica", KR(ret), K(create_time_us), K(modify_time_us),
              K(tenant_id), K(ls_id), K(server), K(sql_port), K(role),
              K(replica_type), K(proposal_id), K(unit_id), K(zone),
              K(paxos_replica_number), K(member_list_to_set), K(learner_list_to_set));
  }

  LOG_DEBUG("construct log stream replica", KR(ret), K(replica));
  return ret;
}

int ObPersistentLSTable::construct_ls_info(
    common::sqlclient::ObMySQLResult &res,
    const bool filter_flag_replica,
    ObLSInfo &ls_info)
{
  UNUSED(filter_flag_replica);
  int ret = OB_SUCCESS;
  ObLSReplica replica;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(res.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next result failed", KR(ret));
      }
      break;
    }

    replica.reset();
    if (OB_FAIL(construct_ls_replica(res, replica))) {
      LOG_WARN("construct_ls_replica failed", KR(ret));
    } else if (OB_FAIL(ls_info.add_replica(replica))) {
      LOG_WARN("log stream add replica failed", KR(ret), K(replica), K(ls_info));
    }
  }

  // TODO: need make sure actions of this function
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ls_info.update_replica_status())) {
      LOG_WARN("update ls replica status failed", KR(ret), K(ls_info));
    }
  }
  return ret;
}

int ObPersistentLSTable::update(
    const ObLSReplica &replica,
    const bool inner_table_only)
{
  int ret = OB_SUCCESS;
  UNUSED(inner_table_only);
  DEBUG_SYNC(BEFORE_UPDATE_LS_META_TABLE);
  const bool lock_replica = replica.is_strong_leader();
  AutoTransProxy proxy(*sql_proxy_);
  bool with_snapshot = false;
  ObLSReplica new_replica;
  uint64_t sql_tenant_id = get_private_table_exec_tenant_id(replica.get_tenant_id());
  if (OB_UNLIKELY(!is_inited()) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPersistentLSTable not init", KR(ret), KP_(sql_proxy));
  } else if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica));
  } else if (lock_replica && OB_FAIL(proxy.start_trans(sql_tenant_id, with_snapshot))) {
    LOG_WARN("start transaction failed", KR(ret), K(with_snapshot), K(sql_tenant_id));
  } else if (OB_FAIL(new_replica.assign(replica))) {
    LOG_WARN("failed to assign new_replica", KR(ret));
  } else if (is_follower(new_replica.get_role())) {
    // aim to set proposal_id to 0 here
    (void)new_replica.update_to_follower_role();
  } else {
    // make sure it is a valid leader replica
    ObRole role = LEADER;
    int tmp_ret = OB_SUCCESS;
    if (replica.get_proposal_id() <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("change leader epoch must > 0 for leader replica", KR(ret), K(replica));
    } else if (OB_SUCCESS != (tmp_ret = get_role(
        replica.get_tenant_id(),
        replica.get_ls_id(),
        role))) {
      LOG_WARN("get role from ObLS failed", KR(ret), K(replica));
      role = FOLLOWER;
    }
    // if role turned from leader to follower, reset proposal_id to 0
    new_replica.set_role(role);
    if (is_follower(role)) {
      (void)new_replica.update_to_follower_role();
    }
  }

  if (OB_SUCC(ret)) {
    int64_t max_proposal_id = palf::INVALID_PROPOSAL_ID;
    // update leader
    if (new_replica.is_strong_leader()) {
      if (OB_FAIL(lock_lines_in_meta_table_for_leader_update_(
                      GCONF.cluster_id,
                      proxy.get_sql_client(),
                      replica.get_tenant_id(),
                      replica.get_ls_id(),
                      max_proposal_id))) {
        // lock lines in meta table for parallely update leader cases
        LOG_WARN("fail to lock lines in meta table", KR(ret),"tenant_id",
                 replica.get_tenant_id(), "ls_id", replica.get_ls_id().id());
      } else if (max_proposal_id != palf::INVALID_PROPOSAL_ID
          && new_replica.get_proposal_id() < max_proposal_id) {
        // this replica is old leader, set it to follower
        new_replica.update_to_follower_role();
      } else if (OB_FAIL(set_role_(
                        proxy.get_sql_client(),
                        replica.get_tenant_id(),
                        replica.get_ls_id(),
                        replica.get_server()))) {
        // new_replica is leader, set other replicas to follower before update
        LOG_WARN("fail to set other replicas to follower", KR(ret), K(replica));
      } else if (OB_FAIL(update_replica_(proxy.get_sql_client(), new_replica))) {
        LOG_WARN("update leader replica failed", KR(ret), K(new_replica));
      }
    }
    // update follower
    if (OB_SUCC(ret) && !new_replica.is_strong_leader()) {
      if (OB_FAIL(update_replica_(proxy.get_sql_client(), new_replica))) {
        // new_replica is follower, just update replica
        LOG_WARN("update follower replica failed", KR(ret), K(new_replica));
      }
    }
  }

  if (proxy.is_trans_started()) {
    int trans_ret = proxy.end_trans(OB_SUCCESS == ret);
    if (OB_SUCCESS != trans_ret) {
      LOG_WARN("end transaction failed", K(trans_ret));
      ret = OB_SUCCESS == ret ? trans_ret : ret;
    }
  }

  return ret;
}

int ObPersistentLSTable::lock_lines_in_meta_table_for_leader_update_(
    const int64_t cluster_id,
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    int64_t &max_proposal_id)
{
  int ret = OB_SUCCESS;
  max_proposal_id = palf::INVALID_PROPOSAL_ID;
  ObSqlString lock_for_leader_sql;
  ObLSInfo ls_info;
  const ObLSReplica *replica = NULL;
  const char *table_name_meta = OB_ALL_LS_META_TABLE_TNAME;
  const uint64_t sql_tenant_id = get_private_table_exec_tenant_id(tenant_id);
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPersistentLSTable not init", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
      || !ls_id.is_valid()
      || OB_INVALID_CLUSTER_ID == cluster_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KT(tenant_id), K(ls_id), K(cluster_id));
  } else if (OB_INVALID_TENANT_ID == sql_tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sql_tenant_id", KR(ret), K(sql_tenant_id));
  } else if (OB_FAIL(ls_info.init(tenant_id, ls_id))) {
    LOG_WARN("fail to init ls info", KR(ret), K(tenant_id), K(ls_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(lock_for_leader_sql.assign_fmt(
          "SELECT * FROM %s"
          " WHERE tenant_id = %lu AND ls_id = %lu"
          " ORDER BY tenant_id, ls_id, svr_ip, svr_port FOR UPDATE",
          table_name_meta, tenant_id, ls_id.id()))) {
        LOG_WARN("assign sql string failed", KR(ret), K(tenant_id),
                 "ls_id", ls_id.id());
      } else if (OB_FAIL(sql_client.read(result, cluster_id, sql_tenant_id, lock_for_leader_sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(cluster_id), K(sql_tenant_id), "sql", lock_for_leader_sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(cluster_id), K(sql_tenant_id), "sql", lock_for_leader_sql.ptr());
      } else if (OB_FAIL(construct_ls_info(
          *result.get_result(),
          false/*unused*/,
          ls_info))) {
        LOG_WARN("construct log stream info failed", KR(ret), K(ls_info));
      } else if (OB_FAIL(ls_info.find_leader(replica))) {
        if (OB_ENTRY_NOT_EXIST == ret) { // ls meta table is empty or ls doesn't have leader in table
          ret = OB_SUCCESS;
          max_proposal_id = palf::INVALID_PROPOSAL_ID;
        } else {
          LOG_WARN("fail to find leader", KR(ret), K(cluster_id), K(tenant_id), K(ls_id), K(ls_info));
        }
      } else if (OB_ISNULL(replica)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get null ptr from ls_info", KR(ret), K(cluster_id), K(tenant_id), K(ls_id), K(ls_info));
      } else {
        max_proposal_id = replica->get_proposal_id();
      }
    }
  }
  return ret;
}

int ObPersistentLSTable::set_role_(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const common::ObAddr &leader_server)
{
  int ret = OB_SUCCESS;
  const ObRole follower_role = FOLLOWER;
  const ObRole leader_role = LEADER;
  const int64_t proposal_id_for_follower = 0;
  const char *table_name = OB_ALL_LS_META_TABLE_TNAME;
  int64_t affected_rows = 0;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  ObSqlString sql;
  const uint64_t sql_tenant_id = get_private_table_exec_tenant_id(tenant_id);
  if (OB_INVALID_TENANT_ID == tenant_id
    || !ls_id.is_valid()
    || !leader_server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KT(tenant_id), K(ls_id), K(leader_server));
  } else if (OB_ISNULL(table_name) || OB_INVALID_TENANT_ID == sql_tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL table_name or invalid sql_tenant_id", KR(ret), K(table_name), K(sql_tenant_id));
  } else if (false == leader_server.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert leader_server ip to string failed", KR(ret), K(leader_server));
  } else if (OB_FAIL(sql.assign_fmt(
        "UPDATE %s "
        "SET role = ("
        "CASE WHEN svr_ip = '%s' AND svr_port = %d THEN %d "
        "ELSE %d end), proposal_id = ("
        "CASE WHEN svr_ip = '%s' AND svr_port = %d THEN proposal_id "
        "ELSE %ld end)  WHERE tenant_id = %lu AND ls_id = %lu",
        table_name, ip, leader_server.get_port(), leader_role, follower_role,
        ip, leader_server.get_port(), proposal_id_for_follower, tenant_id, ls_id.id()))) {
    LOG_WARN("assign sql failed", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(sql_client.write(sql_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), "sql", sql.ptr());
  } else {
    LOG_INFO("succeed to set other replicas to follower", KR(ret), K(tenant_id),
             K(ls_id), K(leader_server), K(affected_rows));
  }
  return ret;
}

int ObPersistentLSTable::update_replica_(
    ObISQLClient &sql_client,
    const ObLSReplica &replica)
{
  int ret = OB_SUCCESS;
  const char *table_name = OB_ALL_LS_META_TABLE_TNAME;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  const uint64_t sql_tenant_id = get_private_table_exec_tenant_id(replica.get_tenant_id());
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", KR(ret), K(replica));
  } else if (OB_FAIL(fill_dml_splicer_(replica, dml))) {
    LOG_WARN("fill dml splicer failed", KR(ret), K(replica));
  } else {
    ObDMLExecHelper exec(sql_client, sql_tenant_id);
    if (OB_FAIL(exec.exec_insert_update(table_name, dml, affected_rows))) {
      //insert_update means if row exist update, if not exist insert
      LOG_WARN("execute update failed", KR(ret), K(replica));
    } else if (OB_UNLIKELY(affected_rows < 0 || affected_rows > 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected_rows", KR(ret), K(affected_rows));
    }
  }
  return ret;
}

int ObPersistentLSTable::fill_dml_splicer_(
    const ObLSReplica &replica,
    ObDMLSqlSplicer &dml_splicer)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  uint64_t compat_version = 0;
  ObSqlString member_list;
  ObSqlString learner_list;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", KR(ret), K(replica));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(replica.get_tenant_id(), compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(replica));
  } else if (false == replica.get_server().ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", KR(ret), "server", replica.get_server());
  } else if (OB_FAIL(ObLSReplica::member_list2text(
              replica.get_member_list(), member_list))) {
    LOG_WARN("member_list2text failed", KR(ret), K(replica));
  } else if (OB_FAIL(replica.get_learner_list().transform_to_string(learner_list))) {
    LOG_WARN("failed to transform GlobalLearnerList to ObSqlString", KR(ret), K(replica));
  } else if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", replica.get_tenant_id()))  //location related
        || OB_FAIL(dml_splicer.add_pk_column("ls_id", replica.get_ls_id().id()))
        || OB_FAIL(dml_splicer.add_pk_column("svr_ip", ip))
        || OB_FAIL(dml_splicer.add_pk_column("svr_port", replica.get_server().get_port()))
        || OB_FAIL(dml_splicer.add_column("sql_port", replica.get_sql_port()))
        || OB_FAIL(dml_splicer.add_column("role", replica.get_role()))
        || OB_FAIL(dml_splicer.add_column("member_list", member_list.empty() ? "" : member_list.ptr()))
        || OB_FAIL(dml_splicer.add_column("proposal_id", replica.get_proposal_id()))
        || OB_FAIL(dml_splicer.add_column("replica_type", replica.get_replica_type()))
        || OB_FAIL(dml_splicer.add_column("replica_status", ob_replica_status_str(replica.get_replica_status())))
        || OB_FAIL(dml_splicer.add_column("restore_status", replica.get_restore_status().get_status()))
        || OB_FAIL(dml_splicer.add_column("memstore_percent", replica.get_memstore_percent()))
        || OB_FAIL(dml_splicer.add_column("unit_id", replica.get_unit_id()))      //meta related
        || OB_FAIL(dml_splicer.add_column("zone", replica.get_zone().ptr()))
        || OB_FAIL(dml_splicer.add_column("paxos_replica_number", replica.get_paxos_replica_number()))
        || OB_FAIL(dml_splicer.add_column("data_size", replica.get_data_size()))
        || OB_FAIL(dml_splicer.add_column("required_size", replica.get_required_size()))){
    LOG_WARN("add column failed", KR(ret), K(replica));
  } else if (compat_version >= DATA_VERSION_4_2_0_0
             && OB_FAIL(dml_splicer.add_column("rebuild", replica.get_rebuild()))) {
    LOG_WARN("add column rebuild failed", KR(ret), K(replica));
  }

  uint64_t tenant_to_check_data_version = replica.get_tenant_id();
  bool is_compatible_with_readonly_replica = false;
  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(ret)) {
  } else if (OB_SUCCESS != (tmp_ret = ObShareUtil::check_compat_version_for_readonly_replica(
                                      tenant_to_check_data_version, is_compatible_with_readonly_replica))) {
    LOG_WARN("fail to check compat version with readonly replica", KR(tmp_ret), K(tenant_to_check_data_version));
  } else if (is_compatible_with_readonly_replica) {
    if (OB_FAIL(dml_splicer.add_column("learner_list", learner_list.empty() ? "" : learner_list.ptr()))) {
      LOG_WARN("fail to add learner list column", KR(ret));
    }
  }

  return ret;
}

int ObPersistentLSTable::get_by_tenant(
    const uint64_t tenant_id,
    ObIArray<ObLSInfo> &ls_infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPersistentLSTable not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      ObSqlString sql;
      const uint64_t sql_tenant_id = get_private_table_exec_tenant_id(tenant_id);
      if (OB_FAIL(sql.assign_fmt(
          "SELECT * FROM %s WHERE tenant_id = %lu ORDER BY tenant_id, ls_id, svr_ip, svr_port",
          OB_ALL_LS_META_TABLE_TNAME, tenant_id))) {
        LOG_WARN("assign sql string failed", KR(ret), K(tenant_id));
      } else if (OB_FAIL(sql_proxy_->read(result, sql_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else if (OB_FAIL(construct_ls_infos_(
          *result.get_result(),
          ls_infos))) {
        LOG_WARN("construct log stream infos failed", KR(ret), K(ls_infos));
      }
    }
  }
  return ret;
}

int ObPersistentLSTable::load_all_ls_in_tenant(
    const uint64_t exec_tenant_id,
    ObIArray<ObLSInfo> &ls_infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPersistentLSTable not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(exec_tenant_id)
      || is_virtual_tenant_id(exec_tenant_id)
      || is_user_tenant(exec_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(exec_tenant_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      ObSqlString sql;
      if (OB_FAIL(sql.assign_fmt(
          "SELECT * FROM %s ORDER BY tenant_id, ls_id, svr_ip, svr_port",
          OB_ALL_LS_META_TABLE_TNAME))) {
        LOG_WARN("assign sql string failed", KR(ret), K(exec_tenant_id));
      } else if (OB_FAIL(sql_proxy_->read(result, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(exec_tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else if (OB_FAIL(construct_ls_infos_(
          *result.get_result(),
          ls_infos))) {
        LOG_WARN("construct log stream infos failed", KR(ret), K(ls_infos));
      }
    }
  }
  return ret;
}

int ObPersistentLSTable::construct_ls_infos_(
    common::sqlclient::ObMySQLResult &res,
    ObIArray<ObLSInfo> &ls_infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPersistentLSTable not init", KR(ret));
  } else {
    ObLSInfo ls_info;
    ObLSReplica replica;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(res.next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next result failed", KR(ret));
        }
        break;
      } else {
        replica.reset();
        if (OB_FAIL(construct_ls_replica(res, replica))) {
          LOG_WARN("fail to construct ls replica", KR(ret));
        } else if (OB_UNLIKELY(!replica.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("construct invalid replica", KR(ret), K(replica));
        } else if (ls_info.is_self_replica(replica)) {
          if (OB_FAIL(ls_info.add_replica(replica))) {
            LOG_WARN("fail to add replica", KR(ret), K(replica));
          }
        } else {
          if (ls_info.is_valid()) {
            // TODO: need make sure actions of update_replica_status
            if (OB_FAIL(ls_info.update_replica_status())) {
              LOG_WARN("update ls replica status failed", KR(ret), K(ls_info));
            } else if (OB_FAIL(ls_infos.push_back(ls_info))) {
              LOG_WARN("fail to push back", KR(ret), K(ls_info));
            }
          }
          ls_info.reset();
          if (FAILEDx(ls_info.init_by_replica(replica))) {
            LOG_WARN("fail to init ls_info by replica", KR(ret), K(replica));
          }
        }
      }
    } // end while
    if (OB_SUCC(ret) && ls_info.is_valid()) {
      // last ls info
      // TODO: need make sure actions of update_replica_status
      if (OB_FAIL(ls_info.update_replica_status())) {
        LOG_WARN("update ls replica status failed", KR(ret), K(ls_info));
      } else if (OB_FAIL(ls_infos.push_back(ls_info))) {
        LOG_WARN("fail to push back", KR(ret), K(ls_info));
      }
    }
  }
  return ret;
}

int ObPersistentLSTable::remove(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObAddr &server,
    const bool inner_table_only)
{
  int ret = OB_SUCCESS;
  UNUSED(inner_table_only);
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  ObSqlString sql;
  const uint64_t sql_tenant_id = get_private_table_exec_tenant_id(tenant_id);
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPersistentLSTable not init", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id) || !server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(server));
  } else if (OB_UNLIKELY(!server.ip_to_string(ip, sizeof(ip)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", KR(ret), K(server));
  } else if (OB_FAIL(sql.assign_fmt(
      "DELETE FROM %s WHERE tenant_id = %lu AND ls_id = %lu"
      " AND svr_ip = '%s' AND svr_port = %d",
      OB_ALL_LS_META_TABLE_TNAME,
      tenant_id,
      ls_id.id(),
      ip,
      server.get_port()))) {
    LOG_WARN("assign sql string failed", KR(ret), K(tenant_id), K(ls_id), K(server));
  } else if (OB_FAIL(sql_proxy_->write(sql_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), "sql", sql.ptr(), K(tenant_id), K(sql_tenant_id));
  } else {
    // do not check affected_rows because ls may be not exist in meta table
    LOG_INFO("delete row from ls meta table", KR(ret),
        K(affected_rows), K(sql), K(tenant_id), K(ls_id), K(server));
  }
  return ret;
}

int ObPersistentLSTable::remove_residual_ls(
    const uint64_t tenant_id,
    const ObAddr &server,
    int64_t &residual_count)
{
  int ret = OB_SUCCESS;
  residual_count = 0;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  ObSqlString sql;
  const uint64_t sql_tenant_id = get_private_table_exec_tenant_id(tenant_id);
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(
      !is_valid_tenant_id(tenant_id)
      || is_virtual_tenant_id(tenant_id)
      || is_sys_tenant(tenant_id)
      || !server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(server));
  } else if (OB_UNLIKELY(!server.ip_to_string(ip, sizeof(ip)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", KR(ret), K(server));
  } else if (OB_FAIL(sql.assign_fmt(
      "DELETE FROM %s WHERE tenant_id = %lu AND svr_ip = '%s' AND svr_port = %d",
      OB_ALL_LS_META_TABLE_TNAME,
      tenant_id,
      ip,
      server.get_port()))) {
    LOG_WARN("assign sql string failed", KR(ret), K(sql));
  } else if (OB_FAIL(sql_proxy_->write(sql_tenant_id, sql.ptr(), residual_count))) {
    LOG_WARN("execute sql failed", KR(ret), "sql", sql.ptr(), K(sql_tenant_id));
  }
  return ret;
}

int ObPersistentLSTable::batch_get(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const common::ObIArray<ObLSID> &ls_ids,
    common::ObIArray<ObLSInfo> &ls_infos)
{
  int ret = OB_SUCCESS;
  ls_infos.reset();
  ObSqlString sql;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPersistentLSTable not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || is_virtual_tenant_id(tenant_id)
      || ls_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(cluster_id), K(tenant_id), K(ls_ids));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT * FROM %s WHERE tenant_id = %lu AND ls_id IN (",
      OB_ALL_LS_META_TABLE_TNAME,
      tenant_id))) {
    LOG_WARN("assign sql string failed", KR(ret), K(sql));
  }
  ARRAY_FOREACH(ls_ids, idx) {
    const ObLSID &ls_id = ls_ids.at(idx);
    if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid ls_id with tenant", KR(ret), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(sql.append_fmt(
        "%s%ld",
        0 == idx ? "" : ",",
        ls_id.id()))) {
      LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(ls_id));
    }
  }
  if (FAILEDx(sql.append_fmt(") ORDER BY tenant_id, ls_id, svr_ip, svr_port"))) {
    LOG_WARN("appent fmt failed", KR(ret), K(tenant_id), K(ls_ids), K(sql));
  } else {
    const uint64_t exec_tenant_id = get_private_table_exec_tenant_id(tenant_id);
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql_proxy_->read(result, cluster_id, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(exec_tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else if (OB_FAIL(construct_ls_infos_(*result.get_result(), ls_infos))) {
        LOG_WARN("construct log stream infos failed", KR(ret), K(ls_infos));
      }
    }
    // if ls not found, return empty ls_info
    if (OB_SUCC(ret) && (ls_ids.count() != ls_infos.count())) {
      ARRAY_FOREACH(ls_ids, i) {
        const ObLSID &ls_id = ls_ids.at(i);
        bool found = false;
        ARRAY_FOREACH(ls_infos, j) {
          if (ls_id == ls_infos.at(j).get_ls_id()) {
            found = true;
            break;
          }
        }
        if (OB_SUCC(ret) && !found) {
          ObLSInfo ls_info;
          if (OB_FAIL(ls_info.init(tenant_id, ls_id))) {
            LOG_WARN("init ls_info failed", KR(ret), K(tenant_id), K(ls_id));
          } else if (OB_FAIL(ls_infos.push_back(ls_info))) {
            LOG_WARN("push back failed", KR(ret), K(ls_info));
          }
        }
      } // end ARRAY_FOREACH ls_ids
      if (OB_SUCC(ret) && OB_UNLIKELY(ls_ids.count() != ls_infos.count())) {
        ret = OB_ERR_DUP_ARGUMENT;
        LOG_WARN("there are duplicate values in ls_ids", KR(ret), K(ls_ids));
      }
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
