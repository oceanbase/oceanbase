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

#define USING_LOG_PREFIX SHARE


#include "ob_unit_table_operator.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/ob_all_server_tracer.h" // for SVR_TRACER
#include "src/share/ob_common_rpc_proxy.h"
#include "rootserver/tenant_snapshot/ob_tenant_snapshot_util.h" // ObTenantSnapshotUtil
#include "share/ob_service_epoch_proxy.h"    // ObServiceEpochProxy

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
namespace share
{
int ObUnitTableTransaction::start(ObISQLClient *proxy,
                  const uint64_t tenant_id,
                  bool with_snapshot,
                  const int32_t group_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMySQLTransaction::start(proxy, tenant_id, with_snapshot, group_id))) {
    LOG_WARN("start transaction failed", KR(ret));
  } else if (OB_FAIL(lock_service_epoch(*this))) {
    LOG_WARN("lock service epoch failed", KR(ret));
  }
  return ret;
}

// this function is meant to lock and check service epoch to protect inner_table from parallel writing.
int ObUnitTableTransaction::lock_service_epoch(common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t persistent_epoch_id = OB_INVALID_ID;
  if (OB_FAIL(ObServiceEpochProxy::select_service_epoch_for_update(
      trans, OB_SYS_TENANT_ID, ObServiceEpochProxy::UNIT_MANAGER_EPOCH, persistent_epoch_id))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to select service epoch for update", KR(ret));
    } else {
      // epoch not init yet, do init
      ret = OB_SUCCESS;
      if (OB_FAIL(ObServiceEpochProxy::insert_service_epoch(
          trans, OB_SYS_TENANT_ID, ObServiceEpochProxy::UNIT_MANAGER_EPOCH, 0/*init_epoch_id*/))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to init unit_manager_epoch", KR(ret));
        }
      }
      if (FAILEDx(ObServiceEpochProxy::select_service_epoch_for_update(
          trans, OB_SYS_TENANT_ID, ObServiceEpochProxy::UNIT_MANAGER_EPOCH, persistent_epoch_id))) {
        LOG_WARN("fail to select service epoch for update", KR(ret));
      }
    }
  }
  return ret;
}

int ObUnitUGOp::assign(const ObUnitUGOp &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    unit_id_ = other.unit_id_;
    old_unit_group_id_ = other.old_unit_group_id_;
    new_unit_group_id_ = other.new_unit_group_id_;
  }
  return ret;
}

ObUnitTableTransaction::~ObUnitTableTransaction()
{
  if (is_started()) {
    int ret = end(false);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to end Unit Table trans", KR(ret));
    }
  }
}

ObUnitTableOperator::ObUnitTableOperator()
  : inited_(false),
    proxy_(NULL)
{
}

ObUnitTableOperator::~ObUnitTableOperator()
{
}

int ObUnitTableOperator::init(common::ObISQLClient &proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    proxy_ = &proxy;
    inited_ = true;
  }
  return ret;
}

int ObUnitTableOperator::get_sys_unit_count(int64_t &cnt) const
{
  int ret = OB_SUCCESS;
  cnt = 0;
  common::ObArray<ObUnit> units;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid proxy", KR(ret), K(proxy_));
  } else if (OB_FAIL(get_units_by_tenant(OB_SYS_TENANT_ID, units))) {
    LOG_WARN("failed to get units by tenant", KR(ret));
  } else {
    cnt = units.count();
  }
  return ret;
}

int ObUnitTableOperator::get_units(common::ObIArray<ObUnit> &units) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSqlString sql;
    ObTimeoutCtx ctx;
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.append_fmt("SELECT * from %s",
        OB_ALL_UNIT_TNAME))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(read_units(sql, units))) {
      LOG_WARN("read_units failed", K(sql), K(ret));
    }
  }
  return ret;
}

// get all tenant ids
int ObUnitTableOperator::get_tenants(common::ObIArray<uint64_t> &tenants) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    tenants.reuse();
    ObSqlString sql;
    if (OB_FAIL(sql.append_fmt("SELECT tenant_id FROM %s", OB_ALL_TENANT_TNAME))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(read_tenants(sql, tenants))) {
      LOG_WARN("fail read tenants", K(sql), K(ret));
    }
  }
  return ret;
}

int ObUnitTableOperator::read_tenant(const ObMySQLResult &result,
                                     uint64_t &tenant_id) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    EXTRACT_INT_FIELD_MYSQL(result, "tenant_id", tenant_id, uint64_t);
  }
  return ret;
}

int ObUnitTableOperator::get_units(const common::ObAddr &server,
                                   common::ObIArray<ObUnit> &units) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    units.reuse();
    ObSqlString sql;
    char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
    if (false == server.ip_to_string(ip, sizeof(ip))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ip_to_string failed", K(server), K(ret));
    } else if (OB_FAIL(sql.append_fmt("SELECT *"
        " FROM %s WHERE (svr_ip = '%s' AND svr_port = %d) "
        "or (migrate_from_svr_ip = '%s' AND migrate_from_svr_port = %d)",
        OB_ALL_UNIT_TNAME, ip, server.get_port(), ip, server.get_port()))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(read_units(sql, units))) {
      LOG_WARN("read_units failed", K(sql), K(ret));
    }
  }
  return ret;
}

int ObUnitTableOperator::check_server_empty(const common::ObAddr &server, bool &is_empty)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit> units;
  is_empty = true;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", KR(ret), K(server));
  } else if (OB_FAIL(get_units(server, units))) {
    LOG_WARN("fail to get units", KR(ret), K(server));
  } else if (units.count() > 0) {
    is_empty = false;
    LOG_TRACE("server exists in the server list or migrate_from_server list", K(server), K(units));
  }
  return ret;
}

int ObUnitTableOperator::get_units_by_pool_id(
    common::ObISQLClient &client,
    const uint64_t pool_id,
    common::ObIArray<ObUnit> &units) const
{
  int ret = OB_SUCCESS;
  units.reuse();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_id(pool_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resource pool id", KR(ret), K(pool_id));
  } else {
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.append_fmt("SELECT * FROM %s WHERE resource_pool_id = %lu",
                  OB_ALL_UNIT_TNAME, pool_id))) {
        LOG_WARN("fail to append fmt", KR(ret), K(pool_id));
      } else if (OB_FAIL(client.read(res, sql.ptr()))) {
        LOG_WARN("fail to read", KR(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get result", KR(ret));
      } else {
        ObUnit unit;
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          unit.reset();
          if (OB_FAIL(read_unit(*result, unit))) {
            LOG_WARN("fail to read unit", KR(ret));
          } else if (OB_FAIL(units.push_back(unit))) {
            LOG_WARN("fail to push back unit", KR(ret));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else if (OB_FAIL(ret)) {
          LOG_WARN("fail to iterate result", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::get_units(const common::ObIArray<uint64_t> &pool_ids,
                                   common::ObIArray<ObUnit> &units) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (pool_ids.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pool_ids), K(ret));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.append_fmt("SELECT *"
        " FROM %s WHERE resource_pool_id in (",
        OB_ALL_UNIT_TNAME))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < pool_ids.count(); ++i) {
        if (OB_FAIL(sql.append_fmt("%s%lu", (0 == i ? "" : ", "), pool_ids.at(i)))) {
          LOG_WARN("sql append_fmt failed", K(ret));
        } else if (i == pool_ids.count() - 1) {
          if (OB_FAIL(sql.append(")"))) {
            LOG_WARN("append failed", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(read_units(sql, units))) {
        LOG_WARN("read_units failed", K(sql), K(ret));
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::insert_unit(common::ObISQLClient &client,
                                     const ObUnit &unit)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!unit.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit", K(unit), KR(ret));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(construct_unit_dml_(unit, true/*include_ug_id*/, dml))) {
      LOG_WARN("construct_unit_dml_ failed", K(unit), KR(ret));
    } else {
      ObDMLExecHelper exec(client, OB_SYS_TENANT_ID);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_insert(OB_ALL_UNIT_TNAME, dml, affected_rows))) {
        LOG_WARN("exec insert failed", "table", OB_ALL_UNIT_TNAME, KR(ret));
      } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect insert single row", K(affected_rows), KR(ret));
      }
    }
  }
  return ret;
}


// update all columns of unit except for ug_id
int ObUnitTableOperator::update_unit_exclude_ug_id(common::ObISQLClient &client,
                                     const ObUnit &unit,
                                     const bool need_check_conflict_with_clone)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!unit.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit", K(unit), KR(ret));
  } else if (need_check_conflict_with_clone
      && OB_FAIL(check_unit_conflict_with_clone_(client, unit))) {
    LOG_WARN("fail to check unit conflict with clone", KR(ret), K(unit));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(construct_unit_dml_(unit, false/*include_ug_id*/, dml))) {
      LOG_WARN("construct unit dml failed", KR(ret), K(unit));
    } else {
      ObDMLExecHelper exec(client, OB_SYS_TENANT_ID);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_update(OB_ALL_UNIT_TNAME, dml, affected_rows))) {
        LOG_WARN("exec update failed", "table", OB_ALL_UNIT_TNAME, KR(ret));
      } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect update single row", K(affected_rows), KR(ret));
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::construct_unit_dml_(
    const ObUnit &unit, const bool include_ug_id, share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char migrate_from_svr_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  const char *unit_status_str = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!unit.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit", K(unit), K(ret));
  } else if (false == unit.server_.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", "server", unit.server_, K(ret));
  } else {
    if (unit.migrate_from_server_.is_valid()) {
      if (false == unit.migrate_from_server_.ip_to_string(
          migrate_from_svr_ip, sizeof(migrate_from_svr_ip))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("convert server ip to string failed",
            "server", unit.migrate_from_server_, K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(unit.get_unit_status_str(unit_status_str))) {
    LOG_WARN("fail to get unit status", K(ret));
  } else if (OB_UNLIKELY(NULL == unit_status_str)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null unit status", K(ret), KP(unit_status_str));
  } else {
    if (OB_FAIL(dml.add_pk_column(OBJ_K(unit, unit_id)))
        || OB_FAIL(dml.add_column(OBJ_K(unit, resource_pool_id)))
        || (include_ug_id && OB_FAIL(dml.add_column(OBJ_K(unit, unit_group_id))))
        || OB_FAIL(dml.add_column("zone", unit.zone_.ptr()))
        || OB_FAIL(dml.add_column("svr_ip", ip))
        || OB_FAIL(dml.add_column("svr_port", unit.server_.get_port()))
        || OB_FAIL(dml.add_column(K(migrate_from_svr_ip)))
        || OB_FAIL(dml.add_column("migrate_from_svr_port", unit.migrate_from_server_.get_port()))
        || OB_FAIL(dml.add_gmt_modified())
        || OB_FAIL(dml.add_column("manual_migrate", unit.is_manual_migrate()))
        || OB_FAIL(dml.add_column("status", unit_status_str))) {
      LOG_WARN("add column failed", K(ret));
    } else {
      // add replica type if needed
      bool compatible_with_logonly_replica = false;
      if (OB_FAIL(ObShareUtil::check_compat_version_for_logonly_replica(compatible_with_logonly_replica))) {
        LOG_WARN("fail to check whether compatible with logonly replica", KR(ret));
      } else if (compatible_with_logonly_replica) {
        if (OB_FAIL(dml.add_column("replica_type", unit.replica_type_))) {
          LOG_WARN("fail to add replica type to dml", KR(ret), K(unit));
        }
      } else if (ObReplicaType::REPLICA_TYPE_LOGONLY == unit.replica_type_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("logonly replica supported from 4.5.1.0", KR(ret), K(unit));
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::check_unit_conflict_with_clone_(common::ObISQLClient &client, const ObUnit &unit)
{
  int ret = OB_SUCCESS;
  ObResourcePool resource_pool;
  rootserver::ObConflictCaseWithClone case_to_check(rootserver::ObConflictCaseWithClone::MODIFY_UNIT);
  if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy_ is null", KR(ret));
  } else if (OB_FAIL(get_resource_pool(*proxy_, unit.resource_pool_id_, false/*select_for_update*/, resource_pool))) {
    // use proxy_ to get resource pool, because tenant may be creating, tenant_id_ should be -1
    LOG_WARN("fail to get resource pool", KR(ret), K(unit));
  } else if (!is_valid_tenant_id(resource_pool.tenant_id_)) {
    // this unit is not granted to tenant, just ignore
  } else if (OB_FAIL(rootserver::ObTenantSnapshotUtil::check_tenant_not_in_cloning_procedure(resource_pool.tenant_id_, case_to_check))) {
    LOG_WARN("fail to check whether tenant is cloning", KR(ret), "tenant_id", resource_pool.tenant_id_, K(case_to_check));
  }
  return ret;
}

// update unit ug_id without affecting other columns
int ObUnitTableOperator::update_unit_ug_id(common::ObISQLClient &client, const ObUnitUGOp &op)
{
  int ret = OB_SUCCESS;
  ObUnit unit;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid op", KR(ret), K(op));
  } else if (OB_FAIL(get_unit_by_id(client, op.get_unit_id(), unit))) {
    LOG_WARN("fail to get unit", KR(ret), K(op));
  } else {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (op.get_old_unit_group_id() == OB_INVALID_ID) {
      // invalid id means do not need to filter by old_unit_group_id, just set by unit_id
      if (OB_FAIL(sql.append_fmt("UPDATE %s SET unit_group_id = %lu WHERE unit_id = %lu",
            OB_ALL_UNIT_TNAME, op.get_new_unit_group_id(), op.get_unit_id()))) {
        LOG_WARN("fail to append fmt", KR(ret), K(op));
      }
    } else if (OB_FAIL(sql.append_fmt("UPDATE %s SET unit_group_id = %lu WHERE unit_id = %lu AND unit_group_id = %lu",
          OB_ALL_UNIT_TNAME, op.get_new_unit_group_id(), op.get_unit_id(), op.get_old_unit_group_id()))) {
      LOG_WARN("fail to append fmt", KR(ret), K(op));
    }
    if (FAILEDx(client.write(sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(sql));
    } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows is not single row", KR(ret), K(affected_rows));
    }
  }
  return ret;
}

int ObUnitTableOperator::remove_units(common::ObISQLClient &client,
                                      const uint64_t resource_pool_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == resource_pool_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(resource_pool_id), K(ret));
  } else {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.append_fmt("delete from %s where resource_pool_id = %ld",
        OB_ALL_UNIT_TNAME, resource_pool_id))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(client.write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    }
  }
  return ret;
}

int ObUnitTableOperator::remove_unit(
    common::ObISQLClient &client,
    const ObUnit &unit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!unit.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unit));
  } else {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.append_fmt("delete from %s where unit_id = %ld",
            OB_ALL_UNIT_TNAME, unit.unit_id_))) {
      LOG_WARN("append fmt failed", K(ret));
    } else if (OB_FAIL(client.write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else {} // no more to do
  }
  return ret;
}

int ObUnitTableOperator::remove_units_in_zones(
    common::ObISQLClient &client,
    const uint64_t resource_pool_id,
    const common::ObIArray<common::ObZone> &to_be_removed_zones)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == resource_pool_id)
             || OB_UNLIKELY(to_be_removed_zones.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(resource_pool_id), K(to_be_removed_zones));
  } else {
    ObSqlString sql;
    for (int64_t i = 0; OB_SUCC(ret) && i < to_be_removed_zones.count(); ++i) {
      int64_t affected_rows = 0;
      sql.reset();
      const common::ObZone &zone = to_be_removed_zones.at(i);
      if (OB_FAIL(sql.append_fmt("delete from %s where resource_pool_id = %ld "
              "and zone = '%s'", OB_ALL_UNIT_TNAME, resource_pool_id, zone.ptr()))) {
        LOG_WARN("append_fmt failed", K(ret));
      } else if (OB_FAIL(client.write(sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql));
      } else {} // no more to do
    }
  }
  return ret;
}


int ObUnitTableOperator::get_resource_pools(common::ObIArray<ObResourcePool> &pools) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.append_fmt("select * from %s",
        OB_ALL_RESOURCE_POOL_TNAME))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(read_resource_pools(sql, pools))) {
      LOG_WARN("read_resource_pools failed", K(sql), K(ret));
    }
  }
  return ret;
}

int ObUnitTableOperator::get_resource_pools(const uint64_t tenant_id,
                                            common::ObIArray<ObResourcePool> &pools) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.append_fmt("select * from %s where tenant_id = %lu",
        OB_ALL_RESOURCE_POOL_TNAME, tenant_id))) {
      LOG_WARN("append_fmt failed", K(tenant_id), K(ret));
    } else if (OB_FAIL(read_resource_pools(sql, pools))) {
      LOG_WARN("read_resource_pools failed", K(sql), K(ret));
    }
  }
  return ret;
}

int ObUnitTableOperator::get_resource_pools(const common::ObIArray<uint64_t> &pool_ids,
                                            common::ObIArray<ObResourcePool> &pools) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (pool_ids.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pool_ids), K(ret));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.append_fmt("select * from %s "
        "where resource_pool_id in (", OB_ALL_RESOURCE_POOL_TNAME))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < pool_ids.count(); ++i) {
        if (OB_FAIL(sql.append_fmt("%s%lu", (0 == i ? "" : ", "), pool_ids.at(i)))) {
          LOG_WARN("append_fmt failed", K(ret));
        } else if (i == pool_ids.count() - 1) {
          if (OB_FAIL(sql.append(")"))) {
            LOG_WARN("append failed", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(read_resource_pools(sql, pools))) {
        LOG_WARN("read_resource_pools failed", K(sql), K(ret));
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::get_resource_pool(common::ObISQLClient &sql_client,
                                           const uint64_t pool_id,
                                           const bool select_for_update,
                                           ObResourcePool &resource_pool) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_string.assign_fmt("SELECT * FROM %s WHERE resource_pool_id = %lu%s",
          OB_ALL_RESOURCE_POOL_TNAME, pool_id,
          select_for_update ? " FOR UPDATE" : ""))) {
        LOG_WARN("assign sql string failed", K(ret), K(pool_id), K(select_for_update));
      } else if (OB_FAIL(sql_client.read(res, sql_string.ptr()))) {
        LOG_WARN("update status of ddl task record failed", K(ret), K(sql_string));
      } else if (OB_UNLIKELY(NULL == (result = res.get_result()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret), KP(result));
      } else if (OB_FAIL(read_resource_pool(*result, resource_pool))) {
        LOG_WARN("fail to read resource pool from result", KR(ret), K(pool_id), K(select_for_update));
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::update_resource_pool(common::ObISQLClient &client,
                                              const ObResourcePool &resource_pool,
                                              const bool need_check_conflict_with_clone)
{
  int ret = OB_SUCCESS;
  ObResourcePool resource_pool_to_lock;
  rootserver::ObConflictCaseWithClone case_to_check(rootserver::ObConflictCaseWithClone::MODIFY_RESOURCE_POOL);
  SMART_VAR(char[MAX_ZONE_LIST_LENGTH], zone_list_str) {
    zone_list_str[0] = '\0';

    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (!resource_pool.is_valid()
      || OB_INVALID_ID == resource_pool.unit_config_id_
      || OB_INVALID_ID == resource_pool.resource_pool_id_
      || resource_pool.zone_list_.count() <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(resource_pool), K(ret));
    } else if (OB_FAIL(zone_list2str(resource_pool.zone_list_,
        zone_list_str, MAX_ZONE_LIST_LENGTH))) {
      LOG_WARN("zone_list2str failed", "zone_list", resource_pool.zone_list_, K(ret));
    // try lock resource pool to update for clone tenant conflict check
    } else if (need_check_conflict_with_clone) {
      if (!is_valid_tenant_id(resource_pool.tenant_id_)) {
        // this resource pool has not granted to any tenant, just ignore
      } else if (OB_FAIL(rootserver::ObTenantSnapshotUtil::check_tenant_not_in_cloning_procedure(
                            resource_pool.tenant_id_,
                            case_to_check))) {
        LOG_WARN("fail to check whether tenant is in cloning procedure", KR(ret), K(resource_pool),
                 K(case_to_check), K(need_check_conflict_with_clone));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      ObDMLSqlSplicer dml;
      ObString resource_pool_name(strlen(resource_pool.name_.ptr()), resource_pool.name_.ptr());
      if (OB_FAIL(dml.add_pk_column(OBJ_K(resource_pool, resource_pool_id)))
          || OB_FAIL(dml.add_column("name", ObHexEscapeSqlStr(resource_pool_name)))
          || OB_FAIL(dml.add_column(OBJ_K(resource_pool, unit_count)))
          || OB_FAIL(dml.add_column(OBJ_K(resource_pool, unit_config_id)))
          || OB_FAIL(dml.add_column("zone_list", zone_list_str))
          || OB_FAIL(dml.add_column(OBJ_K(resource_pool, tenant_id)))
          || OB_FAIL(dml.add_column(OBJ_K(resource_pool, replica_type)))
          || OB_FAIL(dml.add_gmt_modified())) {
        LOG_WARN("add column failed", K(ret));
      } else {
        ObDMLExecHelper exec(client, OB_SYS_TENANT_ID);
        int64_t affected_rows = 0;
        if (OB_FAIL(exec.exec_insert_update(OB_ALL_RESOURCE_POOL_TNAME, dml, affected_rows))) {
          LOG_WARN("exec insert_update failed", "table", OB_ALL_RESOURCE_POOL_TNAME, K(ret));
        } else if (is_zero_row(affected_rows) || affected_rows > 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expect update single row", K(affected_rows), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::remove_resource_pool(common::ObISQLClient &client,
                                              const uint64_t resource_pool_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == resource_pool_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(resource_pool_id), K(ret));
  } else {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.append_fmt("delete from %s where resource_pool_id = %ld",
        OB_ALL_RESOURCE_POOL_TNAME, resource_pool_id))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(client.write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows not one", K(affected_rows), K(ret));
    }
  }
  return ret;
}

int ObUnitTableOperator::get_unit_configs(common::ObIArray<ObUnitConfig> &configs) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSqlString sql;
    ObTimeoutCtx ctx;
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.append_fmt("select * from %s ", OB_ALL_UNIT_CONFIG_TNAME))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(read_unit_configs(sql, configs))) {
      LOG_WARN("read_unit_configs failed", K(sql), K(ret));
    }
  }
  return ret;
}

int ObUnitTableOperator::get_unit_configs_by_tenant(const uint64_t tenant_id,
      common::ObIArray<ObUnitConfig> &configs)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSqlString sql;
    ObTimeoutCtx ctx;
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.append_fmt("select * from %s where unit_config_id in "
            "(select unit_config_id from %s where tenant_id = %lu)", OB_ALL_UNIT_CONFIG_TNAME,
            OB_ALL_RESOURCE_POOL_TNAME, tenant_id))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(read_unit_configs(sql, configs))) {
      LOG_WARN("read_unit_configs failed", K(sql), K(ret));
    }
  }
  return ret;
}

int ObUnitTableOperator::get_unit_configs(const common::ObIArray<uint64_t> &config_ids,
                                          common::ObIArray<ObUnitConfig> &configs) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (config_ids.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(config_ids), K(ret));
  } else {
    ObSqlString sql;
    ObTimeoutCtx ctx;
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.append_fmt("select * from %s "
        "where unit_config_id in (", OB_ALL_UNIT_CONFIG_TNAME))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < config_ids.count(); ++i) {
        if (OB_FAIL(sql.append_fmt("%s%lu", (0 == i ? "" : ", "), config_ids.at(i)))) {
          LOG_WARN("append_fmt failed", K(ret));
        } else if (i == config_ids.count() - 1) {
          if (OB_FAIL(sql.append(")"))) {
            LOG_WARN("append failed", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(read_unit_configs(sql, configs))) {
        LOG_WARN("read_unit_configs failed", K(sql), K(ret));
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::get_unit_config_by_name(
    const common::ObString &unit_name, ObUnitConfig &unit_config)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(unit_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit_name));
  } else {
    ObSqlString sql;
    ObTimeoutCtx ctx;
    ObSEArray<ObUnitConfig, 1> configs;
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.append_fmt("select * from %s "
                                      "where name = '%.*s'",
                                      OB_ALL_UNIT_CONFIG_TNAME,
                                      unit_name.length(), unit_name.ptr()))) {
      LOG_WARN("append_fmt failed", KR(ret), K(unit_name));
    } else if (OB_FAIL(read_unit_configs(sql, configs))) {
      LOG_WARN("read_unit_configs failed", K(sql), K(ret));
    } else if (0 == configs.count()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("unit config not exist", KR(ret), K(unit_name));
    } else if (OB_UNLIKELY(1 != configs.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("find more than one unit config by name not expected", KR(ret), K(unit_name), K(configs));
    } else if (OB_FAIL(unit_config.assign(configs.at(0)))) {
      LOG_WARN("failed to assign unit config", KR(ret), K(configs));
    }
  }
  return ret;

}

int ObUnitTableOperator::update_unit_config(common::ObISQLClient &client,
                                            const ObUnitConfig &config)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!config.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(config), K(ret));
  } else {
    ObDMLSqlSplicer dml;
    ObString unit_name(config.name().size(), config.name().ptr());
    uint64_t data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, data_version))) {
      LOG_WARN("failed to get sys tenant min data version", KR(ret));
    } else if (data_version < DATA_VERSION_4_3_3_0
               && ObUnitResource::DEFAULT_DATA_DISK_SIZE != config.data_disk_size()) {
      // during upgrade, should be default value 0.
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("during upgrade, data_disk_size is expected to be 0", KR(ret), K(config));
    } else if (data_version < DATA_VERSION_4_3_3_0
               && (ObUnitResource::DEFAULT_NET_BANDWIDTH != config.max_net_bandwidth()
                   || ObUnitResource::DEFAULT_NET_BANDWIDTH_WEIGHT != config.net_bandwidth_weight())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("max_net_bandwidth_ and net_bandwidth_weight_ are expected to be default value",
               KR(ret), K(config));
    } else if (
        OB_FAIL(dml.add_pk_column("unit_config_id", config.unit_config_id()))
        || OB_FAIL(dml.add_column("name", ObHexEscapeSqlStr(unit_name)))
        || OB_FAIL(dml.add_column("max_cpu", config.max_cpu()))
        || OB_FAIL(dml.add_column("min_cpu", config.min_cpu()))
        || OB_FAIL(dml.add_column("memory_size", config.memory_size()))
        || OB_FAIL(dml.add_column("log_disk_size", config.log_disk_size()))
        || (data_version >= DATA_VERSION_4_3_3_0
            && OB_FAIL(dml.add_column("data_disk_size", config.data_disk_size())))
        || OB_FAIL(dml.add_column("max_iops", config.max_iops()))
        || OB_FAIL(dml.add_column("min_iops", config.min_iops()))
        || OB_FAIL(dml.add_column("iops_weight", config.iops_weight()))
        || (data_version >= DATA_VERSION_4_3_3_0
            && OB_FAIL(dml.add_column("max_net_bandwidth", config.max_net_bandwidth())))
        || (data_version >= DATA_VERSION_4_3_3_0
            && OB_FAIL(dml.add_column("net_bandwidth_weight", config.net_bandwidth_weight())))
        || OB_FAIL(dml.add_gmt_modified())) {
      LOG_WARN("add column failed", KR(ret), K(config), K(unit_name));
    } else {
      ObDMLExecHelper exec(client, OB_SYS_TENANT_ID);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_insert_update(OB_ALL_UNIT_CONFIG_TNAME, dml, affected_rows))) {
        LOG_WARN("exec insert_update failed", "table", OB_ALL_UNIT_TNAME, K(ret));
      } else if (is_zero_row(affected_rows) || affected_rows > 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect update single row", K(affected_rows), KR(ret));
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::remove_unit_config(common::ObISQLClient &client,
                                            const uint64_t unit_config_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == unit_config_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(unit_config_id), K(ret));
  } else {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.append_fmt("delete from %s where unit_config_id = %ld",
        OB_ALL_UNIT_CONFIG_TNAME, unit_config_id))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(client.write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows not one", K(affected_rows), K(ret));
    }
  }
  return ret;
}

int ObUnitTableOperator::get_units_by_unit_group_id(
    const uint64_t unit_group_id, common::ObIArray<ObUnit> &units)
{
  int ret = OB_SUCCESS;
  units.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObSqlString sql;
    ObTimeoutCtx ctx;
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    } else if (OB_FAIL(sql.append_fmt("SELECT * from %s where unit_group_id = %lu",
                                  OB_ALL_UNIT_TNAME, unit_group_id))) {
      LOG_WARN("append_fmt failed", KR(ret), K(unit_group_id));
    } else if (OB_FAIL(read_units(sql, units))) {
      LOG_WARN("read_units failed", KR(ret), K(sql));
    }
  }

  return ret;
}

int ObUnitTableOperator::get_units_by_unit_group_id(
      const uint64_t tenant_id,
      const uint64_t unit_group_id,
      common::ObIArray<ObUnit> &units) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObSqlString sql;
    ObTimeoutCtx ctx;
    units.reuse();
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    } else if (OB_FAIL(sql.append_fmt("SELECT * from %s where resource_pool_id in "
             "(select resource_pool_id from %s where tenant_id = %lu) and unit_group_id = %lu",
             OB_ALL_UNIT_TNAME, OB_ALL_RESOURCE_POOL_TNAME, tenant_id, unit_group_id))) {
      LOG_WARN("append_fmt failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(read_units(sql, units))) {
      LOG_WARN("read_units failed", KR(ret), K(sql));
    }
  }
  if (OB_SUCC(ret) && 0 == units.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("the tenant doesn't have this unit group", KR(ret), K(tenant_id), K(unit_group_id));
  }
  return ret;
}

int ObUnitTableOperator::get_unit_in_group(
    const uint64_t unit_group_id,
    const common::ObZone &zone,
    share::ObUnit &unit)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObUnit> unit_array;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(zone.is_empty() || OB_INVALID_ID == unit_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone), K(unit_group_id));
  } else if (OB_FAIL(get_units_by_unit_group_id(unit_group_id, unit_array))) {
    LOG_WARN("fail to get unit group", KR(ret), K(unit_group_id));
  } else {
    bool found = false;
    for (int64_t i = 0; !found && OB_SUCC(ret) && i < unit_array.count(); ++i) {
      const share::ObUnit &this_unit = unit_array.at(i);
      if (this_unit.zone_ != zone) {
        // bypass
      } else if (OB_FAIL(unit.assign(this_unit))) {
        LOG_WARN("fail to assign unit info", KR(ret));
      } else {
        found = true;
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("unit not found", KR(ret), K(unit_group_id), K(zone));
    } else {} // good
  }
  return ret;
}

int ObUnitTableOperator::get_units_by_resource_pool_ids(
    const ObIArray<uint64_t> &pool_ids, common::ObIArray<ObUnit> &units) const
{
  int ret = OB_SUCCESS;
  units.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObSqlString sql;
    ObTimeoutCtx ctx;
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_ids.count(); ++i) {
      sql.reset();
      const uint64_t pool_id = pool_ids.at(i);
      if (OB_FAIL(sql.append_fmt(
              "SELECT * FROM %s WHERE resource_pool_id = %ld", OB_ALL_UNIT_TNAME, pool_id))) {
        LOG_WARN("append_fmt failed", KR(ret), K(pool_id));
      } else if (OB_FAIL(read_units(sql, units))) {
        LOG_WARN("read_units failed", KR(ret), K(sql));
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::get_units_by_resource_pools(
    const ObIArray<share::ObResourcePoolName> &pools,
    common::ObIArray<ObUnit> &units)
{
  int ret = OB_SUCCESS;
  units.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObSqlString sql;
    ObTimeoutCtx ctx;
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pools.count(); ++i) {
      sql.reset();
      const ObResourcePoolName pool_name = pools.at(i);
      if (OB_FAIL(sql.append_fmt(
              "SELECT * from %s where resource_pool_id = (select "
              "resource_pool_id  from %s where name = '%s')",
              OB_ALL_UNIT_TNAME, OB_ALL_RESOURCE_POOL_TNAME, pool_name.ptr()))) {
        LOG_WARN("append_fmt failed", KR(ret), K(pool_name));
      } else if (OB_FAIL(read_units(sql, units))) {
        LOG_WARN("read_units failed", KR(ret), K(sql));
      }
    }
  }

  return ret;
}

// what is_strict means?
// it means need to get all the server has the tenant's unit.
// when a unit just finished migrate from A server to B server,
// the unit on A would not GC immediatly but to wait a while.
// so most alive sessions on A won't kill after a migration.
// at this time, the unit on A can't be attached by __all_unit_table.
// so, the is_strict mode would wait the time of 'unit_gc_wait_time' by report retry,
// if there is a unit just created or migrated.
int ObUnitTableOperator::get_all_servers_by_tenant(
  const uint64_t tenant_id,
  common::ObIArray<common::ObAddr> &server_array,
  bool is_strict) const
{
  int ret = OB_SUCCESS;
  server_array.reset();
  common::ObArray<ObUnit> units;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_units_by_tenant(tenant_id, units))) {
    LOG_WARN("fail to get units", KR(ret), K(tenant_id));
  } else {
    const int64_t unit_gc_wait_time = GCONF.unit_gc_wait_time;
    int64_t time_gap = 0;
    for (int64_t index = 0; OB_SUCC(ret) && index < units.count(); ++index) {
      const common::ObAddr &unit_addr = units.at(index).server_;
      const common::ObAddr &unit_migrate_from_server_addr = units.at(index).migrate_from_server_;
      bool is_alive = false;
      if (is_strict) {
        time_gap = 0;
        if (OB_INVALID_TIMESTAMP == units.at(index).time_stamp_) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("timestamp invalid, can't do strict check", KR(ret), K(units));
        } else {
          time_gap = ObTimeUtility::current_time() - units.at(index).time_stamp_;
          if (time_gap < unit_gc_wait_time) {
            ret = OB_EAGAIN;
            LOG_WARN("there may unit not gc but not in list, need wait", KR(ret), K(time_gap), K(unit_gc_wait_time),
                                                                         K(units.at(index)));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(server_array.push_back(unit_addr))) {
          LOG_WARN("push_back failed", KR(ret), K(unit_addr));
        } else if (unit_migrate_from_server_addr.is_valid()
            && OB_FAIL(server_array.push_back(unit_migrate_from_server_addr))) {
          LOG_WARN("push_back failed", KR(ret), K(unit_migrate_from_server_addr));
        }
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::get_alive_servers_by_tenant(
    const uint64_t tenant_id,
    common::ObIArray<common::ObAddr> &server_array,
    bool is_strict) const
{
  int ret = OB_SUCCESS;
  server_array.reset();
  common::ObArray<common::ObAddr> all_server_array;
  if (OB_FAIL(get_all_servers_by_tenant(tenant_id, all_server_array, is_strict))) {
    LOG_WARN("fail to get servers by tenant", KR(ret), K(is_strict));
  } else {
    bool is_alive = false;
    for (int i = 0; i < all_server_array.count() && OB_SUCC(ret); ++i) {
      is_alive = false;
      ObAddr unit_addr = all_server_array.at(i);
      if (OB_FAIL(SVR_TRACER.check_server_alive(unit_addr, is_alive))) {
        LOG_WARN("check_server_alive failed", KR(ret), K(unit_addr));
      } else if (is_alive) {
        if (OB_FAIL(server_array.push_back(unit_addr))) {
          LOG_WARN("push_back failed", KR(ret), K(unit_addr));
        }
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::get_units_by_unit_ids(const ObIArray<uint64_t> &unit_ids,
  common::ObIArray<ObUnit> &units)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(unit_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit_ids));
  } else {
    //防止数组太长，分批来做，100个一批, TODO,目前只有status表用到，暂时没有这个需求
    ObSqlString sql;
    ObTimeoutCtx ctx;
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    } else if (OB_FAIL(sql.assign_fmt("SELECT * from %s where unit_id in (",
                   OB_ALL_UNIT_TNAME))) {
      LOG_WARN("append_fmt failed", KR(ret));
    }
    if (OB_SUCC(ret) && unit_ids.count() > 0) {
      if (OB_FAIL(sql.append_fmt("%lu", unit_ids.at(0)))) {
        LOG_WARN("failed to append fmt", KR(ret), K(sql));
      }
    }
    for (int64_t i = 1; OB_SUCC(ret) && i < unit_ids.count(); ++i) {
      if (OB_FAIL(sql.append_fmt(", %lu", unit_ids.at(i)))) {
        LOG_WARN("failed to append fmt", KR(ret), K(sql), K(i));
      }
    }
    if (FAILEDx(sql.append(")"))) {
      LOG_WARN("failed to append", KR(ret), K(sql));
    } else if (OB_FAIL(read_units(sql, units))) {
      LOG_WARN("read_units failed", KR(ret), K(sql));
    }
  }
  return ret;
}

int ObUnitTableOperator::get_unit_by_id(common::ObISQLClient &client, const uint64_t unit_id, ObUnit &unit) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_id(unit_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit id", KR(ret), K(unit_id));
  } else {
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(sql.append_fmt("SELECT * FROM %s WHERE unit_id = %lu", OB_ALL_UNIT_TNAME, unit_id))) {
      LOG_WARN("fail to append fmt", KR(ret), K(unit_id));
    } else if (OB_FAIL(client.read(res, sql.ptr()))) {
      LOG_WARN("fail to read", KR(ret), K(sql));
    } else if (OB_ISNULL(res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get result", KR(ret));
    } else if (OB_FAIL(res.get_result()->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("unit not found", KR(ret), K(unit_id));
      } else {
        LOG_WARN("fail to iterate result", KR(ret));
      }
    } else {
      if (OB_FAIL(read_unit(*res.get_result(), unit))) {
        LOG_WARN("fail to read unit", KR(ret));
      }
    }
    }
  }
  return ret;
}

// result have no duplicate elements and exclude deleting units and 0
int ObUnitTableOperator::get_pool_unit_group_ids(
    common::ObISQLClient &client,
    const uint64_t resource_pool_id,
    ObIArray<uint64_t> &unit_group_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_id(resource_pool_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resource pool id", KR(ret), K(resource_pool_id));
  } else {
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.append_fmt("SELECT distinct unit_group_id FROM %s "
                  "WHERE resource_pool_id = %lu "
                  "AND status != 'DELETING' AND unit_group_id != 0",
                  OB_ALL_UNIT_TNAME, resource_pool_id))) {
        LOG_WARN("fail to append fmt", KR(ret));
      } else if (OB_FAIL(client.read(res, sql.ptr()))) {
        LOG_WARN("fail to read", KR(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get result", KR(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          uint64_t unit_group_id = 0;
          EXTRACT_INT_FIELD_MYSQL(*result, "unit_group_id", unit_group_id, uint64_t);
          if (FAILEDx(unit_group_ids.push_back(unit_group_id))) {
            LOG_WARN("fail to push back unit group id", KR(ret));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else if (OB_FAIL(ret)) {
          LOG_WARN("fail to iterate result", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::get_units_by_tenant(const uint64_t tenant_id,
    common::ObIArray<ObUnit> &units) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObSqlString sql;
    ObTimeoutCtx ctx;
    units.reuse();
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    } else if (OB_FAIL(sql.append_fmt("SELECT * from %s where resource_pool_id in "
             "(select resource_pool_id from %s where tenant_id = %lu)",
             OB_ALL_UNIT_TNAME, OB_ALL_RESOURCE_POOL_TNAME, tenant_id))) {
      LOG_WARN("append_fmt failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(read_units(sql, units))) {
      LOG_WARN("read_units failed", KR(ret), K(sql));
    }
  }
  return ret;
}

int ObUnitTableOperator::get_unit_stats(common::ObIArray<ObUnitStat> &unit_stats) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObSqlString sql;
    ObTimeoutCtx ctx;
    const char * normal_status_str = ObUnitInfoGetter::get_unit_status_str(ObUnitInfoGetter::ObUnitStatus::UNIT_NORMAL);
    const char * migrate_in_status_str = ObUnitInfoGetter::get_unit_status_str(ObUnitInfoGetter::ObUnitStatus::UNIT_MIGRATE_IN);
    const char * migrate_out_status_str = ObUnitInfoGetter::get_unit_status_str(ObUnitInfoGetter::ObUnitStatus::UNIT_MIGRATE_OUT);
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    } else if (OB_FAIL(sql.append_fmt("SELECT unit_id, "
             "cast(sum(data_disk_in_use) as signed) required_size, "
             "status in ('%s', '%s') is_migrating "
             "from %s where status in ('%s', '%s', '%s') group by unit_id",
             migrate_in_status_str, migrate_out_status_str,
             OB_ALL_VIRTUAL_UNIT_TNAME,
             normal_status_str, migrate_in_status_str, migrate_out_status_str)))
      // only count unit in NORMAL or MIGRATE IN or MIGRATE OUT status on servers
    {
      LOG_WARN("append_fmt failed", KR(ret));
    } else if (OB_FAIL(read_unit_stats(sql, unit_stats))) {
      LOG_WARN("read_unit_stats failed", KR(ret), K(sql));
    }
  }
  return ret;
}

int ObUnitTableOperator::get_tenant_servers(
    common::ObIArray<ObTenantServers> &all_tenant_servers,
    const uint64_t tenant_id /* = OB_INVALID_TENANT_ID */) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObSqlString sql;
    ObTimeoutCtx ctx;
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    } else if (OB_FAIL(sql.append_fmt("SELECT "
              "tenant_id, svr_ip, svr_port, migrate_from_svr_ip, migrate_from_svr_port "
              "from %s unit join %s pool "
              "on unit.resource_pool_id = pool.resource_pool_id ",
              OB_ALL_UNIT_TNAME, OB_ALL_RESOURCE_POOL_TNAME))) {
      LOG_WARN("append_fmt failed", KR(ret));
    } else if (is_valid_tenant_id(tenant_id)) {
      if (OB_FAIL(sql.append_fmt("where tenant_id = %lu", tenant_id))) {
        LOG_WARN("sql append_fmt failed", KR(ret));
      }
    } else if (OB_FAIL(sql.append_fmt("where tenant_id != -1 order by tenant_id"))) {
      LOG_WARN("sql append_fmt failed", KR(ret));
    }
    if (FAILEDx(read_tenant_servers(sql, all_tenant_servers))) {
      LOG_WARN("read_tenant_servers failed", KR(ret), K(sql));
    }
  }
  return ret;
}

int ObUnitTableOperator::zone_list2str(const ObIArray<ObZone> &zone_list,
                                       char *str, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (zone_list.count() <= 0 || NULL == str || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(zone_list), KP(str), K(buf_size), K(ret));
  } else {
    memset(str, 0, static_cast<uint32_t>(buf_size));
    int64_t nwrite = 0;
    int64_t n = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
      n = snprintf(str + nwrite, static_cast<uint32_t>(buf_size - nwrite),
          "%s%s", zone_list.at(i).ptr(), (i != zone_list.count() - 1) ? ";" : "");
      if (n <= 0 || n >= buf_size - nwrite) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("snprintf failed", K(ret));
      } else {
        nwrite += n;
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::str2zone_list(const char *str,
                                       ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  char *item_str = NULL;
  char *save_ptr = NULL;
  zone_list.reuse();
  if (NULL == str) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("str is null", KP(str), K(ret));
  } else {
    while (OB_SUCC(ret)) {
      item_str = strtok_r((NULL == item_str ? const_cast<char *>(str) : NULL), ";", &save_ptr);
      if (NULL != item_str) {
        if (OB_FAIL(zone_list.push_back(ObZone(item_str)))) {
          LOG_WARN("push_back failed", K(ret));
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::read_unit(const ObMySQLResult &result, ObUnit &unit)
{
  int ret = OB_SUCCESS;
  unit.reset();
  int64_t tmp_real_str_len = 0; // used to fill output argument
  char ip[OB_IP_STR_BUFF] = "";
  int64_t port = 0;
  char migrate_from_svr_ip[OB_IP_STR_BUFF] = "";
  char status[MAX_UNIT_STATUS_LENGTH] = "";
  int64_t migrate_from_svr_port = 0;
  common::ObReplicaType replica_type_in_table = ObReplicaType::REPLICA_TYPE_FULL;
    EXTRACT_INT_FIELD_MYSQL(result, "unit_id", unit.unit_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "resource_pool_id", unit.resource_pool_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "unit_group_id", unit.unit_group_id_, uint64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(result, "zone", unit.zone_.ptr(), MAX_ZONE_LENGTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(result, "svr_ip", ip, OB_IP_STR_BUFF, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(result, "svr_port", port, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(result, "migrate_from_svr_ip", migrate_from_svr_ip,
                               OB_IP_STR_BUFF, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(result, "migrate_from_svr_port", migrate_from_svr_port, int64_t);
    EXTRACT_BOOL_FIELD_MYSQL(result, "manual_migrate", unit.is_manual_migrate_);
    EXTRACT_STRBUF_FIELD_MYSQL(result, "status", status, MAX_UNIT_STATUS_LENGTH, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "replica_type", replica_type_in_table, common::ObReplicaType);
    EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(result, "gmt_modified", unit.time_stamp_);
    (void) tmp_real_str_len; // make compiler happy
    unit.server_.set_ip_addr(ip, static_cast<int32_t>(port));
    unit.migrate_from_server_.set_ip_addr(
        migrate_from_svr_ip, static_cast<int32_t>(migrate_from_svr_port));
    unit.status_ = ObUnit::str_to_unit_status(status);
    unit.replica_type_ = replica_type_in_table;
  return ret;
}

int ObUnitTableOperator::read_resource_pool(const ObMySQLResult &result,
                                            ObResourcePool &resource_pool) const
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0; // used to fill output argument
  common::ObReplicaType replica_type_in_table = ObReplicaType::REPLICA_TYPE_FULL;
  SMART_VAR(char[MAX_ZONE_LIST_LENGTH], zone_list_str) {
    zone_list_str[0] = '\0';

    resource_pool.reset();
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else {
      EXTRACT_INT_FIELD_MYSQL(result, "resource_pool_id", resource_pool.resource_pool_id_, uint64_t);
      EXTRACT_STRBUF_FIELD_MYSQL(result, "name", resource_pool.name_.ptr(),
                                 MAX_RESOURCE_POOL_LENGTH, tmp_real_str_len);
      EXTRACT_INT_FIELD_MYSQL(result, "unit_count", resource_pool.unit_count_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(result, "unit_config_id", resource_pool.unit_config_id_, uint64_t);
      EXTRACT_STRBUF_FIELD_MYSQL(result, "zone_list", zone_list_str,
                                 MAX_ZONE_LIST_LENGTH, tmp_real_str_len);
      EXTRACT_INT_FIELD_MYSQL(result, "tenant_id", resource_pool.tenant_id_, uint64_t);
      EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "replica_type", replica_type_in_table, common::ObReplicaType);
      (void) tmp_real_str_len; // make compiler happy
      if (OB_SUCC(ret)) {
        if (OB_FAIL(str2zone_list(zone_list_str, resource_pool.zone_list_))) {
          LOG_WARN("str2zone_list failed", K(zone_list_str), K(ret));
        } else {
          resource_pool.replica_type_ = replica_type_in_table;
        }
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::read_unit_config(const ObMySQLResult &result,
                                          ObUnitConfig &unit_config) const
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0; // used to fill output argument
  unit_config.reset();
  uint64_t unit_config_id = OB_INVALID_ID;
  ObUnitConfigName name;
  double max_cpu = 0;
  double min_cpu = 0;
  int64_t memory_size = 0;
  int64_t log_disk_size = 0;
  int64_t data_disk_size = 0;
  int64_t max_iops = 0;
  int64_t min_iops = 0;
  int64_t iops_weight = ObUnitResource::INVALID_IOPS_WEIGHT;
  int64_t max_net_bandwidth = ObUnitResource::INVALID_NET_BANDWIDTH;
  int64_t net_bandwidth_weight = ObUnitResource::INVALID_NET_BANDWIDTH_WEIGHT;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    EXTRACT_INT_FIELD_MYSQL(result, "unit_config_id", unit_config_id, uint64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(result, "name", name.ptr(),
                               name.capacity(), tmp_real_str_len);
    (void) tmp_real_str_len; // make compiler happy
    EXTRACT_DOUBLE_FIELD_MYSQL(result, "max_cpu", max_cpu, double);
    EXTRACT_DOUBLE_FIELD_MYSQL(result, "min_cpu", min_cpu, double);
    EXTRACT_INT_FIELD_MYSQL(result, "memory_size", memory_size, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "log_disk_size", log_disk_size, int64_t);
    EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "data_disk_size", data_disk_size, int64_t,
      false/*skip_null_error*/, true/*skip_column_error*/, ObUnitResource::DEFAULT_DATA_DISK_SIZE);
    EXTRACT_INT_FIELD_MYSQL(result, "max_iops", max_iops, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "min_iops", min_iops, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "iops_weight", iops_weight, int64_t);
    EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "max_net_bandwidth", max_net_bandwidth, int64_t,
      false/*skip_null_error*/, true/*skip_column_error*/, ObUnitResource::DEFAULT_NET_BANDWIDTH);
    EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "net_bandwidth_weight", net_bandwidth_weight, int64_t,
      false/*skip_null_error*/, true/*skip_column_error*/, ObUnitResource::DEFAULT_NET_BANDWIDTH_WEIGHT);

    if (-1 == data_disk_size) {
      // for compatability, lower version default value may be -1. Set as 0.
      data_disk_size = 0;
    }

    if (OB_SUCCESS == ret) {
      const ObUnitResource ur(
          max_cpu,
          min_cpu,
          memory_size,
          log_disk_size,
          data_disk_size,
          max_iops,
          min_iops,
          iops_weight,
          max_net_bandwidth,
          net_bandwidth_weight);

      if (OB_FAIL(unit_config.init(unit_config_id, name, ur))) {
        LOG_WARN("init unit config fail", KR(ret), K(unit_config_id), K(name), K(ur));
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::read_unit_stat(const ObMySQLResult &result, ObUnitStat &unit_stat) const
{
  int ret = OB_SUCCESS;
  unit_stat.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    uint64_t unit_id = OB_INVALID_ID;
    int64_t required_size = 0;
    bool is_migrating = false;
    EXTRACT_INT_FIELD_MYSQL(result, "unit_id", unit_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "required_size", required_size, int64_t);
    EXTRACT_BOOL_FIELD_MYSQL(result, "is_migrating", is_migrating);
    if (FAILEDx(unit_stat.init(unit_id, required_size, is_migrating))) {
      LOG_WARN("failed to init unit_stat", KR(ret), K(unit_id));
    }
  }
  return ret;
}

#define READ_ITEMS(Item, item) \
  do { \
    if (OB_ISNULL(proxy_)) { \
      ret = OB_NOT_INIT; \
      LOG_WARN("invalid proxy", KR(ret), K(proxy_)); \
    } else { \
      ObSQLClientRetryWeak sql_client_retry_weak(proxy_); \
      SMART_VAR(ObMySQLProxy::MySQLResult, res) { \
        ObMySQLResult *result = NULL; \
        if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) { \
          LOG_WARN("execute sql failed", K(sql), K(ret)); \
        } else if (NULL == (result = res.get_result())) { \
          ret = OB_ERR_UNEXPECTED; \
          LOG_WARN("result is not expected to be NULL", "result", OB_P(result), K(ret)); \
        } else { \
          while (OB_SUCC(ret)) { \
            Item item; \
            if (OB_FAIL(result->next())) { \
              if (OB_ITER_END == ret) { \
                ret = OB_SUCCESS; \
                break; \
              } else { \
                LOG_WARN("get next result failed", K(ret)); \
              } \
            } else if (OB_FAIL(read_##item(*result, item))) { \
              LOG_WARN("read_##item failed", K(ret)); \
            } else if (OB_FAIL(item##s.push_back(item))) { \
              LOG_WARN("push_back failed", K(item), K(ret)); \
            } \
          } \
        } \
      } \
    } \
  } while (false)

int ObUnitTableOperator::read_unit_configs(ObSqlString &sql,
                                           ObIArray<ObUnitConfig> &unit_configs) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql", K(sql), K(ret));
  } else {
    READ_ITEMS(ObUnitConfig, unit_config);
  }
  return ret;
}

int ObUnitTableOperator::read_units(ObSqlString &sql,
                                    ObIArray<ObUnit> &units) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql", K(sql), K(ret));
  } else {
    READ_ITEMS(ObUnit, unit);
  }
  return ret;
}

int ObUnitTableOperator::read_unit_stats(ObSqlString &sql, ObIArray<ObUnitStat> &unit_stats) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql", K(sql), K(ret));
  } else {
    READ_ITEMS(ObUnitStat, unit_stat);
  }
  return ret;
}

int ObUnitTableOperator::read_resource_pools(ObSqlString &sql,
                                             ObIArray<ObResourcePool> &resource_pools) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql", K(sql), K(ret));
  } else {
    READ_ITEMS(ObResourcePool, resource_pool);
  }
  return ret;
}

int ObUnitTableOperator::read_tenants(ObSqlString &sql,
                                      ObIArray<uint64_t> &tenants) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql", K(sql), K(ret));
  } else {
    READ_ITEMS(uint64_t, tenant);
  }
  return ret;
}
#undef READ_ITEMS

int ObUnitTableOperator::read_tenant_server(
    const ObMySQLResult &result,
    uint64_t &tenant_id,
    common::ObAddr &server,
    common::ObAddr &migrate_from_server) const
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_TENANT_ID;
  server.reset();
  migrate_from_server.reset();
  ObString ip;
  int32_t port = 0;
  ObString migrate_from_svr_ip;
  int32_t migrate_from_svr_port = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    EXTRACT_INT_FIELD_MYSQL(result, "tenant_id", tenant_id, uint64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "svr_ip", ip);
    EXTRACT_INT_FIELD_MYSQL(result, "svr_port", port, int32_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "migrate_from_svr_ip", migrate_from_svr_ip);
    EXTRACT_INT_FIELD_MYSQL(result, "migrate_from_svr_port", migrate_from_svr_port, int32_t);
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(false == server.set_ip_addr(ip, port))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to use ip and port to set the server", KR(ret), K(ip), K(port));
    } else if (OB_UNLIKELY(false == migrate_from_server.set_ip_addr(
          migrate_from_svr_ip,
          migrate_from_svr_port))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to use ip and port to set the migrate_from_server", KR(ret), K(migrate_from_svr_ip), K(migrate_from_svr_port));
    }
  }
  return ret;
}

int ObUnitTableOperator::read_tenant_servers(
    ObSqlString &sql,
    ObIArray<ObTenantServers> &all_tenant_servers) const
{
  int ret = OB_SUCCESS;
  all_tenant_servers.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(sql.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql", KR(ret), K(sql));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid proxy", KR(ret), K(proxy_));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(proxy_->read(res, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (nullptr == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is not expected to be NULL", KR(ret), "result", OB_P(result));
      } else {
        ObTenantServers tenant_servers;
        const int64_t renew_time = ObTimeUtility::current_time();
        while (OB_SUCC(ret)) {
          uint64_t tenant_id = OB_INVALID_TENANT_ID;
          common::ObAddr server;
          common::ObAddr migrate_server;
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("get next result failed", KR(ret));
            }
          } else if (OB_FAIL(read_tenant_server(*result, tenant_id, server, migrate_server))) {
            LOG_WARN("read_tenant_server failed", KR(ret));
          } else {
            if (tenant_servers.is_valid()
                  && tenant_id != tenant_servers.get_tenant_id()) {
              if (OB_FAIL(all_tenant_servers.push_back(tenant_servers))) {
                LOG_WARN("push back failed", KR(ret), K(tenant_servers));
              } else {
                tenant_servers.reset();
              }
            }
            if (FAILEDx(tenant_servers.init_or_insert_server(
                  tenant_id,
                  server,
                  migrate_server,
                  renew_time))) {
              LOG_WARN("failed to init_or_insert_server", KR(ret),
                  K(tenant_id), K(server), K(migrate_server), K(renew_time));
            }
          } // end else
        } // end while

        // put the last tenant_servers into tenant_servers
        if (OB_FAIL(ret)) {
        } else if (OB_UNLIKELY(!tenant_servers.is_valid())) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("the query result is empty because the table is empty or there is no required value in the table",
              KR(ret), K(sql));
        } else if (OB_FAIL(all_tenant_servers.push_back(tenant_servers))) {
          LOG_WARN("push_back failed", KR(ret), K(tenant_servers));
        }
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::check_tenant_has_logonly_pools(
    const uint64_t &tenant_id,
    bool &has_logonly_pools) const
{
  int ret = OB_SUCCESS;
  has_logonly_pools = false;
  common::ObArray<ObResourcePool> pools;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("unit table operator not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_resource_pools(gen_user_tenant_id(tenant_id), pools))) {
    LOG_WARN("fail to get resource pools by tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(inner_check_has_logonly_pool_(pools, has_logonly_pools))) {
    LOG_WARN("fail to inner check whether has logonly pools", KR(ret), K(pools));
  }
  LOG_INFO("finish check whether tenant has logonly resource pool", KR(ret), K(tenant_id),
           K(pools), K(has_logonly_pools));
  return ret;
}

int ObUnitTableOperator::check_has_logonly_pools(
    const ObIArray<share::ObResourcePoolName> &pools,
    bool &has_logonly_pools) const
{
  int ret = OB_SUCCESS;
  has_logonly_pools = false;
  ObSqlString sql;
  ObSqlString in_condition;
  common::ObArray<ObResourcePool> resource_pools;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("unit table operator not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(0 >= pools.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pools));
  } else {
    // construct where condition by resource pool names
    bool need_append_comma = false;
    for (int64_t index = 0; index < pools.count() && OB_SUCC(ret); ++index) {
      const ObResourcePoolName &pool_name = pools.at(index);
      if (need_append_comma && OB_FAIL(in_condition.append(", "))) {
        LOG_WARN("fail to append comma", KR(ret));
      } else if (OB_FAIL(in_condition.append_fmt("\"%s\"", pool_name.ptr()))) {
        LOG_WARN("fail to append resource pool name", KR(ret), K(pool_name));
      } else {
        need_append_comma = true;
      }
    }
    // construct whole sql
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt("select * from %s where name in (%s)",
        OB_ALL_RESOURCE_POOL_TNAME, in_condition.ptr()))) {
      LOG_WARN("append_fmt failed", KR(ret), K(in_condition));
    } else if (OB_FAIL(read_resource_pools(sql, resource_pools))) {
      LOG_WARN("read_resource_pools failed", K(sql), KR(ret));
    } else if (OB_FAIL(inner_check_has_logonly_pool_(resource_pools, has_logonly_pools))) {
      LOG_WARN("fail to inner check whether has logonly pools", KR(ret), K(resource_pools));
    }
  }
  LOG_INFO("finish check has logonly pools", KR(ret), K(pools), K(in_condition), K(sql),
           K(resource_pools), K(has_logonly_pools));
  return ret;
}

int ObUnitTableOperator::inner_check_has_logonly_pool_(
    const common::ObIArray<ObResourcePool> &pools,
    bool &has_logonly_pools) const
{
  int ret = OB_SUCCESS;
  has_logonly_pools = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("unit table operator not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(0 >= pools.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pools));
  } else {
    for (int64_t index = 0; index < pools.count() && OB_SUCC(ret); ++index) {
      if (ObReplicaTypeCheck::is_log_replica(pools.at(index).replica_type_)) {
        has_logonly_pools = true;
        break;
      }
    }
  }
  return ret;
}

}//end namespace share
}//end namespace oceanbase
