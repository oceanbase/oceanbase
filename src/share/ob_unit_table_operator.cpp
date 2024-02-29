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

#include "share/ob_unit_table_operator.h"

#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/config/ob_server_config.h"
#include "share/ob_unit_getter.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_sql_client_decorator.h"
#include "common/ob_timeout_ctx.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/tenant_snapshot/ob_tenant_snapshot_util.h" // ObTenantSnapshotUtil

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
namespace share
{
ObUnitTableOperator::ObUnitTableOperator()
  : inited_(false),
    proxy_(NULL)
{
}

ObUnitTableOperator::~ObUnitTableOperator()
{
}

int ObUnitTableOperator::init(common::ObMySQLProxy &proxy)
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
    LOG_DEBUG("server exists in the server list or migrate_from_server list", K(server), K(units));
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

int ObUnitTableOperator::update_unit(common::ObISQLClient &client,
                                     const ObUnit &unit,
                                     const bool need_check_conflict_with_clone)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char migrate_from_svr_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  const char *unit_status_str = NULL;
  rootserver::ObConflictCaseWithClone case_to_check(rootserver::ObConflictCaseWithClone::MODIFY_UNIT);
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
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
  } else if (need_check_conflict_with_clone) {
    if (OB_FAIL(rootserver::ObTenantSnapshotUtil::lock_unit_for_tenant(client, unit, tenant_id))) {
      LOG_WARN("fail to lock __all_unit_table for clone check", KR(ret), K(unit), K(need_check_conflict_with_clone));
    } else if (!is_valid_tenant_id(tenant_id)) {
      // this unit is not granted to tenant, just ignore
    } else if (OB_FAIL(rootserver::ObTenantSnapshotUtil::check_tenant_not_in_cloning_procedure(tenant_id, case_to_check))) {
      LOG_WARN("fail to check whether tenant is cloning", KR(ret), K(tenant_id), K(case_to_check), K(need_check_conflict_with_clone));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(unit.get_unit_status_str(unit_status_str))) {
    LOG_WARN("fail to get unit status", K(ret));
  } else if (OB_UNLIKELY(NULL == unit_status_str)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null unit status", K(ret), KP(unit_status_str));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(OBJ_K(unit, unit_id)))
        || OB_FAIL(dml.add_column(OBJ_K(unit, resource_pool_id)))
        || OB_FAIL(dml.add_column(OBJ_K(unit, unit_group_id)))
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
      ObDMLExecHelper exec(client, OB_SYS_TENANT_ID);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_insert_update(OB_ALL_UNIT_TNAME, dml, affected_rows))) {
        LOG_WARN("exec insert_update failed", "table", OB_ALL_UNIT_TNAME, K(ret));
      } else if (is_zero_row(affected_rows) || affected_rows > 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect update single row", K(affected_rows), K(ret));
      }
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
      } else if (OB_FAIL(rootserver::ObTenantSnapshotUtil::lock_resource_pool_for_tenant(
                      client, resource_pool))) {
        LOG_WARN("fail to lock the resource pool to update", KR(ret), K(resource_pool), K(need_check_conflict_with_clone));
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
    } else if (OB_FAIL(sql.append_fmt("select unit_config_id, name, "
        "max_cpu, min_cpu, memory_size, log_disk_size, "
        "max_iops, min_iops, iops_weight from %s ", OB_ALL_UNIT_CONFIG_TNAME))) {
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
    } else if (OB_FAIL(sql.append_fmt("select unit_config_id, name, "
        "max_cpu, min_cpu, memory_size, log_disk_size, "
        "max_iops, min_iops, iops_weight from %s "
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
    if (OB_FAIL(dml.add_pk_column("unit_config_id", config.unit_config_id()))
        || OB_FAIL(dml.add_column("name", ObHexEscapeSqlStr(unit_name)))
        || OB_FAIL(dml.add_column("max_cpu", config.max_cpu()))
        || OB_FAIL(dml.add_column("min_cpu", config.min_cpu()))
        || OB_FAIL(dml.add_column("memory_size", config.memory_size()))
        || OB_FAIL(dml.add_column("log_disk_size", config.log_disk_size()))
        || OB_FAIL(dml.add_column("max_iops", config.max_iops()))
        || OB_FAIL(dml.add_column("min_iops", config.min_iops()))
        || OB_FAIL(dml.add_column("iops_weight", config.iops_weight()))
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

int ObUnitTableOperator::get_unit_groups_by_tenant(const uint64_t tenant_id,
                          common::ObIArray<ObSimpleUnitGroup> &unit_groups) const
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
    } else if (OB_FAIL(sql.append_fmt("SELECT distinct unit_group_id, status from "
                   "%s where resource_pool_id in "
                   "(select resource_pool_id from %s where tenant_id = %lu) order by unit_group_id ",
                   OB_ALL_UNIT_TNAME, OB_ALL_RESOURCE_POOL_TNAME, tenant_id))) {
      LOG_WARN("append_fmt failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(read_unit_groups(sql, unit_groups))) {
      LOG_WARN("failed to read unit group", KR(ret), K(sql));
    } else {
      uint64_t last_unit_group_id = OB_INVALID_ID;
      for (int64_t i = 0; OB_SUCC(ret) && i < unit_groups.count(); ++i) {
        const uint64_t current_id = unit_groups.at(i).get_unit_group_id();
        if (OB_INVALID_ID == last_unit_group_id || last_unit_group_id != current_id) {
          last_unit_group_id = current_id;
        } else if (last_unit_group_id == current_id) {
          //one ls group can not has different status
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("has unit group with different status", KR(ret), K(last_unit_group_id),
                   K(current_id), K(unit_groups));
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

int ObUnitTableOperator::read_unit_group(const ObMySQLResult &result, ObSimpleUnitGroup &unit_group) const
{
  int ret = OB_SUCCESS;
  unit_group.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObString status_str;
    uint64_t unit_group_id = OB_INVALID_ID;
    EXTRACT_INT_FIELD_MYSQL(result, "unit_group_id", unit_group_id, uint64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "status", status_str);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to get cell", KR(ret), K(unit_group_id), K(status_str));
    } else {
      ObUnit::Status status = ObUnit::str_to_unit_status(status_str);
      unit_group = ObSimpleUnitGroup(unit_group_id, status);
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
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "replica_type", unit.replica_type_, common::ObReplicaType);
  (void) tmp_real_str_len; // make compiler happy
  unit.server_.set_ip_addr(ip, static_cast<int32_t>(port));
  unit.migrate_from_server_.set_ip_addr(
      migrate_from_svr_ip, static_cast<int32_t>(migrate_from_svr_port));
  unit.status_ = ObUnit::str_to_unit_status(status);
  return ret;
}

int ObUnitTableOperator::read_resource_pool(const ObMySQLResult &result,
                                            ObResourcePool &resource_pool) const
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0; // used to fill output argument
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
      EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "replica_type", resource_pool.replica_type_, common::ObReplicaType);
      (void) tmp_real_str_len; // make compiler happy
      if (OB_SUCC(ret)) {
        if (OB_FAIL(str2zone_list(zone_list_str, resource_pool.zone_list_))) {
          LOG_WARN("str2zone_list failed", K(zone_list_str), K(ret));
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
  int64_t max_iops = 0;
  int64_t min_iops = 0;
  int64_t iops_weight = ObUnitResource::INVALID_IOPS_WEIGHT;

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
    EXTRACT_INT_FIELD_MYSQL(result, "max_iops", max_iops, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "min_iops", min_iops, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "iops_weight", iops_weight, int64_t);

    if (OB_SUCCESS == ret) {
      const ObUnitResource ur(
          max_cpu,
          min_cpu,
          memory_size,
          log_disk_size,
          max_iops,
          min_iops,
          iops_weight);

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

int ObUnitTableOperator::read_unit_groups(ObSqlString &sql,
                                    ObIArray<ObSimpleUnitGroup> &unit_groups) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(sql.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql", K(sql), KR(ret));
  } else {
    READ_ITEMS(ObSimpleUnitGroup, unit_group);
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

}//end namespace share
}//end namespace oceanbase
