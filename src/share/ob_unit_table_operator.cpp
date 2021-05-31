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
#include "observer/ob_server_struct.h"
#include "observer/ob_sql_client_decorator.h"
#include "common/ob_timeout_ctx.h"
#include "rootserver/ob_root_utils.h"

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
namespace share {
ObUnitTableOperator::ObUnitTableOperator() : inited_(false), proxy_(NULL), config_(NULL)
{}

ObUnitTableOperator::~ObUnitTableOperator()
{}

int ObUnitTableOperator::init(common::ObMySQLProxy& proxy, common::ObServerConfig* config)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    proxy_ = &proxy;
    config_ = config;
    inited_ = true;
  }
  return ret;
}

int ObUnitTableOperator::get_units(common::ObIArray<ObUnit>& units) const
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
    } else if (OB_FAIL(sql.append_fmt("SELECT * from %s", OB_ALL_UNIT_TNAME))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(read_units(sql, units))) {
      LOG_WARN("read_units failed", K(sql), K(ret));
    }
  }
  return ret;
}

// get all tenant ids
int ObUnitTableOperator::get_tenants(common::ObIArray<uint64_t>& tenants) const
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

int ObUnitTableOperator::read_tenant(const ObMySQLResult& result, uint64_t& tenant_id) const
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

int ObUnitTableOperator::get_units(const common::ObAddr& server, common::ObIArray<ObUnit>& units) const
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
                   OB_ALL_UNIT_TNAME,
                   ip,
                   server.get_port(),
                   ip,
                   server.get_port()))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(read_units(sql, units))) {
      LOG_WARN("read_units failed", K(sql), K(ret));
    }
  }
  return ret;
}

int ObUnitTableOperator::get_units(const common::ObIArray<uint64_t>& pool_ids, common::ObIArray<ObUnit>& units) const
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

int ObUnitTableOperator::update_unit(common::ObISQLClient& client, const ObUnit& unit)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char migrate_from_svr_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  const char* unit_status_str = NULL;
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
      if (false == unit.migrate_from_server_.ip_to_string(migrate_from_svr_ip, sizeof(migrate_from_svr_ip))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("convert server ip to string failed", "server", unit.migrate_from_server_, K(ret));
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
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(OBJ_K(unit, unit_id))) || OB_FAIL(dml.add_column(OBJ_K(unit, resource_pool_id))) ||
        OB_FAIL(dml.add_column(OBJ_K(unit, group_id))) || OB_FAIL(dml.add_column("zone", unit.zone_.ptr())) ||
        OB_FAIL(dml.add_column("svr_ip", ip)) || OB_FAIL(dml.add_column("svr_port", unit.server_.get_port())) ||
        OB_FAIL(dml.add_column(K(migrate_from_svr_ip))) ||
        OB_FAIL(dml.add_column("migrate_from_svr_port", unit.migrate_from_server_.get_port())) ||
        OB_FAIL(dml.add_gmt_modified()) || OB_FAIL(dml.add_column("manual_migrate", unit.is_manual_migrate())) ||
        OB_FAIL(dml.add_column("replica_type", unit.replica_type_)) ||
        OB_FAIL(dml.add_column("status", unit_status_str))) {
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

int ObUnitTableOperator::remove_units(common::ObISQLClient& client, const uint64_t resource_pool_id)
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
    if (OB_FAIL(sql.append_fmt("delete from %s where resource_pool_id = %ld", OB_ALL_UNIT_TNAME, resource_pool_id))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(client.write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    }
  }
  return ret;
}

int ObUnitTableOperator::remove_unit(common::ObISQLClient& client, const ObUnit& unit)
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
    if (OB_FAIL(sql.append_fmt("delete from %s where unit_id = %ld", OB_ALL_UNIT_TNAME, unit.unit_id_))) {
      LOG_WARN("append fmt failed", K(ret));
    } else if (OB_FAIL(client.write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

int ObUnitTableOperator::remove_units_in_zones(common::ObISQLClient& client, const uint64_t resource_pool_id,
    const common::ObIArray<common::ObZone>& to_be_removed_zones)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == resource_pool_id) || OB_UNLIKELY(to_be_removed_zones.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(resource_pool_id), K(to_be_removed_zones));
  } else {
    ObSqlString sql;
    for (int64_t i = 0; OB_SUCC(ret) && i < to_be_removed_zones.count(); ++i) {
      int64_t affected_rows = 0;
      sql.reset();
      const common::ObZone& zone = to_be_removed_zones.at(i);
      if (OB_FAIL(sql.append_fmt("delete from %s where resource_pool_id = %ld "
                                 "and zone = '%s'",
              OB_ALL_UNIT_TNAME,
              resource_pool_id,
              zone.ptr()))) {
        LOG_WARN("append_fmt failed", K(ret));
      } else if (OB_FAIL(client.write(sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObUnitTableOperator::get_resource_pools(common::ObIArray<ObResourcePool>& pools) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.append_fmt("select * from %s", OB_ALL_RESOURCE_POOL_TNAME))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(read_resource_pools(sql, pools))) {
      LOG_WARN("read_resource_pools failed", K(sql), K(ret));
    }
  }
  return ret;
}

int ObUnitTableOperator::get_resource_pools(const uint64_t tenant_id, common::ObIArray<ObResourcePool>& pools) const
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
    if (OB_FAIL(sql.append_fmt("select * from %s where tenant_id = %lu", OB_ALL_RESOURCE_POOL_TNAME, tenant_id))) {
      LOG_WARN("append_fmt failed", K(tenant_id), K(ret));
    } else if (OB_FAIL(read_resource_pools(sql, pools))) {
      LOG_WARN("read_resource_pools failed", K(sql), K(ret));
    }
  }
  return ret;
}

int ObUnitTableOperator::get_resource_pools(
    const common::ObIArray<uint64_t>& pool_ids, common::ObIArray<ObResourcePool>& pools) const
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
                               "where resource_pool_id in (",
            OB_ALL_RESOURCE_POOL_TNAME))) {
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

int ObUnitTableOperator::update_resource_pool(common::ObISQLClient& client, const ObResourcePool& resource_pool)
{
  int ret = OB_SUCCESS;
  char zone_list_str[MAX_ZONE_LIST_LENGTH] = "";
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!resource_pool.is_valid() || OB_INVALID_ID == resource_pool.unit_config_id_ ||
             OB_INVALID_ID == resource_pool.resource_pool_id_ || resource_pool.zone_list_.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(resource_pool), K(ret));
  } else if (OB_FAIL(zone_list2str(resource_pool.zone_list_, zone_list_str, MAX_ZONE_LIST_LENGTH))) {
    LOG_WARN("zone_list2str failed", "zone_list", resource_pool.zone_list_, K(ret));
  } else {
    ObDMLSqlSplicer dml;
    ObString resource_pool_name(strlen(resource_pool.name_.ptr()), resource_pool.name_.ptr());
    if (OB_FAIL(dml.add_pk_column(OBJ_K(resource_pool, resource_pool_id))) ||
        OB_FAIL(dml.add_column("name", ObHexEscapeSqlStr(resource_pool_name))) ||
        OB_FAIL(dml.add_column(OBJ_K(resource_pool, unit_count))) ||
        OB_FAIL(dml.add_column(OBJ_K(resource_pool, unit_config_id))) ||
        OB_FAIL(dml.add_column("zone_list", zone_list_str)) ||
        OB_FAIL(dml.add_column(OBJ_K(resource_pool, tenant_id))) ||
        OB_FAIL(dml.add_column(OBJ_K(resource_pool, replica_type))) || OB_FAIL(dml.add_gmt_modified())) {
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
  return ret;
}

int ObUnitTableOperator::remove_resource_pool(common::ObISQLClient& client, const uint64_t resource_pool_id)
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
    if (OB_FAIL(sql.append_fmt(
            "delete from %s where resource_pool_id = %ld", OB_ALL_RESOURCE_POOL_TNAME, resource_pool_id))) {
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

int ObUnitTableOperator::get_unit_configs(common::ObIArray<ObUnitConfig>& configs) const
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
                                      "max_cpu, min_cpu, max_memory, min_memory, max_disk_size, "
                                      "max_iops, min_iops, max_session_num from %s ",
                   OB_ALL_UNIT_CONFIG_TNAME))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(read_unit_configs(sql, configs))) {
      LOG_WARN("read_unit_configs failed", K(sql), K(ret));
    }
  }
  return ret;
}

int ObUnitTableOperator::get_unit_configs(
    const common::ObIArray<uint64_t>& config_ids, common::ObIArray<ObUnitConfig>& configs) const
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
                                      "max_cpu, min_cpu, max_memory, min_memory, max_disk_size, "
                                      "max_iops, min_iops, max_session_num from %s "
                                      "where unit_config_id in (",
                   OB_ALL_UNIT_CONFIG_TNAME))) {
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

int ObUnitTableOperator::update_unit_config(common::ObISQLClient& client, const ObUnitConfig& config)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!config.is_valid() || OB_INVALID_ID == config.unit_config_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(config), K(ret));
  } else {
    ObDMLSqlSplicer dml;
    ObString unit_name(strlen(config.name_.ptr()), config.name_.ptr());
    if (OB_FAIL(dml.add_pk_column(OBJ_K(config, unit_config_id))) ||
        OB_FAIL(dml.add_column("name", ObHexEscapeSqlStr(unit_name))) ||
        OB_FAIL(dml.add_column(OBJ_K(config, max_cpu))) || OB_FAIL(dml.add_column(OBJ_K(config, min_cpu))) ||
        OB_FAIL(dml.add_column(OBJ_K(config, max_memory))) || OB_FAIL(dml.add_column(OBJ_K(config, min_memory))) ||
        OB_FAIL(dml.add_column(OBJ_K(config, max_disk_size))) || OB_FAIL(dml.add_column(OBJ_K(config, max_iops))) ||
        OB_FAIL(dml.add_column(OBJ_K(config, min_iops))) || OB_FAIL(dml.add_column(OBJ_K(config, max_session_num))) ||
        OB_FAIL(dml.add_gmt_modified())) {
      LOG_WARN("add column failed", K(ret));
    } else {
      ObDMLExecHelper exec(client, OB_SYS_TENANT_ID);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_insert_update(OB_ALL_UNIT_CONFIG_TNAME, dml, affected_rows))) {
        LOG_WARN("exec insert_update failed", "table", OB_ALL_UNIT_TNAME, K(ret));
      } else if (is_zero_row(affected_rows) || affected_rows > 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect update single row", K(affected_rows), K(ret));
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::remove_unit_config(common::ObISQLClient& client, const uint64_t unit_config_id)
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
    if (OB_FAIL(
            sql.append_fmt("delete from %s where unit_config_id = %ld", OB_ALL_UNIT_CONFIG_TNAME, unit_config_id))) {
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

int ObUnitTableOperator::zone_list2str(const ObIArray<ObZone>& zone_list, char* str, const int64_t buf_size)
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
      n = snprintf(str + nwrite,
          static_cast<uint32_t>(buf_size - nwrite),
          "%s%s",
          zone_list.at(i).ptr(),
          (i != zone_list.count() - 1) ? ";" : "");
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

int ObUnitTableOperator::str2zone_list(const char* str, ObIArray<ObZone>& zone_list)
{
  int ret = OB_SUCCESS;
  char* item_str = NULL;
  char* save_ptr = NULL;
  zone_list.reuse();
  if (NULL == str) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("str is null", KP(str), K(ret));
  } else {
    while (OB_SUCC(ret)) {
      item_str = strtok_r((NULL == item_str ? const_cast<char*>(str) : NULL), ";", &save_ptr);
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

int ObUnitTableOperator::read_unit(const ObMySQLResult& result, ObUnit& unit) const
{
  int ret = OB_SUCCESS;
  unit.reset();
  int64_t tmp_real_str_len = 0;  // used to fill output argument
  char ip[OB_IP_STR_BUFF] = "";
  int64_t port = 0;
  char migrate_from_svr_ip[OB_IP_STR_BUFF] = "";
  char status[MAX_UNIT_STATUS_LENGTH] = "";
  int64_t migrate_from_svr_port = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    EXTRACT_INT_FIELD_MYSQL(result, "unit_id", unit.unit_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "resource_pool_id", unit.resource_pool_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "group_id", unit.group_id_, uint64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(result, "zone", unit.zone_.ptr(), MAX_ZONE_LENGTH, tmp_real_str_len);
    EXTRACT_STRBUF_FIELD_MYSQL(result, "svr_ip", ip, OB_IP_STR_BUFF, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(result, "svr_port", port, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(result, "migrate_from_svr_ip", migrate_from_svr_ip, OB_IP_STR_BUFF, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(result, "migrate_from_svr_port", migrate_from_svr_port, int64_t);
    EXTRACT_BOOL_FIELD_MYSQL(result, "manual_migrate", unit.is_manual_migrate_);
    EXTRACT_STRBUF_FIELD_MYSQL(result, "status", status, MAX_UNIT_STATUS_LENGTH, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "replica_type", unit.replica_type_, common::ObReplicaType);
    (void)tmp_real_str_len;  // make compiler happy
    unit.server_.set_ip_addr(ip, static_cast<int32_t>(port));
    unit.migrate_from_server_.set_ip_addr(migrate_from_svr_ip, static_cast<int32_t>(migrate_from_svr_port));
    if (0 == STRCMP(status, ObUnit::unit_status_strings[ObUnit::UNIT_STATUS_ACTIVE])) {
      unit.status_ = ObUnit::UNIT_STATUS_ACTIVE;
    } else if (0 == STRCMP(status, ObUnit::unit_status_strings[ObUnit::UNIT_STATUS_DELETING])) {
      unit.status_ = ObUnit::UNIT_STATUS_DELETING;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid unit status", K(ret));
    }
  }
  return ret;
}

int ObUnitTableOperator::read_resource_pool(const ObMySQLResult& result, ObResourcePool& resource_pool) const
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0;  // used to fill output argument
  char zone_list_str[MAX_ZONE_LIST_LENGTH] = "";
  resource_pool.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    EXTRACT_INT_FIELD_MYSQL(result, "resource_pool_id", resource_pool.resource_pool_id_, uint64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(result, "name", resource_pool.name_.ptr(), MAX_RESOURCE_POOL_LENGTH, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(result, "unit_count", resource_pool.unit_count_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "unit_config_id", resource_pool.unit_config_id_, uint64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(result, "zone_list", zone_list_str, MAX_ZONE_LIST_LENGTH, tmp_real_str_len);
    EXTRACT_INT_FIELD_MYSQL(result, "tenant_id", resource_pool.tenant_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "replica_type", resource_pool.replica_type_, common::ObReplicaType);
    (void)tmp_real_str_len;  // make compiler happy
    if (OB_SUCC(ret)) {
      if (OB_FAIL(str2zone_list(zone_list_str, resource_pool.zone_list_))) {
        LOG_WARN("str2zone_list failed", K(zone_list_str), K(ret));
      }
    }
  }
  return ret;
}

int ObUnitTableOperator::read_unit_config(const ObMySQLResult& result, ObUnitConfig& unit_config) const
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0;  // used to fill output argument
  unit_config.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    EXTRACT_INT_FIELD_MYSQL(result, "unit_config_id", unit_config.unit_config_id_, uint64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(result, "name", unit_config.name_.ptr(), MAX_UNIT_CONFIG_LENGTH, tmp_real_str_len);
    (void)tmp_real_str_len;  // make compiler happy
    EXTRACT_DOUBLE_FIELD_MYSQL(result, "max_cpu", unit_config.max_cpu_, double);
    EXTRACT_DOUBLE_FIELD_MYSQL(result, "min_cpu", unit_config.min_cpu_, double);
    EXTRACT_INT_FIELD_MYSQL(result, "max_memory", unit_config.max_memory_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "min_memory", unit_config.min_memory_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "max_iops", unit_config.max_iops_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "min_iops", unit_config.min_iops_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "max_disk_size", unit_config.max_disk_size_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "max_session_num", unit_config.max_session_num_, int64_t);
  }
  return ret;
}

#define READ_ITEMS(Item, item)                                                         \
  do {                                                                                 \
    bool did_use_weak = false;                                                         \
    ObSQLClientRetryWeak sql_client_retry_weak(proxy_, did_use_weak);                  \
    SMART_VAR(ObMySQLProxy::MySQLResult, res)                                          \
    {                                                                                  \
      ObMySQLResult* result = NULL;                                                    \
      if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {                       \
        LOG_WARN("execute sql failed", K(sql), K(ret));                                \
      } else if (NULL == (result = res.get_result())) {                                \
        ret = OB_ERR_UNEXPECTED;                                                       \
        LOG_WARN("result is not expected to be NULL", "result", OB_P(result), K(ret)); \
      } else {                                                                         \
        while (OB_SUCC(ret)) {                                                         \
          Item item;                                                                   \
          if (OB_FAIL(result->next())) {                                               \
            if (OB_ITER_END == ret) {                                                  \
              ret = OB_SUCCESS;                                                        \
              break;                                                                   \
            } else {                                                                   \
              LOG_WARN("get next result failed", K(ret));                              \
            }                                                                          \
          } else if (OB_FAIL(read_##item(*result, item))) {                            \
            LOG_WARN("read_##item failed", K(ret));                                    \
          } else if (OB_FAIL(item##s.push_back(item))) {                               \
            LOG_WARN("push_back failed", K(item), K(ret));                             \
          }                                                                            \
        }                                                                              \
      }                                                                                \
    }                                                                                  \
  } while (false)

int ObUnitTableOperator::read_unit_configs(ObSqlString& sql, ObIArray<ObUnitConfig>& unit_configs) const
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

int ObUnitTableOperator::read_units(ObSqlString& sql, ObIArray<ObUnit>& units) const
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

int ObUnitTableOperator::read_resource_pools(ObSqlString& sql, ObIArray<ObResourcePool>& resource_pools) const
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

int ObUnitTableOperator::read_tenants(ObSqlString& sql, ObIArray<uint64_t>& tenants) const
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

}  // end namespace share
}  // end namespace oceanbase
