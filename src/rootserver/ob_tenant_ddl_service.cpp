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
#include "rootserver/ob_tenant_ddl_service.h"

#include "rootserver/restore/ob_restore_util.h"
#include "rootserver/restore/ob_tenant_clone_util.h"
#include "share/ob_primary_zone_util.h"
#include "share/ls/ob_ls_creator.h"
#include "rootserver/ob_tenant_thread_helper.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_root_service.h"
#include "share/ls/ob_ls_life_manager.h"
#include "share/location_cache/ob_location_service.h"
#include "rootserver/ob_table_creator.h"
#include "rootserver/ob_balance_group_ls_stat_operator.h"
#include "rootserver/ob_disaster_recovery_task_utils.h" // DisasterRecoveryUtils
#include "share/ob_global_stat_proxy.h"
#include "rootserver/standby/ob_standby_service.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/backup/ob_backup_config.h"
#include "share/ob_schema_status_proxy.h"
#include "share/backup/ob_log_restore_config.h"//ObLogRestoreSourceServiceConfigParser
#include "storage/tx/ob_ts_mgr.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif
#ifdef OB_BUILD_ARBITRATION
#include "rootserver/ob_arbitration_service.h"
#include "share/arbitration_service/ob_arbitration_service_utils.h"
#endif
#include "sql/resolver/ob_resolver_utils.h"
#include "rootserver/ob_tenant_parallel_create_executor.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/ob_zone_merge_info.h"
#include "share/ob_global_merge_table_operator.h"
#include "share/ob_zone_merge_table_operator.h"

#define MODIFY_LOCALITY_NOT_ALLOWED() \
        do { \
          ret = OB_OP_NOT_ALLOW; \
          LOG_WARN("modify locality is not allowed", K(ret)); \
        } while (0)
// The input of value must be a string
#define SET_TENANT_VARIABLE(sysvar_id, value) \
        if (OB_SUCC(ret)) {\
          int64_t store_idx = OB_INVALID_INDEX; \
          if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(sysvar_id, store_idx))) { \
            LOG_WARN("failed to calc sys var store idx", KR(ret), K(sysvar_id)); \
          } else if (OB_UNLIKELY(store_idx < 0 \
                     || store_idx >= ObSysVarFactory::ALL_SYS_VARS_COUNT)) { \
            ret = OB_ERR_UNEXPECTED; \
            LOG_WARN("got store_idx is invalid", K(ret), K(store_idx)); \
          } else if (OB_FAIL(sys_params[store_idx].init( \
                     sys_variable_schema.get_tenant_id(),\
                     ObSysVariables::get_name(store_idx),\
                     ObSysVariables::get_type(store_idx),\
                     value,\
                     ObSysVariables::get_min(store_idx),\
                     ObSysVariables::get_max(store_idx),\
                     ObSysVariables::get_info(store_idx),\
                     ObSysVariables::get_flags(store_idx)))) {\
            LOG_WARN("failed to set tenant variable", \
                     KR(ret), K(value), K(sysvar_id), K(store_idx));\
          }\
        }
// Convert macro integer to string for setting into system variable
#define VAR_INT_TO_STRING(buf, value) \
        if (OB_SUCC(ret)) {\
          if (OB_FAIL(databuff_printf(buf, OB_MAX_SYS_PARAM_VALUE_LENGTH, "%d", static_cast<int>(value)))) {\
            LOG_WARN("failed to print value in buf", K(value), K(ret));\
          }\
        }
#define VAR_UINT_TO_STRING(buf, value) \
        if (OB_SUCC(ret)) {\
          if (OB_FAIL(databuff_printf(buf, OB_MAX_SYS_PARAM_VALUE_LENGTH, "%lu", static_cast<uint64_t>(value)))) {\
            LOG_WARN("failed to print value in buf", K(value), K(ret));\
          }\
        }

namespace oceanbase
{
using namespace obrpc;
using namespace share;
namespace rootserver
{

int ObTenantDDLService::check_inner_stat()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantDDLService is not inited", KR(ret), K(inited_));
  } else if (OB_ISNULL(ddl_service_) || OB_ISNULL(rpc_proxy_) || OB_ISNULL(common_rpc_)
      || OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service_) || OB_ISNULL(lst_operator_)
      || OB_ISNULL(ddl_trans_controller_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", KR(ret), KP(ddl_service_), KP(rpc_proxy_), KP(sql_proxy_),
        KP(schema_service_), KP(lst_operator_), KP(ddl_trans_controller_));
  }
  return ret;
}

#define USE_DDL_FUNCTION(function_name, ...) \
  int ret = OB_SUCCESS; \
  if (OB_ISNULL(ddl_service_)) { \
    ret = OB_NOT_INIT; \
    LOG_WARN("ddl_service_ is null", KR(ret), KP(ddl_service_)); \
  } else if (OB_FAIL(ddl_service_->function_name(__VA_ARGS__))) { \
    LOG_WARN("failed to call " #function_name , KR(ret)); \
  } \
  return ret;

int ObTenantDDLService::get_tenant_schema_guard_with_version_in_inner_table(
    const uint64_t tenant_id,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  USE_DDL_FUNCTION(get_tenant_schema_guard_with_version_in_inner_table, tenant_id, schema_guard);
}

int ObTenantDDLService::publish_schema(const uint64_t tenant_id)
{
  USE_DDL_FUNCTION(publish_schema, tenant_id);
}

int ObTenantDDLService::publish_schema(const uint64_t tenant_id, const common::ObAddrIArray &addrs)
{
  USE_DDL_FUNCTION(publish_schema, tenant_id, addrs);
}

#undef USE_DDL_FUNCTION

int ObTenantDDLService::schedule_create_tenant(const obrpc::ObCreateTenantArg &arg, obrpc::UInt64 &tenant_id)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObParallelCreateTenantExecutor, executor) {
    if (OB_FAIL(executor.init(arg, GCTX.srv_rpc_proxy_, GCTX.rs_rpc_proxy_, GCTX.sql_proxy_,
            GCTX.schema_service_, GCTX.lst_operator_, GCTX.location_service_))) {
      LOG_WARN("failed to init executor", KR(ret), K(arg));
    } else if (OB_FAIL(executor.execute(tenant_id))) {
      LOG_WARN("failed to execute", KR(ret), K(executor));
    }
  }
  return ret;
}

int ObTenantDDLService::init_tenant_configs_(
    const uint64_t tenant_id,
    const common::ObIArray<common::ObConfigPairs> &init_configs,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t tenant_idx = !is_user_tenant(tenant_id) ? 0 : 1;
  if (OB_UNLIKELY(
      init_configs.count() < tenant_idx + 1
      || tenant_id != init_configs.at(tenant_idx).get_tenant_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid init_configs", KR(ret), K(tenant_idx), K(tenant_id), K(init_configs));
  } else if (OB_FAIL(init_tenant_config_(tenant_id, init_configs.at(tenant_idx), trans))) {
    LOG_WARN("fail to init tenant config", KR(ret), K(tenant_id));
  } else if (OB_FAIL(init_tenant_config_from_seed_(tenant_id, trans))) {
    LOG_WARN("fail to init tenant config from seed", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTenantDDLService::init_tenant_config_(
    const uint64_t tenant_id,
    const common::ObConfigPairs &tenant_config,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard hard_code_config(TENANT_CONF(OB_SYS_TENANT_ID));
  int64_t config_cnt = tenant_config.get_configs().count();
  if (OB_UNLIKELY(tenant_id != tenant_config.get_tenant_id() || config_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant config", KR(ret), K(tenant_id), K(tenant_config));
  } else if (!hard_code_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get hard code config", KR(ret), K(tenant_id));
  } else {
    ObDMLSqlSplicer dml;
    ObConfigItem *item = NULL;
    char svr_ip[OB_MAX_SERVER_ADDR_SIZE] = "ANY";
    int64_t svr_port = 0;
    int64_t config_version = omt::ObTenantConfig::INITIAL_TENANT_CONF_VERSION + 1;
    FOREACH_X(config, tenant_config.get_configs(), OB_SUCC(ret)) {
      const ObConfigStringKey key(config->key_.ptr());
      if (OB_ISNULL(hard_code_config->get_container().get(key))
          || OB_ISNULL(item = *(hard_code_config->get_container().get(key)))) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("config not exist", KR(ret), KPC(config));
      } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))
                 || OB_FAIL(dml.add_pk_column("zone", ""))
                 || OB_FAIL(dml.add_pk_column("svr_type", print_server_role(OB_SERVER)))
                 || OB_FAIL(dml.add_pk_column(K(svr_ip)))
                 || OB_FAIL(dml.add_pk_column(K(svr_port)))
                 || OB_FAIL(dml.add_pk_column("name", config->key_.ptr()))
                 || OB_FAIL(dml.add_column("data_type", item->data_type()))
                 || OB_FAIL(dml.add_column("value", config->value_.ptr()))
                 || OB_FAIL(dml.add_column("info", ""))
                 || OB_FAIL(dml.add_column("config_version", config_version))
                 || OB_FAIL(dml.add_column("section", item->section()))
                 || OB_FAIL(dml.add_column("scope", item->scope()))
                 || OB_FAIL(dml.add_column("source", item->source()))
                 || OB_FAIL(dml.add_column("edit_level", item->edit_level()))) {
        LOG_WARN("fail to add column", KR(ret), K(tenant_id), KPC(config));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("fail to finish row", KR(ret), K(tenant_id), KPC(config));
      }
    } // end foreach
    ObSqlString sql;
    int64_t affected_rows = 0;
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
    if (FAILEDx(dml.splice_batch_insert_sql(OB_TENANT_PARAMETER_TNAME, sql))) {
      LOG_WARN("fail to generate sql", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(exec_tenant_id), K(sql));
    } else if (config_cnt != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows not match", KR(ret), K(tenant_id), K(config_cnt), K(affected_rows));
    }
  }
  return ret;
}

int ObTenantDDLService::init_tenant_config_from_seed_(
    const uint64_t tenant_id,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  ObSqlString sql;
  const static char *from_seed = "select config_version, zone, svr_type, svr_ip, svr_port, name, "
                "data_type, value, info, section, scope, source, edit_level "
                "from __all_seed_parameter";
  ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_);
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    int64_t expected_rows = 0;
    int64_t config_version = omt::ObTenantConfig::INITIAL_TENANT_CONF_VERSION + 1;
    bool is_first = true;
    if (OB_FAIL(sql_client_retry_weak.read(result, OB_SYS_TENANT_ID, from_seed))) {
      LOG_WARN("read config from __all_seed_parameter failed", K(from_seed), K(ret));
    } else {
      sql.reset();
      if (OB_FAIL(sql.assign_fmt("INSERT IGNORE INTO %s "
          "(TENANT_ID, ZONE, SVR_TYPE, SVR_IP, SVR_PORT, NAME, DATA_TYPE, VALUE, INFO, "
          "SECTION, SCOPE, SOURCE, EDIT_LEVEL, CONFIG_VERSION) VALUES",
          OB_TENANT_PARAMETER_TNAME))) {
        LOG_WARN("sql assign failed", K(ret));
      }

      while (OB_SUCC(ret) && OB_SUCC(result.get_result()->next())) {
        common::sqlclient::ObMySQLResult *rs = result.get_result();
        if (OB_ISNULL(rs)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("system config result is null", K(ret));
        } else {
          ObString var_zone, var_svr_type, var_svr_ip, var_name, var_data_type;
          ObString var_value, var_info, var_section, var_scope, var_source, var_edit_level;
          int64_t var_svr_port = 0;
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "zone", var_zone);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "svr_type", var_svr_type);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "svr_ip", var_svr_ip);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "name", var_name);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "data_type", var_data_type);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "value", var_value);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "info", var_info);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "section", var_section);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "scope", var_scope);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "source", var_source);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "edit_level", var_edit_level);
          EXTRACT_INT_FIELD_MYSQL(*rs, "svr_port", var_svr_port, int64_t);
          if (FAILEDx(sql.append_fmt("%s('%lu', '%.*s', '%.*s', '%.*s', %ld, '%.*s', '%.*s', '%.*s',"
              "'%.*s', '%.*s', '%.*s', '%.*s', '%.*s', %ld)",
              is_first ? " " : ", ",
              tenant_id,
              var_zone.length(), var_zone.ptr(),
              var_svr_type.length(), var_svr_type.ptr(),
              var_svr_ip.length(), var_svr_ip.ptr(), var_svr_port,
              var_name.length(), var_name.ptr(),
              var_data_type.length(), var_data_type.ptr(),
              var_value.length(), var_value.ptr(),
              var_info.length(), var_info.ptr(),
              var_section.length(), var_section.ptr(),
              var_scope.length(), var_scope.ptr(),
              var_source.length(), var_source.ptr(),
              var_edit_level.length(), var_edit_level.ptr(), config_version))) {
            LOG_WARN("sql append failed", K(ret));
          }
        }
        expected_rows++;
        is_first = false;
      } // while

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
        if (expected_rows > 0) {
          int64_t affected_rows = 0;
          if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
            LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(exec_tenant_id), K(sql));
          } else if (OB_UNLIKELY(affected_rows < 0
                     || expected_rows < affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected affected_rows", KR(ret),  K(expected_rows), K(affected_rows));
          }
        }
      } else {
        LOG_WARN("failed to get result from result set", K(ret));
      }
    } // else
    LOG_INFO("init tenant config", K(ret), K(tenant_id),
               "cost", ObTimeUtility::current_time() - start);
  }
  return ret;

}

int ObTenantDDLService::set_sys_ls_(const uint64_t tenant_id, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not user tenant", KR(ret), K(tenant_id));
  } else {
    share::ObLSAttr new_ls;
    share::ObLSFlag flag(share::ObLSFlag::NORMAL_FLAG);
    uint64_t ls_group_id = 0;
    SCN create_scn = SCN::base_scn();
    share::ObLSAttrOperator ls_operator(tenant_id, sql_proxy_);
    if (OB_FAIL(new_ls.init(SYS_LS, ls_group_id, flag,
            share::OB_LS_NORMAL, share::OB_LS_OP_CREATE_END, create_scn))) {
      LOG_WARN("failed to init new operation", KR(ret), K(flag), K(create_scn));
    } else if (OB_FAIL(ls_operator.insert_ls(new_ls, share::NORMAL_SWITCHOVER_STATUS, &trans))) {
      LOG_WARN("failed to insert new ls", KR(ret), K(new_ls), K(ls_group_id));
    }
  }
  return ret;
}

int ObTenantDDLService::init_user_tenant_env_(const uint64_t tenant_id, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is not user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(set_sys_ls_(tenant_id, trans))) {
    LOG_WARN("failed to set sys ls", KR(ret), K(tenant_id));
  }
  return ret;
}

// 1. run ls_life_agent
//   a. __all_ls_status
//   b. __all_ls_election_reference_info
//   c. __all_ls_recovery_stat
int ObTenantDDLService::fill_user_sys_ls_info_(
    const ObTenantSchema &meta_tenant_schema,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t meta_tenant_id = meta_tenant_schema.get_tenant_id();
  const uint64_t user_tenant_id = gen_user_tenant_id(meta_tenant_id);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("failed to check_inner_stat", KR(ret));
  } else if (!is_meta_tenant(meta_tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant id is not meta", KR(ret), K(meta_tenant_schema));
  } else {
    share::ObLSLifeAgentManager ls_life_agent(*sql_proxy_);
    share::ObLSStatusOperator ls_operator;
    const SCN create_scn = SCN::base_scn();
    share::ObLSStatusInfo status_info;
    ObLSFlag flag(ObLSFlag::NORMAL_FLAG); // TODO: sys ls should be duplicate
    ObZone primary_zone;
    ObSqlString zone_priority;
    if (OB_FAIL(get_tenant_zone_priority(meta_tenant_schema, primary_zone,
            zone_priority))) {
      LOG_WARN("failed to get tenant zone priority", KR(ret), K(primary_zone), K(zone_priority));
    } else if (OB_FAIL(status_info.init(user_tenant_id, SYS_LS, 0/*ls_group_id*/, share::OB_LS_CREATING,
            0/*unit_group_id*/, primary_zone, flag))) {
      LOG_WARN("failed to init ls info", KR(ret), K(primary_zone),
          K(user_tenant_id), K(flag));
    } else if (OB_FAIL(ls_life_agent.create_new_ls_in_trans(status_info, create_scn, zone_priority.string(),
            share::NORMAL_SWITCHOVER_STATUS, trans))) {
      LOG_WARN("failed to create new ls", KR(ret), K(status_info), K(create_scn), K(zone_priority));
    } else if (OB_FAIL(ls_operator.update_ls_status_in_trans(
                  user_tenant_id, SYS_LS, share::OB_LS_CREATING, share::OB_LS_NORMAL,
                  share::NORMAL_SWITCHOVER_STATUS, trans))) {
      LOG_WARN("failed to update ls status", KR(ret));
    }
  }
  return ret;
}

int ObTenantDDLService::get_tenant_zone_priority(const ObTenantSchema &tenant_schema,
    ObZone &primary_zone,
    ObSqlString &zone_priority)
{
  int ret = OB_SUCCESS;
  ObArray<ObZone> primary_zone_list;
  if (OB_FAIL(ObPrimaryZoneUtil::get_tenant_primary_zone_array(tenant_schema, primary_zone_list))) {
    LOG_WARN("failed to get tenant primary zone array", KR(ret));
  } else if (OB_UNLIKELY(0 == primary_zone_list.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("primary zone is empty", KR(ret), K(tenant_schema));
  } else if (FALSE_IT(primary_zone = primary_zone_list.at(0))) {
  } else if (OB_FAIL(ObTenantThreadHelper::get_zone_priority(
          primary_zone, tenant_schema, zone_priority))) {
    LOG_WARN("failed to get zone priority", KR(ret), K(primary_zone_list), K(tenant_schema));
  }
  return ret;
}

int ObTenantDDLService::init_tenant_sys_stats_(const uint64_t tenant_id,
                                         ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  ObSysStat sys_stat;
  if (OB_FAIL(sys_stat.set_initial_values(tenant_id))) {
    LOG_WARN("set initial values failed", K(ret));
  } else if (sys_stat.item_list_.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not system stat item", KR(ret), K(tenant_id));
  } else if (OB_FAIL(replace_sys_stat(tenant_id, sys_stat, trans))) {
    LOG_WARN("replace system stat failed", K(ret));
  }
  LOG_INFO("init sys stat", K(ret), K(tenant_id),
           "cost", ObTimeUtility::current_time() - start);
  return ret;
}

int ObTenantDDLService::replace_sys_stat(const uint64_t tenant_id,
                                    ObSysStat &sys_stat,
                                    ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (sys_stat.item_list_.is_empty()) {
    // skip
  } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s "
      "(TENANT_ID, ZONE, NAME, DATA_TYPE, VALUE, INFO, gmt_modified) VALUES ",
      OB_ALL_SYS_STAT_TNAME))) {
    LOG_WARN("sql append failed", K(ret));
  } else {
    DLIST_FOREACH_X(it, sys_stat.item_list_, OB_SUCC(ret)) {
      if (OB_ISNULL(it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("it is null", K(ret));
      } else {
        char buf[2L<<10] = "";
        int64_t pos = 0;
        if (OB_FAIL(it->value_.print_sql_literal(
                      buf, sizeof(buf), pos))) {
          LOG_WARN("print obj failed", K(ret), "obj", it->value_);
        } else {
          ObString value(pos, buf);
          uint64_t schema_id = OB_INVALID_ID;
          if (OB_FAIL(ObMaxIdFetcher::str_to_uint(value, schema_id))) {
            LOG_WARN("fail to convert str to uint", K(ret), K(value));
          } else if (FALSE_IT(schema_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, schema_id))) {
          } else if (OB_FAIL(sql.append_fmt("%s(%lu, '', '%s', %d, '%ld', '%s', now())",
              (it == sys_stat.item_list_.get_first()) ? "" : ", ",
              ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
              it->name_, it->value_.get_type(),
              static_cast<int64_t>(schema_id),
              it->info_))) {
            LOG_WARN("sql append failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("create system stat sql", K(sql));
      int64_t affected_rows = 0;
      if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(sql));
      } else if (sys_stat.item_list_.get_size() != affected_rows
          && sys_stat.item_list_.get_size() != affected_rows / 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected_rows", K(affected_rows),
            "expected", sys_stat.item_list_.get_size());
      }
    }
  }
  return ret;
}

int ObTenantDDLService::init_meta_tenant_env_(
    const ObTenantSchema &tenant_schema,
    const obrpc::ObCreateTenantArg &create_tenant_arg,
    const common::ObIArray<common::ObConfigPairs> &init_configs,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  const ObTenantRole &tenant_role = create_tenant_arg.get_tenant_role();
  const SCN recovery_until_scn = (create_tenant_arg.is_restore_tenant() || create_tenant_arg.is_clone_tenant())
    ? create_tenant_arg.recovery_until_scn_ : SCN::max_scn();
  if (!is_meta_tenant(tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is not user tenant", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!recovery_until_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid recovery_until_scn", KR(ret), K(recovery_until_scn));
  } else {
    const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
    ObAllTenantInfo tenant_info;
    if (OB_FAIL(tenant_info.init(user_tenant_id, tenant_role, NORMAL_SWITCHOVER_STATUS, 0,
            SCN::base_scn(), SCN::base_scn(), SCN::base_scn(), recovery_until_scn))) {
      LOG_WARN("failed to init tenant info", KR(ret), K(tenant_id), K(tenant_role));
    } else if (OB_FAIL(ObAllTenantInfoProxy::init_tenant_info(tenant_info, &trans))) {
      LOG_WARN("failed to init tenant info", KR(ret), K(tenant_info));
    } else if (OB_FAIL(fill_user_sys_ls_info_(tenant_schema, trans))) {
      LOG_WARN("failed to fill user sys ls info", KR(ret), K(tenant_schema));
    } else if (OB_FAIL(init_tenant_configs_(tenant_id, init_configs, trans))) {
      LOG_WARN("failed to init tenant config", KR(ret), K(tenant_id), K(init_configs));
    } else if (OB_FAIL(init_tenant_configs_(user_tenant_id, init_configs, trans))) {
      LOG_WARN("failed to init tenant config for user tenant", KR(ret), K(user_tenant_id), K(init_configs));
    }
  }
  return ret;
}

int ObTenantDDLService::create_sys_tenant(
    const obrpc::ObCreateTenantArg &arg,
    share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  ObDDLSQLTransaction trans(schema_service_, true, false, false, false);
  ObSchemaService *schema_service = NULL;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init");
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    schema_service = schema_service_->get_schema_service();
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("schema_service must not null", K(ret));
    } else {
      ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
      ObRefreshSchemaStatus tenant_status(OB_SYS_TENANT_ID,
          OB_INVALID_TIMESTAMP, OB_INVALID_VERSION);
      ObSysVariableSchema sys_variable;
      tenant_schema.set_tenant_id(OB_SYS_TENANT_ID);
      const ObSchemaOperationType operation_type = OB_DDL_MAX_OP;
      // When the system tenant is created, the log_operation of the system variable is not recorded separately
      // The update of __all_core_table must be a single-partition transaction.
      // Failure to create a tenant will result in garbage data, but it will not affect
      int64_t refreshed_schema_version = 0; // won't lock
      common::ObConfigPairs config;
      common::ObSEArray<common::ObConfigPairs, 1> init_configs;
      if (OB_ISNULL(schema_status_proxy)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_status_proxy is null", K(ret));
      } else if (OB_FAIL(generate_tenant_init_configs(arg, OB_SYS_TENANT_ID, init_configs))) {
        LOG_WARN("failed to gen tenant init config", KR(ret));
      } else if (OB_FAIL(schema_status_proxy->set_tenant_schema_status(tenant_status))) {
        LOG_WARN("init tenant create partition status failed", K(ret), K(tenant_status));
      } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
        LOG_WARN("start transaction failed", KR(ret));
      } else if (OB_FAIL(set_tenant_compatibility_(arg, tenant_schema))) {
        LOG_WARN("failed to set tenant compatibility", KR(ret), K(arg));
      } else if (OB_FAIL(ddl_operator.create_tenant(tenant_schema, OB_DDL_ADD_TENANT, trans))) {
        LOG_WARN("create tenant failed", K(tenant_schema), K(ret));
      } else if (OB_FAIL(init_system_variables(arg, tenant_schema, sys_variable))) {
        LOG_WARN("fail to init tenant sys params", K(ret), K(tenant_schema));
      } else if (OB_FAIL(ddl_operator.replace_sys_variable(
              sys_variable, tenant_schema.get_schema_version(), trans, operation_type))) {
        LOG_WARN("fail to replace sys variable", K(ret), K(sys_variable));
      } else if (OB_FAIL(ddl_operator.init_tenant_schemas(tenant_schema, sys_variable, trans))) {
        LOG_WARN("init tenant env failed", K(tenant_schema), K(ret));
      } else if (OB_FAIL(init_tenant_sys_stats_(OB_SYS_TENANT_ID, trans))) {
        LOG_WARN("insert default sys stats failed", K(OB_SYS_TENANT_ID), K(ret));
      } else if (OB_FAIL(init_tenant_configs_(OB_SYS_TENANT_ID, init_configs, trans))) {
        LOG_WARN("failed to init tenant config", KR(ret), K(init_configs));
      } else if (OB_FAIL(insert_tenant_merge_info_(OB_DDL_ADD_TENANT, tenant_schema, trans))) {
        LOG_WARN("fail to insert tenant merge info", KR(ret));
      } else if (OB_FAIL(ObServiceEpochProxy::init_service_epoch(
          trans,
          OB_SYS_TENANT_ID,
          0, /*freeze_service_epoch*/
          0, /*arbitration_service_epoch*/
          0, /*server_zone_op_service_epoch*/
          0, /*heartbeat_service_epoch*/
          0 /* service_name_epoch */))) {
        LOG_WARN("fail to init service epoch", KR(ret));
      }
      if (trans.is_started()) {
        int temp_ret = OB_SUCCESS;
        LOG_INFO("end create tenant", "is_commit", OB_SUCCESS == ret, K(ret));
        if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
          ret = (OB_SUCC(ret)) ? temp_ret : ret;
          LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(broadcast_tenant_init_config_(OB_SYS_TENANT_ID))) {
        // If tenant config version in RS is valid first and ddl trans doesn't commit,
        // observer may read from empty __tenant_parameter successfully and raise its tenant config version,
        // which makes some initial tenant configs are not actually updated before related observer restarts.
        // To fix this problem, tenant config version in RS should be valid after ddl trans commits.
        LOG_WARN("failed to set tenant config version", KR(ret), "tenant_id", OB_SYS_TENANT_ID);
      }
    }
  }
  return ret;
}

int ObTenantDDLService::set_tenant_compatibility_(
    const obrpc::ObCreateTenantArg &arg,
    ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  const int64_t set_sys_var_count = arg.sys_var_list_.count();
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  // the default compatibility_mode is MYSQL
  tenant_schema.set_compatibility_mode(ObCompatibilityMode::MYSQL_MODE);
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(arg));
  } else if (!is_user_tenant(tenant_id)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < set_sys_var_count; ++i) {
      ObSysVarIdValue sys_var;
      if (OB_FAIL(arg.sys_var_list_.at(i, sys_var))) {
        LOG_WARN("failed to get sys var", K(i), K(ret));
      } else {
        if (SYS_VAR_OB_COMPATIBILITY_MODE == sys_var.sys_id_) {
          if (0 == sys_var.value_.compare("1")) {
            tenant_schema.set_compatibility_mode(ObCompatibilityMode::ORACLE_MODE);
          } else {
            tenant_schema.set_compatibility_mode(ObCompatibilityMode::MYSQL_MODE);
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantDDLService::generate_tenant_schema(
    const ObCreateTenantArg &arg,
    const share::ObTenantRole &tenant_role,
    share::schema::ObSchemaGetterGuard &schema_guard,
    ObTenantSchema &user_tenant_schema,
    ObTenantSchema &meta_tenant_schema,
    common::ObIArray<common::ObConfigPairs> &init_configs)
{
  int ret = OB_SUCCESS;
  uint64_t user_tenant_id = arg.tenant_schema_.get_tenant_id();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_)
      || OB_ISNULL(schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(schema_service));
  } else if (OB_FAIL(user_tenant_schema.assign(arg.tenant_schema_))) {
    LOG_WARN("fail to assign user tenant schema", KR(ret), K(arg));
  } else if (OB_FAIL(meta_tenant_schema.assign(user_tenant_schema))) {
    LOG_WARN("fail to assign meta tenant schema", KR(ret), K(arg));
  } else if (OB_FAIL(check_create_tenant_schema(
          arg.pool_list_, meta_tenant_schema, schema_guard))) {
    LOG_WARN("check tenant schema failed", KR(ret), K(meta_tenant_schema), K(arg));
  } else if (OB_FAIL(check_create_tenant_schema(
          arg.pool_list_, user_tenant_schema, schema_guard))) {
    LOG_WARN("check tenant schema failed", KR(ret), K(user_tenant_schema), K(arg));
  } else if (OB_FAIL(schema_service_->get_schema_service()->fetch_new_tenant_id(user_tenant_id))) {
    LOG_WARN("fetch_new_tenant_id failed", KR(ret));
  } else if (OB_INVALID_ID == user_tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant id is invalid", KR(ret), K(user_tenant_id));
  } else {
    // user tenant
    if (OB_SUCC(ret)) {
      user_tenant_schema.set_tenant_id(user_tenant_id);
      if (!tenant_role.is_primary()) {
        //standby cluster and restore tenant no need init user tenant system variables
        if (tenant_role.is_restore() || tenant_role.is_clone()) {
          user_tenant_schema.set_status(TENANT_STATUS_RESTORE);
        } else if (arg.is_creating_standby_) {
          user_tenant_schema.set_status(TENANT_STATUS_CREATING_STANDBY);
        }
      } else if (OB_FAIL(set_tenant_compatibility_(arg, user_tenant_schema))) {
        LOG_WARN("fail to set tenant compatibility", KR(ret), K(user_tenant_schema), K(arg));
      }
    }
    // meta tenant
    if (OB_SUCC(ret)) {
      const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
      ObSqlString table_name;
      if (OB_FAIL(table_name.assign_fmt("META$%ld", user_tenant_id))) {
        LOG_WARN("fail to assign tenant name",KR(ret), K(user_tenant_id));
      } else {
        meta_tenant_schema.set_tenant_id(meta_tenant_id);
        meta_tenant_schema.set_tenant_name(table_name.string());
        meta_tenant_schema.set_compatibility_mode(ObCompatibilityMode::MYSQL_MODE);
        if (OB_FAIL(check_tenant_primary_zone_(schema_guard, meta_tenant_schema))) {
          LOG_WARN("fail to check tenant primary zone", KR(ret), K(meta_tenant_schema));
        }
      }
    }
    // init tenant configs
    if (OB_SUCC(ret)) {
      if (OB_FAIL(generate_tenant_init_configs(arg, user_tenant_id, init_configs))) {
        LOG_WARN("failed to generate tenant init configs", KR(ret), K(arg), K(user_tenant_id));
      }
    }
  }
  return ret;
}

int ObTenantDDLService::generate_tenant_init_configs(const obrpc::ObCreateTenantArg &arg,
      const uint64_t user_tenant_id,
      common::ObIArray<common::ObConfigPairs> &init_configs)
{
  int ret = OB_SUCCESS;
  init_configs.reset();
  common::ObConfigPairs config;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
  if (OB_FAIL(gen_tenant_init_config(meta_tenant_id, DATA_CURRENT_VERSION, config))) {
    LOG_WARN("fail to gen tenant init config", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(init_configs.push_back(config))) {
    LOG_WARN("fail to push back config", KR(ret), K(meta_tenant_id), K(config));
    // } else if (!is_sys_tenant(user_tenant_id) && !arg.is_creating_standby_) {
    //
    // FIXME(msy164651) : Data Version scheme is not suitable for Create
    // Standby Tenant. The DDL will fail because GET_MIN_DATA_VERSION will
    // return OB_ENTRY_NOT_EXIST;
    //
    // msy164651 wil fix it.
  } else if (!is_sys_tenant(user_tenant_id)) {
    /**
     * When the primary tenant has done upgrade and create a standby tenant for it,
     * the standby tenant must also perform the upgrade process. Don't set compatible_version for
     * standby tenant so that it can be upgraded from 0 to ensure that the compatible_version matches
     * the internal table. and it also prevent loss of the upgrade action.
     */
    uint64_t compatible_version = (arg.is_restore_tenant() || arg.is_clone_tenant())
      ? arg.compatible_version_
      : DATA_CURRENT_VERSION;
    if (OB_FAIL(gen_tenant_init_config(user_tenant_id, compatible_version, config))) {
      LOG_WARN("fail to gen tenant init config", KR(ret), K(user_tenant_id), K(compatible_version));
    } else if (OB_FAIL(init_configs.push_back(config))) {
      LOG_WARN("fail to push back config", KR(ret), K(user_tenant_id), K(config));
    }
  }

return ret;
}

int ObTenantDDLService::gen_tenant_init_config(
    const uint64_t tenant_id,
    const uint64_t compatible_version,
    common::ObConfigPairs &tenant_config)
{
  int ret = OB_SUCCESS;
  ObString config_name("compatible");
  ObString config_value;
  char version[common::OB_CLUSTER_VERSION_LENGTH] = {'\0'};
  int64_t len = ObClusterVersion::print_version_str(
                version, common::OB_CLUSTER_VERSION_LENGTH, compatible_version);
  tenant_config.reset();
  (void) tenant_config.init(tenant_id);
  if (len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid version", KR(ret), K(tenant_id), K(compatible_version));
  } else if (FALSE_IT(config_value.assign_ptr(version, len))) {
  } else if (OB_FAIL(tenant_config.add_config(config_name, config_value))) {
    LOG_WARN("fail to add config", KR(ret), K(config_name), K(config_value));
  }
  return ret;
}

int ObTenantDDLService::init_schema_status(
    const uint64_t tenant_id,
    const share::ObTenantRole &tenant_role)
{
  int ret = OB_SUCCESS;
  ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (is_meta_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_status_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to init schema status", KR(ret), K(tenant_id));
  } else {
    // user tenant
    if (OB_SUCC(ret) && is_user_tenant(tenant_id)) {
    ObRefreshSchemaStatus partition_status(tenant_id,
        OB_INVALID_TIMESTAMP, OB_INVALID_VERSION);
      if (!tenant_role.is_primary()) {
        // reset tenant's schema status in standby cluster or in physical restore
        partition_status.snapshot_timestamp_ = 0;
        partition_status.readable_schema_version_ = 0;
      }
      if (FAILEDx(schema_status_proxy->set_tenant_schema_status(partition_status))) {
        LOG_WARN("fail to set refreshed schema status", KR(ret), K(partition_status));
      }
    }
    // sys or meta tenant
    if (OB_SUCC(ret)) {
      // sys tenant's meta tenant is itself
      const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
      ObRefreshSchemaStatus meta_partition_status(meta_tenant_id, OB_INVALID_TIMESTAMP, OB_INVALID_VERSION);
      if (OB_FAIL(schema_status_proxy->set_tenant_schema_status(meta_partition_status))) {
        LOG_WARN("fail to set refreshed schema status", KR(ret), K(meta_partition_status));
      }
    }
  }
  return ret;
}

int ObTenantDDLService::create_tenant(const ObCreateTenantArg &arg,
    ObCreateTenantSchemaResult &result)
{
  LOG_INFO("begin to create tenant schema", K(arg));
  int ret = OB_SUCCESS;
  bool need_create = false;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("failed to check_inner_stat", KR(ret));
  } else if (OB_FAIL(create_tenant_check_(arg, need_create, schema_guard))) {
    LOG_WARN("failed to check create tenant", KR(ret), K(arg));
  } else if (!need_create) {
    LOG_INFO("no need to create tenant", KR(ret), K(need_create), K(arg));
    if (OB_FAIL(result.init_with_tenant_exist())) {
      LOG_WARN("failed to init result when tenant exist", KR(ret));
    }
  } else {
    HEAP_VARS_2((ObTenantSchema, user_tenant_schema),
                (ObTenantSchema, meta_tenant_schema)) {
      uint64_t user_tenant_id = OB_INVALID_TENANT_ID;
      int64_t paxos_replica_number = 0;
      ObSEArray<ObConfigPairs, 2> init_configs;
      ObTenantRole tenant_role = arg.get_tenant_role();
      if (OB_FAIL(generate_tenant_schema(arg, tenant_role, schema_guard,
              user_tenant_schema, meta_tenant_schema, init_configs))) {
        LOG_WARN("fail to generate tenant schema", KR(ret), K(arg), K(tenant_role));
      } else if (user_tenant_schema.get_arbitration_service_status().is_enable_like()
                 && OB_FAIL(user_tenant_schema.get_paxos_replica_num(schema_guard, paxos_replica_number))) {
        LOG_WARN("fail to get paxos replica number", KR(ret), K(user_tenant_schema));
      } else if (user_tenant_schema.get_arbitration_service_status().is_enable_like()
                 && paxos_replica_number != 2 && paxos_replica_number != 4) {
        ret = OB_OP_NOT_ALLOW;
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The number of paxos replicas in locality is neither 2 nor 4, create tenant with arbitration service");
        LOG_WARN("can not create tenant, because tenant with arb service, locality must be 2F or 4F", KR(ret), K(user_tenant_schema), K(paxos_replica_number));
      } else if (FALSE_IT(user_tenant_id = user_tenant_schema.get_tenant_id())) {
      } else if (OB_FAIL(init_schema_status(
              user_tenant_schema.get_tenant_id(), tenant_role))) {
        LOG_WARN("fail to init schema status", KR(ret), K(user_tenant_id));
      } else if (OB_FAIL(create_tenant_schema(
                 arg, schema_guard, user_tenant_schema,
                 meta_tenant_schema, init_configs))) {
        LOG_WARN("fail to create tenant schema", KR(ret), K(arg));
      } else if (OB_FAIL(result.init(user_tenant_id))) {
        LOG_WARN("failed to init result", KR(ret), K(user_tenant_id));
      }
      DEBUG_SYNC(AFTER_CREATE_TENANT_SCHEMA);
    } // end HEAP_VARS_4
  }
  return ret;
}

int ObTenantDDLService::generate_drop_tenant_arg(
    const uint64_t tenant_id,
    const ObString &tenant_name,
    ObSqlString &ddl_stmt,
    ObDropTenantArg &arg)
{
  int ret = OB_SUCCESS;
  arg.tenant_name_ = tenant_name;
  arg.tenant_id_ = tenant_id;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  arg.if_exist_ = true;
  arg.delay_to_drop_ = false;
  if (OB_FAIL(ddl_stmt.append_fmt("DROP TENANT IF EXISTS %s FORCE", tenant_name.ptr()))) {
    LOG_WARN("fail to generate sql", KR(ret), K(tenant_id));
  }
  arg.ddl_stmt_str_ = ddl_stmt.string();
  return ret;
}

int ObTenantDDLService::try_force_drop_tenant(const ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    obrpc::ObDropTenantArg arg;
    const uint64_t tenant_id = tenant_schema.get_tenant_id();
    const ObString tenant_name = tenant_schema.get_tenant_name();
    ObSqlString ddl_stmt;
    if (OB_FAIL(generate_drop_tenant_arg(tenant_id, tenant_name, ddl_stmt, arg))) {
      LOG_WARN("failed to generate drop tenant arg", KR(ret), K(tenant_id), K(tenant_name));
    } else if (OB_FAIL(drop_tenant(arg))) {
      LOG_WARN("fail to drop tenant", KR(ret), K(arg));
    }
  }
  return ret;
}


// 1. create new tenant schema
// 2. grant resource pool to new tenant
int ObTenantDDLService::create_tenant_schema(
    const ObCreateTenantArg &arg,
    share::schema::ObSchemaGetterGuard &schema_guard,
    ObTenantSchema &user_tenant_schema,
    ObTenantSchema &meta_tenant_schema,
    const common::ObIArray<common::ObConfigPairs> &init_configs)
{
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("[CREATE_TENANT] STEP 1. start create tenant schema", K(arg));
  int ret = OB_SUCCESS;
  const uint64_t user_tenant_id = user_tenant_schema.get_tenant_id();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_)
             || OB_ISNULL(sql_proxy_)
             || OB_ISNULL(unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(schema_service), KP_(sql_proxy),
             KP_(unit_mgr));
  } else {
    ObDDLSQLTransaction trans(schema_service_);
    int64_t refreshed_schema_version = OB_INVALID_VERSION;
    ObArray<ObResourcePoolName> pools;
    common::ObArray<uint64_t> new_ug_id_array;
    if (OB_FAIL(get_pools(arg.pool_list_, pools))) {
      LOG_WARN("get_pools failed", KR(ret), K(arg));
    } else if (OB_FAIL(schema_guard.get_schema_version(
               OB_SYS_TENANT_ID, refreshed_schema_version))) {
      LOG_WARN("fail to get schema version", KR(ret), K(refreshed_schema_version));
    } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
      LOG_WARN("start transaction failed, ", KR(ret), K(refreshed_schema_version));
    }
#ifdef OB_BUILD_ARBITRATION
    // check arbitration service if needed
    ObArbitrationServiceTableOperator arbitration_service_table_operator;
    const ObString arbitration_service_key("default");
    const bool lock_line = true;
    ObArbitrationServiceInfo arbitration_service_info;
    if (OB_FAIL(ret)) {
    } else if (meta_tenant_schema.get_arbitration_service_status()
               != user_tenant_schema.get_arbitration_service_status()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("tenant has different arbitration service status with its meta tenant", KR(ret),
               "meta_tenant_arb_status", meta_tenant_schema.get_arbitration_service_status(),
               "user_tenant_arb_status", user_tenant_schema.get_arbitration_service_status());
    } else if (meta_tenant_schema.get_arbitration_service_status().is_enabling()
               || meta_tenant_schema.get_arbitration_service_status().is_enabled()) {
      if (OB_FAIL(arbitration_service_table_operator.get(
                      trans,
                      arbitration_service_key,
                      lock_line,
                      arbitration_service_info))) {
        if (OB_ARBITRATION_SERVICE_NOT_EXIST == ret) {
          ret = OB_OP_NOT_ALLOW;
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "arbitration service not exist, create tenant");
        }
        LOG_WARN("fail to get arbitration service", KR(ret), K(arbitration_service_key),
                 K(lock_line), K(arbitration_service_info));
      }
    }
#endif
    // 1. create tenant schema
    if (OB_SUCC(ret)) {
      FLOG_INFO("[CREATE_TENANT] STEP 1.1. start create tenant schema", K(arg));
      const int64_t tmp_start_time = ObTimeUtility::fast_current_time();
      ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
      if (OB_FAIL(ddl_operator.create_tenant(meta_tenant_schema,
                 OB_DDL_ADD_TENANT_START, trans))) {
        LOG_WARN("create tenant failed", KR(ret), K(meta_tenant_schema));
      } else if (OB_FAIL(ddl_operator.create_tenant(user_tenant_schema,
                 OB_DDL_ADD_TENANT_START, trans, &arg.ddl_stmt_str_))) {
        LOG_WARN("create tenant failed", KR(ret), K(user_tenant_schema));
      }
      FLOG_INFO("[CREATE_TENANT] STEP 1.1. finish create tenant schema", KR(ret), K(arg),
               "cost", ObTimeUtility::fast_current_time() - tmp_start_time);
    }

#ifdef OB_BUILD_TDE_SECURITY
    if (OB_SUCC(ret)) {
      FLOG_INFO("[CREATE_TENANT] STEP 1.1.1. start create root key", K(user_tenant_id));
      const int64_t tmp_start_time = ObTimeUtility::fast_current_time();
      ObArray<ObAddr> addrs;
      bool need_create = false;
      if (OB_FAIL(check_need_create_root_key(arg, need_create))) {
        LOG_WARN("fail to check need create root key", K(ret));
      } else if (!need_create) {
        // do nothing
      } else if (arg.is_creating_standby_) {
        if (OB_FAIL(standby_create_root_key(user_tenant_id, arg, addrs))) {
          LOG_WARN("failed to create root key", KR(ret), K(user_tenant_id), K(arg));
        }
      } else if (OB_FAIL(create_root_key(*rpc_proxy_, user_tenant_id, addrs))) {
        LOG_WARN("fail to create root key", KR(ret), K(addrs));
      }
      FLOG_INFO("[CREATE_TENANT] STEP 1.1.1. finish create root key",
               KR(ret), K(user_tenant_id), "cost", ObTimeUtility::fast_current_time() - tmp_start_time);
    }
#endif

    // 2. grant pool
    if (OB_SUCC(ret)) {
      FLOG_INFO("[CREATE_TENANT] STEP 1.2. start grant pools", K(user_tenant_id));
      const int64_t tmp_start_time = ObTimeUtility::fast_current_time();
      lib::Worker::CompatMode compat_mode = get_worker_compat_mode(
                                         user_tenant_schema.get_compatibility_mode());
      if (OB_FAIL(unit_mgr_->grant_pools(
                         trans, new_ug_id_array,
                         compat_mode,
                         pools, user_tenant_id,
                         false/*is_bootstrap*/,
                         arg.source_tenant_id_,
                         false/*check_data_version*/))) {
        LOG_WARN("grant_pools_to_tenant failed", KR(ret), K(arg), K(pools), K(user_tenant_id));
      }
      FLOG_INFO("[CREATE_TENANT] STEP 1.2. finish grant pools", KR(ret), K(user_tenant_id),
               "cost", ObTimeUtility::fast_current_time() - tmp_start_time);
    }

    // 3. persist initial tenant config
    if (OB_SUCC(ret)) {
      FLOG_INFO("[CREATE_TENANT] STEP 1.3. start persist tenant config", K(user_tenant_id));
      const int64_t tmp_start_time = ObTimeUtility::fast_current_time();
      ObArray<ObAddr> addrs;
      if (OB_FAIL(unit_mgr_->get_servers_by_pools(pools, addrs))) {
        LOG_WARN("fail to get tenant's servers", KR(ret), K(user_tenant_id));
      } else if (OB_FAIL(notify_init_tenant_config(*rpc_proxy_, init_configs, addrs))) {
        LOG_WARN("fail to notify broadcast tenant config", KR(ret), K(addrs), K(init_configs));
      }
      FLOG_INFO("[CREATE_TENANT] STEP 1.3. finish persist tenant config",
               KR(ret), K(user_tenant_id), "cost", ObTimeUtility::fast_current_time() - tmp_start_time);
    }

    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      bool commit = OB_SUCC(ret);
      if (OB_SUCCESS != (temp_ret = trans.end(commit))) {
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
        LOG_WARN("trans end failed", K(commit), K(temp_ret));
      }
    }

    // If the transaction returns successfully, modify the unit_mgr memory data structure
    // If the transaction fails, the transaction may be submitted successfully. At this time,
    // the transaction is considered to have failed, and the unit_mgr memory state is not modified at this time,
    // and the transaction 1 is subsequently rolled back through drop tenant.
    if (OB_SUCC(ret)) {
      FLOG_INFO("[CREATE_TENANT] STEP 1.4. start reload unit_manager", K(user_tenant_id));
      const int64_t tmp_start_time = ObTimeUtility::fast_current_time();
      if (OB_FAIL(unit_mgr_->load())) {
        LOG_WARN("unit_manager reload failed", K(ret));
      }
      FLOG_INFO("[CREATE_TENANT] STEP 1.4. finish reload unit_manager", KR(ret), K(user_tenant_id),
               "cost", ObTimeUtility::fast_current_time() - tmp_start_time);
    }

    if (OB_SUCC(ret)) {
      ObArray<ObAddr> addrs;
      ObZone zone; // empty means get all zone's servers
      if (OB_FAIL(unit_mgr_->get_tenant_unit_servers(user_tenant_id, zone, addrs))) {
        LOG_WARN("fail to get tenant's servers", KR(ret), K(user_tenant_id));
      } else if (OB_FAIL(publish_schema(OB_SYS_TENANT_ID, addrs))) {
        LOG_WARN("publish schema failed", KR(ret), K(addrs));
      }
    }
  }
  FLOG_INFO("[CREATE_TENANT] STEP 1. finish create tenant schema", KR(ret), K(user_tenant_id),
           "cost", ObTimeUtility::fast_current_time() - start_time);
  return ret;
}

int ObTenantDDLService::notify_init_tenant_config(
    obrpc::ObSrvRpcProxy &rpc_proxy,
    const common::ObIArray<common::ObConfigPairs> &init_configs,
    const common::ObIArray<common::ObAddr> &addrs)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L; // 10s
  if (OB_UNLIKELY(
      init_configs.count() <= 0
      || addrs.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init configs count is invalid", KR(ret), K(init_configs), K(addrs));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, DEFAULT_TIMEOUT))) {
    LOG_WARN("fail to set default timeout", KR(ret));
  } else {
    ObArenaAllocator allocator("InitTenantConf");
    // 1. construct arg
    obrpc::ObInitTenantConfigArg arg;
    for (int64_t i = 0; OB_SUCC(ret) && i < init_configs.count(); i++) {
     const common::ObConfigPairs &pairs = init_configs.at(i);
     obrpc::ObTenantConfigArg config;
     char *buf = NULL;
     int64_t length = pairs.get_config_str_length();
     if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(length)))) {
       ret = OB_ALLOCATE_MEMORY_FAILED;
       LOG_WARN("alloc memory failed", KR(ret), K(length));
     } else {
       MEMSET(buf, '\0', length);
       if (OB_FAIL(pairs.get_config_str(buf, length))) {
         LOG_WARN("fail to get config str", KR(ret), K(length), K(pairs));
       } else {
         config.tenant_id_ = pairs.get_tenant_id();
         config.config_str_.assign_ptr(buf, strlen(buf));
         if (OB_FAIL(arg.add_tenant_config(config))) {
           LOG_WARN("fail to add config", KR(ret), K(config));
         }
       }
     }
    } // end for
    // 2. send rpc
    rootserver::ObInitTenantConfigProxy proxy(
        rpc_proxy, &obrpc::ObSrvRpcProxy::init_tenant_config);
    bool call_rs = false;
    ObAddr rs_addr = GCONF.self_addr_;
    int64_t timeout = ctx.get_timeout();
    for (int64_t i = 0; OB_SUCC(ret) && i < addrs.count(); i++) {
      const ObAddr &addr = addrs.at(i);
      if (OB_FAIL(proxy.call(addr, timeout, arg))) {
        LOG_WARN("send rpc failed", KR(ret), K(addr), K(timeout), K(arg));
      } else if (rs_addr == addr) {
        call_rs = true;
      }
    } // end for
    if (OB_FAIL(ret) || call_rs) {
    } else if (OB_FAIL(proxy.call(rs_addr, timeout, arg))) {
      LOG_WARN("fail to call rs", KR(ret), K(rs_addr), K(timeout), K(arg));
    }
    // 3. check result
    ObArray<int> return_ret_array;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(proxy.wait_all(return_ret_array))) { // ignore ret
      LOG_WARN("wait batch result failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(proxy.check_return_cnt(return_ret_array.count()))) {
      LOG_WARN("return cnt not match", KR(ret), "return_cnt", return_ret_array.count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < return_ret_array.count(); i++) {
        int return_ret = return_ret_array.at(i);
        const ObAddr &addr = proxy.get_dests().at(i);
        const ObInitTenantConfigRes *result = proxy.get_results().at(i);
        if (OB_SUCCESS != return_ret) {
          ret = return_ret;
          LOG_WARN("rpc return error", KR(ret), K(addr), K(timeout));
        } else if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get empty result", KR(ret), K(addr), K(timeout));
        } else if (OB_SUCCESS != result->get_ret()) {
          ret = result->get_ret();
          LOG_WARN("persist tenant config failed", KR(ret), K(addr), K(timeout));
        }
      } // end for
    }
  }
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObTenantDDLService::check_need_create_root_key(const ObCreateTenantArg &arg, bool &need_create)
{
  int ret = OB_SUCCESS;
  need_create = false;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_0_0) {
    need_create = false;
  } else if (arg.is_restore_tenant() || arg.is_clone_tenant()) {
    need_create = false;
  } else {
    need_create = true;
  }
  return ret;
}

int ObTenantDDLService::standby_create_root_key(
             const uint64_t tenant_id,
             const obrpc::ObCreateTenantArg &arg,
             const common::ObIArray<common::ObAddr> &addrs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id) || !arg.is_creating_standby_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(arg));
  } else {
    obrpc::RootKeyType key_type = obrpc::RootKeyType::INVALID;
    common::ObString root_key;
    ObArenaAllocator allocator("root_key");

    if (OB_FAIL(get_root_key_from_primary(arg, tenant_id, key_type, root_key, allocator))) {
      LOG_WARN("failed to get root key", KR(ret), K(arg), K(tenant_id));
    } else {
      obrpc::ObRootKeyArg root_key_arg;
      obrpc::ObRootKeyResult dummy_result;
      if (OB_FAIL(root_key_arg.init(tenant_id, key_type, root_key))) {
        LOG_WARN("failed to init root key arg", KR(ret), K(tenant_id), K(key_type), K(root_key));
      } else if (OB_FAIL(notify_root_key(*rpc_proxy_, root_key_arg, addrs, dummy_result))) {
        LOG_WARN("fail to notify root key", K(ret), K(root_key_arg));
      }
    }
  }
  return ret;
}

int ObTenantDDLService::get_root_key_from_primary(const obrpc::ObCreateTenantArg &arg,
    const uint64_t tenant_id, obrpc::RootKeyType &key_type,
    common::ObString &key_value,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id) || !arg.is_creating_standby_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(arg));
  } else {
    uint64_t primary_tenant_id = OB_INVALID_TENANT_ID;
    uint64_t cluster_id = OB_INVALID_ID;
    ObArray<ObAddr> addr_list;
    ObLogRestoreSourceServiceConfigParser log_restore_source(ObBackupConfigType::LOG_RESTORE_SOURCE, tenant_id);
    common::ObSqlString value;
    obrpc::ObRootKeyArg root_key_arg;
    obrpc::ObRootKeyResult result;
    if (OB_FAIL(value.assign(arg.log_restore_source_))) {
      LOG_WARN("fail to assign value", KR(ret), K(log_restore_source));
    } else if (OB_FAIL(log_restore_source.get_primary_server_addr(
      value, primary_tenant_id, cluster_id, addr_list))) {
      LOG_WARN("failed to get primary server addr", KR(ret), K(value));
    } else if (OB_FAIL(root_key_arg.init_for_get(primary_tenant_id))) {
      LOG_WARN("failed to init for get", KR(ret), K(primary_tenant_id));
    }
    if (FAILEDx(notify_root_key(*rpc_proxy_, root_key_arg,
            addr_list, result, true/*enable_default*/, true/*skip_call_rs*/,
            cluster_id, &allocator))) {
      LOG_WARN("failed to get root key from obs", KR(ret), K(cluster_id),
          K(root_key_arg), K(addr_list));
    } else {
      key_type = result.key_type_;
      key_value = result.root_key_;
    }
    if (OB_INVALID_ROOT_KEY == ret) {
      LOG_USER_ERROR(OB_INVALID_ROOT_KEY, "Can not get root key from primary tenant");
    }
    LOG_INFO("get root key from primary tenant", K(primary_tenant_id), K(tenant_id), K(value),
        K(addr_list), K(key_type), K(key_value), K(cluster_id));
  }
  return ret;
}

int ObTenantDDLService::create_root_key(
    obrpc::ObSrvRpcProxy &rpc_proxy,
    const uint64_t tenant_id,
    const common::ObIArray<common::ObAddr> &addrs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", KR(ret), K(tenant_id), K(addrs));
  } else {
    char root_key[OB_ROOT_KEY_LEN] = {0};
    obrpc::ObRootKeyArg arg;
    obrpc::ObRootKeyResult dummy_result;
    arg.tenant_id_ = tenant_id;
    arg.is_set_ = true;
    arg.key_type_ = obrpc::RootKeyType::NORMAL;
    if (OB_FAIL(ObKeyGenerator::generate_encrypt_key(root_key, OB_ROOT_KEY_LEN))) {
      LOG_WARN("fail to generate root key", K(ret));
    } else if (FALSE_IT(arg.root_key_.assign_ptr(root_key, OB_ROOT_KEY_LEN))) {
    } else if (OB_FAIL(notify_root_key(rpc_proxy, arg, addrs, dummy_result))) {
      LOG_WARN("fail to notify root key", K(ret));
    }
  }
  return ret;
}

int ObTenantDDLService::notify_root_key(
    obrpc::ObSrvRpcProxy &rpc_proxy,
    const obrpc::ObRootKeyArg &arg,
    const common::ObIArray<common::ObAddr> &addrs,
    obrpc::ObRootKeyResult &result,
    const bool enable_default /*=true*/,
    const bool skip_call_rs /*=false*/,
    const uint64_t &cluster_id /*=OB_INVALID_CLUSTER_ID*/,
    common::ObIAllocator *allocator /*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  bool has_failed = false;
  const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L; // 10s
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, DEFAULT_TIMEOUT))) {
    LOG_WARN("fail to set default timeout", KR(ret));
  } else {
    // 1. send rpc
    rootserver::ObSetRootKeyProxy proxy(
        rpc_proxy, &obrpc::ObSrvRpcProxy::set_root_key);
    int tmp_ret = OB_SUCCESS;
    int return_ret = OB_SUCCESS;
    // need_to_call_rs is true only if skip_call_rs is false and not notify cross-cluster
    bool need_call_rs = (!skip_call_rs) && (OB_INVALID_CLUSTER_ID == cluster_id);
    ObAddr rs_addr = GCONF.self_addr_;
    int64_t timeout = ctx.get_timeout();
    for (int64_t i = 0; OB_SUCC(ret) && i < addrs.count(); i++) {
      const ObAddr &addr = addrs.at(i);
      if (rs_addr == addr && !need_call_rs) {
        // skip rs_addr if need_call_rs is false
      } else if (OB_TMP_FAIL(proxy.call(addr, timeout, cluster_id, OB_SYS_TENANT_ID, arg))) {
        has_failed = true;
        return_ret= tmp_ret;
        LOG_WARN("send rpc failed", KR(ret), KR(tmp_ret), K(addr), K(timeout), K(cluster_id));
      } else if (rs_addr == addr) {
        need_call_rs = false;
      }
    } // end for
    if (OB_FAIL(ret) || !need_call_rs) {
    } else if (OB_TMP_FAIL(proxy.call(rs_addr, timeout, cluster_id, OB_SYS_TENANT_ID, arg))) {
      has_failed = true;
      return_ret= tmp_ret;
      LOG_WARN("fail to call rs", KR(ret), KR(tmp_ret), K(rs_addr), K(timeout), K(cluster_id));
    }
    // 2. check result
    ObArray<int> return_ret_array;
    if (OB_TMP_FAIL(proxy.wait_all(return_ret_array))) { // ignore ret
      LOG_WARN("wait batch result failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    } else if (OB_FAIL(ret) || has_failed) {
    } else {
      // don't use arg/dest here because call() may has failure.
      for (int64_t i = 0; OB_SUCC(ret) && i < return_ret_array.count() && !has_failed; ++i) {
        if (OB_TMP_FAIL(return_ret_array.at(i))) {
          has_failed = true;
          return_ret = tmp_ret;
          LOG_WARN("rpc return error", KR(tmp_ret), K(i));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (arg.is_set_) {
      if (OB_UNLIKELY(has_failed)) {
        ret = return_ret;
        LOG_WARN("failed to set root key", KR(ret));
      }
    } else {
      // 1. don't use arg/dest here because call() may has failure.
      // 2. result may be meanless when related return ret is not OB_SUCCESS
      obrpc::RootKeyType key_type = obrpc::RootKeyType::INVALID;
      ObString root_key;
      for (int64_t i = 0; OB_SUCC(ret) && i < proxy.get_results().count(); ++i) {
        const ObRootKeyResult *rpc_result = proxy.get_results().at(i);
        if (OB_ISNULL(rpc_result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get empty result", KR(ret), K(i), K(addrs));
        } else if (obrpc::RootKeyType::INVALID == rpc_result->key_type_) {
          //There may be no root_key information on some observers
        } else if (rpc_result->key_type_ != key_type) {
          if (OB_UNLIKELY(obrpc::RootKeyType::INVALID != key_type)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("root key type is conflict", KR(ret), K(key_type), KPC(rpc_result));
          }
          if (OB_SUCC(ret)) {
            key_type = rpc_result->key_type_;
            root_key = rpc_result->root_key_;
          }
        } else if (OB_UNLIKELY(0 != root_key.compare(rpc_result->root_key_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("root key is conflict", KR(ret), K(root_key), KPC(rpc_result));
        }
      } // end for
      if (OB_SUCC(ret) && obrpc::RootKeyType::INVALID == key_type) {
        if (has_failed) {
          ret = OB_INVALID_ROOT_KEY;
          LOG_WARN("failed to get root key from obs", KR(ret), K(cluster_id),
            K(addrs), K(key_type), K(root_key));
        } else if (enable_default) {
          //If the root_key cannot be obtained from all current observers,
          //set default. This tenant may be an upgraded tenant.
          //The addrs are obtained from the __all_virtual_log_stat in standby cluster,
          //it may not be all observers, ignore this situation
          key_type = obrpc::RootKeyType::DEFAULT;
          LOG_INFO("can not get root key from all observer, set default", K(cluster_id),
                   K(addrs), K(key_type), K(root_key));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (obrpc::RootKeyType::INVALID == key_type) {
        result.key_type_ = key_type;
        result.root_key_.reset();
      } else if (OB_INVALID_CLUSTER_ID != cluster_id) {
        if (OB_ISNULL(allocator)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("allocator is null", K(ret));
        } else if (OB_FAIL(deep_copy_ob_string(*allocator, root_key, result.root_key_))) {
          LOG_WARN("failed to deep copy string", KR(ret));
        } else {
          result.key_type_ = key_type;
        }
      } else if (OB_FAIL(ObMasterKeyGetter::instance().set_root_key(arg.tenant_id_,
                                                key_type, root_key))) {
        LOG_WARN("failed to set root key", K(ret));
      } else if (OB_FAIL(ObMasterKeyGetter::instance().get_root_key(arg.tenant_id_,
                                                result.key_type_, result.root_key_))) {
        LOG_WARN("failed to get root key", K(ret));
      } else if (OB_UNLIKELY(key_type != result.key_type_ ||
                              0 != root_key.compare(result.root_key_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpect root key", K(ret));
      }
    }
  }
  return ret;
}
#endif

int ObTenantDDLService::broadcast_tenant_init_config_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_meta_tenant(tenant_id) && !is_sys_tenant(tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only meta tenant can braodcast tenant init config", KR(ret), K(tenant_id));
  } else {
    const int64_t config_version = omt::ObTenantConfig::INITIAL_TENANT_CONF_VERSION + 1;
    const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
    if (OB_FAIL(OTC_MGR.set_tenant_config_version(tenant_id, config_version))) {
      LOG_WARN("failed to set tenant config version", KR(ret), K(tenant_id));
    } else if (is_meta_tenant(tenant_id) && OB_FAIL(OTC_MGR.set_tenant_config_version(user_tenant_id,
            config_version))) {
      LOG_WARN("failed to set tenant config version", KR(ret), K(user_tenant_id));
    }
  }
  return ret;
}

int ObTenantDDLService::get_tenant_schema_(
    const obrpc::ObParallelCreateNormalTenantArg &arg,
    ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tmp_tenant_schema = nullptr;
  const uint64_t tenant_id = arg.tenant_id_;
  if (!arg.is_valid() || !is_valid_tenant_id(tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(arg));
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(
          OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tmp_tenant_schema))) {
    LOG_WARN("failed to get tenant info", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tmp_tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema is NULL", KR(ret), KP(tmp_tenant_schema));
  } else if (OB_FAIL(tenant_schema.assign(*tmp_tenant_schema))) {
    LOG_WARN("failed to assign tenant_schema", KR(ret), KP(tmp_tenant_schema));
  } else if (!tenant_schema.is_creating()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant status is not creating", KR(ret), K(tenant_schema));
  } else if (OB_FAIL(set_tenant_schema_charset_and_collation(tenant_schema, arg.create_tenant_arg_))) {
    // charset and collation are not written in __all_tenant
    // create database will use the charset
    // so if user set charset in create_tenant sql, we should assign it to tenant_schema
    LOG_WARN("failed to set tenant charset and collation", KR(ret));
  }
  return ret;
}

int ObTenantDDLService::insert_tenant_merge_info_(
    const ObSchemaOperationType op,
    const ObTenantSchema &tenant_schema,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
    // add zone merge info
    if ((OB_DDL_ADD_TENANT_START == op) || (OB_DDL_ADD_TENANT == op)) {
      HEAP_VARS_4((ObGlobalMergeInfo, global_info),
                  (ObZoneMergeInfoArray, merge_info_array),
                  (ObZoneArray, zone_list),
                  (ObZoneMergeInfo, tmp_merge_info)) {

        global_info.tenant_id_ = tenant_id;
        tmp_merge_info.tenant_id_ = tenant_id;
        if (OB_FAIL(tenant_schema.get_zone_list(zone_list))) {
          LOG_WARN("fail to get zone list", KR(ret));
        }

        for (int64_t i = 0; (i < zone_list.count()) && OB_SUCC(ret); ++i) {
          tmp_merge_info.zone_ = zone_list.at(i);
          if (OB_FAIL(merge_info_array.push_back(tmp_merge_info))) {
            LOG_WARN("fail to push_back", KR(ret));
          }
        }
        // add zone merge info of current tenant(sys tenant or meta tenant)
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObGlobalMergeTableOperator::insert_global_merge_info(trans,
              tenant_id, global_info))) {
            LOG_WARN("fail to insert global merge info of current tenant", KR(ret), K(global_info));
          } else if (OB_FAIL(ObZoneMergeTableOperator::insert_zone_merge_infos(
                     trans, tenant_id, merge_info_array))) {
            LOG_WARN("fail to insert zone merge infos of current tenant", KR(ret), K(tenant_id),
              K(merge_info_array));
          }
        }
        // add zone merge info of relative user tenant if current tenant is meta tenant
        if (OB_SUCC(ret) && is_meta_tenant(tenant_id)) {
          const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
          global_info.tenant_id_ = user_tenant_id;
          for (int64_t i = 0; i < merge_info_array.count(); ++i) {
            merge_info_array.at(i).tenant_id_ = user_tenant_id;
          }
          if (OB_FAIL(ObGlobalMergeTableOperator::insert_global_merge_info(trans,
              user_tenant_id, global_info))) {
            LOG_WARN("fail to insert global merge info of user tenant", KR(ret), K(global_info));
          } else if (OB_FAIL(ObZoneMergeTableOperator::insert_zone_merge_infos(
                    trans, user_tenant_id, merge_info_array))) {
            LOG_WARN("fail to insert zone merge infos of user tenant", KR(ret), K(user_tenant_id),
              K(merge_info_array));
          }
        }
      }
    }
  }

  return ret;
}
int ObTenantDDLService::init_tenant_env_after_schema_(
    const ObTenantSchema &tenant_schema,
    const obrpc::ObParallelCreateNormalTenantArg &arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("[CREATE_TENANT] STEP 2.6. start init_tenant_env_after_schema_", K(tenant_id));
  const ObCreateTenantArg &create_tenant_arg = arg.create_tenant_arg_;
  const ObTenantRole &tenant_role = create_tenant_arg.get_tenant_role();
  const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
  if (OB_FAIL(trans.start(sql_proxy_, tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(tenant_id));
  } else if (!tenant_role.is_primary() && OB_FAIL(insert_restore_or_clone_tenant_job_(
          user_tenant_id, create_tenant_arg.tenant_schema_.get_tenant_name(), tenant_role,
          create_tenant_arg.source_tenant_id_, trans))) {
    LOG_WARN("failed to insert restore or clone tenant job", KR(ret), K(create_tenant_arg));
  } else if (create_tenant_arg.is_creating_standby_ &&
      OB_FAIL(set_log_restore_source_(user_tenant_id, create_tenant_arg.log_restore_source_, trans))) {
    LOG_WARN("failed to set_log_restore_source", KR(ret), K(user_tenant_id), K(create_tenant_arg));
  } else if (is_meta_tenant(tenant_id)) {
    if (OB_FAIL(insert_tenant_merge_info_(OB_DDL_ADD_TENANT_START, tenant_schema, trans))) {
      LOG_WARN("fail to insert tenant merge info", KR(ret), K(tenant_schema));
    } else if (OB_FAIL(ObServiceEpochProxy::init_service_epoch(
            trans,
            tenant_id,
            0, /*freeze_service_epoch*/
            0, /*arbitration_service_epoch*/
            0, /*server_zone_op_service_epoch*/
            0, /*heartbeat_service_epoch*/
            0 /* service_name_epoch */))) {
      LOG_WARN("fail to init service epoch", KR(ret));
    }
  }
  if (trans.is_started()) {
    bool commit = OB_SUCC(ret);
    if (OB_TMP_FAIL(trans.end(commit))) {
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      LOG_WARN("trans end failed", K(commit), K(tmp_ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (is_meta_tenant(tenant_id) && OB_FAIL(broadcast_tenant_init_config_(tenant_id))) {
    // If tenant config version in RS is valid first and ddl trans doesn't commit,
    // observer may read from empty __tenant_parameter successfully and raise its tenant config version,
    // which makes some initial tenant configs are not actually updated before related observer restarts.
    // To fix this problem, tenant config version in RS should be valid after ddl trans commits.
    LOG_WARN("failed to braodcast tenant init config", KR(ret), K(tenant_id));
  }
  FLOG_INFO("[CREATE_TENANT] STEP 2.6. finish init_tenant_env_after_schema_", K(tenant_id),
      "cost", ObTimeUtility::fast_current_time() - start_time);
  return ret;
}

int ObTenantDDLService::init_tenant_env_before_schema_(
    const ObTenantSchema &tenant_schema,
    const obrpc::ObParallelCreateNormalTenantArg &arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("[CREATE_TENANT] STEP 2.4. start init_tenant_env_before_schema_", K(tenant_id));
  const ObCreateTenantArg &create_tenant_arg = arg.create_tenant_arg_;
  common::ObArray<common::ObConfigPairs> init_configs;
  if (OB_FAIL(generate_tenant_init_configs(create_tenant_arg, user_tenant_id, init_configs))) {
    LOG_WARN("failed to generate tenant init configs", KR(ret), K(arg), K(tenant_id));
  } else if (OB_FAIL(add_extra_tenant_init_config_(user_tenant_id, init_configs))) {
    LOG_WARN("fail to add_extra_tenant_init_config", KR(ret), K(tenant_id));
  } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(tenant_id));
  } else if (OB_FAIL(init_tenant_global_stat_(tenant_id, init_configs, trans))) {
    LOG_WARN("failed to init tenant global stat", KR(ret), K(tenant_id), K(init_configs));
  } else if (OB_FAIL(init_tenant_sys_stats_(tenant_id, trans))) {
    LOG_WARN("insert default sys stats failed", K(tenant_id), K(ret));
  } else if (is_meta_tenant(tenant_id) && OB_FAIL(init_meta_tenant_env_(tenant_schema,
          create_tenant_arg, init_configs, trans))) {
    LOG_WARN("failed to init meta tenant env", KR(ret), K(tenant_schema), K(init_configs));
  } else if (is_user_tenant(tenant_id) && OB_FAIL(init_user_tenant_env_(tenant_id, trans))) {
    LOG_WARN("failed to init user tenant env", KR(ret), K(tenant_id));
  }
  if (trans.is_started()) {
    bool commit = OB_SUCC(ret);
    if (OB_TMP_FAIL(trans.end(commit))) {
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      LOG_WARN("trans end failed", K(commit), K(tmp_ret));
    }
  }
  FLOG_INFO("[CREATE_TENANT] STEP 2.4. finish init_tenant_env_before_schema_", K(tenant_id),
      "cost", ObTimeUtility::fast_current_time() - start_time);
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_CREATE_META_NORMAL_TENANT_ERROR);
ERRSIM_POINT_DEF(ERRSIM_CREATE_USER_NORMAL_TENANT_ERROR);
// this function is running on parallel ddl thread while not hold any ddl lock
// it does the following things:
// 1. broadcast schema
// 2. create tablet
// 3. create other schema
int ObTenantDDLService::create_normal_tenant(obrpc::ObParallelCreateNormalTenantArg &arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;
  const ObCreateTenantArg &create_tenant_arg = arg.create_tenant_arg_;
  ObCurTraceId::TraceId old_trace_id = OB_ISNULL(ObCurTraceId::get_trace_id()) ?
    ObCurTraceId::TraceId() : *ObCurTraceId::get_trace_id();
  ObCurTraceId::TraceId new_trace_id;
  new_trace_id.init(GCONF.self_addr_);
  FLOG_INFO("[CREATE_TENANT] change trace_id for create tenant", K(new_trace_id), K(tenant_id));
  common::ObTraceIdGuard trace_id_guard(new_trace_id);
  TIMEGUARD_INIT(create_normal_tenant, 10_s);
  FLOG_INFO("[CREATE_TENANT] STEP 2. start create normal tenant", K(tenant_id));
  ObArenaAllocator arena_allocator("InnerTableSchem", OB_MALLOC_MIDDLE_BLOCK_SIZE);
  ObSArray<ObTableSchema> tables;
  ObTenantSchema tenant_schema;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_FAIL(get_tenant_schema_(arg, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), "tenant_id", arg.tenant_id_);
  }
  if (is_user_tenant(tenant_id)) {
    DEBUG_SYNC(BEFORE_CREATE_USER_TENANT);
  } else if (is_meta_tenant(tenant_id)) {
    DEBUG_SYNC(BEFORE_CREATE_META_TENANT);
  }
  // user tenant sys ls is created before meta tenant table writable
  // which may cause user tenant trying to write meta tenant's table when meta tenant's tablets are creating
  // in this case, SQL.ENG will return -4138 which will retry immediately.
  // the retries will explod user's work queue .
  // so we create tablet before braodcast schema, make them retrun OB_TABLE_NOT_EXIST which will not be retried
  if (FAILEDx(ObSchemaUtils::construct_inner_table_schemas(tenant_id, tables, arena_allocator))) {
    LOG_WARN("fail to get inner table schemas in tenant space", KR(ret), K(tenant_id));
  } else if (CLICK_FAIL(create_tenant_sys_tablets(tenant_id, tables))) {
    LOG_WARN("fail to create tenant partitions", KR(ret), K(tenant_id));
  } else if (CLICK_FAIL(broadcast_sys_table_schemas(tenant_id, tables))) {
    LOG_WARN("fail to broadcast sys table schemas", KR(ret), K(tenant_id));
  }
  if (is_meta_tenant(tenant_id)) {
    DEBUG_SYNC(AFTER_CREATE_META_TENANT_TABLETS);
  } else if (is_user_tenant(tenant_id)) {
    DEBUG_SYNC(AFTER_CREATE_USER_TENANT_TABLETS);
  }
  if (OB_FAIL(ret)) {
    // for code we need to execute before creating tenant user ls, we should put it in `before`
    // otherwise, they should be put in `after`
  } else if (CLICK_FAIL(init_tenant_env_before_schema_(tenant_schema, arg))) {
    LOG_WARN("failed to init tenant env before schema", KR(ret), K(tenant_schema), K(arg));
  } else if (CLICK_FAIL(init_tenant_schema(arg.create_tenant_arg_, tenant_schema, tables))) {
    LOG_WARN("fail to init tenant schema", KR(ret), K(tenant_id));
  } else if (CLICK_FAIL(init_tenant_env_after_schema_(tenant_schema, arg))) {
    LOG_WARN("failed to init tenant env after schema", KR(ret), K(tenant_schema), K(arg));
  }
  CLICK();
  if (is_user_tenant(tenant_id)) {
    DEBUG_SYNC(AFTER_CREATE_USER_NORMAL_TENANT);
  } else if (is_meta_tenant(tenant_id)) {
    DEBUG_SYNC(AFTER_CREATE_META_NORMAL_TENANT);
  }
  if (OB_FAIL(ret)) {
  } else if (is_user_tenant(tenant_id) && OB_FAIL(ERRSIM_CREATE_USER_NORMAL_TENANT_ERROR)) {
    LOG_WARN("ERRSIM_CREATE_USER_NORMAL_TENANT_ERROR hit", KR(ret));
  } else if (is_meta_tenant(tenant_id) && OB_FAIL(ERRSIM_CREATE_META_NORMAL_TENANT_ERROR)) {
    LOG_WARN("ERRSIM_CREATE_META_NORMAL_TENANT_ERROR hit", KR(ret));
  }
  FLOG_INFO("[CREATE_TENANT] STEP 2. finish create normal tenant", KR(ret), K(new_trace_id), K(tenant_id));
  return ret;
}

int ObTenantDDLService::broadcast_sys_table_schemas(
    const uint64_t tenant_id,
    common::ObIArray<ObTableSchema> &tables)
{
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("[CREATE_TENANT] STEP 2.3. start broadcast sys table schemas", K(tenant_id));
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (is_sys_tenant(tenant_id) || is_virtual_tenant_id(tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    // Ensure observer which contains rs or tenant's sys ls leader has avaliable schemas.
    const uint64_t user_tenant_id = is_user_tenant(tenant_id) ? tenant_id : gen_user_tenant_id(tenant_id);
    ObArray<ObAddr> addrs;
    ObArray<ObUnit> units;
    ObAddr leader;
    ObUnitTableOperator unit_operator;
    if (OB_FAIL(get_ls_member_list_for_creating_tenant_(tenant_id, ObLSID::SYS_LS_ID, leader, addrs))) {
      // we just need leader, so addrs is not used, it will be reset later
      LOG_WARN("failed to get ls member list for creating tenant", KR(ret), K(tenant_id));
    } else if (OB_FAIL(unit_operator.init(*sql_proxy_))) {
      LOG_WARN("failed to init unit operator", KR(ret));
    } else if (OB_FAIL(unit_operator.get_units_by_tenant(user_tenant_id, units))) {
      LOG_WARN("failed to get units by tenant", KR(ret), K(user_tenant_id));
    } else {
      ObTimeoutCtx ctx;
      ObBatchBroadcastSchemaProxy proxy(*rpc_proxy_,
                                        &ObSrvRpcProxy::batch_broadcast_schema);
      obrpc::ObBatchBroadcastSchemaArg arg;
      int64_t sys_schema_version = OB_INVALID_VERSION;
      addrs.reset();
      FOREACH_X(unit, units, OB_SUCC(ret)) {
        if (!unit->is_valid() || !unit->is_active_status()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid unit", KR(ret), K(*unit));
        } else if (OB_FAIL(addrs.push_back(unit->server_))) {
          LOG_WARN("failed to push_back addr", KR(ret), K(*unit));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!is_contain(addrs, GCONF.self_addr_) && OB_FAIL(addrs.push_back(GCONF.self_addr_))) {
          LOG_WARN("fail to push back rs addr", KR(ret));
      } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
        LOG_WARN("fail to set timeout ctx", KR(ret));
      } else if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(
                 OB_SYS_TENANT_ID, sys_schema_version))) {
      } else if (OB_FAIL(arg.init(tenant_id, sys_schema_version, tables))) {
        LOG_WARN("fail to init arg", KR(ret), K(tenant_id), K(sys_schema_version));
      }
      const int64_t timeout_ts = ctx.get_timeout(0);
      for (int64_t i = 0; OB_SUCC(ret) && i < addrs.count(); i++) {
        const ObAddr &addr = addrs.at(i);
        if (OB_FAIL(proxy.call(addr, timeout_ts, arg))) {
          LOG_WARN("fail to send rpc", KR(ret), K(tenant_id),
                   K(sys_schema_version), K(addr), K(timeout_ts));
        }
      } // end for

      ObArray<int> return_code_array;
      int tmp_ret = OB_SUCCESS; // always wait all
      if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
        LOG_WARN("wait batch result failed", KR(tmp_ret), KR(ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      } else if (OB_FAIL(ret)) {
      } else if (OB_FAIL(proxy.check_return_cnt(return_code_array.count()))) {
        LOG_WARN("return cnt not match", KR(ret), "return_cnt", return_code_array.count());
      } else {
        int tmp_ret = OB_SUCCESS;
        for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); i++) {
          const ObAddr &addr = proxy.get_dests().at(i);
          if (OB_TMP_FAIL(return_code_array.at(i))) {
            LOG_WARN("broadcast schema failed", KR(tmp_ret), K(addr), K(tenant_id));
            if (addr == leader || addr == GCONF.self_addr_) {
              ret = tmp_ret;
              LOG_WARN("rs or tenant sys ls leader braodcast schema failed", KR(ret), K(addr), K(leader));
            }
          }
        } // end for
      }
    }
  }
  FLOG_INFO("[CREATE_TENANT] STEP 2.3. finish broadcast sys table schemas", KR(ret), K(tenant_id),
           "cost", ObTimeUtility::fast_current_time() - start_time);
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_CREATE_SYS_TABLETS_ERROR);
int ObTenantDDLService::create_tenant_sys_tablets(
    const uint64_t tenant_id,
    common::ObIArray<ObTableSchema> &tables)
{
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("[CREATE_TENANT] STEP 2.2. start create sys table tablets", K(tenant_id));
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (OB_ISNULL(rpc_proxy_)
             || OB_ISNULL(lst_operator_)
             || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(rpc_proxy), KP_(lst_operator));
  } else {
    // FIXME: (yanmu.ztl) use actual trans later
    ObMySQLTransaction trans;
    share::schema::ObSchemaGetterGuard dummy_guard;
    SCN frozen_scn = SCN::base_scn();
    ObTableCreator table_creator(tenant_id,
                                 frozen_scn,
                                 trans);
    ObNewTableTabletAllocator new_table_tablet_allocator(
                              tenant_id,
                              dummy_guard,
                              sql_proxy_);
    common::ObArray<share::ObLSID> ls_id_array;
    const ObTablegroupSchema *dummy_tablegroup_schema = NULL;
    ObArray<const share::schema::ObTableSchema*> table_schemas;
    ObArray<uint64_t> index_tids;
    ObArray<bool> need_create_empty_majors;
    if (OB_FAIL(trans.start(sql_proxy_, tenant_id))) {
      LOG_WARN("fail to start trans", KR(ret), K(tenant_id));
    } else if (OB_FAIL(table_creator.init(false/*need_tablet_cnt_check*/))) {
      LOG_WARN("fail to init tablet creator", KR(ret), K(tenant_id));
    } else if (OB_FAIL(new_table_tablet_allocator.init())) {
      LOG_WARN("fail to init new table tablet allocator", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
      const ObTableSchema &data_table = tables.at(i);
      const uint64_t data_table_id = data_table.get_table_id();
      if (data_table.has_partition()) {
        table_schemas.reset();
        need_create_empty_majors.reset();
        if (OB_FAIL(table_schemas.push_back(&data_table)) || OB_FAIL(need_create_empty_majors.push_back(true))) {
          LOG_WARN("fail to push back data table ptr", KR(ret), K(data_table_id));
        } else if (ObSysTableChecker::is_sys_table_has_index(data_table_id)) {
          if (OB_FAIL(ObSysTableChecker::get_sys_table_index_tids(data_table_id, index_tids))) {
            LOG_WARN("fail to get sys index tids", KR(ret), K(data_table_id));
          } else if (i + index_tids.count()  >= tables.count()
                     || index_tids.count() <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sys table's index should be next to its data table",
                     KR(ret), K(i), "index_cnt",  index_tids.count());
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < index_tids.count(); j++) {
              const ObTableSchema &index_schema = tables.at(i + j + 1);
              const int64_t index_id = index_schema.get_table_id();
              if (index_id != index_tids.at(j)
                  || data_table_id != index_schema.get_data_table_id()) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("sys index schema order is not match", KR(ret), K(data_table_id), K(j), K(index_schema));
              } else if (OB_FAIL(table_schemas.push_back(&index_schema))) {
                LOG_WARN("fail to push back index schema", KR(ret), K(index_id), K(data_table_id));
              } else if (OB_FAIL(need_create_empty_majors.push_back(true))) {
                LOG_WARN("fail to push back need create empty major", KR(ret), K(index_id), K(data_table_id));
              }
            } // end for
          }
        }

        if (OB_SUCC(ret) && is_system_table(data_table_id)) {
          uint64_t lob_meta_table_id = OB_INVALID_ID;
          uint64_t lob_piece_table_id = OB_INVALID_ID;
          if (OB_ALL_CORE_TABLE_TID == data_table_id) {
            // do nothing
          } else if (!get_sys_table_lob_aux_table_id(data_table_id, lob_meta_table_id, lob_piece_table_id)) {
            ret = OB_ENTRY_NOT_EXIST;
            LOG_WARN("fail to get_sys_table_lob_aux_table_id", KR(ret), K(data_table_id));
          } else {
            int64_t meta_idx = -1;
            int64_t piece_idx = -1;
            for (int64_t k = i + 1; OB_SUCC(ret) && k < tables.count(); k++) {
              if (tables.at(k).get_table_id() == lob_meta_table_id) {
                meta_idx = k;
              }
              if (tables.at(k).get_table_id() == lob_piece_table_id) {
                piece_idx = k;
              }
              if (meta_idx != -1 && piece_idx != -1) {
                break;
              }
            }
            if (meta_idx == -1 || piece_idx == -1) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("sys table's lob table not matched", KR(ret), K(meta_idx), K(piece_idx),
                       K(lob_piece_table_id), K(lob_meta_table_id), K(data_table_id));
            } else {
              if (OB_FAIL(table_schemas.push_back(&tables.at(meta_idx))) || OB_FAIL(need_create_empty_majors.push_back(true))) {
                LOG_WARN("fail to push back lob meta aux table ptr", KR(ret), K(meta_idx), K(data_table_id));
              } else if (OB_FAIL(table_schemas.push_back(&tables.at(piece_idx))) || OB_FAIL(need_create_empty_majors.push_back(true))) {
                LOG_WARN("fail to push back lob piece aux table ptr", KR(ret), K(piece_idx), K(data_table_id));
              }
            }
          }
        }
        if (OB_FAIL(ret)) {
          // failed, bypass
        } else if (OB_FAIL(new_table_tablet_allocator.prepare(trans, data_table, dummy_tablegroup_schema))) {
          LOG_WARN("fail to prepare ls for sys table tablets");
        } else if (OB_FAIL(new_table_tablet_allocator.get_ls_id_array(
                ls_id_array))) {
          LOG_WARN("fail to get ls id array", KR(ret));
        } else if (OB_FAIL(table_creator.add_create_tablets_of_tables_arg(
                table_schemas,
                ls_id_array,
                DATA_CURRENT_VERSION,
                need_create_empty_majors/*need_create_empty_major_sstable*/))) {
          LOG_WARN("fail to add create tablets of table", KR(ret), K(data_table), K(table_schemas), K(need_create_empty_majors));
        }
      }
    } // end for
    if (FAILEDx(table_creator.execute())) {
      LOG_WARN("fail to execute creator", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ERRSIM_CREATE_SYS_TABLETS_ERROR)) {
      LOG_WARN("ERRSIM_CREATE_SYS_TABLETS_ERROR", KR(ret));
    } else {
      ALLOW_NEXT_LOG();
      LOG_INFO("create tenant sys tables tablet", KR(ret), K(tenant_id));
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      bool commit = OB_SUCC(ret);
      if (OB_SUCCESS != (temp_ret = trans.end(commit))) {
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
        LOG_WARN("trans end failed", K(commit), K(temp_ret));
      }
    }

    // finishing is always invoked for new table tablet allocator
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = new_table_tablet_allocator.finish(OB_SUCCESS == ret))) {
      LOG_WARN("fail to finish new table tablet allocator", KR(tmp_ret));
    }
  }
  FLOG_INFO("[CREATE_TENANT] STEP 2.2. finish create sys table tablets", KR(ret), K(tenant_id),
           "cost", ObTimeUtility::fast_current_time() - start_time);
  return ret;
}

int ObTenantDDLService::insert_restore_or_clone_tenant_job_(
    const uint64_t tenant_id,
    const ObString &tenant_name,
    const share::ObTenantRole &tenant_role,
    const uint64_t source_tenant_id,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("failed to check_inner_stat", KR(ret));
  } else if (is_sys_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", KR(ret), K(tenant_id));
  } else if (is_meta_tenant(tenant_id)) {
    // no need to insert job for meta tenant
  } else if (!tenant_role.is_restore() && !tenant_role.is_clone()) {
    // no need to insert restore/clone job
  } else if (tenant_role.is_clone()) {
    // insert clone job
    if (OB_FAIL(ObTenantCloneUtil::insert_user_tenant_clone_job(*sql_proxy_, tenant_name, tenant_id, trans))) {
      LOG_WARN("failed to insert user tenant clone job", KR(ret), K(tenant_name), K(tenant_id));
    }
  } else if (OB_FAIL(ObRestoreUtil::insert_user_tenant_restore_job(*sql_proxy_, tenant_name, tenant_id, trans))) {
    LOG_WARN("failed to insert user tenant restore job", KR(ret), K(tenant_id), K(tenant_name));
  }
  return ret;
}

int ObTenantDDLService::set_log_restore_source_(
    const uint64_t tenant_id,
    const common::ObString &log_restore_source,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  share::ObBackupConfigParserMgr config_parser_mgr;
  common::ObSqlString name;
  common::ObSqlString value;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || log_restore_source.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(log_restore_source));
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("failed to check_inner_stat", KR(ret));
  } else if (OB_FAIL(name.assign("log_restore_source"))) {
    LOG_WARN("assign sql failed", KR(ret));
  } else if (OB_FAIL(value.assign(log_restore_source))) {
    LOG_WARN("fail to assign value", KR(ret), K(log_restore_source));
  } else if (OB_FAIL(config_parser_mgr.init(name, value, gen_user_tenant_id(tenant_id)))) {
    LOG_WARN("fail to init backup config parser mgr", KR(ret), K(name), K(value), K(tenant_id));
    // TODO use the interface without rpc_proxy_
  } else if (OB_FAIL(config_parser_mgr.update_inner_config_table(*rpc_proxy_, trans))) {
    LOG_WARN("fail to update inner config table", KR(ret), K(name), K(value));
  }
  return ret;
}

int ObTenantDDLService::init_tenant_schema(
    const obrpc::ObCreateTenantArg &create_tenant_arg,
    const ObTenantSchema &tenant_schema,
    common::ObIArray<ObTableSchema> &tables)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  TIMEGUARD_INIT(init_tenant_schema, 8_s);
  const uint64_t SCHEMA_VERSION_MAX_COUNT = 100000; // 10w
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("[CREATE_TENANT] STEP 2.5. start init tenant schemas", K(tenant_id));
  ObDDLSQLTransaction trans(schema_service_, true, true, false, false);
  const int64_t init_schema_version = tenant_schema.get_schema_version();
  const int64_t refreshed_schema_version = 0;
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (OB_FAIL(ddl_trans_controller_->reserve_schema_version(tenant_id, SCHEMA_VERSION_MAX_COUNT))) {
    LOG_WARN("failed to reserve schema version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id, refreshed_schema_version))) {
    LOG_WARN("fail to start trans", KR(ret), K(tenant_id));
  } else {
    HEAP_VAR(ObSysVariableSchema, sys_variable) {
      ObSchemaService *schema_service_impl = schema_service_->get_schema_service();
      ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
      if (OB_ISNULL(schema_service_impl)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pointer is null", KR(ret), KP(schema_service_impl));
      } else if (CLICK_FAIL(create_sys_table_schemas(ddl_operator, trans, tables))) {
        LOG_WARN("fail to create sys tables", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_service_impl->gen_new_schema_version(
              tenant_id, init_schema_version, new_schema_version))) {
        LOG_WARN("failed to gen_new_schema_version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(init_system_variables(create_tenant_arg, tenant_schema, sys_variable))) {
        LOG_WARN("failed to generate tenant sys variable", KR(ret), K(create_tenant_arg), K(tenant_schema));
      } else if (CLICK_FAIL(ddl_operator.replace_sys_variable(
              sys_variable, new_schema_version, trans, OB_DDL_ALTER_SYS_VAR))) {
        LOG_WARN("fail to replace sys variable", KR(ret), K(sys_variable));
      } else if (CLICK_FAIL(ddl_operator.init_tenant_schemas(tenant_schema, sys_variable, trans))) {
        LOG_WARN("init tenant env failed", KR(ret), K(tenant_schema), K(sys_variable));
      }
    }
  }
  if (trans.is_started()) {
    bool commit = OB_SUCC(ret);
    if (OB_TMP_FAIL(trans.end(commit))) {
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      LOG_WARN("trans end failed", K(commit), K(tmp_ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (CLICK_FAIL(refresh_creating_tenant_schema_(tenant_schema))) {
    LOG_WARN("failed to refresh creating tenant schema", KR(ret), K(tenant_schema));
  }

  FLOG_INFO("[CREATE_TENANT] STEP 2.5. finish init tenant schemas", KR(ret), K(tenant_id),
           "cost", ObTimeUtility::fast_current_time() - start_time);
  return ret;
}

int ObTenantDDLService::create_sys_table_schemas(
    ObDDLOperator &ddl_operator,
    ObMySQLTransaction &trans,
    common::ObIArray<ObTableSchema> &tables)
{
  int ret = OB_SUCCESS;
  uint64_t start_time = ObTimeUtility::current_time();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)
             || OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(sql_proxy), KP_(schema_service));
  } else {
    // persist __all_core_table's schema in inner table, which is only used for sys views.
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
      ObTableSchema &table = tables.at(i);
      const int64_t table_id = table.get_table_id();
      const ObString &table_name = table.get_table_name();
      const ObString *ddl_stmt = NULL;
      bool need_sync_schema_version = !(ObSysTableChecker::is_sys_table_index_tid(table_id) ||
                                        is_sys_lob_table(table_id));
      if (FAILEDx(ddl_operator.create_table(table, trans, ddl_stmt,
                                            need_sync_schema_version,
                                            false /*is_truncate_table*/))) {
        LOG_WARN("add table schema failed", KR(ret), K(table_id), K(table_name));
      } else {
        LOG_INFO("add table schema succeed", K(i), K(table_id), K(table_name));
      }
    }
  }
  FLOG_INFO("[CREATE_TENANT] finish create sys table schemas", KR(ret), "table count", tables.count(),
      "cost", ObTimeUtility::current_time() - start_time);
  return ret;
}

int ObTenantDDLService::init(
    ObUnitManager &unit_mgr,
    ObDDLService &ddl_service,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    obrpc::ObCommonRpcProxy &common_rpc,
    common::ObMySQLProxy &sql_proxy,
    share::schema::ObMultiVersionSchemaService &schema_service,
    share::ObLSTableOperator &lst_operator,
    ObZoneManager &zone_mgr)
{
  int ret = OB_SUCCESS;
  unit_mgr_ = &unit_mgr;
  ddl_service_ = &ddl_service;
  rpc_proxy_ = &rpc_proxy;
  common_rpc_ = &common_rpc;
  sql_proxy_ = &sql_proxy;
  schema_service_ = &schema_service;
  lst_operator_ = &lst_operator;
  ddl_trans_controller_ = &schema_service.get_ddl_trans_controller();
  zone_mgr_ = &zone_mgr;
  inited_ = true;
  stopped_ = false;
  return ret;
}

int ObTenantDDLService::add_extra_tenant_init_config_(
                  const uint64_t tenant_id,
                  common::ObIArray<common::ObConfigPairs> &init_configs)
{
  // For clusters upgraded from a lower version, modifying config default values may affect existing tenants.
  // Therefore, you can add config default values here that should apply to newly created tenants.
  int ret = OB_SUCCESS;
  bool find = false;
  ObString config_name("_parallel_ddl_control");
  ObSqlString config_value;
  ObString config_name_mysql_compatible_dates("_enable_mysql_compatible_dates");
  ObString config_value_mysql_compatible_dates("true");
  if (OB_FAIL(ObParallelDDLControlMode::generate_parallel_ddl_control_config_for_create_tenant(config_value))) {
    LOG_WARN("fail to generate parallel ddl control config value", KR(ret));
  }
  for (int index = 0 ; !find && OB_SUCC(ret) && index < init_configs.count(); ++index) {
    if (tenant_id == init_configs.at(index).get_tenant_id()) {
      find = true;
      common::ObConfigPairs &parallel_table_config = init_configs.at(index);
      if (OB_FAIL(parallel_table_config.add_config(config_name, config_value.string()))) {
        LOG_WARN("fail to add config", KR(ret), K(config_name), K(config_value));
      } else if (OB_FAIL(parallel_table_config.add_config(config_name_mysql_compatible_dates, config_value_mysql_compatible_dates))) {
        LOG_WARN("fail to add config", KR(ret), K(config_name_mysql_compatible_dates), K(config_value_mysql_compatible_dates));
      }
    }
  }
  // ---- Add new tenant init config above this line -----
  // At the same time, to verify modification, you need modify test case tenant_init_config(_oracle).test
  if (OB_SUCC(ret) && !find) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no matched tenant config", KR(ret), K(tenant_id), K(init_configs));
  }
  return ret;

}

/*
 * After the schema is split, there are two scenarios for cross-tenant transactions involved in modify_tenant:
 *
 * Scenario 1: Modify tenant option and system variable at the same time through alter_tenant.
 *  For this scenario, the following restrictions are introduced:
 *  1. It is not allowed to modify tenant option and system variable at the same time.
 *  2. For redundant system variables in the tenant schema and system variable schema,
 *    the synchronization of the two will no longer be guaranteed in the future
 * - read only: For the read only attribute, in order to avoid the failure of inner sql to write user tenant system tables,
 *   inner sql skips the read only check. For external SQL, the read only attribute is subject to the system variable;
 * - name_case_mode: This value is specified when the tenant is created. It is a read only system variable
 *   (lower_case_table_names), and subsequent modifications are not allowed;
 * - ob_compatibility_mode: This value needs to be specified when the tenant is created.
 *   It is a read only system variable and cannot be modified later.

 * Scenario 2:
 * When the tenant locality is modified, the primary_zone is set in database/tablegroup/table
 * and the locality of the tablegroup/table adopts inherited semantics, there will be a scenario
 * where the primary_zone does not match the locality. In this case, it need to modify the primary_zone
 * of each database object under the tenant through DDL.
 *
 * After the schema is split, in order to avoid cross-tenant transactions, the process is split into two transactions.
 * The first transaction modifies the primary_zone of the database object under the tenant, and the second transaction
 * modifies the tenant schema. DDL failed, manual intervention to modify the schema.
 */
int ObTenantDDLService::modify_tenant(const ObModifyTenantArg &arg)
{
  LOG_INFO("receive modify tenant request", K(arg));
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *orig_tenant_schema = NULL;
  const ObString &tenant_name = arg.tenant_schema_.get_tenant_name();
  bool is_restore = false;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init");
  } else if (0 != arg.sys_var_list_.count() &&
             !arg.alter_option_bitset_.is_empty()) {
    // After the schema is split, because __all_sys_variable is split under the tenant, in order to avoid
    // cross-tenant transactions, it is forbidden to modify the tenant option and the system variable at the same time.
    // For this reason, the read only column of the tenant option is no longer maintained,
    // and it is subject to system variables.
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("modify tenant option and system variable at the same time", K(ret), K(arg));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "modify tenant option and system variable at the same time");
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_name, orig_tenant_schema))) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_USER_ERROR(OB_TENANT_NOT_EXIST, tenant_name.length(), tenant_name.ptr());
    LOG_WARN("tenant not exists", K(arg), K(ret));
  } else if (OB_UNLIKELY(NULL == orig_tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_USER_ERROR(OB_TENANT_NOT_EXIST, tenant_name.length(), tenant_name.ptr());
    LOG_WARN("tenant not exists", K(arg), K(ret));
  } else if (FALSE_IT(is_restore = orig_tenant_schema->is_restore())) {
  } else if (!is_restore) {
    // The physical recovery may be in the system table recovery stage, and it is necessary to avoid
    // the situation where SQL cannot be executed and hang
    if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(
                orig_tenant_schema->get_tenant_id(), schema_guard))) {
      LOG_WARN("fail to get schema guard with version in inner table",
               K(ret), "tenant_id",  orig_tenant_schema->get_tenant_id());
    } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_name, orig_tenant_schema))) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_USER_ERROR(OB_TENANT_NOT_EXIST, tenant_name.length(), tenant_name.ptr());
      LOG_WARN("tenant not exists", K(arg), K(ret));
    } else if (OB_UNLIKELY(NULL == orig_tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_USER_ERROR(OB_TENANT_NOT_EXIST, tenant_name.length(), tenant_name.ptr());
      LOG_WARN("tenant not exists", K(arg), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(modify_tenant_inner_phase(arg, orig_tenant_schema, schema_guard, is_restore))) {
    LOG_WARN("modify_tenant_inner_phase fail", K(ret));
  } else if (OB_FAIL(DisasterRecoveryUtils::wakeup_tenant_dr_service(orig_tenant_schema->get_tenant_id()))) {
    LOG_WARN("failed to wakeup dr service", KR(ret), K(arg), K(orig_tenant_schema->get_tenant_id()));
  }
  return ret;
}

/*
 * The locality of tenant is changed in the following function. At present,
 * the locality settings of tenant and table have the following forms:
 * # describe
 *   1. The locality of the tenant must not be empty. The tenant locality upgraded from the version before 1.3 is empty
 *    in the internal table, but when the schema is refreshed, the locality of the tenant will be filled in
 *    as a full-featured replication of each zone.
 *   2. The locality of the table can be empty, which means that the locality of the tenant is inherited.
 *    When the locality of the table is not empty, it means that it does not completely match the locality of the tenant;
 * # locality change semantics
 *   1. When the locality of a tenant changes, the distribution of replications of all tables whose locality is empty
 *    under that tenant will change accordingly. When the locality of the tenant is changed for a table
 *    whose locality is not empty, the distribution of the corresponding replication will not change.
 *   2. Alter table can change the distribution of replications of a table whose locality is not empty.
 * # Mutual restriction of tenant and table locality changes
 *   1. When the old round of tenant locality has not been changed,
 *    the new round of tenant locality changes are not allowed to be executed.
 *   2. When the change of the table whose locality is not empty under tenant is not completed,
 *    the change of tenant locality is not allowed to be executed.
 *   3. When the locality change of tenant is not initiated, the locality change of the table
 *    whose locality is not empty is not allowed to be executed.
 * # Change rules
 *   1. One locality change is only allowed to do one of the operations of adding paxos,
 *    subtracting paxos and paxos type conversion (paxos->paxos), paxos->non_paxos is regarded as subtracting paxos,
 *    non_paxos->paxos is regarded as adding paxos;
 *   2. In a locality change:
 *    2.1. For adding paxos operation, orig_locality's paxos num >= majority(new_locality's paxos num);
 *    2.2. For subtracting paxos operation, new_locality's paxos num >= majority(orig_locality's paxos num);
 *    2.3. For converting paxos type operation, only one paxos type conversion is allowed for one locality change;
 *   3. For replication type conversion, the following constraints need to be met:
 *    3.1. For L-type replications, the replications other than F are not allowed to be converted to L,
 *      and L is not allowed to be converted to other replication types;
 *    3.2. There will be no restrictions for the rest of the situation
 *   4. In particular, in a scenario where only one replication of paxos is added,
 *    paxos num is allowed to go from 1 -> 2, but paxos num is not allowed to go from 2-> 1;
 *   5. Non_paxos replications can occur together with the above changes, and there is no limit to the number.
 * # after 1.4.7.1, the locality form of @region is no longer supported
 */
int ObTenantDDLService::set_new_tenant_options(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const ObModifyTenantArg &arg,
    share::schema::ObTenantSchema &new_tenant_schema,
    const share::schema::ObTenantSchema &orig_tenant_schema,
    AlterLocalityOp &alter_locality_op)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObZone> zones_in_pool;
  alter_locality_op = ALTER_LOCALITY_OP_INVALID;
  if (OB_FAIL(set_raw_tenant_options(arg, new_tenant_schema))) {
    LOG_WARN("fail to set raw tenant options", K(ret));
  } else if (arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::LOCALITY)) {
    common::ObArray<share::schema::ObZoneRegion> zone_region_list;
    AlterLocalityType alter_locality_type = ALTER_LOCALITY_INVALID;
    bool tenant_pools_in_shrinking = false;
    common::ObArray<share::ObResourcePoolName> resource_pool_names;
    if (new_tenant_schema.get_locality_str().empty()) {
      // It is not allowed to change the locality as an inherited attribute
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter locality to empty");
      LOG_WARN("alter locality to empty is not allowed", K(ret));
    } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit_mgr_ is null", K(ret), KP(unit_mgr_));
    } else if (OB_FAIL(unit_mgr_->check_tenant_pools_in_shrinking(
            orig_tenant_schema.get_tenant_id(), tenant_pools_in_shrinking))) {
      LOG_WARN("fail to check tenant pools in shrinking", K(ret));
    } else if (tenant_pools_in_shrinking) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter tenant locality when tenant pool is shrinking");
      LOG_WARN("alter tenant locality not allowed", K(ret), K(orig_tenant_schema));
    } else if (OB_FAIL(get_new_tenant_pool_zone_list(
            arg, new_tenant_schema, resource_pool_names, zones_in_pool, zone_region_list))) {
      LOG_WARN("fail to get new tenant pool zone list", K(ret));
    } else if (OB_FAIL(new_tenant_schema.set_locality(arg.tenant_schema_.get_locality_str()))) {
      LOG_WARN("fail to set locality", K(ret));
    } else if (OB_FAIL(parse_and_set_create_tenant_new_locality_options(
            schema_guard, new_tenant_schema, resource_pool_names, zones_in_pool, zone_region_list))) {
      LOG_WARN("fail to parse and set new locality option", K(ret));
    } else if (OB_FAIL(check_locality_compatible_(new_tenant_schema, false /*for_create_tenant*/))) {
      LOG_WARN("fail to check locality with data version", KR(ret), K(new_tenant_schema));
    } else if (OB_FAIL(check_alter_tenant_locality_type(
            schema_guard, orig_tenant_schema, new_tenant_schema, alter_locality_type))) {
      LOG_WARN("fail to check alter tenant locality allowed", K(ret));
    } else if (ALTER_LOCALITY_INVALID == alter_locality_type) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter tenant locality when previous operation is in progress");
      LOG_WARN("alter tenant locality not allowed", K(ret), K(orig_tenant_schema));
    } else if (ROLLBACK_LOCALITY == alter_locality_type) {
      // Roll back the currently ongoing alter locality
      if (OB_FAIL(try_rollback_modify_tenant_locality(
              arg, new_tenant_schema, orig_tenant_schema, zones_in_pool,
              zone_region_list, alter_locality_op))) {
        LOG_WARN("fail to try rollback modify tenant locality",
                 K(ret), K(new_tenant_schema), K(orig_tenant_schema));
      } else {} // no more to do
    } else if (TO_NEW_LOCALITY == alter_locality_type) {
      if (arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::FORCE_LOCALITY)) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("only locality rollback can be forced", KR(ret), K(arg));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "only locality rollback can be forced, "
            "forcing to be in a new locality is"); // forcing to be in a new locality is not allowed
      }
      if (FAILEDx(try_modify_tenant_locality(
              arg, new_tenant_schema, orig_tenant_schema, zones_in_pool,
              zone_region_list, alter_locality_op))) {
        LOG_WARN("fail to try modify tenant locality",
                 K(ret), K(new_tenant_schema), K(zones_in_pool));
      } else {} // no more to do
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected alter locality type", K(ret), K(alter_locality_type));
    }
    if (OB_SUCC(ret)) {
      common::ObArray<share::ObResourcePoolName> pool_names;
      if (OB_UNLIKELY(NULL == unit_mgr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit_mgr_ is null", K(ret), KP(unit_mgr_));
      } else if (arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::RESOURCE_POOL_LIST)) {
        ret = get_pools(arg.pool_list_, pool_names);
      } else {
        ret = unit_mgr_->get_pool_names_of_tenant(new_tenant_schema.get_tenant_id(), pool_names);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to get pool names", K(ret));
      } else if (OB_FAIL(check_pools_unit_num_enough_for_schema_locality(
              pool_names, schema_guard, new_tenant_schema))) {
        LOG_WARN("pools unit num is not enough for locality", K(ret));
      } else {} // no more to do
    }
  } else {} // locality do not changed, do nothing
  LOG_DEBUG("set new tenant options", K(arg), K(new_tenant_schema), K(orig_tenant_schema));
  return ret;
}

/*
 * The locality change currently allows the following types of transformations:
 * 1. Increase the locality of @zone; for example "F@zone1"-->"F@zone1,F@zone2"
 * 2. Delete the locality of @zone; for example "F@zone1,F@zone2"-->"F@zone1"
 * 3. Modify the locality of @zone; for example "F@zone1"-->"L@zone1"
 *
 * The change of locality needs to meet the following restrictions at the same time:
 * 1. One locality change is only allowed to do one of the operations of
 *  adding paxos, subtracting paxos and paxos type conversion (paxos->paxos),
 * 2. In a locality change:
 *   2.1. for adding paxos operation, orig_locality's paxos num >= majority(new_locality's paxos num);
 *   2.2. for subtracting paxos operation, new_locality's paxos num >= majority(orig_locality's paxos num);
 *   2.3. for converting operation, only one paxos type conversion is allowed for one locality change;
 * 3. For replication type conversion, the following constraints need to be met:
 *   3.1. For L-type replications, the replications other than F are not allowed to be converted to L,
 *    and L is not allowed to be converted to other replication types;
 *   3.2 There will be no restrictions for the rest of the situation
 * In particular, in a scenario where only one replicaiton of paxos is added,
 * paxos num is allowed to go from 1 -> 2, but paxos num is not allowed to go from 2-> 1;
 *
 * for example
 * 1. F@z1,F@z2,F@z3 -> F@z1,L@z3,F@z4 : z3 has done a paxos type conversion, adding F to z4, and subtracting F from z2,
 *  which does not meet condition 1;
 * 2. F@z1,F@z2,R@z3 -> F@z1,F@z2,F@z3,F@z4 : z3 and z4 plus F, does not meet condition 2.1
 * 3. F@z1,F@z2,F@z3,F@z4 -> F@z1,F@z2,R@z3 : As z3 and z4 minus F, condition 2.2 is not met
 * 4. F@z1,F@z2,F@z3,F@z4,F@z5 -> F@z1,F@z2,F@z3,L@z4,L@z5 : Both z4 and z5 have done paxos type conversion
 *  and do not meet condition 2.3
 * 5. F@z1,F@z2,R@z3 -> F@z1,F@z2,L@z3 : do not meet condition 3.1
 * 6. F@z1 -> F@z1,F@z2 : Meet special rules
 * 7. F@z1 -> F@z2,F@z3 : Subtract F, add two F, does not meet special rules
 *
 */
int ObTenantDDLService::try_modify_tenant_locality(
    const ObModifyTenantArg &arg,
    share::schema::ObTenantSchema &new_tenant_schema,
    const share::schema::ObTenantSchema &orig_tenant_schema,
    const common::ObIArray<common::ObZone> &zones_in_pool,
    const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list,
    AlterLocalityOp &alter_locality_op)
{
  int ret = OB_SUCCESS;
  UNUSED(zones_in_pool);
  UNUSED(arg);
  alter_locality_op = ALTER_LOCALITY_OP_INVALID;
  // after 1.4.7.1, The locality writing method of @region is not supported, only the scenario of @zone is considered here
  ObArray<AlterPaxosLocalityTask> alter_paxos_tasks;
  ObArray<share::ObZoneReplicaNumSet> pre_zone_locality;
  common::ObArray<share::ObZoneReplicaAttrSet> cur_zone_locality;
  const ObString &previous_locality = orig_tenant_schema.get_locality_str();
  if (!orig_tenant_schema.get_previous_locality_str().empty()) {
    // Defensive check, go to this branch, orig_tenant_schema previous locality should be empty
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("previous locality is not empty", K(ret),
             "pre_locality", orig_tenant_schema.get_previous_locality_str());
  } else if (OB_FAIL(new_tenant_schema.get_zone_replica_attr_array(cur_zone_locality))) {
    LOG_WARN("fail to get zone replica attr array", K(ret));
  } else {
    ObLocalityDistribution locality_dist;
    int64_t pre_paxos_num = 0;  // not used
    int64_t cur_paxos_num = 0;  // not used
    bool non_paxos_locality_modified = false;
    if (OB_FAIL(locality_dist.init())) {
      LOG_WARN("fail to init locality dist", K(ret));
    } else if (OB_FAIL(locality_dist.parse_locality(
            previous_locality, zones_in_pool, &zone_region_list))) {
      LOG_WARN("fail to parse locality", K(ret));
    } else if (OB_FAIL(locality_dist.get_zone_replica_attr_array(pre_zone_locality))) {
      LOG_WARN("fail to get zone region replica num array", K(ret));
    } else if (OB_FAIL(ObLocalityCheckHelp::check_alter_locality(
            pre_zone_locality, cur_zone_locality,
            alter_paxos_tasks, non_paxos_locality_modified,
            pre_paxos_num, cur_paxos_num, new_tenant_schema.get_arbitration_service_status()))) {
      LOG_WARN("fail to check and get paxos replica task",
               K(ret), K(pre_zone_locality), K(cur_zone_locality), "arbitration service status",
               new_tenant_schema.get_arbitration_service_status());
    } else if (0 < alter_paxos_tasks.count()
               || non_paxos_locality_modified) {
      if (OB_FAIL(new_tenant_schema.set_previous_locality(
              orig_tenant_schema.get_locality_str()))) {
        LOG_WARN("fail to set previous locality", K(ret));
      } else {
        alter_locality_op = ALTER_LOCALITY;
      }
    } else {
      alter_locality_op = NOP_LOCALITY_OP;
    }
  }
  return ret;
}

int ObTenantDDLService::try_rollback_modify_tenant_locality(
    const obrpc::ObModifyTenantArg &arg,
    share::schema::ObTenantSchema &new_schema,
    const share::schema::ObTenantSchema &orig_schema,
    const common::ObIArray<common::ObZone> &zones_in_pool,
    const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list,
    AlterLocalityOp &alter_locality_op)
{
  int ret = OB_SUCCESS;
  UNUSED(arg);
  alter_locality_op = ALTER_LOCALITY_OP_INVALID;
  ObArray<AlterPaxosLocalityTask> alter_paxos_tasks;
  ObArray<share::ObZoneReplicaNumSet> pre_zone_locality;
  common::ObArray<share::ObZoneReplicaAttrSet> cur_zone_locality;
  const ObString &previous_locality = orig_schema.get_locality_str();
  if (new_schema.get_locality_str().empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant locality", K(ret));
  } else if (orig_schema.get_previous_locality_str() != new_schema.get_locality_str()) {
    MODIFY_LOCALITY_NOT_ALLOWED();
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter tenant locality when the previous operation is in progress");
  } else if (OB_FAIL(new_schema.get_zone_replica_attr_array(cur_zone_locality))) {
    LOG_WARN("fail to get zone replica attr array", K(ret));
  } else {
    // In the following two cases, locality rollback will not succeed, so check is needed:
    // 1.Since the implementation is not yet supported, the two-way conversion between types is not supported,
    //  for example, currently supports F->L, but does not support L->F
    // 2. Support paxos member number 1->2, but does not support paxos member number 2->1
    ObLocalityDistribution locality_dist;
    int64_t pre_paxos_num = 0;  // not used
    int64_t cur_paxos_num = 0;  // not used
    bool non_paxos_locality_modified = false;
    if (OB_FAIL(locality_dist.init())) {
      LOG_WARN("fail to init locality dist", K(ret));
    } else if (OB_FAIL(locality_dist.parse_locality(
            previous_locality, zones_in_pool, &zone_region_list))) {
      LOG_WARN("fail to parse locality", K(ret));
    } else if (OB_FAIL(locality_dist.get_zone_replica_attr_array(pre_zone_locality))) {
      LOG_WARN("fail to get zone region replica num array", K(ret));
    } else if (OB_FAIL(ObLocalityCheckHelp::check_alter_locality(
            pre_zone_locality, cur_zone_locality,
            alter_paxos_tasks, non_paxos_locality_modified,
            pre_paxos_num, cur_paxos_num, new_schema.get_arbitration_service_status()))) {
      LOG_WARN("fail to check and get paxos replica task", K(ret), K(pre_zone_locality), K(cur_zone_locality),
               "arbitration service status", new_schema.get_arbitration_service_status());
    } else if (0 < alter_paxos_tasks.count() || non_paxos_locality_modified) {
      if (arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::FORCE_LOCALITY)) {
        if (OB_FAIL(new_schema.set_previous_locality(""))) {
          LOG_WARN("fail to set previous locality", KR(ret));
        }
      } else {
        if (OB_FAIL(new_schema.set_previous_locality(orig_schema.get_locality_str()))) {
          LOG_WARN("fail to set previous locality", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        alter_locality_op = ROLLBACK_ALTER_LOCALITY;
      }
    }
  }
  return ret;
}

int ObTenantDDLService::check_alter_tenant_locality_type(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObTenantSchema &orig_tenant_schema,
    const share::schema::ObTenantSchema &new_tenant_schema,
    AlterLocalityType &alter_locality_type)
{
  int ret = OB_SUCCESS;
  alter_locality_type = ALTER_LOCALITY_INVALID;
  const uint64_t tenant_id = orig_tenant_schema.get_tenant_id();
  const common::ObString &locality = orig_tenant_schema.get_locality_str();
  const common::ObString &previous_locality = orig_tenant_schema.get_previous_locality_str();
  const bool is_restore = new_tenant_schema.is_restore();
  if (OB_UNLIKELY(locality.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, tenant locality is empty", K(ret), K(locality), K(tenant_id));
  } else if (previous_locality.empty()) {
    // previous locality is null, need check locality of tablegroup in tenant
    alter_locality_type = TO_NEW_LOCALITY;
  } else {
    // The previous locality is not empty, the tenant is undergoing locality changes
    // Currently, it is allowed to roll back the locality that is being changed.
    // Rollback is currently defined as being set to be exactly the same as the original locality.
    if (previous_locality != new_tenant_schema.get_locality_str()) {
      alter_locality_type = ALTER_LOCALITY_INVALID;
    } else {
      // locality from 1->2, then rollback 2->1, The bottom layer does not support it, i
      // it should be rejected
      // Check in try_rollback_modify_tenant_locality
      alter_locality_type = ROLLBACK_LOCALITY;
    }
  }
  return ret;
}

int ObTenantDDLService::set_raw_tenant_options(
    const ObModifyTenantArg &arg,
    ObTenantSchema &new_tenant_schema)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema &alter_tenant_schema = arg.tenant_schema_;
  //replace alter options
  for (int32_t i = ObModifyTenantArg::REPLICA_NUM;
       ret == OB_SUCCESS && i < ObModifyTenantArg::MAX_OPTION; ++i) {
    if (arg.alter_option_bitset_.has_member(i)) {
      switch (i) {
        case ObModifyTenantArg::REPLICA_NUM: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("modify replica num is not supported!", K(i), K(ret));
          break;
        }
        case ObModifyTenantArg::CHARSET_TYPE: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("modify replica num is not supported!", K(i), K(ret));
          break;
        }
        case ObModifyTenantArg::COLLATION_TYPE: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("modify replica num is not supported!", K(i), K(ret));
          break;
        }
        case ObModifyTenantArg::PRIMARY_ZONE: {
          new_tenant_schema.set_primary_zone(alter_tenant_schema.get_primary_zone());
          break;
        }
        case ObModifyTenantArg::ZONE_LIST: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("modify zone list is not supported!", K(i), K(ret));
          break;
        }
        case ObModifyTenantArg::RESOURCE_POOL_LIST: {
          break;
        }
        case ObModifyTenantArg::READ_ONLY: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("modify tenant readonly option not supported", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "modify tenant readonly option");
          break;
        }
        case ObModifyTenantArg::COMMENT: {
          new_tenant_schema.set_comment(alter_tenant_schema.get_comment());
          break;
        }
        case ObModifyTenantArg::LOCALITY: {
          // locality change is processed in try_modify_tenant_locality, skip
          break;
        }
        case ObModifyTenantArg::DEFAULT_TABLEGROUP: {
          if (OB_FAIL(new_tenant_schema.set_default_tablegroup_name(
              alter_tenant_schema.get_default_tablegroup_name()))) {
            LOG_WARN("failed to set default tablegroup name", K(ret));
          } else if (OB_FAIL(ddl_service_->set_default_tablegroup_id(new_tenant_schema))) {
            LOG_WARN("failed to set default tablegroup id", K(ret));
          }
          break;
        }
        case ObModifyTenantArg::FORCE_LOCALITY: {
          // do nothing
          break;
        }
        case ObModifyTenantArg::ENABLE_ARBITRATION_SERVICE: {
          // do nothing
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unknown option!", K(i));
        }
      }
    }
  }
  return ret;
}

// What we need to retrieve is the zone of all resource_pools under the tenant's name,
// not just the zone_list of the tenant itself
int ObTenantDDLService::get_new_tenant_pool_zone_list(
    const ObModifyTenantArg &arg,
    const share::schema::ObTenantSchema &tenant_schema,
    common::ObIArray<share::ObResourcePoolName> &resource_pool_names,
    common::ObIArray<common::ObZone> &zones_in_pool,
    common::ObIArray<share::schema::ObZoneRegion> &zone_region_list)
{
  int ret = OB_SUCCESS;
  zones_in_pool.reset();
  zone_region_list.reset();
  if (arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::RESOURCE_POOL_LIST)) {
    if (OB_FAIL(get_pools(arg.pool_list_, resource_pool_names))) {
      LOG_WARN("fail to get pools", K(ret), "pool_list", arg.pool_list_);
    } else {} // got pool names, ok
  } else {
    uint64_t tenant_id = tenant_schema.get_tenant_id();
    if (OB_UNLIKELY(NULL == unit_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit_mgr_ is null", K(ret), KP(unit_mgr_));
    } else if (OB_FAIL(unit_mgr_->get_pool_names_of_tenant(tenant_id, resource_pool_names))) {
      LOG_WARN("fail to get pools of tenant", K(ret));
    } else {} // got pool names, ok
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_zones_of_pools(resource_pool_names, zones_in_pool))) {
    LOG_WARN("fail to get zones of pools", K(ret));
  } else if (OB_FAIL(construct_zone_region_list(zone_region_list, zones_in_pool))) {
    LOG_WARN("fail to construct zone region list", K(ret));
  } else {} // no more to do
  return ret;
}

int ObTenantDDLService::get_zones_of_pools(
    const common::ObIArray<share::ObResourcePoolName> &resource_pool_names,
    common::ObIArray<common::ObZone> &zones_in_pool)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObZone> temp_zones;
  zones_in_pool.reset();
  if (OB_UNLIKELY(resource_pool_names.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "resource pool count", resource_pool_names.count());
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit mgr is null", K(ret), KP(unit_mgr_));
  } else if (OB_FAIL(unit_mgr_->get_zones_of_pools(resource_pool_names, temp_zones))) {
    LOG_WARN("get zones of pools failed", K(ret), K(resource_pool_names));
  } else if (temp_zones.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty zone array", K(ret));
  } else {
    lib::ob_sort(temp_zones.begin(), temp_zones.end());
    FOREACH_X(zone, temp_zones, OB_SUCC(ret)) {
      if (OB_ISNULL(zone)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone is null", K(ret));
      } else if (0 == zones_in_pool.count()
          || zones_in_pool.at(zones_in_pool.count() - 1) != *zone) {
        if (OB_FAIL(zones_in_pool.push_back(*zone))) {
          LOG_WARN("fail to push back", K(ret));
        } else {}
      } else {} // duplicated zone, no need to push into
    }
  }
  return ret;
}

int ObTenantDDLService::get_tenant_pool_zone_list(
    const share::schema::ObTenantSchema &tenant_schema,
    common::ObIArray<common::ObZone> &zones_in_pool)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObResourcePoolName> resource_pool_names;
  zones_in_pool.reset();
  uint64_t tenant_id = tenant_schema.get_tenant_id();
  if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ is null", K(ret), KP(unit_mgr_));
  } else if (OB_FAIL(unit_mgr_->get_pool_names_of_tenant(tenant_id, resource_pool_names))) {
    LOG_WARN("fail to get pools of tenant", K(ret));
  } else if (OB_FAIL(get_zones_of_pools(resource_pool_names, zones_in_pool))) {
    LOG_WARN("fail to get zones of pools", K(ret));
  } else {} // no more to do
  return ret;
}

int ObTenantDDLService::check_alter_tenant_replica_options(
    const obrpc::ObModifyTenantArg &arg,
    share::schema::ObTenantSchema &new_tenant_schema,
    const share::schema::ObTenantSchema &orig_tenant_schema,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObArray<ObZone> zone_list;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", K(ret));
  } else if (OB_FAIL(new_tenant_schema.get_zone_list(zone_list))) {
    LOG_WARN("fail to get zone list", K(ret));
  } else if (OB_FAIL(check_alter_schema_replica_options(
          arg.alter_option_bitset_.has_member(ObModifyTenantArg::PRIMARY_ZONE),
          new_tenant_schema, orig_tenant_schema, zone_list, schema_guard))) {
    LOG_WARN("fail to check replica options", K(ret));
  } else {} // no more
  if (OB_OP_NOT_ALLOW == ret
      && arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::FORCE_LOCALITY)) {
    ret = OB_SUCCESS;
    LOG_WARN("FORCE ROLLBACK LOCALITY should skip all checks", KR(ret), K(arg),
        K(orig_tenant_schema), K(new_tenant_schema));
  }
  return ret;
}

int ObTenantDDLService::check_alter_schema_replica_options(
    const bool alter_primary_zone,
    share::schema::ObTenantSchema &new_schema,
    const share::schema::ObTenantSchema &orig_schema,
    common::ObArray<common::ObZone> &zone_list,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", K(ret));
  } else if (!ObPrimaryZoneUtil::is_specific_primary_zone(
        new_schema.get_primary_zone())) {
    const common::ObArray<ObZoneScore> empty_pz_array;
    if (OB_FAIL(new_schema.set_primary_zone_array(empty_pz_array))) {
      LOG_WARN("fail to set primary zone array empty", K(ret));
    } else if (OB_FAIL(check_empty_primary_zone_locality_condition(
            new_schema, zone_list, schema_guard))) {
      LOG_WARN("fail to check empty primary zone locality condition", K(ret));
    }
  } else {
    if (OB_FAIL(check_schema_zone_list(zone_list))) {
      LOG_WARN("fail to check schema zone list", K(ret), K(zone_list));
    } else if (alter_primary_zone) {
      if (OB_FAIL(check_and_set_primary_zone(new_schema, zone_list, schema_guard))) {
        LOG_WARN("fail to check and set primary zone", K(ret));
      }
    } else {
      // Currently alter tenant/database/table may cause zone_list to change
      // We need to remove the zones that are not in the zone_list and in the primary zone
      if (OB_FAIL(trim_and_set_primary_zone(new_schema, orig_schema, zone_list, schema_guard))) {
        LOG_WARN("fail to trim and set primary zone", K(ret));
      }
    }
  }

  // retrun OB_OP_NOT_ALLOW if first_primary_zone changed when tenant rebalance is disabled.
  if (FAILEDx(check_alter_tenant_when_rebalance_is_disabled_(orig_schema, new_schema))) {
    LOG_WARN("failed to check alter tenant when rebalance is disabled", KR(ret), K(orig_schema), K(new_schema));
  }

  if (OB_SUCC(ret)) {
    int64_t paxos_num = 0;
    if (OB_FAIL(new_schema.get_paxos_replica_num(schema_guard, paxos_num))) {
      LOG_WARN("fail to get paxos replica num", K(ret));
    } else if (paxos_num <= 0 || paxos_num > common::OB_MAX_MEMBER_NUMBER) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid paxos replica num", K(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality paxos replica num");
    } else {} // good
  }
  return ret;
}

// alter tenant with primary_zone changed is not allowed when tenant rebalance is disabled.
int ObTenantDDLService::check_alter_tenant_when_rebalance_is_disabled_(
    const share::schema::ObTenantSchema &orig_tenant_schema,
    const share::schema::ObTenantSchema &new_tenant_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = orig_tenant_schema.get_tenant_id();
  ObArray<ObZone> orig_first_primary_zone;
  ObArray<ObZone> new_first_primary_zone;
  bool is_allowed = true;
  bool is_first_primary_zone_changed = false;
  if (OB_UNLIKELY(orig_tenant_schema.get_tenant_id() != new_tenant_schema.get_tenant_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input tenant schema", KR(ret), K(orig_tenant_schema), K(new_tenant_schema));
  } else if (is_sys_tenant(tenant_id)) {
    // primary_zone and locality changes in sys tenant do not cause rebalance,
    // so alter sys tenant is not controlled by enable_rebalance.
    is_allowed = true;
  } else if (ObShareUtil::is_tenant_enable_rebalance(tenant_id)) {
    is_allowed = true;
  } else if (OB_FAIL(ObRootUtils::is_first_priority_primary_zone_changed(
      orig_tenant_schema,
      new_tenant_schema,
      orig_first_primary_zone,
      new_first_primary_zone,
      is_first_primary_zone_changed))) {
    LOG_WARN("fail to check is_first_priority_primary_zone_changed", KR(ret), K(orig_tenant_schema), K(new_tenant_schema));
  } else if (is_first_primary_zone_changed) {
    is_allowed = false;
  }
  if (OB_SUCC(ret) && !is_allowed) {
    ObSqlString orig_str;
    ObSqlString new_str;
    ARRAY_FOREACH(orig_first_primary_zone, idx) {
      if (OB_FAIL(orig_str.append_fmt(0 == idx ? "%s" : ",%s", orig_first_primary_zone.at(idx).ptr()))) {
        LOG_WARN("append fmt failed", KR(ret), K(orig_first_primary_zone), K(idx));
      }
    }
    ARRAY_FOREACH(new_first_primary_zone, idx) {
      if (OB_FAIL(new_str.append_fmt(0 == idx ? "%s" : ",%s", new_first_primary_zone.at(idx).ptr()))) {
        LOG_WARN("append fmt failed", KR(ret), K(new_first_primary_zone), K(idx));
      }
    }
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("enable_rebalance is disabled, alter tenant with primary zone changed not allowed", KR(ret),
        K(tenant_id), K(orig_first_primary_zone), K(new_first_primary_zone));
    char err_msg[DEFAULT_BUF_LENGTH];
    (void)snprintf(err_msg, sizeof(err_msg),
        "Tenant (%lu) Primary Zone with the first priority will be changed from '%s' to '%s', "
        "but tenant 'enable_rebalance' is disabled, alter tenant", tenant_id, orig_str.ptr(), new_str.ptr());
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
  }
  return ret;
}

int ObTenantDDLService::force_set_locality(
    ObSchemaGetterGuard &schema_guard,
    ObTenantSchema &new_tenant)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObZone> zones_in_pool;
  common::ObArray<share::schema::ObZoneRegion> zone_region_list;
  common::ObArray<share::ObResourcePoolName> resource_pool_names;
  uint64_t tenant_id = new_tenant.get_tenant_id();
  const ObTenantSchema *orig_meta_tenant = NULL;
  ObTenantSchema new_meta_tenant;

  obrpc::ObModifyTenantArg dummy_arg;
  if (OB_FAIL(get_new_tenant_pool_zone_list(dummy_arg, new_tenant, resource_pool_names,
                                            zones_in_pool, zone_region_list))) {
    LOG_WARN("fail to get new tenant pool zone list", KR(ret), K(new_tenant));
  } else if (OB_FAIL(parse_and_set_create_tenant_new_locality_options(
                     schema_guard, new_tenant, resource_pool_names,
                     zones_in_pool, zone_region_list))) {
    LOG_WARN("fail to parse and set new locality option", KR(ret), K(new_tenant));
  } else {
    // deal with meta tenant related to a certain user tenant
    if (is_user_tenant(tenant_id)) {
      if (OB_FAIL(schema_guard.get_tenant_info(gen_meta_tenant_id(tenant_id), orig_meta_tenant))) {
        LOG_WARN("fail to get meta tenant schema", KR(ret), "meta_tenant_id", gen_meta_tenant_id(tenant_id));
      } else if (OB_ISNULL(orig_meta_tenant)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("meta tenant not exist", KR(ret), "meta_tenant_id", gen_meta_tenant_id(tenant_id));
      } else if (OB_FAIL(new_meta_tenant.assign(*orig_meta_tenant))) {
        LOG_WARN("fail to assgin meta tenant schema", KR(ret), KPC(orig_meta_tenant));
      } else if (OB_FAIL(new_meta_tenant.set_locality(new_tenant.get_locality_str()))) {
        LOG_WARN("fail to set locality", KR(ret), "locality str", new_tenant.get_locality_str());
      } else if (OB_FAIL(new_meta_tenant.set_previous_locality(ObString("")))) {
        LOG_WARN("fail to reset meta tenant previous locality", KR(ret));
      } else if (OB_FAIL(parse_and_set_create_tenant_new_locality_options(
                     schema_guard, new_meta_tenant, resource_pool_names,
                     zones_in_pool, zone_region_list))) {
        LOG_WARN("fail to parse and set meta tenant new locality option", KR(ret), K(new_meta_tenant));
      }
    }
    if (OB_SUCC(ret)) {
      ObDDLSQLTransaction trans(schema_service_);
      ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
      int64_t refreshed_schema_version = 0;
      if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, refreshed_schema_version))) {
        LOG_WARN("failed to get tenant schema version", KR(ret));
      } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
        LOG_WARN("start transaction failed", KR(ret), K(refreshed_schema_version));
      } else if (OB_FAIL(ddl_operator.alter_tenant(new_tenant, trans))) {
        LOG_WARN("failed to alter tenant", KR(ret));
      } else if (is_user_tenant(tenant_id) && OB_FAIL(ddl_operator.alter_tenant(new_meta_tenant, trans))) {
        LOG_WARN("failed to alter meta tenant", KR(ret));
      }
      if (trans.is_started()) {
        int temp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
          ret = (OB_SUCC(ret)) ? temp_ret : ret;
        }
      }
      // publish schema
      if (OB_SUCC(ret) && OB_FAIL(publish_schema(OB_SYS_TENANT_ID))) {
        LOG_WARN("publish schema failed, ", K(ret));
      }
    }
  }
  return ret;
}

template<typename SCHEMA>
int ObTenantDDLService::trim_and_set_primary_zone(
    SCHEMA &new_schema,
    const SCHEMA &orig_schema,
    const common::ObIArray<common::ObZone> &zone_list,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::schema::ObZoneScore> new_zone_score_array;
  const ObIArray<ObZoneScore> &orig_zone_score_array = orig_schema.get_primary_zone_array();
  common::ObZone zone;
  for (int64_t i = 0; i < orig_zone_score_array.count() && OB_SUCC(ret); ++i) {
    zone.reset();
    if (OB_FAIL(zone.assign(orig_zone_score_array.at(i).zone_.ptr()))) {
      LOG_WARN("fail to assign zone", K(ret));
    } else if (!has_exist_in_array(zone_list, zone)) {
      // No longer in zone_list, remove this zone
    } else if (OB_FAIL(new_zone_score_array.push_back(orig_zone_score_array.at(i)))) {
      LOG_WARN("fail to push back", K(ret));
    } else {} // no more to do
  }
  if (new_zone_score_array.count() <= 0) {
    const common::ObArray<ObZoneScore> empty_pz_array;
    if (OB_FAIL(new_schema.set_primary_zone(ObString::make_string("")))) {
      LOG_WARN("fail to set primary zone", K(ret));
    } else if (OB_FAIL(new_schema.set_primary_zone_array(empty_pz_array))) {
      LOG_WARN("fail to set primary zone array empty", K(ret));
    } else if (OB_FAIL(check_empty_primary_zone_locality_condition(
            new_schema, zone_list, schema_guard))) {
      LOG_WARN("fail to check empty primary zone locality condition", K(ret));
    } else {} // no more to do
  } else {
    lib::ob_sort(new_zone_score_array.begin(), new_zone_score_array.end());
    SMART_VAR(char[MAX_ZONE_LIST_LENGTH], primary_zone_str) {
    if (OB_FAIL(format_primary_zone_from_zone_score_array(
            new_zone_score_array, primary_zone_str, MAX_ZONE_LIST_LENGTH))) {
      LOG_WARN("fail to construct primary zone from zone score array", K(ret));
    } else if (OB_FAIL(new_schema.set_primary_zone(ObString::make_string(primary_zone_str)))) {
      LOG_WARN("fail to set primary zone", K(ret));
    } else if (OB_FAIL(check_and_set_primary_zone(new_schema, zone_list, schema_guard))) {
      LOG_WARN("fail to check and set primary zone", K(ret));
    } else {} // no more to do
    } // end smart var
  }
  return ret;
}

int ObTenantDDLService::format_primary_zone_from_zone_score_array(
    common::ObIArray<share::schema::ObZoneScore> &zone_score_array,
    char *buf,
    int64_t buf_len)
{
  int ret = OB_SUCCESS;
  MEMSET(buf, 0, buf_len);
  if (zone_score_array.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "array num", zone_score_array.count());
  } else {
    int64_t pos = 0;
    bool start_format = false;
    int64_t prev_zone_score = zone_score_array.at(0).score_;
    const char *separator_token = NULL;
    for (int64_t i = 0; i < zone_score_array.count() && OB_SUCC(ret); ++i) {
      ObZoneScore &cur_zone_score = zone_score_array.at(i);
      const bool same_p = (cur_zone_score.score_ == prev_zone_score);
      separator_token = (same_p ? "," : ";");
      if (OB_FAIL(databuff_printf(buf, buf_len, pos,
              "%s", (!start_format ? "" : separator_token)))) {
        LOG_WARN("fail to format separator", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%.*s",
              static_cast<int32_t>(cur_zone_score.zone_.length()),
              cur_zone_score.zone_.ptr()))) {
        LOG_WARN("fail to format zone", K(ret));
      } else {
        start_format = true;
        prev_zone_score = cur_zone_score.score_;
      }
    } // for
  }
  return ret;
}

// not used
// When alter tenant, tenant option and sys variable are both set to readonly,
// the current implementation is based on sys variable
int ObTenantDDLService::update_sys_variables(const common::ObIArray<obrpc::ObSysVarIdValue> &sys_var_list,
    const share::schema::ObSysVariableSchema &orig_sys_variable,
    share::schema::ObSysVariableSchema &new_sys_variable,
    bool &value_changed)
{
  int ret = OB_SUCCESS;
  bool found = false;

  value_changed = false;
  if (!sys_var_list.empty()) {
    const int64_t set_sys_var_count = sys_var_list.count();
    const ObSysVarSchema *sysvar = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < set_sys_var_count; ++i) {
      const obrpc::ObSysVarIdValue &sysvar_value = sys_var_list.at(i);
      /* look ahead to find same variable, if found, jump this action.
         After doing so, only the rightmost set action can be accepted. */
      found = false;
      for (int64_t j = i + 1; OB_SUCC(ret) && j < set_sys_var_count && (!found); ++j) {
        const obrpc::ObSysVarIdValue &tmp_var = sys_var_list.at(j);
        if (sysvar_value.sys_id_ == tmp_var.sys_id_) {
          found = true;
        }
      }
      if (OB_SUCC(ret) && !found) {
        if (OB_FAIL(orig_sys_variable.get_sysvar_schema(sysvar_value.sys_id_, sysvar))) {
          LOG_WARN("failed to get sysvar schema", K(sysvar_value), K(ret));
        } else if (OB_ISNULL(sysvar)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sysvar is null", K(sysvar_value), K(ret));
        } else {
          ObSysVarSchema new_sysvar;
          if (OB_FAIL(new_sysvar.assign(*sysvar))) {
            LOG_WARN("fail to assign sys var schema", KR(ret), KPC(sysvar));
          } else if (SYS_VAR_OB_COMPATIBILITY_MODE
              == ObSysVarFactory::find_sys_var_id_by_name(new_sysvar.get_name())) {
            ret = OB_OP_NOT_ALLOW;
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "change tenant compatibility mode");
          } else if (new_sysvar.is_read_only()) {
            ret = OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR;
            LOG_USER_ERROR(OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR,
                new_sysvar.get_name().length(),
                new_sysvar.get_name().ptr(),
                (int)strlen("read only"),
                "read only");
          } else if (new_sysvar.get_value() != sysvar_value.value_) {
            value_changed = true;
            if (OB_FAIL(new_sysvar.set_value(sysvar_value.value_))) {
              LOG_WARN("failed to set_value", K(ret));
            } else if (OB_FAIL(new_sys_variable.add_sysvar_schema(new_sysvar))) {
              LOG_WARN("failed to add sysvar schema", K(ret));
            } else {
              LOG_DEBUG("succ to update sys value", K(sysvar_value));
              sysvar = NULL;
            }
          }
        }
      }
    }
  }
  return ret;
}

/* long_pool_name_list and short_pool_name_list has sorted
 * in parameter condition:
 *   The length of long_pool_name_list is 1 larger than the length of short_pool_name_list
 * This function has two functions:
 *   1 check whether long_pool_name_list is only one more resource_pool_name than short_pool_name_list
 *   2 Put this extra resource_pool_name into the diff_pools array.
 */
int ObTenantDDLService::cal_resource_pool_list_diff(
    const common::ObIArray<ObResourcePoolName> &long_pool_name_list,
    const common::ObIArray<ObResourcePoolName> &short_pool_name_list,
    common::ObIArray<ObResourcePoolName> &diff_pools)
{
  int ret = OB_SUCCESS;
  if (long_pool_name_list.count() != short_pool_name_list.count() + 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(long_pool_name_list), K(short_pool_name_list));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool list");
  } else {
    diff_pools.reset();
    int64_t index = 0;
    for (; OB_SUCC(ret) && index < short_pool_name_list.count(); ++index) {
      if (short_pool_name_list.at(index) != long_pool_name_list.at(index)) {
        if (OB_FAIL(diff_pools.push_back(long_pool_name_list.at(index)))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
          break; // got it, exit loop
        }
      } else {} // still the same, go on next
    }
    if (OB_FAIL(ret)) {
    } else if (index >= short_pool_name_list.count()) {
      // The pool of diff is the last element of long_pool_name_list
      if (index >= long_pool_name_list.count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid resource pool list", K(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool list");
      } else if (OB_FAIL(diff_pools.push_back(long_pool_name_list.at(index)))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    } else {
      // The pool of diff is not the last element of long_pool_name_list. The diff has been found in the previous for loop.
      // It is necessary to further check whether short_pool_name_list and long_pool_name_list are consistent after index.
      for (; OB_SUCC(ret) && index < short_pool_name_list.count(); ++index) {
        if (index + 1 >= long_pool_name_list.count()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid resource pool list", K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool list");
        } else if (short_pool_name_list.at(index) != long_pool_name_list.at(index + 1)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid resource pool list", K(ret), K(short_pool_name_list), K(long_pool_name_list));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool list");
        } else {} // go on next
      }
    }
  }
  return ret;
}

int ObTenantDDLService::check_grant_pools_permitted(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const common::ObIArray<share::ObResourcePoolName> &to_be_grant_pools,
    const share::schema::ObTenantSchema &tenant_schema,
    bool &is_permitted)
{
  int ret = OB_SUCCESS;
  UNUSED(schema_guard);
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", K(ret));
  } else {
    if (OB_UNLIKELY(nullptr == unit_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit mgr ptr is null", K(ret));
    } else if (OB_FAIL(unit_mgr_->check_locality_for_logonly_unit(
            tenant_schema, to_be_grant_pools, is_permitted))) {
      LOG_WARN("fail to check locality for logonly unit", K(ret));
    }
  }
  return ret;
}

int ObTenantDDLService::check_normal_tenant_revoke_pools_permitted(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const common::ObIArray<share::ObResourcePoolName> &new_pool_name_list,
    const share::schema::ObTenantSchema &tenant_schema,
    bool &is_permitted)
{
  int ret = OB_SUCCESS;
  is_permitted = true;
  common::ObArray<common::ObZone> zone_list;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", K(ret));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ ptr is null", K(ret));
  } else if (OB_FAIL(tenant_schema.get_zone_list(zone_list))) {
    LOG_WARN("fail to get zones of pools", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count() && is_permitted; ++i) {
      const common::ObZone &zone = zone_list.at(i);
      int64_t total_unit_num = 0;
      int64_t full_unit_num = 0;
      int64_t logonly_unit_num = 0;
      bool enough = false;
      if (OB_FAIL(unit_mgr_->get_zone_pools_unit_num(
              zone, new_pool_name_list, total_unit_num, full_unit_num, logonly_unit_num))) {
        LOG_WARN("fail to get pools unit num", K(ret));
      } else if (total_unit_num != full_unit_num + logonly_unit_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit num value not match", K(ret),
                 K(total_unit_num), K(full_unit_num), K(logonly_unit_num));
      } else if (!tenant_schema.get_previous_locality_str().empty()) {
        is_permitted = false;
        LOG_USER_ERROR(OB_OP_NOT_ALLOW,
                       "revoking resource pools when tenant in locality modification");
      } else if (OB_FAIL(unit_mgr_->check_schema_zone_unit_enough(
              zone, total_unit_num, full_unit_num, logonly_unit_num,
              tenant_schema, schema_guard, enough))) {
        LOG_WARN("fail to check schema zone unit enough", K(ret));
      } else if (!enough) {
        is_permitted = false;
        LOG_USER_ERROR(OB_OP_NOT_ALLOW,
                       "revoking resource pools with tenant locality on");
      } else { /* good */ }
    }
  }
  return ret;
}

int ObTenantDDLService::check_revoke_pools_permitted(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const common::ObIArray<share::ObResourcePoolName> &new_pool_name_list,
    const share::schema::ObTenantSchema &tenant_schema,
    bool &is_permitted)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", K(ret));
  } else {
    if (OB_FAIL(check_normal_tenant_revoke_pools_permitted(
            schema_guard, new_pool_name_list, tenant_schema, is_permitted))) {
      LOG_WARN("fail to check normal tenant revoke pools permitted", K(ret));
    }
  }
  return ret;
}


/* Modify the internal table related to the resource pool, and calculate the transformation of
 * the resource pool list of the alter tenant at the same time. Currently, only one is allowed to be added,
 * one resource pool is reduced or the resource pool remains unchanged.
 * input:
 *   tenant_id:       tenant_id corresponding to alter tenant
 *   new_pool_list:   The new resource pool list passed in by alter tenant
 * output:
 *   grant:           subtract resource pool: false; add resource pool: true
 *   diff_pools:      the diff from newresource pool list and old resource pool list.
 */
int ObTenantDDLService::modify_and_cal_resource_pool_diff(
    common::ObMySQLTransaction &trans,
    common::ObIArray<uint64_t> &new_ug_id_array,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObTenantSchema &new_tenant_schema,
    const common::ObIArray<common::ObString> &new_pool_list,
    bool &grant,
    common::ObIArray<ObResourcePoolName> &diff_pools)
{
  int ret = OB_SUCCESS;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  common::ObArray<ObResourcePoolName> new_pool_name_list;
  common::ObArray<ObResourcePoolName> old_pool_name_list;
  const uint64_t tenant_id = new_tenant_schema.get_tenant_id();
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)
      || OB_UNLIKELY(new_pool_list.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(new_pool_list));
  } else if (OB_FAIL(unit_mgr_->get_pool_names_of_tenant(tenant_id, old_pool_name_list))) {
    LOG_WARN("fail to get pool names of tenant", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_compat_mode(tenant_id, compat_mode))) {
    LOG_WARN("fail to get compat mode", K(ret));
  } else if (OB_UNLIKELY(old_pool_name_list.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old pool name list null", K(ret), K(old_pool_name_list));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < new_pool_list.count(); ++i) {
      if (OB_FAIL(new_pool_name_list.push_back(new_pool_list.at(i).ptr()))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    }
    if (OB_SUCC(ret)) {
      lib::ob_sort(new_pool_name_list.begin(), new_pool_name_list.end());
      lib::ob_sort(old_pool_name_list.begin(), old_pool_name_list.end());
      bool is_permitted = false;
      if (new_pool_name_list.count() == old_pool_name_list.count() + 1) {
        grant = true;
        if (OB_FAIL(cal_resource_pool_list_diff(
                new_pool_name_list, old_pool_name_list, diff_pools))) {
          LOG_WARN("fail to cal resource pool list diff", K(ret));
        } else if (OB_FAIL(check_grant_pools_permitted(
                schema_guard, diff_pools, new_tenant_schema, is_permitted))) {
          LOG_WARN("fail to check grant pools permitted", K(ret));
        } else if (!is_permitted) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("fail to grant pool", K(ret), K(diff_pools));
        } else if (OB_FAIL(unit_mgr_->grant_pools(
                trans, new_ug_id_array, compat_mode, diff_pools, tenant_id,
                false/*is_bootstrap*/, OB_INVALID_TENANT_ID/*source_tenant_id*/,
                true/*check_data_version*/))) {
          LOG_WARN("fail to grant pools", K(ret));
        }
      } else if (new_pool_name_list.count() + 1 == old_pool_name_list.count()) {
        grant = false;
        if (OB_FAIL(cal_resource_pool_list_diff(
                old_pool_name_list, new_pool_name_list, diff_pools))) {
          LOG_WARN("fail to cal resource pool list diff", K(ret));
        } else if (OB_FAIL(check_revoke_pools_permitted(
                schema_guard, new_pool_name_list, new_tenant_schema, is_permitted))) {
          LOG_WARN("fail to check revoke pools permitted", K(ret));
        } else if (!is_permitted) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("revoking resource pools is not allowed", K(ret), K(diff_pools));
        } else if (OB_FAIL(unit_mgr_->revoke_pools(
                trans, new_ug_id_array, diff_pools, tenant_id))) {
          LOG_WARN("fail to revoke pools", K(ret));
        } else {} // no more to do
      } else if (new_pool_name_list.count() == old_pool_name_list.count()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < new_pool_name_list.count(); ++i) {
          if (new_pool_name_list.at(i) != old_pool_name_list.at(i)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", K(ret), K(new_pool_name_list), K(old_pool_name_list));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool list");
          }
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(new_pool_name_list), K(old_pool_name_list));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool list");
      }
    }
    LOG_INFO("cal resource pool list result",
             K(new_pool_name_list),
             K(old_pool_name_list),
             K(diff_pools),
             K(grant));
  }
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
int ObTenantDDLService::check_tenant_arbitration_service_status_(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const share::ObArbitrationServiceStatus &old_status,
    const share::ObArbitrationServiceStatus &new_status)
{
  int ret = OB_SUCCESS;
  bool is_compatible = false;
  bool can_promote = false;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !old_status.is_valid()
                  || !new_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(old_status), K(new_status));
  } else if (OB_FAIL(ObShareUtil::check_compat_version_for_arbitration_service(tenant_id, is_compatible))) {
    LOG_WARN("fail to check data version", KR(ret), K(tenant_id));
  } else if (!is_compatible) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not change arbitration service status with data version below 4.1", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "data version is below 4.1, change tenant arbitration service status");
  } else if ((new_status.is_enabled() && !old_status.is_enabling())
             || (new_status.is_disabled() && !old_status.is_disabling())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("unexpected status", KR(ret), K(new_status), K(old_status));
  } else if (OB_FAIL(ObArbitrationServiceUtils::check_can_promote_arbitration_service_status(trans, tenant_id, old_status, new_status, can_promote))) {
    LOG_WARN("fail to check whether can enable arb service", KR(ret), K(tenant_id), K(old_status), K(new_status));
  } else if (!can_promote) {
    // LOG_USER_ERROR will raise inside check_can_promote_arbitration_service_status
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("promote conditions not satisfied", KR(ret), K(tenant_id), K(old_status), K(new_status), K(can_promote));
  }
  return ret;
}
#endif

int ObTenantDDLService::try_alter_meta_tenant_schema(
    ObDDLOperator &ddl_operator,
    const obrpc::ObModifyTenantArg &arg,
    common::ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &sys_schema_guard,
    const share::schema::ObTenantSchema &user_tenant_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = user_tenant_schema.get_tenant_id();
  // only locality and primary_zone can be modified in meta_tenant
  bool meta_tenant_has_option_changed = arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::LOCALITY)
                                        || arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::PRIMARY_ZONE)
                                        || arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::ENABLE_ARBITRATION_SERVICE);
  if (is_meta_tenant(tenant_id) || is_sys_tenant(tenant_id)) {
    /* bypass, when this is a meta tenant,
     * alter meta tenant shall be invoked in the upper layer
     */
  } else if (!meta_tenant_has_option_changed) {
    // do nothing
    LOG_INFO("nothing changed to this tenant", KR(ret), K(arg), K(tenant_id));
  } else {
    const share::schema::ObTenantSchema *meta_tenant_schema = nullptr;
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    share::schema::ObTenantSchema new_meta_tenant_schema;
    if (OB_FAIL(sys_schema_guard.get_tenant_info(
            meta_tenant_id,
            meta_tenant_schema))) {
      LOG_WARN("fail to get tenant schema", KR(ret), K(meta_tenant_id));
    } else if (OB_UNLIKELY(nullptr == meta_tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta_tenant_schema ptr is null", KR(ret),
               K(meta_tenant_id), KP(meta_tenant_schema));
    } else if (OB_FAIL(new_meta_tenant_schema.assign(
            *meta_tenant_schema))) {
      LOG_WARN("fail to assign new meta tenant schema", KR(ret));
    } else {
      if (arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::LOCALITY)) {
        common::ObArray<common::ObZone> user_zone_list;
        if (OB_FAIL(new_meta_tenant_schema.set_previous_locality(
                user_tenant_schema.get_previous_locality()))) {
          LOG_WARN("fail to set previous locality", KR(ret));
        } else if (OB_FAIL(new_meta_tenant_schema.set_locality(
                user_tenant_schema.get_locality()))) {
          LOG_WARN("fail to set locality", KR(ret));
        } else if (OB_FAIL(user_tenant_schema.get_zone_list(user_zone_list))) {
          LOG_WARN("fail to get zone list from user schema", KR(ret), K(user_zone_list));
        } else if (OB_FAIL(new_meta_tenant_schema.set_zone_list(user_zone_list))) {
          LOG_WARN("fail to set zone list", KR(ret));
        } else if (OB_FAIL(new_meta_tenant_schema.set_primary_zone(user_tenant_schema.get_primary_zone()))) {
          LOG_WARN("fail to set primary zone", KR(ret), "primary_zone", user_tenant_schema.get_primary_zone());
        }
      } else if (arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::PRIMARY_ZONE)) {
        if (OB_FAIL(new_meta_tenant_schema.set_primary_zone(user_tenant_schema.get_primary_zone()))) {
          LOG_WARN("fail to set primary zone", KR(ret), "primary_zone", user_tenant_schema.get_primary_zone());
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::ENABLE_ARBITRATION_SERVICE)) {
      new_meta_tenant_schema.set_arbitration_service_status(user_tenant_schema.get_arbitration_service_status());
    }

    if (FAILEDx(ddl_operator.alter_tenant(
                    new_meta_tenant_schema,
                    trans,
                    nullptr /* do not record ddl stmt str */))) {
      LOG_WARN("fail to alter meta tenant locality", KR(ret), K(meta_tenant_id));
    }
  }
  return ret;
}

int ObTenantDDLService::record_tenant_locality_event_history(
    const AlterLocalityOp &alter_locality_op,
    const obrpc::ObModifyTenantArg &arg,
    const share::schema::ObTenantSchema &tenant_schema,
	  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  int64_t job_id = 0;
  ObRsJobType job_type = JOB_TYPE_INVALID;
  if (ALTER_LOCALITY_OP_INVALID == alter_locality_op) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid alter locality op", K(ret), K(alter_locality_op));
  } else {
    int64_t job_id = 0;
    if (OB_FAIL(ObAlterLocalityFinishChecker::find_rs_job(tenant_schema.get_tenant_id(), job_id, trans))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to find rs job", K(ret), "tenant_id", tenant_schema.get_tenant_id());
      }
    } else {
      ret = RS_JOB_COMPLETE(job_id, OB_CANCELED, trans);
      FLOG_INFO("[ALTER_TENANT_LOCALITY NOTICE] cancel an old inprogress rs job due to rollback", KR(ret),
          "tenant_id", tenant_schema.get_tenant_id(), K(job_id));
    }
  }
  if (OB_SUCC(ret) && ROLLBACK_ALTER_LOCALITY == alter_locality_op) {
    if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, tenant_data_version))) {
      LOG_WARN("fail to get sys tenant's min data version", KR(ret));
    } else if (tenant_data_version < DATA_VERSION_4_2_1_0) {
      job_type = ObRsJobType::JOB_TYPE_ROLLBACK_ALTER_TENANT_LOCALITY;
    }
  }
  if (OB_SUCC(ret)) {
    // ALTER_LOCALITY, ROLLBACK_ALTER_LOCALITY(only 4.2), NOP_LOCALITY_OP
    job_type =  ObRsJobType::JOB_TYPE_INVALID == job_type ?
                ObRsJobType::JOB_TYPE_ALTER_TENANT_LOCALITY : job_type;
    const int64_t extra_info_len = common::MAX_ROOTSERVICE_JOB_EXTRA_INFO_LENGTH;
    HEAP_VAR(char[extra_info_len], extra_info) {
      memset(extra_info, 0, extra_info_len);
      int64_t pos = 0;
      if (OB_FAIL(databuff_printf(extra_info, extra_info_len, pos,
              "FROM: '%.*s', TO: '%.*s'", tenant_schema.get_previous_locality_str().length(),
              tenant_schema.get_previous_locality_str().ptr(), tenant_schema.get_locality_str().length(),
              tenant_schema.get_locality_str().ptr()))) {
        LOG_WARN("format extra_info failed", KR(ret), K(tenant_schema));
      } else if (OB_FAIL(RS_JOB_CREATE_WITH_RET(job_id, job_type, trans,
        "tenant_name", tenant_schema.get_tenant_name(),
        "tenant_id", tenant_schema.get_tenant_id(),
        "sql_text", ObHexEscapeSqlStr(arg.ddl_stmt_str_),
        "extra_info", ObHexEscapeSqlStr(extra_info)))) {
        LOG_WARN("failed to create new rs job", KR(ret), K(job_type), K(tenant_schema), K(extra_info));
      }
    }
    FLOG_INFO("[ALTER_TENANT_LOCALITY NOTICE] create a new rs job", KR(ret),
        "tenant_id", tenant_schema.get_tenant_id(), K(job_id), K(alter_locality_op));
  }
  if (OB_SUCC(ret)) {
    if ((ROLLBACK_ALTER_LOCALITY == alter_locality_op
        && arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::FORCE_LOCALITY))
        || NOP_LOCALITY_OP == alter_locality_op) {
      ret = RS_JOB_COMPLETE(job_id, 0, trans);
      FLOG_INFO("[ALTER_TENANT_LOCALITY NOTICE] complete a new rs job immediately", KR(ret),
          "tenant_id", tenant_schema.get_tenant_id(), K(job_id), K(alter_locality_op));
    }
  }
  return ret;
}

int ObTenantDDLService::modify_tenant_inner_phase(const ObModifyTenantArg &arg, const ObTenantSchema *orig_tenant_schema, ObSchemaGetterGuard &schema_guard, bool is_restore)
{
  int ret = OB_SUCCESS;
  if (0 != arg.sys_var_list_.count()) {
    // modify system variable
    const ObSysVariableSchema *orig_sys_variable = NULL;
    const uint64_t tenant_id = orig_tenant_schema->get_tenant_id();
    int64_t schema_version = OB_INVALID_VERSION;
    ObDDLSQLTransaction trans(schema_service_);
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    bool value_changed = false;
    if (is_restore && is_user_tenant(tenant_id)) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("ddl operation is not allowed in standby cluster", K(ret));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "ddl operation in standby cluster");
    } else if (OB_FAIL(schema_guard.get_sys_variable_schema(
                       orig_tenant_schema->get_tenant_id(), orig_sys_variable))) {
      LOG_WARN("get sys variable schema failed", K(ret));
    } else if (OB_ISNULL(orig_sys_variable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys variable schema is null", K(ret));
    } else {
      ObSysVariableSchema new_sys_variable;
      if (OB_FAIL(new_sys_variable.assign(*orig_sys_variable))) {
        LOG_WARN("fail to assign sys variable schema", KR(ret), KPC(orig_sys_variable));
      } else if (FALSE_IT(new_sys_variable.reset_sysvars())) {
      } else if (OB_FAIL(update_sys_variables(arg.sys_var_list_, *orig_sys_variable, new_sys_variable, value_changed))) {
        LOG_WARN("failed to update_sys_variables", K(ret));
      } else if (value_changed == true) {
        int64_t refreshed_schema_version = 0;
        if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
          LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
        } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id, refreshed_schema_version))) {
          LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
        } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id, schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else {
          const ObSchemaOperationType operation_type = OB_DDL_ALTER_SYS_VAR;
          if (OB_FAIL(ddl_operator.replace_sys_variable(new_sys_variable, schema_version, trans, operation_type, &arg.ddl_stmt_str_))) {
            LOG_WARN("failed to replace sys variable", K(ret), K(new_sys_variable));
          }
        }
        if (trans.is_started()) {
          int temp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
            LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
            ret = (OB_SUCC(ret)) ? temp_ret : ret;
          }
        }
        // publish schema
        if (OB_SUCC(ret) && OB_FAIL(publish_schema(tenant_id))) {
          LOG_WARN("publish schema failed, ", K(ret));
        }
      }
    }
  } else if (!arg.alter_option_bitset_.is_empty()) {
    // modify tenant option
    const uint64_t tenant_id = orig_tenant_schema->get_tenant_id();
    bool grant = true;
    ObArray<ObResourcePoolName> diff_pools;
    AlterLocalityOp alter_locality_op = ALTER_LOCALITY_OP_INVALID;
    ObTenantSchema new_tenant_schema;
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);

    if (OB_FAIL(new_tenant_schema.assign(*orig_tenant_schema))) {
      LOG_WARN("fail to assign tenant schema", KR(ret));
    } else if (is_meta_tenant(tenant_id)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not allowed to modify meta tenant's options manually", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_new_tenant_options(schema_guard, arg, new_tenant_schema,
        *orig_tenant_schema, alter_locality_op))) {
      LOG_WARN("failed to set new tenant options", K(ret));
    } else if (OB_FAIL(check_alter_tenant_replica_options(
            arg, new_tenant_schema, *orig_tenant_schema, schema_guard))) {
      LOG_WARN("check tenant replica options failed", K(new_tenant_schema), K(ret));
    }
    // modify tenant option
    if (OB_SUCC(ret)) {
      ObDDLSQLTransaction trans(schema_service_);
      int64_t refreshed_schema_version = 0;
      common::ObArray<uint64_t> new_ug_id_array;
      if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, refreshed_schema_version))) {
        LOG_WARN("failed to get tenant schema version", KR(ret));
      } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
        LOG_WARN("start transaction failed", KR(ret), K(refreshed_schema_version));
      } else if (arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::RESOURCE_POOL_LIST)
            && OB_FAIL(modify_and_cal_resource_pool_diff(
                trans, new_ug_id_array, schema_guard,
                new_tenant_schema, arg.pool_list_, grant, diff_pools))) {
        LOG_WARN("fail to grant_pools", K(ret));
      }

      if (OB_SUCC(ret) && !is_restore) {
        if (OB_FAIL(check_tenant_primary_zone_(schema_guard, new_tenant_schema))) {
          LOG_WARN("fail to check tenant primary zone", KR(ret), K(new_tenant_schema));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::ENABLE_ARBITRATION_SERVICE)) {
#ifndef OB_BUILD_ARBITRATION
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("modify tenant arbitration service status in CE version not supprted", KR(ret));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "modify tenant arbitration service status in CE version");
#else
        const ObArbitrationServiceStatus old_status = orig_tenant_schema->get_arbitration_service_status();
        const ObArbitrationServiceStatus new_status = arg.tenant_schema_.get_arbitration_service_status();
        if ((new_status.is_enabling() && old_status.is_enable_like())
             || ((new_status.is_disabling() && old_status.is_disable_like()))) {
          // do nothing
        } else if (OB_FAIL(check_tenant_arbitration_service_status_(
                                trans,
                                tenant_id,
                                old_status,
                                new_status))) {
          LOG_WARN("fail to check tenant arbitration service", KR(ret), K(tenant_id), K(old_status), K(new_status));
        } else {
          new_tenant_schema.set_arbitration_service_status(new_status);
        }
#endif
      }

      if (FAILEDx(ObAlterPrimaryZoneChecker::create_alter_tenant_primary_zone_rs_job_if_needed(
          arg,
          tenant_id,
          *orig_tenant_schema,
          new_tenant_schema,
          trans))) {
        // if the command is alter tenant primary zone, we need to check whether first priority zone
        // has been changed. if so, the number of ls will be changed as well.
        // when the change occurs, we need to create a rs job ALTER_TENANT_PRIMARY_ZONE to
        // track if the number of ls matches the number of first primary zone
        // otherwise, the rs job is completed immediately
        LOG_WARN("fail to execute create_alter_tenant_primary_zone_rs_job_if_needed", KR(ret),
            K(arg), K(tenant_id), KPC(orig_tenant_schema), K(new_tenant_schema));
      } else if (OB_FAIL(ddl_operator.alter_tenant(new_tenant_schema, trans, &arg.ddl_stmt_str_))) {
        LOG_WARN("failed to alter tenant", K(ret));
      } else if (OB_FAIL(try_alter_meta_tenant_schema(
              ddl_operator, arg, trans, schema_guard, new_tenant_schema))) {
        LOG_WARN("failed to try alter meta tenant schema", KR(ret));
      }

      if (OB_SUCC(ret)
          && arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::LOCALITY)) {
        if (OB_FAIL(record_tenant_locality_event_history(alter_locality_op, arg, new_tenant_schema, trans))) {
          LOG_WARN("fail to record tenant locality event history", K(ret));
        }
      }

      if (trans.is_started()) {
        int temp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
          ret = (OB_SUCC(ret)) ? temp_ret : ret;
        }
      }
      // publish schema
      if (OB_SUCC(ret) && OB_FAIL(publish_schema(OB_SYS_TENANT_ID))) {
        LOG_WARN("publish schema failed, ", K(ret));
      }

      // When the new and old resource pool lists are consistent, no diff is generated, diff_pools is empty,
      // and there is no need to call the following function
      if (OB_SUCC(ret)
          && arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::RESOURCE_POOL_LIST)
          && diff_pools.count() > 0) {
        if (OB_FAIL(unit_mgr_->load())) {
          LOG_WARN("unit_manager reload failed", K(ret));
        }
      }
    }
  } else if (!arg.new_tenant_name_.empty()) {
    // rename tenant
    const uint64_t tenant_id = orig_tenant_schema->get_tenant_id();
    const ObString new_tenant_name = arg.new_tenant_name_;
    ObTenantSchema new_tenant_schema;
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    ObDDLSQLTransaction trans(schema_service_);
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(new_tenant_schema.assign(*orig_tenant_schema))) {
      LOG_WARN("fail to assign tenant schema", KR(ret));
    } else if (is_meta_tenant(tenant_id)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not allowed to modify meta tenant's options manually", KR(ret), K(tenant_id));
    } else if (orig_tenant_schema->is_restore()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("rename tenant while tenant is in physical restore status is not allowed",
               KR(ret), KPC(orig_tenant_schema));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "rename tenant while tenant is in physical restore status is");
    } else if (orig_tenant_schema->get_tenant_id() <= OB_MAX_RESERVED_TENANT_ID) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("rename special tenant not supported",
               K(ret), K(orig_tenant_schema->get_tenant_id()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "rename special tenant");
    } else if (NULL != schema_guard.get_tenant_info(new_tenant_name)) {
      ret = OB_TENANT_EXIST;
      LOG_USER_ERROR(OB_TENANT_EXIST, to_cstring(new_tenant_name));
      LOG_WARN("tenant already exists", K(ret), K(new_tenant_name));
    } else if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret));
    } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(refreshed_schema_version));
    } else if (OB_FAIL(new_tenant_schema.set_tenant_name(new_tenant_name))) {
      LOG_WARN("failed to rename tenant", K(ret),
                                          K(new_tenant_name),
                                          K(new_tenant_schema));
    } else if (OB_FAIL(ddl_operator.rename_tenant(new_tenant_schema,
                                                  trans,
                                                  &arg.ddl_stmt_str_))) {
      LOG_WARN("failed to rename tenant", K(ret), K(new_tenant_schema));
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
    // publish schema
    if (OB_SUCC(ret) && OB_FAIL(publish_schema(OB_SYS_TENANT_ID))) {
      LOG_WARN("publish schema failed, ", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys variable or tenant option should changed", K(ret), K(arg));
  }
  return ret;
}
template <typename SCHEMA>
int ObTenantDDLService::get_schema_primary_regions(
    const SCHEMA &schema,
    share::schema::ObSchemaGetterGuard &schema_guard,
    common::ObIArray<common::ObRegion> &primary_regions)
{
  int ret = OB_SUCCESS;
  primary_regions.reset();
  common::ObArray<common::ObZone> zone_list;
  ObArenaAllocator allocator("PrimaryZone");
  ObPrimaryZone primary_zone_schema(allocator);
  if (OB_FAIL(schema.get_primary_zone_inherit(schema_guard, primary_zone_schema))) {
    LOG_WARN("fail to get primary zone inherit", K(ret));
  } else if (ObString(OB_RANDOM_PRIMARY_ZONE) == primary_zone_schema.primary_zone_str_
      || primary_zone_schema.primary_zone_str_.empty()) {
    common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
    if (OB_FAIL(schema.get_zone_replica_attr_array_inherit(schema_guard, zone_locality))) {
      LOG_WARN("fail to get zone replica attr set", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); ++i) {
        const share::ObZoneReplicaAttrSet &this_locality = zone_locality.at(i);
        if (this_locality.get_full_replica_num() <= 0) {
          // bypass
        } else if (OB_FAIL(append(zone_list, this_locality.get_zone_set()))) {
          LOG_WARN("fail to append zone set", K(ret));
        }
      }
    }
  } else {
    const ObIArray<ObZoneScore> &primary_zone_array = primary_zone_schema.primary_zone_array_;
    if (primary_zone_array.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, tenant primary zone array count less than 0", K(ret));
    } else {
      const ObZoneScore &sample_zone_score = primary_zone_array.at(0);
      for (int64_t i = 0; OB_SUCC(ret) && i < primary_zone_array.count(); ++i) {
        const ObZoneScore &this_zone_score = primary_zone_array.at(i);
        if (this_zone_score.score_ != sample_zone_score.score_) {
          break;
        } else if (OB_FAIL(zone_list.push_back(this_zone_score.zone_))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (zone_list.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone list count is zero", K(ret));
    } else if (OB_FAIL(construct_region_list(primary_regions, zone_list))) {
      LOG_WARN("fail to construct region list", K(ret));
    }
  }
  return ret;
}

int ObTenantDDLService::construct_region_list(
    common::ObIArray<common::ObRegion> &region_list,
    const common::ObIArray<common::ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  region_list.reset();
  if (OB_UNLIKELY(NULL == zone_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("zone mgr is null", K(ret));
  } else {
    common::ObArray<share::ObZoneInfo> zone_infos;
    if (OB_FAIL(zone_mgr_->get_zone(zone_infos))) {
      LOG_WARN("fail to get zone", K(ret));
    } else {
      for (int64_t i = 0; i < zone_infos.count() && OB_SUCC(ret); ++i) {
        ObRegion region;
        share::ObZoneInfo &zone_info = zone_infos.at(i);
        if (OB_FAIL(region.assign(zone_info.region_.info_.ptr()))) {
          LOG_WARN("fail to assign region", K(ret));
        } else if (!has_exist_in_array(zone_list, zone_info.zone_)) {
          // this zone do not exist in my zone list, ignore it
        } else if (has_exist_in_array(region_list, region)) {
          // this region already exist in array
        } else if (OB_FAIL(region_list.push_back(region))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      }
    }
  }
  return ret;
}

int ObTenantDDLService::check_tenant_primary_zone_(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObTenantSchema &new_tenant_schema)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObRegion> tenant_primary_regions;
  if (OB_FAIL(get_schema_primary_regions(
          new_tenant_schema, schema_guard, tenant_primary_regions))) {
    LOG_WARN("fail to get tenant primary regions", K(ret));
  } else if (tenant_primary_regions.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("primary regions unexpected", K(ret));
  } else if (tenant_primary_regions.count() > 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant primary zone span regions not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant primary zone span regions");
  }
  return ret;
}

int ObTenantDDLService::init_system_variables(
    const ObCreateTenantArg &arg,
    const ObTenantSchema &tenant_schema,
    ObSysVariableSchema &sys_variable_schema)
{
  int ret = OB_SUCCESS;
  const int64_t params_capacity = ObSysVarFactory::ALL_SYS_VARS_COUNT;
  int64_t var_amount = ObSysVariables::get_amount();
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  ObMalloc alloc(ObModIds::OB_TEMP_VARIABLES);
  ObPtrGuard<ObSysParam, ObSysVarFactory::ALL_SYS_VARS_COUNT> sys_params_guard(alloc);
  sys_variable_schema.reset();
  sys_variable_schema.set_tenant_id(tenant_id);
  ObSysParam *sys_params = NULL;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_service_)
             || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(schema_service), KP_(sql_proxy));
  } else if (OB_FAIL(sys_params_guard.init())) {
    LOG_WARN("alloc sys parameters failed", KR(ret));
  } else if (FALSE_IT(sys_params = sys_params_guard.ptr())) {
  } else if (OB_ISNULL(sys_params) || OB_UNLIKELY(var_amount > params_capacity)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(sys_params), K(params_capacity), K(var_amount));
  } else {
    HEAP_VARS_2((char[OB_MAX_SYS_PARAM_VALUE_LENGTH], val_buf),
                (char[OB_MAX_SYS_PARAM_VALUE_LENGTH], version_buf)) {
      // name_case_mode
      if (is_meta_tenant(tenant_id)) {
        sys_variable_schema.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
      } else if (OB_NAME_CASE_INVALID == arg.name_case_mode_) {
        sys_variable_schema.set_name_case_mode(OB_LOWERCASE_AND_INSENSITIVE);
      } else {
        sys_variable_schema.set_name_case_mode(arg.name_case_mode_);
      }

      // init default values
      for (int64_t i = 0; OB_SUCC(ret) && i < var_amount; ++i) {
        if (OB_FAIL(sys_params[i].init(tenant_id,
                                       ObSysVariables::get_name(i),
                                       ObSysVariables::get_type(i),
                                       ObSysVariables::get_value(i),
                                       ObSysVariables::get_min(i),
                                       ObSysVariables::get_max(i),
                                       ObSysVariables::get_info(i),
                                       ObSysVariables::get_flags(i)))) {
          LOG_WARN("fail to init param", KR(ret), K(tenant_id), K(i));
        }
      }

      int64_t set_sys_var_count = arg.sys_var_list_.count();
      bool use_default_parallel_servers_target = true;
      bool explicit_set_compatibility_version = false;
      bool explicit_set_security_version = false;
      bool read_only = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < set_sys_var_count; ++j) {
        ObSysVarIdValue sys_var;
        if (OB_FAIL(arg.sys_var_list_.at(j, sys_var))) {
          LOG_WARN("failed to get sys var", K(j), K(ret));
        } else {
          const ObString &new_value = sys_var.value_;
          SET_TENANT_VARIABLE(sys_var.sys_id_, new_value);
          // sync tenant schema
          if (SYS_VAR_READ_ONLY == sys_var.sys_id_) {
            if (is_user_tenant(tenant_id)) {
              read_only = (0 == sys_var.value_.compare("1"));
            }
          } else if (SYS_VAR_PARALLEL_SERVERS_TARGET == sys_var.sys_id_) {
            use_default_parallel_servers_target = false;
          } else if (SYS_VAR_OB_COMPATIBILITY_VERSION == sys_var.sys_id_) {
            explicit_set_compatibility_version = true;
          } else if (SYS_VAR_OB_SECURITY_VERSION == sys_var.sys_id_) {
            explicit_set_security_version = true;
          }
        }
      } // end for

      // For read_only, its priority: sys variable > tenant option.
      if (OB_SUCC(ret)) {
        ObString read_only_value = read_only ? "1" : "0";
        SET_TENANT_VARIABLE(SYS_VAR_READ_ONLY, read_only_value);
      }

      // For compatibility_mode, its priority: sys variable > tenant option.
      if (OB_SUCC(ret)) {
        ObString compat_mode_value = tenant_schema.is_oracle_tenant() ? "1" : "0";
        SET_TENANT_VARIABLE(SYS_VAR_OB_COMPATIBILITY_MODE, compat_mode_value);
      }

      if (OB_SUCC(ret)) {
        char version[common::OB_CLUSTER_VERSION_LENGTH] = {0};
        int64_t len = ObClusterVersion::print_version_str(
                  version, common::OB_CLUSTER_VERSION_LENGTH, DATA_CURRENT_VERSION);
        SET_TENANT_VARIABLE(SYS_VAR_PRIVILEGE_FEATURES_ENABLE, ObString(len, version));
      }

      if (OB_SUCC(ret)) {
        ObString enable = "1";
        SET_TENANT_VARIABLE(SYS_VAR__ENABLE_MYSQL_PL_PRIV_CHECK, enable);
      }

      // If the user does not specify parallel_servers_target when creating tenant,
      // then calculate a default value based on cpu_count.
      // Considering that a tenant may have multiple resource pools, it is currently rudely considered
      // that the units in the pool are of the same structure, and directly take the unit config of the first resource pool
      // WARNING: If the unit is not structured, the number of threads allocated by default may be too large/too small
      int64_t default_px_thread_count = 0;
      if (OB_SUCC(ret) && (use_default_parallel_servers_target)) {
        HEAP_VAR(ObUnitConfig, unit_config) {
          if (OB_SYS_TENANT_ID == sys_variable_schema.get_tenant_id()) {
            // When creating a system tenant, the default value of px_thread_count is related to
            // default sys tenant min cpu
            const int64_t sys_default_min_cpu =
                static_cast<int64_t>(GCONF.get_sys_tenant_default_min_cpu());
            default_px_thread_count = ObTenantCpuShare::calc_px_pool_share(
                sys_variable_schema.get_tenant_id(), sys_default_min_cpu);
          } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit_mgr_ is null", K(ret), KP(unit_mgr_));
          } else if (arg.pool_list_.count() <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tenant should have at least one pool", K(ret));
          } else if (OB_FAIL(unit_mgr_->get_unit_config_by_pool_name(
                      arg.pool_list_.at(0), unit_config))) {
            LOG_WARN("fail to get unit config", K(ret));
          } else {
            int64_t cpu_count = static_cast<int64_t>(unit_config.unit_resource().min_cpu());
            default_px_thread_count = ObTenantCpuShare::calc_px_pool_share(
                sys_variable_schema.get_tenant_id(), cpu_count);
          }
        }
      }

      if (OB_SUCC(ret) && use_default_parallel_servers_target && default_px_thread_count > 0) {
        // target cannot be less than 3, otherwise any px query will not come in
        int64_t default_px_servers_target = std::max(3L, static_cast<int64_t>(default_px_thread_count));
        VAR_INT_TO_STRING(val_buf, default_px_servers_target);
        SET_TENANT_VARIABLE(SYS_VAR_PARALLEL_SERVERS_TARGET, val_buf);
      }

      VAR_UINT_TO_STRING(version_buf, CLUSTER_CURRENT_VERSION);
      if (OB_SUCC(ret) && !(is_user_tenant(tenant_id) && explicit_set_compatibility_version)) {
        SET_TENANT_VARIABLE(SYS_VAR_OB_COMPATIBILITY_VERSION, version_buf);
      }

      if (OB_SUCC(ret) && !(is_user_tenant(tenant_id) && explicit_set_security_version)) {
        SET_TENANT_VARIABLE(SYS_VAR_OB_SECURITY_VERSION, version_buf);
      }

      if (FAILEDx(update_mysql_tenant_sys_var(
          tenant_schema, sys_variable_schema, sys_params, params_capacity))) {
        LOG_WARN("failed to update_mysql_tenant_sys_var",
                 KR(ret), K(tenant_schema), K(sys_variable_schema));
      } else if (OB_FAIL(update_oracle_tenant_sys_var(
          tenant_schema, sys_variable_schema, sys_params, params_capacity))) {
        LOG_WARN("failed to update_oracle_tenant_sys_var",
                 KR(ret), K(tenant_schema), K(sys_variable_schema));
      } else if (OB_FAIL(update_special_tenant_sys_var(
                 sys_variable_schema, sys_params, params_capacity))) {
        LOG_WARN("failed to update_special_tenant_sys_var", K(ret), K(sys_variable_schema));
      }

      // set sys_variable
      if (OB_SUCC(ret)) {
        ObSysVarSchema sysvar_schema;
        for (int64_t i = 0; OB_SUCC(ret) && i < var_amount; i++) {
          sysvar_schema.reset();
          if (OB_FAIL(ObSchemaUtils::convert_sys_param_to_sysvar_schema(sys_params[i], sysvar_schema))) {
            LOG_WARN("convert to sysvar schema failed", K(ret));
          } else if (OB_FAIL(sys_variable_schema.add_sysvar_schema(sysvar_schema))) {
            LOG_WARN("add system variable failed", K(ret));
          }
        } //end for
      }
    } // end HEAP_VAR
  }
  return ret;
}

int ObTenantDDLService::update_mysql_tenant_sys_var(
    const ObTenantSchema &tenant_schema,
    const ObSysVariableSchema &sys_variable_schema,
    ObSysParam *sys_params,
    int64_t params_capacity)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = sys_variable_schema.get_tenant_id();
  if (OB_ISNULL(sys_params) || OB_UNLIKELY(params_capacity < ObSysVarFactory::ALL_SYS_VARS_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(sys_params), K(params_capacity));
  } else if (tenant_schema.is_mysql_tenant()) {
    HEAP_VAR(char[OB_MAX_SYS_PARAM_VALUE_LENGTH], val_buf) {
      // If it is a tenant in mysql mode, you need to consider setting the charset and collation
      // corresponding to the tenant to sys var
      VAR_INT_TO_STRING(val_buf, tenant_schema.get_collation_type());
      // set collation and char set
      SET_TENANT_VARIABLE(SYS_VAR_COLLATION_DATABASE, val_buf);
      SET_TENANT_VARIABLE(SYS_VAR_COLLATION_SERVER, val_buf);
      SET_TENANT_VARIABLE(SYS_VAR_CHARACTER_SET_DATABASE, val_buf);
      SET_TENANT_VARIABLE(SYS_VAR_CHARACTER_SET_SERVER, val_buf);
    } // end HEAP_VAR
  }
  return ret;
}

int ObTenantDDLService::update_oracle_tenant_sys_var(
    const ObTenantSchema &tenant_schema,
    const ObSysVariableSchema &sys_variable_schema,
    ObSysParam *sys_params,
    int64_t params_capacity)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = sys_variable_schema.get_tenant_id();
  if (OB_ISNULL(sys_params) || OB_UNLIKELY(params_capacity < ObSysVarFactory::ALL_SYS_VARS_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(sys_params), K(params_capacity));
  } else if (tenant_schema.is_oracle_tenant()) {
    HEAP_VAR(char[OB_MAX_SYS_PARAM_VALUE_LENGTH], val_buf) {
      // For oracle tenants, the collation of sys variable and tenant_option is set to binary by default.
      // set group_concat_max_len = 4000
      // set autocommit = off
      // When setting oracle variables, try to keep the format consistent
      VAR_INT_TO_STRING(val_buf, OB_DEFAULT_GROUP_CONCAT_MAX_LEN_FOR_ORACLE);
      SET_TENANT_VARIABLE(SYS_VAR_GROUP_CONCAT_MAX_LEN, val_buf);

      SET_TENANT_VARIABLE(SYS_VAR_AUTOCOMMIT, "0");

      VAR_INT_TO_STRING(val_buf, tenant_schema.get_collation_type());
      SET_TENANT_VARIABLE(SYS_VAR_COLLATION_DATABASE, val_buf);
      SET_TENANT_VARIABLE(SYS_VAR_COLLATION_SERVER, val_buf);
      SET_TENANT_VARIABLE(SYS_VAR_CHARACTER_SET_DATABASE, val_buf);
      SET_TENANT_VARIABLE(SYS_VAR_CHARACTER_SET_SERVER, val_buf);

      // Here is the collation of the connection, OB currently only supports the client as utf8mb4
      VAR_INT_TO_STRING(val_buf, CS_TYPE_UTF8MB4_BIN);
      SET_TENANT_VARIABLE(SYS_VAR_COLLATION_CONNECTION, val_buf);
      SET_TENANT_VARIABLE(SYS_VAR_CHARACTER_SET_CONNECTION, val_buf);

      /*
       * In Oracle mode, we are only compatible with binary mode, so collate can only end with _bin
       */
      if (ObCharset::is_bin_sort(tenant_schema.get_collation_type())) {
        VAR_INT_TO_STRING(val_buf, tenant_schema.get_collation_type());
        SET_TENANT_VARIABLE(SYS_VAR_CHARACTER_SET_SERVER, val_buf);
        SET_TENANT_VARIABLE(SYS_VAR_CHARACTER_SET_DATABASE, val_buf);
        ObCharsetType charset_type = ObCharset::charset_type_by_coll(tenant_schema.get_collation_type());
        OZ(databuff_printf(val_buf, OB_MAX_SYS_PARAM_VALUE_LENGTH, "%s",
                           ObCharset::get_oracle_charset_name_by_charset_type(charset_type)));
        SET_TENANT_VARIABLE(SYS_VAR_NLS_CHARACTERSET, val_buf);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant collation set error", K(ret), K(tenant_schema.get_collation_type()));
      }

      // update oracle tenant schema
      if (OB_SUCC(ret)) {
        if (OB_FAIL(databuff_printf(sys_params[SYS_VAR_SQL_MODE].value_,
            sizeof(sys_params[SYS_VAR_SQL_MODE].value_), "%llu", DEFAULT_ORACLE_MODE))) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("set oracle tenant default sql mode failed",  K(ret));
        }
      }
    } // end HEAP_VAR
  }
  return ret;
}

// The value of certain system variables of the system/meta tenant
int ObTenantDDLService::update_special_tenant_sys_var(
    const ObSysVariableSchema &sys_variable_schema,
    ObSysParam *sys_params,
    int64_t params_capacity)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = sys_variable_schema.get_tenant_id();
  if (OB_ISNULL(sys_params) || OB_UNLIKELY(params_capacity < ObSysVarFactory::ALL_SYS_VARS_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(sys_params), K(params_capacity));
  } else {
    HEAP_VAR(char[OB_MAX_SYS_PARAM_VALUE_LENGTH], val_buf) {
      if (is_sys_tenant(tenant_id)) {
        VAR_INT_TO_STRING(val_buf, sys_variable_schema.get_name_case_mode());
        SET_TENANT_VARIABLE(SYS_VAR_LOWER_CASE_TABLE_NAMES, val_buf);

        OZ(databuff_printf(val_buf, OB_MAX_SYS_PARAM_VALUE_LENGTH, "%s", OB_SYS_HOST_NAME));
        SET_TENANT_VARIABLE(SYS_VAR_OB_TCP_INVITED_NODES, val_buf);
      } else if (is_meta_tenant(tenant_id)) {
        ObString compatibility_mode("0");
        SET_TENANT_VARIABLE(SYS_VAR_OB_COMPATIBILITY_MODE, compatibility_mode);
      }
    } // end HEAP_VAR
  }
  return ret;
}

int ObTenantDDLService::check_create_tenant_schema(
    const ObIArray<ObString> &pool_list,
    ObTenantSchema &tenant_schema,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (tenant_schema.get_tenant_name_str().length() > OB_MAX_TENANT_NAME_LENGTH) {
    ret = OB_INVALID_TENANT_NAME;
    LOG_USER_ERROR(OB_INVALID_TENANT_NAME,
        to_cstring(tenant_schema.get_tenant_name_str()), OB_MAX_TENANT_NAME_LENGTH);
    LOG_WARN("tenant name can't over max_tenant_name_length", KR(ret), K(OB_MAX_TENANT_NAME_LENGTH));
  } else if (OB_FAIL(check_create_tenant_locality(pool_list, tenant_schema, schema_guard))) {
    LOG_WARN("fail to check create tenant locality", KR(ret), K(pool_list), K(tenant_schema));
  } else if (OB_FAIL(check_create_tenant_replica_options(tenant_schema, schema_guard))) {
    LOG_WARN("check replica options failed", KR(ret), K(tenant_schema));
  }
  return ret;
}

int ObTenantDDLService::get_pools(const ObIArray<ObString> &pool_strs,
                            ObIArray<ObResourcePoolName> &pools)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < pool_strs.count(); ++i) {
    ObResourcePoolName pool;
    if (OB_FAIL(pool.assign(pool_strs.at(i)))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(pools.push_back(pool))) {
      LOG_WARN("push_back failed", K(ret));
    }
  }
  return ret;
}

int ObTenantDDLService::check_create_tenant_locality(
    const ObIArray<ObString> &pool_list,
    ObTenantSchema &tenant_schema,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObArray<ObResourcePoolName> pools;
  ObArray<ObZone> pool_zones;
  ObArray<ObZone> temp_zones;
  ObArray<share::schema::ObZoneRegion> zone_region_list;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init");
  } else if (0 == pool_list.count()) {
    ret = OB_EMPTY_RESOURCE_POOL_LIST;
    LOG_WARN("pool list can not be empty", K(pool_list), K(ret));
  } else if (OB_FAIL(get_pools(pool_list, pools))) {
    LOG_WARN("get_pools failed", K(pool_list), K(ret));
  } else if (OB_FAIL(unit_mgr_->get_zones_of_pools(pools, temp_zones))) {
    LOG_WARN("get_zones_of_pools failed", K(pools), K(ret));
  } else if (temp_zones.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_zones_of_pools return empty zone array", K(ret));
  } else {
    // get zones of resource pools, remove duplicated zone
    lib::ob_sort(temp_zones.begin(), temp_zones.end());
    FOREACH_X(zone, temp_zones, OB_SUCC(ret)) {
      if (OB_ISNULL(zone)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone is null", K(ret));
      } else if (0 == pool_zones.count() || pool_zones.at(pool_zones.count() - 1) != *zone) {
        if (OB_FAIL(pool_zones.push_back(*zone))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      } else {} // duplicated zone, no need to push into.
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(construct_zone_region_list(zone_region_list, pool_zones))) {
      LOG_WARN("fail to construct zone_region list", K(ret));
    } else if (OB_FAIL(parse_and_set_create_tenant_new_locality_options(
            schema_guard, tenant_schema, pools, pool_zones, zone_region_list))) {
      LOG_WARN("fail to parse and set new locality option", K(ret));
    } else if (OB_FAIL(check_locality_compatible_(tenant_schema, true /*for_create_tenant*/))) {
      LOG_WARN("fail to check locality with data version", KR(ret), K(tenant_schema));
    } else if (OB_FAIL(check_pools_unit_num_enough_for_schema_locality(
            pools, schema_guard, tenant_schema))) {
      LOG_WARN("pools unit num is not enough for locality", K(ret));
    } else {} // no more to do
  }
  return ret;
}

template<typename SCHEMA>
int ObTenantDDLService::check_pools_unit_num_enough_for_schema_locality(
    const common::ObIArray<ObResourcePoolName> &pools,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const SCHEMA &schema)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObZone> zone_list;
  common::ObArray<share::ObZoneReplicaNumSet> zone_locality;
  bool is_legal = true;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", K(ret));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr ptr is null", K(ret), KP(unit_mgr_));
  } else if (OB_FAIL(schema.get_zone_list(schema_guard, zone_list))) {
    LOG_WARN("fail to get zone list", K(ret));
  } else if (OB_FAIL(schema.get_zone_replica_attr_array_inherit(schema_guard, zone_locality))) {
    LOG_WARN("fail to get zone replica num array", K(ret));
  } else if (OB_FAIL(unit_mgr_->check_pools_unit_legality_for_locality(
          pools, zone_list, zone_locality, is_legal))) {
    LOG_WARN("fail to check", K(ret));
  } else if (!is_legal) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("pool unit num is not enough for tenant locality", K(pools));
  } else {} // no more
  return ret;
}

int ObTenantDDLService::check_locality_compatible_(
    ObTenantSchema &schema,
    const bool for_create_tenant)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  const uint64_t tenant_id = for_create_tenant ? OB_SYS_TENANT_ID : schema.get_tenant_id();
  bool is_compatible_with_readonly_replica = false;
  bool is_compatible_with_columnstore_replica = false;
  if (OB_FAIL(ObShareUtil::check_compat_version_for_readonly_replica(
                tenant_id, is_compatible_with_readonly_replica))) {
    LOG_WARN("fail to check compatible with readonly replica", KR(ret), K(schema));
  } else if (OB_FAIL(ObShareUtil::check_compat_version_for_columnstore_replica(
                       tenant_id, is_compatible_with_columnstore_replica))) {
    LOG_WARN("fail to check compatible with columnstore replica", KR(ret), K(schema));
  } else if (OB_FAIL(schema.get_zone_replica_attr_array(zone_locality))) {
    LOG_WARN("fail to get locality from schema", K(ret), K(schema));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); ++i) {
      const share::ObZoneReplicaAttrSet &this_set = zone_locality.at(i);
      if (this_set.zone_set_.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone set count unexpected", K(ret), "zone_set_cnt", this_set.zone_set_.count());
      } else if (! is_compatible_with_readonly_replica
                 && 0 != this_set.get_readonly_replica_num()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("can not create tenant with read-only replica below data version 4.2", KR(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Create tenant with R-replica in locality below data version 4.2");
      } else if (! is_compatible_with_columnstore_replica
                 && 0 != this_set.get_columnstore_replica_num()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("can not create tenant with column-store replica below data version 4.3.3", KR(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Create tenant with C-replica in locality below data version 4.3.3");
      } else if (GCTX.is_shared_storage_mode()
                  && 0 != this_set.get_columnstore_replica_num()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("can not create tenant with column-store replica in shared-storage mode", KR(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "In shared-storage mode, C-replica is");
      }
    }
  }
  return ret;
}

int ObTenantDDLService::parse_and_set_create_tenant_new_locality_options(
    share::schema::ObSchemaGetterGuard &schema_guard,
    ObTenantSchema &schema,
    const common::ObIArray<share::ObResourcePoolName> &pools,
    const common::ObIArray<common::ObZone> &zone_list,
    const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list)
{
  int ret = OB_SUCCESS;
  char locality_str[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  ObLocalityDistribution locality_dist;
  ObArray<ObUnitInfo> unit_infos;
  if (OB_FAIL(locality_dist.init())) {
    LOG_WARN("fail to init locality dist", K(ret));
  } else if (OB_FAIL(locality_dist.parse_locality(
              schema.get_locality_str(), zone_list, &zone_region_list))) {
    LOG_WARN("fail to parse locality", K(ret));
  } else if (OB_FAIL(locality_dist.output_normalized_locality(
              locality_str, MAX_LOCALITY_LENGTH, pos))) {
    LOG_WARN("fail to normalized locality", K(ret));
  } else if (OB_FAIL(schema.set_locality(locality_str))) {
    LOG_WARN("fail to set normalized locality back to schema", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_unit_infos(pools, unit_infos))) {
    LOG_WARN("fail to get unit infos", K(ret));
  } else if (OB_FAIL(set_schema_replica_num_options(schema, locality_dist, unit_infos))) {
    LOG_WARN("fail to set schema replica num options", K(ret));
  } else if (OB_FAIL(set_schema_zone_list(
              schema_guard, schema, zone_region_list))) {
    LOG_WARN("fail to set table schema zone list", K(ret));
  } else {} // no more to do
  LOG_DEBUG("parse and set new locality", K(ret), K(locality_str), K(schema),
            K(pools), K(zone_list), K(zone_region_list), K(unit_infos));
  return ret;
}

template<typename T>
int ObTenantDDLService::set_schema_zone_list(
    share::schema::ObSchemaGetterGuard &schema_guard,
    T &schema,
    const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObZone> zone_list;
  common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  if (OB_FAIL(schema.get_zone_replica_attr_array_inherit(schema_guard, zone_locality))) {
    LOG_WARN("fail to get zone replica num arrary", K(ret));
  } else if (OB_FAIL(generate_zone_list_by_locality(
          zone_locality, zone_region_list, zone_list))) {
    LOG_WARN("fail to generate zone list by locality",
             K(ret), K(zone_locality), K(zone_region_list));
  } else if (OB_FAIL(schema.set_zone_list(zone_list))) {
    LOG_WARN("fail to set zone list", K(ret), K(zone_list));
  } else {} // no more to do
  return ret;
}

int ObTenantDDLService::generate_zone_list_by_locality(
    const ZoneLocalityIArray &zone_locality,
    const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list,
    common::ObArray<common::ObZone> &zone_list) const
{
  int ret = OB_SUCCESS;
  zone_list.reset();
  UNUSED(zone_region_list);
  common::ObArray<common::ObZone> tmp_zone_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); ++i) {
    const ObZoneReplicaAttrSet &zone_num_set = zone_locality.at(i);
    const ObIArray<common::ObZone> &zone_set = zone_num_set.zone_set_;
    if (OB_FAIL(append(tmp_zone_list, zone_set))) {
      LOG_WARN("fail to append zone set", K(ret));
    } else {} // ok, go on next
  }

  if (OB_SUCC(ret)) {
    lib::ob_sort(tmp_zone_list.begin(), tmp_zone_list.end());
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_zone_list.count(); ++i) {
      common::ObZone &this_zone = tmp_zone_list.at(i);
      if (0 == zone_list.count() || zone_list.at(zone_list.count() - 1) != this_zone) {
        if (OB_FAIL(zone_list.push_back(this_zone))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      } else {} // duplicated zone, no need to push into.
    }
  }
  return ret;
}

template<typename SCHEMA>
int ObTenantDDLService::set_schema_replica_num_options(
    SCHEMA &schema,
    ObLocalityDistribution &locality_dist,
    ObIArray<ObUnitInfo> &unit_infos)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObZoneReplicaAttrSet> zone_replica_attr_array;
  if (OB_FAIL(locality_dist.get_zone_replica_attr_array(
          zone_replica_attr_array))) {
    LOG_WARN("fail to get zone region replica num array", K(ret));
  } else if (OB_FAIL(schema.set_zone_replica_attr_array(zone_replica_attr_array))) {
    LOG_WARN("fail to set zone replica num set", K(ret));
  } else {
    int64_t full_replica_num = 0;
    for (int64_t i = 0; i < zone_replica_attr_array.count(); ++i) {
      ObZoneReplicaNumSet &zone_replica_num_set = zone_replica_attr_array.at(i);
      full_replica_num += zone_replica_num_set.get_full_replica_num();
    }
    if (full_replica_num <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, should have at least one paxos replica");
      LOG_WARN("full replica num is zero", K(ret), K(full_replica_num), K(schema));
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < zone_replica_attr_array.count() && OB_SUCC(ret); ++i) {
      ObZoneReplicaAttrSet &zone_replica_set = zone_replica_attr_array.at(i);
      if (zone_replica_set.zone_set_.count() > 1) {
        if (zone_replica_set.zone_set_.count() != zone_replica_set.get_paxos_replica_num()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, too many paxos replicas in multiple zones");
          LOG_WARN("too many paxos replicas in multi zone", K(ret));
        }
      } else if (zone_replica_set.get_full_replica_num() > 1
                 || zone_replica_set.get_logonly_replica_num() > 1
                 || zone_replica_set.get_encryption_logonly_replica_num() > 1) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality");
        LOG_WARN("one zone should only have one paxos replica", K(ret), K(zone_replica_set));
      } else if (zone_replica_set.get_full_replica_num() == 1
                 && (zone_replica_set.get_logonly_replica_num() == 1
                     || zone_replica_set.get_encryption_logonly_replica_num() == 1)) {
        bool find = false;
        for (int64_t j = 0; j < unit_infos.count() && OB_SUCC(ret); j++) {
          if (unit_infos.at(j).unit_.zone_ == zone_replica_set.zone_
              && REPLICA_TYPE_LOGONLY == unit_infos.at(j).unit_.replica_type_) {
            find = true;
            break;
          }
        } //end for unit_infos
        if (!find) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality");
          LOG_WARN("no logonly unit exist", K(ret), K(zone_replica_set));
        }
      }
    }
  }
  return ret;
}

int ObTenantDDLService::construct_zone_region_list(
    common::ObIArray<ObZoneRegion> &zone_region_list,
    const common::ObIArray<common::ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  zone_region_list.reset();
  if (OB_UNLIKELY(NULL == zone_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("zone mgr is null", K(ret));
  } else {
    common::ObArray<share::ObZoneInfo> zone_infos;
    if (OB_FAIL(zone_mgr_->get_zone(zone_infos))) {
      LOG_WARN("fail to get zone", K(ret));
    } else {
      ObZoneRegion zone_region;
      for (int64_t i = 0; i < zone_infos.count() && OB_SUCC(ret); ++i) {
        zone_region.reset();
        share::ObZoneInfo &zone_info = zone_infos.at(i);
        if (OB_FAIL(zone_region.zone_.assign(zone_info.zone_.ptr()))) {
          LOG_WARN("fail to assign zone", K(ret));
        } else if (OB_FAIL(zone_region.region_.assign(zone_info.region_.info_.ptr()))) {
          LOG_WARN("fail to assign region", K(ret));
        } else if (OB_FAIL(zone_region.set_check_zone_type(zone_info.zone_type_.value_))) {
          LOG_WARN("fail to set check zone type", KR(ret));
        } else if (!has_exist_in_array(zone_list, zone_region.zone_)) {
          // this zone do not exist in my zone list, ignore it
        } else if (OB_FAIL(zone_region_list.push_back(zone_region))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      }
    }
  }
  return ret;
}

int ObTenantDDLService::check_create_tenant_replica_options(
    share::schema::ObTenantSchema &tenant_schema,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObArray<ObZone> zone_list;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", K(ret));
  } else if (OB_FAIL(tenant_schema.get_zone_list(zone_list))) {
    LOG_WARN("fail to get zone list", K(ret));
  } else if (OB_FAIL(check_create_schema_replica_options(
          tenant_schema, zone_list, schema_guard))) {
    LOG_WARN("fail to check replica options", K(ret));
  }
  return ret;
}

int ObTenantDDLService::check_schema_zone_list(
    common::ObArray<common::ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  lib::ob_sort(zone_list.begin(), zone_list.end());
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count() - 1; ++i) {
    if (zone_list.at(i) == zone_list.at(i+1)) {
      ret = OB_ZONE_DUPLICATED;
      LOG_USER_ERROR(OB_ZONE_DUPLICATED, to_cstring(zone_list.at(i)), to_cstring(zone_list));
      LOG_WARN("duplicate zone in zone list", K(zone_list), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
      bool zone_exist = false;
      if (OB_FAIL(zone_mgr_->check_zone_exist(zone_list.at(i), zone_exist))) {
        LOG_WARN("check_zone_exist failed", "zone", zone_list.at(i), K(ret));
      } else if (!zone_exist) {
        ret = OB_ZONE_INFO_NOT_EXIST;
        LOG_USER_ERROR(OB_ZONE_INFO_NOT_EXIST, to_cstring(zone_list.at(i)));
        LOG_WARN("zone not exist", "zone", zone_list.at(i), K(ret));
        break;
      }
    }
  }
  return ret;
}

template<typename SCHEMA>
int ObTenantDDLService::check_create_schema_replica_options(
    SCHEMA &schema,
    common::ObArray<common::ObZone> &zone_list,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", K(ret));
  } else if (!ObPrimaryZoneUtil::is_specific_primary_zone(schema.get_primary_zone())) {
    const common::ObArray<ObZoneScore> empty_pz_array;
    if (OB_FAIL(schema.set_primary_zone_array(empty_pz_array))) {
      LOG_WARN("fail to set primary zone array empty", K(ret));
    } else if (OB_FAIL(check_empty_primary_zone_locality_condition(
            schema, zone_list, schema_guard))) {
      LOG_WARN("fail to check empty primary zone locality condition", K(ret));
    }
  } else {
    if (OB_FAIL(check_schema_zone_list(zone_list))) {
      LOG_WARN("fail to check schema zone list", K(ret), K(zone_list));
    } else if (OB_FAIL(check_and_set_primary_zone(schema, zone_list, schema_guard))) {
      LOG_WARN("fail to check and set primary zone", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t paxos_num = 0;
    if (OB_FAIL(schema.get_paxos_replica_num(schema_guard, paxos_num))) {
      LOG_WARN("fail to get paxos replica num", K(ret));
    } else if (paxos_num <= 0 || paxos_num > common::OB_MAX_MEMBER_NUMBER) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid paxos replica num", K(ret), K(schema));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality paxos replica num");
    } else {} // good
  }
  return ret;
}

/* 1 First clarify two concepts:
 *   1.1 mix locality: mix locality is refers to the locality of multiple zones after the'@' mark
 *    such as F,L@[z1,z2] in the locality. We call the locality of F,L@[z1,z2] a mixe locality.
 *   1.2 independent locality: independent locality refers to the locality where there is only one zone after the'@'
 *    in the locality. We call the locality like F@z1 an "independent" locality.
 *
 * 2 After locality adds a mixed scene, the relationship between primary zone and locality
 *  includes the following restrictions:
 *   2.1 The region where the primary zone is located has at least two fully functional copies
 *   2.2 Each zone in the mixed locality must belong to the same region
 *   2.3 The zone in the mixed locality cannot contain the primary zone with the highest priority.
 *       for example locality='F,F{memstore_percent:0},L@[z1,z2,z3]',primary_zone='z1' is not allowed
 *       If there is a need to set the primary zone on z1. The locality and primary zone can be set as follows:
 *       locality = 'F@z1,F{memstore_percent:0},L@[z2,z3]', primary_zone = 'z1'
 *   2.4 Contrary to the logic of 2.3, if the locality contains both mixed locality and independent locality,
 *    the highest priority primary zone must be set to one of the independent locality.
 *       for example locality='F@z1, F{memstore_percent:0},L@[z2,z3]' It is not allowed not to set the primary zone
 *       If there is no preference location setting for the primary zone, the locality can be set as follows:
 *       locality = 'F,F{memstore_percent:0},L@[z1,z2,z3]'
 *   2.5 Currently, there are no application scenarios for multiple mixed localities,
 *    and multiple mixed localities are not supported for the time being.
 */
template<typename SCHEMA>
int ObTenantDDLService::check_and_set_primary_zone(
    SCHEMA &schema,
    const common::ObIArray<common::ObZone> &zone_list,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObZoneRegion> zone_region_list;
  if (schema.get_primary_zone().empty()) {
    LOG_INFO("primary zone is null, noting todo");
    //nothing todo
  } else if (OB_FAIL(construct_zone_region_list(zone_region_list, zone_list))) {
    LOG_WARN("fail to construct zone region list", K(ret));
  } else {
    SMART_VARS_2((char[MAX_ZONE_LIST_LENGTH], primary_zone_str),
                 (ObPrimaryZoneUtil, primary_zone_util,
                  schema.get_primary_zone(), &zone_region_list)) {
    int64_t pos = 0;
    if (OB_FAIL(primary_zone_util.init(zone_list))) {
      LOG_WARN("fail to init primary zone util", K(ret));
    } else if (OB_FAIL(primary_zone_util.check_and_parse_primary_zone())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid primary zone", K(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "primary zone");
    } else if (OB_FAIL(primary_zone_util.output_normalized_primary_zone(
                primary_zone_str, MAX_ZONE_LIST_LENGTH, pos))) {
      LOG_WARN("fail to output normalized primary zone", K(ret));
    } else if (OB_FAIL(schema.set_primary_zone(primary_zone_str))) {
      LOG_WARN("fail to set primary zone", K(ret));
    } else if (OB_FAIL(schema.set_primary_zone_array(primary_zone_util.get_zone_array()))) {
      LOG_WARN("fail to set primary zone array", K(ret));
    } else if (OB_FAIL(check_primary_zone_locality_condition(
            schema, zone_list, zone_region_list, schema_guard))) {
      LOG_WARN("fail to check primary zone region condition", K(ret));
    } else {} // no more to do
    } // end smart var
  }
  return ret;
}

/* 1 First clarify two concepts:
 *   1.1 mix locality: mix locality is refers to the locality of multiple zones after the'@' mark
 *    such as F,L@[z1,z2] in the locality. We call the locality of F,L@[z1,z2] a mixe locality.
 *   1.2 independent locality: independent locality refers to the locality where there is only one zone after the'@'
 *    in the locality. We call the locality like F@z1 an "independent" locality.
 *
 * 2 After locality adds a mixed scene, the relationship between primary zone and locality
 *  includes the following restrictions:
 *   2.1 The region where the primary zone is located has at least two fully functional copies
 *   2.2 Each zone in the mixed locality must belong to the same region
 *   2.3 The zone in the mixed locality cannot contain the primary zone with the highest priority.
 *       for example locality='F,F{memstore_percent:0},L@[z1,z2,z3]',primary_zone='z1' is not allowed
 *       If there is a need to set the primary zone on z1. The locality and primary zone can be set as follows:
 *       locality = 'F@z1,F{memstore_percent:0},L@[z2,z3]', primary_zone = 'z1'
 *   2.4 Contrary to the logic of 2.3, if the locality contains both mixed locality and independent locality,
 *    the highest priority primary zone must be set to one of the independent locality.
 *       for example locality='F@z1, F{memstore_percent:0},L@[z2,z3]' It is not allowed not to set the primary zone
 *       If there is no preference location setting for the primary zone, the locality can be set as follows:
 *       locality = 'F,F{memstore_percent:0},L@[z1,z2,z3]'
 *   2.5 Currently, there are no application scenarios for multiple mixed localities,
 *    and multiple mixed localities are not supported for the time being.
 */
template <typename SCHEMA>
int ObTenantDDLService::check_primary_zone_locality_condition(
    const SCHEMA &schema,
    const ObIArray<common::ObZone> &zone_list,
    const ObIArray<share::schema::ObZoneRegion> &zone_region_list,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObArray<ObZone> first_primary_zone_array;
  ObArray<ObRegion> primary_regions;
  ObArray<ObZone> zones_in_primary_regions;
  ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  ObString locality_str;
  if (zone_list.count() != zone_region_list.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "zone count", zone_list.count(),
             "zone region count", zone_region_list.count());
  } else if (zone_list.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), "zone count", zone_list.count());
  } else if (OB_FAIL(extract_first_primary_zone_array(
          schema, zone_list, first_primary_zone_array))) {
    LOG_WARN("fail to extract first primary zone array", K(ret));
  } else if (OB_FAIL(get_primary_regions_and_zones(
          zone_list, zone_region_list, first_primary_zone_array,
          primary_regions, zones_in_primary_regions))) {
    LOG_WARN("fail to get primary regions and zones", K(ret));
  } else if (OB_FAIL(schema.get_zone_replica_attr_array_inherit(
          schema_guard, zone_locality))) {
    LOG_WARN("fail to get zone replica attr array", K(ret));
  } else if (OB_FAIL(do_check_primary_zone_locality_condition(
          zone_region_list, first_primary_zone_array, zones_in_primary_regions,
          primary_regions, zone_locality))) {
    LOG_WARN("fail to do check primary zone region condition", K(ret));
  } else {} // no more to do
  return ret;
}

template <typename SCHEMA>
int ObTenantDDLService::extract_first_primary_zone_array(
    const SCHEMA &schema,
    const ObIArray<common::ObZone> &zone_list,
    ObIArray<common::ObZone> &first_primary_zone_array)
{
  int ret = OB_SUCCESS;
  if (schema.get_primary_zone_array().count() <= 0) {
    // bypass
  } else {
    const ObIArray<ObZoneScore> &primary_zone_score_array = schema.get_primary_zone_array();
    const ObZoneScore &sample_zone = primary_zone_score_array.at(0);
    for (int64_t i = 0; OB_SUCC(ret) && i < primary_zone_score_array.count(); ++i) {
      common::ObZone this_zone;
      if (sample_zone.score_ != primary_zone_score_array.at(i).score_) {
        break;
      } else if (OB_FAIL(this_zone.assign(primary_zone_score_array.at(i).zone_.ptr()))) {
        LOG_WARN("fail to assign zone", K(ret), "zone", primary_zone_score_array.at(i).zone_);
      } else if (!has_exist_in_array(zone_list, this_zone)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("primary zone not in zone list", K(ret), K(zone_list), K(this_zone));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "primary zone, primary zone not in zone list");
      } else if (OB_FAIL(first_primary_zone_array.push_back(this_zone))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    }
  }
  return ret;
}

int ObTenantDDLService::do_check_primary_zone_locality_condition(
    const ObIArray<share::schema::ObZoneRegion> &zone_region_list,
    const ObIArray<common::ObZone> &first_primary_zone_array,
    const ObIArray<common::ObZone> &zones_in_primary_regions,
    const ObIArray<common::ObRegion> &primary_regions,
    const ObIArray<share::ObZoneReplicaAttrSet> &zone_locality)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && zone_region_list.count() > 1 && first_primary_zone_array.count() > 0) {
    if (OB_FAIL(do_check_primary_zone_region_condition(
            zones_in_primary_regions, primary_regions, zone_locality))) {
      LOG_WARN("fail to check primary zone region condition", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_check_mixed_zone_locality_condition(
            zone_region_list, zone_locality))) {
      LOG_WARN("fail to check mixed zone locality condition", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_check_mixed_locality_primary_zone_condition(
            first_primary_zone_array, zone_locality))) {
      LOG_WARN("fail to check mixed locality primary zone condition", K(ret));
    }
  }

  return ret;
}

int ObTenantDDLService::do_check_primary_zone_region_condition(
    const ObIArray<common::ObZone> &zones_in_primary_regions,
    const ObIArray<common::ObRegion> &primary_regions,
    const ObIArray<share::ObZoneReplicaAttrSet> &zone_locality)
{
  int ret = OB_SUCCESS;
  int64_t full_replica_num = 0;
  UNUSED(primary_regions);
  for (int64_t i = 0; i < zones_in_primary_regions.count(); ++i) {
    const ObZone &this_zone = zones_in_primary_regions.at(i);
    for (int64_t j = 0; j < zone_locality.count(); ++j) {
      const ObZoneReplicaAttrSet &zone_replica_num = zone_locality.at(j);
      if (this_zone == zone_replica_num.zone_) {
        full_replica_num += zone_replica_num.get_full_replica_num();
        break;
      } else {} // go on and check next
    }
  }
  if (full_replica_num <= 1) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("primary zone F type replica is not enough in its region is not allowed", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "primary zone F type replica not enough in its region");
  }
  return ret;
}

int ObTenantDDLService::do_check_mixed_zone_locality_condition(
    const ObIArray<share::schema::ObZoneRegion> &zone_region_list,
    const ObIArray<share::ObZoneReplicaAttrSet> &zone_locality)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); ++i) {
    const ObZoneReplicaAttrSet &zone_attr = zone_locality.at(i);
    const common::ObIArray<common::ObZone> &zone_set = zone_attr.get_zone_set();
    if (zone_set.count() <= 1) {
      // bypass
    } else {
      common::ObRegion sample_region;
      for (int64_t j = 0; OB_SUCC(ret) && j < zone_set.count(); ++j) {
        const common::ObZone &this_zone = zone_set.at(j);
        common::ObRegion this_region;
        if (OB_FAIL(get_zone_region(this_zone, zone_region_list, this_region))) {
          LOG_WARN("fail to get zone region", K(ret), K(this_zone), K(zone_region_list));
        } else if (sample_region.is_empty()) {
          sample_region = this_region;
        } else if (sample_region != this_region) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("mixed zone locality in more than one region is not allowed", K(ret));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "mixed zone locality in more than one region");
        } else {} // next zone
      }
    }
  }
  return ret;
}

int ObTenantDDLService::get_zone_region(
    const common::ObZone &zone,
    const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list,
    common::ObRegion &region)
{
  int ret = OB_SUCCESS;
  bool find = false;
  for (int64_t i = 0; !find && i < zone_region_list.count(); ++i) {
    const share::schema::ObZoneRegion &zone_region = zone_region_list.at(i);
    if (zone_region.zone_ == zone) {
      region = zone_region.region_;
      find = true;
    } else {} // go on check next;
  }
  if (!find) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("entry not exist", K(ret));
  }
  return ret;
}

int ObTenantDDLService::do_check_mixed_locality_primary_zone_condition(
    const ObIArray<common::ObZone> &first_primary_zone_array,
    const ObIArray<share::ObZoneReplicaAttrSet> &zone_locality)
{
  int ret = OB_SUCCESS;
  // first primary zone cannot be assigned in mixed locality zones
  ObArray<common::ObZone> independent_zones;
  ObArray<int64_t> independent_zone_idxs;
  int64_t mixed_locality_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); ++i) {
    const share::ObZoneReplicaAttrSet &zone_attr = zone_locality.at(i);
    const ObIArray<common::ObZone> &zone_set = zone_attr.get_zone_set();
    if (zone_set.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone set count unexpected", K(ret));
    } else if (zone_set.count() == 1) {
      if (OB_FAIL(independent_zones.push_back(zone_set.at(0)))) {
        LOG_WARN("fail to push back independent zones", K(ret));
      } else if (OB_FAIL(independent_zone_idxs.push_back(i))) {
        LOG_WARN("fail to push back", K(ret));
      }
    } else {
      ++mixed_locality_count;
      for (int64_t j = 0; OB_SUCC(ret) && j < first_primary_zone_array.count(); ++j) {
        const common::ObZone &first_primary_zone = first_primary_zone_array.at(j);
        if (!has_exist_in_array(zone_set, first_primary_zone)) {
          // good, go on check
        } else {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("primary zone assigned with mix zone locality is not allowed", K(ret),
                   K(first_primary_zone), K(zone_set));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "primary zone assigned with mix zone locality");
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    // bypass
  } else if (0 == mixed_locality_count) {
    // bypass
  } else if (1 == mixed_locality_count) {
    if (independent_zones.count() <= 0) {
      // bypass
    } else if (independent_zones.count() != independent_zone_idxs.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("array count not match", K(ret), "zone_count", independent_zones.count(),
               "idx_count", independent_zone_idxs.count());
    } else {
      bool all_non_full_independent_zone = true;
      bool find_full_pz = false;
      for (int64_t i = 0; !find_full_pz && i < independent_zones.count(); ++i) {
        const common::ObZone &this_zone = independent_zones.at(i);
        const int64_t locality_idx = independent_zone_idxs.at(i);
        const ObZoneReplicaAttrSet &zone_replica_set = zone_locality.at(locality_idx);
        if (!has_exist_in_array(first_primary_zone_array, this_zone)) {
          if (zone_replica_set.get_full_replica_num() > 0) {
            all_non_full_independent_zone = false;
          }
        } else {
          if (zone_replica_set.get_full_replica_num() > 0) {
            find_full_pz = true;
          }
        }
      }
      if (find_full_pz) {
        // good, find full primary zone
      } else if (all_non_full_independent_zone) {
        // no full replica primary zone, but all others independent zones are not full, still good
      } else {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("independent locality with mixed locality without full primary zone is not allowed", K(ret));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "independent locality with mixed locality without full primary zone");
      }
    }
  } else {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("more than one mixed zone locality is not allowed", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "more than one mixed zone locality");
  }
  return ret;
}

int ObTenantDDLService::get_primary_regions_and_zones(
    const ObIArray<common::ObZone> &zone_list,
    const ObIArray<share::schema::ObZoneRegion> &zone_region_list,
    const ObIArray<common::ObZone> &first_primary_zone_array,
    ObIArray<common::ObRegion> &primary_regions,
    ObIArray<common::ObZone> &zones_in_primary_regions)
{
  int ret = OB_SUCCESS;
  primary_regions.reset();
  zones_in_primary_regions.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < first_primary_zone_array.count(); ++i) {
    const ObZone &this_zone = first_primary_zone_array.at(i);
    if (this_zone.is_empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("this zone is invalid", K(ret), K(this_zone));
    } else if (!has_exist_in_array(zone_list, this_zone)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("this zone is not exist in zone list", K(ret), K(this_zone), K(zone_list));
    } else {
      ObRegion this_region;
      bool find = false;
      for (int64_t j = 0; !find && j < zone_region_list.count(); ++j) {
        if (this_zone == zone_region_list.at(j).zone_) {
          this_region = zone_region_list.at(j).region_;
          find = true;
        } else {} // go on to check next
      }
      if (!find) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("region not found", K(ret), K(this_zone));
      } else if (this_region.is_empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("region invalid", K(ret), K(this_region));
      } else if (has_exist_in_array(primary_regions, this_region)) {
        // Already exist in primary regions, ignore
      } else if (OB_FAIL(primary_regions.push_back(this_region))) {
        LOG_WARN("fail to push back", K(ret), K(this_region));
      } else {} // no more to do
      // Push all the zones belonging to this region to zones_in_primary_regions
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_region_list.count(); ++i) {
        const share::schema::ObZoneRegion &zone_region = zone_region_list.at(i);
        if (zone_region.region_ != this_region) { // ignore
        } else if (has_exist_in_array(zones_in_primary_regions, zone_region.zone_)) { // ignore
        } else if (OB_FAIL(zones_in_primary_regions.push_back(zone_region.zone_))) {
          LOG_WARN("fail to push back", K(ret), "region", zone_region.zone_);
        } else {} // no more to do
      }
    }
  }
  return ret;
}

template<typename SCHEMA>
int ObTenantDDLService::check_empty_primary_zone_locality_condition(
    SCHEMA &schema,
    const common::ObIArray<common::ObZone> &zone_list,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("PrimaryZone");
  ObPrimaryZone primary_zone_schema(allocator);
  ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  ObArray<common::ObZone> first_primary_zone_array;
  ObArray<share::schema::ObZoneRegion> zone_region_list;
  if (OB_FAIL(construct_zone_region_list(zone_region_list, zone_list))) {
    LOG_WARN("fail to construct zone region list", K(ret));
  } else if (OB_FAIL(schema.get_primary_zone_inherit(schema_guard, primary_zone_schema))) {
    LOG_WARN("fail to get primary zone inherit", K(ret));
  } else if (OB_FAIL(extract_first_primary_zone_array(
          primary_zone_schema, zone_list, first_primary_zone_array))) {
    LOG_WARN("fail to extract first primary zone array", K(ret));
  } else if (OB_FAIL(schema.get_zone_replica_attr_array_inherit(
          schema_guard, zone_locality))) {
    LOG_WARN("fail to get zone replica attr array", K(ret));
  } else if (OB_FAIL(do_check_mixed_zone_locality_condition(
          zone_region_list, zone_locality))) {
    LOG_WARN("fail to do check mixed zone locality condition", K(ret));
  } else if (OB_FAIL(do_check_mixed_locality_primary_zone_condition(
          first_primary_zone_array, zone_locality))) {
    LOG_WARN("fail to check mixed locality primary zone condition", K(ret));
  }
  return ret;
}

int ObTenantDDLService::try_drop_sys_ls_(const uint64_t meta_tenant_id,
                       common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init");
  } else if (OB_UNLIKELY(!is_meta_tenant(meta_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not meta tenant", KR(ret), K(meta_tenant_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    //check ls exist in status
    ObLSLifeAgentManager life_agent(*sql_proxy_);
    ObLSStatusOperator ls_status;
    ObLSStatusInfo sys_ls_info;
    if (OB_FAIL(ls_status.get_ls_status_info(meta_tenant_id, SYS_LS, sys_ls_info, trans))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("sys ls not exist, no need to drop", KR(ret), K(meta_tenant_id));
      } else {
        LOG_WARN("failed to get ls status info", KR(ret), K(meta_tenant_id));
      }
    } else if (OB_FAIL(life_agent.drop_ls_in_trans(meta_tenant_id, SYS_LS, share::NORMAL_SWITCHOVER_STATUS, trans))) {
      LOG_WARN("failed to drop ls in trans", KR(ret), K(meta_tenant_id));
    }
  }
  return ret;
}


int ObTenantDDLService::drop_resource_pool_pre(const uint64_t tenant_id,
                                         common::ObIArray<uint64_t> &drop_ug_id_array,
                                         ObIArray<ObResourcePoolName> &pool_names,
                                         ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init");
  } else if (OB_FAIL(unit_mgr_->get_pool_names_of_tenant(tenant_id, pool_names))) {
    LOG_WARN("get_pool_names_of_tenant failed", K(tenant_id), KR(ret));
  } else if (OB_FAIL(unit_mgr_->revoke_pools(trans, drop_ug_id_array, pool_names, tenant_id))) {
    LOG_WARN("revoke_pools failed", K(pool_names), K(tenant_id), KR(ret));
  }
  return ret;
}

int ObTenantDDLService::drop_resource_pool_final(const uint64_t tenant_id,
                                           common::ObIArray<uint64_t> &drop_ug_id_array,
                                           ObIArray<ObResourcePoolName> &pool_names)
{
  int ret = OB_SUCCESS;
  const bool grant = false;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init");
  } else if (OB_FAIL(unit_mgr_->load())) {
    LOG_WARN("unit_manager reload failed", K(ret));
  }

  // delete from __all_schema_status
  // The update of __all_core_table must be guaranteed to be a single partition transaction,
  // so a separate transaction is required here.
  // The failure of the transaction will not affect it, but there is garbage data in __all_core_table.
  if (OB_SUCC(ret)) {
    int temp_ret = OB_SUCCESS;
    ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
    if (OB_ISNULL(schema_status_proxy)) {
      temp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_status_proxy is null", K(temp_ret));
    } else if (OB_SUCCESS !=
              (temp_ret = schema_status_proxy->del_tenant_schema_status(tenant_id))) {
      LOG_ERROR("del tenant schema status failed", KR(temp_ret), "tenant_id", tenant_id);
    }
  }
  return ret;
}

/*
 * This interface includes 4 situations of primary and standalone cluster in total
 * primary tenant
 * drop tenant force: The tenant is forced to be deleted, with the highest priority. Variable identification: drop_force
 * drop tenant and recyclebin is enable: put tenant into recyclebin. Variable identification: to_recyclebin
 * drop tenant and recyclebin is disable or drop tenant purge: Both cases take the path of delayed deletion
 * Variable identification: delay_to_drop
 * The priority of the 3 variables is reduced, and there can only be one state at the same time
 *
 * standby tenant
 * drop tenant force: The tenant is forced to be deleted, with the highest priority. Variable identification: drop_force
 * drop tenant and recyclebin is enable: put tenant into recyclebin. Variable identification: to_recyclebin
 * drop tenant and recyclebin is disable: equal to drop tneant force;
 *
 * meta tenant can only be force dropped with its user tenant.
 */
int ObTenantDDLService::drop_tenant(const ObDropTenantArg &arg)
{
  int ret = OB_SUCCESS;
  ObDDLSQLTransaction trans(schema_service_);
  const bool if_exist = arg.if_exist_;
  bool drop_force = !arg.delay_to_drop_;
  const bool open_recyclebin = arg.open_recyclebin_;
  const ObTenantSchema *tenant_schema = NULL;
  ObSchemaGetterGuard schema_guard;
  ObArray<ObResourcePoolName> pool_names;
  ObArray<share::ObResourcePool*> pools;
  ret = OB_E(EventTable::EN_DROP_TENANT_FAILED) OB_SUCCESS;
  bool is_standby = false;
  uint64_t user_tenant_id = common::OB_INVALID_ID;
  int64_t refreshed_schema_version = 0;
  common::ObArray<uint64_t> drop_ug_id_array;
  bool specify_tenant_id = OB_INVALID_TENANT_ID != arg.tenant_id_;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init");
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", K(ret));
  } else if (specify_tenant_id && OB_FAIL(schema_guard.get_tenant_info(arg.tenant_id_, tenant_schema))) {
    LOG_WARN("fail to gt tenant info", KR(ret), K(arg));
  } else if (!specify_tenant_id && OB_FAIL(schema_guard.get_tenant_info(arg.tenant_name_, tenant_schema))) {
    LOG_WARN("fail to gt tenant info", KR(ret), K(arg));
  } else if (OB_ISNULL(tenant_schema)) {
    if (if_exist) {
      LOG_USER_NOTE(OB_TENANT_NOT_EXIST, arg.tenant_name_.length(), arg.tenant_name_.ptr());
      LOG_INFO("tenant not exist, no need to delete it", K(arg));
    } else {
      ret = OB_TENANT_NOT_EXIST;
      LOG_USER_ERROR(OB_TENANT_NOT_EXIST, arg.tenant_name_.length(), arg.tenant_name_.ptr());
      LOG_WARN("tenant not exist, can't delete it", K(arg), KR(ret));
    }
  } else if (FALSE_IT(user_tenant_id = tenant_schema->get_tenant_id())) {
  } else if (!is_user_tenant(user_tenant_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't drop sys or meta tenant", KR(ret), K(user_tenant_id));
  } else if (drop_force) {
    //is drop force, no need to check
    //pay attention
  } else if (tenant_schema->is_in_recyclebin()) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant in recyclebin, can't delete it", K(arg), KR(ret));
    LOG_USER_ERROR(OB_TENANT_NOT_EXIST, arg.tenant_name_.length(), arg.tenant_name_.ptr());
  } else if (tenant_schema->is_restore() ||
          tenant_schema->is_creating() || tenant_schema->is_dropping()) {
    // Due to the particularity of restore tenants, in order to avoid abnormal behavior of the cluster,
    // restore tenants cannot be placed in the recycle bin.
    // The creating state is the intermediate state of tenant creation, and it will become the normal state
    // if it is successfully created
    // The dropping state is the previous delayed deletion state. The two states are managed by the gc thread,
    // responsible for deletion and cannot be placed in the recycle bin.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("drop tenant to recyclebin is not supported", KR(ret), K(arg));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "should drop tenant force, delay drop tenant");
  } else {
    ObAllTenantInfo tenant_info;
    if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
          user_tenant_id, sql_proxy_, false, tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K(arg), K(user_tenant_id));
    } else if (!tenant_info.is_primary() && !tenant_info.is_standby()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("drop tenant not in primary or standby role is not supported",
               KR(ret), K(arg), K(tenant_info));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "should drop tenant force, drop tenant");
    } else if (tenant_info.is_standby() && !open_recyclebin) {
      //if standby tenant and no recyclebin, need drop force
      drop_force = true;
      FLOG_INFO("is standby tenant, need drop force", K(tenant_info));
    }
  }
  if (OB_FAIL(ret)) {
    // ignore
  } else if (OB_ISNULL(tenant_schema)) {
    // We need to ignore the drop tenant if exists statement in the case that the tenant has already been deleted
  } else if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, refreshed_schema_version))) {
    LOG_WARN("failed to get tenant schema version", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
    LOG_WARN("start transaction failed", KR(ret), K(user_tenant_id), K(refreshed_schema_version));
  } else {
    /*
    * drop tenant force: delay_to_drop_ is false
    * delay_to_drop_ is true in rest the situation
    * drop tenant && recyclebin enable: in recyclebin
    * (drop tenant && recyclebin disable) || drop tenant purge: delay delete
    */
    const bool to_recyclebin = (arg.delay_to_drop_ && open_recyclebin);
    const bool delay_to_drop = (arg.delay_to_drop_ && !open_recyclebin);
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    //1.drop tenant force
    if (drop_force) {
      const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
      if (arg.drop_only_in_restore_) {
        // if drop_restore_tenant is true, it demands that the tenant must be in restore status after drop tenant trans start.
        if (OB_ISNULL(tenant_schema)) {
          ret = OB_TENANT_NOT_EXIST;
          LOG_USER_ERROR(OB_TENANT_NOT_EXIST, arg.tenant_name_.length(), arg.tenant_name_.ptr());
          LOG_WARN("tenant not exist, can't delete it", K(arg), KR(ret));
        } else if (!tenant_schema->is_restore()) {
          ret = OB_OP_NOT_ALLOW;
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Cancel tenant not in restore is");
          LOG_WARN("Cancel tenant not in restore is not allowed", K(ret), K(user_tenant_id));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(drop_resource_pool_pre(
              user_tenant_id, drop_ug_id_array, pool_names, trans))) {
        LOG_WARN("fail to drop resource pool pre", KR(ret));
      } else if (OB_FAIL(ddl_operator.drop_tenant(user_tenant_id, trans, &arg.ddl_stmt_str_))) {
        LOG_WARN("ddl_operator drop_tenant failed", K(user_tenant_id), KR(ret));
      } else if (OB_FAIL(ddl_operator.drop_tenant(meta_tenant_id, trans))) {
        LOG_WARN("ddl_operator drop_tenant failed", K(meta_tenant_id), KR(ret));
      } else if (OB_FAIL(try_drop_sys_ls_(meta_tenant_id, trans))) {
        LOG_WARN("failed to drop sys ls", KR(ret), K(meta_tenant_id));
      } else if (tenant_schema->is_in_recyclebin()) {
        // try recycle record from __all_recyclebin
        ObArray<ObRecycleObject> recycle_objs;
        ObSchemaService *schema_service_impl = NULL;
        if (OB_ISNULL(schema_service_)
            || OB_ISNULL(schema_service_->get_schema_service())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema service is null", KR(ret), KP_(schema_service));
        } else if (FALSE_IT(schema_service_impl = schema_service_->get_schema_service())) {
        } else if (OB_FAIL(schema_service_impl->fetch_recycle_object(
                           OB_SYS_TENANT_ID,
                           tenant_schema->get_tenant_name_str(),
                           ObRecycleObject::TENANT,
                           trans,
                           recycle_objs))) {
            LOG_WARN("get_recycle_object failed", KR(ret), KPC(tenant_schema));
        } else if (0 == recycle_objs.size()) {
          // skip
        } else if (1 < recycle_objs.size()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("records should not be more than 1",
                   KR(ret), KPC(tenant_schema), K(recycle_objs));
        } else if (OB_FAIL(schema_service_impl->delete_recycle_object(
                           OB_SYS_TENANT_ID,
                           recycle_objs.at(0),
                           trans))) {
          LOG_WARN("delete_recycle_object failed", KR(ret), KPC(tenant_schema));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ddl_service_->reset_parallel_cache(meta_tenant_id))) {
          LOG_WARN("fail to reset parallel cache", KR(ret), K(meta_tenant_id));
        } else if (OB_FAIL(ddl_service_->reset_parallel_cache(user_tenant_id))) {
          LOG_WARN("fail to reset parallel cache", KR(ret), K(user_tenant_id));
        }
      }
    } else {// put tenant into recyclebin
      ObTenantSchema new_tenant_schema;
      ObSqlString new_tenant_name;
      if (OB_FAIL(new_tenant_schema.assign(*tenant_schema))) {
        LOG_WARN("failed to assign tenant schema", KR(ret), KPC(tenant_schema));
      } else if (OB_FAIL(ddl_operator.construct_new_name_for_recyclebin(
              new_tenant_schema, new_tenant_name))) {
        LOG_WARN("fail to construct new name", K(ret));
      } else if (to_recyclebin) {
        //2. tenant in recyclebin
        if (new_tenant_name.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant name is null", K(ret));
        } else if (OB_FAIL(ddl_operator.drop_tenant_to_recyclebin(
                new_tenant_name,
                new_tenant_schema,
                trans, &arg.ddl_stmt_str_))) {
          LOG_WARN("fail to drop tenant in recyclebin", KR(ret), K(user_tenant_id));
        }
      } else if (delay_to_drop) {
        //3. tenant delay delete
        if (OB_FAIL(ddl_operator.delay_to_drop_tenant(new_tenant_schema, trans,
                &arg.ddl_stmt_str_))) {
          LOG_WARN("fail to delay_to drop tenant", K(ret));
        } else {
          // ObLSManager will process force_drop_tenant() logic each 100ms.
        }
      }
    }
  }

  if (trans.is_started()) {
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
      ret = (OB_SUCC(ret)) ? temp_ret : ret;
    }
  }

  if (drop_force) {
    if (OB_SUCC(ret) && OB_NOT_NULL(tenant_schema)) {
      if (OB_FAIL(drop_resource_pool_final(
              tenant_schema->get_tenant_id(), drop_ug_id_array,
              pool_names))) {
        LOG_WARN("fail to drop resource pool finsl", KR(ret), KPC(tenant_schema));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(publish_schema(OB_SYS_TENANT_ID))) {
      LOG_WARN("publish schema failed", KR(ret));
    }
  }

  LOG_INFO("drop tenant", K(arg), KR(ret));
  return ret;
}

int ObTenantDDLService::flashback_tenant(const obrpc::ObFlashBackTenantArg &arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = NULL;
  ObArenaAllocator allocator(ObModIds::OB_TENANT_INFO);
  ObString final_tenant_name;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (arg.tenant_id_ != OB_SYS_TENANT_ID) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "falshback tenant must in sys tenant");
    LOG_WARN("falshback tenant must in sys tenant", K(ret));
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(
                     OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(arg.origin_tenant_name_, tenant_schema))) {
    LOG_WARN("failt to get tenant info", K(ret));
  } else if (OB_ISNULL(tenant_schema)) {
    const bool is_flashback = true;
    ObString new_tenant_name;
    if (OB_FAIL(get_tenant_object_name_with_origin_name_in_recyclebin(arg.origin_tenant_name_,
                                                                      new_tenant_name, &allocator,
                                                                      is_flashback))) {
      LOG_WARN("fail to get tenant obfect name", K(ret));
    } else if (OB_FAIL(schema_guard.get_tenant_info(new_tenant_name, tenant_schema))) {
      LOG_WARN("fail to get tenant info", K(ret));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant name is not exist", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (!tenant_schema->is_in_recyclebin()) {
      ret = OB_ERR_OBJECT_NOT_IN_RECYCLEBIN;
      LOG_WARN("tenant schema is not in recyclebin", K(ret), K(arg), K(*tenant_schema));
    } else if (!arg.new_tenant_name_.empty()) {
      final_tenant_name = arg.new_tenant_name_;
    } else {}//nothing todo
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(flashback_tenant_in_trans(*tenant_schema,
                                          final_tenant_name,
                                          schema_guard,
                                          arg.ddl_stmt_str_))) {
      LOG_WARN("flashback tenant in trans failed", K(ret));
    } else if (OB_FAIL(publish_schema(OB_SYS_TENANT_ID))) {
      LOG_WARN("publish_schema failed", K(ret));
    }
  }
  LOG_INFO("finish flashback tenant", K(arg), K(ret));
  return ret;
}

int ObTenantDDLService::flashback_tenant_in_trans(const share::schema::ObTenantSchema &tenant_schema,
                                            const ObString &new_tenant_name,
                                            share::schema::ObSchemaGetterGuard &schema_guard,
                                            const ObString &ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    ObDDLSQLTransaction trans(schema_service_);
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret));
    } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(refreshed_schema_version));
    } else if (OB_FAIL(ddl_operator.flashback_tenant_from_recyclebin(tenant_schema,
                                                                     trans,
                                                                     new_tenant_name,
                                                                     schema_guard,
                                                                     ddl_stmt_str))) {
      LOG_WARN("flashback tenant from recyclebin failed", K(ret));
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
  }
  return ret;
}

int ObTenantDDLService::get_tenant_object_name_with_origin_name_in_recyclebin(
    const ObString &origin_tenant_name,
    ObString &object_name,
    common::ObIAllocator *allocator,
    const bool is_flashback)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    const char *desc_or_asc = (is_flashback ? "desc" : "asc");
    if (OB_FAIL(sql.append_fmt(
        "select object_name from oceanbase.__all_recyclebin where "
        "original_name = '%.*s' and TYPE = 7 order by gmt_create %s limit 1",
        origin_tenant_name.length(),
        origin_tenant_name.ptr(),
        desc_or_asc))) {
      LOG_WARN("failed to append sql",
             K(ret), K(origin_tenant_name), K(*desc_or_asc));
    } else if (OB_FAIL(sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
      LOG_WARN("failed to execute sql", K(sql), K(ret));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get result", K(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_ERR_OBJECT_NOT_IN_RECYCLEBIN;
        LOG_WARN("origin tenant_name not exist in recyclebin", K(ret), K(sql));
      } else {
        LOG_WARN("iterate next result fail", K(ret), K(sql));
      }
    } else {
      ObString tmp_object_name;
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "object_name", tmp_object_name);
      if (OB_FAIL(deep_copy_ob_string(*allocator, tmp_object_name, object_name))) {
        LOG_WARN("failed to deep copy member list", K(ret), K(object_name));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ITER_END != result->next()) {
      // The result will not exceed one line
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result failed", K(ret), K(sql));
    }
  }
  return ret;
}

int ObTenantDDLService::purge_tenant(
    const obrpc::ObPurgeTenantArg &arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = NULL;
  ObArenaAllocator allocator(ObModIds::OB_TENANT_INFO);
  ObArray<ObResourcePoolName> pool_names;
  ObAllTenantInfo tenant_info;
  bool is_standby_tenant = false;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (arg.tenant_id_ != OB_SYS_TENANT_ID) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "purge tenant must in sys tenant");
    LOG_WARN("purge tenant must in sys tenant", K(ret));
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(
                     OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(arg.tenant_name_, tenant_schema))) {
    LOG_WARN("fail to get tenant info", K(ret));
  } else if (OB_ISNULL(tenant_schema)) {
    const bool is_flashback = false;
    ObString new_tenant_name;
    if (OB_FAIL(get_tenant_object_name_with_origin_name_in_recyclebin(arg.tenant_name_,
                                                                      new_tenant_name, &allocator,
                                                                      is_flashback))) {
      LOG_WARN("fail to get tenant obfect name", K(ret));
    } else if (OB_FAIL(schema_guard.get_tenant_info(new_tenant_name, tenant_schema))) {
      LOG_WARN("fail to get tenant info", K(ret));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant name is not exist", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (!tenant_schema->is_in_recyclebin()) {
      ret = OB_ERR_OBJECT_NOT_IN_RECYCLEBIN;
      LOG_WARN("tenant not in recyclebin, can not be purge", K(arg), K(*tenant_schema), K(ret));
    }
  }
  if (FAILEDx(ObAllTenantInfoProxy::load_tenant_info(
          tenant_schema->get_tenant_id(), sql_proxy_, false, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(arg), KPC(tenant_schema));
  } else if (FALSE_IT(is_standby_tenant = tenant_info.is_standby())) {
    //can not be there
  }

  if (OB_FAIL(ret)) {
  } else if (is_standby_tenant) {
    //drop tenant force
    if (OB_FAIL(try_force_drop_tenant(*tenant_schema))) {
      LOG_WARN("failed to try drop tenant force", KR(ret), KPC(tenant_schema));
    }
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    ObDDLSQLTransaction trans(schema_service_);
    const uint64_t tenant_id = tenant_schema->get_tenant_id();
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret));
    } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(refreshed_schema_version));
    } else if (OB_FAIL(ddl_operator.purge_tenant_in_recyclebin(
                       *tenant_schema,
                       trans,
                       &arg.ddl_stmt_str_))) {
      LOG_WARN("purge tenant failed", K(ret));
    }

    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(publish_schema(OB_SYS_TENANT_ID))) {
        LOG_WARN("publish_schema failed", K(ret));
      }
    }
  }
  LOG_INFO("finish purge tenant", K(arg), K(ret));
  return ret;
}

int ObTenantDDLService::lock_tenant(const ObString &tenant_name, const bool is_lock)
{
  int ret = OB_SUCCESS;
  ObDDLSQLTransaction trans(schema_service_);
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = NULL;
  int64_t refreshed_schema_version = 0;
  ObTenantSchema new_tenant_schema;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init");
  } else if (tenant_name.length() <= 0) {
    ret = OB_INVALID_TENANT_NAME;
    LOG_WARN("invalid tenant name", K(tenant_name), K(ret));
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_name, tenant_schema)) ||
      NULL == tenant_schema) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist, can't lock it", K(tenant_name), K(ret));
  } else if (tenant_schema->get_locked() == is_lock) {
    ret = OB_SUCCESS;
  } else if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, refreshed_schema_version))) {
    LOG_WARN("failed to get tenant schema version", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
    LOG_WARN("start transaction failed", KR(ret), K(refreshed_schema_version));
  } else if (OB_FAIL(new_tenant_schema.assign(*tenant_schema))) {
    LOG_WARN("fail to assign tenant schema", KR(ret));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    new_tenant_schema.set_locked(is_lock);
    if (OB_FAIL(ddl_operator.alter_tenant(new_tenant_schema, trans))) {
      LOG_WARN("ddl_operator alter tenant failed", K(new_tenant_schema), K(ret));
    }
  }

  if (trans.is_started()) {
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
      ret = (OB_SUCC(ret)) ? temp_ret : ret;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(publish_schema(OB_SYS_TENANT_ID))) {
      LOG_WARN("publish schema failed", K(ret));
    }
  }
  return ret;
}

int ObTenantDDLService::commit_alter_tenant_locality(
    const rootserver::ObCommitAlterTenantLocalityArg &arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *orig_tenant_schema = NULL;
  const ObTenantSchema *orig_meta_tenant_schema = NULL;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(arg.tenant_id_, orig_tenant_schema))) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", KR(ret), "tenant_id", arg.tenant_id_);
  } else if (OB_UNLIKELY(NULL == orig_tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", KR(ret), "tenant_id", arg.tenant_id_);
  } else if (OB_UNLIKELY(orig_tenant_schema->get_locality_str().empty())
             || OB_UNLIKELY(orig_tenant_schema->get_previous_locality_str().empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant locality status error", KR(ret),
             "tenant_id", orig_tenant_schema->get_tenant_id(),
             "tenant locality", orig_tenant_schema->get_locality_str(),
             "tenant previous locality", orig_tenant_schema->get_previous_locality_str());
  } else {
    // deal with meta tenant related to certain user tenant
    if (is_user_tenant(arg.tenant_id_)) {
      if (OB_FAIL(schema_guard.get_tenant_info(gen_meta_tenant_id(arg.tenant_id_), orig_meta_tenant_schema))) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("meta tenant not exist", KR(ret), "meta_tenant_id", gen_meta_tenant_id(arg.tenant_id_));
      } else if (OB_UNLIKELY(NULL == orig_meta_tenant_schema)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("meta tenant not exist", KR(ret), "meta_tenant_id", gen_meta_tenant_id(arg.tenant_id_));
      } else if (OB_UNLIKELY(orig_meta_tenant_schema->get_locality_str().empty())
                 || OB_UNLIKELY(orig_meta_tenant_schema->get_previous_locality_str().empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("meta tenant locality status error", KR(ret),
                 "meta tenant_id", orig_meta_tenant_schema->get_tenant_id(),
                 "meta tenant locality", orig_meta_tenant_schema->get_locality_str(),
                 "meta tenant previous locality", orig_meta_tenant_schema->get_previous_locality_str());
      }
    }
    if (OB_SUCC(ret)) {
      ObDDLSQLTransaction trans(schema_service_);
      int64_t refreshed_schema_version = 0;
      if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, refreshed_schema_version))) {
        LOG_WARN("failed to get tenant schema version", KR(ret));
      } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
        LOG_WARN("fail to start transaction", KR(ret), K(refreshed_schema_version));
      } else {
        ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
        ObTenantSchema new_tenant_schema;
        ObTenantSchema new_meta_tenant_schema;
        // refresh sys/user tenant schema
        if (OB_FAIL(new_tenant_schema.assign(*orig_tenant_schema))) {
          LOG_WARN("fail to assign tenant schema", KR(ret), KPC(orig_tenant_schema));
        } else if (OB_FAIL(new_tenant_schema.set_previous_locality(ObString::make_string("")))) {
          LOG_WARN("fail to set previous locality", KR(ret));
        } else if (OB_FAIL(ddl_operator.alter_tenant(new_tenant_schema, trans))) {
          LOG_WARN("fail to alter tenant", KR(ret), K(new_tenant_schema));
        } else {
          // refresh meta tenant schema
          if (is_user_tenant(new_tenant_schema.get_tenant_id())) {
            if (OB_FAIL(new_meta_tenant_schema.assign(*orig_meta_tenant_schema))) {
              LOG_WARN("fail to assign meta tenant schema", KR(ret), KPC(orig_meta_tenant_schema));
            } else if (OB_FAIL(new_meta_tenant_schema.set_previous_locality(ObString::make_string("")))) {
              LOG_WARN("fail to set meta tenant previous locality", KR(ret));
            } else if (OB_FAIL(ddl_operator.alter_tenant(new_meta_tenant_schema, trans))) {
              LOG_WARN("fail to alter meta tenant", KR(ret));
            }
          }
        }
        int temp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("trans end failed", K(temp_ret), "is_commit", OB_SUCCESS == ret);
          ret = (OB_SUCCESS == ret ? temp_ret : ret);
        } else {} // ok

        if (OB_SUCC(ret)) {
          if (OB_FAIL(publish_schema(OB_SYS_TENANT_ID))) {// force return success
            LOG_WARN("fail to publish schema", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantDDLService::get_tenant_external_consistent_ts(const int64_t tenant_id, SCN &scn)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_us = THIS_WORKER.is_timeout_ts_valid() ?
      THIS_WORKER.get_timeout_remain() : GCONF.rpc_timeout;
  bool is_external_consistent = false;
  if (OB_FAIL(transaction::ObTsMgr::get_instance().get_ts_sync(tenant_id, timeout_us, scn,
                                                               is_external_consistent))) {
    LOG_WARN("fail to get_ts_sync", K(ret), K(tenant_id));
  } else if (!is_external_consistent) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("got ts of tenant is not external consistent", K(ret), K(tenant_id), K(scn),
             K(is_external_consistent));
  } else {
    LOG_INFO("success to get_tenant_external_consistent_ts", K(tenant_id), K(scn),
             K(is_external_consistent));
  }
  return ret;
}

int ObTenantDDLService::create_tenant_end(const uint64_t tenant_id)
{
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("[CREATE_TENANT] STEP 3. start create tenant end", K(tenant_id));
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = NULL;
  ObAllTenantInfo tenant_info;
  int64_t sys_schema_version = OB_INVALID_VERSION;
  ObDDLSQLTransaction trans(schema_service_, true, false, false, false);
  DEBUG_SYNC(BEFORE_CREATE_TENANT_END);
  ObTenantSchema new_tenant_schema;
  ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
  ObRefreshSchemaStatus schema_status;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (OB_ISNULL(schema_status_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema status proxy", KR(ret));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
          tenant_id, sql_proxy_, false, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret), KP_(schema_service));
  /*
    After the inner-table is synchronized by the network standby tenant, the schema refresh switch
    is turned on, but standby tenant may not be in the same observer with RS, causing RS to use the
    old cache when creating tenant end, which may cause create tenant end to fail.
    So here, force trigger schema refresh refresh cache
  */
  } else if (OB_FAIL(schema_status_proxy->load_refresh_schema_status(tenant_id, schema_status))) {
    LOG_WARN("fail to load refresh schema status", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", K(ret));
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", K(ret));
  } else if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, sys_schema_version))) {
    LOG_WARN("fail to get tenant schema version", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, sys_schema_version))) {
    LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(sys_schema_version));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", K(ret), K(tenant_id));
  } else if (tenant_schema->is_normal()) {
    // skip, Guaranteed reentrant
  } else if (!tenant_schema->is_creating()
             && !tenant_schema->is_restore()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("state not match", K(ret), K(tenant_id));
  } else if (OB_FAIL(new_tenant_schema.assign(*tenant_schema))) {
    LOG_WARN("fail to assign tenant schema", KR(ret));
  } else {
    ObDDLSQLTransaction tenant_trans(schema_service_);
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    int64_t refreshed_schema_version = OB_INVALID_VERSION;
    if (!tenant_info.is_standby()) {
      // Push the system tenant schema_version, and the standalone cluster may fail due to unsynchronized heartbeat.
      // The standalone cluster uses the synchronized schema_version,
      // and there is no need to increase the system tenant schema_version.
      int64_t new_schema_version = OB_INVALID_VERSION;
      ObSchemaService *schema_service_impl = schema_service_->get_schema_service();
      // Ensure that the schema_version monotonically increases among tenants' cross-tenant transactions
      //
      if (OB_ISNULL(schema_service_impl)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_service_impl is null", K(ret));
      } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
        LOG_WARN("fail to get tenant schema version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(tenant_trans.start(sql_proxy_, tenant_id, refreshed_schema_version))) {
        LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
      } else {
        refreshed_schema_version = sys_schema_version > refreshed_schema_version ? sys_schema_version : refreshed_schema_version;
        if (OB_FAIL(schema_service_impl->gen_new_schema_version(OB_SYS_TENANT_ID, refreshed_schema_version, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      const ObString *ddl_stmt_str_ptr = NULL;
      const int64_t DDL_STR_BUF_SIZE = 128;
      char ddl_str_buf[DDL_STR_BUF_SIZE];
      MEMSET(ddl_str_buf, 0, DDL_STR_BUF_SIZE);
      ObString ddl_stmt_str;
      if (tenant_schema->is_restore()) {
        SCN gts;
        int64_t pos = 0;
        if (OB_FAIL(get_tenant_external_consistent_ts(tenant_id, gts))) {
          SERVER_LOG(WARN, "failed to get_tenant_gts", KR(ret), K(tenant_id));
        } else if (OB_FAIL(databuff_printf(ddl_str_buf, DDL_STR_BUF_SIZE, pos,
                                           "schema_version=%ld; tenant_gts=%lu",
                                           refreshed_schema_version, gts.get_val_for_inner_table_field()))) {
          SERVER_LOG(WARN, "failed to construct ddl_stmt_str", KR(ret), K(tenant_id), K(refreshed_schema_version), K(gts));
        } else {
          ddl_stmt_str.assign_ptr(ddl_str_buf, pos);
          ddl_stmt_str_ptr = &ddl_stmt_str;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ddl_operator.create_tenant(new_tenant_schema, OB_DDL_ADD_TENANT_END, trans, ddl_stmt_str_ptr))) {
        LOG_WARN("create tenant failed", K(new_tenant_schema), K(ret));
      } else {/*do nothing*/}
    }

    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_CREATE_TENANT_TRANS_THREE_FAILED) OB_SUCCESS;
    }
    int temp_ret = OB_SUCCESS;
    if (trans.is_started()) {
      LOG_INFO("end create tenant", "is_commit", OB_SUCCESS == ret, K(ret));
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
      }
    }
    if (tenant_trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      const bool is_commit = false;//no need commit, only for check and lock
      if (OB_SUCCESS != (temp_ret = tenant_trans.end(is_commit))) {
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
        LOG_WARN("trans end failed",  KR(ret), KR(temp_ret), K(is_commit));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_SUCCESS != (temp_ret = publish_schema(OB_SYS_TENANT_ID))) {
        LOG_WARN("publish schema failed", K(temp_ret));
      }
    }
  }
  FLOG_INFO("[CREATE_TENANT] STEP 3. finish create tenant end", KR(ret), K(tenant_id),
           "cost", ObTimeUtility::fast_current_time() - start_time);
  return ret;
}

int ObTenantDDLService::refresh_creating_tenant_schema_(const ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  int tenant_id = tenant_schema.get_tenant_id();
  ObAddrArray addrs;
  ObAddr leader;
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = tenant_id;
  int64_t baseline_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (!tenant_schema.is_creating()) {
    // only primary creating tenant need to refresh schema
  } else if (OB_FAIL(get_ls_member_list_for_creating_tenant_(tenant_id, ObLSID::SYS_LS_ID,
          leader, addrs))) {
    LOG_WARN("failed to get sys ls member list", KR(ret));
  } else if (OB_FAIL(publish_schema(tenant_id, addrs))) {
    LOG_WARN("fail to publish schema", KR(ret), K(tenant_id), K(addrs));
    // set baseline schema version
  } else if (OB_FAIL(schema_service_->get_schema_version_in_inner_table(
          *sql_proxy_, schema_status, baseline_schema_version))) {
    LOG_WARN("fail to gen new schema version", KR(ret), K(schema_status));
  } else {
    ObGlobalStatProxy global_stat_proxy(*sql_proxy_, tenant_id);
    if (OB_FAIL(global_stat_proxy.set_baseline_schema_version(baseline_schema_version))) {
      LOG_WARN("fail to set baseline schema version",
          KR(ret), K(tenant_id), K(baseline_schema_version));
    }
  }
  return ret;
}

int ObTenantDDLService::init_tenant_global_stat_(
    const uint64_t tenant_id,
    const common::ObIArray<common::ObConfigPairs> &init_configs,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[CREATE_TENANT] STEP 2.4.1 start init global stat", K(tenant_id));
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("failed to check_inner_stat", KR(ret));
  } else {
    const int64_t core_schema_version = OB_CORE_SCHEMA_VERSION + 1;
    const int64_t baseline_schema_version = OB_INVALID_VERSION;
    const int64_t ddl_epoch = 0;
    const SCN snapshot_gc_scn  = SCN::min_scn();
    // find compatible version
    uint64_t data_version = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < init_configs.count(); i++) {
      const ObConfigPairs &config = init_configs.at(i);
      if (tenant_id == config.get_tenant_id()) {
        for (int64_t j = 0; data_version == 0 && OB_SUCC(ret) && j < config.get_configs().count(); j++) {
          const ObConfigPairs::ObConfigPair &pair = config.get_configs().at(j);
          if (0 != pair.key_.case_compare("compatible")) {
          } else if (OB_FAIL(ObClusterVersion::get_version(pair.value_.ptr(), data_version))) {
            LOG_WARN("fail to get compatible version", KR(ret), K(tenant_id), K(pair));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      ObGlobalStatProxy global_stat_proxy(trans, tenant_id);
      if (OB_FAIL(ret)) {
      } else if (0 == data_version) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("compatible version not defined", KR(ret), K(tenant_id), K(init_configs));
      } else if (OB_FAIL(global_stat_proxy.set_tenant_init_global_stat(
              core_schema_version, baseline_schema_version,
              snapshot_gc_scn, ddl_epoch, data_version, data_version, data_version))) {
        LOG_WARN("fail to set tenant init global stat", KR(ret), K(tenant_id),
            K(core_schema_version), K(baseline_schema_version),
            K(snapshot_gc_scn), K(ddl_epoch), K(data_version));
      } else if (is_user_tenant(tenant_id) && OB_FAIL(OB_STANDBY_SERVICE.write_upgrade_barrier_log(
              trans, tenant_id, data_version))) {
        LOG_WARN("fail to write_upgrade_barrier_log", KR(ret), K(tenant_id), K(data_version));
      } else if (is_user_tenant(tenant_id) &&
          OB_FAIL(OB_STANDBY_SERVICE.write_upgrade_data_version_barrier_log(
              trans, tenant_id, data_version))) {
        LOG_WARN("fail to write_upgrade_data_version_barrier_log", KR(ret),
            K(tenant_id), K(data_version));
      }
    }
  }
  FLOG_INFO("[CREATE_TENANT] STEP 2.4.1 finish init global stat", KR(ret), K(tenant_id));
  return ret;
}

int ObTenantDDLService::get_ls_member_list_for_creating_tenant_(
    const uint64_t tenant_id,
    const int64_t ls_id,
    ObAddr &leader,
    ObIArray<ObAddr> &addrs)
{
  int ret = OB_SUCCESS;
  bool is_cache_hit = false;
  ObLSLocation location;
  ObLSID ls(ls_id);
  leader.reset();
  if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.location_service_));
  } else if (OB_FAIL(GCTX.location_service_->get(GCONF.cluster_id, tenant_id, ls,
            0/*expire_renew_time*/, is_cache_hit, location))) {
    LOG_WARN("fail to get sys ls info", KR(ret), K(tenant_id));
  } else {
    const ObIArray<ObLSReplicaLocation> &replica_locations = location.get_replica_locations();
    for (int64_t i = 0; i < replica_locations.count() && OB_SUCC(ret); i++) {
      const ObLSReplicaLocation &replica = replica_locations.at(i);
      const ObAddr &addr = replica.get_server();
      if (!addr.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("addr is invalid", KR(ret), K(addr));
      } else if (OB_FAIL(addrs.push_back(addr))) {
        LOG_WARN("failed to push_back replica addr", KR(ret), K(replica));
      } else if (replica.is_strong_leader()) {
        leader = addr;
      }
    }
  }
  return ret;
}

int ObTenantDDLService::set_tenant_schema_charset_and_collation(
    ObTenantSchema &tenant_schema,
    const ObCreateTenantArg &create_tenant_arg)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  const ObTenantSchema &src_tenant_schema = create_tenant_arg.tenant_schema_;
  if (!tenant_schema.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_schema is invalid", KR(ret));
  } else if (FALSE_IT(tenant_id = tenant_schema.get_tenant_id())) {
  } else if (is_user_tenant(tenant_id)) {
    tenant_schema.set_charset_type(src_tenant_schema.get_charset_type());
    tenant_schema.set_collation_type(src_tenant_schema.get_collation_type());
  } else if (is_meta_tenant(tenant_id)) {
    tenant_schema.set_charset_type(ObCharset::get_default_charset());
    tenant_schema.set_collation_type(ObCharset::get_default_collation(
          ObCharset::get_default_charset()));
  }
  return ret;
}

int ObTenantDDLService::create_tenant_check_(const obrpc::ObCreateTenantArg &arg,
    bool &need_create,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const ObString &tenant_name = arg.tenant_schema_.get_tenant_name_str();
  need_create = false;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_MISS_ARGUMENT;
    if (tenant_name.empty()) {
      LOG_USER_ERROR(OB_MISS_ARGUMENT, "tenant name");
    } else if (arg.pool_list_.count() <= 0) {
      LOG_USER_ERROR(OB_MISS_ARGUMENT, "resource_pool_list");
    }
    LOG_WARN("missing arg to create tenant", KR(ret), K(arg));
  } else if (tenant_name.case_compare(OB_DIAG_TENANT_NAME) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_name \'diag\' is reserved for diagnose tenant", KR(ret), K(arg));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant_name (\'diag\' is reserved for diagnose tenant)");
  } else if (GCONF.in_upgrade_mode()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("create tenant when cluster is upgrading not allowed", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "create tenant when cluster is upgrading");
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(sql_proxy));
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else {
    // check tenant exist
    bool tenant_exist = false;
    if (OB_NOT_NULL(schema_guard.get_tenant_info(tenant_name))) {
      tenant_exist = true;
    } else {
      if (!arg.is_restore_tenant()) {
        if (OB_FAIL(ObRestoreUtil::check_has_physical_restore_job(*sql_proxy_, tenant_name, tenant_exist))) {
          LOG_WARN("failed to check has physical restore job", KR(ret), K(tenant_name));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (tenant_exist) {
        // do nothing
      } else if (!arg.is_clone_tenant()) {
        // check whether has clone job, if has clone job then tenant_exist should be true
        if (OB_FAIL(ObTenantCloneUtil::check_clone_tenant_exist(*sql_proxy_, tenant_name, tenant_exist))) {
          LOG_WARN("failed to check clone tenant exist", KR(ret), K(tenant_name));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (tenant_exist) {
      if (arg.if_not_exist_) {
        ret = OB_SUCCESS;
        LOG_USER_NOTE(OB_TENANT_EXIST, to_cstring(tenant_name));
        LOG_INFO("tenant already exists, not need to create", KR(ret), K(tenant_name));
      } else {
        ret = OB_TENANT_EXIST;
        LOG_USER_ERROR(OB_TENANT_EXIST, to_cstring(tenant_name));
        LOG_WARN("tenant already exists", KR(ret), K(tenant_name));
      }
    } else {
      need_create = true;
    }
  }
  return ret;
}
}
}
