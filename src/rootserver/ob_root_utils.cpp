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

#include "ob_root_utils.h"
#include "ob_balance_info.h"
#include "ob_unit_manager.h"
#include "lib/json/ob_json.h"
#include "lib/string/ob_sql_string.h"
#include "lib/hash/ob_hashset.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_share_util.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_multi_version_schema_service.h" // ObMultiVersionSchemaService
#include "share/schema/ob_schema_getter_guard.h" // ObSchemaGetterGuard
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "storage/tx/ob_ts_mgr.h"
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_ddl_service.h"
#include "observer/ob_server_struct.h"
#include "logservice/palf_handle_guard.h"
#include "logservice/ob_log_service.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "share/ob_primary_zone_util.h"           // ObPrimaryZoneUtil
#include "share/ob_server_table_operator.h"
#include "share/ob_zone_table_operation.h"
#include "rootserver/ob_tenant_balance_service.h"    // for ObTenantBalanceService

using namespace oceanbase::rootserver;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::obrpc;

int ObTenantUtils::get_tenant_ids(
    ObMultiVersionSchemaService *schema_service,
    ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(schema_service)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema service not init", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", K(ret));
  }
  return ret;
}

int ObTenantUtils::get_tenant_ids(
    ObMultiVersionSchemaService *schema_service,
    int64_t &sys_schema_version,
    ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(schema_service)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema service not init", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, sys_schema_version))) {
    LOG_WARN("fail to get schema version", KR(ret));
  }
  return ret;
}

bool ObTenantUtils::is_balance_target_schema(
     const share::schema::ObSimpleTableSchemaV2 &table_schema)
{
  return USER_TABLE == table_schema.get_table_type()
         || TMP_TABLE == table_schema.get_table_type()
         || MATERIALIZED_VIEW == table_schema.get_table_type()
         || TMP_TABLE_ORA_SESS == table_schema.get_table_type()
         || TMP_TABLE_ORA_TRX == table_schema.get_table_type()
         || TMP_TABLE_ALL == table_schema.get_table_type()
         || AUX_VERTIAL_PARTITION_TABLE == table_schema.get_table_type()
         || table_schema.is_global_index_table();
}


bool ObRootServiceRoleChecker::is_rootserver()
{
  bool bret = false;
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;

  MTL_SWITCH(tenant_id) {
    int64_t proposal_id = -1;
    ObRole role = FOLLOWER;
    palf::PalfHandleGuard palf_handle_guard;
    logservice::ObLogService *log_service = nullptr;

    if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("MTL ObLogService is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(log_service->open_palf(SYS_LS, palf_handle_guard))) {
      LOG_WARN("open palf failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(palf_handle_guard.get_role(role, proposal_id))) {
      LOG_WARN("get role failed", KR(ret), K(tenant_id));
    } else {
      bret = (is_strong_leader(role));
      LOG_DEBUG("get __all_core_table role", K(role), K(bret));
    }
  } else {
    if (OB_TENANT_NOT_IN_SERVER == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tenant storage", KR(ret), "tenant_id", OB_SYS_TENANT_ID);
    }
  }
  return bret;
}

const char *ObRootBalanceHelp::BalanceItem[] = {
  "ENABLE_REBUILD",
  "ENABLE_EMERGENCY_REPLICATE",
  "ENABLE_TYPE_TRANSFORM",
  "ENABLE_DELETE_REDUNDANT",
  "ENABLE_REPLICATE_TO_UNIT",
  "ENABLE_SHRINK",
  "ENABLE_REPLICATE",
  "ENABLE_COORDINATE_PG",
  "ENABLE_MIGRATE_TO_UNIT",
  "ENABLE_PARTITION_BALANCE",
  "ENABLE_UNIT_BALANCE",
  "ENABLE_SERVER_BALANCE",
  "ENABLE_CANCEL_UNIT_MIGRATION",
  "ENABLE_MODIFY_PAXOS_REPLICA_NUMBER",
  "ENABLE_STOP_SERVER",
  ""
};

int ObRootBalanceHelp::parse_balance_info(const ObString &json_str,
                                            ObRootBalanceHelp::BalanceController &switch_info)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);
  json::Parser parser;
  json::Value *data = NULL;
  switch_info.reset();
  if (json_str.empty()) {
    switch_info.init();
  } else if (OB_FAIL(parser.init(&allocator))) {
    LOG_WARN("json parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(json_str.ptr(), json_str.length(), data))) {
    LOG_WARN("parse json failed", K(ret), K(json_str));
  } else if (NULL == data) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no root value", K(ret));
  } else if (json::JT_OBJECT != data->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error json format", K(ret), K(json_str));
  } else {
    DLIST_FOREACH_X(it, data->get_object(), OB_SUCC(ret)) {
      bool find = false;
      for (int64_t i = 0; i < ARRAYSIZEOF(BalanceItem) - 1 && !find && OB_SUCC(ret); i++) {
        if (it->name_.case_compare(BalanceItem[i]) == 0) {
          find = true;
          if (json::JT_STRING != it->value_->get_type()) {
            ret = OB_INVALID_CONFIG;
            LOG_WARN("invalid config", K(ret), K(json_str));
          } else if (it->value_->get_string().case_compare("true") == 0) {
            switch_info.set(i, true);
          } else if (it->value_->get_string().case_compare("false") == 0) {
            switch_info.set(i, false);
          } else {
            ret = OB_INVALID_CONFIG;
            LOG_WARN("invalid config", K(ret), K(json_str));
          }
        } // if (it->name_.case_compare
      } //for (int64_t i = 0;
      if (!find) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("get invalid token", K(ret), K(*it));
      }
    } //DLIST_FOREACH_X(it
  }
  return ret;
}

int ObLocalityUtil::parse_zone_list_from_locality_str(ObString &locality_str,
                                                       ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  zone_list.reset();
  if (locality_str.empty()) {
    //nothing todo
  } else {
    ObArray<ObString> sub_locality;
    ObString trimed_string = locality_str.trim();
    if (OB_FAIL(split_on(trimed_string, ',', sub_locality))) {
      LOG_WARN("fail to split string", K(ret), "locality", trimed_string);
    } else {
      ObArray<ObString> zone_infos;
      ObZone tmp_zone;
      for (int64_t i = 0; i < sub_locality.count() && OB_SUCC(ret); i++) {
        zone_infos.reset();
        tmp_zone.reset();
        ObString sub_trimed_string = sub_locality.at(i).trim();
        if (OB_FAIL(split_on(sub_trimed_string, '@', zone_infos))) {
          LOG_WARN("fail to split on string", K(ret), "string", sub_trimed_string);
        } else if (zone_infos.count() != 2) {
          //FULL{1},READONLY{1}@z3;
          //nothing todo
        } else if (OB_FAIL(tmp_zone.assign(zone_infos.at(1)))) {
          LOG_WARN("fail to assign zone", K(ret), K(zone_infos));
        } else if (has_exist_in_array(zone_list, tmp_zone)) {
          //nothint todo
        } else if (OB_FAIL(zone_list.push_back(tmp_zone))) {
          LOG_WARN("fail to push back", K(ret));
        }
        LOG_DEBUG("split sub locality", K(ret), K(sub_trimed_string), K(tmp_zone), K(zone_infos), K(tmp_zone), K(zone_list));
      }
    }
  }
  return ret;
}

int ObTenantGroupParser::parse_tenant_groups(
    const common::ObString &ttg_str,
    common::ObIArray<TenantNameGroup> &tenant_groups)
{
  int ret = OB_SUCCESS;
  int64_t end = ttg_str.length();
  int64_t pos = 0;
  while (OB_SUCC(ret) && pos < end) {
    if (OB_FAIL(get_next_tenant_group(pos, end, ttg_str, tenant_groups))) {
      LOG_WARN("fail to parse single tenant group", K(ret), K(ttg_str));
    } else if (OB_FAIL(jump_to_next_ttg(pos, end, ttg_str))) {
      LOG_WARN("fail to jump to next", K(ret));
    }
  }
  return ret;
}

int ObTenantGroupParser::get_next_tenant_group(
    int64_t &pos,
    const int64_t end,
    const common::ObString &ttg_str,
    common::ObIArray<TenantNameGroup> &tenant_groups)
{
  int ret = OB_SUCCESS;
  jump_over_space(pos, end, ttg_str);
  if (pos >= end) {
    // reach to the end
  } else if ('(' != ttg_str[pos]) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
  } else {
    // begin with a left brace
    ++pos;
    jump_over_space(pos, end, ttg_str);
    if (pos >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else if ('"' == ttg_str[pos]) {
      ret = parse_vector_tenant_group(pos, end, ttg_str, tenant_groups);
    } else if ('(' == ttg_str[pos]) {
      ret = parse_matrix_tenant_group(pos, end, ttg_str, tenant_groups);
    } else {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
    }
    // end with a right brace
    if (OB_SUCC(ret)) {
      jump_over_space(pos, end, ttg_str);
      if (pos >= end) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
      } else if (')' != ttg_str[pos]) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
      } else {
        ++pos;
      }
    }
  }
  return ret;
}

int ObTenantGroupParser::get_next_tenant_name(
    int64_t &pos,
    const int64_t end,
    const common::ObString &ttg_str,
    common::ObString &tenant_name)
{
  int ret = OB_SUCCESS;
  jump_over_space(pos, end, ttg_str);
  if (pos >= end) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
  } else if ('"' != ttg_str[pos]) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
  } else {
    ++pos; // jump over the " symbol
    jump_over_space(pos, end, ttg_str);
    int64_t start = pos;
    while (pos < end && !isspace(ttg_str[pos]) && '"' != ttg_str[pos]) {
      ++pos;
    }
    if (pos >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else {
      tenant_name.assign_ptr(ttg_str.ptr() + start, static_cast<int32_t>(pos - start));
    }
    // end with a " symbol
    if (OB_SUCC(ret)) {
      jump_over_space(pos, end, ttg_str);
      if (pos >= end) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
      } else if ('"' != ttg_str[pos]) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
      } else {
        ++pos;
      }
    }
  }
  return ret;
}

int ObTenantGroupParser::jump_to_next_tenant_name(
    int64_t &pos,
    const int64_t end,
    const common::ObString &ttg_str)
{
  int ret = OB_SUCCESS;
  jump_over_space(pos, end, ttg_str);
  if (pos >= end) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
  } else if (',' == ttg_str[pos]) {
    ++pos;
    jump_over_space(pos, end, ttg_str);
    if (pos >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
    } else if ('"' != ttg_str[pos]) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
    } else {} // good, come across a " symbol
  } else if (')' == ttg_str[pos]) {
    // good, reach the end of this vector
  } else {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
  }
  return ret;
}

int ObTenantGroupParser::parse_tenant_vector(
    int64_t &pos,
    const int64_t end,
    const common::ObString &ttg_str,
    common::ObIArray<common::ObString> &tenant_names)
{
  int ret = OB_SUCCESS;
  tenant_names.reset();
  while (OB_SUCC(ret) && pos < end && ')' != ttg_str[pos]) {
    ObString tenant_name;
    if (OB_FAIL(get_next_tenant_name(pos, end, ttg_str, tenant_name))) {
      LOG_WARN("fail to get next tenant name", K(ret));
    } else if (has_exist_in_array(all_tenant_names_, tenant_name)) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else if (OB_FAIL(tenant_names.push_back(tenant_name))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(all_tenant_names_.push_back(tenant_name))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(jump_to_next_tenant_name(pos, end, ttg_str))) {
      LOG_WARN("fail to jump to next tenant name", K(ret));
    } else {} // next loop round
  }
  return ret;
}

int ObTenantGroupParser::parse_vector_tenant_group(
    int64_t &pos,
    const int64_t end,
    const common::ObString &ttg_str,
    common::ObIArray<TenantNameGroup> &tenant_groups)
{
  int ret = OB_SUCCESS;
  TenantNameGroup tenant_group;
  common::ObArray<common::ObString> tenant_names;
  if (OB_FAIL(parse_tenant_vector(pos, end, ttg_str, tenant_names))) {
    LOG_WARN("fail to parse tenant vector", K(ret));
  } else if (OB_FAIL(append(tenant_group.tenants_, tenant_names))) {
    LOG_WARN("fail to append", K(ret));
  } else {
    tenant_group.row_ = 1;
    tenant_group.column_ = tenant_group.tenants_.count();
    if (OB_FAIL(tenant_groups.push_back(tenant_group))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  return ret;
}

int ObTenantGroupParser::parse_matrix_tenant_group(
    int64_t &pos,
    const int64_t end,
    const common::ObString &ttg_str,
    common::ObIArray<TenantNameGroup> &tenant_groups)
{
  int ret = OB_SUCCESS;
  TenantNameGroup tenant_group;
  common::ObArray<common::ObString> tenant_names;
  bool first = true;
  // parse the second layer of two layer bracket structure
  while (OB_SUCC(ret) && pos < end && ')' != ttg_str[pos]) {
    tenant_names.reset();
    jump_over_space(pos, end, ttg_str);
    if (pos >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else if ('(' != ttg_str[pos]) { // start with left brace
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else {
      ++pos; // jump over left brace
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(parse_tenant_vector(pos, end, ttg_str, tenant_names))) {
        LOG_WARN("fail to parse tenant vector", K(ret));
      } else if (first) {
        tenant_group.column_ = tenant_names.count();
        tenant_group.row_ = 0;
        first = false;
      } else if (tenant_group.column_ != tenant_names.count()) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(append(tenant_group.tenants_, tenant_names))) {
        LOG_WARN("fail to append", K(ret));
      } else {
        ++tenant_group.row_;
      }
    }
    if (OB_SUCC(ret)) {
      jump_over_space(pos, end, ttg_str);
      if (pos >= end) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
      } else if (')' != ttg_str[pos]) { // end up with right brace for a row tenant
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
      } else {
        ++pos;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(jump_to_next_tenant_vector(pos, end, ttg_str))) {
        LOG_WARN("fail to jump to next tenant vector", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (tenant_group.row_ <= 0 || tenant_group.column_ <= 0) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else if (OB_FAIL(tenant_groups.push_back(tenant_group))) {
      LOG_WARN("fail to push back", K(ret));
    } else {} // no more to do
  }
  return ret;
}

int ObTenantGroupParser::jump_to_next_tenant_vector(
    int64_t &pos,
    const int64_t end,
    const common::ObString &ttg_str)
{
  int ret = OB_SUCCESS;
  while (pos < end && isspace(ttg_str[pos])) {
    ++pos;
  }
  if (pos >= end) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
  } else if (')' == ttg_str[pos]) {
    // reach to the end of this tenant group
  } else if (',' == ttg_str[pos]) {
    ++pos; // good, and jump over the ',' symbol
    while (pos < end && isspace(ttg_str[pos])) {
      ++pos;
    }
    if (pos >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else if ('(' == ttg_str[pos]) {
      // the next tenant vector left brace
    } else {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    }
  } else {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
  }
  return ret;
}

void ObTenantGroupParser::jump_over_space(
     int64_t &pos,
     const int64_t end,
     const common::ObString &ttg_str)
{
  while (pos < end && isspace(ttg_str[pos])) {
    ++pos;
  }
  return;
}

int ObTenantGroupParser::jump_to_next_ttg(
    int64_t &pos,
    const int64_t end,
    const common::ObString &ttg_str)
{
  int ret = OB_SUCCESS;
  while (pos < end && isspace(ttg_str[pos])) {
    ++pos;
  }
  if (pos >= end) {
    // good, reach end
  } else if (',' == ttg_str[pos]) {
    ++pos; // good, and jump over the ',' symbol
    while (pos < end && isspace(ttg_str[pos])) {
      ++pos;
    }
    if (pos >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else if ('(' == ttg_str[pos]) {
      // good, the next ttg left brace symbol
    } else {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    }
  } else {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
  }
  return ret;
}

int ObLocalityTaskHelp::filter_logonly_task(const common::ObIArray<share::ObResourcePoolName> &pools,
                                            ObUnitManager &unit_mgr,
                                            ObIArray<share::ObZoneReplicaNumSet> &zone_locality)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnitInfo> logonly_unit_infos;
  ObArray<ObUnitInfo> unit_infos;
  if (pools.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pools));
  } else if (OB_FAIL(unit_mgr.get_unit_infos(pools, unit_infos))) {
    LOG_WARN("fail to get unit infos", K(ret), K(pools));
  } else {
    for (int64_t i = 0; i < unit_infos.count() && OB_SUCC(ret); ++i) {
      if (REPLICA_TYPE_LOGONLY != unit_infos.at(i).unit_.replica_type_) {
        // only L unit is counted
      } else if (OB_FAIL(logonly_unit_infos.push_back(unit_infos.at(i)))) {
        LOG_WARN("fail to push back", K(ret), K(i), K(unit_infos));
      }
    }
    for (int64_t i = 0; i < zone_locality.count() && OB_SUCC(ret); ++i) {
      share::ObZoneReplicaAttrSet &zone_replica_attr_set = zone_locality.at(i);
      if (zone_replica_attr_set.get_logonly_replica_num()
          + zone_replica_attr_set.get_encryption_logonly_replica_num() <= 0) {
        // no L replica : nothing todo
      } else if (zone_replica_attr_set.zone_set_.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone set unexpected", K(ret), K(zone_replica_attr_set));
      } else {
        for (int64_t j = 0; j < logonly_unit_infos.count(); j++) {
          const ObUnitInfo &unit_info = logonly_unit_infos.at(j);
          if (!has_exist_in_array(zone_replica_attr_set.zone_set_, unit_info.unit_.zone_)) {
            // bypass
          } else if (zone_replica_attr_set.get_logonly_replica_num()
                     + zone_replica_attr_set.get_encryption_logonly_replica_num() <= 0) {
            // bypass
          } else if (zone_replica_attr_set.get_logonly_replica_num() > 0) {
            ret = zone_replica_attr_set.sub_logonly_replica_num(ReplicaAttr(1, 100));
          } else {
            ret = zone_replica_attr_set.sub_encryption_logonly_replica_num(ReplicaAttr(1, 100));
          }
        }
      }
    }
  }
  return ret;
}

int ObLocalityTaskHelp::get_logonly_task_with_logonly_unit(const uint64_t tenant_id,
                                                           ObUnitManager &unit_mgr,
                                                           share::schema::ObSchemaGetterGuard &schema_guard,
                                                           ObIArray<share::ObZoneReplicaNumSet> &zone_locality)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnitInfo> logonly_unit_infos;
  const ObTenantSchema *tenant_schema = NULL;
  zone_locality.reset();
  common::ObArray<share::ObZoneReplicaAttrSet> tenant_zone_locality;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant info", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("get invalid tenant schema", K(ret), K(tenant_schema));
  } else if (OB_FAIL(unit_mgr.get_logonly_unit_by_tenant(tenant_id, logonly_unit_infos))) {
    LOG_WARN("fail to get logonly unit infos", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_schema->get_zone_replica_attr_array(tenant_zone_locality))) {
    LOG_WARN("fail to get zone replica attr array", K(ret));
  } else {
    share::ObZoneReplicaNumSet logonly_set;
    for (int64_t i = 0; i < logonly_unit_infos.count() && OB_SUCC(ret); i++) {
      const ObUnitInfo &unit = logonly_unit_infos.at(i);
      for (int64_t j = 0; j < tenant_zone_locality.count(); j++) {
        logonly_set.reset();
        const ObZoneReplicaNumSet &zone_set = tenant_zone_locality.at(j);
        if (zone_set.zone_ == unit.unit_.zone_
            && zone_set.get_logonly_replica_num() == 1) {
          logonly_set.zone_ = zone_set.zone_;
          if (OB_FAIL(logonly_set.replica_attr_set_.add_logonly_replica_num(ReplicaAttr(1, 100)))) {
            LOG_WARN("fail to add logonly replica num", K(ret));
          } else if (OB_FAIL(zone_locality.push_back(logonly_set))) {
            LOG_WARN("fail to push back", K(ret));
          }
        } else if (zone_set.zone_ == unit.unit_.zone_
            && zone_set.get_encryption_logonly_replica_num() == 1) {
          logonly_set.zone_ = zone_set.zone_;
          if (OB_FAIL(logonly_set.replica_attr_set_.add_encryption_logonly_replica_num(ReplicaAttr(1, 100)))) {
            LOG_WARN("fail to add logonly replica num", K(ret));
          } else if (OB_FAIL(zone_locality.push_back(logonly_set))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObLocalityTaskHelp::filter_logonly_task(const uint64_t tenant_id,
                                            ObUnitManager &unit_mgr,
                                            share::schema::ObSchemaGetterGuard &schema_guard,
                                            ObIArray<share::ObZoneReplicaAttrSet> &zone_locality)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnitInfo> logonly_unit_infos;
  if (OB_FAIL(unit_mgr.get_logonly_unit_by_tenant(schema_guard, tenant_id, logonly_unit_infos))) {
    LOG_WARN("fail to get loggonly unit by tenant", K(ret), K(tenant_id));
  } else {
    LOG_DEBUG("get all logonly unit", K(tenant_id), K(logonly_unit_infos), K(zone_locality));
    for (int64_t i = 0; i < zone_locality.count() && OB_SUCC(ret); ++i) {
      share::ObZoneReplicaAttrSet &zone_replica_attr_set = zone_locality.at(i);
      if (zone_replica_attr_set.get_logonly_replica_num()
          + zone_replica_attr_set.get_encryption_logonly_replica_num() <= 0) {
        // no L replica : nothing todo
      } else if (zone_replica_attr_set.zone_set_.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone set unexpected", K(ret), K(zone_replica_attr_set));
      } else {
        for (int64_t j = 0; j < logonly_unit_infos.count(); j++) {
          const ObUnitInfo &unit_info = logonly_unit_infos.at(j);
          if (!has_exist_in_array(zone_replica_attr_set.zone_set_, unit_info.unit_.zone_)) {
            // bypass
          } else if (zone_replica_attr_set.get_logonly_replica_num()
             + zone_replica_attr_set.get_encryption_logonly_replica_num() <= 0) {
            // bypass
          } else if (zone_replica_attr_set.get_logonly_replica_num() > 0) {
            ret = zone_replica_attr_set.sub_logonly_replica_num(ReplicaAttr(1, 100));
          } else {
            ret = zone_replica_attr_set.sub_encryption_logonly_replica_num(ReplicaAttr(1, 100));
          }
        }
      }
    }
  }
  return ret;
}

int ObLocalityTaskHelp::alloc_logonly_replica(ObUnitManager &unit_mgr,
                                              const ObIArray<share::ObResourcePoolName> &pools,
                                              const common::ObIArray<ObZoneReplicaNumSet> &zone_locality,
                                              ObPartitionAddr &partition_addr)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnitInfo> logonly_units;
  ObArray<ObUnitInfo> unit_infos;
  if (OB_FAIL(unit_mgr.get_unit_infos(pools, unit_infos))) {
    LOG_WARN("fail to get unit infos", K(ret), K(pools));
  } else {
    for (int64_t i = 0; i < unit_infos.count() && OB_SUCC(ret); i++) {
      if (REPLICA_TYPE_LOGONLY != unit_infos.at(i).unit_.replica_type_) {
        //nothing todo
      } else if (OB_FAIL(logonly_units.push_back(unit_infos.at(i)))) {
        LOG_WARN("fail to push back", K(ret), K(i), K(unit_infos));
      }
    }
  }
  ObReplicaAddr raddr;
  for (int64_t i = 0; i < logonly_units.count() && OB_SUCC(ret); i++) {
    for (int64_t j = 0; j < zone_locality.count() && OB_SUCC(ret); j++) {
      if (zone_locality.at(j).zone_ == logonly_units.at(i).unit_.zone_
          && (zone_locality.at(j).get_logonly_replica_num() == 1
              || zone_locality.at(j).get_encryption_logonly_replica_num() == 1)) {
        raddr.reset();
        raddr.unit_id_ = logonly_units.at(i).unit_.unit_id_;
        raddr.addr_ = logonly_units.at(i).unit_.server_;
        raddr.zone_ = logonly_units.at(i).unit_.zone_;
        raddr.replica_type_ = zone_locality.at(j).get_logonly_replica_num() == 1
                              ? REPLICA_TYPE_LOGONLY
                              : REPLICA_TYPE_ENCRYPTION_LOGONLY;
        if (OB_FAIL(partition_addr.push_back(raddr))) {
          LOG_WARN("fail to push back", K(ret), K(raddr));
        } else {
          LOG_INFO("alloc partition for logonly replica", K(raddr));
        }
      }
    }
  }
  return ret;
}

int ObLocalityCheckHelp::calc_paxos_replica_num(
    const common::ObIArray<share::ObZoneReplicaNumSet> &zone_locality,
    int64_t &paxos_num)
{
  int ret = OB_SUCCESS;
  paxos_num = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); i++) {
    if (0 > zone_locality.at(i).get_paxos_replica_num()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid paxos replica num", K(ret), "zone_locality_set", zone_locality.at(i));
    } else {
      paxos_num += zone_locality.at(i).get_paxos_replica_num();
    }
  }
  return ret;
}

/*
 * the laws of alter locality:
 * 1. each locality altering only allows to execute one operation: add paoxs, reduce paxos and paxos type transform(paxos->paxos, F->L).
 *    paxos->non_paxos is reduce paxos, non_paxos->paxos is add paxos.
 * 2. in a locality altering:
 *    2.1 if add paxos, need the orig_locality's paxos num >= majority(new_locality's paxos num);
 *    2.2 if reduce paxos, need new_locality's paxos num >= majority(orig_locality's paxos num);
 *    2.3 if paxos type transform, only one type transform is allowed for a locality altering.
 * 3. the laws of type transform:
 *   3.1 for L-replica, it is not allowed to transform replica_type other than F->L,
 *       and it is not allowed to transform L to other type.
 * 4. enable paxos num 1->2. must not paxos num 2->1
 * 5. there is no restriction on non-paoxs type transform
*/
int ObLocalityCheckHelp::check_alter_locality(
    const ObIArray<share::ObZoneReplicaNumSet> &pre_zone_locality,
    const ObIArray<share::ObZoneReplicaNumSet> &cur_zone_locality,
    ObIArray<AlterPaxosLocalityTask> &alter_paxos_tasks,
    bool &non_paxos_locality_modified,
    int64_t &pre_paxos_num,
    int64_t &cur_paxos_num,
    const share::ObArbitrationServiceStatus &arb_service_status)
{
  int ret = OB_SUCCESS;
  pre_paxos_num = 0;
  cur_paxos_num = 0;
  alter_paxos_tasks.reset();
  if (OB_UNLIKELY(pre_zone_locality.count() <= 0 || cur_zone_locality.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
             "pre_cnt", pre_zone_locality.count(),
             "cur_cnt", cur_zone_locality.count());
  } else if (OB_FAIL(get_alter_paxos_replica_number_replica_task(pre_zone_locality, cur_zone_locality,
                     alter_paxos_tasks, non_paxos_locality_modified))) {
    LOG_WARN("fail to get alter paxos replica task", K(ret));
  } else if (OB_FAIL(calc_paxos_replica_num(pre_zone_locality, pre_paxos_num))) {
    LOG_WARN("fail to calc paxos replica num", K(ret));
  } else if (OB_FAIL(calc_paxos_replica_num(cur_zone_locality, cur_paxos_num))) {
    LOG_WARN("fail to calc paxos replica num", K(ret));
  } else if (OB_FAIL(check_alter_locality_valid(alter_paxos_tasks,
                                                pre_paxos_num, cur_paxos_num, arb_service_status))) {
    LOG_WARN("check alter locality valid failed", K(ret), K(alter_paxos_tasks),
             K(pre_paxos_num), K(cur_paxos_num), K(non_paxos_locality_modified), K(arb_service_status));
  }
  return ret;
}

/*
 * Two zone set either are perfect matched or have intersection
 */
int ObLocalityCheckHelp::check_multi_zone_locality_intersect(
    const common::ObIArray<share::ObZoneReplicaAttrSet> &multi_pre_zone_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet> &multi_cur_zone_locality,
    bool &intersect)
{
  int ret = OB_SUCCESS;
  intersect = false;
  for (int64_t i = 0;
       !intersect && OB_SUCC(ret) && i < multi_pre_zone_locality.count();
       ++i) {
    for (int64_t j = 0;
         !intersect && OB_SUCC(ret) && j < multi_cur_zone_locality.count();
         ++j) {
      const share::ObZoneReplicaAttrSet &pre_locality = multi_pre_zone_locality.at(i);
      const share::ObZoneReplicaAttrSet &cur_locality = multi_cur_zone_locality.at(j);
      const common::ObIArray<common::ObZone> &pre_zone_set = pre_locality.zone_set_;
      const common::ObIArray<common::ObZone> &cur_zone_set = cur_locality.zone_set_;
      if (pre_zone_set.count() <= 1 || cur_zone_set.count() <= 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone set count unexpected", K(ret));
      } else {
        intersect = check_zone_set_intersect(pre_zone_set, cur_zone_set);
      }
    }
  }
  return ret;
}

bool ObLocalityCheckHelp::check_zone_set_intersect(
     const common::ObIArray<common::ObZone> &pre_zone_set,
     const common::ObIArray<common::ObZone> &cur_zone_set)
{
  bool intersect = false;
  if (pre_zone_set.count() == cur_zone_set.count()) {
    // either there is no crossover or there is a perfect match
    int64_t counter = 0;
    for (int64_t i = 0; i < pre_zone_set.count(); ++i) {
      const common::ObZone &this_pre_zone = pre_zone_set.at(i);
      if (has_exist_in_array(cur_zone_set, this_pre_zone)) {
        counter++;
      }
    }
    if (0 == counter || pre_zone_set.count() == counter) {
      intersect = false;
    } else {
      intersect = true;
    }
  } else {
    // must no cross
    intersect = false;
    for (int64_t i = 0; !intersect && i < pre_zone_set.count(); ++i) {
      const common::ObZone &this_pre_zone = pre_zone_set.at(i);
      if (has_exist_in_array(cur_zone_set, this_pre_zone)) {
        intersect = true;
      }
    }
  }
  return intersect;
}

int ObLocalityCheckHelp::split_single_and_multi_zone_locality(
    const common::ObIArray<share::ObZoneReplicaAttrSet> &zone_locality,
    common::ObIArray<share::ObZoneReplicaAttrSet> &single_zone_locality,
    common::ObIArray<share::ObZoneReplicaAttrSet> &multi_zone_locality)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(zone_locality.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
             "zone_locality_cnt", zone_locality.count());
  } else {
    single_zone_locality.reset();
    multi_zone_locality.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); ++i) {
      const share::ObZoneReplicaAttrSet &this_set = zone_locality.at(i);
      if (this_set.zone_set_.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone set count unexpected", K(ret), "zone_set_cnt", this_set.zone_set_.count());
      } else if (this_set.zone_set_.count() == 1) {
        if (OB_FAIL(single_zone_locality.push_back(this_set))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {
        if (OB_FAIL(multi_zone_locality.push_back(this_set))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLocalityCheckHelp::single_zone_locality_search(
    const share::ObZoneReplicaAttrSet &this_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet> &single_zone_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet> &multi_zone_locality,
    SingleZoneLocalitySearch &search_flag,
    int64_t &search_index,
    const share::ObZoneReplicaAttrSet *&search_zone_locality)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(this_locality.zone_set_.count() != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    bool found = false;
    const common::ObZone &compare_zone = this_locality.zone_set_.at(0);
    for (int64_t i = 0; !found && OB_SUCC(ret) && i < single_zone_locality.count(); ++i) {
      const share::ObZoneReplicaAttrSet &this_zone_locality = single_zone_locality.at(i);
      if (this_zone_locality.zone_set_.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this zone set unexpected", K(ret),
                 "zone_set_count", this_zone_locality.zone_set_.count());
      } else if (compare_zone != this_zone_locality.zone_set_.at(0)) {
        // go on check next
      } else {
        found = true;
        search_flag = SingleZoneLocalitySearch::SZLS_IN_ASSOC_SINGLE;
        search_index = i;
        search_zone_locality = &this_zone_locality;
      }
    }
    for (int64_t i = 0; !found && OB_SUCC(ret) && i < multi_zone_locality.count(); ++i) {
      const share::ObZoneReplicaAttrSet &this_zone_locality = multi_zone_locality.at(i);
      if (this_zone_locality.zone_set_.count() <= 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this zone set unexpected", K(ret),
                 "zone_set_count", this_zone_locality.zone_set_.count());
      } else if (!has_exist_in_array(this_zone_locality.zone_set_, compare_zone)) {
        // go on check next
      } else {
        found = true;
        search_flag = SingleZoneLocalitySearch::SZLS_IN_ASSOC_MULTI;
        search_index = i;
        search_zone_locality = &this_zone_locality;
      }
    }
    if (OB_SUCC(ret)) {
      if (!found) {
        search_flag = SingleZoneLocalitySearch::SZLS_NOT_FOUND;
        search_index = -1;
        search_zone_locality = nullptr;
      }
    }
  }
  return ret;
}

int ObLocalityCheckHelp::try_add_single_zone_alter_paxos_task(
    const share::ObZoneReplicaAttrSet &pre_locality,
    const share::ObZoneReplicaAttrSet &cur_locality,
    common::ObIArray<AlterPaxosLocalityTask> &alter_paxos_tasks,
    bool &non_paxos_locality_modified)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pre_locality.zone_set_.count() != 1)
      || OB_UNLIKELY(cur_locality.zone_set_.count() != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pre_locality), K(cur_locality));
  } else if (OB_FAIL(check_alter_single_zone_locality_valid(
          cur_locality, pre_locality, non_paxos_locality_modified))) {
    LOG_WARN("alter single zone locality is invalid", K(ret),
             K(cur_locality), K(pre_locality), K(non_paxos_locality_modified));
  } else {
    AlterPaxosLocalityTask alter_paxos_task;
    if (pre_locality.get_paxos_replica_num() == cur_locality.get_paxos_replica_num()) {
      if (!pre_locality.is_paxos_locality_match(cur_locality)) {
        alter_paxos_task.reset();
        alter_paxos_task.task_type_ = NOP_PAXOS_REPLICA_NUMBER;
        if (cur_locality.get_full_replica_num() > pre_locality.get_full_replica_num()) {
          if (OB_FAIL(alter_paxos_task.associated_replica_type_set_.push_back(REPLICA_TYPE_FULL))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
        if (OB_SUCC(ret)
            && cur_locality.get_logonly_replica_num()
               > pre_locality.get_logonly_replica_num()) {
          if (OB_FAIL(alter_paxos_task.associated_replica_type_set_.push_back(REPLICA_TYPE_LOGONLY))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
        if (OB_SUCC(ret)
            && cur_locality.get_encryption_logonly_replica_num()
               > pre_locality.get_encryption_logonly_replica_num()) {
          if (OB_FAIL(alter_paxos_task.associated_replica_type_set_.push_back(
                  REPLICA_TYPE_ENCRYPTION_LOGONLY))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
        if (OB_SUCC(ret)
            && cur_locality.get_logonly_replica_num() == pre_locality.get_logonly_replica_num()
            && cur_locality.get_encryption_logonly_replica_num() == pre_locality.get_encryption_logonly_replica_num()
            && cur_locality.get_full_replica_num() == pre_locality.get_full_replica_num()) {
          // memstore_percent changed
          if (OB_FAIL(alter_paxos_task.associated_replica_type_set_.push_back(REPLICA_TYPE_FULL))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(alter_paxos_task.zone_set_.assign(pre_locality.zone_set_))) {
          LOG_WARN("fail to assign array", K(ret));
        } else if (OB_FAIL(alter_paxos_tasks.push_back(alter_paxos_task))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {} // paxos locality match
    } else {
      alter_paxos_task.reset();
      const int64_t delta = cur_locality.get_paxos_replica_num() - pre_locality.get_paxos_replica_num();
      alter_paxos_task.task_type_ = delta > 0 ? ADD_PAXOS_REPLICA_NUMBER : SUB_PAXOS_REPLICA_NUMBER;
      if (pre_locality.get_full_replica_num() != cur_locality.get_full_replica_num()) {
        if (OB_FAIL(alter_paxos_task.associated_replica_type_set_.push_back(REPLICA_TYPE_FULL))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
      if (OB_SUCC(ret)
          && pre_locality.get_logonly_replica_num() != cur_locality.get_logonly_replica_num()) {
        if (OB_FAIL(alter_paxos_task.associated_replica_type_set_.push_back(REPLICA_TYPE_LOGONLY))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
      if (OB_SUCC(ret)
          && pre_locality.get_encryption_logonly_replica_num() != cur_locality.get_encryption_logonly_replica_num()) {
        if (OB_FAIL(alter_paxos_task.associated_replica_type_set_.push_back(REPLICA_TYPE_ENCRYPTION_LOGONLY))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(alter_paxos_task.zone_set_.assign(pre_locality.zone_set_))) {
        LOG_WARN("fail to assign array", K(ret));
      } else if (OB_FAIL(alter_paxos_tasks.push_back(alter_paxos_task))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObLocalityCheckHelp::process_pre_single_zone_locality(
    const common::ObIArray<share::ObZoneReplicaAttrSet> &single_pre_zone_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet> &single_cur_zone_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet> &multi_cur_zone_locality,
    common::ObArray<XyIndex> &pre_in_cur_multi_indexes,
    common::ObIArray<AlterPaxosLocalityTask> &alter_paxos_tasks,
    bool &non_paxos_locality_modified)
{
  int ret = OB_SUCCESS;
  pre_in_cur_multi_indexes.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < single_pre_zone_locality.count(); ++i) {
    const share::ObZoneReplicaAttrSet &this_locality = single_pre_zone_locality.at(i);
    SingleZoneLocalitySearch search_flag = SingleZoneLocalitySearch::SZLS_INVALID;
    int64_t search_index = -1;
    const share::ObZoneReplicaAttrSet *search_zone_locality = nullptr;
    if (OB_FAIL(single_zone_locality_search(
            this_locality, single_cur_zone_locality, multi_cur_zone_locality,
            search_flag, search_index, search_zone_locality))) {
      LOG_WARN("fail to search single zone locality", K(ret));
    } else if (SingleZoneLocalitySearch::SZLS_IN_ASSOC_SINGLE == search_flag) {
      if (nullptr == search_zone_locality) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("search zone locality ptr is null", K(ret));
      } else if (OB_FAIL(try_add_single_zone_alter_paxos_task(
              this_locality, *search_zone_locality, alter_paxos_tasks, non_paxos_locality_modified))) {
        LOG_WARN("fail to try add single zone alter paxos task", K(ret));
      }
    } else if (SingleZoneLocalitySearch::SZLS_IN_ASSOC_MULTI == search_flag) {
      if (search_index >= multi_cur_zone_locality.count() || search_index < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("search index unexpected", K(ret), K(search_index),
                 "multi_cur_zone_locality_cnt", multi_cur_zone_locality.count());
      } else if (OB_FAIL(pre_in_cur_multi_indexes.push_back(XyIndex(i, search_index)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    } else if (SingleZoneLocalitySearch::SZLS_NOT_FOUND == search_flag) {
      // dealing with locality remove zone.
      ObZoneReplicaAttrSet cur_set;
      if (OB_FAIL(cur_set.zone_set_.assign(this_locality.zone_set_))) {
        LOG_WARN("fail to assign zone set", K(ret));
      } else if (OB_FAIL(try_add_single_zone_alter_paxos_task(
              this_locality, cur_set, alter_paxos_tasks, non_paxos_locality_modified))) {
        LOG_WARN("fail to try add single zone alter paxos task", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("search flag unexpected", K(ret), K(search_flag));
    }
  }
  if (OB_SUCC(ret)) {
    YIndexCmp cmp_operator;
    std::sort(pre_in_cur_multi_indexes.begin(), pre_in_cur_multi_indexes.end(), cmp_operator);
  }
  return ret;
}

int ObLocalityCheckHelp::process_cur_single_zone_locality(
    const common::ObIArray<share::ObZoneReplicaAttrSet> &single_cur_zone_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet> &single_pre_zone_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet> &multi_pre_zone_locality,
    common::ObArray<XyIndex> &cur_in_pre_multi_indexes,
    common::ObIArray<AlterPaxosLocalityTask> &alter_paxos_tasks,
    bool &non_paxos_locality_modified)
{
  int ret = OB_SUCCESS;
  cur_in_pre_multi_indexes.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < single_cur_zone_locality.count(); ++i) {
    const share::ObZoneReplicaAttrSet &this_locality = single_cur_zone_locality.at(i);
    SingleZoneLocalitySearch search_flag = SingleZoneLocalitySearch::SZLS_INVALID;
    int64_t search_index = -1;
    const share::ObZoneReplicaAttrSet *search_zone_locality = nullptr;
    if (OB_FAIL(single_zone_locality_search(
            this_locality, single_pre_zone_locality, multi_pre_zone_locality,
            search_flag, search_index, search_zone_locality))) {
      LOG_WARN("fail to search single zone locality", K(ret));
    } else if (SingleZoneLocalitySearch::SZLS_IN_ASSOC_SINGLE == search_flag) {
      // already process in process_pre_single_zone_locality
    } else if (SingleZoneLocalitySearch::SZLS_IN_ASSOC_MULTI == search_flag) {
      if (search_index >= multi_pre_zone_locality.count() || search_index < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("search index unexpected", K(ret), K(search_index),
                 "multi_pre_zone_locality_cnt", multi_pre_zone_locality.count());
      } else if (OB_FAIL(cur_in_pre_multi_indexes.push_back(XyIndex(i, search_index)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    } else if (SingleZoneLocalitySearch::SZLS_NOT_FOUND == search_flag) {
      // dealing with locality add zone
      ObZoneReplicaAttrSet pre_set;
      if (OB_FAIL(pre_set.zone_set_.assign(this_locality.zone_set_))) {
        LOG_WARN("fail to assign zone set", K(ret));
      } else if (OB_FAIL(try_add_single_zone_alter_paxos_task(
              pre_set, this_locality, alter_paxos_tasks, non_paxos_locality_modified))) {
        LOG_WARN("fail to try add single zone alter paxos task", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("search flag unexpected", K(ret), K(search_flag));
    }
  }
  if (OB_SUCC(ret)) {
    YIndexCmp cmp_operator;
    std::sort(cur_in_pre_multi_indexes.begin(), cur_in_pre_multi_indexes.end(), cmp_operator);
  }
  return ret;
}

int ObLocalityCheckHelp::add_multi_zone_locality_task(
    common::ObIArray<AlterPaxosLocalityTask> &alter_paxos_tasks,
    const share::ObZoneReplicaAttrSet &multi_zone_locality,
    const PaxosReplicaNumberTaskType paxos_replica_number_task_type)
{
  int ret = OB_SUCCESS;
  AlterPaxosLocalityTask alter_paxos_task;
  alter_paxos_task.reset();
  alter_paxos_task.task_type_ = paxos_replica_number_task_type;
  if (OB_FAIL(alter_paxos_task.zone_set_.assign(multi_zone_locality.zone_set_))) {
    LOG_WARN("fail to assign zone set", K(ret));
  } else {
    if (multi_zone_locality.get_full_replica_num() > 0) {
      if (OB_FAIL(alter_paxos_task.associated_replica_type_set_.push_back(REPLICA_TYPE_FULL))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret) && multi_zone_locality.get_logonly_replica_num() > 0) {
      if (OB_FAIL(alter_paxos_task.associated_replica_type_set_.push_back(REPLICA_TYPE_LOGONLY))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret) && multi_zone_locality.get_encryption_logonly_replica_num() > 0) {
      if (OB_FAIL(alter_paxos_task.associated_replica_type_set_.push_back(REPLICA_TYPE_ENCRYPTION_LOGONLY))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(alter_paxos_tasks.push_back(alter_paxos_task))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}
int ObLocalityCheckHelp::check_alter_locality_match(
    const share::ObZoneReplicaAttrSet &in_locality,
    const share::ObZoneReplicaAttrSet &out_locality)
{
  int ret = OB_SUCCESS;
  bool match = false;
  match = in_locality.is_alter_locality_match(out_locality);
  if (!match) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("too many locality changes are illegal", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "too many locality changes");
  }
  return ret;
}

int ObLocalityCheckHelp::check_single_and_multi_zone_locality_match(
    const int64_t start,
    const int64_t end,
    const common::ObIArray<share::ObZoneReplicaAttrSet> &single_zone_locality,
    const common::ObIArray<XyIndex> &in_multi_indexes,
    const share::ObZoneReplicaAttrSet &multi_zone_locality)
{
  int ret = OB_SUCCESS;
  if (start >= end || start >= in_multi_indexes.count() || end > in_multi_indexes.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start), K(end),
             "in_multi_indexes_cnt", in_multi_indexes.count());
  } else {
    share::ObZoneReplicaAttrSet new_multi_zone_locality;
    for (int64_t i = start; OB_SUCC(ret) && i < end; ++i) {
      const int64_t x = in_multi_indexes.at(i).x_;
      if (x < 0 || x >= single_zone_locality.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("x unexpected", K(ret), K(x),
                 "single_zone_locality_cnt", single_zone_locality.count());
      } else if (OB_FAIL(new_multi_zone_locality.append(single_zone_locality.at(x)))) {
        LOG_WARN("fail to append", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_alter_locality_match(
          new_multi_zone_locality, multi_zone_locality))) {
        LOG_WARN("fail to check alter locality match", K(ret));
      }
    }
  }
  return ret;
}

int ObLocalityCheckHelp::process_single_in_multi(
    const common::ObIArray<share::ObZoneReplicaAttrSet> &single_left_zone_locality,
    const common::ObIArray<XyIndex> &left_in_multi_indexes,
    const common::ObIArray<share::ObZoneReplicaAttrSet> &multi_right_zone_locality,
    common::ObIArray<AlterPaxosLocalityTask> &alter_paxos_tasks)
{
  int ret = OB_SUCCESS;
  int64_t start = 0;
  int64_t end = left_in_multi_indexes.count();
  while (OB_SUCC(ret) && start < end) {
    int64_t cursor = start;
    int64_t y_index = left_in_multi_indexes.at(start).y_;
    for (/*nop*/; cursor < end; ++cursor) {
      if (left_in_multi_indexes.at(start).y_ == left_in_multi_indexes.at(cursor).y_) {
        // go on
      } else {
        break;
      }
    }
    if (y_index < 0 || y_index >= multi_right_zone_locality.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("y_index unexpected", K(ret), K(y_index),
               "multi_zone_locality_cnt", multi_right_zone_locality.count());
    } else if (OB_FAIL(check_single_and_multi_zone_locality_match(
            start, cursor, single_left_zone_locality,
            left_in_multi_indexes, multi_right_zone_locality.at(y_index)))) {
    } else if (OB_FAIL(add_multi_zone_locality_task(
            alter_paxos_tasks, multi_right_zone_locality.at(y_index), PaxosReplicaNumberTaskType::NOP_PAXOS_REPLICA_NUMBER))) {
      LOG_WARN("fail to add multi zone locality task", K(ret));
    } else {
      start = cursor;
    }
  }
  return ret;
}

bool ObLocalityCheckHelp::has_exist_in_yindex_array(
     const common::ObIArray<XyIndex> &index_array,
     const int64_t y)
{
  bool found = false;
  for (int64_t i = 0; !found && i < index_array.count(); ++i) {
    const XyIndex &this_index = index_array.at(i);
    if (y == this_index.y_) {
      found = true;
    }
  }
  return found;
}

int ObLocalityCheckHelp::process_pre_multi_locality(
    const common::ObIArray<XyIndex> &pre_in_cur_multi_indexes,
    const common::ObIArray<XyIndex> &cur_in_pre_multi_indexes,
    const common::ObIArray<share::ObZoneReplicaAttrSet> &multi_pre_zone_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet> &multi_cur_zone_locality,
    common::ObIArray<AlterPaxosLocalityTask> &alter_paxos_tasks)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < multi_pre_zone_locality.count(); ++i) {
    const share::ObZoneReplicaAttrSet &this_pre_locality = multi_pre_zone_locality.at(i);
    if (has_exist_in_yindex_array(cur_in_pre_multi_indexes, i)) {
      // bypass, already processed
    } else {
      bool found = false;
      for (int64_t j = 0; !found && j < multi_cur_zone_locality.count(); ++j) {
        const share::ObZoneReplicaAttrSet &this_cur_locality = multi_cur_zone_locality.at(j);
        if (has_exist_in_yindex_array(pre_in_cur_multi_indexes, j)) {
          found = true;
          // bypass, already processed
        } else if (this_pre_locality == this_cur_locality) {
          found = true;
          // locality not modified
        } else if (this_cur_locality.is_zone_set_match(this_pre_locality)) {
          // is processing in process_cur_multi_locality
          found = true;
        } else { /* go on check next */ }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(check_alter_locality_match(
              this_pre_locality, this_cur_locality))) {
            LOG_WARN("fail to check locality match", K(ret));
          }
        }
      }
      if (!found && OB_SUCC(ret)) {
        if (OB_FAIL(add_multi_zone_locality_task(
                alter_paxos_tasks, this_pre_locality, PaxosReplicaNumberTaskType::SUB_PAXOS_REPLICA_NUMBER))) {
          LOG_WARN("fail to add multi zone locality task", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLocalityCheckHelp::process_cur_multi_locality(
    const common::ObIArray<XyIndex> &pre_in_cur_multi_indexes,
    const common::ObIArray<XyIndex> &cur_in_pre_multi_indexes,
    const common::ObIArray<share::ObZoneReplicaAttrSet> &multi_pre_zone_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet> &multi_cur_zone_locality,
    common::ObIArray<AlterPaxosLocalityTask> &alter_paxos_tasks)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < multi_cur_zone_locality.count(); ++i) {
    const share::ObZoneReplicaAttrSet &this_cur_locality = multi_cur_zone_locality.at(i);
    if (has_exist_in_yindex_array(pre_in_cur_multi_indexes, i)) {
      // bypass, already process
    } else {
      bool found = false;
      for (int64_t j = 0; !found && j < multi_pre_zone_locality.count(); ++j) {
        const share::ObZoneReplicaAttrSet &this_pre_locality = multi_pre_zone_locality.at(j);
        if (has_exist_in_yindex_array(cur_in_pre_multi_indexes, j)) {
          found = true;
          // bypass, already processed
        } else if (this_cur_locality == this_pre_locality) {
          found = true;
          // locality not modified
        } else if (this_cur_locality.is_zone_set_match(this_pre_locality)) {
          found = true;
          if (OB_FAIL(add_multi_zone_locality_task(
                  alter_paxos_tasks, this_cur_locality, PaxosReplicaNumberTaskType::NOP_PAXOS_REPLICA_NUMBER))) {
            LOG_WARN("fail to add multi zone locality task", K(ret));
          }
        } else { /* go on check next */ }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(check_alter_locality_match(
              this_cur_locality, this_pre_locality))) {
            LOG_WARN("fail to check alter locality match", K(ret));
          }
        }
      }
      if (!found && OB_SUCC(ret)) {
        if (OB_FAIL(add_multi_zone_locality_task(
                alter_paxos_tasks, this_cur_locality, PaxosReplicaNumberTaskType::ADD_PAXOS_REPLICA_NUMBER))) {
          LOG_WARN("fail to add multi zone locality task", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLocalityCheckHelp::get_alter_paxos_replica_number_replica_task(
    const common::ObIArray<share::ObZoneReplicaAttrSet> &pre_zone_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet> &cur_zone_locality,
    ObIArray<AlterPaxosLocalityTask> &alter_paxos_tasks,
    bool &non_paxos_locality_modified)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObZoneReplicaAttrSet> single_pre_zone_locality;
  common::ObArray<share::ObZoneReplicaAttrSet> multi_pre_zone_locality;
  common::ObArray<share::ObZoneReplicaAttrSet> single_cur_zone_locality;
  common::ObArray<share::ObZoneReplicaAttrSet> multi_cur_zone_locality;
  common::ObArray<XyIndex> pre_in_cur_multi_indexes;
  common::ObArray<XyIndex> cur_in_pre_multi_indexes;
  bool intersect = false;
  if (OB_UNLIKELY(pre_zone_locality.count() <= 0)
      || OB_UNLIKELY(cur_zone_locality.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
             "pre_cnt", pre_zone_locality.count(),
             "cur_cnt", cur_zone_locality.count());
  } else if (OB_FAIL(split_single_and_multi_zone_locality(
          pre_zone_locality, single_pre_zone_locality, multi_pre_zone_locality))) {
    LOG_WARN("fail to split single and multi zone locality", K(ret));
  } else if (OB_FAIL(split_single_and_multi_zone_locality(
          cur_zone_locality, single_cur_zone_locality, multi_cur_zone_locality))) {
    LOG_WARN("fail to split single and multi zone locality", K(ret));
  } else if (OB_FAIL(check_multi_zone_locality_intersect(
          multi_pre_zone_locality, multi_cur_zone_locality, intersect))) {
    LOG_WARN("fail to check multi zone locality intersect", K(ret));
  } else if (intersect) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("multi zone locality intersect before and after is not allowed", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "multi zone locality intersect before and after");
  } else if (OB_FAIL(process_pre_single_zone_locality(
          single_pre_zone_locality, single_cur_zone_locality, multi_cur_zone_locality,
          pre_in_cur_multi_indexes, alter_paxos_tasks, non_paxos_locality_modified))) {
    LOG_WARN("fail to process pre single zone locality", K(ret));
  } else if (OB_FAIL(process_cur_single_zone_locality(
          single_cur_zone_locality, single_pre_zone_locality, multi_pre_zone_locality,
          cur_in_pre_multi_indexes, alter_paxos_tasks, non_paxos_locality_modified))) {
    LOG_WARN("fail to process cur single zone locality", K(ret));
  } else if (OB_FAIL(process_single_in_multi(
          single_pre_zone_locality, pre_in_cur_multi_indexes,
          multi_cur_zone_locality, alter_paxos_tasks))) {
    LOG_WARN("fail to process pre single in cur multi", K(ret));
  } else if (OB_FAIL(process_single_in_multi(
          single_cur_zone_locality, cur_in_pre_multi_indexes,
          multi_pre_zone_locality, alter_paxos_tasks))) {
    LOG_WARN("fail to process cur single in pre multi", K(ret));
  } else if (OB_FAIL(process_pre_multi_locality(
          pre_in_cur_multi_indexes, cur_in_pre_multi_indexes,
          multi_pre_zone_locality, multi_cur_zone_locality, alter_paxos_tasks))) {
    LOG_WARN("fail to process pre multi locality", K(ret));
  } else if (OB_FAIL(process_cur_multi_locality(
          pre_in_cur_multi_indexes, cur_in_pre_multi_indexes,
          multi_pre_zone_locality, multi_cur_zone_locality, alter_paxos_tasks))) {
    LOG_WARN("fail to process cur multi locality", K(ret));
  }
  return ret;
}

int ObLocalityCheckHelp::check_alter_locality_valid(
    ObIArray<AlterPaxosLocalityTask> &alter_paxos_tasks,
    int64_t pre_paxos_num,
    int64_t cur_paxos_num,
    const share::ObArbitrationServiceStatus &arb_service_status)
{
  int ret = OB_SUCCESS;
  if (pre_paxos_num <= 0 || cur_paxos_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pre_paxos_num or cur_paxos_num is invalid",
             K(ret), K(pre_paxos_num), K(cur_paxos_num));
  } else {
    // ensure only has ADD_PAXOS_REPLICA_NUMBER task or only SUB_PAXOS_REPLICA_NUMBER task or only one NOP_PAXOS_REPLICA_NUMBER task.
    int64_t add_task_num = 0;
    int64_t remove_task_num = 0;
    int64_t nop_task_num = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < alter_paxos_tasks.count(); i++) {
      const AlterPaxosLocalityTask &this_alter_paxos_task = alter_paxos_tasks.at(i);
      if (OB_FAIL(ret)) {
      } else if (SUB_PAXOS_REPLICA_NUMBER == this_alter_paxos_task.task_type_) {
        if (nop_task_num != 0 || add_task_num != 0) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("should only has one nop_task", K(ret), K(alter_paxos_tasks));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter locality with multi task");
        } else {
          remove_task_num++;
        }
      } else if (ADD_PAXOS_REPLICA_NUMBER == alter_paxos_tasks.at(i).task_type_) {
        if (nop_task_num != 0 || remove_task_num != 0) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("should only has add_tasks", K(ret), K(alter_paxos_tasks));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter locality with multi task");
        } else {
          add_task_num++;
        }
      } else if (NOP_PAXOS_REPLICA_NUMBER == alter_paxos_tasks.at(i).task_type_) {
        if (nop_task_num != 0 || add_task_num != 0 || remove_task_num != 0) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("should only has one nop_task", K(ret), K(alter_paxos_tasks));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter locality with multi task");
        } else {
          nop_task_num++;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid task_type", K(ret), "alter_paxos_task", alter_paxos_tasks.at(i));
      }
    }
    // 2. Locality's paxos member change need to meet the following principles:
    //    1) only add paxos member and pre_paxos_num >= majority(cur_paxos_num)
    //    2) only remove paxos member and cur_paxos_num >= majority(pre_paxos_num)
    //    3) only has a paxos -> paxos's type transform task.
    //       non_paxos -> paxos's type transform is add paxos member.
    //       paxos -> non_paxos's type transform is remove paxos member
    if (OB_SUCC(ret)) {
      bool passed = true;
      ObSqlString message_to_user;
      if (0 != nop_task_num) {
        // only has paxos->paxos's type transform task, no quorum value change.
      } else if (0 != add_task_num) {
        if (arb_service_status.is_enable_like()) {
          if (2 == pre_paxos_num && 2 == add_task_num) {
            // special process: tenant with arb service should only support 2->4
          } else if (OB_FAIL(message_to_user.assign("paxos replica number should be 2 or 4 when arbitration service is enabled, "
                                                    "alter tenant locality"))) {
            LOG_WARN("fail to construct message to user", KR(ret));
          } else {
            passed = false;
          }
        } else if (1 == pre_paxos_num && 1 == add_task_num) {
          // special process: enable locality's paxos member 1 -> 2
        } else if (pre_paxos_num >= majority(cur_paxos_num)) {
          // passed
        } else if (OB_FAIL(message_to_user.assign("violate locality principal"))) {
          LOG_WARN("fail to construct message to user", KR(ret));
        } else {
          passed = false;
        }
      } else if (0 != remove_task_num) {
        if (arb_service_status.is_enable_like()) {
          if (4 == pre_paxos_num && 2 == remove_task_num) {
            // special process: tenant with arb service should only support 4->2
          } else if (OB_FAIL(message_to_user.assign("paxos replica number should be 2 or 4 when arbitration service is enabled, "
                                                    "alter tenant locality"))) {
            LOG_WARN("fail to construct message to user", KR(ret));
          } else {
            passed = false;
          }
        } else if (cur_paxos_num >= majority(pre_paxos_num)) {
          // passed
        } else  if (OB_FAIL(message_to_user.assign("violate locality principal"))) {
          LOG_WARN("fail to construct message to user", KR(ret));
        } else {
          passed = false;
        }
      }
      if (!passed) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("violate locality principal", K(ret), K(pre_paxos_num), K(cur_paxos_num),
                 K(add_task_num), K(remove_task_num), K(nop_task_num));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, message_to_user.ptr());
      }
    }
  }
  return ret;
}

int ObLocalityCheckHelp::check_alter_single_zone_locality_valid(
     const ObZoneReplicaAttrSet &new_locality,
     const ObZoneReplicaAttrSet &orig_locality,
     bool &non_paxos_locality_modified)
{
  int ret = OB_SUCCESS;
  bool is_legal = true;
  // 1. check whether non_paxos member change
  if (!non_paxos_locality_modified) {
    const ObIArray<ReplicaAttr> &pre_readonly_replica = orig_locality.replica_attr_set_.get_readonly_replica_attr_array();
    const ObIArray<ReplicaAttr> &cur_readonly_replica = new_locality.replica_attr_set_.get_readonly_replica_attr_array();
    if (pre_readonly_replica.count() != cur_readonly_replica.count()) {
      non_paxos_locality_modified = true;
    } else {
      for (int64_t i = 0; !non_paxos_locality_modified && i < pre_readonly_replica.count(); ++i) {
        const ReplicaAttr &pre_replica_attr = pre_readonly_replica.at(i);
        const ReplicaAttr &cur_replica_attr = cur_readonly_replica.at(i);
        if (pre_replica_attr != cur_replica_attr) {
          non_paxos_locality_modified = true;
        }
      }
    }
  }
  // 2. check whether alter locality is legal.
  if (new_locality.get_logonly_replica_num() < orig_locality.get_logonly_replica_num()) {
    // L-replica must not transfrom to other replica type.
    if (new_locality.get_full_replica_num() > orig_locality.get_full_replica_num()) {
      is_legal = false; // maybe L->F
    } else if (ObLocalityDistribution::ALL_SERVER_CNT == new_locality.get_readonly_replica_num()
        || ObLocalityDistribution::ALL_SERVER_CNT == orig_locality.get_readonly_replica_num()
        || new_locality.get_readonly_replica_num() > orig_locality.get_readonly_replica_num()) {
      if (0 != new_locality.get_readonly_replica_num()) {
        is_legal = false; // maybe L->R
      }
    } else {} // good
  } else if (new_locality.get_logonly_replica_num() > orig_locality.get_logonly_replica_num()) {
    // only enable F transform to L
    if (new_locality.get_full_replica_num() < orig_locality.get_full_replica_num()) {
      if (ObLocalityDistribution::ALL_SERVER_CNT == new_locality.get_readonly_replica_num()
          && ObLocalityDistribution::ALL_SERVER_CNT != orig_locality.get_readonly_replica_num()) {
        if (0 != orig_locality.get_readonly_replica_num()) {
          is_legal = false; // maybe R->L
        }
      } else if (ObLocalityDistribution::ALL_SERVER_CNT != new_locality.get_readonly_replica_num()
          && ObLocalityDistribution::ALL_SERVER_CNT == orig_locality.get_readonly_replica_num()) {
        is_legal = false; // maybe R->L
      } else if (new_locality.get_readonly_replica_num() < orig_locality.get_readonly_replica_num()) {
        is_legal = false; // maybe R->L
      }
    } else {
      if (ObLocalityDistribution::ALL_SERVER_CNT == new_locality.get_readonly_replica_num()
          || ObLocalityDistribution::ALL_SERVER_CNT == orig_locality.get_readonly_replica_num()
          || new_locality.get_readonly_replica_num() < orig_locality.get_readonly_replica_num()) {
        if (0 != orig_locality.get_readonly_replica_num()) {
          is_legal = false; // maybe R->L
        }
      } else {} // good
    }
  }
  if (!is_legal) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("invalid replica_type transformation", K(ret), K(new_locality), K(orig_locality));
  }

  return ret;
}

int ObRootUtils::get_rs_default_timeout_ctx(ObTimeoutCtx &ctx)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_TIMEOUT_US = 2 * 1000 * 1000; // 2s
  if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, DEFAULT_TIMEOUT_US))) {
    LOG_WARN("fail to set default_timeout_ctx", KR(ret));
  }
  return ret;
}

//get all observer that is stopeed, start_service_time<=0 and lease expire
int ObRootUtils::get_invalid_server_list(
    const ObIArray<ObServerInfoInTable> &servers_info,
    ObIArray<ObAddr> &invalid_server_list)
{
  int ret = OB_SUCCESS;
  invalid_server_list.reset();
  ObArray<ObAddr> stopped_server_list;
  ObArray<ObZone> stopped_zone_list;
  ObZone empty_zone;
  if (OB_FAIL(get_stopped_zone_list(stopped_zone_list, stopped_server_list))) {
    LOG_WARN("fail to get stopped zone list", KR(ret));
  } else if (OB_FAIL(invalid_server_list.assign(stopped_server_list))) {
    LOG_WARN("fail to assign array", KR(ret), K(stopped_zone_list));
  } else {
    for (int64_t i = 0; i < servers_info.count() && OB_SUCC(ret); i++) {
      const ObServerInfoInTable &server_info = servers_info.at(i);
      if ((!server_info.is_alive() || !server_info.in_service())
          && !has_exist_in_array(invalid_server_list, server_info.get_server())) {
        if (OB_FAIL(invalid_server_list.push_back(server_info.get_server()))) {
          LOG_WARN("fail to push back", KR(ret), K(server_info));
        }
      }
    }
  }
  return ret;
}

int ObRootUtils::find_server_info(
  const ObIArray<share::ObServerInfoInTable> &servers_info,
  const common::ObAddr &server,
  share::ObServerInfoInTable &server_info)
{
  int ret = OB_SUCCESS;
  bool server_exists = false;
  server_info.reset();
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", KR(ret), K(server));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !server_exists && i < servers_info.count(); i++) {
      const ObServerInfoInTable & server_info_i = servers_info.at(i);
      if (OB_UNLIKELY(!server_info_i.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("server_info_i is not valid", KR(ret), K(server_info_i));
      } else if (server == server_info_i.get_server()) {
        server_exists = true;
        if (OB_FAIL(server_info.assign(server_info_i))) {
          LOG_WARN("fail to assign server_info", KR(ret), K(server_info_i));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !server_exists) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("server not exists", KR(ret), K(server));
  }
  return ret;
}

int ObRootUtils::get_servers_of_zone(
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    const common::ObZone &zone,
    ObIArray<common::ObAddr> &servers,
    bool only_active_servers)
{
  int ret = OB_SUCCESS;
  servers.reset();
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid zone", KR(ret), K(zone));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < servers_info.count(); i++) {
      const ObServerInfoInTable &server_info = servers_info.at(i);
      if (OB_UNLIKELY(!server_info.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid server_info", KR(ret), K(server_info));
      } else if (zone != server_info.get_zone() || (only_active_servers && !server_info.is_active())) {
        // do nothing
      } else if (OB_FAIL(servers.push_back(server_info.get_server()))) {
        LOG_WARN("fail to push an element into servers", KR(ret), K(server_info));
      }
    }
  }
  return ret;
}
int ObRootUtils::get_server_count(
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    const ObZone &zone,
    int64_t &alive_count,
    int64_t &not_alive_count)
{
  int ret = OB_SUCCESS;
  alive_count = 0;
  not_alive_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < servers_info.count(); ++i) {
    const ObServerInfoInTable &server_info = servers_info.at(i);
    if (server_info.get_zone() == zone || zone.is_empty()) {
      if (server_info.is_alive()) {
        ++alive_count;
      } else {
        ++not_alive_count;
      }
    }
  }
  return ret;
}
int ObRootUtils::check_server_alive(
      const ObIArray<ObServerInfoInTable> &servers_info,
      const ObAddr &server,
      bool &is_alive)
{
  int ret = OB_SUCCESS;
  is_alive = false;
  ObServerInfoInTable server_info;
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", KR(ret), K(server));
  } else if (OB_FAIL(find_server_info(servers_info, server, server_info))) {
    LOG_WARN("fail to find server_info", KR(ret), K(servers_info), K(server));
  } else {
    is_alive = server_info.is_alive();
  }
  return ret;
}
int ObRootUtils::get_server_resource_info(
    const ObIArray<obrpc::ObGetServerResourceInfoResult> &server_resources_info,
    const ObAddr &server,
    share::ObServerResourceInfo &resource_info)
{
  int ret = OB_SUCCESS;
  bool server_exists = false;
  resource_info.reset();
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", KR(ret), K(server));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !server_exists && i < server_resources_info.count(); i++) {
      const obrpc::ObGetServerResourceInfoResult &server_resource_info_i = server_resources_info.at(i);
      if (OB_UNLIKELY(!server_resource_info_i.is_valid())){
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("server_resource_info_i is not valid", KR(ret), K(server_resource_info_i));
      } else if (server == server_resource_info_i.get_server()) {
        server_exists = true;
        resource_info = server_resource_info_i.get_resource_info();
      }
    }
  }
  if (OB_SUCC(ret) && !server_exists) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("server not exists", KR(ret), K(server));
  }
  return ret;
}

int ObRootUtils::get_stopped_zone_list(
    ObIArray<ObZone> &stopped_zone_list,
    ObIArray<ObAddr> &stopped_server_list)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObTimeoutCtx ctx;
  stopped_zone_list.reset();
  stopped_server_list.reset();
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (OB_FAIL(sql.assign_fmt("SELECT s.svr_ip, s.svr_port, s.zone "
      "FROM %s AS s JOIN (SELECT zone, info FROM %s WHERE name = 'status') AS z "
      "ON s.zone = z.zone WHERE s.stop_time > 0 OR z.info = 'INACTIVE'",
      OB_ALL_SERVER_TNAME, OB_ALL_ZONE_TNAME))) {
    LOG_WARN("fail to append sql", KR(ret));
  } else if (OB_FAIL(ObZoneTableOperation::get_inactive_zone_list(*GCTX.sql_proxy_, stopped_zone_list))) {
    LOG_WARN("fail to get inactive zone_list", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      int tmp_ret = OB_SUCCESS;
      ObMySQLResult *result = NULL;
      if (OB_FAIL(GCTX.sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), K(sql));
      } else {
        ObZone zone;
        ObAddr server;
        ObString tmp_zone;
        ObString svr_ip;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("result next failed", KR(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else {
            int64_t svr_port = 0;
            server.reset();
            zone.reset();
            svr_ip.reset();
            tmp_zone.reset();
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "svr_ip", svr_ip);
            EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", svr_port, int64_t);
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "zone", tmp_zone);
            if (OB_UNLIKELY(!server.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port)))) {
              ret = OB_INVALID_DATA;
              LOG_WARN("fail to set ip addr", KR(ret), K(svr_ip), K(svr_port));
            } else if (OB_FAIL(zone.assign(tmp_zone))) {
              LOG_WARN("fail to assign zone", KR(ret), K(tmp_zone));
            } else if (OB_FAIL(stopped_server_list.push_back(server))) {
              LOG_WARN("fail to push an element into stopped_server_list", KR(ret), K(server));
            } else if (has_exist_in_array(stopped_zone_list, zone)) {
              // do nothing
            } else if (OB_FAIL(stopped_zone_list.push_back(zone))) {
              LOG_WARN("fail to push an element into stopped_zone_list", KR(ret), K(zone));
            }
          }
        }
      }
    }
  }
  LOG_INFO("get stopped zone list", KR(ret), K(stopped_server_list), K(stopped_zone_list));
  return ret;
}
bool ObRootUtils::have_other_stop_task(const ObZone &zone)
{
  int ret = OB_SUCCESS;
  bool bret = true;
  int64_t cnt = 0;
  ObSqlString sql;
  ObTimeoutCtx ctx;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else if (OB_FAIL(sql.assign_fmt("SELECT COUNT(*) AS cnt FROM "
      "(SELECT zone FROM %s WHERE stop_time > 0 AND zone != '%s' UNION "
      "SELECT zone FROM %s WHERE name = 'status' AND info = 'INACTIVE' AND zone != '%s')",
      OB_ALL_SERVER_TNAME, zone.ptr(), OB_ALL_ZONE_TNAME, zone.ptr()))) {
    LOG_WARN("fail to append sql", KR(ret), K(zone));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      int tmp_ret = OB_SUCCESS;
      ObMySQLResult *result = NULL;
      if (OB_FAIL(GCTX.sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get next", KR(ret), K(sql));;
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "cnt", cnt, int64_t);
      }
      if (OB_SUCC(ret) && (OB_ITER_END != (tmp_ret = result->next()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K(sql));
      }
    }
  }
  if (OB_SUCC(ret) && 0 == cnt) {
    bret = false;
  }
  LOG_INFO("have other stop task", KR(ret), K(bret), K(zone), K(cnt));
  return bret;
}
int ObRootUtils::get_tenant_intersection(ObUnitManager &unit_mgr,
                                         ObIArray<ObAddr> &this_server_list,
                                         ObIArray<ObAddr> &other_server_list,
                                         ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  ObHashSet<uint64_t> this_tenant_ids_set;
  ObHashSet<uint64_t> other_tenant_ids_set;
  const int64_t TENANT_BUCKET_NUM = 1000;
  if (OB_FAIL(this_tenant_ids_set.create(TENANT_BUCKET_NUM))) {
    LOG_WARN("fail to create hashset", KR(ret));
  } else if (OB_FAIL(other_tenant_ids_set.create(TENANT_BUCKET_NUM))) {
    LOG_WARN("fail to create hashset", KR(ret));
  }
  for (int64_t i = 0; i < this_server_list.count() && OB_SUCC(ret); i++) {
    const ObAddr &server = this_server_list.at(i);
    if (OB_FAIL(unit_mgr.get_tenants_of_server(server, this_tenant_ids_set))) {
      LOG_WARN("fail to get tenant unit", KR(ret), K(server));
    }
  }
  for (int64_t i = 0; i < other_server_list.count() && OB_SUCC(ret); i++) {
    const ObAddr &server = other_server_list.at(i);
    if (OB_FAIL(unit_mgr.get_tenants_of_server(server, other_tenant_ids_set))) {
      LOG_WARN("fail to get tenant unit", KR(ret), K(server));
    }
  }
  ObHashSet<uint64_t>::const_iterator iter;
  for (iter = this_tenant_ids_set.begin(); iter != this_tenant_ids_set.end() && OB_SUCC(ret); iter++) {
    if (OB_FAIL(other_tenant_ids_set.exist_refactored(iter->first))) {
      if (OB_HASH_EXIST == ret) {
        if (OB_FAIL(tenant_ids.push_back(iter->first))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      } else if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to check exist", KR(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", KR(ret));
    }
  } //end for iter
  return ret;
}


template<class T>
bool ObRootUtils::has_intersection(const common::ObIArray<T> &this_array,
                                   const common::ObIArray<T> &other_array)
{
  bool bret = false;
  for (int64_t i = 0; i < this_array.count() && !bret; i++) {
    if (has_exist_in_array(other_array, this_array.at(i))) {
      bret = true;
    }
  }
  return bret;
}

//iter the tenant, table's primary_zone is covered by zone_list
int ObRootUtils::check_primary_region_in_zonelist(ObMultiVersionSchemaService *schema_service,
                                                  ObDDLService *ddl_service,
                                                  ObUnitManager &unit_mgr,
                                                  ObZoneManager &zone_mgr,
                                                  const ObIArray<uint64_t> &tenant_ids,
                                                  const ObIArray<ObZone> &zone_list,
                                                  bool &is_in)
{
  int ret = OB_SUCCESS;
  is_in = false;
  if (0 >= zone_list.count() || OB_ISNULL(schema_service) || OB_ISNULL(ddl_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone_list));
  } else if (0 == tenant_ids.count()) {
    is_in = false;
  } else {
    ObSchemaGetterGuard tenant_schema_guard;
    ObArray<ObZone> tenant_zone_list;
    const ObTenantSchema *tenant_info = NULL;
    for (int64_t i = 0; i < tenant_ids.count() && !is_in; i++) {
      const uint64_t tenant_id = tenant_ids.at(i);
      tenant_zone_list.reset();
      if (OB_FAIL(unit_mgr.get_tenant_pool_zone_list(tenant_id, tenant_zone_list))) {
        LOG_WARN("fail to get tenant pool zone list", KR(ret), K(tenant_id));
      } else if (is_subset(zone_list, tenant_zone_list))  {
        //all zones of tenant is in the zone_list
        is_in = true;
      } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, tenant_schema_guard))) {
        LOG_WARN("get_schema_guard failed", K(ret));
      } else if (OB_FAIL(tenant_schema_guard.get_tenant_info(tenant_id, tenant_info))) {
        LOG_WARN("fail to get tenant info", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_info)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("invalid tenant info", KR(ret), K(tenant_id));
      } else { 
        bool has = false;
        if (OB_FAIL(check_left_f_in_primary_zone(zone_mgr, tenant_schema_guard, *tenant_info, zone_list, has))) {
          LOG_WARN("fail to check tenant locality", KR(ret), K(tenant_id));
        } else if (!has) {
          is_in = true;
          LOG_INFO("tenant primary zone has no full replica exist", K(tenant_id), K(zone_list));
        }
      }
    } //end for tenant_ids 
  } //end else
  LOG_INFO("check primary region in zonelist", KR(ret), K(tenant_ids),
           K(zone_list), K(is_in));
  return ret;
}

int ObRootUtils::get_primary_zone(ObZoneManager &zone_mgr,
                                  const ObIArray<ObZoneScore> &zone_score_array,
                                  ObIArray<ObZone> &primary_zone)
{
  int ret= OB_SUCCESS;
  int64_t first_score = -1;
  ObArray<ObZone> zone_list;
  if (OB_UNLIKELY(0 >= zone_score_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone_score_array));
  } else {
    for (int64_t i = 0; i < zone_score_array.count() && OB_SUCC(ret); i++) {
      const ObZoneScore &score = zone_score_array.at(i);
      if (-1 == first_score) {
        first_score = score.score_;
        if (OB_FAIL(zone_list.push_back(score.zone_))) {
          LOG_WARN("fail to push back", KR(ret), K(score));
        }
      } else if (first_score == score.score_) {
        if (OB_FAIL(zone_list.push_back(score.zone_))) {
          LOG_WARN("fail to push back", KR(ret), "zone", zone_score_array.at(i).zone_);
        }
      }
    }
    //get all zones in one region
    ObArray<ObRegion> region_list;
    ObRegion region;
    for (int64_t i = 0; i < zone_list.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(zone_mgr.get_region(zone_list.at(i), region))) {
        LOG_WARN("fail to get region", KR(ret), KR(i), K(zone_list));
      } else if (has_exist_in_array(region_list, region)) {
        //nothing todo
      } else if (OB_FAIL(region_list.push_back(region))) {
        LOG_WARN("fail to push region", KR(ret), K(region));
      }
    }
    //get all read_write zone
    ObArray<ObZoneInfo> all_zones;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(zone_mgr.get_zone(all_zones))) {
      LOG_WARN("fail to get zone", KR(ret));
    } else {
      for (int64_t i = 0; i < all_zones.count() && OB_SUCC(ret); i++) {
        const ObZoneInfo &zone = all_zones.at(i);
        ObZoneType zone_type = static_cast<ObZoneType>(zone.zone_type_.value_);
        ObRegion region(zone.region_.info_.ptr());
        if (has_exist_in_array(region_list, region)
            && common::ZONE_TYPE_READWRITE  == zone_type) {
          if (OB_FAIL(primary_zone.push_back(zone.zone_))) {
            LOG_WARN("fail to push back", KR(ret), K(zone));
          }
        }
      }
    }
  }
  return ret;
}

template<class T>
int ObRootUtils::check_left_f_in_primary_zone(ObZoneManager &zone_mgr,
                                              ObSchemaGetterGuard &schema_guard,
                                              const T &schema_info,
                                              const ObIArray<ObZone> &zone_list,
                                              bool &has)
{
  int ret = OB_SUCCESS;
  has = false;
  ObArray<ObZoneReplicaAttrSet> zone_locality_array;
  ObArenaAllocator allocator("PrimaryZone");
  ObPrimaryZone primary_zone(allocator);
  ObArray<ObZone> primary_zone_array;
  if (OB_UNLIKELY(!schema_info.is_valid())
      || OB_UNLIKELY(0 >= zone_list.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(schema_info), K(zone_list));
  } else if (OB_FAIL(schema_info.get_primary_zone_inherit(schema_guard, primary_zone))) {
    LOG_WARN("fail to get primary zone", KR(ret));
  } else if (ObPrimaryZoneUtil::no_need_to_check_primary_zone(primary_zone.get_primary_zone())) {
    //if primary_zone is random, no need to check left f in primary zone
    has = true;
    LOG_INFO("primary zone is RANDOM or empty, no need to check", KR(ret), K(primary_zone));
  } else if (OB_FAIL(schema_info.get_zone_replica_attr_array_inherit(schema_guard,
                                                                     zone_locality_array))) {
    LOG_WARN("fail to get zone replica array", KR(ret));
  } else if (OB_FAIL(get_primary_zone(zone_mgr, primary_zone.get_primary_zone_array(),
                                      primary_zone_array))) {
    LOG_WARN("fail to get primary zone", KR(ret), K(primary_zone));
  } else {
    for (int64_t i = 0; i < primary_zone_array.count() && OB_SUCC(ret) && !has; i++) {
      const ObZone &zone = primary_zone_array.at(i);
      for (int64_t j = 0; j < zone_locality_array.count() && OB_SUCC(ret) && !has; j++) {
        const ObZoneReplicaAttrSet &set = zone_locality_array.at(j);
        int64_t full_replica_num = set.get_full_replica_num();
        if (0 < full_replica_num
            && (has_exist_in_array(set.zone_set_, zone)
             || zone == set.zone_)
            && !has_intersection(set.zone_set_, zone_list)) {
          //there is F replica in the zone_set where the primary_zone is located,
          //and, no zone of the zone_set is in zone_list(stopped).
          has = true;
        }
      } //end for zone_locality_array
    } //end primary_zone_array
  }
  LOG_INFO("check left f in primary zone", KR(ret), K(zone_list), K(has));
  return ret;
}

int ObRootUtils::get_proposal_id_from_sys_ls(int64_t &proposal_id, ObRole &role)
{
  int ret = OB_SUCCESS;
  storage::ObLSHandle ls_handle;
  logservice::ObLogHandler *handler = nullptr;
  MTL_SWITCH(OB_SYS_TENANT_ID) {
    if (OB_FAIL(MTL(ObLSService*)->get_ls(SYS_LS, ls_handle, ObLSGetMod::RS_MOD))) {
      LOG_WARN("fail to get ls", KR(ret));
    } else if (OB_UNLIKELY(!ls_handle.is_valid())
        || OB_ISNULL(ls_handle.get_ls())
        || OB_ISNULL(handler = ls_handle.get_ls()->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", KR(ret), KP(ls_handle.get_ls()),
          KP(ls_handle.get_ls()->get_log_handler()));
    } else if (OB_FAIL(handler->get_role(role, proposal_id))) {
      LOG_WARN("fail to get role", KR(ret));
          }
  }
  return ret;
}

int ObRootUtils::try_notify_switch_ls_leader(
      obrpc::ObSrvRpcProxy *rpc_proxy,
      const share::ObLSInfo &ls_info,
      const obrpc::ObNotifySwitchLeaderArg::SwitchLeaderComment &comment)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_info.is_valid()) || OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_info), K(rpc_proxy));
  } else {
    ObArray<ObAddr> server_list;
    obrpc::ObNotifySwitchLeaderArg arg;
    const uint64_t tenant_id = ls_info.get_tenant_id();
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_info.get_replicas_cnt(); ++i) {
      if (OB_FAIL(server_list.push_back(ls_info.get_replicas().at(i).get_server()))) {
        LOG_WARN("failed to push back server", KR(ret), K(i), K(ls_info));
      }
    }
    if (FAILEDx(arg.init(tenant_id, ls_info.get_ls_id(), ObAddr(), comment))) {
      LOG_WARN("failed to init switch leader arg", KR(ret), K(tenant_id), K(ls_info), K(comment));
    } else if (OB_FAIL(notify_switch_leader(rpc_proxy, tenant_id, arg, server_list))) {
      LOG_WARN("failed to notify switch leader", KR(ret), K(arg), K(tenant_id), K(server_list));
    }
  }
  return ret;

}


int ObRootUtils::notify_switch_leader(
      obrpc::ObSrvRpcProxy *rpc_proxy,
      const uint64_t tenant_id,
      const obrpc::ObNotifySwitchLeaderArg &arg,
      const ObIArray<common::ObAddr> &addr_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
        || !arg.is_valid() || 0 == addr_list.count()) || OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(arg), K(addr_list), KP(rpc_proxy));
  } else {
    ObTimeoutCtx ctx;
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
      LOG_WARN("fail to set timeout ctx", KR(ret));
    } else {
      ObNotifySwitchLeaderProxy proxy(*rpc_proxy, &obrpc::ObSrvRpcProxy::notify_switch_leader);
      for (int64_t i = 0; i < addr_list.count(); ++i) {
        const int64_t timeout =  ctx.get_timeout();
        if (OB_TMP_FAIL(proxy.call(addr_list.at(i), timeout, GCONF.cluster_id, tenant_id, arg))) {
          ret = OB_SUCC(ret) ? tmp_ret : ret;
          LOG_WARN("failed to send rpc", KR(ret), K(i), K(tenant_id), K(arg), K(addr_list));
        }
      }//end for
      if (OB_TMP_FAIL(proxy.wait())) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_WARN("failed to wait all result", KR(ret), KR(tmp_ret));
      }
    }
  }
  return ret;
}

int ObRootUtils::check_tenant_ls_balance(uint64_t tenant_id, int &check_ret)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  bool pass = false;
  check_ret = OB_NEED_WAIT;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K(tenant_id));
  } else if (tenant_data_version < DATA_VERSION_4_2_1_0 || !is_user_tenant(tenant_id)) {
    // in v4.1 no need to check ls balance
    // to let rs jobs' return_code be OB_SUCCESS, we set has_checked true
    // non-user tenant has no ls balance
    check_ret = OB_SUCCESS;
  } else if (!ObShareUtil::is_tenant_enable_rebalance(tenant_id)) {
    check_ret = OB_SKIP_CHECKING_LS_STATUS;
  } else if (OB_FAIL(ObTenantBalanceService::is_ls_balance_finished(tenant_id, pass))) {
    LOG_WARN("fail to execute is_ls_balance_finished", KR(ret), K(tenant_id));
  } else if (pass) {
    check_ret = OB_SUCCESS;
  } else {
    check_ret = OB_NEED_WAIT;
  }
  return ret;
}

int ObRootUtils::is_first_priority_primary_zone_changed(
    const share::schema::ObTenantSchema &orig_tenant_schema,
    const share::schema::ObTenantSchema &new_tenant_schema,
    ObIArray<ObZone> &orig_first_primary_zone,
    ObIArray<ObZone> &new_first_primary_zone,
    bool &is_changed)
{
  int ret = OB_SUCCESS;
  orig_first_primary_zone.reset();
  new_first_primary_zone.reset();
  is_changed = false;
  if (OB_UNLIKELY(orig_tenant_schema.get_tenant_id() != new_tenant_schema.get_tenant_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input tenant schema", KR(ret), K(orig_tenant_schema), K(new_tenant_schema));
  } else if (OB_FAIL(ObPrimaryZoneUtil::get_tenant_primary_zone_array(
      orig_tenant_schema,
      orig_first_primary_zone))) {
    LOG_WARN("fail to get tenant primary zone array", KR(ret),
        K(orig_tenant_schema), K(orig_first_primary_zone));
  } else if (OB_FAIL(ObPrimaryZoneUtil::get_tenant_primary_zone_array(
      new_tenant_schema,
      new_first_primary_zone))) {
    LOG_WARN("fail to get tenant primary zone array", KR(ret),
        K(new_tenant_schema), K(new_first_primary_zone));
  } else if (orig_first_primary_zone.count() != new_first_primary_zone.count()) {
    is_changed = true;
  } else {
    ARRAY_FOREACH(new_first_primary_zone, idx) {
      const ObZone &zone = new_first_primary_zone.at(idx);
      if (!common::has_exist_in_array(orig_first_primary_zone, zone)) {
        is_changed = true;
        break;
      }
    }
  }
  return ret;
}

int ObRootUtils::check_ls_balance_and_commit_rs_job(
    const uint64_t tenant_id,
    const int64_t rs_job_id,
    const ObRsJobType rs_job_type)
{
  int ret = OB_SUCCESS;
  int check_ret = OB_NEED_WAIT;
  const char* rs_job_type_str = NULL;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(!ObRsJobTableOperator::is_valid_job_type(rs_job_type)
      || NULL == (rs_job_type_str = ObRsJobTableOperator::get_job_type_str(rs_job_type))
      || rs_job_id < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job type", KR(ret), K(rs_job_type), KP(rs_job_type_str), K(rs_job_id));
  } else if (OB_FAIL(ObRootUtils::check_tenant_ls_balance(tenant_id, check_ret))) {
    LOG_WARN("fail to execute check_tenant_ls_balance", KR(ret), K(tenant_id));
  } else if (OB_NEED_WAIT != check_ret) {
    DEBUG_SYNC(BEFORE_FINISH_UNIT_NUM);
    if (OB_SUCC(RS_JOB_COMPLETE(rs_job_id, check_ret, *GCTX.sql_proxy_))) {
      FLOG_INFO("[COMMIT_RS_JOB NOTICE] complete an inprogress rs job",
          KR(ret), K(tenant_id), K(rs_job_id), K(check_ret), K(rs_job_type), K(rs_job_type_str));
    } else {
      LOG_WARN("fail to complete rs job", KR(ret), K(tenant_id), K(rs_job_id), K(check_ret),
          K(rs_job_type), K(rs_job_type_str));
      if (OB_EAGAIN == ret) {
        FLOG_WARN("[COMMIT_RS_JOB NOTICE] the specified rs job might has "
            "been already completed due to a new job or deleted in table manually",
            KR(ret), K(tenant_id), K(rs_job_id), K(check_ret), K(rs_job_type), K(rs_job_type_str));
        ret = OB_SUCCESS; // no need to return the error code
      }
    }
  }
  return ret;
}

///////////////////////////////

ObClusterRole ObClusterInfoGetter::get_cluster_role_v2()
{
  ObClusterRole cluster_role = PRIMARY_CLUSTER;
  return cluster_role;
}

ObClusterRole ObClusterInfoGetter::get_cluster_role()
{
  ObClusterRole cluster_role = PRIMARY_CLUSTER;
  
  return cluster_role;
}

const char *oceanbase::rootserver::resource_type_to_str(const ObResourceType &t)
{
  const char* str = "UNKNOWN";
  if (RES_CPU == t) { str = "CPU"; }
  else if (RES_MEM == t) { str = "MEMORY"; }
  else if (RES_LOG_DISK == t) { str = "LOG_DISK"; }
  else { str = "NONE"; }
  return str;
}
