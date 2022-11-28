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

#define USING_LOG_PREFIX SERVER

#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/backup/ob_backup_struct.h"
#include "lib/utility/utility.h"
#include "ob_backup_operator.h"

namespace oceanbase
{
namespace share
{
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;

int ObITenantBackupTaskOperator::get_tenant_ids(
    const uint64_t tenant_id,
    const common::ObSqlString &sql,
    common::ObIArray<uint64_t> &tenant_ids,
    common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    if (OB_UNLIKELY(!sql.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        int64_t tmp_tenant_id = 0;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else {
          EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tmp_tenant_id, int64_t);
          if (OB_FAIL(tenant_ids.push_back(tmp_tenant_id))) {
            LOG_WARN("failed to push tenant id into array", K(ret), K(tmp_tenant_id));
          }
        }
      }
    }
  }
  return ret;
}

template <typename T>
int ObTenantBackupInfoOperation::set_info_item(
    const char *name, const char *info_str, T &info)
{
  int ret = OB_SUCCESS;
  // %value and %info_str can be arbitrary values
  if (NULL == name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid name", K(ret));
  } else {
    ObBackupInfoItem *it = info.list_.get_first();
    while (OB_SUCCESS == ret && it != info.list_.get_header()) {
      if (NULL == it) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null iter", K(ret));
      } else {
        if (strncasecmp(it->name_, name, OB_MAX_COLUMN_NAME_LENGTH) == 0) {
          it->value_ = info_str;
          break;
        }
        it = it->get_next();
      }
    }
    if (OB_SUCC(ret)) {
      // ignore unknown item
      if (it == info.list_.get_header()) {
        LOG_WARN("unknown item", K(name), "value", info_str);
      }
    }
  }
  return ret;
}

template <typename T>
int ObTenantBackupInfoOperation::load_info(common::ObISQLClient &sql_client, T &info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(info.tenant_id_);
  
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObBackupUtils::get_backup_info_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.assign_fmt("SELECT name, value FROM %s WHERE tenant_id = %lu FOR UPDATE",
        OB_ALL_BACKUP_INFO_TNAME, info.tenant_id_))) {
      LOG_WARN("append sql failed", K(ret));
    } else if (OB_FAIL(sql_client.read(res, exec_tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(exec_tenant_id), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get sql result", K(ret));
    } else {
      int64_t tmp_real_str_len = 0;
      char name[OB_INNER_TABLE_DEFAULT_KEY_LENTH] = "";
      char value_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH + 1] = "";
      while (OB_SUCCESS == ret && OB_SUCCESS == (ret = result->next())) {
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name", name,
                                   static_cast<int64_t>(sizeof(name)), tmp_real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "value", value_str,
                                   static_cast<int64_t>(sizeof(value_str)), tmp_real_str_len);
        (void) tmp_real_str_len; // make compiler happy
        if (OB_SUCC(ret)) {
          if (OB_FAIL(set_info_item(name, value_str, info))) {
            LOG_WARN("set info item failed", K(ret), K(name), K(value_str));
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get result failed", K(ret), K(sql));
      }
    }
  }
  return ret;
}

int ObTenantBackupInfoOperation::load_base_backup_info(ObISQLClient &sql_client, ObBaseBackupInfo &info)
{
  return load_info(sql_client, info);
}

template <typename T>
int ObTenantBackupInfoOperation::insert_info(ObISQLClient &sql_client, T &info)
{
  int ret = OB_SUCCESS;
  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info));
  } else {
    DLIST_FOREACH(it, info.list_) {
      if (OB_FAIL(update_info_item(sql_client, info.tenant_id_, *it))) {
        LOG_WARN("insert item failed", K(ret), "tenant_id", info.tenant_id_, "item", *it);
        break;
      }
    }
  }
  return ret;
}

int ObTenantBackupInfoOperation::load_info_item(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    ObBackupInfoItem &item,
    const bool need_lock)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  ObSqlString sql;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
  
    if (OB_INVALID_ID == tenant_id || !item.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(item), K(tenant_id));
    } else if (OB_FAIL(ObBackupUtils::get_backup_info_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.assign_fmt(
        "SELECT name, value FROM %s WHERE name = '%s' AND tenant_id = %lu",
        OB_ALL_BACKUP_INFO_TNAME, item.name_, tenant_id))) {
      LOG_WARN("assign sql failed", K(ret));
    } else if (need_lock && OB_FAIL(sql.append(" FOR UPDATE"))) {
      LOG_WARN("failed to append lock for sql", K(ret), K(sql));
    } else if (OB_FAIL(sql_client.read(res, exec_tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql), K(exec_tenant_id));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get sql result", K(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_BACKUP_INFO_NOT_EXIST;
        LOG_WARN("backup info is not exist yet, wait later", K(ret), K(tenant_id), K(sql));
      } else {
        LOG_WARN("failed to get next", K(ret));
      }
    } else {
      int64_t tmp_real_str_len = 0;
      char name[OB_INNER_TABLE_DEFAULT_KEY_LENTH] = "";
      char value_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = "";
      EXTRACT_STRBUF_FIELD_MYSQL(*result, "name", name,
                                 static_cast<int64_t>(sizeof(name)), tmp_real_str_len);
      EXTRACT_STRBUF_FIELD_MYSQL(*result, "value", value_str,
                                 static_cast<int64_t>(sizeof(value_str)), tmp_real_str_len);
      (void) tmp_real_str_len; // make compiler happy
      if (OB_SUCC(ret)) {
        if (0 != strcmp(name, item.name_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to select item", K(ret), K(item), K(name));
        } else {
          MEMCPY(item.value_.ptr(), value_str, sizeof(value_str));
        }
      }
    }
  
  }
  return ret;
}

int ObTenantBackupInfoOperation::get_backup_snapshot_version(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    int64_t &backup_snapshot_version)
{
  int ret = OB_SUCCESS;
  const bool need_lock = false;
  ObBackupInfoItem item;
  item.name_ = "backup_snapshot_version";
  backup_snapshot_version = 0;

  if (OB_FAIL(load_info_item(sql_proxy, tenant_id, item, need_lock))) {
    if (OB_BACKUP_INFO_NOT_EXIST == ret) {
      backup_snapshot_version = 0;
      LOG_WARN("tenant backup info not exist", K(ret), K(backup_snapshot_version));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get backup snapshot_version", K(ret));
    }
  } else if (OB_FAIL(item.get_int_value(backup_snapshot_version))) {
    LOG_WARN("failed to get int value", K(ret), K(item));
  }

  return ret;
}

int ObTenantBackupInfoOperation::get_backup_schema_version(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id, int64_t &backup_schema_version)
{
  int ret = OB_SUCCESS;
  const bool need_lock = false;
  ObBackupInfoItem item;
  item.name_ = "backup_schema_version";
  backup_schema_version = 0;

  if (OB_FAIL(load_info_item(sql_proxy, tenant_id, item, need_lock))) {
    if (OB_BACKUP_INFO_NOT_EXIST == ret) {
      backup_schema_version = 0;
      LOG_WARN("tenant backup info not exist", K(ret), K(backup_schema_version));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get backup schema version", K(ret));
    }
  } else if (OB_FAIL(item.get_int_value(backup_schema_version))) {
    LOG_WARN("failed to get int value", K(ret), K(item));
  }

  return ret;
}

int ObTenantBackupInfoOperation::get_tenant_name_backup_schema_version(
    common::ObISQLClient &sql_proxy,
    int64_t &backup_schema_version)
{
  int ret = OB_SUCCESS;
  const bool need_lock = false;
  ObBackupInfoItem item;
  item.name_ = OB_STR_TENANT_NAME_BACKUP_SCHEMA_VERSION;
  backup_schema_version = 0;

  if (OB_FAIL(load_info_item(sql_proxy, OB_SYS_TENANT_ID, item, need_lock))) {
    if (OB_BACKUP_INFO_NOT_EXIST == ret) {
      backup_schema_version = 0;
      LOG_WARN("tenant backup info not exist", K(ret), K(backup_schema_version));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tenant_name_backup_schema_version", K(ret));
    }
  } else if (OB_FAIL(item.get_int_value(backup_schema_version))) {
    LOG_WARN("failed to get int value", K(ret), K(item));
  }

  return ret;
}

int ObTenantBackupInfoOperation::update_tenant_name_backup_schema_version(
    common::ObISQLClient &sql_client,
    const int64_t backup_schema_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  // %zone can be empty
  if (backup_schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(backup_schema_version));
  } else if (OB_FAIL(sql.assign_fmt(
        "replace into %s(tenant_id, name, value) values(%lu, '%s', %ld)",
        OB_ALL_BACKUP_INFO_TNAME, OB_SYS_TENANT_ID, OB_STR_TENANT_NAME_BACKUP_SCHEMA_VERSION, backup_schema_version))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(sql_client.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(sql));
  } else {
    LOG_INFO("succeed to update_tenant_name_backup_schema_version", K(backup_schema_version));
  }
  return ret;
}

int ObTenantBackupInfoOperation::clean_backup_scheduler_leader(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const common::ObAddr &scheduler_leader)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char scheduler_leader_str[MAX_IP_PORT_LENGTH] = "";

  if (tenant_id != OB_SYS_TENANT_ID || !scheduler_leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(scheduler_leader));
  } else if (OB_FAIL(scheduler_leader.ip_port_to_string(scheduler_leader_str, MAX_IP_PORT_LENGTH))) {
    LOG_WARN("failed to add addr to buf", K(ret), K(scheduler_leader));
  } else if (OB_FAIL(sql.assign_fmt(
        "update %s set value = '' where name = '%s' and value='%s'",
        OB_ALL_BACKUP_INFO_TNAME, OB_STR_BACKUP_SCHEDULER_LEADER, scheduler_leader_str))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(sql));
  } else if (0 != affected_rows) {
    FLOG_INFO("succeed to clean_backup_scheduler_leader", K(scheduler_leader_str),
        K(sql), K(affected_rows));
  }
  return ret;

}

int ObTenantBackupInfoOperation::update_info_item(common::ObISQLClient &sql_client,
    const uint64_t tenant_id, const ObBackupInfoItem &item)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  // %zone can be empty
  if (OB_INVALID_ID == tenant_id || !item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
        "UPDATE %s SET value = '%s', gmt_modified = now(6) "
        "WHERE tenant_id = %lu AND name = '%s'", OB_ALL_BACKUP_INFO_TNAME, item.value_.ptr(),
        tenant_id, item.name_))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(sql));
  } else if (!(is_single_row(affected_rows) || is_zero_row(affected_rows))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", K(ret), K(affected_rows));
  } else {
    LOG_INFO("execute sql success", K(sql));
  }
  return ret;
}

int ObTenantBackupInfoOperation::remove_base_backup_info(
    ObISQLClient &sql_client,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t item_cnt = 0;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu",
      OB_ALL_BACKUP_INFO_TNAME, tenant_id))) {
    LOG_WARN("sql assign_fmt failed", K(ret));
  } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(sql), K(ret));
  } else if (OB_FAIL(get_backup_info_item_count(item_cnt))) {
    LOG_WARN("get zone item count failed", K(ret));
  } else if (item_cnt != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows not right", "expected affected_rows",
        item_cnt, K(affected_rows), K(ret));
  }
  return ret;
}

int ObTenantBackupInfoOperation::get_backup_info_item_count(int64_t &cnt)
{
  int ret = OB_SUCCESS;
  ObMalloc alloc(ObModIds::OB_TEMP_VARIABLES);
  ObPtrGuard<ObBaseBackupInfo> base_backup_info_guard(alloc);
  if (OB_FAIL(base_backup_info_guard.init())) {
    LOG_WARN("init temporary variable failed", K(ret));
  } else if (NULL == base_backup_info_guard.ptr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null zone info ptr", K(ret));
  } else {
    cnt = base_backup_info_guard.ptr()->get_item_count();
  }
  return ret;
}

int ObTenantBackupInfoOperation::insert_info_item(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObBackupInfoItem &item)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  int64_t affected_rows = 0;
  if (OB_INVALID_ID == tenant_id || !item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
        "INSERT INTO %s (tenant_id, name, value) VALUES(%lu, '%s', '%s')",
        OB_ALL_BACKUP_INFO_TNAME, tenant_id, item.name_, item.value_.ptr()))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(exec_tenant_id), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", K(ret), K(affected_rows));
  } else {
    LOG_INFO("execute sql success", K(sql));
  }
  return ret;
}

int ObTenantBackupInfoOperation::remove_info_item(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const char *name)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  if (OB_INVALID_ID == tenant_id || OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), KP(name));
  } else if (OB_FAIL(sql.assign_fmt(
        "DELETE FROM %s WHERE name = '%s'",
        OB_ALL_BACKUP_INFO_TNAME, name))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(sql));
  }
  return ret;

}

///////////////////// functions for ObBackupInfoOperator //////////////////////////
int ObBackupInfoOperator::get_inner_table_version(common::ObISQLClient &sql_proxy,
    ObBackupInnerTableVersion &version)
{
  int ret = OB_SUCCESS;
  int64_t value = -1;

  if (OB_FAIL(get_int_value_(sql_proxy, false /*for update*/, OB_STR_BACKUP_INNER_TABLE_VERSION, value))) {
    if (OB_ENTRY_NOT_EXIST  != ret) {
      LOG_WARN("Failed to get inner table version", K(ret));
    } else {
      version = OB_BACKUP_INNER_TABLE_V1;
      ret = OB_SUCCESS;
      LOG_INFO("inner table version not exist, set as V1", K(ret), K(version));
    }
  } else {
    version = static_cast<ObBackupInnerTableVersion>(value);
    if (!is_valid_backup_inner_table_version(version)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid version", K(ret), K(version));
    }
  }

  return ret;
}

int ObBackupInfoOperator::set_inner_table_version(common::ObISQLClient &sql_proxy,
    const ObBackupInnerTableVersion &version)
{
  int ret = OB_SUCCESS;

  if (!is_valid_backup_inner_table_version(version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(version));
  } else if (OB_FAIL(set_int_value_(
      sql_proxy, OB_STR_BACKUP_INNER_TABLE_VERSION, static_cast<const int64_t>(version)))) {
    LOG_WARN("failed to set int value", K(ret), K(version));
  }

  return ret;
}

int ObBackupInfoOperator::set_max_piece_id(common::ObISQLClient &sql_proxy,
    const int64_t max_piece_id)
{
  int ret = OB_SUCCESS;
  const bool for_update = true;
  int64_t cur_max_piece_id = 0;

  if (max_piece_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid args", K(ret), K(max_piece_id));
  } else if (OB_FAIL(get_int_value_(sql_proxy, for_update, OB_STR_MAX_BACKUP_PIECE_ID, cur_max_piece_id))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("Failed to get max piece id", K(ret));
    }
  } else if (cur_max_piece_id > max_piece_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("new max piece id must not less than cur max piece id", K(ret), K(max_piece_id), K(cur_max_piece_id));
  }

  if (FAILEDx(set_int_value_(sql_proxy, OB_STR_MAX_BACKUP_PIECE_ID, max_piece_id))) {
    LOG_WARN("failed to set int value", K(ret), K(max_piece_id));
  }

  return ret;
}

int ObBackupInfoOperator::set_max_piece_create_date(common::ObISQLClient &sql_proxy,
    const int64_t max_piece_create_date)
{
  int ret = OB_SUCCESS;

  if (max_piece_create_date < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(max_piece_create_date));
  } else if (OB_FAIL(set_int_value_(sql_proxy, OB_STR_MAX_BACKUP_PIECE_CREATE_DATE, max_piece_create_date))) {
    LOG_WARN("failed to set max_piece_create_date", K(ret), K(max_piece_create_date));
  }

  return ret;
}

int ObBackupInfoOperator::get_tenant_name_backup_schema_version(common::ObISQLClient &sql_proxy,
    int64_t &backup_schema_version)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(get_int_value_(sql_proxy, false /*for update*/,OB_STR_TENANT_NAME_BACKUP_SCHEMA_VERSION, backup_schema_version))) {
    if (OB_ENTRY_NOT_EXIST  != ret) {
      LOG_WARN("Failed to get inner table version", K(ret));
    } else {
      backup_schema_version = 0;
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObBackupInfoOperator::get_backup_leader(common::ObISQLClient &sql_proxy,
    const bool for_update,
    common::ObAddr &leader, bool &has_leader)
{
  int ret = OB_SUCCESS;
  char buf[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = {0};
  leader.reset();
  has_leader = false;

  if (OB_FAIL(get_string_value_(sql_proxy, for_update, OB_STR_BACKUP_SCHEDULER_LEADER, buf, sizeof(buf)))) {
    if (OB_ENTRY_NOT_EXIST  != ret) {
      LOG_WARN("failed to get scheduler leader", K(ret));
    }
  } else if (0 == strlen(buf)) {
    has_leader = false;
  } else if (OB_FAIL(leader.parse_from_cstring(buf))) {
    LOG_WARN("failed to parse addr", K(ret), K(buf));
  } else {
    has_leader = true;
  }

  return ret;
}

int ObBackupInfoOperator::set_backup_leader(common::ObISQLClient &sql_proxy,
    const common::ObAddr &leader)
{
  int ret = OB_SUCCESS;
  char buf[MAX_IP_PORT_LENGTH] = "";

  if (!leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(leader));
  } else if (OB_FAIL(leader.ip_port_to_string(buf, sizeof(buf)))) {
    LOG_WARN("Failed to ip_port_to_string", K(ret), K(leader));
  } else if (OB_FAIL(set_string_value_(sql_proxy, OB_STR_BACKUP_SCHEDULER_LEADER, buf))) {
    LOG_WARN("Failed to set backup leader", K(ret), K(leader));
  }
  return ret;
}

int ObBackupInfoOperator::clean_backup_leader(common::ObISQLClient &sql_proxy,
      const common::ObAddr &leader)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char buf[MAX_IP_PORT_LENGTH] = "";

  if (!leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(leader));
  } else if (OB_FAIL(leader.ip_port_to_string(buf, sizeof(buf)))) {
    LOG_WARN("Failed to ip_port_to_string", K(ret), K(leader));
  } else if (OB_FAIL(sql.assign_fmt("update %s set value = '' where name = '%s' and value = '%s'",
      OB_ALL_BACKUP_INFO_TNAME, OB_STR_BACKUP_SCHEDULER_LEADER, buf))) {
    LOG_WARN("Failed to clean backup leader", K(ret));
  } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql));
  } else if (0 != affected_rows) {
    FLOG_INFO("succeed to clean backup leader", K(ret), "old_leader", leader);
  }
  return ret;
}


int ObBackupInfoOperator::get_backup_leader_epoch(common::ObISQLClient &sql_proxy,
    const bool for_update, int64_t &epoch)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_int_value_(sql_proxy, for_update,OB_STR_BACKUP_SCHEDULER_LEADER_EPOCH, epoch))) {
    if (OB_ENTRY_NOT_EXIST  != ret) {
      LOG_WARN("Failed to get inner table version", K(ret));
    } else {
      epoch = 0;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObBackupInfoOperator::set_backup_leader_epoch(common::ObISQLClient &sql_proxy, const int64_t epoch)
{
  int ret = OB_SUCCESS;
  if (epoch < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(epoch));
  } else if (OB_FAIL(set_int_value_(
      sql_proxy, OB_STR_BACKUP_SCHEDULER_LEADER_EPOCH, epoch))) {
    LOG_WARN("failed to set int value", K(ret), K(OB_STR_TENANT_NAME_BACKUP_SCHEMA_VERSION));
  }
  return ret;
}

int ObBackupInfoOperator::get_int_value_(common::ObISQLClient &sql_proxy, const bool for_update,
    const char *name, int64_t &value)
{
  int ret = OB_SUCCESS;
  char value_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = {0};
  value = -1;

  if (OB_FAIL(get_string_value_(sql_proxy, for_update, name,  value_str, sizeof(value_str)))) {
    LOG_WARN("failed to get string value", K(ret), K(name));
  } else if (OB_FAIL(ob_atoll(value_str, value))) {
    LOG_WARN("failed to parse int", K(ret), K(value_str));
  }

  return ret;
}

int ObBackupInfoOperator::set_int_value_(common::ObISQLClient &sql_proxy, const char *name, const int64_t &value)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml_splicer;

  if (OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(name));
  } else if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column("name", name))) {
    LOG_WARN("failed to add column", K(ret), K(name));
  } else if (OB_FAIL(dml_splicer.add_column("value", value))) {
    LOG_WARN("failed to add column", K(ret), K(value));
  } else if (OB_FAIL(dml_splicer.splice_insert_update_sql(OB_ALL_BACKUP_INFO_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", K(ret));
  } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql), K(affected_rows));
  } else {
    LOG_INFO("succeed to set int value", K(ret), K(name), K(value), K(affected_rows), K(sql));
  }

  return ret;
}

int ObBackupInfoOperator::get_string_value_(common::ObISQLClient &sql_proxy, const bool for_update,
    const char *name, char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  if (OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(name));
  } else {
    buf[0] = '\0';
  }

  SMART_VAR(ObMySQLProxy::ReadResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    int real_length = 0;

    if (OB_FAIL(sql.assign_fmt("select value from %s where tenant_id=%lu and name = '%s'", OB_ALL_BACKUP_INFO_TNAME, OB_SYS_TENANT_ID, name))) {
      LOG_WARN("failed to init sql", K(ret));
    } else if (for_update && OB_FAIL(sql.append(" for update"))) {
      LOG_WARN("failed to add for update", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("failed to read sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        LOG_WARN("failed to get next", K(ret), K(sql));
      }
    } else {
      EXTRACT_STRBUF_FIELD_MYSQL(*result, "value", buf, buf_len, real_length);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to extract field", K(ret), K(name), K(sql));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("get backup info string value", K(sql), K(name), K(real_length), K(buf));
      if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result must not be nullptr", K(ret));
      } else {
        int tmp_ret = result->next();
        if (OB_ITER_END != tmp_ret) {
          char value_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = {0};
          EXTRACT_STRBUF_FIELD_MYSQL(*result, name, value_str, sizeof(value_str), real_length);
          ret = OB_SUCCESS == tmp_ret ? OB_ERR_UNEXPECTED : tmp_ret;
          LOG_WARN("got more than one row", K(ret), K(tmp_ret), K(name), K(value_str), K(sql));
        }
      }
    }
  }

  return ret;
}

int ObBackupInfoOperator::set_string_value_(
    common::ObISQLClient &sql_proxy, const char *name, const char *value)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml_splicer;

  if (OB_ISNULL(name) || OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(name), KP(value));
  } else if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column("name", name))) {
    LOG_WARN("failed to add column", K(ret), K(name));
  } else if (OB_FAIL(dml_splicer.add_column("value", value))) {
    LOG_WARN("failed to add column", K(ret), K(value));
  } else if (OB_FAIL(dml_splicer.splice_insert_update_sql(OB_ALL_BACKUP_INFO_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", K(ret));
  } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql), K(affected_rows));
  } else {
    LOG_INFO("succeed to set string value", K(ret), K(name), K(value), K(affected_rows), K(sql));
  }

  return ret;

}

} //share
} //oceanbase

