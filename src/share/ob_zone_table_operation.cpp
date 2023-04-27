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

#include "share/ob_zone_table_operation.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_zone_info.h"
#include "common/ob_timeout_ctx.h"
#include "rootserver/ob_root_utils.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
namespace share
{

template <typename T>
int ObZoneTableOperation::set_info_item(
    const char *name, const int64_t value, const char *info_str, T &info)
{
  int ret = OB_SUCCESS;
  // %value and %info_str can be arbitrary values
  if (NULL == name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid name", K(ret));
  } else {
    ObZoneInfoItem *it = info.list_.get_first();
    while (OB_SUCCESS == ret && it != info.list_.get_header()) {
      if (NULL == it) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null iter", K(ret));
      } else {
        if (strncasecmp(it->name_, name, OB_MAX_COLUMN_NAME_LENGTH) == 0) {
          it->value_ = value;
          it->info_ = info_str;
          break;
        }
        it = it->get_next();
      }
    }
    if (OB_SUCC(ret)) {
      // ignore unknown item
      if (it == info.list_.get_header()) {
        LOG_WARN("unknown item", K(name), K(value), "info", info_str);
      }
    }
  }
  return ret;
}

template <typename T>
int ObZoneTableOperation::load_info(
    common::ObISQLClient &sql_client,
    T &info,
    const bool check_zone_exists)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  bool zone_exists = false;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    ObTimeoutCtx ctx;
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.assign_fmt("SELECT name, value, info FROM %s WHERE zone = '%s'",
        OB_ALL_ZONE_TNAME, info.zone_.ptr()))) {
      LOG_WARN("append sql failed", K(ret));
    } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get sql result", K(ret));
    } else {
      int64_t tmp_real_str_len = 0; // only used for output parameter
      char name[OB_MAX_COLUMN_NAME_BUF_LENGTH] = "";
      int64_t value = 0;
      char info_str[MAX_ZONE_INFO_LENGTH + 1] = "";
      while (OB_SUCCESS == ret && OB_SUCCESS == (ret = result->next())) {
        zone_exists = true;
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name", name,
                                   static_cast<int64_t>(sizeof(name)), tmp_real_str_len);
        EXTRACT_INT_FIELD_MYSQL(*result, "value", value, int64_t);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "info", info_str,
                                   static_cast<int64_t>(sizeof(info_str)), tmp_real_str_len);
        (void) tmp_real_str_len; // make compiler happy
        if (OB_SUCC(ret)) {
          if (OB_FAIL(set_info_item(name, value, info_str, info))) {
            LOG_WARN("set info item failed", K(ret), K(name), K(value), K(info_str));
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        if (check_zone_exists && !zone_exists) {
          ret = OB_ZONE_INFO_NOT_EXIST;
          LOG_WARN("zone not exists", KR(ret), K(sql));
        }
      } else {
        LOG_WARN("get result failed", K(ret), K(sql));
      }
    }

  }
  return ret;
}

int ObZoneTableOperation::load_global_info(
    ObISQLClient &sql_client,
    ObGlobalInfo &info,
    const bool check_zone_exists /* = false */)
{
  return load_info(sql_client, info, check_zone_exists);
}

int ObZoneTableOperation::load_zone_info(
    ObISQLClient &sql_client,
    ObZoneInfo &info,
    const bool check_zone_exists /* = false */)
{
  return load_info(sql_client, info, check_zone_exists);
}

template <typename T>
int ObZoneTableOperation::insert_info(ObISQLClient &sql_client, T &info)
{
  int ret = OB_SUCCESS;
  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info));
  } else {
    DLIST_FOREACH(it, info.list_) {
      const bool insert = true;
      if (OB_FAIL(update_info_item(sql_client, info.zone_, *it, insert))) {
        LOG_WARN("insert item failed", K(ret), "zone", info.zone_, "item", *it);
        break;
      }
    }
  }
  return ret;
}

int ObZoneTableOperation::insert_global_info(ObISQLClient &sql_client, ObGlobalInfo &info)
{
  int ret = OB_SUCCESS;
  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info));
  } else if (OB_FAIL(insert_info(sql_client, info))) {
    LOG_WARN("insert info failed", K(ret), K(info));
  }
  return ret;
}

int ObZoneTableOperation::insert_zone_info(ObISQLClient &sql_client, ObZoneInfo &info)
{
  int ret = OB_SUCCESS;
  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info));
  } else if (OB_FAIL(insert_info(sql_client, info))) {
    LOG_WARN("insert info failed", K(ret), K(info));
  }
  return ret;
}

int ObZoneTableOperation::update_info_item(common::ObISQLClient &sql_client,
    const common::ObZone &zone, const ObZoneInfoItem &item, bool insert /* = false */)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  // %zone can be empty
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item));
  } else {
    if (insert) {
      if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (zone, name, value, info, gmt_modified, gmt_create)"
          " VALUES('%s', '%s', %ld, '%s', now(6), now(6))",
          OB_ALL_ZONE_TNAME, zone.ptr(),
          item.name_, item.value_, item.info_.ptr()))) {
        LOG_WARN("assign sql failed", K(ret));
      }
    } else {
      if (OB_FAIL(sql.assign_fmt(
          "UPDATE %s SET value = %ld, info = '%s', gmt_modified = now(6) "
          "WHERE zone = '%s' AND name = '%s'",
          OB_ALL_ZONE_TNAME, item.value_, item.info_.ptr(), zone.ptr(), item.name_))) {
        LOG_WARN("assign sql failed", K(ret));
      }
    }
  }

  int64_t affected_rows = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql_client.write(sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(sql));
  } else if (!(is_single_row(affected_rows) || is_zero_row(affected_rows))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", K(ret), K(affected_rows));
  } else {
    LOG_TRACE("execute sql success", K(sql));
  }
  return ret;
}

int ObZoneTableOperation::get_zone_lease_info(ObISQLClient &sql_client,
    ObZoneLeaseInfo &info)
{
  int ret = OB_SUCCESS;
  HEAP_VARS_3((ObGlobalInfo, global_info), (ObZoneInfo, zone_info),
              (ObMySQLProxy::MySQLResult, res)) {
    zone_info.zone_= info.zone_;
    ObMySQLResult *result = NULL;
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
        "SELECT zone, name, value, info FROM %s WHERE zone = '' OR zone = '%s'",
        OB_ALL_ZONE_TNAME, info.zone_.ptr()))) {
      LOG_WARN("assign sql failed", K(ret));
    } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get sql result", K(ret));
    } else {
      int64_t tmp_real_str_len = 0; // only used for output parameter
      while (OB_SUCCESS == ret && OB_SUCCESS == (ret = result->next())) {
        ObZone zone;
        char name[OB_MAX_COLUMN_NAME_BUF_LENGTH] = "";
        int64_t value = 0;
        char info_str[MAX_ZONE_INFO_LENGTH + 1] = "";
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "zone", zone.ptr(),
                                   MAX_ZONE_LENGTH, tmp_real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name", name,
                                   static_cast<int64_t>(sizeof(name)), tmp_real_str_len);
        EXTRACT_INT_FIELD_MYSQL(*result, "value", value, int64_t);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "info", info_str,
                                   static_cast<int64_t>(sizeof(info_str)), tmp_real_str_len);
        (void) tmp_real_str_len; // make compiler happy
        if (OB_SUCC(ret)) {
          if (zone.is_empty()) {
            if (OB_FAIL(set_info_item(name, value, info_str, global_info))) {
              LOG_WARN("set info item failed", K(ret), K(name), K(value), K(info_str));
            }
          } else {
            if (OB_FAIL(set_info_item(name, value, info_str, zone_info))) {
              LOG_WARN("set info item failed", K(ret), K(name), K(value), K(info_str));
            }
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get result failed", K(ret), K(sql));
      }
    }
    if (OB_SUCC(ret)) {
      ObZoneLeaseInfo t;
      t.zone_ = zone_info.zone_;
      t.privilege_version_ = global_info.privilege_version_;
      t.config_version_ = global_info.config_version_;
      t.lease_info_version_ = global_info.lease_info_version_;
      t.time_zone_info_version_ = global_info.time_zone_info_version_;
      if (t.privilege_version_ < 0 || t.config_version_ < 0
          || t.lease_info_version_ < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid zone lease info", K(ret), "lease_info", t);
      } else if (t.lease_info_version_ < info.lease_info_version_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("lease_info_version must not less than old lease_info_version",
            K(ret), "new_value", t.lease_info_version_, "old_value", info.lease_info_version_);
      } else {
        info = t;
      }
    }
  }
  return ret;
}

int ObZoneTableOperation::get_zone_list(
    ObISQLClient &sql_client, common::ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  zone_list.reset();
  ObZone zone;

  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    char sql[OB_SHORT_SQL_LENGTH];
    int n = snprintf(sql, sizeof(sql), "select distinct(zone) as zone "
                     "from %s where zone != ''", OB_ALL_ZONE_TNAME);
    ObTimeoutCtx ctx;
    if (n < 0 || n >= OB_SHORT_SQL_LENGTH) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("sql buf not enough", K(ret), K(n));
    } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql_client.read(res, sql))) {
      LOG_WARN("failed to do read", K(sql), K(ret));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get result", K(sql), K(ret));
    } else {
      int64_t tmp_real_str_len = 0; // only used for output parameter
      while (OB_SUCCESS == ret && OB_SUCCESS == (ret = result->next())) {
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "zone", zone.ptr(), MAX_ZONE_LENGTH, tmp_real_str_len);
        (void) tmp_real_str_len; // make compiler happy
        if (OB_FAIL(zone_list.push_back(zone))) {
          LOG_WARN("failed to add zone list", K(ret));
        }
      }

      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get zone list", K(sql), K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }

  }
  return ret;
}

int ObZoneTableOperation::remove_zone_info(ObISQLClient &sql_client,
                                           const ObZone &zone)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t item_cnt = 0;
  if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE zone = '%s'",
      OB_ALL_ZONE_TNAME, zone.ptr()))) {
    LOG_WARN("sql assign_fmt failed", K(ret));
  } else if (OB_FAIL(sql_client.write(sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(sql), K(ret));
  } else if (OB_FAIL(get_zone_item_count(item_cnt))) {
    LOG_WARN("get zone item count failed", K(ret));
  } else if (item_cnt != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows not right", "expected affected_rows",
        item_cnt, K(affected_rows), K(ret));
  }
  return ret;
}

int ObZoneTableOperation::get_zone_item_count(int64_t &cnt)
{
  int ret = OB_SUCCESS;
  ObMalloc alloc(ObModIds::OB_TEMP_VARIABLES);
  ObPtrGuard<ObZoneInfo> zone_info_guard(alloc);
  if (OB_FAIL(zone_info_guard.init())) {
    LOG_WARN("init temporary variable failed", K(ret));
  } else if (NULL == zone_info_guard.ptr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null zone info ptr", K(ret));
  } else {
    cnt = zone_info_guard.ptr()->get_item_count();
  }
  return ret;
}


int ObZoneTableOperation::get_region_list(
    common::ObISQLClient &sql_client, common::ObIArray<common::ObRegion> &region_list)
{
  int ret = OB_SUCCESS;
  region_list.reset();
  ObRegion region;

  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    char sql[OB_SHORT_SQL_LENGTH];
    int n = snprintf(sql, sizeof(sql), "select info as region"
                     " from %s where zone != '' and name = 'region' ", OB_ALL_ZONE_TNAME);
    ObTimeoutCtx ctx;
    if (n < 0 || n >= OB_SHORT_SQL_LENGTH) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("sql buf not enough", K(ret), K(n));
    } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql_client.read(res, sql))) {
      LOG_WARN("failed to do read", K(sql), K(ret));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get result", K(sql), K(ret));
    } else {
      int64_t tmp_real_str_len = 0; // 仅用于填充出参，不起作用，需保证对应的字符串中间没有'\0'字符
      while (OB_SUCCESS == ret && OB_SUCCESS == (ret = result->next())) {
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "region", region.ptr(), MAX_REGION_LENGTH, tmp_real_str_len);
        (void) tmp_real_str_len; // make compiler happy
        if (OB_FAIL(region_list.push_back(region))) {
          LOG_WARN("failed to add zone list", K(ret));
        }
      }

      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get zone list", K(sql), K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }

  }
  return ret;
}

int ObZoneTableOperation::check_encryption_zone(
    common::ObISQLClient &sql_client,
    const common::ObZone &zone,
    bool &encryption)
{
  int ret = OB_SUCCESS;
  encryption = false;
  HEAP_VAR(ObZoneInfo, zone_info) {
    if (OB_UNLIKELY(zone.is_empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("the zone is empty", KR(ret), K(zone));
    } else if (OB_FAIL(get_zone_info(zone, sql_client, zone_info))) {
      LOG_WARN("fail to get zone info", KR(ret), K(zone));
    } else {
      encryption = zone_info.is_encryption();
    }
  }
  return ret;
}
int ObZoneTableOperation::check_zone_active(
    common::ObISQLClient &sql_client,
    const common::ObZone &zone,
    bool &is_active)
{
  int ret = OB_SUCCESS;
  is_active = false;
  HEAP_VAR(ObZoneInfo, zone_info) {
    if (OB_UNLIKELY(zone.is_empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("the zone is empty", KR(ret), K(zone));
    } else if (OB_FAIL(get_zone_info(zone, sql_client, zone_info))) {
      LOG_WARN("fail to get zone info", KR(ret), K(zone));
    } else {
      is_active = zone_info.is_active();
    }
  }
  return ret;
}
int ObZoneTableOperation::get_inactive_zone_list(
      common::ObISQLClient &sql_client,
      common::ObIArray<common::ObZone> &zone_list)
{
  return get_zone_list_(sql_client, zone_list, false /* is_active */);
}
int ObZoneTableOperation::get_active_zone_list(
      common::ObISQLClient &sql_client,
      common::ObIArray<common::ObZone> &zone_list)
{
  return get_zone_list_(sql_client, zone_list, true /* is_active */);
}
int ObZoneTableOperation::get_zone_list_(
    common::ObISQLClient &sql_client,
    common::ObIArray<common::ObZone> &zone_list,
    const bool is_active)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObTimeoutCtx ctx;
  zone_list.reset();
  ObZone zone;
  if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (OB_FAIL(sql.assign_fmt("SELECT zone FROM %s WHERE name = 'status' AND info = '%s'",
      OB_ALL_ZONE_TNAME, is_active ? "ACTIVE" : "INACTIVE"))) {
    LOG_WARN("fail to append sql", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_client.read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), K(sql));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("result next failed", KR(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else {
            int64_t tmp_real_str_len = 0;
            zone.reset();
            EXTRACT_STRBUF_FIELD_MYSQL(*result, "zone", zone.ptr(), MAX_ZONE_LENGTH, tmp_real_str_len);
            (void) tmp_real_str_len; // make compiler happy
            if (OB_FAIL(zone_list.push_back(zone))) {
              LOG_WARN("fail to push an element into zone_list", KR(ret), K(zone));
            }
          }
        }
      }
    }
  }
  FLOG_INFO("get inactive zone_list", KR(ret), K(zone_list));
  return ret;
}
int ObZoneTableOperation::get_zone_info(
      const ObZone &zone,
      common::ObISQLClient &sql_client,
      ObZoneInfo &zone_info)
{
  int ret = OB_SUCCESS;
  zone_info.reset();
  zone_info.zone_ = zone;
  bool check_zone_exists = true;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the zone is empty", KR(ret), K(zone));
  } else if (OB_FAIL(load_zone_info(sql_client, zone_info, check_zone_exists))) {
    LOG_WARN("fail to load zone info", KR(ret), K(zone));
  } else if (OB_UNLIKELY(!zone_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone_info is unexpectedly invalid",
        KR(ret), K(zone), K(zone_info));
  } else {}
  return ret;
}
}//end namespace share
}//end namespace oceanbase
