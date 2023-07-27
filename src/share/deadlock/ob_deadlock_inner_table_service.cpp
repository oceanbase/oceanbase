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

#include "share/ob_occam_time_guard.h"
#include "ob_deadlock_inner_table_service.h"
#include "observer/ob_server_struct.h"
#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/ob_define.h"
#include "lib/utility/utility.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "ob_deadlock_parameters.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "rootserver/ob_root_service.h"

namespace oceanbase
{
namespace share
{
namespace detector
{
using namespace common;

#define INSERT_DEADLOCK_EVENT_SQL "\
  insert into %s \
  values (%lu, %lu, '%.*s', %d, %lu, \
  '%s', %ld, %ld, '%.*s', '%.*s', %lu, '%s', %lu,\
  '%.*s', '%.*s', '%.*s',\
  '%.*s', '%.*s', '%.*s', '%.*s', '%.*s', '%.*s')"

#define LIMIT_VARCHAR_LEN 128

static const char* extra_info_if_exist(const ObIArray<ObString> &extra_info, int64_t idx)
{
  return idx < extra_info.count() ? extra_info.at(idx).ptr() : "";
}

int ObDeadLockInnerTableService::insert(const ObDetectorInnerReportInfo &inner_info,
                                        int64_t idx,
                                        int64_t size,
                                        int64_t current_ts)
{
  int ret = OB_SUCCESS;
  const ObDetectorUserReportInfo &user_info = inner_info.get_user_report_info();
  const ObIArray<ObString> &extra_names = user_info.get_extra_columns_names();
  const ObIArray<ObString> &extra_values = user_info.get_extra_columns_values();
  const uint64_t tenant_id = inner_info.get_tenant_id();
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);

  ObSqlString sql;
  int64_t affected_rows = 0;
  char ip_buffer[MAX_IP_ADDR_LENGTH + 1];

  DETECT_TIME_GUARD(100_ms);
  if (CLICK() && false == inner_info.get_addr().ip_to_string(ip_buffer, MAX_IP_ADDR_LENGTH)) {
    DETECT_LOG(WARN, "ip to string failed");
  } else if (CLICK() && OB_FAIL(sql.assign_fmt(INSERT_DEADLOCK_EVENT_SQL,
                                      OB_ALL_DEADLOCK_EVENT_HISTORY_TNAME,
                                      tenant_id,
                                      inner_info.get_event_id(),
                                      int(MAX_IP_ADDR_LENGTH), ip_buffer,
                                      inner_info.get_addr().get_port(),
                                      inner_info.get_detector_id(),
                                      ObTime2Str::ob_timestamp_str_range<YEAR, USECOND>(current_ts),
                                      idx, size,
                                      LIMIT_VARCHAR_LEN, inner_info.get_role().ptr(),
                                      LIMIT_VARCHAR_LEN, inner_info.get_priority().get_range_str(),
                                      inner_info.get_priority().get_value(),
                                      ObTime2Str::ob_timestamp_str_range<YEAR, USECOND>(inner_info.get_created_time()),
                                      inner_info.get_start_delay(),
                                      LIMIT_VARCHAR_LEN, user_info.get_module_name().ptr(),
                                      LIMIT_VARCHAR_LEN, user_info.get_resource_visitor().ptr(),
                                      LIMIT_VARCHAR_LEN, user_info.get_required_resource().ptr(),
                                      LIMIT_VARCHAR_LEN, extra_info_if_exist(extra_names, 0),
                                      LIMIT_VARCHAR_LEN, extra_info_if_exist(extra_values, 0),
                                      LIMIT_VARCHAR_LEN, extra_info_if_exist(extra_names, 1),
                                      LIMIT_VARCHAR_LEN, extra_info_if_exist(extra_values, 1),
                                      LIMIT_VARCHAR_LEN, extra_info_if_exist(extra_names, 2),
                                      LIMIT_VARCHAR_LEN, extra_info_if_exist(extra_values, 2)))) {
    DETECT_LOG(WARN, "format sql fail", KR(ret), K(sql));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "sql_proxy_ not init yet, report abort", KR(ret), K(sql));
  } else if (CLICK() && OB_FAIL(GCTX.sql_proxy_->write(exec_tenant_id,
                                                             sql.ptr(),
                                                             affected_rows))) {
    DETECT_LOG(WARN, "execute sql fail", KR(ret), K(tenant_id), K(exec_tenant_id), K(sql));
  } else {
    DETECT_LOG(INFO, "execute sql success", KR(ret), K(sql));
  }

  return ret;
  #undef CLICK_GUARD
}

int ObDeadLockInnerTableService::insert_all(const ObIArray<ObDetectorInnerReportInfo> &infos)
{
  int ret = OB_SUCCESS;

  DETECT_TIME_GUARD(100_ms);
  const int64_t current_ts = ObClockGenerator::getRealClock();
  for (int64_t i = 0; i < infos.count() && OB_SUCC(ret); ++i) {
    const ObDetectorInnerReportInfo &info = infos.at(i);
    if (CLICK() && OB_FAIL(insert(info, i + 1, infos.count(), current_ts))) {
      DETECT_LOG(WARN, "insert item failed", KR(ret), K(info));
    }
  }

  if (OB_SUCC(ret)) {
    DETECT_LOG(INFO, "insert items success", K(infos));
  }

  return ret;
}

// called by rs
int ObDeadLockInnerTableService::ObDeadLockEventHistoryTableOperator::async_delete()
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tenant_ids;
  const int64_t now = ObClockGenerator::getRealClock();
  ObSqlString sql;
  const int64_t rs_delete_timestap = now - REMAIN_RECORD_DURATION;

  DETECT_TIME_GUARD(3_s);
  rootserver::ObRootService *root_service = GCTX.root_service_;
  if (OB_ISNULL(root_service)) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "ptr is null", KR(ret), KP(root_service));
  } else if (CLICK() && OB_FAIL(schema::ObMultiVersionSchemaService::
                         get_instance().get_tenant_schema_guard(OB_SYS_TENANT_ID,
                                                                schema_guard))) {
    DETECT_LOG(WARN, "get schema guard failed", KR(ret));
  } else if (CLICK() && OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    DETECT_LOG(WARN, "get tenant ids failed", KR(ret));
  } else if (CLICK() && OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE report_time < %ld LIMIT 1024",
                                               OB_ALL_DEADLOCK_EVENT_HISTORY_TNAME,
                                               rs_delete_timestap))) {
    DETECT_LOG(WARN, "assign_fmt failed", KR(ret));
  } else {
    CLICK();
    for (int64_t idx = 0; OB_SUCC(ret) && idx < tenant_ids.count(); ++idx) {
      int64_t affected_rows = 0;
      uint64_t tenant_id = tenant_ids.at(idx);
      int temp_ret = OB_SUCCESS;
      if (!root_service->is_full_service()) {
        ret = OB_CANCELED;
        DETECT_LOG(WARN, "rs exit", KR(ret));
      } else if (is_user_tenant(tenant_id)) {
        // skip
      } else if (OB_SUCCESS !=
                 (temp_ret = root_service->get_sql_proxy().write(
                  tenant_id, sql.ptr(), affected_rows))) {
        DETECT_LOG(WARN, "execute delete sql failed", K(sql), K(tenant_id), KR(temp_ret));
      } else {
        DETECT_LOG(INFO, "delete old history record event", K(sql), K(tenant_id), K(affected_rows));
      }
    }
  }

  return ret;
}

ObDeadLockInnerTableService::ObDeadLockEventHistoryTableOperator
  &ObDeadLockInnerTableService::ObDeadLockEventHistoryTableOperator::get_instance()
{
  static ObDeadLockInnerTableService::ObDeadLockEventHistoryTableOperator op;
  return op;
}

}// namespace detector
}// namespace share
}// namespace oceanbase
