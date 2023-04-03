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

#include "share/ob_tenant_memstore_info_operator.h"

#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/unit/ob_unit_info.h"
#include "share/inner_table/ob_inner_table_schema.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
namespace share
{

bool ObTenantMemstoreInfoOperator::TenantServerMemInfo::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && server_.is_valid()
      && active_memstore_used_ >= 0 && total_memstore_used_ >= 0
      && major_freeze_trigger_ >= 0 && memstore_limit_ >= 0;
}

int ObTenantMemstoreInfoOperator::get(
    const uint64_t tenant_id,
    const common::ObIArray<common::ObAddr> &unit_servers,
    ObIArray<TenantServerMemInfo> &mem_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString unit_servers_str;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_ids is empty", K(tenant_id), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_servers.count(); ++i) {
      char svr_ip_str[common::MAX_IP_ADDR_LENGTH] = "";
      const common::ObAddr &unit_server = unit_servers.at(i);
      if (!unit_server.ip_to_string(svr_ip_str, static_cast<int32_t>(MAX_IP_ADDR_LENGTH))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to convert ip to string", K(ret));
      } else if (OB_FAIL(unit_servers_str.append_fmt(
              "%s(svr_ip='%s' and svr_port=%d)",
              (0 != i) ? " or " : "",
              svr_ip_str,
              unit_server.get_port()))) {
        LOG_WARN("fail to append fmt", K(ret));
      } else {} // no more to do
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.assign_fmt("SELECT tenant_id, svr_ip, svr_port, active_span, "
        "memstore_used, freeze_trigger, memstore_limit FROM %s "
        "WHERE tenant_id = %ld and (%s)",
        OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_TNAME, tenant_id, unit_servers_str.ptr()))) {
      LOG_WARN("assign_fmt failed", K(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObMySQLResult *result = NULL;
        if (OB_FAIL(proxy_.read(res, sql.ptr()))) {
          LOG_WARN("execute sql failed", K(sql), K(ret));
        } else if (NULL == (result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("execute sql failed", K(sql), K(ret));
        } else {
          TenantServerMemInfo mem_info;
          int64_t tmp_real_str_len = 0; // It is only used to fill out the parameters and does not work. It is necessary to ensure that there is no'\0' character in the corresponding string
          char svr_ip[OB_IP_STR_BUFF] = "";
          int64_t svr_port = 0;
          while (OB_SUCC(ret)) {
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END != ret) {
                LOG_WARN("result next failed", K(ret));
              } else {
                ret = OB_SUCCESS;
                break;
              }
            } else {
              EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", mem_info.tenant_id_, uint64_t);
              EXTRACT_STRBUF_FIELD_MYSQL(*result, "svr_ip", svr_ip, OB_IP_STR_BUFF, tmp_real_str_len);
              (void) tmp_real_str_len; // make compiler happy
              EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", svr_port, int64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "active_span",
                  mem_info.active_memstore_used_, int64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "memstore_used",
                  mem_info.total_memstore_used_, int64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "freeze_trigger",
                  mem_info.major_freeze_trigger_, int64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "memstore_limit",
                  mem_info.memstore_limit_, int64_t);
              if (OB_SUCC(ret)) {
                if (!mem_info.server_.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("invalid svr_ip or invalid svr_port", K(svr_ip), K(svr_port), K(ret));
                } else if (OB_FAIL(mem_infos.push_back(mem_info))) {
                  LOG_WARN("push_back failed", K(ret));
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

}//end namespace share
}//end namespace oceanbase

