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

#include "ob_ls_election_reference_info_operator.h"
#include "lib/net/ob_addr.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "lib/string/ob_sql_string.h"//ObSqlString
#include "lib/mysqlclient/ob_mysql_transaction.h"//ObMySQLTransaction
#include "common/ob_timeout_ctx.h"
#include "share/ob_share_util.h"
#include "share/ls/ob_ls_status_operator.h"
#include "share/scn.h"

namespace oceanbase
{
namespace share
{

using namespace common;

int ObLsElectionReferenceInfoOperator::create_new_ls(const ObLSStatusInfo &ls_info,
                                                     const SCN &create_ls_scn,
                                                     const common::ObString &zone_priority,
                                                     const share::ObTenantSwitchoverStatus &working_sw_status,
                                                     ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  UNUSEDx(create_ls_scn, working_sw_status);
  if (OB_UNLIKELY(!ls_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid_argument", KR(ret), K(ls_info));
  } else {
    common::ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
            "INSERT into %s (tenant_id, ls_id, zone_priority, manual_leader_server, blacklist) "
            "values (%ld, %ld, '%s', '%s', '%s')",
            OB_ALL_LS_ELECTION_REFERENCE_INFO_TNAME, ls_info.tenant_id_,
            ls_info.ls_id_.id(), zone_priority.ptr(), "0.0.0.0:0", ""))) {
      OB_LOG(WARN, "failed to assing sql", KR(ret), K(ls_info), K(create_ls_scn));
    } else if (OB_FAIL(exec_write(ls_info.tenant_id_, sql, this, trans))) {
      OB_LOG(WARN, "failed to exec write", KR(ret), K(ls_info), K(sql));
    }
    OB_LOG(INFO, "[LS_ELECTION] create new ls", KR(ret), K(ls_info), K(create_ls_scn));
  }
  return ret;
}

int ObLsElectionReferenceInfoOperator::drop_ls(const uint64_t &tenant_id,
                                               const share::ObLSID &ls_id,
                                               const ObTenantSwitchoverStatus &working_sw_status,
                                               ObMySQLTransaction &trans)
{
  UNUSEDx(working_sw_status);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid() || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN,"invalid_argument", KR(ret), K(ls_id), K(tenant_id));
  } else {
    common::ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("DELETE from %s where ls_id = %ld and tenant_id = %lu",
                               OB_ALL_LS_ELECTION_REFERENCE_INFO_TNAME, ls_id.id(), tenant_id))) {
      OB_LOG(WARN,"failed to assign sql", KR(ret), K(ls_id), K(sql));
    } else if (OB_FAIL(exec_write(tenant_id, sql, this, trans))) {
      OB_LOG(WARN,"failed to exec write", KR(ret), K(tenant_id), K(ls_id), K(sql));
    }
    OB_LOG(INFO,"[LS_ELECTION] drop ls", KR(ret), K(tenant_id), K(ls_id));
  }
  return ret;
}

int ObLsElectionReferenceInfoOperator::set_ls_offline(const uint64_t &,
                                                      const share::ObLSID &,
                                                      const ObLSStatus &,
                                                      const SCN &,
                                                      const ObTenantSwitchoverStatus &,
                                                      ObMySQLTransaction &) { return OB_SUCCESS; }


int ObLsElectionReferenceInfoOperator::update_ls_primary_zone(
      const uint64_t &tenant_id,
      const share::ObLSID &id,
      const common::ObZone &primary_zone,
      const common::ObString &zone_priority,
      ObMySQLTransaction &trans)
{
  UNUSEDx(primary_zone);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!id.is_valid()
                  || primary_zone.is_empty()
                  || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid_argument", KR(ret), K(id), K(primary_zone), K(tenant_id));
  } else {
    common::ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("UPDATE %s set zone_priority = '%.*s' where ls_id "
                               "= %ld and tenant_id = %lu",
                               OB_ALL_LS_ELECTION_REFERENCE_INFO_TNAME, zone_priority.length(),
                               zone_priority.ptr(), id.id(), tenant_id))) {
      OB_LOG(WARN, "failed to assign sql", KR(ret), K(id), K(primary_zone), K(sql), K(tenant_id));
    } else if (OB_FAIL(exec_write(tenant_id, sql, this, trans))) {
      OB_LOG(WARN, "failed to exec write", KR(ret), K(id), K(sql), K(tenant_id));
    }
  }
  return ret;
}


}
}
