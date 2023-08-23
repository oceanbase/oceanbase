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

#include "logservice/ob_switch_leader_adapter.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "logservice/leader_coordinator/table_accessor.h"

namespace oceanbase
{
namespace logservice
{

int ObSwitchLeaderAdapter::add_to_election_blacklist(const int64_t palf_id, const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (palf_id < 0 || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    logservice::coordinator::LsElectionReferenceInfoRow row(MTL_ID(), share::ObLSID(palf_id));
    if (OB_FAIL(row.add_server_to_blacklist(server, logservice::coordinator::InsertElectionBlacklistReason::MIGRATE)) &&
        OB_ENTRY_EXIST != ret) {
      CLOG_LOG(WARN, "add_to_election_blacklist failed", K(ret), K(palf_id), K(server));
    } else {
      ret = OB_SUCCESS;
      CLOG_LOG(INFO, "add_to_election_blacklist success", K(ret), K(palf_id), K(server));
    }
  }
  return ret;
}

int ObSwitchLeaderAdapter::remove_from_election_blacklist(const int64_t palf_id, const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (palf_id < 0 || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    logservice::coordinator::LsElectionReferenceInfoRow row(MTL_ID(), share::ObLSID(palf_id));
    if (OB_FAIL(row.delete_server_from_blacklist(server)) && OB_ENTRY_NOT_EXIST != ret) {
      bool is_dropped = false;
      int tmp_ret = OB_SUCCESS;
      const bool is_meta_t = is_meta_tenant(MTL_ID());
      const bool is_sys_t = is_sys_tenant(MTL_ID());
      // 1. election blacklist of user tenant stores in corresponding meta tenant,
      // so check if meta tenant exists when failed to delete
      // 2. election blacklist of meta tenant stores in sys tenant, just return if failed
      if (is_meta_t || is_sys_t ||
          OB_SUCCESS != (tmp_ret = is_meta_tenant_dropped_(MTL_ID(), is_dropped)) ||
          !is_dropped) {
        CLOG_LOG(WARN, "delete_server_from_blacklist failed", K(ret), K(tmp_ret), K(palf_id), K(server), K(is_sys_t), K(is_meta_t), K(is_dropped));
      } else {
        ret = OB_SUCCESS;
        CLOG_LOG(INFO, "delete_server_from_blacklist success", K(ret), K(palf_id), K(server), K(is_dropped));
      }
    } else {
      ret = OB_SUCCESS;
      CLOG_LOG(INFO, "delete_server_from_blacklist success", K(ret), K(palf_id), K(server));
    }
  }
  return ret;
}

int ObSwitchLeaderAdapter::set_election_blacklist(const int64_t palf_id, const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (palf_id < 0 || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    logservice::coordinator::LsElectionReferenceInfoRow row(MTL_ID(), share::ObLSID(palf_id));
    if (OB_FAIL(row.set_or_replace_server_in_blacklist(server, logservice::coordinator::InsertElectionBlacklistReason::MIGRATE)) &&
        OB_ENTRY_EXIST != ret) {
      CLOG_LOG(WARN, "set_election_blacklist failed", K(ret), K(palf_id), K(server));
    } else {
      ret = OB_SUCCESS;
      CLOG_LOG(INFO, "set_election_blacklist success", K(ret), K(palf_id), K(server));
    }
  }
  return ret;
}

int ObSwitchLeaderAdapter::is_meta_tenant_dropped_(const uint64_t tenant_id, bool &is_dropped)
{
  int ret = OB_SUCCESS;
  share::schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  share::schema::ObSchemaGetterGuard guard;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  is_dropped = false;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "schema_service is null", KR(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    CLOG_LOG(WARN, "fail to get schema guard", KR(ret), K(tenant_id), K(meta_tenant_id));
  } else if (OB_FAIL(guard.check_if_tenant_has_been_dropped(meta_tenant_id, is_dropped))) {
    CLOG_LOG(WARN, "fail to check if tenant has been dropped", KR(ret), K(tenant_id), K(meta_tenant_id));
  }
  return ret;
}

} // end namespace logservice
} // end namespace oceanbase
