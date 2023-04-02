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

#include "share/ls/ob_ls_table.h"      // for functions in this cpp
#include "share/ls/ob_ls_info.h"       // for ObLSInfo
#include "storage/tx_storage/ob_ls_service.h" // ObLSService
#include "logservice/ob_log_service.h"        // ObLogService
#include "storage/tx_storage/ob_ls_handle.h" // ObLSHandle

namespace oceanbase
{
namespace common
{
class ObAddr;
}

namespace share
{

class ObLSReplica;
class ObLSInfo;

ObLSTable::ObLSTable()
{
}

ObLSTable:: ~ObLSTable()
{
}

bool ObLSTable::is_valid_key(const uint64_t tenant_id, const ObLSID &ls_id)
{
  bool valid = true;
  if (common::OB_INVALID_TENANT_ID == tenant_id || !ls_id.is_valid()) {
    valid = false;
    LOG_WARN_RET(OB_INVALID_ERROR, "invalid tenant and log stream id", KT(tenant_id), K(ls_id));
  }
  return valid;
}

int ObLSTable::check_need_update(
    const ObLSReplica &lhs,
    const ObLSReplica &rhs,
    bool &update)
{
  int ret = common::OB_SUCCESS;
  if (!lhs.is_valid() || !rhs.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(lhs), K(rhs));
  } else if (lhs.get_tenant_id() != rhs.get_tenant_id()
      || lhs.get_ls_id() != rhs.get_ls_id()
      || lhs.get_server() != rhs.get_server()) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("different replicas of lss compared", KR(ret), K(lhs), K(rhs));
  } else {
    bool diff = false;
    if (lhs.get_member_list().count() != rhs.get_member_list().count()) {
      diff = true;
    } else {
      for (int64_t i = 0; i < lhs.get_member_list().count(); ++i) {
        if (lhs.get_member_list().at(i) != rhs.get_member_list().at(i)) {
          diff = true;
          break;
        }
      }
    }
    update = (diff
        || lhs.get_sql_port() != rhs.get_sql_port()
        || lhs.get_role() != rhs.get_role()
        || lhs.get_replica_type() != rhs.get_replica_type()
        || lhs.get_proposal_id() != rhs.get_proposal_id()
        || lhs.get_replica_status() != rhs.get_replica_status()
        || lhs.get_restore_status() != rhs.get_restore_status()
        || lhs.get_memstore_percent() != rhs.get_memstore_percent()
        || lhs.get_unit_id() != rhs.get_unit_id()
        || lhs.get_zone() != rhs.get_zone()
        || lhs.get_paxos_replica_number() != rhs.get_paxos_replica_number()
        || lhs.get_data_size() != rhs.get_data_size()
        || lhs.get_required_size() != rhs.get_required_size());
  }
  return ret;
}

int ObLSTable::get_member_list(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObMemberList &member_list)
{
  int ret = OB_SUCCESS;
  int64_t paxos_replica_number = 0;

  MTL_SWITCH(tenant_id) {
    ObLSService *ls_svr = nullptr;
    ObLSHandle ls_handle;
    if (OB_ISNULL(ls_svr = MTL(ObLSService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("MTL ObLSService failed", KR(ret), K(tenant_id), K(ls_id), K(MTL_ID()));
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::SHARE_MOD))) {
      LOG_WARN("get ls handle failed", KR(ret), K(tenant_id), K(ls_id));
    } else if (OB_ISNULL(ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls_handle.get_ls() is nullptr", KR(ret));
    } else if (OB_FAIL(ls_handle.get_ls()->get_paxos_member_list(member_list, paxos_replica_number))) {
      LOG_WARN("get role from ObLS failed", KR(ret), K(tenant_id), K(ls_id), K(ls_handle));
    }
  }
  return ret;
}

int ObLSTable::get_role(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    common::ObRole &role)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    palf::PalfHandleGuard palf_handle_guard;
    logservice::ObLogService *log_service = nullptr;
    int64_t proposal_id = 0;  // unused
    if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("MTL ObLogService is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(log_service->open_palf(ls_id, palf_handle_guard))) {
      LOG_WARN("open palf failed", KR(ret), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(palf_handle_guard.get_role(role, proposal_id))) {
      LOG_WARN("get role failed", KR(ret), K(tenant_id), K(ls_id));
    }
  }
  return ret;
}
} // end namespace share
} // end namespace oceanbase
