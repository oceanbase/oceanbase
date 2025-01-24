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

#include "ob_reconfig_checker_adapter.h"
#include "rootserver/mview/ob_collect_mv_merge_info_task.h"

namespace oceanbase
{
namespace logservice
{

ObReconfigCheckerAdapter::ObReconfigCheckerAdapter() : guard_()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  timeout_ = -1;
}

int ObReconfigCheckerAdapter::init(const uint64_t tenant_id,
                                   const share::ObLSID &ls_id,
                                   const int64_t &timeout)
{
  int ret = OB_SUCCESS;

  if (OB_INVALID_TENANT_ID == tenant_id
      || !ls_id.is_valid()
      || timeout < 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K(tenant_id), K(ls_id), K(timeout));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    timeout_ = timeout;
    ret = guard_.init(tenant_id, ls_id, timeout);
  }
  return ret;
}

int ObReconfigCheckerAdapter::check_can_add_member(const ObAddr &server,
                                                   const int64_t timeout_us)
{
  int ret = OB_SUCCESS;

  const int64_t start_time = fast_current_time();
  const int64_t sleep_time = 300 * 1000; // 300ms
  do {
    if (OB_FAIL(ObMVCheckReplicaHelper::check_can_add_member(server, tenant_id_, ls_id_, timeout_us))) {
      PALF_LOG(WARN, "check can add member failed", K(ret),
                K(timeout_us), K(ls_id_), K(tenant_id_));
    }
    if (fast_current_time() - start_time >= timeout_us) {
      // if passed check, not return timeout
      ret = OB_SUCC(ret) ?  OB_SUCCESS : OB_TIMEOUT;
      PALF_LOG(WARN, "check can add member timeout", K(ret), KPC(this), K(start_time), K(timeout_us));
    }
    // if check failed, retry to timeout
    if (OB_FAIL(ret) && ret != OB_TIMEOUT) {
      usleep(sleep_time);
    }
  } while (OB_FAIL(ret) && ret != OB_TIMEOUT);

  if (OB_SUCC(ret)) {
    ret = guard_.check_can_add_member(server, timeout_us);
  }

  return ret;
}

int ObReconfigCheckerAdapter::check_can_change_memberlist(const ObMemberList &new_member_list,
                                                          const int64_t paxos_replica_num,
                                                          const int64_t timeout_us)
{
  return guard_.check_can_change_member(new_member_list, paxos_replica_num, timeout_us);
}

} // end namespace logservice
} // end namespace oceanbase
