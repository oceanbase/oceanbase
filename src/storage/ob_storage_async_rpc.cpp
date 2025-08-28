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

#define USING_LOG_PREFIX STORAGE

#include "ob_storage_async_rpc.h"
#include "share/ob_force_print_log.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;


ObHAAsyncRpcArg::ObHAAsyncRpcArg()
  : tenant_id_(OB_INVALID_ID),
    group_id_(0),
    rpc_timeout_(0),
    member_addr_list_()
{
}

ObHAAsyncRpcArg::~ObHAAsyncRpcArg()
{
}

bool  ObHAAsyncRpcArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && group_id_ >= 0
      && rpc_timeout_ > 0
      && !member_addr_list_.empty();
}

void ObHAAsyncRpcArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  group_id_ = 0;
  rpc_timeout_ = 0;
  member_addr_list_.reset();
}

int ObHAAsyncRpcArg::assign(const ObHAAsyncRpcArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign ha async arg get invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(member_addr_list_.assign(arg.member_addr_list_))) {
    LOG_WARN("failed to assign member addr list", K(ret), K(arg));
  } else {
    tenant_id_ = arg.tenant_id_;
    group_id_ = arg.group_id_;
    rpc_timeout_ = arg.rpc_timeout_;
  }
  return ret;
}

int ObHAAsyncRpcArg::set_ha_async_arg(
    const uint64_t tenant_id,
    const int32_t group_id,
    const int64_t rpc_timeout,
    const common::ObIArray<common::ObAddr> &member_addr_list)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || group_id < 0 || rpc_timeout <= 0 || member_addr_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set ha async arg get invalid argument", K(ret), K(tenant_id), K(group_id), K(rpc_timeout), K(member_addr_list));
  } else if (OB_FAIL(member_addr_list_.assign(member_addr_list))) {
    LOG_WARN("failed to assign member addr list", K(ret), K(member_addr_list));
  } else {
    tenant_id_ = tenant_id;
    group_id_ = group_id;
    rpc_timeout_ = rpc_timeout;
  }
  return ret;
}

