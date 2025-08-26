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

#include "ob_flashback_standby_log_struct.h"

namespace oceanbase
{
namespace share
{
int ObFlashbackStandbyLogArg::init(const uint64_t tenant_id, const SCN &flashback_log_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || !flashback_log_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(flashback_log_scn));
  } else {
    tenant_id_ = tenant_id;
    flashback_log_scn_ = flashback_log_scn;
  }
  return ret;
}
bool ObFlashbackStandbyLogArg::is_valid() const
{
  return is_user_tenant(tenant_id_) && flashback_log_scn_.is_valid_and_not_min();
}
int ObFlashbackStandbyLogArg::assign(const ObFlashbackStandbyLogArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    flashback_log_scn_ = other.flashback_log_scn_;
  }
  return ret;
}
void ObFlashbackStandbyLogArg::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  flashback_log_scn_.set_invalid();
}

OB_SERIALIZE_MEMBER(ObClearFetchedLogCacheArg, tenant_id_);
int ObClearFetchedLogCacheArg::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
  }
  return ret;
}
bool ObClearFetchedLogCacheArg::is_valid() const
{
  return is_user_tenant(tenant_id_);
}
int ObClearFetchedLogCacheArg::assign(const ObClearFetchedLogCacheArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
  }
  return ret;
}
void ObClearFetchedLogCacheArg::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
}

OB_SERIALIZE_MEMBER(ObClearFetchedLogCacheRes, tenant_id_, addr_);
int ObClearFetchedLogCacheRes::init(const uint64_t tenant_id, const ObAddr addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || !addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(addr));
  } else {
    tenant_id_ = tenant_id;
    addr_ = addr;
  }
  return ret;
}
bool ObClearFetchedLogCacheRes::is_valid() const
{
  return is_user_tenant(tenant_id_) && addr_.is_valid();
}
int ObClearFetchedLogCacheRes::assign(const ObClearFetchedLogCacheRes &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    addr_ = other.addr_;
  }
  return ret;
}
void ObClearFetchedLogCacheRes::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  addr_.reset();
}
} // share
} // oceanbase