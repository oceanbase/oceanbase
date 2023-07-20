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

#include "ob_log_restore_define.h"
#include "lib/net/ob_addr.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "logservice/palf/log_define.h"
#include "share/rc/ob_tenant_base.h"
#include <cstdint>

namespace oceanbase
{
namespace logservice
{
void ObLogRestoreErrorContext::reset()
{
  ret_code_ = OB_SUCCESS;
  trace_id_.reset();
  err_lsn_ = palf::LSN(palf::LOG_INVALID_LSN_VAL);
}

ObLogRestoreErrorContext &ObLogRestoreErrorContext::operator=(const ObLogRestoreErrorContext &other)
{
  ret_code_ = other.ret_code_;
  trace_id_.set(other.trace_id_);
  return *this;
}

void ObRestoreLogContext::reset()
{
  seek_done_ = false;
  lsn_ = palf::LSN(palf::LOG_INVALID_LSN_VAL);
}

void ObLogRestoreSourceTenant::reset()
{
  source_cluster_id_ = OB_INVALID_CLUSTER_ID;
  source_tenant_id_ = OB_INVALID_TENANT_ID;
  user_name_.reset();
  user_passwd_.reset();
  is_oracle_ = false;
  ip_list_.reset();
}

int ObLogRestoreSourceTenant::set(const ObLogRestoreSourceTenant &other)
{
  int ret = OB_SUCCESS;
  source_cluster_id_ = other.source_cluster_id_;
  source_tenant_id_ = other.source_tenant_id_;
  is_oracle_ = other.is_oracle_;
  if (OB_FAIL(user_name_.assign(other.user_name_))) {
    CLOG_LOG(WARN, "assign user_name_ failed", K(other));
  } else if (OB_FAIL(user_passwd_.assign(other.user_passwd_))) {
    CLOG_LOG(WARN, "assign user_passwd_ failed", K(other));
  } else if (OB_FAIL(ip_list_.assign(other.ip_list_))) {
    CLOG_LOG(WARN, "assign ip_list_ failed", K(other));
  }
  return ret;
}

bool ObLogRestoreSourceTenant::is_valid() const
{
  return OB_INVALID_CLUSTER_ID != source_cluster_id_
    && OB_INVALID_TENANT_ID != source_tenant_id_
    && !user_name_.is_empty()
    && !user_passwd_.is_empty()
    && ip_list_.count() > 0;
}

} // namespace logservice
} // namespace oceanbase
