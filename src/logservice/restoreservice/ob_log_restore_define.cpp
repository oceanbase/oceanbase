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
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "logservice/palf/log_define.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_tenant.h"
#include <cstdint>

namespace oceanbase
{
namespace logservice
{
void ObLogRestoreErrorContext::reset()
{
  ret_code_ = OB_SUCCESS;
  trace_id_.reset();
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

int64_t get_restore_concurrency_by_max_cpu(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t concurrency = 0;
  omt::ObTenant *tenant = NULL;
  if (OB_FAIL(GCTX.omt_->get_tenant(tenant_id, tenant))) {
    CLOG_LOG(WARN, "get tenant failed", K(tenant_id));
  } else {
    concurrency = (tenant->unit_max_cpu() + 7) / 8;
  }
  return concurrency;
}

} // namespace logservice
} // namespace oceanbase
