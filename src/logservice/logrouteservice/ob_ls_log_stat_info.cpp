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

#define USING_LOG_PREFIX  OBLOG
#include "ob_ls_log_stat_info.h"

namespace oceanbase
{
namespace logservice
{
/////////////////////////////////// LogStatRecord ///////////////////////////////
int64_t LogStatRecord::to_string(char *buffer, int64_t length) const
{
  int64_t pos = 0;
  (void)databuff_printf(buffer, length, pos, "{svr=%s, role=%d, LSN:[%ld, %ld]}",
      to_cstring(server_), role_, begin_lsn_.val_, end_lsn_.val_);

  return pos;
}

void ObLSLogInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  log_stat_records_.reset();
}

int ObLSLogInfo::init(const uint64_t tenant_id, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(ls_id));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    log_stat_records_.reset();
  }

  return ret;
}

int ObLSLogInfo::add(const LogStatRecord &log_stat_record)
{
  int ret = OB_SUCCESS;

  if (!is_valid() || !log_stat_record.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "ObLSLogInfo", *this, K(log_stat_record));
  } else {
    if (OB_FAIL(log_stat_records_.push_back(log_stat_record))) {
      LOG_WARN("insert log_stat_record failed", KR(ret), K(log_stat_record));
    }
  }

  return ret;
}

} // namespace logservice
} // namespace oceanbase

