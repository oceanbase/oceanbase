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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_store_key.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"  // databuff_printf

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{
ObLogStoreKey::ObLogStoreKey() : tenant_ls_id_(), log_lsn_()
{
}

ObLogStoreKey::~ObLogStoreKey()
{
  reset();
}

void ObLogStoreKey::reset()
{
  tenant_ls_id_.reset();
  log_lsn_.reset();
}

int ObLogStoreKey::init(const logservice::TenantLSID &tenant_ls_id,
    const palf::LSN &log_lsn)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!tenant_ls_id.is_valid())
        || OB_UNLIKELY(!log_lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    tenant_ls_id_ = tenant_ls_id;
    log_lsn_ = log_lsn;
  }

  return ret;
}

bool ObLogStoreKey::is_valid() const
{
  bool bool_ret = false;

  bool_ret = tenant_ls_id_.is_valid()
    && log_lsn_.is_valid();

  return bool_ret;
}

int ObLogStoreKey::get_key(std::string &key)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("store_key is not valid", KR(ret), KPC(this));
  } else {
    key.append(std::to_string(tenant_ls_id_.get_tenant_id()));
    key.append("_");
    key.append(std::to_string(tenant_ls_id_.get_ls_id().id()));
    key.append("_");
    key.append(std::to_string(log_lsn_.val_));
  }

  return ret;
}

int64_t ObLogStoreKey::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != buf && buf_len > 0) {
    (void)common::databuff_printf(buf, buf_len, pos, "key:%lu_", tenant_ls_id_.get_tenant_id());
    (void)common::databuff_printf(buf, buf_len, pos, "%ld_", tenant_ls_id_.get_ls_id().id());
    (void)common::databuff_printf(buf, buf_len, pos, "%lu", log_lsn_.val_);
  }
  return pos;
}

}
}
