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

#include "ob_define.h"
#include "share/ob_server_struct.h"
#include "share/inner_table/ob_sslog_table_schema.h"

namespace oceanbase {
namespace common {

bool is_shared_storage_sslog_table(const uint64_t tid)
{
  return is_shared_storage_sslog_exist() && is_sslog_table(tid);
}

bool is_sslog_table(const uint64_t tid)
{
  return share::OB_ALL_SSLOG_TABLE_TID == tid;
}

bool is_shared_storage_sslog_exist()
{
  return GCTX.is_shared_storage_mode();
}

bool is_hardcode_schema_table(const uint64_t tid)
{
  return is_shared_storage_sslog_table(tid) || share::OB_ALL_CORE_TABLE_TID == tid;
}

bool is_tenant_sslog_ls(const uint64_t tenant_id, const share::ObLSID &ls_id)
{
  return GCTX.is_shared_storage_mode()
      && ls_id.is_sslog_ls()
      && (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id));
}

bool is_tenant_has_sslog(const uint64_t tenant_id)
{
  return GCTX.is_shared_storage_mode() && (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id));
}

}
} // namespace oceanbase
