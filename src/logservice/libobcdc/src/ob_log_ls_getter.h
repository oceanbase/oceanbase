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

#ifndef OCEANBASE_LOG_LS_GETTER_H_
#define OCEANBASE_LOG_LS_GETTER_H_

#include "lib/hash/ob_linear_hash_map.h"        // ObLinearHashMap
#include "ob_log_systable_helper.h"         // ObLogSysTableHelper
#include "ob_log_tenant.h"

namespace oceanbase
{
namespace libobcdc
{
class ObLogLsGetter
{
public:
  ObLogLsGetter();
  ~ObLogLsGetter();
  int init(const common::ObIArray<uint64_t> &tenant_ids);
  void destroy();

  int get_ls_ids(
      const uint64_t tenant_id,
      common::ObIArray<share::ObLSID> &ls_id_array);

private:
  int query_and_set_tenant_ls_info_(
      const uint64_t tenant_id);

  int query_tenant_ls_info_(
      const uint64_t tenant_id,
      ObLogSysTableHelper::TenantLSIDs &tenant_ls_ids);

private:
  typedef ObArray<share::ObLSID> LSIDArray;
  typedef common::ObLinearHashMap<TenantID, LSIDArray> TenantLSIDsCache;

  bool is_inited_;
  TenantLSIDsCache tenant_ls_ids_cache_;
};

} // namespace libobcdc
} // namespace oceanbase

#endif
