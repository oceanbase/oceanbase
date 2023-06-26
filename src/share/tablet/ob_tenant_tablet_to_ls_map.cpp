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

#include "common/ob_tablet_id.h"                    // ObTabletID
#include "lib/mysqlclient/ob_mysql_proxy.h"         // ObMySQLProxy
#include "share/tablet/ob_tablet_info.h"            // ObTabletLSPair
#include "share/tablet/ob_tablet_to_ls_iterator.h" // ObTenantTabletToLSIterator

#include "share/tablet/ob_tenant_tablet_to_ls_map.h"

namespace oceanbase
{
namespace share
{

int ObTenantTabletToLSMap::build(const uint64_t tenant_id,
    common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;

  ObTenantTabletToLSIterator iter;
  if (OB_FAIL(iter.init(sql_proxy, tenant_id))) {
    LOG_WARN("init iter fail", KR(ret), K(tenant_id));
  } else {
    ObTabletLSPair tablet_ls_pair;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.next(tablet_ls_pair))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("iter next fail", KR(ret), K(tenant_id));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(map_.set_refactored(tablet_ls_pair.get_tablet_id(), tablet_ls_pair.get_ls_id()))) {
        LOG_WARN("tablet_to_ls map set fail", KR(ret), K(tenant_id), K(tablet_ls_pair));
      }
    }
  }

  return ret;
}


} // end namespace share
} // end namespace oceanbase
