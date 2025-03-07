//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX SHARE
#include "share/storage/ob_i_pre_warmer.h"
#include "storage/ob_tenant_tablet_stat_mgr.h"
namespace oceanbase
{
namespace share
{

int ObPreWarmerParam::init(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPreWarmerType tmp_type = PRE_WARM_TYPE_NONE;
  if (tablet_id.is_user_tablet()) {
    storage::ObTabletStatAnalyzer tablet_analyzer;
    if (OB_TMP_FAIL(MTL(storage::ObTenantTabletStatMgr *)
                ->get_tablet_analyzer(ls_id, tablet_id, tablet_analyzer))) {
      if (OB_HASH_NOT_EXIST != tmp_ret) {
        LOG_WARN_RET(tmp_ret, "Failed to get tablet stat analyzer", K(ls_id), K(tablet_id));
      }
    } else {
      tmp_type = MEM_PRE_WARM;
    }
  }
  type_ = tmp_type;
  return ret;
}

} // namespace share
} // namespace oceanbase
