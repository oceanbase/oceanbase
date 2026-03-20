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

#ifndef OCEANBASE_SHARE_OB_SYNC_STANDBY_DEST_UTILS_H_
#define OCEANBASE_SHARE_OB_SYNC_STANDBY_DEST_UTILS_H_

#include "share/ob_rpc_struct.h"
#include "share/ob_sync_standby_dest_parser.h"

namespace oceanbase
{
namespace share
{
class ObSyncStandbyDestUtils
{
public:
  static int check_set_sync_standby_dest(const obrpc::ObAdminSetConfigArg &arg,
      bool &is_set_sync_standby_dest, uint64_t &tenant_id);
  static int admin_set_sync_standby_dest_config(const obrpc::ObAdminSetConfigArg &arg);
private:
  static int parse_sync_standby_dest_config(const uint64_t user_tenant_id,
     const common::ObString &value, ObSyncStandbyDestStruct &sync_standby_dest);
  static int update_sync_standby_dest(const uint64_t user_tenant_id, const bool is_empty,
    const ObSyncStandbyDestStruct &sync_standby_dest);
  static int check_can_change_sync_standby_dest(const ObAllTenantInfo &tenant_info);
};
}
}
#endif // OCEANBASE_SHARE_OB_SYNC_STANDBY_DEST_UTILS_H_