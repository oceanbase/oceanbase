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

#ifndef OCEANBASE_SHARE_OB_SYNC_STANDBY_DEST_OPERATOR_H_
#define OCEANBASE_SHARE_OB_SYNC_STANDBY_DEST_OPERATOR_H_

#include "share/ob_sync_standby_dest_parser.h"

namespace oceanbase
{
namespace share
{
class ObSyncStandbyDestOperator
{
public:
  ObSyncStandbyDestOperator();
  ~ObSyncStandbyDestOperator();
  static int read_sync_standby_dest(ObISQLClient &sql_client, const uint64_t meta_tenant_id, const bool for_update,
    bool &is_empty, ObSyncStandbyDestStruct &sync_standby_dest, int64_t *ora_rowscn = nullptr);
  static int write_sync_standby_dest(ObISQLClient &sql_client, const uint64_t meta_tenant_id, const ObSyncStandbyDestStruct &sync_standby_dest);
  static int delete_sync_standby_dest(ObISQLClient &sql_client, const uint64_t meta_tenant_id);
private:
  static const uint64_t OB_SYNC_STANDBY_DEST_ID;
};
} // namespace share
} // namespace oceanbase
#endif // OCEANBASE_SHARE_OB_SYNC_STANDBY_DEST_OPERATOR_H_