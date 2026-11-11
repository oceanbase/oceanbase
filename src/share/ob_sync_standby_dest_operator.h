/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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