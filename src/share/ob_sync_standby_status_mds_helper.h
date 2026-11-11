/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_SYNC_STANDBY_STATUS_MDS_HELPER_H_
#define OCEANBASE_SHARE_OB_SYNC_STANDBY_STATUS_MDS_HELPER_H_

#include "storage/multi_data_source/buffer_ctx.h"

namespace oceanbase
{
namespace share
{
class ObSyncStandbyStatusMDSHelper
{
public:
  static int on_register(
      const char* buf,
      const int64_t len,
      storage::mds::BufferCtx &ctx);
  static int on_replay(
      const char* buf,
      const int64_t len,
      const share::SCN &scn,
      storage::mds::BufferCtx &ctx);

};
}
}
#endif // OCEANBASE_SHARE_OB_SYNC_STANDBY_STATUS_MDS_HELPER_H_
