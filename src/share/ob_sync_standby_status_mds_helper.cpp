/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/ob_sync_standby_status_mds_helper.h"

using namespace oceanbase;
using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{
int ObSyncStandbyStatusMDSHelper::on_register(
    const char* buf,
    const int64_t len,
    storage::mds::BufferCtx &ctx)
{
  UNUSEDx(buf, len, ctx);
  return OB_SUCCESS;
}

int ObSyncStandbyStatusMDSHelper::on_replay(
    const char* buf,
    const int64_t len,
    const share::SCN &scn,
    storage::mds::BufferCtx &ctx)
{
  UNUSEDx(buf, len, scn, ctx);
  return OB_SUCCESS;
}
}
}
