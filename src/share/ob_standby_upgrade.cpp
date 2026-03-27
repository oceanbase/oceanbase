/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "ob_standby_upgrade.h"

using namespace oceanbase;
using namespace oceanbase::common;
namespace oceanbase
{
namespace share
{

OB_SERIALIZE_MEMBER(ObStandbyUpgrade, data_version_);

int ObUpgradeDataVersionMDSHelper::on_register(
    const char* buf,
    const int64_t len,
    storage::mds::BufferCtx &ctx)
{
  UNUSEDx(buf, len, ctx);
  return OB_SUCCESS;
}

int ObUpgradeDataVersionMDSHelper::on_replay(
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
