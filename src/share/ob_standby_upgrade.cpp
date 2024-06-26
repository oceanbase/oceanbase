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
