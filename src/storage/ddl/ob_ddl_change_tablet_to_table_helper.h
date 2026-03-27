/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_CHANGE_TABLET_TO_TABLE_HELPER
#define OCEANBASE_STORAGE_OB_CHANGE_TABLET_TO_TABLE_HELPER

#include "common/ob_tablet_id.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/ob_define.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace share
{
class SCN;
}
namespace storage
{
namespace mds
{
struct BufferCtx;
}

class ObChangeTabletToTableHelper final
{
public:
  static int on_register(const char* buf,
                         const int64_t len,
                         mds::BufferCtx &ctx); // 出参，将对应修改记录在Ctx中

  static int on_replay(const char* buf,
                       const int64_t len,
                       const share::SCN &scn, // 日志scn
                       mds::BufferCtx &ctx); // 备机回放
};

inline int ObChangeTabletToTableHelper::on_register(const char* buf,
                                            const int64_t len,
                                            mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  return ret;
}

inline int ObChangeTabletToTableHelper::on_replay(const char* buf,
                                                  const int64_t len,
                                                  const share::SCN &scn, // 日志scn
                                                  mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  return ret;
}
} // namespace storage
} // namespace oceanbase
#endif