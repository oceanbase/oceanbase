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