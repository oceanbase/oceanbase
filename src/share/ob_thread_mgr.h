/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_THREAD_MGR_H_
#define OB_THREAD_MGR_H_

#include "lib/thread/thread_mgr.h"
#include "share/ob_thread_pool.h"

namespace oceanbase
{
namespace lib {
namespace TGDefIDs {
enum OBTGDefIDEnum
{
  OB_START = TGDefIDs::LIB_END - 1,
#define TG_DEF(id, ...) id,
#include "share/ob_thread_define.h"
#undef TG_DEF
  OB_END,
};
}
} // end of namespace lib
} // end of namespace oceanbase

#endif // OB_THREAD_MGR_H_
