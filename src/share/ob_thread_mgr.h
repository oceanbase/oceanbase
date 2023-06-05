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
