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

#ifdef NEED_DEFINE
#include "share/deadlock/test/test_key.h"// module unittest
#include "storage/tx/ob_trans_define.h"
#include "storage/tablelock/ob_table_lock_deadlock.h"
#endif

// user's key forward declaration
// for template specialization
#ifdef NEED_DECLARATION
namespace oceanbase
{
namespace unittest
{
  class ObDeadLockTestIntKey;
  class ObDeadLockTestDoubleKey;
}// namespace unittest
namespace transaction
{
  class ObTransID;
namespace tablelock
{
  class ObTransLockPartID;
  class ObTransLockPartBlockCallBack;
} // namespace tablelock

}// namespace transaction
}
#endif

// user choose a unique ID for his key
// for runtime reflection
#ifdef NEED_REGISTER
#define REGISTER(T, ID) USER_REGISTER(T, ID)
REGISTER(oceanbase::transaction::ObTransID, 1)
REGISTER(oceanbase::unittest::ObDeadLockTestIntKey, 65535)
REGISTER(oceanbase::unittest::ObDeadLockTestDoubleKey, 65536)
REGISTER(oceanbase::transaction::tablelock::ObTransLockPartID, 65537)
#undef REGISTER
#endif
