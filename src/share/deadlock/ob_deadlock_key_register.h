/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
REGISTER(oceanbase::transaction::ObTransDeadlockDetectorKey, 65538)
#undef REGISTER
#endif
